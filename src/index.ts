import express, { Request, Response } from 'express';
import cors from 'cors';
import path from 'path';
import fs from 'fs';
import pino from 'pino';
import dotenv from 'dotenv';
import { execFile } from 'child_process';
import { promisify } from 'util';
import os from 'os';
import makeWASocket, {
    useMultiFileAuthState,
    DisconnectReason,
    fetchLatestBaileysVersion,
    makeCacheableSignalKeyStore,
    WASocket,
    delay
} from '@whiskeysockets/baileys';
import { Boom } from '@hapi/boom';
import QRCode from 'qrcode';

const execFileAsync = promisify(execFile);

/**
 * Convert an audio buffer (any format supported by ffmpeg) to OGG/Opus.
 * Returns the converted buffer, or the original if ffmpeg is unavailable.
 */
async function convertToOgg(inputBuffer: Buffer, logFn: (msg: string) => void): Promise<Buffer> {
    const tmpIn = path.join(os.tmpdir(), `wa_audio_${Date.now()}_in.webm`);
    const tmpOut = path.join(os.tmpdir(), `wa_audio_${Date.now()}_out.ogg`);
    try {
        fs.writeFileSync(tmpIn, inputBuffer);
        await execFileAsync('ffmpeg', [
            '-y', '-i', tmpIn,
            '-vn',
            '-c:a', 'libopus',
            '-b:a', '32k',
            '-ar', '48000',
            '-ac', '1',
            tmpOut
        ]);
        const converted = fs.readFileSync(tmpOut);
        logFn(`ffmpeg conversion success: ${inputBuffer.length} bytes -> ${converted.length} bytes`);
        return converted;
    } catch (err: any) {
        logFn(`ffmpeg conversion failed: ${err.message}. Sending original buffer.`);
        return inputBuffer;
    } finally {
        try { fs.unlinkSync(tmpIn); } catch {}
        try { fs.unlinkSync(tmpOut); } catch {}
    }
}

dotenv.config();

const app = express();
const port = process.env.PORT || 3000;
const apiKey = process.env.API_KEY || 'Blehkenzz123';

app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ limit: '50mb', extended: true }));

// Secure endpoints with API Key
app.use((req: Request, res: Response, next) => {
    if (req.path === '/') {
        return next();
    }

    const reqApiKey = req.headers['apikey'] || req.query.apiKey;
    if (!reqApiKey || reqApiKey !== apiKey) {
        return res.status(401).json({ error: 'Unauthorized: Invalid API Key' });
    }
    next();
});

// Global state trackers
const activeSockets: { [sessionId: string]: WASocket } = {};
const qrCodes: { [sessionId: string]: string } = {};
const sessionStates: {
    [sessionId: string]: {
        status: 'connected' | 'disconnected' | 'connecting';
        phone?: string;
        profileName?: string;
        profilePicture?: string;
    }
} = {};

// Custom message store definition for decryption retries
interface CustomStoreMessage {
    key: {
        remoteJid?: string | null;
        id?: string | null;
        fromMe?: boolean | null;
    };
    message?: any;
    messageTimestamp?: number | null;
    pushName?: string | null;
}

class CustomMessageStore {
    private messages: Map<string, CustomStoreMessage> = new Map();
    private sessionDir: string;

    constructor(sessionDir: string) {
        this.sessionDir = sessionDir;
    }

    private getFilePath(): string {
        return path.join(this.sessionDir, 'messages-store.json');
    }

    public load() {
        const filePath = this.getFilePath();
        try {
            if (fs.existsSync(filePath)) {
                const data = JSON.parse(fs.readFileSync(filePath, 'utf8'));
                for (const [key, msg] of Object.entries(data)) {
                    this.messages.set(key, msg as CustomStoreMessage);
                }
                console.log(`[CustomStore] Loaded ${this.messages.size} messages from cache for ${path.basename(this.sessionDir)}.`);
            }
        } catch (e) {
            console.error('[CustomStore] Failed to load messages store:', e);
        }
    }

    public save() {
        const filePath = this.getFilePath();
        try {
            const data: Record<string, CustomStoreMessage> = {};
            for (const [key, msg] of this.messages.entries()) {
                data[key] = msg;
            }
            fs.writeFileSync(filePath, JSON.stringify(data, null, 2));
        } catch (e) {
            console.error('[CustomStore] Failed to save messages store:', e);
        }
    }

    public loadMessage(remoteJid: string, id: string): CustomStoreMessage | undefined {
        const key = `${remoteJid}:${id}`;
        return this.messages.get(key);
    }

    public saveMessage(remoteJid: string, id: string, message: CustomStoreMessage) {
        const key = `${remoteJid}:${id}`;
        this.messages.set(key, message);

        // Keep size manageable: prune messages older than 2 days or if size > 5000
        if (this.messages.size > 5000) {
            const keysArray = Array.from(this.messages.keys());
            for (let i = 0; i < 1000; i++) {
                this.messages.delete(keysArray[i]);
            }
        }
    }

    public bind(ev: any) {
        ev.on('messages.upsert', (m: any) => {
            if (m.type !== 'notify' && m.type !== 'append') return;
            for (const message of m.messages) {
                if (message.key.remoteJid && message.key.id) {
                    this.saveMessage(message.key.remoteJid, message.key.id, message);
                }
            }
        });

        ev.on('messages.update', (updates: any[]) => {
            for (const update of updates) {
                if (update.key.remoteJid && update.key.id) {
                    const existing = this.loadMessage(update.key.remoteJid, update.key.id);
                    if (existing) {
                        if (update.update) {
                            if (update.update.message) {
                                existing.message = update.update.message;
                            }
                            if (update.update.messageTimestamp) {
                                existing.messageTimestamp = update.update.messageTimestamp;
                            }
                            if (update.update.pushName) {
                                existing.pushName = update.update.pushName;
                            }
                        }
                        this.saveMessage(update.key.remoteJid, update.key.id, existing);
                    }
                }
            }
        });
    }
}

const stores: { [sessionId: string]: CustomMessageStore } = {};
const storeIntervals: { [sessionId: string]: NodeJS.Timeout } = {};
const reconnectAttempts: { [sessionId: string]: number } = {};
const sentMessageIds = new Map<string, number>();

function markAsSent(messageId: string) {
    sentMessageIds.set(messageId, Date.now());
    if (sentMessageIds.size > 20000) {
        const twoHoursAgo = Date.now() - 7200000;
        for (const [id, ts] of sentMessageIds.entries()) {
            if (ts < twoHoursAgo) {
                sentMessageIds.delete(id);
            }
        }
    }
}

function hasBeenSent(messageId: string): boolean {
    return sentMessageIds.has(messageId);
}

const logger = pino({ level: 'silent' });

/**
 * Helper to recursively unwrap ephemeral or view-once messages to get the real content.
 */
function getMessageContent(messageContent: any): any {
    if (!messageContent) return null;
    
    let content = messageContent;
    let unwrapped = true;
    
    while (unwrapped) {
        unwrapped = false;
        if (!content || typeof content !== 'object') {
            break;
        }
        if (content.ephemeralMessage && content.ephemeralMessage.message) {
            content = content.ephemeralMessage.message;
            unwrapped = true;
        } else if (content.viewOnceMessage && content.viewOnceMessage.message) {
            content = content.viewOnceMessage.message;
            unwrapped = true;
        } else if (content.viewOnceMessageV2 && content.viewOnceMessageV2.message) {
            content = content.viewOnceMessageV2.message;
            unwrapped = true;
        } else if (content.viewOnceMessageV2Extension && content.viewOnceMessageV2Extension.message) {
            content = content.viewOnceMessageV2Extension.message;
            unwrapped = true;
        } else if (content.documentWithCaptionMessage && content.documentWithCaptionMessage.message) {
            content = content.documentWithCaptionMessage.message;
            unwrapped = true;
        }
    }
    
    return content;
}


// Ensure session root folder exists
const sessionsRootDir = path.join(__dirname, '../sessions');
if (!fs.existsSync(sessionsRootDir)) {
    fs.mkdirSync(sessionsRootDir, { recursive: true });
}

function getLidMap(sessionId: string): Record<string, string> {
    const mapPath = path.join(sessionsRootDir, sessionId, 'lid-map.json');
    try {
        if (fs.existsSync(mapPath)) {
            return JSON.parse(fs.readFileSync(mapPath, 'utf8'));
        }
    } catch (e) {
        console.error(`[WA-Engine] Failed to read lid-map.json for session ${sessionId}:`, e);
    }
    return {};
}

function saveLidMap(sessionId: string, map: Record<string, string>) {
    const mapPath = path.join(sessionsRootDir, sessionId, 'lid-map.json');
    try {
        const dir = path.dirname(mapPath);
        if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir, { recursive: true });
        }
        fs.writeFileSync(mapPath, JSON.stringify(map, null, 2));
    } catch (e) {
        console.error(`[WA-Engine] Failed to save lid-map.json for session ${sessionId}:`, e);
    }
}

function resolveLidFromAllSessions(lid: string): string | null {
    try {
        const folders = fs.readdirSync(sessionsRootDir);
        for (const folder of folders) {
            const mapPath = path.join(sessionsRootDir, folder, 'lid-map.json');
            if (fs.existsSync(mapPath)) {
                const map = JSON.parse(fs.readFileSync(mapPath, 'utf8'));
                if (map[lid]) {
                    return map[lid];
                }
            }
        }
    } catch (e) {
        console.error('[WA-Engine] Failed to resolve LID from all sessions:', e);
    }
    return null;
}

function isKnownLid(lid: string): boolean {
    try {
        const folders = fs.readdirSync(sessionsRootDir);
        for (const folder of folders) {
            const mapPath = path.join(sessionsRootDir, folder, 'lid-map.json');
            if (fs.existsSync(mapPath)) {
                const map = JSON.parse(fs.readFileSync(mapPath, 'utf8'));
                if (lid in map) {
                    return true;
                }
            }
        }
    } catch (e) {}
    return false;
}

function resolvePhoneToLidFromAllSessions(phone: string): string | null {
    try {
        const folders = fs.readdirSync(sessionsRootDir);
        for (const folder of folders) {
            const mapPath = path.join(sessionsRootDir, folder, 'lid-map.json');
            if (fs.existsSync(mapPath)) {
                const map = JSON.parse(fs.readFileSync(mapPath, 'utf8'));
                for (const [lid, mappedPhone] of Object.entries(map)) {
                    if (mappedPhone === phone) {
                        return lid;
                    }
                }
            }
        }
    } catch (e) {
        console.error('[WA-Engine] Failed to resolve Phone to LID from all sessions:', e);
    }
    return null;
}

function formatJid(to: string): string {
    if (to.includes('@')) {
        return to;
    }
    const cleanTo = to.replace(/\D/g, '');
    if (isKnownLid(cleanTo)) {
        return `${cleanTo}@lid`;
    }
    const resolvedLid = resolvePhoneToLidFromAllSessions(cleanTo);
    if (resolvedLid) {
        return `${resolvedLid}@lid`;
    }
    return `${cleanTo}@s.whatsapp.net`;
}


/**
 * Initialize a single Baileys WhatsApp Session.
 */
async function initSession(sessionId: string): Promise<WASocket> {
    if (activeSockets[sessionId]) {
        return activeSockets[sessionId];
    }

    console.log(`[WA-Engine] Initializing WhatsApp session: ${sessionId}`);
    const sessionDir = path.join(sessionsRootDir, sessionId);
    if (!fs.existsSync(sessionDir)) {
        fs.mkdirSync(sessionDir, { recursive: true });
    }

    const store = new CustomMessageStore(sessionDir);
    store.load();
    stores[sessionId] = store;

    const saveInterval = setInterval(() => {
        try {
            store.save();
        } catch (err) {}
    }, 10000);
    storeIntervals[sessionId] = saveInterval;

    const { state, saveCreds } = await useMultiFileAuthState(sessionDir);
    const { version } = await fetchLatestBaileysVersion();

    const sock = makeWASocket({
        version,
        auth: {
            creds: state.creds,
            keys: makeCacheableSignalKeyStore(state.keys, logger),
        },
        printQRInTerminal: false,
        logger,
        getMessage: async (key) => {
            const sessionStore = stores[sessionId];
            if (sessionStore) {
                const msg = await sessionStore.loadMessage(key.remoteJid!, key.id!);
                return msg?.message || undefined;
            }
            return undefined;
        }
    });

    store.bind(sock.ev);

    activeSockets[sessionId] = sock;
    sessionStates[sessionId] = { status: 'connecting' };

    sock.ev.on('creds.update', saveCreds);

    sock.ev.on('connection.update', async (update: any) => {
        const { connection, lastDisconnect, qr } = update;

        if (qr) {
            try {
                // Generate a highly compact base64 data-URI QR code for the frontend
                const qrBase64 = await QRCode.toDataURL(qr);
                qrCodes[sessionId] = qrBase64;
                sessionStates[sessionId] = { status: 'connecting' };
                console.log(`[WA-Engine] QR Code generated for session: ${sessionId}`);
            } catch (err) {
                console.error('[WA-Engine] Failed to generate QR base64:', err);
            }
        }

        if (connection === 'close') {
            const statusCode = (lastDisconnect?.error as Boom)?.output?.statusCode;
            delete qrCodes[sessionId];

            if (storeIntervals[sessionId]) {
                clearInterval(storeIntervals[sessionId]);
                delete storeIntervals[sessionId];
            }

            reconnectAttempts[sessionId] = (reconnectAttempts[sessionId] || 0) + 1;
            const isLoggedOut = statusCode === DisconnectReason.loggedOut;
            const maxAttemptsExceeded = reconnectAttempts[sessionId] > 5;

            console.log(`[WA-Engine] Connection closed for ${sessionId} (LoggedOut: ${isLoggedOut}, Retries: ${reconnectAttempts[sessionId]}/5, Code: ${statusCode})`);

            if (isLoggedOut || maxAttemptsExceeded) {
                console.log(`[WA-Engine] Stopping reconnection attempts for ${sessionId}.`);
                sessionStates[sessionId] = { status: 'disconnected' };
                delete activeSockets[sessionId];
                delete stores[sessionId];
                if (isLoggedOut) {
                    try {
                        fs.rmSync(sessionDir, { recursive: true, force: true });
                    } catch (e) {
                        console.error('[WA-Engine] Failed to delete session dir:', e);
                    }
                }
            } else {
                delete activeSockets[sessionId];
                // Wait 5 seconds before trying to reconnect
                setTimeout(() => initSession(sessionId), 5000);
            }
        } else if (connection === 'open') {
            console.log(`[WA-Engine] Session ${sessionId} connected successfully!`);
            reconnectAttempts[sessionId] = 0;
            delete qrCodes[sessionId];

            const jid = sock.user?.id;
            const phone = jid ? jid.split(':')[0] : '';
            const profileName = sock.user?.name || 'WhatsApp User';

            let profilePicture = '';
            try {
                // Fetch profile avatar
                profilePicture = await sock.profilePictureUrl(jid!, 'image') || '';
            } catch (e) {
                // Fallback if picture is private or missing
            }

            sessionStates[sessionId] = {
                status: 'connected',
                phone,
                profileName,
                profilePicture,
            };
        }
    });

    // Handle Incoming Chats and forward them back to Laravel via Webhook
    sock.ev.on('messages.upsert', async (m: any) => {
        if (m.type !== 'notify' && m.type !== 'append') return;

        for (const message of m.messages) {
            // Keep fromMe messages too so replies sent directly from WhatsApp/Web
            // by internal CS are synced into NexaBot's inbox history.
            if (!message.message) continue;

            // Unwrap ephemeral or view once messages recursively
            const content = getMessageContent(message.message);
            if (!content || typeof content !== 'object' || Object.keys(content).length === 0) {
                continue;
            }

            // Intercept message revocation/deletion (revoke event)
            const protocolMsg = content.protocolMessage;
            if (protocolMsg && (protocolMsg.type === 0 || protocolMsg.type === 'REVOKE' || protocolMsg.type === 'revoke')) {
                const revokedMessageId = protocolMsg.key?.id;
                if (revokedMessageId) {
                    console.log(`[WA-Engine] Message revoked/deleted: ${revokedMessageId}`);
                    const senderJid = message.key.remoteJid;
                    if (senderJid) {
                        let senderPhone = senderJid.split('@')[0];
                        let whatsappLid = senderJid.endsWith('@lid') ? senderJid.split('@')[0] : null;
                        const keyAny = message.key as any;
                        const altJid = keyAny.senderPn || keyAny.remoteJidAlt;
                        if (altJid && typeof altJid === 'string' && altJid.endsWith('@s.whatsapp.net')) {
                            senderPhone = altJid.split('@')[0];
                            whatsappLid = senderJid.split('@')[0];
                        }
                        
                        try {
                            const laravelWebhookUrl = 'http://app:8000/api/whatsapp/webhook';
                            const payload = {
                                sessionId,
                                senderPhone,
                                senderName: message.pushName || 'WhatsApp Contact',
                                messageId: message.key.id,
                                text: '',
                                messageType: 'revoke',
                                mediaUrl: null,
                                timestamp: message.messageTimestamp,
                                fromMe: message.key.fromMe || false,
                                whatsappLid,
                                revokedMessageId
                            };
                            
                            await fetch(laravelWebhookUrl, {
                                method: 'POST',
                                headers: { 'Content-Type': 'application/json' },
                                body: JSON.stringify(payload)
                            });
                        } catch (e) {
                            console.error('[WA-Engine] Revoke webhook exception:', e);
                        }
                    }
                    continue;
                }
            }

            // Blacklist of system / protocol / reaction messages
            const systemKeys = [
                'protocolMessage',
                'senderKeyDistributionMessage',
                'reactionMessage',
                'pinInChatMessage',
                'keepInChatMessage',
                'chatInviteLinkGroupTypeMessage',
                'cancelPaymentRequestMessage',
                'paymentRequestMessage',
                'declinePaymentRequestMessage',
                'paymentInviteMessage'
            ];
            
            const hasSystemKey = Object.keys(content).some(key => systemKeys.includes(key));
            if (hasSystemKey) {
                console.log(`[WA-Engine] Skipping system/protocol message with keys: ${Object.keys(content).join(', ')}`);
                continue;
            }

            let text = content.conversation ||
                content.extendedTextMessage?.text ||
                '';
            
            let messageType = 'text';
            let mediaUrl = null;

            // Handle Media Messages
            if (content.imageMessage) {
                messageType = 'image';
                text = content.imageMessage.caption || text;
                try {
                    const { downloadMediaMessage } = require('@whiskeysockets/baileys');
                    const buffer = await downloadMediaMessage(
                        message,
                        'buffer',
                        {},
                        { logger }
                    );
                    const mimetype = content.imageMessage.mimetype || 'image/jpeg';
                    mediaUrl = `data:${mimetype};base64,${buffer.toString('base64')}`;
                } catch (e) {
                    console.error('[WA-Engine] Failed to download image:', e);
                }
            } else if (content.documentMessage) {
                messageType = 'document';
                text = content.documentMessage.title || content.documentMessage.fileName || text;
                try {
                    const { downloadMediaMessage } = require('@whiskeysockets/baileys');
                    const buffer = await downloadMediaMessage(
                        message,
                        'buffer',
                        {},
                        { logger }
                    );
                    const mimetype = content.documentMessage.mimetype || 'application/octet-stream';
                    mediaUrl = `data:${mimetype};base64,${buffer.toString('base64')}`;
                } catch (e) {
                    console.error('[WA-Engine] Failed to download document:', e);
                }
            } else if (content.videoMessage) {
                messageType = 'video';
                text = content.videoMessage.caption || text;
                try {
                    const { downloadMediaMessage } = require('@whiskeysockets/baileys');
                    const buffer = await downloadMediaMessage(
                        message,
                        'buffer',
                        {},
                        { logger }
                    );
                    const mimetype = content.videoMessage.mimetype || 'video/mp4';
                    mediaUrl = `data:${mimetype};base64,${buffer.toString('base64')}`;
                } catch (e) {
                    console.error('[WA-Engine] Failed to download video:', e);
                }
            } else if (content.audioMessage) {
                messageType = 'audio';
                text = text || '[Audio]';
                try {
                    const { downloadMediaMessage } = require('@whiskeysockets/baileys');
                    const buffer = await downloadMediaMessage(
                        message,
                        'buffer',
                        {},
                        { logger }
                    );
                    const mimetype = content.audioMessage.mimetype || 'audio/ogg';
                    mediaUrl = `data:${mimetype};base64,${buffer.toString('base64')}`;
                } catch (e) {
                    console.error('[WA-Engine] Failed to download audio:', e);
                }
            }

            const senderJid = message.key.remoteJid;
            if (!senderJid) continue;

            // Skip status updates (SW) and group messages
            if (senderJid === 'status@broadcast' || senderJid.endsWith('@g.us') || senderJid.endsWith('@broadcast')) {
                continue;
            }

            let senderPhone = senderJid.split('@')[0];
            let whatsappLid: string | null = senderJid.endsWith('@lid') ? senderJid.split('@')[0] : null;

            const keyAny = message.key as any;
            const altJid = keyAny.senderPn || keyAny.remoteJidAlt;
            if (altJid && typeof altJid === 'string' && altJid.endsWith('@s.whatsapp.net')) {
                senderPhone = altJid.split('@')[0];
                whatsappLid = senderJid.split('@')[0];
                
                // Save to local cache
                if (whatsappLid) {
                    const map = getLidMap(sessionId);
                    if (map[whatsappLid] !== senderPhone) {
                        map[whatsappLid] = senderPhone;
                        saveLidMap(sessionId, map);
                    }
                }
            } else if (whatsappLid) {
                // Try to resolve from cache
                const map = getLidMap(sessionId);
                if (map[whatsappLid]) {
                    senderPhone = map[whatsappLid];
                } else {
                    const resolved = resolveLidFromAllSessions(whatsappLid);
                    if (resolved) {
                        senderPhone = resolved;
                        map[whatsappLid] = resolved;
                        saveLidMap(sessionId, map);
                    } else {
                        map[whatsappLid] = whatsappLid;
                        saveLidMap(sessionId, map);
                    }
                }
            }

            const senderName = message.pushName || 'WhatsApp Contact';

            console.log(`[WA-Engine] Incoming message from ${senderPhone} (LID: ${whatsappLid || 'none'}) to session ${sessionId}: "${text}" (Type: ${messageType})`);

            // POST event back to Laravel App Container on private docker network
            try {
                const laravelWebhookUrl = 'http://app:8000/api/whatsapp/webhook';
                const payload = {
                    sessionId,
                    senderPhone,
                    senderName,
                    messageId: message.key.id,
                    text,
                    messageType,
                    mediaUrl,
                    timestamp: message.messageTimestamp,
                    fromMe: message.key.fromMe || false,
                    whatsappLid
                };

                // Node Fetch or standard fetch in Node 18+
                const response = await fetch(laravelWebhookUrl, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload)
                });
                if (response.ok && message.key.id) {
                    markAsSent(message.key.id);
                }
            } catch (e) {
                console.error('[WA-Engine] Webhook dispatcher exception:', e);
            }
        }
    });

    // Handle Decrypted/Updated Messages (retry decryption success)
    sock.ev.on('messages.update', async (updates: any[]) => {
        for (const update of updates) {
            const messageId = update.key.id;
            if (!messageId || hasBeenSent(messageId)) {
                continue;
            }

            const senderJid = update.key.remoteJid;
            if (!senderJid) continue;

            // Skip status updates (SW) and group messages
            if (senderJid === 'status@broadcast' || senderJid.endsWith('@g.us') || senderJid.endsWith('@broadcast')) {
                continue;
            }

            const sessionStore = stores[sessionId];
            if (!sessionStore) continue;

            const fullMsg = await sessionStore.loadMessage(senderJid, messageId);
            if (!fullMsg || !fullMsg.message) continue;

            // Unwrap ephemeral or view once messages recursively
            const content = getMessageContent(fullMsg.message);
            if (!content || typeof content !== 'object' || Object.keys(content).length === 0) {
                continue;
            }

            // Blacklist of system / protocol / reaction messages
            const systemKeys = [
                'protocolMessage',
                'senderKeyDistributionMessage',
                'reactionMessage',
                'pinInChatMessage',
                'keepInChatMessage',
                'chatInviteLinkGroupTypeMessage',
                'cancelPaymentRequestMessage',
                'paymentRequestMessage',
                'declinePaymentRequestMessage',
                'paymentInviteMessage'
            ];
            
            const hasSystemKey = Object.keys(content).some(key => systemKeys.includes(key));
            if (hasSystemKey) {
                continue;
            }

            let text = content.conversation ||
                content.extendedTextMessage?.text ||
                '';

            // Skip if text is empty and there's no media
            if (!text && !content.imageMessage && !content.documentMessage && !content.videoMessage && !content.audioMessage) {
                continue;
            }

            let messageType = 'text';
            let mediaUrl = null;

            // Handle Media Messages
            if (content.imageMessage) {
                messageType = 'image';
                text = content.imageMessage.caption || text;
                try {
                    const { downloadMediaMessage } = require('@whiskeysockets/baileys');
                    const buffer = await downloadMediaMessage(
                        fullMsg,
                        'buffer',
                        {},
                        { logger }
                    );
                    const mimetype = content.imageMessage.mimetype || 'image/jpeg';
                    mediaUrl = `data:${mimetype};base64,${buffer.toString('base64')}`;
                } catch (e) {
                    console.error('[WA-Engine] Failed to download image in update:', e);
                }
            } else if (content.documentMessage) {
                messageType = 'document';
                text = content.documentMessage.title || content.documentMessage.fileName || text;
                try {
                    const { downloadMediaMessage } = require('@whiskeysockets/baileys');
                    const buffer = await downloadMediaMessage(
                        fullMsg,
                        'buffer',
                        {},
                        { logger }
                    );
                    const mimetype = content.documentMessage.mimetype || 'application/octet-stream';
                    mediaUrl = `data:${mimetype};base64,${buffer.toString('base64')}`;
                } catch (e) {
                    console.error('[WA-Engine] Failed to download document in update:', e);
                }
            } else if (content.videoMessage) {
                messageType = 'video';
                text = content.videoMessage.caption || text;
                try {
                    const { downloadMediaMessage } = require('@whiskeysockets/baileys');
                    const buffer = await downloadMediaMessage(
                        fullMsg,
                        'buffer',
                        {},
                        { logger }
                    );
                    const mimetype = content.videoMessage.mimetype || 'video/mp4';
                    mediaUrl = `data:${mimetype};base64,${buffer.toString('base64')}`;
                } catch (e) {
                    console.error('[WA-Engine] Failed to download video in update:', e);
                }
            } else if (content.audioMessage) {
                messageType = 'audio';
                text = text || '[Audio]';
                try {
                    const { downloadMediaMessage } = require('@whiskeysockets/baileys');
                    const buffer = await downloadMediaMessage(
                        fullMsg,
                        'buffer',
                        {},
                        { logger }
                    );
                    const mimetype = content.audioMessage.mimetype || 'audio/ogg';
                    mediaUrl = `data:${mimetype};base64,${buffer.toString('base64')}`;
                } catch (e) {
                    console.error('[WA-Engine] Failed to download audio in update:', e);
                }
            }

            let senderPhone = senderJid.split('@')[0];
            let whatsappLid: string | null = senderJid.endsWith('@lid') ? senderJid.split('@')[0] : null;

            const keyAny = fullMsg.key as any;
            const altJid = keyAny.senderPn || keyAny.remoteJidAlt;
            if (altJid && typeof altJid === 'string' && altJid.endsWith('@s.whatsapp.net')) {
                senderPhone = altJid.split('@')[0];
                whatsappLid = senderJid.split('@')[0];
            } else if (whatsappLid) {
                const map = getLidMap(sessionId);
                if (map[whatsappLid]) {
                    senderPhone = map[whatsappLid];
                } else {
                    const resolved = resolveLidFromAllSessions(whatsappLid);
                    if (resolved) {
                        senderPhone = resolved;
                    }
                }
            }

            const senderName = fullMsg.pushName || 'WhatsApp Contact';

            console.log(`[WA-Engine] Decrypted/Updated message from ${senderPhone} (LID: ${whatsappLid || 'none'}) to session ${sessionId}: "${text}" (Type: ${messageType})`);

            // POST event back to Laravel App Container
            try {
                const laravelWebhookUrl = 'http://app:8000/api/whatsapp/webhook';
                const payload = {
                    sessionId,
                    senderPhone,
                    senderName,
                    messageId,
                    text,
                    messageType,
                    mediaUrl,
                    timestamp: fullMsg.messageTimestamp,
                    fromMe: fullMsg.key.fromMe || false,
                    whatsappLid
                };

                const response = await fetch(laravelWebhookUrl, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload)
                });

                if (response.ok) {
                    markAsSent(messageId);
                }
            } catch (e) {
                console.error('[WA-Engine] Webhook dispatcher update exception:', e);
            }
        }
    });

    return sock;
}

/**
 * Auto-Resume all existing sessions in persistent folder on server start
 */
function autoResumeSessions() {
    try {
        const folders = fs.readdirSync(sessionsRootDir);
        for (const folder of folders) {
            const sessionPath = path.join(sessionsRootDir, folder);
            if (fs.statSync(sessionPath).isDirectory()) {
                const credsPath = path.join(sessionPath, 'creds.json');
                if (fs.existsSync(credsPath)) {
                    console.log(`[WA-Engine] Resuming authenticated session folder: ${folder}`);
                    initSession(folder).catch(err => {
                        console.error(`[WA-Engine] Failed to auto-resume session ${folder}:`, err);
                    });
                } else {
                    // Clean up dead session folder without credentials
                    try {
                        fs.rmSync(sessionPath, { recursive: true, force: true });
                        console.log(`[WA-Engine] Cleaned up dead session folder without credentials: ${folder}`);
                    } catch (cleanErr) {}
                }
            }
        }
    } catch (e) {
        console.error('[WA-Engine] Failed to read sessions folder for auto-resume:', e);
    }
}

// REST Routes

/**
 * Create/Start a session
 */
app.post('/session/create', async (req: Request, res: Response) => {
    const { sessionId } = req.body;
    if (!sessionId) {
        return res.status(400).json({ error: 'sessionId is required' });
    }

    try {
        await initSession(sessionId);
        return res.json({ success: true, status: 'connecting' });
    } catch (err: any) {
        return res.status(500).json({ error: err.message });
    }
});

/**
 * Request a pairing code for a session
 */
app.post('/session/pairing-code/:sessionId', async (req: Request, res: Response) => {
    const { sessionId } = req.params;
    const { phone } = req.body;

    if (!phone) {
        return res.status(400).json({ error: 'phone is required' });
    }

    try {
        const sock = await initSession(sessionId);
        const cleanedPhone = phone.replace(/\D/g, '');
        console.log(`[WA-Engine] Requesting pairing code for session ${sessionId} with phone ${cleanedPhone}`);

        const code = await sock.requestPairingCode(cleanedPhone);
        qrCodes[sessionId] = code;
        sessionStates[sessionId] = { status: 'connecting' };

        return res.json({ success: true, code });
    } catch (err: any) {
        console.error('[WA-Engine] Failed to request pairing code:', err);
        return res.status(500).json({ error: err.message });
    }
});

/**
 * Check connection status / fetch QR code
 */
app.get('/session/status/:sessionId', (req: Request, res: Response) => {
    const { sessionId } = req.params;
    const qr = qrCodes[sessionId];
    const state = sessionStates[sessionId];

    if (!state) {
        return res.json({ connected: false, state: 'disconnected' });
    }

    if (state.status === 'connecting' && qr) {
        const isPairingCode = qr && !qr.startsWith('data:') && qr.length <= 12;
        return res.json({
            connected: false,
            state: 'connecting',
            qrCode: qr,
            qrType: isPairingCode ? 'code' : 'image'
        });
    }

    return res.json({
        connected: state.status === 'connected',
        state: state.status,
        phone: state.phone,
        profile_name: state.profileName,
        profile_picture: state.profilePicture
    });
});

/**
 * Disconnect a session
 */
app.post('/session/disconnect/:sessionId', async (req: Request, res: Response) => {
    const { sessionId } = req.params;
    const sock = activeSockets[sessionId];

    if (sock) {
        try {
            await sock.logout();
            delete activeSockets[sessionId];
            delete qrCodes[sessionId];
            sessionStates[sessionId] = { status: 'disconnected' };
            return res.json({ success: true });
        } catch (err: any) {
            return res.status(500).json({ error: err.message });
        }
    }

    return res.status(404).json({ error: 'Session not active' });
});

/**
 * Delete a session completely
 */
app.delete('/session/delete/:sessionId', async (req: Request, res: Response) => {
    const { sessionId } = req.params;
    const sock = activeSockets[sessionId];

    if (sock) {
        try {
            await sock.logout();
        } catch (e) { }
        delete activeSockets[sessionId];
    }

    delete qrCodes[sessionId];
    sessionStates[sessionId] = { status: 'disconnected' };

    const sessionDir = path.join(sessionsRootDir, sessionId);
    if (fs.existsSync(sessionDir)) {
        try {
            fs.rmSync(sessionDir, { recursive: true, force: true });
        } catch (e) { }
    }

    return res.json({ success: true });
});

/**
 * Outbound message api
 */
app.post('/session/send-text/:sessionId', async (req: Request, res: Response) => {
    const { sessionId } = req.params;
    const { to, text, quoted } = req.body;

    if (!to || !text) {
        return res.status(400).json({ error: 'Parameters "to" and "text" are required' });
    }

    const sock = activeSockets[sessionId];
    const state = sessionStates[sessionId];
    if (!sock || !state || state.status !== 'connected') {
        return res.status(400).json({ error: 'Session is not connected yet. Please scan the QR code first.' });
    }
    try {
        let cleanedNum = formatJid(to);
        if (cleanedNum.endsWith('@s.whatsapp.net')) {
            try {
                const results = await sock.onWhatsApp(cleanedNum);
                const result = results && results.length > 0 ? results[0] : null;
                if (result && result.exists && result.jid.endsWith('@lid')) {
                    console.log(`[WA-Engine] onWhatsApp resolved ${cleanedNum} to LID: ${result.jid}`);
                    cleanedNum = result.jid;
                    const lid = result.jid.split('@')[0];
                    const phone = to.replace(/\D/g, '');
                    const map = getLidMap(sessionId);
                    if (map[lid] !== phone) {
                        map[lid] = phone;
                        saveLidMap(sessionId, map);
                    }
                }
            } catch (err: any) {
                console.warn(`[WA-Engine] Failed to check onWhatsApp for ${cleanedNum}:`, err.message);
            }
        }
        console.log(`[WA-Engine] Sending message from session ${sessionId} to ${cleanedNum}: "${text}"` + (quoted ? ` (replying to ${quoted.id})` : ''));
        let options: any = {};
        if (quoted && quoted.id) {
            options.quoted = {
                key: {
                    remoteJid: cleanedNum,
                    fromMe: quoted.fromMe ?? false,
                    id: quoted.id,
                    participant: quoted.participant || undefined
                },
                message: {
                    conversation: quoted.text || ''
                }
            };
        }
        const sentMsg = await sock.sendMessage(cleanedNum, { text: text }, options);
        try {
            await sock.sendPresenceUpdate('paused', cleanedNum);
        } catch (presErr) {}
        console.log(`[WA-Engine] Message sent successfully. returned messageId: ${sentMsg?.key?.id}`);
        return res.json({ success: true, messageId: sentMsg?.key?.id });
    } catch (err: any) {
        console.error(`[WA-Engine] Error sending message: ${err.message}`, err);
        return res.status(500).json({ error: err.message });
    }
});

/**
 * Send read receipt (mark as read / blue checkmark)
 */
app.post('/session/mark-read/:sessionId', async (req: Request, res: Response) => {
    const { sessionId } = req.params;
    const { to, messageId } = req.body;

    if (!to || !messageId) {
        return res.status(400).json({ error: 'Parameters "to" and "messageId" are required' });
    }

    const sock = activeSockets[sessionId];
    if (!sock) {
        return res.status(400).json({ error: 'Session is not active' });
    }

    try {
        const cleanedNum = formatJid(to);

        await sock.readMessages([{
            remoteJid: cleanedNum,
            id: messageId,
            fromMe: false
        }]);

        return res.json({ success: true });
    } catch (err: any) {
        console.error(`[WA-Engine] Error marking message read: ${err.message}`);
        return res.status(500).json({ error: err.message });
    }
});

/**
 * Send presence update (typing status)
 */
app.post('/session/presence/:sessionId', async (req: Request, res: Response) => {
    const { sessionId } = req.params;
    const { to, presence } = req.body; // presence: 'composing' | 'recording' | 'paused'

    if (!to || !presence) {
        return res.status(400).json({ error: 'Parameters "to" and "presence" are required' });
    }

    const sock = activeSockets[sessionId];
    if (!sock) {
        return res.status(400).json({ error: 'Session is not active' });
    }

    try {
        const cleanedNum = formatJid(to);

        await sock.sendPresenceUpdate(presence, cleanedNum);
        return res.json({ success: true });
    } catch (err: any) {
        console.error(`[WA-Engine] Error updating presence: ${err.message}`);
        return res.status(500).json({ error: err.message });
    }
});

/**
 * Outbound media api
 */
app.post('/session/send-media/:sessionId', async (req: Request, res: Response) => {
    const { sessionId } = req.params;
    const { to, url, type, mimeType, fileName, caption, quoted } = req.body;

    const logPath = path.join(__dirname, '../sessions/engine.log');
    const logToFile = (msg: string) => {
        try {
            fs.appendFileSync(logPath, `[${new Date().toISOString()}] [session: ${sessionId}] ${msg}\n`);
        } catch (e) {}
    };

    logToFile(`Received send-media request. to: ${to}, type: ${type}, url: ${url}`);

    if (!to || !url || !type) {
        logToFile(`Error: Parameters missing`);
        return res.status(400).json({ error: 'Parameters "to", "url" and "type" are required' });
    }

    const sock = activeSockets[sessionId];
    const state = sessionStates[sessionId];
    if (!sock || !state || state.status !== 'connected') {
        logToFile(`Error: Session not connected`);
        return res.status(400).json({ error: 'Session is not connected yet.' });
    }

    try {
        let cleanedNum = formatJid(to);
        if (cleanedNum.endsWith('@s.whatsapp.net')) {
            try {
                const results = await sock.onWhatsApp(cleanedNum);
                const result = results && results.length > 0 ? results[0] : null;
                if (result && result.exists && result.jid.endsWith('@lid')) {
                    logToFile(`onWhatsApp resolved ${cleanedNum} to LID: ${result.jid}`);
                    cleanedNum = result.jid;
                    const lid = result.jid.split('@')[0];
                    const phone = to.replace(/\D/g, '');
                    const map = getLidMap(sessionId);
                    if (map[lid] !== phone) {
                        map[lid] = phone;
                        saveLidMap(sessionId, map);
                    }
                }
            } catch (err: any) {
                logToFile(`Failed to check onWhatsApp for ${cleanedNum}: ${err.message}`);
            }
        }

        logToFile(`Resolved destination to: ${cleanedNum}`);
        
        let mediaSource: any;
        if (url.startsWith('data:') || (!url.startsWith('http') && !url.startsWith('/'))) {
            const base64Data = url.startsWith('data:') ? url.split(',')[1] : url;
            mediaSource = Buffer.from(base64Data, 'base64');
            logToFile(`Detected base64 URL. Converted to Buffer. Length: ${mediaSource.length}`);
        } else {
            mediaSource = { url: url };
            logToFile(`Using URL source: ${url}`);
        }

        let messagePayload: any = {};
        if (type === 'image') {
            messagePayload = { image: mediaSource, caption: caption || undefined };
        } else if (type === 'video') {
            messagePayload = { video: mediaSource, caption: caption || undefined };
        } else if (type === 'audio') {
            // Determine effective mime type — strip extra params for matching
            const baseMime = (mimeType || 'audio/webm').split(';')[0].trim().toLowerCase();
            let audioBuffer: Buffer = mediaSource instanceof Buffer ? mediaSource : Buffer.from(mediaSource);

            // WhatsApp PTT requires OGG/Opus. Convert WebM/other formats using ffmpeg.
            const needsConversion = baseMime !== 'audio/ogg' && baseMime !== 'audio/oga';
            if (needsConversion) {
                logToFile(`Converting audio from ${baseMime} to OGG/Opus for WhatsApp PTT...`);
                audioBuffer = await convertToOgg(audioBuffer, logToFile);
            }

            messagePayload = {
                audio: audioBuffer,
                mimetype: 'audio/ogg; codecs=opus',
                ptt: true
            };
            logToFile(`Audio send: baseMime=${baseMime}, needsConversion=${needsConversion}, ptt=true`);
        } else {
            // Document
            messagePayload = { 
                document: mediaSource, 
                mimetype: mimeType || 'application/octet-stream',
                fileName: fileName || 'file',
                caption: caption || undefined
            };
        }

        logToFile(`Constructed messagePayload: ${JSON.stringify(messagePayload)}`);
        logToFile(`Calling sock.sendMessage...`);

        let options: any = {};
        if (quoted && quoted.id) {
            options.quoted = {
                key: {
                    remoteJid: cleanedNum,
                    fromMe: quoted.fromMe ?? false,
                    id: quoted.id,
                    participant: quoted.participant || undefined
                },
                message: {
                    conversation: quoted.text || ''
                }
            };
        }

        const sentMsg = await sock.sendMessage(cleanedNum, messagePayload, options);
        try {
            await sock.sendPresenceUpdate('paused', cleanedNum);
        } catch (presErr) {}
        
        logToFile(`sock.sendMessage resolved. messageId: ${sentMsg?.key?.id}`);
        return res.json({ success: true, messageId: sentMsg?.key?.id });
    } catch (err: any) {
        logToFile(`Exception during sendMedia: ${err.message}`);
        if (err.stack) {
            logToFile(`Stack: ${err.stack}`);
        }
        return res.status(500).json({ error: err.message });
    }
});

/**
 * Health Check
 */
app.get('/', (req: Request, res: Response) => {
    res.json({
        status: 'ok',
        service: 'NexaBot WhatsApp Engine',
        version: '1.0.0'
    });
});

// Start Express
app.listen(port, () => {
    console.log(`[WA-Engine] NexaBot WhatsApp microservice listening on port ${port}`);
    // Auto-Resume previously authenticated logins
    autoResumeSessions();
});
