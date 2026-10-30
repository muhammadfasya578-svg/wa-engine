FROM node:20-alpine

WORKDIR /usr/src/app

RUN apk add --no-cache git ffmpeg

COPY package*.json ./
RUN npm install

COPY . .

RUN npm run build

EXPOSE 5000

ENV PORT=5000

CMD ["node", "dist/index.js"]
