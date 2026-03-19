FROM node:20-slim
WORKDIR /app
COPY package*.json tsconfig.json ./
RUN npm install
COPY system_a/ ./system_a/
RUN npm run build
CMD ["node", "dist/app.js"]