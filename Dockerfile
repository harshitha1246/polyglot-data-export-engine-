# multistage build for a Node.js export engine

FROM node:18-alpine as build
WORKDIR /usr/src/app
COPY package*.json ./
# npm ci requires a lockfile; using npm install for flexibility
RUN npm install --production
COPY . .

FROM node:18-alpine as runtime
WORKDIR /usr/src/app
COPY --from=build /usr/src/app .
ENV NODE_ENV=production
EXPOSE 8080
CMD ["node", "src/index.js"]
