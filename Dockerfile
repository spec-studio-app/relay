# Studio relay: a single-user remote MCP server for Spec Studio.
FROM node:22-alpine AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY tsconfig.json ./
COPY src ./src
COPY test ./test
RUN npm run build

FROM node:22-alpine AS deps
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev

FROM node:22-alpine
ENV NODE_ENV=production \
    PORT=8760
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY --from=build /app/dist/src ./dist/src
COPY package.json ./
# The node user (uid 1000), numeric so that runAsNonRoot can check it.
USER 1000:1000
EXPOSE 8760
HEALTHCHECK --interval=30s --timeout=3s CMD wget -qO- http://127.0.0.1:8760/healthz >/dev/null || exit 1
CMD ["node", "dist/src/main.js"]
