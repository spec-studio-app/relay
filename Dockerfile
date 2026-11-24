# Spec Studio relay: a self-hosted remote MCP server with a Temporal worker.
# Multi-arch (linux/amd64, linux/arm64). The Temporal SDK needs glibc, so the
# image uses Debian slim, not Alpine.
FROM node:22-bookworm-slim AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY tsconfig.json ./
COPY src ./src
COPY test ./test
RUN npm run build

FROM node:22-bookworm-slim AS deps
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force

FROM node:22-bookworm-slim
ENV NODE_ENV=production \
    PORT=8760
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY --from=build /app/dist/src ./dist/src
COPY package.json ./
RUN ln -s /app/dist/src/cli.js /usr/local/bin/relay && chmod +x /app/dist/src/cli.js
# The node user (uid 1000), numeric so that runAsNonRoot can check it.
USER 1000:1000
EXPOSE 8760
HEALTHCHECK --interval=30s --timeout=3s --start-period=30s \
  CMD node -e "fetch('http://127.0.0.1:'+(process.env.PORT||8760)+'/healthz').then(r=>process.exit(r.ok?0:1),()=>process.exit(1))"
# `docker run <image>` runs the relay; `docker run <image> keys` or `pair-code` runs a command.
ENTRYPOINT ["node", "dist/src/cli.js"]
CMD ["serve"]
