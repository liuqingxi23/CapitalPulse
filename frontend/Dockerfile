FROM oven/bun:1 AS builder

WORKDIR /app

COPY package.json ./
RUN bun install

COPY . .

# Build the Next.js application
RUN bun run build

# ── Production image ──────────────────────────────────────────────────────────
FROM oven/bun:1-slim AS runner

WORKDIR /app

ENV NODE_ENV=production

# Copy standalone output
COPY --from=builder /app/.next/standalone ./
COPY --from=builder /app/.next/static ./.next/static
COPY --from=builder /app/public ./public

EXPOSE 3000

CMD ["bun", "server.js"]
