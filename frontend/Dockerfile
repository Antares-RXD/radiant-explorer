# Builder stage
FROM node:25-alpine AS builder
WORKDIR /app

# Install dependencies
COPY package.json package-lock.json* ./
# Generate Prisma Client (requires schema)
COPY prisma ./prisma
RUN npm install
RUN npx prisma generate

# Build application
COPY . .
ENV NEXT_TELEMETRY_DISABLED=1
RUN npm run build

# Runner stage
FROM node:25-alpine AS runner
WORKDIR /app

ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1

# Install PM2 globally
RUN npm install -g pm2

RUN addgroup --system --gid 1001 nodejs
RUN adduser --system --uid 1001 nextjs

# Copy standalone build
COPY --from=builder /app/public ./public
COPY ecosystem.config.js ./
RUN mkdir .next
RUN chown nextjs:nodejs .next

# Copy output
COPY --from=builder --chown=nextjs:nodejs /app/.next/standalone/app ./
COPY --from=builder --chown=nextjs:nodejs /app/.next/static ./.next/static

USER nextjs

EXPOSE 3000
ENV PORT=3000
ENV HOSTNAME="0.0.0.0"
CMD ["pm2-runtime", "start", "ecosystem.config.js"]
