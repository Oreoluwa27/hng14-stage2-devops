FROM node:24-alpine 
WORKDIR /app
COPY package*.json ./
RUN npm ci --only=production
COPY app.js ./
COPY views ./views

RUN addgroup -g 1001 appgroup && \
    adduser -D -u 1001 -G appgroup appuser

USER appuser
EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:3000 || exit 1

CMD ["node", "app.js"]