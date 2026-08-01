FROM node:20-alpine

# Create non-root user (Pod Security Standards: restricted)
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /app

# Copy package files and install deps
COPY package*.json ./
RUN npm install --omit=dev

# Copy application source
COPY server.js tracing.js ./
COPY public ./public/

# Run as non-root
USER appuser

EXPOSE 3000

# OTel tracing loaded via -r flag in package.json "start" script
CMD ["node", "-r", "./tracing.js", "server.js"]
