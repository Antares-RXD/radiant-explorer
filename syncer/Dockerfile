# Build stage
FROM rust:1.92-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache musl-dev pkgconfig openssl-dev openssl-libs-static

# Copy manifest files first for dependency caching
COPY Cargo.toml Cargo.lock* ./

# Create dummy source to build dependencies
RUN mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    cargo build --release && \
    rm -rf src

# Copy schema and actual source code
COPY schema.sql ./schema.sql
COPY src ./src

# Build the actual application
RUN touch src/main.rs && cargo build --release

# Runtime stage
FROM alpine:3.21

RUN apk add --no-cache ca-certificates

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/target/release/neurai-syncer /usr/local/bin/

# Copy config file
COPY config.json ./

# Run as non-root user
RUN adduser -D -u 1000 syncer
USER syncer

CMD ["neurai-syncer"]
