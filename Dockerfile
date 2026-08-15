# Multi-stage Dockerfile for Rust WebSocket Chat
# Stage 1: Build the Rust binary
FROM rust:1.86-slim AS builder

WORKDIR /app

# Copy manifest first for better layer caching
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && \
    cargo build --release && \
    rm -rf src

# Copy the actual source and build
COPY src/ src/
COPY static/ static/
RUN touch src/main.rs && cargo build --release

# Stage 2: Minimal runtime image
FROM debian:bookworm-slim

WORKDIR /app

# Copy the binary and static files
COPY --from=builder /app/target/release/rust-websocket-chat ./
COPY static/ ./static/

# Fly.io sets PORT env; our app reads it in main.rs
ENV PORT=8080
EXPOSE 8080

CMD ["./rust-websocket-chat"]
