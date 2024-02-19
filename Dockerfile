FROM rust:1.75-bookworm as builder
WORKDIR /app

# Cache downloaded+built dependencies
COPY Cargo.toml Cargo.lock ./
RUN \
    mkdir /app/src && \
    echo 'fn main() {}' > /app/src/main.rs && \
    cargo build --release && \
    rm -Rvf /app/src

COPY src src
RUN \
    touch src/main.rs && \
    cargo install --path .

FROM debian:bookworm-slim
RUN apt-get update && apt-get install ca-certificates -y
WORKDIR /app
COPY --from=builder /usr/local/cargo/bin/price-collector price-collector
COPY .env .
COPY coins.json .
CMD ["/app/price-collector"]
