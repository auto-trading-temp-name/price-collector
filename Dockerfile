FROM rust as builder
WORKDIR /app
COPY . .
RUN rustup target add x86_64-unknown-linux-musl
RUN cargo install --path . --target=x86_64-unknown-linux-musl

FROM alpine
COPY --from=builder /usr/local/cargo/bin/price-collector /usr/local/bin/price-collector
CMD ["price-collector"]