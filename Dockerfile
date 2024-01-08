FROM rust:alpine as builder
WORKDIR /app
COPY . .
RUN cargo install --path .

FROM alpine
COPY --from=builder /usr/local/cargo/bin/price-collector /usr/local/bin/price-collector
CMD ["price-collector"]