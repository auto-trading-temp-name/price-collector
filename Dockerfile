FROM rust:1.75 as builder
WORKDIR /usr/src/price-collector
COPY . .
RUN cargo install --path .


FROM debian:bullseye-slim
COPY --from=builder /usr/local/cargo/bin/price-collector /usr/local/bin/price-collector
CMD ["price-collector"]