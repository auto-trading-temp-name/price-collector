FROM rust:1.75-bullseye as builder
WORKDIR /usr/src/price-collector
COPY . .
RUN cargo install --path .

FROM debian:bullseye-slim
RUN apt-get update && apt-get install ca-certificates -y
COPY --from=builder /usr/local/cargo/bin/price-collector /usr/local/bin/price-collector
COPY --from=builder /usr/src/price-collector/.env .
COPY --from=builder /usr/src/price-collector/coins.json .
CMD ["price-collector"]
