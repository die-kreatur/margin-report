FROM rust:1.88-slim

RUN apt update && apt install -y pkg-config libssl-dev

WORKDIR /app

RUN cargo init

COPY ./Cargo.toml /app/Cargo.toml
COPY ./Cargo.lock /app/Cargo.lock

RUN cargo fetch

RUN cargo build --release

COPY ./src ./src

RUN touch ./src/main.rs && cargo build --release

CMD ["./target/release/report"]
