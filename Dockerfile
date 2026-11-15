# syntax=docker/dockerfile:1
# Sluice: engine + web UI + load generator in one image.

FROM node:24-bookworm-slim AS ui
WORKDIR /ui
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY frontend/ ./
RUN npm run build

FROM rust:1-bookworm AS engine
# clang for RocksDB's bindings, protoc for the gRPC service.
RUN apt-get update \
    && apt-get install -y --no-install-recommends clang libclang-dev protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cargo build --release --bin sluice --bin sluice-loadgen

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libstdc++6 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=engine /src/target/release/sluice /src/target/release/sluice-loadgen /usr/local/bin/
COPY --from=ui /ui/dist /opt/sluice/ui
COPY examples /opt/sluice/examples
ENV SLUICE_UI_DIR=/opt/sluice/ui \
    SLUICE_DATA_DIR=/data \
    SLUICE_HTTP_ADDR=0.0.0.0:8080 \
    SLUICE_GRPC_ADDR=0.0.0.0:50051 \
    RUST_LOG=info
EXPOSE 8080 50051
VOLUME /data
CMD ["sluice"]
