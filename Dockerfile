# syntax=docker/dockerfile:1

# The fleetringd container image (FR-034).
#
#   docker build -t fleetringd .
#   docker run -d -p 7400:7400 -v fleetring-data:/data fleetringd
#
# SQLite is compiled into the binary, so the runtime needs only glibc and libgcc_s: a distroless
# image with no shell or package manager. The daemon runs as UID 65532 and keeps its databases in
# the /data volume.

# Keep equal to RUST_VERSION in .github/workflows/release.yml, so images and archives build with
# the same compiler.
ARG RUST_VERSION=1.99

FROM rust:${RUST_VERSION}-slim-bookworm AS build
WORKDIR /fleetring
COPY . .
ENV CARGO_INCREMENTAL=0 \
    RUSTFLAGS="--remap-path-prefix=/usr/local/cargo=/cargo"
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/fleetring/target,sharing=locked \
    cargo build --locked --release -p fleetringd \
    && install -D -m 0755 target/release/fleetringd /out/fleetringd \
    && install -d -m 0750 -o 65532 -g 65532 /out/data

# Debian 12, the glibc the build stage links against.
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /out/fleetringd /usr/local/bin/fleetringd
COPY --from=build --chown=65532:65532 /out/data /data
COPY deploy/fleetringd.toml /etc/fleetring/fleetringd.toml
USER 65532:65532
VOLUME ["/data"]
EXPOSE 7400
ENTRYPOINT ["/usr/local/bin/fleetringd"]
CMD ["--config", "/etc/fleetring/fleetringd.toml"]
