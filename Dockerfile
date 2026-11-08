# syntax=docker/dockerfile:1

# The fleetring container image (FR-034).
#
#   docker build -t fleetring .
#   docker run -d -p 7400:7400 -v fleetring-data:/data fleetring
#   docker run --rm fleetring db list --control 10.0.0.1:7400
#
# The image runs `fleetring serve --config /etc/fleetring/fleetringd.toml` unless given other
# arguments. `/usr/local/bin/fleetringd` is a link to `fleetring`, its name until 1.0, and a
# command of `--config <file>`, without `serve`, still runs a node (ADR-0043, point 1.8).
# `/usr/local/bin/fleet` is a link to `fleetring` too, its short name; nothing else on the image
# is called `fleet`.
#
# SQLite is compiled into the binary, so the runtime needs only glibc and libgcc_s: a distroless
# image with no shell or package manager. The node runs as UID 65532 and keeps its databases in
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
    && install -D -m 0755 target/release/fleetring /out/bin/fleetring \
    && ln -s fleetring /out/bin/fleetringd \
    && ln -s fleetring /out/bin/fleet \
    && install -d -m 0750 -o 65532 -g 65532 /out/data

# Debian 12, the glibc the build stage links against.
FROM gcr.io/distroless/cc-debian12:nonroot
# A directory copy, which keeps the `fleetringd` and `fleet` links as links.
COPY --from=build /out/bin/ /usr/local/bin/
COPY --from=build --chown=65532:65532 /out/data /data
COPY deploy/fleetringd.toml /etc/fleetring/fleetringd.toml
USER 65532:65532
VOLUME ["/data"]
EXPOSE 7400
ENTRYPOINT ["/usr/local/bin/fleetring"]
CMD ["serve", "--config", "/etc/fleetring/fleetringd.toml"]
