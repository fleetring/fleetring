# syntax=docker/dockerfile:1

# The node image for the fault-injection harness (FR-065, ADR-0036). Not a release image: it
# runs as the harness's user, with iptables and libfaketime for faults.
#
#   docker build -f deploy/chaos/Dockerfile -t fleetring-chaos:local .

# Keep equal to RUST_VERSION in the root Dockerfile and .github/workflows/release.yml.
ARG RUST_VERSION=1.99

FROM rust:${RUST_VERSION}-slim-bookworm AS build
WORKDIR /fleetring
COPY . .
ENV CARGO_INCREMENTAL=0
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/fleetring/target,sharing=locked \
    cargo build --locked --release -p fleetringd \
    && install -D -m 0755 target/release/fleetring /out/fleetring

# glibc, dynamically linked, so LD_PRELOAD can load libfaketime.
FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends iptables libfaketime procps ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(find /usr/lib -name libfaketime.so.1 -print -quit)" /usr/local/lib/libfaketime.so.1 \
    && test -e /usr/local/lib/libfaketime.so.1
COPY --from=build /out/fleetring /usr/local/bin/fleetring
# `fleetringd` is the binary's name until 1.0 (ADR-0043, point 1).
RUN ln -s fleetring /usr/local/bin/fleetringd
EXPOSE 7400
ENTRYPOINT ["/usr/local/bin/fleetring"]
CMD ["serve", "--config", "/etc/fleetring/fleetringd.toml"]
