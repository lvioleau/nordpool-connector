# Multi-stage build for the Nord Pool connector binary.
#
# protoc is needed by the three build.rs protobuf codegen steps. TLS for the WebSocket,
# OAuth (ureq), and S3 calls is rustls with bundled webpki roots, but the S3 credential
# providers (container endpoint) and any system-cert path still want ca-certificates
# present at runtime.

FROM rust:1-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        protobuf-compiler \
        libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*

ENV PROTOC=/usr/bin/protoc

WORKDIR /app
COPY . .
# The build.rs files pass only `proto/` as the protoc include root (the nix protoc used in
# dev bundles its own well-known types). Stage the system google/protobuf/*.proto imports
# into that root so `import "google/protobuf/wrappers.proto"` resolves under the container's
# Debian protoc too.
RUN cp -r /usr/include/google proto/google
# Cargo profile to build. Defaults to `release` (prod/CI); the dev launcher passes
# `release-debug` for DWARF-rich stack dumps. The target subdir matches the profile name.
ARG CARGO_PROFILE=release
RUN cargo build --profile ${CARGO_PROFILE} -p nordpool-connector --bin nordpool-connector

FROM debian:bookworm-slim AS runtime

# Read back by the config's ${ARCHIVE_DIR} at startup, so the staging area is declared once.
# A runtime override only works if that path is writable by uid 10001 (i.e. a mounted volume);
# this mkdir has already run by then.
ENV ARCHIVE_DIR=/data/archive

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -r -u 10001 -m -d /app appuser \
    # Container-local archive staging area; deployments that want persistence mount a volume over it.
    && mkdir -p "$ARCHIVE_DIR" \
    && chown appuser:appuser "$ARCHIVE_DIR"

WORKDIR /app
ARG CARGO_PROFILE=release
COPY --from=builder /app/target/${CARGO_PROFILE}/nordpool-connector /usr/local/bin/nordpool-connector
# Config resolves relative to cwd (or NORDPOOL_CONFIG). Ship the template; ${VAR}
# references are filled from the environment, then the AWS secret bundle at startup.
COPY nordpool.ini /app/nordpool.ini

USER appuser
# Absolute path so argv[0] (the host-visible cmdline, read by the dd-agent process
# check's `/nordpool-connector` search_string) carries the leading slash.
ENTRYPOINT ["/usr/local/bin/nordpool-connector"]
