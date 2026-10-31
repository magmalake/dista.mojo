# The Rust image for bench/rust: the release binary on the same
# debian:bookworm-slim base dista's images use, so the sizes compare like
# for like.
#
#     docker build -t dista-rust:taxi bench/rust
FROM rust:1.94-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
# The dependencies first, against a stub crate, so the build's two costs are
# separate steps: bench/cloud/build.sh times them apart.
RUN mkdir src && echo 'fn main() {}' > src/main.rs && cargo build --release --locked
COPY src src
RUN touch src/main.rs && cargo build --release --locked

FROM debian:bookworm-slim
# rustls verifies S3's certificate against the system's roots, which slim
# images leave out.
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/taxi-rs /usr/local/bin/taxi-rs
ENTRYPOINT ["/usr/local/bin/taxi-rs"]
