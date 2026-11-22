# Shopping Sessions: one statically linked Rust binary in an empty image.
# The scorer builds this natively for linux/amd64 and runs CMD as uid 1000
# with a read-only root, no network, /data read-only and /output writable.
FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
ENV RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static"
RUN cargo build --release --locked --target x86_64-unknown-linux-gnu \
 && strip target/x86_64-unknown-linux-gnu/release/shopping \
 && cp target/x86_64-unknown-linux-gnu/release/shopping /shopping

FROM scratch
COPY --from=build /shopping /shopping
ENV DATA_DIR=/data OUTPUT_DIR=/output NCPU=8 MIMALLOC_ALLOW_LARGE_OS_PAGES=1
USER 1000:1000
CMD ["/shopping"]
