# Shopping Sessions: one statically linked Rust binary in an empty image.
# The scorer builds this natively for linux/amd64 and runs CMD as uid 1000
# with a read-only root, no network, /data read-only and /output writable.
FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY tools/generator ./tools/generator
# Scorers are Intel Xeon, Cascade Lake or newer: build for Cascade Lake (AVX-512).
# Measured 23-28% faster than x86-64-v3 on CI, identical output.
ENV BASE_FLAGS="-C target-cpu=cascadelake -C target-feature=+crt-static"
ENV T=x86_64-unknown-linux-gnu
# Profile-guided build (measured 2.5-2.8% faster, identical output): train on
# small datasets from the official generator (seeds unrelated to any scoring
# data), then rebuild with the profile. If any training step fails, fall back
# to the plain build, so the image always builds.
RUN set -e; \
    if ( rustup component add llvm-tools \
         && cargo build --release --locked --manifest-path tools/generator/Cargo.toml \
         && tools/generator/target/release/generate --seed 7 --events 4000000 --clicks 1000000 --output /tmp/tr7 \
         && tools/generator/target/release/generate --seed 8 --events 1000000 --clicks 250000 --output /tmp/tr8 \
         && RUSTFLAGS="$BASE_FLAGS -Cprofile-generate=/tmp/pgo" cargo build --release --locked --target $T --target-dir /tmp/gen \
         && mkdir -p /tmp/o7 /tmp/o8 \
         && DATA_DIR=/tmp/tr7 OUTPUT_DIR=/tmp/o7 NCPU=8 /tmp/gen/$T/release/shopping \
         && DATA_DIR=/tmp/tr8 OUTPUT_DIR=/tmp/o8 NCPU=8 /tmp/gen/$T/release/shopping \
         && "$(find "$(rustc --print sysroot)" -name llvm-profdata | head -1)" merge -o /tmp/pgo.profdata /tmp/pgo ); \
    then echo "PGO build"; RUSTFLAGS="$BASE_FLAGS -Cprofile-use=/tmp/pgo.profdata" cargo build --release --locked --target $T; \
    else echo "plain build"; RUSTFLAGS="$BASE_FLAGS" cargo build --release --locked --target $T; \
    fi; \
    strip target/$T/release/shopping && cp target/$T/release/shopping /shopping

FROM scratch
COPY --from=build /shopping /shopping
ENV DATA_DIR=/data OUTPUT_DIR=/output NCPU=8 MIMALLOC_ALLOW_LARGE_OS_PAGES=1
USER 1000:1000
CMD ["/shopping"]
