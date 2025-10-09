FROM lukemathwalker/cargo-chef:latest-rust-1 AS chef
WORKDIR /app

# Step 1: Compute a recipe file
FROM chef as planner
WORKDIR /app
COPY src ./src
COPY Cargo.* .
RUN cargo chef prepare --recipe-path recipe.json

# Step 2.1: Cache project dependencies
FROM chef as builder
WORKDIR /app
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# Step 2.2: Build the binary
# NOTE: Feature flags may need to be enabled.
COPY src ./src
COPY Cargo.* .
RUN cargo build --release --bin deltactl

# Step 4: Copy the binary to a runtime image
FROM linuxcontainers/debian-slim:12 as runtime
WORKDIR /app
COPY --from=builder /app/target/release/deltactl /usr/local/bin
ENTRYPOINT ["/usr/local/bin/deltactl"]
