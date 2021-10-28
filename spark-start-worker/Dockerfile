FROM duskmoon/cargo-chef:nightly as chef
WORKDIR /app

FROM chef as planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef as builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

COPY . .
RUN cargo build --release --bin spark-start-worker

FROM gcr.io/distroless/cc
COPY --from=builder /app/target/release/spark-start-worker /spark-start-worker
ENTRYPOINT ["/spark-start-worker"]
