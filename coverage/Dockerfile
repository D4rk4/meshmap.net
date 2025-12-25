FROM golang:1.24 AS build
WORKDIR /build

COPY . .
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential && \
    go mod download
RUN CGO_ENABLED=1 GOOS=linux go build -v -trimpath -ldflags="-s -w" -o coverage ./cmd/coverage

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libstdc++6 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /build/coverage /usr/local/bin/coverage
ENTRYPOINT ["/usr/local/bin/coverage"]
