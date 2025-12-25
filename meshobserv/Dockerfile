FROM golang:1.24-trixie AS build
WORKDIR /build

COPY . .
# Generate Meshtastic protobuf code
RUN apt-get update && \
    apt-get install -y --no-install-recommends libprotobuf-dev protobuf-compiler git ca-certificates build-essential && \
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest && \
    git clone --depth 1 https://github.com/meshtastic/protobufs.git /tmp/protobufs && \
    protoc \
      -I=/usr/local/include \
      -I=/usr/include \
      -I=/tmp/protobufs \
      --go_out=/build/internal/meshtastic \
      --go_opt=module=github.com/meshtastic/go \
      /tmp/protobufs/nanopb.proto \
      /tmp/protobufs/meshtastic/*.proto
# Download dependencies
RUN go mod download
# Build the meshobserv binary (CGO enabled for duckdb)
RUN CGO_ENABLED=1 GOOS=linux go build -v -trimpath -ldflags="-s -w" -o meshobserv ./cmd/meshobserv

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libstdc++6 && rm -rf /var/lib/apt/lists/*
WORKDIR /
COPY --from=build /build/meshobserv /meshobserv
ENTRYPOINT ["/meshobserv"]
