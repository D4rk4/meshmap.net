FROM golang:1.24 AS build
WORKDIR /build

COPY . .
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -v -trimpath -ldflags="-s -w" -o coverage ./main.go

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /build/coverage /usr/local/bin/coverage
ENTRYPOINT ["/usr/local/bin/coverage"]
