# Build stage
FROM golang:1.21-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum* ./
RUN go mod download

# Copy source code
COPY . .

# Build
ARG VERSION=dev
ARG BUILD_TIME
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags "-X github.com/sofon/data-pipeline-service/internal/service.Version=${VERSION} -X github.com/sofon/data-pipeline-service/internal/service.BuildTime=${BUILD_TIME}" \
    -o /app/server ./cmd/server

# Final stage
FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

# Copy binary
COPY --from=builder /app/server /app/server

# Create config directory
RUN mkdir -p /app/config

# Copy default config
COPY configs/config.yaml /app/config/config.yaml

# Non-root user
RUN adduser -D -u 1000 appuser && \
    chown -R appuser:appuser /app
USER appuser

EXPOSE 8080

ENTRYPOINT ["/app/server"]
CMD ["-config", "/app/config/config.yaml"]
