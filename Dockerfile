# ==========================================
# Stage 1: Builder
# ==========================================
FROM golang:1.23-alpine AS builder

WORKDIR /build

# Install security certificates
RUN apk add --no-cache ca-certificates git

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source tree
COPY . .

# Build statically-linked, stripped binaries
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w -extldflags '-static'" \
    -o /build/bin/proxy ./cmd/proxy

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w -extldflags '-static'" \
    -o /build/bin/chaosctl ./cmd/chaosctl

# ==========================================
# Stage 2: Minimal Distroless Runtime (<15MB)
# ==========================================
FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app

# Copy compiled binaries from builder
COPY --from=builder /build/bin/proxy /app/proxy
COPY --from=builder /build/bin/chaosctl /app/chaosctl

# Run as non-root user (UID 65532)
USER nonroot:nonroot

# Expose Data Plane, Control Plane, and Observability ports
EXPOSE 8090 9090 9091

# Default entrypoint and flags
ENTRYPOINT ["/app/proxy"]
CMD ["--listen", ":8090", "--target", "localhost:8080", "--grpc", ":9090", "--metrics", ":9091"]
