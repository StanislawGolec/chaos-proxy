# Go-Chaos-Proxy

[![Go Version](https://img.shields.io/badge/Go-1.23%2B-blue.svg)](https://go.dev/)
[![Control Plane](https://img.shields.io/badge/Control%20Plane-gRPC%20%2F%20Protobuf-00a4d3.svg)](https://grpc.io/)
[![Transport](https://img.shields.io/badge/Transport-TCP%20%26%20WebSockets-green.svg)]()
[![License](https://img.shields.io/badge/License-MIT-lightgrey.svg)](LICENSE)

An L4/L7 network proxy and fault injection engine written in Go. Uses a separated data and control plane architecture:
- **Data Plane:** Transparent TCP and WebSocket stream proxying with pooled buffers.
- **Control Plane:** gRPC service managed dynamically via the `chaosctl` CLI.

---

## Features

- **Latency and Jitter:** Configurable packet delays with normal distribution.
- **Packet Loss:** Discard frames at configurable rates to test socket timeouts.
- **Connection Tearing:** Force socket closure via TCP RST resets.
- **Payload Mutation:** In-flight frame bit flipping for parser resilience testing.
- **Metrics:** Built-in Prometheus `/metrics` endpoint reporting traffic volume, dropped packets, and latency histograms.

---

## Getting Started

### Prerequisites
- Go 1.23 or newer
- `protoc` (optional, for regenerating protobuf stubs)

### Building and Running
```bash
git clone https://github.com/StanislawGolec/Go-Chaos-Proxy.git
cd Go-Chaos-Proxy
go build -o bin/proxy ./cmd/proxy
go build -o bin/chaosctl ./cmd/chaosctl

# Start proxy forwarding port 8090 to target server 8080
./bin/proxy --listen :8090 --target localhost:8080 --grpc :9090
```
