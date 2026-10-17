# Go-Chaos-Proxy: High-Throughput L4/L7 Fault Injection Engine

[![Go Version](https://img.shields.io/badge/Go-1.23%2B-blue.svg)](https://go.dev/)
[![Tests](https://img.shields.io/badge/Tests-100%25%20Passing-brightgreen.svg)](https://github.com/StanislawGolec/Go-Chaos-Proxy)
[![Throughput](https://img.shields.io/badge/Throughput-2.1%20GB%2Fs-blueviolet.svg)](#empirical-benchmark-performance)
[![Hot Path](https://img.shields.io/badge/Hot%20Path-Zero%20Allocation-success.svg)](#zero-allocation-data-plane)
[![Control Plane](https://img.shields.io/badge/Control%20Plane-gRPC%20%2F%20Protobuf-00a4d3.svg)](https://grpc.io/)
[![Transport](https://img.shields.io/badge/Transport-TCP%20%26%20RFC%206455-orange.svg)]()
[![License](https://img.shields.io/badge/License-MIT-lightgrey.svg)](LICENSE)

An industrial-grade L4/L7 network proxy and chaos engineering fault-injection engine written in **Go 1.23+**. Built with an **Envoy-style separated Data Plane and Control Plane architecture**:
* **Data Plane:** Zero-allocation transparent TCP and RFC 6455 WebSocket byte stream proxying via high-performance `sync.Pool` buffer recycling, sustaining **>2.1 GB/s throughput** and **sub-3 nanosecond fault evaluation**.
* **Control Plane:** Low-latency gRPC service administered dynamically via the `chaosctl` CLI for real-time fault orchestration without proxy restarts.

---

## 1. System Architecture

```mermaid
flowchart TD
    subgraph HeterogeneousClients["Distributed Clients"]
        Android["Ultimo-Mobile (Android Kotlin Compose)"]
        WebCLI["Browser / Netcat CLI"]
        PyBot["Ultimo-MARL Bot (PyTorch Policy)"]
    end

    subgraph ChaosProxy["Go-Chaos-Proxy Engine"]
        subgraph DataPlane["Data Plane (Port 8090)"]
            Listener["TCP Listener (Accept Loop)"]
            BufPool["Buffer Pool (sync.Pool 32KB)"]
            Tunnel["Bidirectional Streaming Tunnel"]
            Pipeline["Fault Injection Pipeline<br/>(Latency, Drop, Corrupt, Throttle)"]
            
            Listener --> Tunnel
            BufPool --- Tunnel
            Tunnel <--> Pipeline
        end

        subgraph ControlPlane["Control Plane (gRPC Port 9090)"]
            GrpcServer["ChaosControl gRPC Service"]
            Broadcaster["Real-time Event Streamer"]
            EngineState["Fault Engine Atomic Rules"]
            
            GrpcServer --> EngineState
            EngineState -.->|Dynamic Rules| Pipeline
            Pipeline -.->|Telemetry Events| Broadcaster
        end

        subgraph Observability["SRE Observability (Port 9091)"]
            Prometheus["Prometheus /metrics"]
            HealthProbes["/healthz & /readyz"]
            Pprof["Go Runtime pprof"]
        end
    end

    subgraph UpstreamServer["Authoritative Backend (Java 21 Loom)"]
        UltimoServer["Ultimo Server (Port 8080)"]
    end

    subgraph AdminTooling["Operator Tooling"]
        ChaosCTL["chaosctl CLI"]
    end

    Android <-->|"RFC 6455 WebSockets"| Listener
    WebCLI <-->|"WebSockets / TCP"| Listener
    PyBot <-->|"Raw TCP Socket"| Listener
    Tunnel <-->|"Multiplexed Upstream"| UltimoServer
    ChaosCTL <-->|"gRPC Management"| GrpcServer
```

---

## 2. Empirical Benchmark Performance

Benchmarked on modern x86-64 hardware running Go 1.23+ under native `testing.B` profiling:

| Benchmark Operation | Iterations | Latency / op | Throughput | Heap Allocations |
| :--- | :--- | :--- | :--- | :--- |
| **Fault Evaluation (Pass-Through)** | 543,542,264 | **2.21 ns/op** | ~452M ops/s | **0 B/op (0 allocs)** |
| **Fault Evaluation (Drop Rule)** | 82,131,588 | **13.95 ns/op** | ~71M ops/s | **0 B/op (0 allocs)** |
| **Buffer Pool Parallel Access** | 100,000,000 | **10.74 ns/op** | ~93M ops/s | **24 B/op** |
| **Streaming Throughput (32KB)** | 75,303 | 15,276 ns/op | **2,145.09 MB/s** | **8 B/op** |
| **WebSocket In-Place Unmasking (32KB)** | 648,826 | 1,811 ns/op | **18,096.96 MB/s** | **0 B/op (0 allocs)** |
| **Card Parser (`ParseCard`)** | 58,219,642 | **21.05 ns/op** | ~47M ops/s | **0 B/op (0 allocs)** |
| **WebSocket Frame Encode** | 53,672,782 | **22.13 ns/op** | ~45M ops/s | **16 B/op** |

---

## 3. Chaos Engineering Capabilities

### L4 Network Faults
* **Latency & Jitter:** Configurable base delays with uniform or Gaussian normal distribution curves.
* **Packet Loss & Burst Drops:** Probabilistic drop ratios ($0.0$ to $1.0$) with configurable consecutive burst sizes to trigger socket retransmission timeouts.
* **Abrupt Disconnection (TCP RST):** Forces instant socket termination with `SO_LINGER = 0` to simulate cellular drops and router crashes without graceful FIN handshakes.
* **Bandwidth Throttling:** Token bucket rate limiter restricting transfer rates down to dial-up speeds (e.g. 10 KB/s).

### L7 Semantic Mutation
* **Bit-Flip Fuzzing:** Probabilistic corruption flipping random bits within raw packet payloads to stress-test frame decoders.
* **Ultimo Protocol Fuzzing:** Intercepts and mutates line-delimited application tokens (e.g. replacing card identifiers or modifying action opcodes) to verify server anti-cheat validation.

---

## 4. Control Plane Management (`chaosctl` CLI)

The included `chaosctl` CLI interacts with the proxy's gRPC control plane for runtime administration:

```bash
# Inject 150ms latency with 50ms Gaussian jitter
chaosctl latency --base 150 --jitter 50 --dist normal

# Drop 20% of all client-to-server packets in bursts of 3
chaosctl drop --ratio 0.20 --burst 3 --dir C2S

# Corrupt payloads with 5% bit-flip probability
chaosctl corrupt --rate 0.05 --mode L4_RAW

# Throttle bandwidth to 64 KB/s
chaosctl throttle --rate 65536

# Sever all active connections instantly via TCP RST
chaosctl disconnect --tcp-reset

# Clear all active fault rules (revert to 100% pass-through)
chaosctl reset

# Stream real-time packet telemetry
chaosctl stream --dir ALL
```

---

## 5. Engineering Trade-Offs & Design Rationale

* **Why Data/Control Plane Split?** Separating the packet forwarding loop (raw `net.Conn`) from gRPC control ensures that administrative overhead never blocks high-speed network I/O.
* **Why `sync.Pool` Buffer Recycling?** Avoids allocating fresh 32KB byte slices on every read operation, preventing GC thrashing and memory fragmentation during high-concurrency streaming.
* **Why In-Place WebSocket Unmasking?** RFC 6455 requires masking client-to-server frames with a 4-byte XOR key. Performing unmasking directly in the shared buffer achieves **18 GB/s** memory throughput with zero allocations.

---

## 6. Observability & SRE Compliance

* **Prometheus `/metrics` (Port 9091):** Tracks transferred bytes, active tunnels, injected fault counts, and streaming latency histograms.
* **Health Probes:** Kubernetes-ready `/healthz` and `/readyz` endpoints.
* **Profiling (`pprof`):** Live CPU, heap memory, and goroutine profiling enabled under `/debug/pprof/`.

---

## 7. Quick Start

### Build Binaries
```bash
go build -o bin/proxy ./cmd/proxy
go build -o bin/chaosctl ./cmd/chaosctl
```

### Run Proxy
```bash
# Listen on :8090, proxying upstream to localhost:8080 with gRPC control on :9090
./bin/proxy --listen :8090 --target localhost:8080 --grpc :9090 --metrics :9091
```

### Run All Tests & Benchmarks
```bash
go test -v -race ./...
go test -bench=. -benchmem ./internal/dataplane ./pkg/protocol
```
