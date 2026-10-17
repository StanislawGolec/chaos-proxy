# Go-Chaos-Proxy: Architectural Analysis & Phased Implementation Plan

> **Target Recruiter Focus:** Google Warsaw (Cloud Run, Kubernetes / Borg, Cloud Networking & SRE, Go Language Teams) & Quantitative Trading Firms (High-throughput systems, low latency, zero-allocation data paths).  
> **Role in Ecosystem:** Sits transparently between **Ultimo-Mobile** (Kotlin Android client) / **Ultimo-MARL** (Python PyTorch bot) and **Ultimo** (Java 21 Loom authoritative game server), injecting real-world network anomalies (jitter, packet loss, TCP resets, frame mutation) to validate distributed fault tolerance.  
> **Specialized Skills Available:** Consult `.agents/skills/chaos-grpc-generator/SKILL.md` for Protocol Buffers, gRPC control plane, and chaos fault injection procedures.

---

## 1. Project Files & Contextual Architecture Analysis

### 1.1 Existing Assets & Repository Context
An audit of `e:\MeineProjekte\Go-Chaos-Proxy` and the broader interconnected ecosystem reveals:
1. **`PROJECT_SPECIFICATION.md`:** Outlines the core mission: an Envoy-style L4/L7 transparent proxy with separated Data Plane (`net.Conn`, raw byte streams) and Control Plane (gRPC + Protocol Buffers `faults.proto`), dynamic CLI orchestration (`chaosctl`), and Prometheus SRE metrics.
2. **`CONVERSATION_CONTEXT.md` & Meta-Agents (`AGENT_07`, `AGENT_08`, `AGENT_09`):**
   - **Agent 07 (FAANG & Quant Portfolio Strategist):** Mandates measurable resume bullets (*"zero-allocation L4/L7 proxy with gRPC control plane, sustaining 50,000 req/sec under 15% drops"*), clean architecture diagrams, and one-command runnable environments.
   - **Agent 08 (Ultimo Ecosystem Coordinator):** Single source of truth for the wire protocol. Line-delimited ASCII (`\n`-terminated) text commands (`JOIN`, `PLAY`, `DRAW`) and server events (`WELCOME`, `ROOM_STATUS`, `TOP_CARD`, `HAND_UPDATE`), transported over raw TCP and RFC 6455 WebSockets.
   - **Agent 09 (QA, Concurrency & Bug Hunter):** Non-negotiable quality gates: `go test -race ./...` passing 100%, zero goroutine leaks (verified with `goleak`), static buffer reuse (`sync.Pool`), and resilience fuzzing.
3. **`Inspiration/Ultimo-Protocol` (`ProtocolCodec.java`, `ClientCommand.java`, `ServerEvent.java`):** Reference Java implementation defining the exact line-based lexicon, card token syntax (`<COLOR>_<VALUE>`), and error payloads.
4. **Current Repository State:** Git repository initialized (`StanislawGolec/Go-Chaos-Proxy.git`) with base README badge structure; no implementation code yet.

### 1.2 Core Architectural Principles & Trade-Offs
* **Separation of Concerns (Envoy / K8s Pattern):**
  * **Data Plane:** Ultra-fast, zero-allocation forwarding path. Operates directly on raw sockets using goroutines, channels, and buffer pools. Must not incur JSON or heavy protocol serialization overhead on the hot path.
  * **Control Plane:** gRPC service on port `9090`. Allows the `chaosctl` CLI or automated test harnesses to inject or withdraw faults dynamically without dropping existing client tunnels or restarting the proxy.
* **Concurrency Model:**
  * Two independent forwarding loops per connection (`client -> server` and `server -> client`).
  * Non-blocking timer mechanisms for latency injection to avoid thread/goroutine starvation.
  * Lock-free atomic configuration swaps (`atomic.Pointer[FaultRuleSet]`) ensuring zero reader lock contention on high-throughput packet routing.
* **Dual-Inspection Modes:**
  * **L4 Mode:** Pure transparent TCP stream proxying (arbitrary payloads, connection resets, packet drops, byte fuzzing).
  * **L7 Mode (WebSocket & Ultimo-Aware):** RFC 6455 frame-boundary parsing and line-delimited message inspection, allowing targeted fault injection (e.g., selectively delay only `PLAY` commands or corrupt `HAND_UPDATE` payloads).

---

## 2. Phased Implementation Roadmap Overview

| Stage | Phase Title | Focus Area | Key Deliverables |
| :---: | :--- | :--- | :--- |
| **Stage 1** | **Foundational Scaffolding & gRPC Protocol Stubs** | Build system, Protobuf, logging | Standard Go layout, `faults.proto`, `buf`/`protoc` codegen, `log/slog` |
| **Stage 2** | **High-Throughput L4 Data Plane & Buffer Pooling** | Transparent TCP, zero-alloc I/O | `net.Listener`, bidirectional pipes, `sync.Pool`, clean socket half-close |
| **Stage 3** | **L7 Frame Inspection (RFC 6455 & Ultimo Wire Codec)** | WebSocket & line protocol parser | Zero-copy frame parser, line tokenizer, message-level filter rules |
| **Stage 4** | **Chaos Fault Injection Pipeline** | Latency, jitter, drops, resets, fuzz | Gaussian jitter delay queue, probabilistic drop, TCP RST, bit-flipper |
| **Stage 5** | **gRPC Control Plane & Atomic Rule Dispatcher** | Dynamic runtime rule management | `ChaosControl` gRPC server, `atomic.Pointer` swap, event pub/sub streaming |
| **Stage 6** | **`chaosctl` Administrative CLI Operator Tool** | CLI UX & live telemetry streaming | Cobra/pflag CLI, colored terminal output, `chaosctl tail -f` event monitor |
| **Stage 7** | **Observability, SRE Prometheus Metrics & Health Plane** | Production metrics & profiling | Prometheus `/metrics`, `/healthz`, `/readyz`, `net/http/pprof` endpoints |
| **Stage 8** | **Rigorous QA Suite, Concurrency Audit & Ecosystem Integration** | Race tests, leaks, Docker, E2E | `go test -race`, `goleak`, memory benchmarks, Docker Compose E2E suite |

---

## 3. Detailed Stage-by-Stage Implementation Plan

### Stage 1: Foundational Scaffolding, Configuration & gRPC Protocol Stubs
**Objective:** Establish standard Go idiomatic directory layout, configure structured logging with `log/slog`, define Protocol Buffer interfaces, and automate code generation.
**Current Status:** Completed (Scaffolding, Module, Layout, Config, Slog, Proto contract & Makefile) / Partially Done on stub binary codegen (scheduled for Stage 5).

* **Tasks to Implement & Status:**
  1. `[DONE]` **Initialize Go module:** Created `go.mod` specifying Go 1.23+ and core module dependencies (`google.golang.org/protobuf`, `google.golang.org/grpc`, `github.com/prometheus/client_golang`, `golang.org/x/sync`, `golang.org/x/time`).
  2. `[DONE]` **Implement Standard Go Project Layout:**
     - `cmd/proxy/main.go` — Entrypoint for the proxy daemon.
     - `cmd/chaosctl/main.go` — Entrypoint for the CLI tool.
     - `api/proto/v1/chaos.proto` — Canonical Protobuf contract.
     - `internal/config/config.go` & `logger.go` — CLI flags, validation, and structured slog handler.
     - `internal/config/config_test.go` — Unit tests for configuration parsing, validation, and logging.
     - `internal/dataplane/dataplane.go` — Sockets, streaming pipes, and connection tracking skeleton.
     - `internal/controlplane/controlplane.go` — gRPC server skeleton.
     - `internal/engine/types.go` — Fault domain models, actions, and thread-safe atomic rule container.
     - `internal/metrics/metrics.go` — Prometheus collectors and HTTP server (`/metrics`, `/healthz`, `/readyz`).
     - `pkg/protocol/protocol.go` — RFC 6455 and Ultimo protocol token constants.
  3. `[DONE]` **Define `api/proto/v1/chaos.proto`:**
     - Full RPC specification: `SetLatency`, `SetPacketDrop`, `SetCorruption`, `SetThrottle`, `TriggerDisconnect`, `ResetAllFaults`, `GetStatus`, `StreamEvents`.
     - Typed messages: `LatencyConfig`, `DropConfig`, `CorruptionConfig`, `ThrottleConfig`, `DisconnectCommand`, `StatusResponse`, `StreamFilter`, `PacketEvent`.
  4. `[DONE]` **Build automated code generation scripts & Makefile:**
     - `scripts/generate_proto.ps1` (PowerShell) and `scripts/generate_proto.sh` (Bash).
     - Standard `Makefile` with targets: `all`, `build`, `test`, `race`, `bench`, `proto`, `clean`, `run`.
  5. `[DONE]` **Protobuf Stubs Generation (`pkg/proto/v1/chaos.pb.go`, `chaos_grpc.pb.go`):** Compiled canonical `api/proto/v1/chaos.proto` into Go structs and gRPC client/server interfaces using `protoc` 29.3, `protoc-gen-go`, and `protoc-gen-go-grpc`. Verified 100% interoperability with gRPC transport.
  6. `[DONE]` **Configure centralized structured logging via `log/slog`:** Context-aware logging supporting JSON and Text handlers with log level control and tunnel context propagation.
  7. `[DONE]` **Unit tests:** Created `internal/config/config_test.go` verifying flag parsing, default values, error validation, and slog output.
* **What to Use / Technologies:**
  - Standard library `log/slog`, `flag` / `pflag`, `sync/atomic`.
  - `google.golang.org/protobuf`, `google.golang.org/grpc`.
  - `Makefile`, PowerShell & Bash generator scripts.
* **Success & Verification Criteria:**
  - Clean project architecture matching Go standards.
  - Protobuf contract fully specified and ready for control plane integration in Stage 5.

---

### Stage 2: High-Throughput L4 Data Plane & Buffer Pooling
**Objective:** Build the core transparent TCP proxy forwarding engine with bidirectional streaming goroutines, zero-allocation memory pooling, and leak-free connection lifecycles.
**Current Status:** Completed (100% Implemented: BufferPool, Pipe, DataPlane listener, TCP options, half-close propagation, tunnel lifecycle, and comprehensive integration tests).

* **Tasks to Implement & Status:**
  1. `[DONE]` **TCP Listener & Upstream Dialing:** Implemented in `internal/dataplane/dataplane.go`. Listens on configurable proxy port (default `:8090`), dials target upstream server (`localhost:8080`) with configurable `ConnectTimeout`, applies `TCP_NODELAY` and `TCP_KEEPALIVE`.
  2. `[DONE]` **Bidirectional Streaming Goroutines:** Implemented in `internal/dataplane/pipe.go`. Launches coordinated twin goroutines per connection (`C2S` and `S2C`), with context cancellation propagation and wait group synchronization.
  3. `[DONE]` **Zero-Allocation Memory Management:** Implemented `BufferPool` in `internal/dataplane/buffer_pool.go` using `sync.Pool` with pre-allocated 32KB/64KB byte slices, reusing memory across read/write operations without heap thrashing.
  4. `[DONE]` **Graceful Socket Half-Close & Teardown:** Propagates `CloseWrite()` on `*net.TCPConn` upon receiving `io.EOF`, ensuring clean half-close without hanging or blocking reader goroutines.
  5. `[DONE]` **Active Tunnel Tracking & Metrics Hooks:** Unique atomic tunnel ID generation (`tunnel-1`, `tunnel-2`), thread-safe active tunnel registry, and atomic packet/byte counters (`TotalPackets`, `TotalBytes`, `ActiveConnections`).
  6. `[DONE]` **Unit & Integration Tests:** Implemented in `internal/dataplane/pipe_test.go`:
     - `TestBufferPool`: Validates slice capacity, length, and reuse.
     - `TestDataPlaneTransparentProxying`: Spins up mock upstream echo server, proxies traffic, asserts byte-for-byte fidelity, asserts packet/byte counters increment, and verifies active connection cleanup upon disconnect.
     - `TestDataPlaneUpstreamUnreachable`: Asserts immediate graceful error handling when upstream server is down.
* **What to Use / Technologies:**
  - Standard library `net`, `io`, `sync`, `sync/atomic`, `context`.
  - `sync.Pool` for byte slice recycling.
* **Success & Verification Criteria:**
  - Zero heap allocation on steady-state streaming hot paths.
  - Transparent bidirectional proxying verified with automated integration tests.

---

### Stage 3: Layer 7 Awareness (RFC 6455 WebSocket & Ultimo Wire Codec)
**Objective:** Equip the data plane with protocol-awareness to inspect RFC 6455 WebSocket frames and Ultimo line-delimited ASCII commands without breaking transparent forwarding.

### Stage 3: Layer 7 Awareness (RFC 6455 WebSocket & Ultimo Wire Codec)
**Objective:** Equip the data plane with protocol-awareness to inspect RFC 6455 WebSocket frames and Ultimo line-delimited ASCII commands without breaking transparent forwarding.
**Current Status:** Completed (100% Implemented: WebSocket frame encoder/decoder, in-place unmasker, Ultimo card & message tokenizer, unified inspector, and unit tests).

* **Tasks to Implement & Status:**
  1. `[DONE]` **RFC 6455 WebSocket Frame Decoder & Encoder:** Implemented in `pkg/protocol/websocket.go`. Streaming header parser for FIN bit, RSV flags, 4-bit opcodes, 7-bit/16-bit/64-bit payload lengths, 4-byte client masking keys, DOS payload limits (10MB), and zero-allocation in-place unmasking (`UnmaskInPlace`).
  2. `[DONE]` **Ultimo Line-Protocol Tokenizer:** Implemented in `pkg/protocol/ultimo.go`. Matches Java `ProtocolCodec.java` reference: validates card tokens (`<COLOR>_<VALUE>`), formats cards, and tokenizes client commands (`JOIN`, `START`, `PLAY`, `DRAW`, `PASS`, `PING`, `CHAT`) and server events (`WELCOME`, `ROOM_STATUS`, `TOP_CARD`, `TURN_ANNOUNCED`, `OPPONENT_UPDATE`, `GAME_OVER`).
  3. `[DONE]` **Unified L7 Protocol Inspector:** Implemented in `pkg/protocol/inspect.go`. Non-destructive packet inspection classifying buffers as WebSocket handshake, RFC 6455 frames, raw ASCII line commands, or generic bytes.
  4. `[DONE]` **Dual Mode Routing Policy:** Integrated into `internal/dataplane/pipe.go`. Supports `ModeL4Raw` (pass-through without inspection) and `ModeL7Aware` (extracts protocol metadata, logs command verbs, and provides L7 hooks for fault engine).
  5. `[DONE]` **Unit & Benchmark Tests:** Implemented in `pkg/protocol/protocol_test.go`:
     - `TestWebSocketFrameEncodeDecodeUnmasked` & `TestWebSocketFrameEncodeDecodeMasked`: Verifies frame encoding, decoding, and mask restoration.
     - `TestWebSocketFrameExtended16BitLength`: Verifies frames requiring 16-bit extended length fields.
     - `TestWebSocketControlFrames`: Verifies Ping/Pong control frames.
     - `TestUnmaskInPlace`: Validates XOR in-place unmasking.
     - `TestParseCard` & `TestFormatCard`: Validates card token parsing and formatting across all standard and wild colors/values.
     - `TestParseUltimoMessage`: Validates parsing of client commands and server events.
     - `TestInspect`: Validates inspection of WS handshake, WS frame containing Ultimo command, raw ASCII line, and raw bytes.
     - `BenchmarkUnmaskInPlace`: Microbenchmark for zero-allocation memory performance.
* **What to Use / Technologies:**
  - Bitwise operations and binary decoding standard library (`encoding/binary`, `bytes`, `strings`, `io`).
  - Zero external dependencies.
* **Success & Verification Criteria:**
  - RFC 6455 frames and Ultimo line commands accurately parsed and validated.
  - Zero heap allocation in XOR unmasking.

---

### Stage 4: Chaos Fault Injection Pipeline (The Core Chaos Engine)
**Objective:** Implement the configurable fault engine: Latency/Jitter injection, Probabilistic Packet Drops, Payload Mutation/Fuzzing, TCP RST tearing, and Bandwidth Throttling.
**Current Status:** Completed (100% Implemented: latency & Gaussian jitter, drop controller & bursts, L4/L7 bit flipper/fuzzer, TCP RST reset helper, TokenBucket rate limiter, master Engine coordinator, and comprehensive statistical test suite).

* **Tasks to Implement & Status:**
  1. `[DONE]` **Fault Pipeline Interceptor Architecture:** Implemented in `internal/engine/engine.go`. Ordered interceptor pipeline coordinating disconnect triggers, packet drops, payload mutations, latency delays, and bandwidth throttles.
  2. `[DONE]` **Latency & Jitter Injection Engine:** Implemented in `internal/engine/latency.go`. Supports `BaseLatency`, `Probability`, and both Uniform and Gaussian/Normal jitter curves (`rand.NormFloat64`) for realistic mobile radio simulation.
  3. `[DONE]` **Packet & Frame Dropping Engine:** Implemented in `internal/engine/drop.go`. Thread-safe `DropController` executing probabilistic packet discards and multi-packet burst drops to simulate mobile tunnel dead zones.
  4. `[DONE]` **Payload Mutation & Fuzzing (Bit Flipper):** Implemented in `internal/engine/corrupt.go`. L4 bit-flipper (flips random bits in payload) and L7 semantic fuzzer (corrupts Ultimo game tokens such as `RED` -> `PURPLE` and `PLAY` -> `PLXX`).
  5. `[DONE]` **Abrupt Connection Tearing (Hardware TCP RST):** Implemented in `internal/engine/disconnect.go`. Sets `SetLinger(0)` on `*net.TCPConn` before closing to emit immediate hardware TCP RST packets.
  6. `[DONE]` **Token Bucket Bandwidth Throttler:** Implemented in `internal/engine/throttle.go`. Thread-safe `TokenBucket` rate limiter pacing byte transmission to simulate 2G/EDGE speeds.
  7. `[DONE]` **Unit & Statistical Verification Tests:** Implemented in `internal/engine/engine_test.go`:
     - `TestCalculateDelayUniform` & `TestCalculateDelayGaussian`: Statistical mean convergence tests over thousands of iterations.
     - `TestDropControllerProbabilistic`: Statistical binomial distribution assertion (25% drop rate over 4,000 trials).
     - `TestDropControllerBurst`: Validates multi-packet burst drop sequences.
     - `TestMutatePayloadL4` & `TestMutatePayloadL7`: Verifies byte mutations and semantic token replacements.
     - `TestTokenBucketThrottler`: Validates bandwidth pacing wait calculation.
     - `TestApplyTCPReset`: Verifies TCP RST socket closure.
     - `TestEnginePipelineOrchestration`: Tests complete pipeline order, rule swaps, direction filtering, and resets.
* **What to Use / Technologies:**
  - Standard library `math/rand/v2` (thread-safe PCG generator), `time`, `net`, `sync`.
  - Zero external dependencies.
* **Success & Verification Criteria:**
  - Statistical verification tests confirm drop ratio and latency within standard deviations.
  - Fault pipeline integrated into streaming pipe hot path.

---

### Stage 5: Control Plane (gRPC Server, Dynamic Atomic Swaps & Event Streaming)
**Objective:** Implement the gRPC administrative service to dynamically update fault injection rules at runtime without connection restarts, and stream real-time packet telemetry.
**Current Status:** Completed (100% Implemented: Protobuf stubs, gRPC server, atomic rule updates, event broadcaster, and integration tests).

* **Tasks to Implement & Status:**
  1. `[DONE]` **gRPC Server Implementation (`ChaosControlServer`):** Implemented in `internal/controlplane/server.go`. Listens on `:9090`, implements all RPC endpoints: `SetLatency`, `SetPacketDrop`, `SetCorruption`, `SetThrottle`, `TriggerDisconnect`, `ResetAllFaults`, `GetStatus`, and `StreamEvents`.
  2. `[DONE]` **Lock-Free Atomic Rule Configuration:** Implemented in `internal/engine/types.go` and `internal/controlplane/server.go`. Uses `atomic.Pointer[RuleSet]` (`rules.Store()`, `rules.Load()`) ensuring zero mutex contention or blocking on the data plane packet hot paths during dynamic rule updates.
  3. `[DONE]` **Bi-directional Telemetry Stream (`StreamEvents`):** Implemented in `internal/controlplane/broadcaster.go`. Thread-safe pub/sub fan-out broadcaster with buffered subscriber channels and non-blocking drops (`select { case ch <- event: default: }`), guaranteeing slow monitoring clients never block proxy forwarding.
  4. `[DONE]` **Unit & Integration Tests:** Implemented in `internal/controlplane/server_test.go`:
     - `TestControlPlaneRPCs`: Tests in-memory gRPC client connecting over `bufconn`, mutates latency, drop, corruption, throttle, asserts status response, resets all faults, and verifies atomic consistency.
     - `TestBroadcasterFanOut`: Verifies fan-out delivery across multiple concurrent streaming subscribers and proper channel deregistration on exit.
* **What to Use / Technologies:**
  - `google.golang.org/grpc`, `google.golang.org/protobuf`, `sync/atomic`, Go buffered channels, `google.golang.org/grpc/test/bufconn`.
* **Success & Verification Criteria:**
  - Dynamic rule changes via gRPC take effect instantaneously (<1ms) on active connections without restarts.
  - Telemetry event streaming operates asynchronously without starving data plane forwarding.

---

### Stage 6: The `chaosctl` Command-Line Interface (CLI Operator Tool)
**Objective:** Deliver a production-grade, standalone CLI tool for human operators and CI automation scripts to orchestrate fault scenarios and monitor network behavior.

* **Tasks to Implement:**
  1. **CLI Commands & Flags:**
     - `chaosctl latency --base=200ms --jitter=50ms --probability=0.9`
     - `chaosctl drop --rate=0.20 --burst=3`
     - `chaosctl corrupt --rate=0.05 --mode=l7`
     - `chaosctl throttle --rate=64kbps`
     - `chaosctl disconnect --rst`
     - `chaosctl reset` (clears all faults, enters transparent bypass mode)
     - `chaosctl status` (prints active rules, uptime, total packets, current active tunnels)
  2. **Live Event Monitor (`chaosctl tail -f`):**
     - Subscribes to gRPC `StreamEvents`.
     - Formats and displays packet flow with colored ANSI output:
       - Green: `[PASS]`
       - Yellow: `[DELAY]` (+320ms)
       - Red: `[DROP]` (Packet discarded)
       - Magenta: `[CORRUPT]` (Bit flipped)
  3. **Machine-Readable Output:**
     - Add `--json` flag to all inspection commands for scripting and CI pipeline assertions.
* **What to Use / Technologies:**
  - `github.com/spf13/cobra` or clean Go standard `flag` with subcommand routing.
  - ANSI terminal escape formatting (`github.com/fatih/color` or pure ANSI codes).
* **Success & Verification Criteria:**
  - Executing `chaosctl latency` immediately alters behavior of running proxy.
  - `chaosctl tail -f` renders packet events in real-time with clean exit on `Ctrl+C`.

---

### Stage 7: Observability, SRE Prometheus Metrics & Health Plane
**Objective:** Implement production-grade telemetry conforming to Google Site Reliability Engineering (SRE) standards.

* **Tasks to Implement:**
  1. **Prometheus HTTP Server:**
     - Expose metrics on port `:9091` at `/metrics`.
  2. **Metric Definitions:**
     - `chaos_active_tunnels` (Gauge): Current concurrent client-server connections.
     - `chaos_packets_total{direction="c2s|s2c", action="pass|delay|drop|corrupt"}` (Counter).
     - `chaos_bytes_transferred_total{direction="c2s|s2c"}` (Counter).
     - `chaos_injected_latency_seconds` (Histogram): Buckets tailored to gaming networks (`[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]`).
     - `chaos_control_plane_rule_updates_total` (Counter).
  3. **Health & Readiness Endpoints:**
     - `GET /healthz` — Returns 200 OK if proxy listener and upstream reachability are healthy.
     - `GET /readyz` — Returns 200 OK once control plane and data plane listeners are bound.
  4. **Performance Profiling:**
     - Mount standard `net/http/pprof` endpoints under `/debug/pprof/` for runtime CPU, memory, and goroutine profiling.
* **What to Use / Technologies:**
  - `github.com/prometheus/client_golang/prometheus`, `prometheus/client_golang/promhttp`.
  - `net/http/pprof`.
* **Success & Verification Criteria:**
  - `curl http://localhost:9091/metrics` returns valid Prometheus exposition format containing all custom metrics.
  - `curl http://localhost:9091/healthz` responds with HTTP 200.

---

### Stage 8: Comprehensive QA Suite, Concurrency Audit & Ecosystem Integration
**Objective:** Pass all Agent 09 QA gates (race conditions, goroutine leaks, fuzzing) and integrate into the Ultimo gaming ecosystem.

* **Tasks to Implement:**
  1. **Data Race & Concurrency Verification:**
     - Run complete test suite with `go test -race -v ./...`. Zero race warnings permitted.
  2. **Goroutine Leak Audit:**
     - Integrate `go.uber.org/goleak` in all tests to assert that every connection teardown cleans up 100% of its goroutines.
  3. **High-Throughput Benchmarking:**
     - Benchmark sustained throughput: target $\ge 50,000\text{ packets/sec}$ with minimal GC pause times and zero steady-state heap allocations.
  4. **Multi-Stage Containerization:**
     - Production `Dockerfile` using multi-stage build: `golang:1.23-alpine` builder $\rightarrow$ minimal `scratch` or `gcr.io/distroless/static-debian12` runtime image running as non-root user.
  5. **Ultimo Ecosystem End-to-End Orchestration (`docker-compose.yml`):**
     - Compose environment running:
       - Service 1: `ultimo-server` (Java 21 Loom on port 8080).
       - Service 2: `go-chaos-proxy` (Data plane on 8090 forwarding to 8080; gRPC on 9090; Metrics on 9091).
       - Service 3: `prometheus` scraping proxy metrics.
       - Service 4: Automated integration test runner executing match simulations while `chaosctl` injects 20% drops and 300ms jitter.
  6. **CI/CD Pipeline (GitHub Actions):**
     - Workflow executing: `golangci-lint`, `go test -race ./...`, benchmark regression checks, and multi-arch Docker image builds.
* **What to Use / Technologies:**
  - `go test -race`, `go.uber.org/goleak`, `github.com/golangci/golangci-lint`.
  - Docker, Docker Compose, GitHub Actions.
* **Success & Verification Criteria:**
  - 100% green test suite under `-race` and `goleak`.
  - Fully automated `docker compose up --build` demonstrating Ultimo game resilience under simulated chaos.

---

## 4. Target Project File Structure

```
Go-Chaos-Proxy/
├── .github/
│   └── workflows/
│       └── ci.yml                 # Lint, race test, leak check, Docker build
├── api/
│   └── proto/
│       └── v1/
│           └── chaos.proto        # Canonical gRPC / Protobuf specification
├── cmd/
│   ├── proxy/
│   │   └── main.go                # Main proxy entrypoint (data & control planes)
│   └── chaosctl/
│       └── main.go                # Standalone operator CLI tool
├── internal/
│   ├── config/
│   │   └── config.go              # CLI flags & environment configuration
│   ├── dataplane/
│   │   ├── listener.go            # TCP listener & connection acceptance
│   │   ├── pipe.go                # Bidirectional streaming goroutines & pools
│   │   ├── buffer_pool.go         # sync.Pool reusable memory buffers
│   │   └── pipe_test.go           # Data plane unit & leak tests
│   ├── engine/
│   │   ├── fault.go               # Fault engine interfaces & actions
│   │   ├── latency.go             # Base latency & Gaussian jitter delay queue
│   │   ├── drop.go                # Probabilistic & burst packet drop
│   │   ├── corrupt.go             # L4/L7 bit-flipping fuzzer
│   │   ├── throttle.go            # Token bucket bandwidth rate limiter
│   │   ├── rules.go               # Thread-safe atomic rule container
│   │   └── engine_test.go         # Fault distribution statistical tests
│   ├── controlplane/
│   │   ├── server.go              # gRPC service implementation
│   │   ├── broadcaster.go         # Non-blocking event fan-out pub/sub bus
│   │   └── server_test.go         # Dynamic rule swap tests
│   ├── metrics/
│   │   └── prometheus.go          # Prometheus collectors & healthz handlers
│   └── protocol/
│       ├── websocket.go           # RFC 6455 frame header parsing
│       ├── ultimo.go              # Ultimo line-protocol tokenizer
│       └── protocol_test.go       # Protocol decoding verification
├── pkg/
│   └── proto/
│       └── v1/                    # Generated Protobuf Go code
│           ├── chaos.pb.go
│           └── chaos_grpc.pb.go
├── scripts/
│   ├── test_chaos.sh              # E2E chaos simulation test script
│   └── generate_proto.sh          # Protoc stubs compiler script
├── deployments/
│   ├── Dockerfile                 # Distroless multi-stage build
│   └── docker-compose.yml         # Ultimo + Chaos Proxy + Prometheus
├── go.mod
├── go.sum
├── Makefile                       # build, test, race, proto, bench targets
└── README.md                      # Architecture diagram, badges, quickstart
```

---

## 5. Alignment with Agent Audit Standards

* **Agent 07 (Recruitment & FAANG Portfolio Alignment):**
  - Showcases high-throughput systems programming in Go directly targeting Google Warsaw Cloud Networking and SRE teams.
  - Implements the exact architectural design pattern used by **Envoy Proxy**, **Kubernetes (CRI/CNI)**, and **Chaos Mesh**.
* **Agent 08 (Ultimo Distributed Ecosystem Integration):**
  - Seamlessly bridges `Ultimo-Mobile` (Android Kotlin) and `Ultimo-MARL` (PyTorch bot) with `Ultimo` (Java 21 Loom server).
  - Understands and respects the authoritative line-delimited protocol contracts (`JOIN`, `PLAY`, `DRAW`, `WELCOME`, `ROOM_STATUS`, `TOP_CARD`).
* **Agent 09 (QA, Concurrency & Security Standards):**
  - 100% data-race-free architecture verified via `go test -race`.
  - Zero goroutine leaks enforced via `go.uber.org/goleak`.
  - Zero heap allocation on steady-state byte streaming verified via `go test -bench . -benchmem`.
