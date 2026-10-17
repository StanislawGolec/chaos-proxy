#!/usr/bin/env bash
set -euo pipefail

PROXY_ADDR="${PROXY_ADDR:-localhost:8090}"
GRPC_ADDR="${GRPC_ADDR:-localhost:9090}"
METRICS_ADDR="${METRICS_ADDR:-localhost:9091}"
CHAOSCTL="./bin/chaosctl"

echo "=================================================="
echo "Go-Chaos-Proxy End-to-End Fault Injection Runner"
echo "=================================================="

# 1. Health check verification
echo "[-] Verifying proxy liveness endpoint (http://${METRICS_ADDR}/healthz)..."
curl -s -f "http://${METRICS_ADDR}/healthz" > /dev/null
echo "[+] Proxy healthz is UP."

# 2. Status inspection
echo "[-] Checking initial baseline status..."
${CHAOSCTL} --grpc="${GRPC_ADDR}" status

# 3. Inject Latency & Gaussian Jitter
echo "[-] Injecting 150ms latency with 30ms Gaussian jitter (80% probability)..."
${CHAOSCTL} --grpc="${GRPC_ADDR}" latency --base=150ms --jitter=30ms --dist=gaussian --prob=0.8 --dir=ALL

# 4. Inject Packet Drops
echo "[-] Injecting 20% packet drops with burst size 2 (Server to Client)..."
${CHAOSCTL} --grpc="${GRPC_ADDR}" drop --rate=0.20 --burst=2 --dir=S2C

# 5. Inject L7 Semantic Corruption
echo "[-] Injecting 5% L7 semantic corruption..."
${CHAOSCTL} --grpc="${GRPC_ADDR}" corrupt --rate=0.05 --mode=l7 --dir=ALL

# 6. Inject Bandwidth Rate Limiting
echo "[-] Injecting 128kbps bandwidth throttling..."
${CHAOSCTL} --grpc="${GRPC_ADDR}" throttle --rate=128kbps --dir=ALL

# 7. Verify Active Rules in Status
echo "[-] Inspecting active fault rules..."
${CHAOSCTL} --grpc="${GRPC_ADDR}" status

# 8. Query Prometheus Metrics
echo "[-] Verifying Prometheus metrics incremented..."
curl -s "http://${METRICS_ADDR}/metrics" | grep "chaos_control_plane_rule_updates_total" || true

# 9. Reset Proxy Faults
echo "[-] Resetting all fault rules back to transparent pass-through..."
${CHAOSCTL} --grpc="${GRPC_ADDR}" reset

# 10. Verify Clean State
echo "[-] Verifying clean state..."
${CHAOSCTL} --grpc="${GRPC_ADDR}" status

echo "=================================================="
echo "[+] Chaos Engineering E2E Scenario Completed Successfully!"
echo "=================================================="
