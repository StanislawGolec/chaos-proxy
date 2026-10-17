<#
.SYNOPSIS
    Automated End-to-End Fault Injection Runner for Go-Chaos-Proxy on Windows.
#>

[CmdletBinding()]
param (
    [string]$ProxyAddr = "127.0.0.1:8090",
    [string]$GrpcAddr = "127.0.0.1:9090",
    [string]$MetricsAddr = "127.0.0.1:9091",
    [string]$ChaosctlPath = ".\bin\chaosctl.exe"
)

$ErrorActionPreference = "Stop"

Write-Host "==================================================" -ForegroundColor Cyan
Write-Host "Go-Chaos-Proxy End-to-End Fault Injection Runner" -ForegroundColor Cyan
Write-Host "==================================================" -ForegroundColor Cyan

# 1. Health check verification
Write-Host "[-] Verifying proxy liveness endpoint (http://$MetricsAddr/healthz)..." -ForegroundColor Yellow
try {
    $health = Invoke-RestMethod -Uri "http://$MetricsAddr/healthz" -Method Get
    Write-Host "[+] Proxy healthz is UP: $($health.status)" -ForegroundColor Green
} catch {
    Write-Warning "Could not connect to proxy healthz on http://$MetricsAddr/healthz. Ensure proxy is running."
}

# 2. Status inspection
Write-Host "[-] Checking initial baseline status..." -ForegroundColor Yellow
& $ChaosctlPath --grpc="$GrpcAddr" status

# 3. Inject Latency & Gaussian Jitter
Write-Host "[-] Injecting 150ms latency with 30ms Gaussian jitter (80% probability)..." -ForegroundColor Yellow
& $ChaosctlPath --grpc="$GrpcAddr" latency --base=150ms --jitter=30ms --dist=gaussian --prob=0.8 --dir=ALL

# 4. Inject Packet Drops
Write-Host "[-] Injecting 20% packet drops with burst size 2 (Server to Client)..." -ForegroundColor Yellow
& $ChaosctlPath --grpc="$GrpcAddr" drop --rate=0.20 --burst=2 --dir=S2C

# 5. Inject L7 Semantic Corruption
Write-Host "[-] Injecting 5% L7 semantic corruption..." -ForegroundColor Yellow
& $ChaosctlPath --grpc="$GrpcAddr" corrupt --rate=0.05 --mode=l7 --dir=ALL

# 6. Inject Bandwidth Rate Limiting
Write-Host "[-] Injecting 128kbps bandwidth throttling..." -ForegroundColor Yellow
& $ChaosctlPath --grpc="$GrpcAddr" throttle --rate=128kbps --dir=ALL

# 7. Verify Active Rules in Status
Write-Host "[-] Inspecting active fault rules..." -ForegroundColor Yellow
& $ChaosctlPath --grpc="$GrpcAddr" status

# 8. Reset Proxy Faults
Write-Host "[-] Resetting all fault rules back to transparent pass-through..." -ForegroundColor Yellow
& $ChaosctlPath --grpc="$GrpcAddr" reset

# 9. Verify Clean State
Write-Host "[-] Verifying clean state..." -ForegroundColor Yellow
& $ChaosctlPath --grpc="$GrpcAddr" status

Write-Host "==================================================" -ForegroundColor Cyan
Write-Host "[+] Chaos Engineering E2E Scenario Completed Successfully!" -ForegroundColor Green
Write-Host "==================================================" -ForegroundColor Cyan
