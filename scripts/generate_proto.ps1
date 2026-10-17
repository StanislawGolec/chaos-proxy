# Generate Go stubs from api/proto/v1/chaos.proto
Write-Host "Compiling Protobuf definitions with protoc..." -ForegroundColor Cyan

$protoDir = "api/proto/v1"
$outDir = "pkg/proto/v1"

if (-not (Test-Path $outDir)) {
    New-Item -ItemType Directory -Path $outDir -Force | Out-Null
}

protoc `
    --proto_path=$protoDir `
    --go_out=$outDir --go_opt=paths=source_relative `
    --go-grpc_out=$outDir --go-grpc_opt=paths=source_relative `
    "$protoDir/chaos.proto"

if ($LASTEXITCODE -eq 0) {
    Write-Host "Protobuf compilation successful -> $outDir" -ForegroundColor Green
} else {
    Write-Host "Protobuf compilation failed. Ensure protoc, protoc-gen-go and protoc-gen-go-grpc are in PATH." -ForegroundColor Red
}
