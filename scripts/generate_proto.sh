#!/usr/bin/env bash
set -euo pipefail

PROTO_DIR="api/proto/v1"
OUT_DIR="pkg/proto/v1"

mkdir -p "${OUT_DIR}"

echo "Compiling Protobuf definitions from ${PROTO_DIR}..."
protoc \
    --proto_path="${PROTO_DIR}" \
    --go_out="${OUT_DIR}" --go_opt=paths=source_relative \
    --go-grpc_out="${OUT_DIR}" --go-grpc_opt=paths=source_relative \
    "${PROTO_DIR}/chaos.proto"

echo "Protobuf compilation successful -> ${OUT_DIR}"
