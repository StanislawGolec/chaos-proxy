.PHONY: all build test race bench bench-cpu bench-mem profile proto clean lint run

BINARY_DIR := bin
PROXY_BIN := $(BINARY_DIR)/proxy
CHAOSCTL_BIN := $(BINARY_DIR)/chaosctl

all: build test

build:
	@mkdir -p $(BINARY_DIR)
	go build -v -o $(PROXY_BIN) ./cmd/proxy
	go build -v -o $(CHAOSCTL_BIN) ./cmd/chaosctl

test:
	go test -v -cover ./...

race:
	go test -v -race ./...

bench:
	go test -v -run=^$$ -bench=. -benchmem ./internal/dataplane ./pkg/protocol

bench-cpu:
	go test -run=^$$ -bench=. -benchmem -cpuprofile=cpu.pprof ./internal/dataplane ./pkg/protocol

bench-mem:
	go test -run=^$$ -bench=. -benchmem -memprofile=mem.pprof ./internal/dataplane ./pkg/protocol

profile:
	go tool pprof -http=:8080 cpu.pprof

lint:
	golangci-lint run ./...

proto:
	@bash scripts/generate_proto.sh

clean:
	rm -rf $(BINARY_DIR)

run: build
	./$(PROXY_BIN) --listen :8090 --target localhost:8080 --grpc :9090
