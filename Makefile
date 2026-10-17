.PHONY: all build test race bench proto clean lint run

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
	go test -v -bench=. -benchmem ./...

lint:
	golangci-lint run ./...

proto:
	@bash scripts/generate_proto.sh

clean:
	rm -rf $(BINARY_DIR)

run: build
	./$(PROXY_BIN) --listen :8090 --target localhost:8080 --grpc :9090
