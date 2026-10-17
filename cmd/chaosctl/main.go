package main

import (
	"flag"
	"fmt"
	"os"
)

var (
	grpcAddr = flag.String("grpc", "localhost:9090", "Control plane gRPC address")
	jsonOut  = flag.Bool("json", false, "Output results in JSON format")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "chaosctl - Operator CLI for Go-Chaos-Proxy\n\n")
		fmt.Fprintf(os.Stderr, "Usage:\n")
		fmt.Fprintf(os.Stderr, "  chaosctl [global-flags] <command> [command-flags]\n\n")
		fmt.Fprintf(os.Stderr, "Commands:\n")
		fmt.Fprintf(os.Stderr, "  latency       Configure packet delay and jitter\n")
		fmt.Fprintf(os.Stderr, "  drop          Configure packet drop ratios\n")
		fmt.Fprintf(os.Stderr, "  corrupt       Configure random bit flipping\n")
		fmt.Fprintf(os.Stderr, "  throttle      Configure bandwidth rate limiting\n")
		fmt.Fprintf(os.Stderr, "  disconnect    Force socket termination (TCP RST)\n")
		fmt.Fprintf(os.Stderr, "  reset         Clear all active faults\n")
		fmt.Fprintf(os.Stderr, "  status        Display active faults and metrics\n")
		fmt.Fprintf(os.Stderr, "  tail          Stream real-time packet event telemetry\n\n")
		fmt.Fprintf(os.Stderr, "Global Flags:\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		os.Exit(1)
	}

	cmd := args[0]
	fmt.Printf("chaosctl connecting to %s to execute '%s'...\n", *grpcAddr, cmd)
	fmt.Printf("[Stage 1 Scaffolding] Command '%s' parsed. Full gRPC client implementation scheduled for Stage 6.\n", cmd)
}
