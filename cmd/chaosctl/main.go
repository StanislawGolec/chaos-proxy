package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	chaosv1 "github.com/StanislawGolec/go-chaos-proxy/pkg/proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ANSI color codes for terminal formatting
const (
	colorReset   = "\033[0m"
	colorBold    = "\033[1m"
	colorDim     = "\033[2m"
	colorRed     = "\033[31m"
	colorGreen   = "\033[32m"
	colorYellow  = "\033[33m"
	colorMagenta = "\033[35m"
	colorCyan    = "\033[36m"
	colorWhite   = "\033[37m"
)

// CLI represents the chaosctl operator interface.
type CLI struct {
	grpcAddr string
	jsonOut  bool
	noColor  bool
	client   chaosv1.ChaosControlClient
	conn     *grpc.ClientConn
	stdout   io.Writer
	stderr   io.Writer
}

// NewCLI creates a new CLI instance with custom stdout/stderr writers.
func NewCLI(stdout, stderr io.Writer) *CLI {
	return &CLI{
		grpcAddr: "localhost:9090",
		stdout:   stdout,
		stderr:   stderr,
	}
}

// SetClient overrides the gRPC client (useful for in-memory / unit testing).
func (c *CLI) SetClient(client chaosv1.ChaosControlClient) {
	c.client = client
}

// Connect dials the gRPC control plane if client is not already configured.
func (c *CLI) Connect(ctx context.Context) error {
	if c.client != nil {
		return nil
	}

	conn, err := grpc.NewClient(
		c.grpcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to control plane at %s: %w", c.grpcAddr, err)
	}

	c.conn = conn
	c.client = chaosv1.NewChaosControlClient(conn)
	return nil
}

// Close terminates any active gRPC client connection.
func (c *CLI) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// Run parses arguments and dispatches to appropriate command handler.
func (c *CLI) Run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		c.printUsage()
		return errors.New("no command specified")
	}

	// Parse global flags if provided before subcommand
	var subArgs []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "-grpc" || arg == "--grpc" {
			if i+1 < len(args) {
				c.grpcAddr = args[i+1]
				i++
				continue
			}
		} else if strings.HasPrefix(arg, "-grpc=") || strings.HasPrefix(arg, "--grpc=") {
			parts := strings.SplitN(arg, "=", 2)
			c.grpcAddr = parts[1]
			continue
		} else if arg == "-json" || arg == "--json" {
			c.jsonOut = true
			continue
		} else if arg == "-no-color" || arg == "--no-color" {
			c.noColor = true
			continue
		} else if arg == "-h" || arg == "--help" || arg == "help" {
			c.printUsage()
			return nil
		} else if !strings.HasPrefix(arg, "-") {
			// Subcommand encountered
			subArgs = args[i:]
			break
		}
	}

	if len(subArgs) == 0 {
		c.printUsage()
		return errors.New("no command specified")
	}

	cmd := subArgs[0]
	cmdArgs := subArgs[1:]

	var err error
	switch cmd {
	case "latency":
		err = c.runLatency(ctx, cmdArgs)
	case "drop":
		err = c.runDrop(ctx, cmdArgs)
	case "corrupt":
		err = c.runCorrupt(ctx, cmdArgs)
	case "throttle":
		err = c.runThrottle(ctx, cmdArgs)
	case "disconnect":
		err = c.runDisconnect(ctx, cmdArgs)
	case "reset":
		err = c.runReset(ctx, cmdArgs)
	case "status":
		err = c.runStatus(ctx, cmdArgs)
	case "tail":
		err = c.runTail(ctx, cmdArgs)
	default:
		c.printUsage()
		return fmt.Errorf("unknown command: %s", cmd)
	}

	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	return err
}

func (c *CLI) printUsage() {
	fmt.Fprintf(c.stderr, `chaosctl - Dynamic L4/L7 Fault Injection Operator CLI

Usage:
  chaosctl [global-flags] <command> [command-flags]

Commands:
  latency       Configure network latency and jitter distribution
  drop          Configure probabilistic or burst packet drops
  corrupt       Configure payload bit flipping or L7 token mutation
  throttle      Configure bandwidth rate limiting (Token Bucket)
  disconnect    Trigger socket termination (TCP RST tearing)
  reset         Clear all active faults (transparent pass-through)
  status        Display active faults, operational uptime, and tunnel metrics
  tail          Stream real-time packet event telemetry with ANSI highlights

Global Flags:
  --grpc string    Control plane gRPC endpoint (default: "localhost:9090")
  --json           Format command output as machine-readable JSON
  --no-color       Disable ANSI color escape formatting
`)
}

// --- Subcommand Implementations ---

func (c *CLI) runLatency(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("latency", flag.ContinueOnError)
	fs.SetOutput(c.stderr)

	baseStr := fs.String("base", "200ms", "Base latency delay (e.g. 200ms, 1s)")
	jitterStr := fs.String("jitter", "50ms", "Jitter variance (e.g. 50ms)")
	distStr := fs.String("dist", "uniform", "Distribution model: uniform or normal/gaussian")
	prob := fs.Float64("prob", 1.0, "Probability of applying latency (0.0 to 1.0)")
	dir := fs.String("dir", "ALL", "Direction: ALL, C2S (client->server), S2C (server->client)")
	jsonFlag := fs.Bool("json", false, "Output response in JSON format")
	noColorFlag := fs.Bool("no-color", false, "Disable ANSI color coding")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if *jsonFlag {
		c.jsonOut = true
	}
	if *noColorFlag {
		c.noColor = true
	}

	baseMs, err := parseDurationMs(*baseStr)
	if err != nil {
		return fmt.Errorf("invalid base latency: %w", err)
	}

	jitterMs, err := parseDurationMs(*jitterStr)
	if err != nil {
		return fmt.Errorf("invalid jitter: %w", err)
	}

	var dist chaosv1.LatencyDistribution
	switch strings.ToLower(*distStr) {
	case "normal", "gaussian":
		dist = chaosv1.LatencyDistribution_NORMAL
	default:
		dist = chaosv1.LatencyDistribution_UNIFORM
	}

	if err := c.Connect(ctx); err != nil {
		return err
	}

	req := &chaosv1.LatencyConfig{
		BaseMs:       baseMs,
		JitterMs:     jitterMs,
		Distribution: dist,
		Probability:  *prob,
		Direction:    strings.ToUpper(*dir),
	}

	resp, err := c.client.SetLatency(ctx, req)
	if err != nil {
		return fmt.Errorf("gRPC SetLatency failed: %w", err)
	}

	return c.printResponse(resp, map[string]any{
		"action":       "latency",
		"base_ms":      baseMs,
		"jitter_ms":    jitterMs,
		"distribution": dist.String(),
		"probability":  *prob,
		"direction":    strings.ToUpper(*dir),
	})
}

func (c *CLI) runDrop(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("drop", flag.ContinueOnError)
	fs.SetOutput(c.stderr)

	rate := fs.Float64("rate", 0.20, "Drop probability ratio (0.0 to 1.0)")
	burst := fs.Uint("burst", 1, "Consecutive packets to drop per burst")
	dir := fs.String("dir", "ALL", "Direction: ALL, C2S, S2C")
	jsonFlag := fs.Bool("json", false, "Output response in JSON format")
	noColorFlag := fs.Bool("no-color", false, "Disable ANSI color coding")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if *jsonFlag {
		c.jsonOut = true
	}
	if *noColorFlag {
		c.noColor = true
	}

	if err := c.Connect(ctx); err != nil {
		return err
	}

	req := &chaosv1.DropConfig{
		DropRatio:  *rate,
		BurstCount: uint32(*burst),
		Direction:  strings.ToUpper(*dir),
	}

	resp, err := c.client.SetPacketDrop(ctx, req)
	if err != nil {
		return fmt.Errorf("gRPC SetPacketDrop failed: %w", err)
	}

	return c.printResponse(resp, map[string]any{
		"action":      "drop",
		"drop_ratio":  *rate,
		"burst_count": *burst,
		"direction":   strings.ToUpper(*dir),
	})
}

func (c *CLI) runCorrupt(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("corrupt", flag.ContinueOnError)
	fs.SetOutput(c.stderr)

	rate := fs.Float64("rate", 0.05, "Probability of corruption per packet (0.0 to 1.0)")
	mode := fs.String("mode", "l4", "Corruption mode: l4 (raw bit flip) or l7 (Ultimo semantic fuzzer)")
	dir := fs.String("dir", "ALL", "Direction: ALL, C2S, S2C")
	jsonFlag := fs.Bool("json", false, "Output response in JSON format")
	noColorFlag := fs.Bool("no-color", false, "Disable ANSI color coding")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if *jsonFlag {
		c.jsonOut = true
	}
	if *noColorFlag {
		c.noColor = true
	}

	modeUpper := "L4_RAW"
	if strings.EqualFold(*mode, "l7") || strings.EqualFold(*mode, "l7_semantic") {
		modeUpper = "L7_SEMANTIC"
	}

	if err := c.Connect(ctx); err != nil {
		return err
	}

	req := &chaosv1.CorruptionConfig{
		BitFlipRate: *rate,
		Mode:        modeUpper,
		Direction:   strings.ToUpper(*dir),
	}

	resp, err := c.client.SetCorruption(ctx, req)
	if err != nil {
		return fmt.Errorf("gRPC SetCorruption failed: %w", err)
	}

	return c.printResponse(resp, map[string]any{
		"action":        "corrupt",
		"bit_flip_rate": *rate,
		"mode":          modeUpper,
		"direction":     strings.ToUpper(*dir),
	})
}

func (c *CLI) runThrottle(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("throttle", flag.ContinueOnError)
	fs.SetOutput(c.stderr)

	rateStr := fs.String("rate", "64kbps", "Bandwidth rate limit (e.g. 64kbps, 1mbps, 64000)")
	dir := fs.String("dir", "ALL", "Direction: ALL, C2S, S2C")
	jsonFlag := fs.Bool("json", false, "Output response in JSON format")
	noColorFlag := fs.Bool("no-color", false, "Disable ANSI color coding")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if *jsonFlag {
		c.jsonOut = true
	}
	if *noColorFlag {
		c.noColor = true
	}

	bytesPerSec, err := parseBandwidthBytes(*rateStr)
	if err != nil {
		return fmt.Errorf("invalid bandwidth rate: %w", err)
	}

	if err := c.Connect(ctx); err != nil {
		return err
	}

	req := &chaosv1.ThrottleConfig{
		BytesPerSecond: bytesPerSec,
		Direction:      strings.ToUpper(*dir),
	}

	resp, err := c.client.SetThrottle(ctx, req)
	if err != nil {
		return fmt.Errorf("gRPC SetThrottle failed: %w", err)
	}

	return c.printResponse(resp, map[string]any{
		"action":           "throttle",
		"bytes_per_second": bytesPerSec,
		"direction":        strings.ToUpper(*dir),
	})
}

func (c *CLI) runDisconnect(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("disconnect", flag.ContinueOnError)
	fs.SetOutput(c.stderr)

	immediate := fs.Bool("immediate", true, "Force immediate hardware socket teardown (TCP RST)")
	dir := fs.String("dir", "ALL", "Direction: ALL, C2S, S2C")
	jsonFlag := fs.Bool("json", false, "Output response in JSON format")
	noColorFlag := fs.Bool("no-color", false, "Disable ANSI color coding")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if *jsonFlag {
		c.jsonOut = true
	}
	if *noColorFlag {
		c.noColor = true
	}

	if err := c.Connect(ctx); err != nil {
		return err
	}

	req := &chaosv1.DisconnectCommand{
		Immediate: *immediate,
		Direction: strings.ToUpper(*dir),
	}

	resp, err := c.client.TriggerDisconnect(ctx, req)
	if err != nil {
		return fmt.Errorf("gRPC TriggerDisconnect failed: %w", err)
	}

	return c.printResponse(resp, map[string]any{
		"action":    "disconnect",
		"immediate": *immediate,
		"direction": strings.ToUpper(*dir),
	})
}

func (c *CLI) runReset(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("reset", flag.ContinueOnError)
	fs.SetOutput(c.stderr)

	jsonFlag := fs.Bool("json", false, "Output response in JSON format")
	noColorFlag := fs.Bool("no-color", false, "Disable ANSI color coding")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *jsonFlag {
		c.jsonOut = true
	}
	if *noColorFlag {
		c.noColor = true
	}

	if err := c.Connect(ctx); err != nil {
		return err
	}

	resp, err := c.client.ResetAllFaults(ctx, &chaosv1.Empty{})
	if err != nil {
		return fmt.Errorf("gRPC ResetAllFaults failed: %w", err)
	}

	return c.printResponse(resp, map[string]any{
		"action": "reset",
	})
}

func (c *CLI) runStatus(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	fs.SetOutput(c.stderr)

	jsonFlag := fs.Bool("json", false, "Output response in JSON format")
	noColorFlag := fs.Bool("no-color", false, "Disable ANSI color coding")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *jsonFlag {
		c.jsonOut = true
	}
	if *noColorFlag {
		c.noColor = true
	}

	if err := c.Connect(ctx); err != nil {
		return err
	}

	status, err := c.client.GetStatus(ctx, &chaosv1.Empty{})
	if err != nil {
		return fmt.Errorf("gRPC GetStatus failed: %w", err)
	}

	if c.jsonOut {
		enc := json.NewEncoder(c.stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(status)
	}

	// Human-readable formatted terminal table
	c.formatStatusTable(status)
	return nil
}

func (c *CLI) runTail(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("tail", flag.ContinueOnError)
	fs.SetOutput(c.stderr)

	dir := fs.String("dir", "ALL", "Filter by direction: ALL, C2S, S2C")
	actions := fs.String("action", "", "Filter by comma-separated actions (e.g. DROP,DELAY)")
	jsonFlag := fs.Bool("json", false, "Stream packet events as JSON lines")
	noColorFlag := fs.Bool("no-color", false, "Disable ANSI color coding")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if *jsonFlag {
		c.jsonOut = true
	}
	if *noColorFlag {
		c.noColor = true
	}

	if err := c.Connect(ctx); err != nil {
		return err
	}

	filter := &chaosv1.StreamFilter{
		Direction: strings.ToUpper(*dir),
	}
	if *actions != "" {
		for _, a := range strings.Split(*actions, ",") {
			filter.Actions = append(filter.Actions, strings.ToUpper(strings.TrimSpace(a)))
		}
	}

	stream, err := c.client.StreamEvents(ctx, filter)
	if err != nil {
		return fmt.Errorf("gRPC StreamEvents failed: %w", err)
	}

	if !c.jsonOut {
		fmt.Fprintf(c.stdout, "%sSubscribed to real-time packet event stream (Ctrl+C to stop)...%s\n", c.color(colorDim), c.color(colorReset))
		fmt.Fprintf(c.stdout, "%-12s %-10s %-5s %-10s %-8s %s\n", "TIME", "ACTION", "DIR", "TUNNEL", "BYTES", "DETAILS")
		fmt.Fprintf(c.stdout, "%s\n", strings.Repeat("-", 64))
	}

	for {
		event, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(ctx.Err(), context.Canceled) {
				return nil
			}
			return fmt.Errorf("event stream error: %w", err)
		}

		if c.jsonOut {
			b, _ := json.Marshal(event)
			fmt.Fprintf(c.stdout, "%s\n", string(b))
		} else {
			c.formatEventLine(event)
		}
	}
}

// --- Formatters & Helpers ---

func (c *CLI) color(code string) string {
	if c.noColor {
		return ""
	}
	return code
}

func (c *CLI) printResponse(resp *chaosv1.Response, metadata map[string]any) error {
	if c.jsonOut {
		out := map[string]any{
			"success":  resp.GetSuccess(),
			"message":  resp.GetMessage(),
			"metadata": metadata,
		}
		enc := json.NewEncoder(c.stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(out)
	}

	statusPrefix := fmt.Sprintf("%s[OK]%s", c.color(colorGreen), c.color(colorReset))
	if !resp.GetSuccess() {
		statusPrefix = fmt.Sprintf("%s[FAILED]%s", c.color(colorRed), c.color(colorReset))
	}

	fmt.Fprintf(c.stdout, "%s %s\n", statusPrefix, resp.GetMessage())
	return nil
}

func (c *CLI) formatStatusTable(s *chaosv1.StatusResponse) {
	fmt.Fprintf(c.stdout, "\n%s=== Go-Chaos-Proxy Operational Status ===%s\n", c.color(colorBold), c.color(colorReset))
	fmt.Fprintf(c.stdout, "  Uptime:                %s\n", formatDuration(time.Duration(s.GetUptimeSeconds())*time.Second))
	fmt.Fprintf(c.stdout, "  Active Tunnels:        %d\n", s.GetActiveTunnels())
	fmt.Fprintf(c.stdout, "  Total Packets:         %d\n", s.GetTotalPacketsProcessed())
	fmt.Fprintf(c.stdout, "  Total Data:            %s\n", formatBytes(s.GetTotalBytesTransferred()))
	fmt.Fprintf(c.stdout, "\n%s--- Active Fault Injection Rules ---%s\n", c.color(colorBold), c.color(colorReset))

	// Latency
	if lat := s.GetLatency(); lat != nil && (lat.GetBaseMs() > 0 || lat.GetJitterMs() > 0) {
		fmt.Fprintf(c.stdout, "  %sLatency:%s               Base=%dms, Jitter=±%dms, Dist=%s, Prob=%.2f, Dir=%s\n",
			c.color(colorYellow), c.color(colorReset),
			lat.GetBaseMs(), lat.GetJitterMs(), lat.GetDistribution(), lat.GetProbability(), lat.GetDirection())
	} else {
		fmt.Fprintf(c.stdout, "  Latency:               %sNone (0ms)%s\n", c.color(colorDim), c.color(colorReset))
	}

	// Packet Drop
	if drop := s.GetDrop(); drop != nil && drop.GetDropRatio() > 0 {
		fmt.Fprintf(c.stdout, "  %sPacket Drop:%s           Ratio=%.1f%%, BurstCount=%d, Dir=%s\n",
			c.color(colorRed), c.color(colorReset),
			drop.GetDropRatio()*100, drop.GetBurstCount(), drop.GetDirection())
	} else {
		fmt.Fprintf(c.stdout, "  Packet Drop:           %sNone (0%%)%s\n", c.color(colorDim), c.color(colorReset))
	}

	// Payload Corruption
	if cor := s.GetCorruption(); cor != nil && cor.GetBitFlipRate() > 0 {
		fmt.Fprintf(c.stdout, "  %sCorruption:%s            Rate=%.1f%%, Mode=%s, Dir=%s\n",
			c.color(colorMagenta), c.color(colorReset),
			cor.GetBitFlipRate()*100, cor.GetMode(), cor.GetDirection())
	} else {
		fmt.Fprintf(c.stdout, "  Corruption:            %sNone%s\n", c.color(colorDim), c.color(colorReset))
	}

	// Bandwidth Throttle
	if thr := s.GetThrottle(); thr != nil && thr.GetBytesPerSecond() > 0 {
		fmt.Fprintf(c.stdout, "  %sBandwidth Throttle:%s    Rate=%s/s, Dir=%s\n",
			c.color(colorCyan), c.color(colorReset),
			formatBytes(thr.GetBytesPerSecond()), thr.GetDirection())
	} else {
		fmt.Fprintf(c.stdout, "  Bandwidth Throttle:    %sUnlimited%s\n", c.color(colorDim), c.color(colorReset))
	}
	fmt.Fprintln(c.stdout)
}

func (c *CLI) formatEventLine(event *chaosv1.PacketEvent) {
	t := time.Unix(0, event.GetTimestampNs()).Format("15:04:05.000")
	action := event.GetActionTaken()

	var actionColored string
	switch action {
	case "PASSED":
		actionColored = fmt.Sprintf("%s[PASS]%s    ", c.color(colorGreen), c.color(colorReset))
	case "DELAYED":
		actionColored = fmt.Sprintf("%s[DELAY]%s   ", c.color(colorYellow), c.color(colorReset))
	case "DROPPED":
		actionColored = fmt.Sprintf("%s[DROP]%s    ", c.color(colorRed), c.color(colorReset))
	case "CORRUPTED":
		actionColored = fmt.Sprintf("%s[CORRUPT]%s ", c.color(colorMagenta), c.color(colorReset))
	case "THROTTLED":
		actionColored = fmt.Sprintf("%s[THROTTLE]%s", c.color(colorCyan), c.color(colorReset))
	default:
		actionColored = fmt.Sprintf("%-10s", action)
	}

	details := event.GetSummary()
	if action == "DELAYED" && event.GetDelayAppliedMs() > 0 {
		details = fmt.Sprintf("+%dms | %s", event.GetDelayAppliedMs(), details)
	}

	fmt.Fprintf(c.stdout, "%-12s %s %-5s %-10s %-8d %s\n",
		t, actionColored, event.GetDirection(), event.GetTunnelId(), event.GetByteCount(), details)
}

func parseDurationMs(s string) (uint32, error) {
	s = strings.TrimSpace(s)
	if d, err := time.ParseDuration(s); err == nil {
		return uint32(d.Milliseconds()), nil
	}
	// Fallback to integer string
	val, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("unable to parse duration '%s': %w", s, err)
	}
	return uint32(val), nil
}

func parseBandwidthBytes(s string) (uint64, error) {
	s = strings.TrimSpace(strings.ToLower(s))
	if strings.HasSuffix(s, "kbps") {
		numStr := strings.TrimSuffix(s, "kbps")
		n, err := strconv.ParseFloat(numStr, 64)
		if err != nil {
			return 0, err
		}
		// 1 kbps = 1000 bits/sec / 8 = 125 bytes/sec
		return uint64(n * 1000 / 8), nil
	}
	if strings.HasSuffix(s, "mbps") {
		numStr := strings.TrimSuffix(s, "mbps")
		n, err := strconv.ParseFloat(numStr, 64)
		if err != nil {
			return 0, err
		}
		// 1 mbps = 1,000,000 bits/sec / 8 = 125,000 bytes/sec
		return uint64(n * 1000000 / 8), nil
	}
	if strings.HasSuffix(s, "kb") || strings.HasSuffix(s, "k") {
		numStr := strings.TrimRight(s, "kb")
		n, err := strconv.ParseUint(numStr, 10, 64)
		if err != nil {
			return 0, err
		}
		return n * 1024, nil
	}
	if strings.HasSuffix(s, "mb") || strings.HasSuffix(s, "m") {
		numStr := strings.TrimRight(s, "mb")
		n, err := strconv.ParseUint(numStr, 10, 64)
		if err != nil {
			return 0, err
		}
		return n * 1024 * 1024, nil
	}
	return strconv.ParseUint(s, 10, 64)
}

func formatBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

func formatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second
	return fmt.Sprintf("%02dh:%02dm:%02ds", h, m, s)
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cli := NewCLI(os.Stdout, os.Stderr)
	defer cli.Close()

	if err := cli.Run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
