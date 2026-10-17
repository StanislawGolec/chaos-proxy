package engine

import (
	"sync/atomic"
	"time"
)

// Direction represents network traffic flow direction.
type Direction string

const (
	DirectionAll Direction = "ALL"
	DirectionC2S Direction = "C2S" // Client to Server
	DirectionS2C Direction = "S2C" // Server to Client
)

// ActionType defines the verdict applied by the fault engine.
type ActionType string

const (
	ActionPass       ActionType = "PASSED"
	ActionDelay      ActionType = "DELAYED"
	ActionDrop       ActionType = "DROPPED"
	ActionCorrupt    ActionType = "CORRUPTED"
	ActionDisconnect ActionType = "DISCONNECT"
)

// Action encapsulates the fault verdict and any parameters (e.g. delay duration).
type Action struct {
	Type     ActionType
	Delay    time.Duration
	Mutation func([]byte) []byte
	Reason   string
}

// LatencyRule configures simulated delay and jitter.
type LatencyRule struct {
	BaseLatency  time.Duration
	Jitter       time.Duration
	Probability  float64 // 0.0 to 1.0
	IsGaussian   bool
	Direction    Direction
}

// DropRule configures probabilistic or burst packet drops.
type DropRule struct {
	DropRatio  float64 // 0.0 to 1.0
	BurstCount uint32
	Direction  Direction
}

// CorruptionRule configures random bit flipping or token mutations.
type CorruptionRule struct {
	BitFlipRate float64 // 0.0 to 1.0
	Mode        string  // "L4_RAW" or "L7_SEMANTIC"
	Direction   Direction
}

// ThrottleRule configures bandwidth rate limiting.
type ThrottleRule struct {
	BytesPerSecond uint64
	Direction      Direction
}

// DisconnectRule forces socket termination.
type DisconnectRule struct {
	TCPReset  bool
	Immediate bool
	Direction Direction
}

// RuleSet aggregates all active fault rules in an immutable snapshot.
type RuleSet struct {
	Latency    *LatencyRule
	Drop       *DropRule
	Corruption *CorruptionRule
	Throttle   *ThrottleRule
	Disconnect *DisconnectRule
}

// RuleContainer holds an atomic pointer to RuleSet, enabling lock-free reads on the data plane hot path.
type RuleContainer struct {
	current atomic.Pointer[RuleSet]
}

// NewRuleContainer initializes an empty RuleContainer.
func NewRuleContainer() *RuleContainer {
	c := &RuleContainer{}
	c.current.Store(&RuleSet{})
	return c
}

// Load atomically fetches the current active RuleSet.
func (c *RuleContainer) Load() *RuleSet {
	return c.current.Load()
}

// Store atomically replaces the active RuleSet with a new snapshot.
func (c *RuleContainer) Store(rules *RuleSet) {
	if rules == nil {
		rules = &RuleSet{}
	}
	c.current.Store(rules)
}

// Reset clears all active fault rules, returning the proxy to transparent pass-through.
func (c *RuleContainer) Reset() {
	c.Store(&RuleSet{})
}
