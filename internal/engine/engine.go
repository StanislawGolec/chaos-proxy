package engine

import (
	"time"
)

// Engine orchestrates the chaos fault injection pipeline across all network directions.
type Engine struct {
	rules      *RuleContainer
	dropCtrl   *DropController
	throttleC2 *TokenBucket
	throttleS2 *TokenBucket
}

// NewEngine creates a new Engine wrapping a RuleContainer.
func NewEngine(rules *RuleContainer) *Engine {
	if rules == nil {
		rules = NewRuleContainer()
	}

	return &Engine{
		rules:    rules,
		dropCtrl: NewDropController(),
	}
}

var defaultActionPass = &Action{
	Type:   ActionPass,
	Reason: "passed",
}

// Process evaluates all active fault interceptors sequentially for an in-flight packet.
func (e *Engine) Process(dir Direction, payload []byte) *Action {
	if e == nil || e.rules == nil {
		return defaultActionPass
	}

	rules := e.rules.Load()
	if rules == nil {
		return defaultActionPass
	}

	// 1. Connection Disconnect Interceptor
	if rules.Disconnect != nil {
		if rules.Disconnect.Direction == DirectionAll || rules.Disconnect.Direction == dir {
			return &Action{
				Type:   ActionDisconnect,
				Reason: "disconnect_command_triggered",
			}
		}
	}

	// 2. Packet Drop & Burst Interceptor
	if rules.Drop != nil {
		if e.dropCtrl.ShouldDrop(dir, rules.Drop) {
			return &Action{
				Type:   ActionDrop,
				Reason: "packet_dropped",
			}
		}
	}

	// 3. Payload Corruption / Fuzzing Interceptor
	if rules.Corruption != nil {
		if rules.Corruption.Direction == DirectionAll || rules.Corruption.Direction == dir {
			if ShouldCorrupt(rules.Corruption) {
				return &Action{
					Type: ActionCorrupt,
					Mutation: func(b []byte) []byte {
						return MutatePayload(b, rules.Corruption)
					},
					Reason: "payload_corrupted",
				}
			}
		}
	}

	// 4. Latency & Jitter Delay Interceptor
	var totalDelay time.Duration
	if rules.Latency != nil {
		if rules.Latency.Direction == DirectionAll || rules.Latency.Direction == dir {
			delay := CalculateDelay(rules.Latency)
			if delay > 0 {
				totalDelay += delay
			}
		}
	}

	// 5. Bandwidth Throttling Interceptor
	if rules.Throttle != nil && rules.Throttle.BytesPerSecond > 0 {
		if rules.Throttle.Direction == DirectionAll || rules.Throttle.Direction == dir {
			tb := e.getOrCreateBucket(dir, rules.Throttle.BytesPerSecond)
			throttleDelay := tb.Take(len(payload))
			if throttleDelay > 0 {
				totalDelay += throttleDelay
			}
		}
	}

	if totalDelay > 0 {
		return &Action{
			Type:   ActionDelay,
			Delay:  totalDelay,
			Reason: "delayed_or_throttled",
		}
	}

	return defaultActionPass
}

// getOrCreateBucket retrieves or initializes the direction's token bucket.
func (e *Engine) getOrCreateBucket(dir Direction, bytesPerSec uint64) *TokenBucket {
	if dir == DirectionC2S {
		if e.throttleC2 == nil {
			e.throttleC2 = NewTokenBucket(bytesPerSec)
		} else {
			e.throttleC2.UpdateRate(bytesPerSec)
		}
		return e.throttleC2
	}

	if e.throttleS2 == nil {
		e.throttleS2 = NewTokenBucket(bytesPerSec)
	} else {
		e.throttleS2.UpdateRate(bytesPerSec)
	}
	return e.throttleS2
}

// Reset clears stateful drop burst counts and throttle buckets.
func (e *Engine) Reset() {
	if e.dropCtrl != nil {
		e.dropCtrl.Reset()
	}
	e.throttleC2 = nil
	e.throttleS2 = nil
	if e.rules != nil {
		e.rules.Reset()
	}
}
