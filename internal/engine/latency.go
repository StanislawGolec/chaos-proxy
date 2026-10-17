package engine

import (
	"math/rand/v2"
	"time"
)

// CalculateDelay computes the delay duration for a packet based on LatencyRule.
func CalculateDelay(rule *LatencyRule) time.Duration {
	if rule == nil || (rule.BaseLatency <= 0 && rule.Jitter <= 0) {
		return 0
	}

	// Probability check
	if rule.Probability > 0 && rule.Probability < 1.0 {
		if rand.Float64() >= rule.Probability {
			return 0
		}
	}

	base := rule.BaseLatency
	if rule.Jitter <= 0 {
		return base
	}

	var jitterOffset time.Duration
	if rule.IsGaussian {
		// Normal/Gaussian distribution: mean=0, stdDev=Jitter/2
		stdDev := float64(rule.Jitter) / 2.0
		val := rand.NormFloat64() * stdDev
		jitterOffset = time.Duration(val)
	} else {
		// Uniform distribution between -Jitter and +Jitter
		jitterRange := int64(rule.Jitter * 2)
		val := rand.Int64N(jitterRange) - int64(rule.Jitter)
		jitterOffset = time.Duration(val)
	}

	delay := base + jitterOffset
	if delay < 0 {
		return 0
	}
	return delay
}
