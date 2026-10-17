package engine

import (
	"math/rand/v2"
	"sync"
)

// DropController tracks probabilistic and burst packet drops thread-safely.
type DropController struct {
	mu            sync.Mutex
	burstRemainC2S uint32
	burstRemainS2C uint32
}

// NewDropController initializes a new DropController.
func NewDropController() *DropController {
	return &DropController{}
}

// ShouldDrop determines if an incoming packet should be discarded according to DropRule.
func (dc *DropController) ShouldDrop(dir Direction, rule *DropRule) bool {
	if rule == nil || rule.DropRatio <= 0 {
		return false
	}

	if rule.Direction != DirectionAll && rule.Direction != dir {
		return false
	}

	dc.mu.Lock()
	defer dc.mu.Unlock()

	// Check if active burst drop is in progress for this direction
	if dir == DirectionC2S && dc.burstRemainC2S > 0 {
		dc.burstRemainC2S--
		return true
	}
	if dir == DirectionS2C && dc.burstRemainS2C > 0 {
		dc.burstRemainS2C--
		return true
	}

	// Probabilistic roll
	if rand.Float64() < rule.DropRatio {
		burst := rule.BurstCount
		if burst > 1 {
			// Trigger multi-packet burst drop
			if dir == DirectionC2S {
				dc.burstRemainC2S = burst - 1
			} else {
				dc.burstRemainS2C = burst - 1
			}
		}
		return true
	}

	return false
}

// Reset clears any active burst drop states.
func (dc *DropController) Reset() {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	dc.burstRemainC2S = 0
	dc.burstRemainS2C = 0
}
