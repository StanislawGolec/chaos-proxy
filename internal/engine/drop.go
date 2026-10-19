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
// If rule.TargetVerb is set, the drop probability is only applied when the payload matches that L7 verb.
func (dc *DropController) ShouldDrop(dir Direction, rule *DropRule, payload ...[]byte) bool {
	if len(payload) > 0 {
		return dc.ShouldDropPayload(dir, rule, payload[0])
	}
	return dc.ShouldDropPayload(dir, rule, nil)
}

// ShouldDropPayload evaluates drop eligibility for a packet payload without variadic slice overhead.
func (dc *DropController) ShouldDropPayload(dir Direction, rule *DropRule, payload []byte) bool {
	if rule == nil {
		return false
	}

	if rule.Direction != DirectionAll && rule.Direction != dir {
		return false
	}

	if rule.TargetVerb != "" && !MatchesTargetVerb(payload, rule.TargetVerb) {
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

	if rule.DropRatio <= 0 {
		return false
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

// MatchesTargetVerb checks whether payload parses into or contains the specified L7 protocol verb
// at token boundaries using zero heap allocations.
func MatchesTargetVerb(payload []byte, targetVerb string) bool {
	tStart, tEnd := 0, len(targetVerb)
	for tStart < tEnd && isASCIIWhitespace(targetVerb[tStart]) {
		tStart++
	}
	for tEnd > tStart && isASCIIWhitespace(targetVerb[tEnd-1]) {
		tEnd--
	}
	target := targetVerb[tStart:tEnd]
	if len(target) == 0 {
		return true
	}
	if len(payload) == 0 {
		return false
	}

	if containsVerbToken(payload, target) {
		return true
	}

	// Check RFC 6455 masked WebSocket text frame (Opcode 0x1) in-place without allocating
	if len(payload) >= 6 && (payload[0]&0x0F) == 0x01 && (payload[1]&0x80) != 0 {
		pLen := int(payload[1] & 0x7F)
		hdrLen := 2
		switch pLen {
		case 126:
			if len(payload) < 8 {
				return false
			}
			pLen = int(payload[2])<<8 | int(payload[3])
			hdrLen = 4
		case 127:
			if len(payload) < 14 {
				return false
			}
			pLen = int(payload[6])<<24 | int(payload[7])<<16 | int(payload[8])<<8 | int(payload[9])
			hdrLen = 10
		}
		maskKey := payload[hdrLen : hdrLen+4]
		hdrLen += 4
		if pLen > 0 && len(payload) >= hdrLen+pLen {
			if containsMaskedVerbToken(payload[hdrLen:hdrLen+pLen], maskKey, target) {
				return true
			}
		}
	}

	return false
}

func containsVerbToken(data []byte, target string) bool {
	tLen := len(target)
	n := len(data)
	if n < tLen {
		return false
	}

	limit := n - tLen
	for i := 0; i <= limit; i++ {
		if i > 0 && isVerbChar(data[i-1]) {
			continue
		}
		if i+tLen < n && isVerbChar(data[i+tLen]) {
			continue
		}
		if equalFoldASCII(data[i:i+tLen], target) {
			return true
		}
	}
	return false
}

func containsMaskedVerbToken(maskedData []byte, maskKey []byte, target string) bool {
	tLen := len(target)
	n := len(maskedData)
	if n < tLen {
		return false
	}

	limit := n - tLen
	for i := 0; i <= limit; i++ {
		if i > 0 && isVerbChar(maskedData[i-1]^maskKey[(i-1)&3]) {
			continue
		}
		if i+tLen < n && isVerbChar(maskedData[i+tLen]^maskKey[(i+tLen)&3]) {
			continue
		}
		matched := true
		for j := 0; j < tLen; j++ {
			b := maskedData[i+j] ^ maskKey[(i+j)&3]
			if toUpperASCII(b) != toUpperASCII(target[j]) {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

func equalFoldASCII(b []byte, s string) bool {
	if len(b) != len(s) {
		return false
	}
	for i := 0; i < len(b); i++ {
		if toUpperASCII(b[i]) != toUpperASCII(s[i]) {
			return false
		}
	}
	return true
}

func toUpperASCII(b byte) byte {
	if b >= 'a' && b <= 'z' {
		return b - ('a' - 'A')
	}
	return b
}

func isVerbChar(b byte) bool {
	return (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9') || b == '_'
}

func isASCIIWhitespace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\r' || b == '\n'
}

// Reset clears any active burst drop states.
func (dc *DropController) Reset() {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	dc.burstRemainC2S = 0
	dc.burstRemainS2C = 0
}
