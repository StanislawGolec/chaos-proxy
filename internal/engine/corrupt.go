package engine

import (
	"bytes"
	"math/rand/v2"
	"strings"
)

// ShouldCorrupt determines whether a payload should undergo mutation.
func ShouldCorrupt(rule *CorruptionRule) bool {
	if rule == nil || rule.BitFlipRate <= 0 {
		return false
	}
	return rand.Float64() < rule.BitFlipRate
}

// MutatePayload applies random bit flips (L4) or semantic token corruption (L7).
func MutatePayload(payload []byte, rule *CorruptionRule) []byte {
	if len(payload) == 0 || rule == nil {
		return payload
	}

	result := make([]byte, len(payload))
	copy(result, payload)

	if strings.EqualFold(rule.Mode, "L7_SEMANTIC") {
		return mutateL7(result)
	}

	return mutateL4(result)
}

// mutateL4 flips 1 to 3 random bits in a randomly chosen byte offset.
func mutateL4(b []byte) []byte {
	idx := rand.IntN(len(b))
	numFlips := rand.IntN(3) + 1
	for i := 0; i < numFlips; i++ {
		bit := rand.IntN(8)
		b[idx] ^= (1 << bit)
	}
	return b
}

// mutateL7 selectively fuzzes card tokens or protocol verbs.
func mutateL7(b []byte) []byte {
	content := string(b)

	// Fuzz card tokens
	replacements := []struct {
		target string
		fuzzed string
	}{
		{"RED", "PURPLE"},
		{"BLUE", "CYAN"},
		{"GREEN", "ORANGE"},
		{"YELLOW", "MAGENTA"},
		{"PLAY", "PLXX"},
		{"JOIN", "JOXX"},
		{"DRAW", "DRXX"},
		{"SKIP", "SKXX"},
		{"REVERSE", "RVXX"},
	}

	for _, r := range replacements {
		if strings.Contains(content, r.target) {
			content = strings.Replace(content, r.target, r.fuzzed, 1)
			return []byte(content)
		}
	}

	// Fallback to L4 bit flip if no known text tokens are found
	return mutateL4(b)
}

// InvertBits inverts every bit in a single byte offset.
func InvertBits(b []byte, offset int) {
	if offset >= 0 && offset < len(b) {
		b[offset] ^= 0xFF
	}
}

// IsCorrupted compares original and mutated bytes.
func IsCorrupted(original, mutated []byte) bool {
	return !bytes.Equal(original, mutated)
}
