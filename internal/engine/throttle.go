package engine

import (
	"sync"
	"time"
)

// TokenBucket implements a thread-safe token bucket rate limiter for bandwidth throttling.
type TokenBucket struct {
	mu           sync.Mutex
	rate         float64 // Tokens per second (bytes/sec)
	capacity     float64 // Maximum burst bucket capacity in bytes
	tokens       float64
	lastRefilled time.Time
}

// NewTokenBucket creates a new token bucket with rate bytes per second.
func NewTokenBucket(bytesPerSec uint64) *TokenBucket {
	rate := float64(bytesPerSec)
	capacity := rate // Allow 1-second burst capacity by default
	if capacity < 32768 {
		capacity = 32768
	}

	return &TokenBucket{
		rate:         rate,
		capacity:     capacity,
		tokens:       capacity,
		lastRefilled: time.Now(),
	}
}

// Take attempts to consume n bytes. If tokens are insufficient, returns duration to wait.
func (tb *TokenBucket) Take(n int) time.Duration {
	if tb == nil || tb.rate <= 0 {
		return 0
	}

	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(tb.lastRefilled).Seconds()
	tb.lastRefilled = now

	// Replenish tokens
	tb.tokens += elapsed * tb.rate
	if tb.tokens > tb.capacity {
		tb.tokens = tb.capacity
	}

	requested := float64(n)
	if tb.tokens >= requested {
		tb.tokens -= requested
		return 0
	}

	// Calculate delay required to generate deficit tokens
	deficit := requested - tb.tokens
	tb.tokens = 0
	waitSec := deficit / tb.rate

	return time.Duration(waitSec * float64(time.Second))
}

// UpdateRate dynamically alters the bandwidth rate limit.
func (tb *TokenBucket) UpdateRate(bytesPerSec uint64) {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.rate = float64(bytesPerSec)
	tb.capacity = tb.rate
	if tb.capacity < 32768 {
		tb.capacity = 32768
	}
	tb.tokens = tb.capacity
	tb.lastRefilled = time.Now()
}
