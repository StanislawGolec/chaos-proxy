package dataplane

import (
	"sync"
)

// BufferPool manages reusable byte slices via sync.Pool to eliminate GC heap thrashing.
type BufferPool struct {
	pool sync.Pool
	size int
}

// NewBufferPool initializes a pool returning pre-allocated byte slices of fixed size.
func NewBufferPool(size int) *BufferPool {
	if size <= 0 {
		size = 32 * 1024 // 32KB default
	}

	return &BufferPool{
		size: size,
		pool: sync.Pool{
			New: func() any {
				buf := make([]byte, size)
				return &buf
			},
		},
	}
}

// Get retrieves a pre-allocated byte slice of the configured size from the pool.
func (bp *BufferPool) Get() []byte {
	bufPtr := bp.pool.Get().(*[]byte)
	return (*bufPtr)[:bp.size]
}

// Put returns a byte slice to the pool if its capacity matches the pool size.
func (bp *BufferPool) Put(b []byte) {
	if cap(b) < bp.size {
		return
	}
	b = b[:bp.size]
	bp.pool.Put(&b)
}

// BufferSize returns the fixed slice size in bytes.
func (bp *BufferPool) BufferSize() int {
	return bp.size
}
