package controlplane

import (
	"strings"
	"sync"

	chaosv1 "github.com/StanislawGolec/Go-Chaos-Proxy/pkg/proto/v1"
)

type subscriber struct {
	id     uint64
	ch     chan *chaosv1.PacketEvent
	filter *chaosv1.StreamFilter
}

// Broadcaster manages non-blocking fan-out of packet events to concurrent gRPC streaming subscribers.
type Broadcaster struct {
	mu     sync.RWMutex
	subs   map[uint64]*subscriber
	subSeq uint64
}

// NewBroadcaster constructs an empty event broadcaster.
func NewBroadcaster() *Broadcaster {
	return &Broadcaster{
		subs: make(map[uint64]*subscriber),
	}
}

// Subscribe registers a new subscriber channel with an optional filter, returning the event channel and cancel func.
func (b *Broadcaster) Subscribe(filter *chaosv1.StreamFilter) (<-chan *chaosv1.PacketEvent, func()) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.subSeq++
	id := b.subSeq

	sub := &subscriber{
		id:     id,
		ch:     make(chan *chaosv1.PacketEvent, 128), // Buffered channel for resilience against brief bursts
		filter: filter,
	}

	b.subs[id] = sub

	cancel := func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if s, ok := b.subs[id]; ok {
			delete(b.subs, id)
			close(s.ch)
		}
	}

	return sub.ch, cancel
}

// Broadcast distributes a PacketEvent to all eligible subscribers without blocking.
func (b *Broadcaster) Broadcast(event *chaosv1.PacketEvent) {
	if b == nil || event == nil {
		return
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, sub := range b.subs {
		if sub.filter != nil {
			// Direction filter
			reqDir := strings.ToUpper(sub.filter.Direction)
			if reqDir != "" && reqDir != "ALL" && reqDir != strings.ToUpper(event.Direction) {
				continue
			}

			// Actions filter
			if len(sub.filter.Actions) > 0 {
				match := false
				for _, act := range sub.filter.Actions {
					if strings.EqualFold(act, event.ActionTaken) {
						match = true
						break
					}
				}
				if !match {
					continue
				}
			}
		}

		// Non-blocking transmission ensures slow subscribers never block the proxy forwarding loop
		select {
		case sub.ch <- event:
		default:
		}
	}
}

// ActiveSubscribers returns the count of active event streams.
func (b *Broadcaster) ActiveSubscribers() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subs)
}
