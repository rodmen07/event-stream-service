package main

import (
	"sync"
	"time"
)

const ringSize = 50

// Hub manages SSE subscribers and broadcasts events to all of them.
// It keeps a fixed-size ring buffer so late joiners receive recent history.
type Hub struct {
	mu          sync.RWMutex
	subscribers map[string]chan Event
	ring        []Event
}

func NewHub() *Hub {
	return &Hub{
		subscribers: make(map[string]chan Event),
		ring:        make([]Event, 0, ringSize),
	}
}

// Subscribe registers a new subscriber and returns its ID, its receive channel,
// and a snapshot of the current ring buffer for replay.
func (h *Hub) Subscribe(id string) (chan Event, []Event) {
	h.mu.Lock()
	defer h.mu.Unlock()

	ch := make(chan Event, 32)
	h.subscribers[id] = ch

	replay := make([]Event, len(h.ring))
	copy(replay, h.ring)

	return ch, replay
}

// Unsubscribe removes a subscriber and closes its channel.
func (h *Hub) Unsubscribe(id string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if ch, ok := h.subscribers[id]; ok {
		close(ch)
		delete(h.subscribers, id)
	}
}

// Publish adds an event to the ring buffer and fans it out to all subscribers.
// Slow subscribers are dropped rather than blocked.
func (h *Hub) Publish(e Event) {
	if e.Timestamp == "" {
		e.Timestamp = time.Now().UTC().Format(time.RFC3339)
	}

	h.mu.Lock()
	if len(h.ring) < ringSize {
		h.ring = append(h.ring, e)
	} else {
		// Overwrite oldest: shift left and append.
		copy(h.ring, h.ring[1:])
		h.ring[ringSize-1] = e
	}
	subs := make([]chan Event, 0, len(h.subscribers))
	for _, ch := range h.subscribers {
		subs = append(subs, ch)
	}
	h.mu.Unlock()

	for _, ch := range subs {
		select {
		case ch <- e:
		default:
			// subscriber too slow — drop rather than block
		}
	}
}

// ClientCount returns the number of active subscribers.
func (h *Hub) ClientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subscribers)
}
