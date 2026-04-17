// Package store holds the server-side in-process trace store.
//
// The gateway maps entity_id → (traceID, spanID) so it can resolve
// helix.parent.ids into OTel span links when child spans arrive.
// Cross-process parents whose child arrives before the parent is known
// are not linked (a metric is incremented instead); the helix.parent.ids
// attribute remains on the span for Grafana provenance queries.
package store

import "sync"

// SpanRef is the minimal context needed to construct an OTel span link.
// TraceID is 16 bytes; SpanID is 8 bytes — matching the OTLP proto wire format.
type SpanRef struct {
	TraceID []byte
	SpanID  []byte
}

// TraceStore is a thread-safe bounded FIFO map: entity_id → SpanRef.
// When full the oldest entry is evicted to make room for the new one.
type TraceStore struct {
	mu      sync.Mutex
	entries map[string]*SpanRef
	order   []string // insertion order for FIFO eviction
	maxSize int
}

func New(maxSize int) *TraceStore {
	return &TraceStore{
		entries: make(map[string]*SpanRef, maxSize),
		maxSize: maxSize,
	}
}

func (s *TraceStore) Put(entityID string, ref *SpanRef) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.entries[entityID]; !exists {
		if len(s.entries) >= s.maxSize {
			oldest := s.order[0]
			s.order = s.order[1:]
			delete(s.entries, oldest)
		}
		s.order = append(s.order, entityID)
	}
	s.entries[entityID] = ref
}

func (s *TraceStore) Get(entityID string) *SpanRef {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.entries[entityID]
}
