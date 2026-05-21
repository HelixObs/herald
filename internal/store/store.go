// Package store holds the server-side in-process trace store.
//
// The herald maps entity_id → (traceID, spanID) so it can resolve
// helix.parent.ids into OTel span links when child spans arrive.
// Cross-process parents whose child arrives before the parent is known
// are not linked (a metric is incremented instead); the helix.parent.ids
// attribute remains on the span for Grafana provenance queries.
package store

import (
	"sync"
	"time"
)

// SpanRef is the minimal context needed to construct an OTel span link.
// TraceID is 16 bytes; SpanID is 8 bytes — matching the OTLP proto wire format.
type SpanRef struct {
	TraceID []byte
	SpanID  []byte
}

// storeMetrics is the subset of herald metrics used by the trace store.
// Using an interface keeps the store package free of an import cycle.
type storeMetrics interface {
	TraceStoreHit()
	TraceStoreMiss()
	TraceStoreEviction()
	TraceStoreSetSize(n int)
	RecordTraceStoreLookup(d time.Duration)
}

// TraceStore is a thread-safe bounded FIFO map: entity_id → SpanRef.
// When full the oldest entry is evicted to make room for the new one.
type TraceStore struct {
	mu      sync.Mutex
	entries map[string]*SpanRef
	order   []string // insertion order for FIFO eviction
	maxSize int
	m       storeMetrics // nil in tests
}

func New(maxSize int, m storeMetrics) *TraceStore {
	return &TraceStore{
		entries: make(map[string]*SpanRef, maxSize),
		maxSize: maxSize,
		m:       m,
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
			if s.m != nil {
				s.m.TraceStoreEviction()
			}
		}
		s.order = append(s.order, entityID)
	}
	s.entries[entityID] = ref

	if s.m != nil {
		s.m.TraceStoreSetSize(len(s.entries))
	}
}

func (s *TraceStore) Get(entityID string) *SpanRef {
	start := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	ref := s.entries[entityID]
	if s.m != nil {
		if ref != nil {
			s.m.TraceStoreHit()
		} else {
			s.m.TraceStoreMiss()
		}
		s.m.RecordTraceStoreLookup(time.Since(start))
	}
	return ref
}
