package store_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/HelixObs/gateway/internal/store"
)

func ref(traceID, spanID byte) *store.SpanRef {
	return &store.SpanRef{
		TraceID: []byte{traceID},
		SpanID:  []byte{spanID},
	}
}

func TestPutAndGet(t *testing.T) {
	s := store.New(10, nil)
	s.Put("entity-1", ref(1, 1))
	got := s.Get("entity-1")
	if got == nil {
		t.Fatal("expected non-nil SpanRef")
	}
	if got.TraceID[0] != 1 {
		t.Errorf("TraceID mismatch: got %v", got.TraceID)
	}
}

func TestGetMissingReturnsNil(t *testing.T) {
	s := store.New(10, nil)
	if s.Get("nonexistent") != nil {
		t.Fatal("expected nil for missing key")
	}
}

func TestOverwrite(t *testing.T) {
	s := store.New(10, nil)
	s.Put("entity-1", ref(1, 1))
	s.Put("entity-1", ref(2, 2))
	got := s.Get("entity-1")
	if got.TraceID[0] != 2 {
		t.Errorf("expected overwritten value, got TraceID %v", got.TraceID)
	}
}

func TestMaxSizeEvictsOldest(t *testing.T) {
	s := store.New(3, nil)
	for i := 0; i < 3; i++ {
		s.Put(fmt.Sprintf("entity-%d", i), ref(byte(i), byte(i)))
	}
	// Adding a 4th should evict entity-0.
	s.Put("entity-3", ref(3, 3))
	if s.Get("entity-0") != nil {
		t.Error("entity-0 should have been evicted")
	}
	if s.Get("entity-3") == nil {
		t.Error("entity-3 should be present")
	}
}

func TestMaxSizeRetainsMostRecent(t *testing.T) {
	s := store.New(3, nil)
	for i := 0; i < 5; i++ {
		s.Put(fmt.Sprintf("entity-%d", i), ref(byte(i), byte(i)))
	}
	for i := 2; i < 5; i++ {
		if s.Get(fmt.Sprintf("entity-%d", i)) == nil {
			t.Errorf("entity-%d should be present", i)
		}
	}
}

func TestConcurrentPuts(t *testing.T) {
	s := store.New(1_000, nil)
	var wg sync.WaitGroup
	for g := 0; g < 10; g++ {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				s.Put(fmt.Sprintf("entity-%d-%d", g, i), ref(byte(g), byte(i)))
			}
		}()
	}
	wg.Wait()
}

func TestConcurrentPutsAndGets(t *testing.T) {
	s := store.New(1_000, nil)
	s.Put("shared", ref(99, 99))
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				s.Get("shared")
			}
		}()
	}
	for i := 0; i < 5; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				s.Put(fmt.Sprintf("new-%d-%d", i, j), ref(byte(i), byte(j)))
			}
		}()
	}
	wg.Wait()
}
