// Package silence checks whether a notification event is suppressed by an active rule.
package silence

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/HelixObs/gateway/internal/db"
)

// Store caches active silence rules per instrument with a short TTL to avoid
// querying the DB on every notification event.
type Store struct {
	dbStore  silenceDB
	mu       sync.RWMutex
	cache    map[string]cachedSilences // instrument_id → cached result
	cacheTTL time.Duration
}

type cachedSilences struct {
	silences  []db.Silence
	fetchedAt time.Time
}

type silenceDB interface {
	ActiveSilences(ctx context.Context, instrumentID string) ([]db.Silence, error)
}

// New creates a silence Store. cacheTTL controls how often the DB is re-queried
// per instrument. 60 seconds is a sensible default.
func New(d silenceDB, cacheTTL time.Duration) *Store {
	return &Store{dbStore: d, cache: make(map[string]cachedSilences), cacheTTL: cacheTTL}
}

// IsSilenced returns true if any active silence rule matches the given event.
// Scope resolution: instrument-wide > event-type > specific fingerprint.
func (s *Store) IsSilenced(ctx context.Context, instrumentID, eventType, fingerprint string) bool {
	silences := s.get(ctx, instrumentID)
	for _, sl := range silences {
		if sl.EventType == "" {
			return true // instrument-wide silence
		}
		if sl.EventType == eventType {
			if sl.Fingerprint == "" {
				return true // all errors of this event type
			}
			if sl.Fingerprint == fingerprint {
				return true // specific fingerprint
			}
		}
	}
	return false
}

// Invalidate drops the cache for an instrument so the next check re-queries the DB.
func (s *Store) Invalidate(instrumentID string) {
	s.mu.Lock()
	delete(s.cache, instrumentID)
	s.mu.Unlock()
}

func (s *Store) get(ctx context.Context, instrumentID string) []db.Silence {
	s.mu.RLock()
	c, ok := s.cache[instrumentID]
	s.mu.RUnlock()
	if ok && time.Since(c.fetchedAt) < s.cacheTTL {
		return c.silences
	}

	silences, err := s.dbStore.ActiveSilences(ctx, instrumentID)
	if err != nil {
		slog.Warn("silence: failed to fetch active silences", "instrument", instrumentID, "error", err)
		if ok {
			return c.silences // serve stale cache on DB error
		}
		return nil
	}

	s.mu.Lock()
	s.cache[instrumentID] = cachedSilences{silences: silences, fetchedAt: time.Now()}
	s.mu.Unlock()
	return silences
}
