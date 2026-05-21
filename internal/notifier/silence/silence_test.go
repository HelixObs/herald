package silence_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/HelixObs/herald/internal/db"
	"github.com/HelixObs/herald/internal/notifier/silence"
)

type mockDB struct {
	silences []db.Silence
	err      error
	calls    int
}

func (m *mockDB) ActiveSilences(_ context.Context, _ string) ([]db.Silence, error) {
	m.calls++
	return m.silences, m.err
}

func TestIsSilenced_InstrumentWide(t *testing.T) {
	db := &mockDB{silences: []db.Silence{{InstrumentID: "CHIME", EventType: ""}}}
	s := silence.New(db, time.Minute)
	if !s.IsSilenced(context.Background(), "CHIME", "helix.error", "fp1") {
		t.Error("expected silenced by instrument-wide rule")
	}
}

func TestIsSilenced_EventType(t *testing.T) {
	db := &mockDB{silences: []db.Silence{{InstrumentID: "CHIME", EventType: "helix.error", Fingerprint: ""}}}
	s := silence.New(db, time.Minute)
	if !s.IsSilenced(context.Background(), "CHIME", "helix.error", "anyfingerprint") {
		t.Error("expected silenced by event-type rule")
	}
	if s.IsSilenced(context.Background(), "CHIME", "helix.warning", "anyfingerprint") {
		t.Error("expected not silenced for different event type")
	}
}

func TestIsSilenced_Fingerprint(t *testing.T) {
	db := &mockDB{silences: []db.Silence{{InstrumentID: "CHIME", EventType: "helix.error", Fingerprint: "abc123"}}}
	s := silence.New(db, time.Minute)
	if !s.IsSilenced(context.Background(), "CHIME", "helix.error", "abc123") {
		t.Error("expected silenced by fingerprint rule")
	}
	if s.IsSilenced(context.Background(), "CHIME", "helix.error", "xyz789") {
		t.Error("expected not silenced for different fingerprint")
	}
}

func TestIsSilenced_NoMatch(t *testing.T) {
	db := &mockDB{silences: []db.Silence{}}
	s := silence.New(db, time.Minute)
	if s.IsSilenced(context.Background(), "CHIME", "helix.error", "fp1") {
		t.Error("expected not silenced when no rules")
	}
}

func TestCachePreventsDuplicateDBCalls(t *testing.T) {
	mock := &mockDB{silences: []db.Silence{}}
	s := silence.New(mock, time.Minute)
	ctx := context.Background()

	s.IsSilenced(ctx, "CHIME", "helix.error", "fp1")
	s.IsSilenced(ctx, "CHIME", "helix.error", "fp2")
	s.IsSilenced(ctx, "CHIME", "helix.error", "fp3")

	if mock.calls != 1 {
		t.Errorf("expected 1 DB call due to cache, got %d", mock.calls)
	}
}

func TestInvalidate_RefreshesCache(t *testing.T) {
	mock := &mockDB{silences: []db.Silence{}}
	s := silence.New(mock, time.Minute)
	ctx := context.Background()

	s.IsSilenced(ctx, "CHIME", "helix.error", "fp1")
	s.Invalidate("CHIME")
	s.IsSilenced(ctx, "CHIME", "helix.error", "fp1")

	if mock.calls != 2 {
		t.Errorf("expected 2 DB calls after invalidation, got %d", mock.calls)
	}
}

func TestStaleCache_ServedOnDBError(t *testing.T) {
	mock := &mockDB{silences: []db.Silence{{InstrumentID: "CHIME", EventType: ""}}}
	s := silence.New(mock, time.Millisecond) // very short TTL
	ctx := context.Background()

	// Populate cache.
	s.IsSilenced(ctx, "CHIME", "helix.error", "fp1")

	// Make DB fail on next call.
	mock.err = errors.New("db down")
	mock.silences = nil

	// Wait for TTL to expire.
	time.Sleep(5 * time.Millisecond)

	// Should still return true from stale cache despite DB error.
	if !s.IsSilenced(ctx, "CHIME", "helix.error", "fp1") {
		t.Error("expected stale cache to be served on DB error")
	}
}

func TestCacheTTL_Expires(t *testing.T) {
	mock := &mockDB{silences: []db.Silence{}}
	s := silence.New(mock, time.Millisecond)
	ctx := context.Background()

	s.IsSilenced(ctx, "CHIME", "helix.error", "fp1")
	time.Sleep(5 * time.Millisecond)
	s.IsSilenced(ctx, "CHIME", "helix.error", "fp1")

	if mock.calls < 2 {
		t.Errorf("expected at least 2 DB calls after TTL expiry, got %d", mock.calls)
	}
}
