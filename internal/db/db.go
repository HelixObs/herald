// Package db is the gateway's TimescaleDB persistence layer.
//
// Writes are called from goroutines in the interceptor — errors are returned
// to the caller so it can log and increment the DB write error metric.
// The hot path (span processing, span link injection) is never blocked here.
package db

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Entity is one row written to the entities hypertable.
type Entity struct {
	ID           string
	InstrumentID string
	TraceID      string
	TimestampNs  int64
	ParentIDs    []string
	Metadata     map[string]string
}

// EntityEvent is one row written to the entity_events hypertable.
type EntityEvent struct {
	InstrumentID string
	EntityID     string
	EventName    string
	TimestampNs  int64
	Metadata     map[string]string
}

// Store wraps a pgxpool.Pool with HelixObs-specific write methods.
type Store struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, connStr string) (*Store, error) {
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("open pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping db: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

// WriteEntity upserts one entity row. Duplicate (id, instrument_id) pairs
// are silently ignored — client retries on network failure may re-submit.
func (s *Store) WriteEntity(ctx context.Context, e Entity) error {
	meta, err := json.Marshal(e.Metadata)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO entities
			(id, instrument_id, trace_id, timestamp_ns, parent_ids, metadata)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (id, instrument_id) DO NOTHING`,
		e.ID, e.InstrumentID, e.TraceID, e.TimestampNs, e.ParentIDs, meta,
	)
	return err
}

// WriteEntityEvent inserts one entity_event row.
func (s *Store) WriteEntityEvent(ctx context.Context, ev EntityEvent) error {
	meta, err := json.Marshal(ev.Metadata)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO entity_events
			(instrument_id, entity_id, event_name, timestamp_ns, metadata)
		VALUES ($1, $2, $3, $4, $5)`,
		ev.InstrumentID, ev.EntityID, ev.EventName, ev.TimestampNs, meta,
	)
	return err
}
