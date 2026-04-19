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
	"time"

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
	CreatedAt    time.Time
}

// EntityOperation is one row written to the entity_operations hypertable.
// It records an independent operation performed on an existing entity after
// that entity's creation trace has ended.
type EntityOperation struct {
	EntityID     string
	InstrumentID string
	Operation    string
	TraceID      string
	TimestampNs  int64
	Metadata     map[string]string
	CreatedAt    time.Time
}

// EntityEvent is one row written to the entity_events hypertable.
type EntityEvent struct {
	InstrumentID string
	EntityID     string
	EventName    string
	TimestampNs  int64
	Metadata     map[string]string
	CreatedAt    time.Time
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
	parentIDs := e.ParentIDs
	if parentIDs == nil {
		parentIDs = []string{}
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO entities
			(id, instrument_id, trace_id, timestamp_ns, parent_ids, metadata)
		SELECT $1, $2, $3, $4, $5, $6
		WHERE NOT EXISTS (
			SELECT 1 FROM entities WHERE id = $1 AND instrument_id = $2
		)`,
		e.ID, e.InstrumentID, e.TraceID, e.TimestampNs, parentIDs, meta,
	)
	return err
}

// WriteEntityOperation inserts one entity_operation row.
// If the target entity does not yet exist (e.g. the operation was submitted
// before the creation span arrived, or the entity was never formally tracked),
// a minimal placeholder entity row is created so the Entity Inspector always
// has something to display.
func (s *Store) WriteEntityOperation(ctx context.Context, op EntityOperation) error {
	meta, err := json.Marshal(op.Metadata)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}

	// Ensure entity exists — no-op if it was already created normally.
	_, err = s.pool.Exec(ctx, `
		INSERT INTO entities (id, instrument_id, timestamp_ns, parent_ids, metadata)
		SELECT $1, $2, $3, '{}', '{}'
		WHERE NOT EXISTS (
			SELECT 1 FROM entities WHERE id = $1 AND instrument_id = $2
		)`,
		op.EntityID, op.InstrumentID, op.TimestampNs,
	)
	if err != nil {
		return fmt.Errorf("ensure entity: %w", err)
	}

	_, err = s.pool.Exec(ctx, `
		INSERT INTO entity_operations
			(entity_id, instrument_id, operation, trace_id, timestamp_ns, metadata)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		op.EntityID, op.InstrumentID, op.Operation, op.TraceID, op.TimestampNs, meta,
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
