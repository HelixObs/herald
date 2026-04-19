package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// ── Filter types ──────────────────────────────────────────────────────────────

type EntityFilter struct {
	InstrumentID string
	From         time.Time
	To           time.Time
	Limit        int
	Offset       int
}

type OperationFilter struct {
	Operation string
	From      time.Time
	To        time.Time
	Limit     int
}

type EventFilter struct {
	EventName string
	From      time.Time
	To        time.Time
	Limit     int
}

const defaultLimit = 100
const maxLimit     = 1000

func clampLimit(l int) int {
	if l <= 0 {
		return defaultLimit
	}
	if l > maxLimit {
		return maxLimit
	}
	return l
}

// ── Read methods ──────────────────────────────────────────────────────────────

// GetEntity returns the most recent entity row with the given ID, or nil if not found.
func (s *Store) GetEntity(ctx context.Context, id string) (*Entity, error) {
	const q = `
		SELECT id, instrument_id, COALESCE(trace_id,''), timestamp_ns,
		       COALESCE(parent_ids, ARRAY[]::text[]),
		       COALESCE(metadata, '{}')::text, created_at
		FROM entities
		WHERE id = $1
		ORDER BY created_at DESC
		LIMIT 1`

	var e Entity
	var meta []byte
	err := s.pool.QueryRow(ctx, q, id).Scan(
		&e.ID, &e.InstrumentID, &e.TraceID, &e.TimestampNs,
		&e.ParentIDs, &meta, &e.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get entity: %w", err)
	}
	if len(meta) > 0 {
		_ = json.Unmarshal(meta, &e.Metadata)
	}
	if e.ParentIDs == nil {
		e.ParentIDs = []string{}
	}
	return &e, nil
}

// ListEntities returns entities matching the filter, newest-first.
func (s *Store) ListEntities(ctx context.Context, f EntityFilter) ([]Entity, error) {
	q := `SELECT id, instrument_id, COALESCE(trace_id,''), timestamp_ns,
	             COALESCE(parent_ids, ARRAY[]::text[]),
	             COALESCE(metadata, '{}')::text, created_at
	      FROM entities WHERE 1=1`
	args := []any{}
	n := 1

	if f.InstrumentID != "" {
		q += fmt.Sprintf(" AND instrument_id = $%d", n)
		args = append(args, f.InstrumentID)
		n++
	}
	if !f.From.IsZero() {
		q += fmt.Sprintf(" AND created_at >= $%d", n)
		args = append(args, f.From)
		n++
	}
	if !f.To.IsZero() {
		q += fmt.Sprintf(" AND created_at <= $%d", n)
		args = append(args, f.To)
		n++
	}

	q += fmt.Sprintf(" ORDER BY created_at DESC LIMIT $%d OFFSET $%d", n, n+1)
	args = append(args, clampLimit(f.Limit), f.Offset)

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list entities: %w", err)
	}
	defer rows.Close()

	var out []Entity
	for rows.Next() {
		var e Entity
		var meta []byte
		if err := rows.Scan(
			&e.ID, &e.InstrumentID, &e.TraceID, &e.TimestampNs,
			&e.ParentIDs, &meta, &e.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan entity: %w", err)
		}
		if len(meta) > 0 {
			_ = json.Unmarshal(meta, &e.Metadata)
		}
		if e.ParentIDs == nil {
			e.ParentIDs = []string{}
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// ListEntityOperations returns operations for the given entity, oldest-first.
func (s *Store) ListEntityOperations(ctx context.Context, entityID string, f OperationFilter) ([]EntityOperation, error) {
	q := `SELECT entity_id, instrument_id, operation, COALESCE(trace_id,''),
	             timestamp_ns, COALESCE(metadata, '{}')::text, created_at
	      FROM entity_operations WHERE entity_id = $1`
	args := []any{entityID}
	n := 2

	if f.Operation != "" {
		q += fmt.Sprintf(" AND operation = $%d", n)
		args = append(args, f.Operation)
		n++
	}
	if !f.From.IsZero() {
		q += fmt.Sprintf(" AND created_at >= $%d", n)
		args = append(args, f.From)
		n++
	}
	if !f.To.IsZero() {
		q += fmt.Sprintf(" AND created_at <= $%d", n)
		args = append(args, f.To)
		n++
	}

	q += fmt.Sprintf(" ORDER BY created_at ASC LIMIT $%d", n)
	args = append(args, clampLimit(f.Limit))

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list operations: %w", err)
	}
	defer rows.Close()

	var out []EntityOperation
	for rows.Next() {
		var op EntityOperation
		var meta []byte
		if err := rows.Scan(
			&op.EntityID, &op.InstrumentID, &op.Operation, &op.TraceID,
			&op.TimestampNs, &meta, &op.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan operation: %w", err)
		}
		if len(meta) > 0 {
			_ = json.Unmarshal(meta, &op.Metadata)
		}
		out = append(out, op)
	}
	return out, rows.Err()
}

// ListEntityEvents returns events for the given entity, oldest-first.
func (s *Store) ListEntityEvents(ctx context.Context, entityID string, f EventFilter) ([]EntityEvent, error) {
	q := `SELECT instrument_id, entity_id, event_name, timestamp_ns,
	             COALESCE(metadata, '{}')::text, created_at
	      FROM entity_events WHERE entity_id = $1`
	args := []any{entityID}
	n := 2

	if f.EventName != "" {
		q += fmt.Sprintf(" AND event_name = $%d", n)
		args = append(args, f.EventName)
		n++
	}
	if !f.From.IsZero() {
		q += fmt.Sprintf(" AND created_at >= $%d", n)
		args = append(args, f.From)
		n++
	}
	if !f.To.IsZero() {
		q += fmt.Sprintf(" AND created_at <= $%d", n)
		args = append(args, f.To)
		n++
	}

	q += fmt.Sprintf(" ORDER BY created_at ASC LIMIT $%d", n)
	args = append(args, clampLimit(f.Limit))

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list events: %w", err)
	}
	defer rows.Close()

	var out []EntityEvent
	for rows.Next() {
		var ev EntityEvent
		var meta []byte
		if err := rows.Scan(
			&ev.InstrumentID, &ev.EntityID, &ev.EventName, &ev.TimestampNs,
			&meta, &ev.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		if len(meta) > 0 {
			_ = json.Unmarshal(meta, &ev.Metadata)
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}
