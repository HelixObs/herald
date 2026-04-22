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
}

// EntityEvent is one row written to the entity_events hypertable.
type EntityEvent struct {
	InstrumentID string
	EntityID     string
	EventName    string
	TimestampNs  int64
	Metadata     map[string]string
}

// dbMetrics is the subset of gateway metrics used by the DB store.
type dbMetrics interface {
	DBWriteRecord(table, status string, dur time.Duration)
	DBPoolStats(inUse, total int)
}

// Store wraps a pgxpool.Pool with HelixObs-specific write methods.
type Store struct {
	pool *pgxpool.Pool
	m    dbMetrics // nil in tests
}

func New(ctx context.Context, connStr string, m dbMetrics) (*Store, error) {
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("open pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping db: %w", err)
	}
	return &Store{pool: pool, m: m}, nil
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) recordWrite(table string, err error, start time.Time) {
	if s.m == nil {
		return
	}
	status := "success"
	if err != nil {
		status = "failed"
	}
	s.m.DBWriteRecord(table, status, time.Since(start))
	stat := s.pool.Stat()
	s.m.DBPoolStats(int(stat.AcquiredConns()), int(stat.TotalConns()))
}

// GraphNode is one entity in a provenance graph response.
type GraphNode struct {
	ID           string            `json:"id"`
	InstrumentID string            `json:"instrument_id"`
	TraceID      string            `json:"trace_id"`
	TimestampNs  int64             `json:"timestamp_ns"`
	ParentIDs    []string          `json:"parent_ids"`
	Metadata     map[string]string `json:"metadata"`
	HasError     bool              `json:"has_error"`
}

// GraphEdge is a directed edge from parent to child.
type GraphEdge struct {
	Source string `json:"source"` // parent entity ID
	Target string `json:"target"` // child entity ID
}

// EntityGraph is the full provenance graph returned by the API.
type EntityGraph struct {
	Nodes []GraphNode `json:"nodes"`
	Edges []GraphEdge `json:"edges"`
}

// QueryEntityGraph returns the provenance DAG for one entity — all ancestors
// up to maxDepth levels and direct descendants one level down.
// Nodes are de-duplicated; edges are derived from parent_ids relationships.
func (s *Store) QueryEntityGraph(ctx context.Context, entityID string, maxDepth int) (*EntityGraph, error) {
	rows, err := s.pool.Query(ctx, `
		WITH RECURSIVE deduped AS (
			-- Pick the most informative row per entity when the race between
			-- WriteEntity and WriteEntityOperation produces duplicates.
			SELECT DISTINCT ON (id)
				id, instrument_id, trace_id, timestamp_ns, parent_ids, metadata
			FROM entities
			ORDER BY id,
				array_length(parent_ids, 1) DESC NULLS LAST,
				trace_id NULLS LAST,
				created_at ASC
		),
		ancestors AS (
			SELECT id, instrument_id, trace_id, timestamp_ns, parent_ids, metadata, 0 AS depth
			FROM deduped
			WHERE id = $1
			UNION
			SELECT e.id, e.instrument_id, e.trace_id, e.timestamp_ns, e.parent_ids, e.metadata, a.depth + 1
			FROM deduped e
			INNER JOIN ancestors a ON e.id = ANY(a.parent_ids)
			WHERE a.depth < $2
		),
		descendants AS (
			SELECT id, instrument_id, trace_id, timestamp_ns, parent_ids, metadata, 0 AS depth
			FROM deduped
			WHERE id = $1
			UNION
			SELECT e.id, e.instrument_id, e.trace_id, e.timestamp_ns, e.parent_ids, e.metadata, d.depth + 1
			FROM deduped e
			INNER JOIN descendants d ON d.id = ANY(e.parent_ids)
			WHERE d.depth < 2
		)
		SELECT DISTINCT id, instrument_id, COALESCE(trace_id, ''), timestamp_ns, parent_ids, metadata
		FROM (SELECT * FROM ancestors UNION SELECT * FROM descendants) combined
		LIMIT 500`,
		entityID, maxDepth,
	)
	if err != nil {
		return nil, fmt.Errorf("graph query: %w", err)
	}
	defer rows.Close()

	nodeMap := make(map[string]*GraphNode)
	for rows.Next() {
		var (
			n       GraphNode
			metaRaw []byte
			parents []string
		)
		if err := rows.Scan(&n.ID, &n.InstrumentID, &n.TraceID, &n.TimestampNs, &parents, &metaRaw); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		if parents == nil {
			parents = []string{}
		}
		n.ParentIDs = parents
		if err := json.Unmarshal(metaRaw, &n.Metadata); err != nil {
			n.Metadata = map[string]string{}
		}
		if existing, ok := nodeMap[n.ID]; !ok ||
			len(n.ParentIDs) > len(existing.ParentIDs) ||
			(n.TraceID != "" && existing.TraceID == "") {
			nodeMap[n.ID] = &n
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}

	if len(nodeMap) == 0 {
		return nil, nil // entity not found
	}

	// Mark nodes that have helix.error events.
	ids := make([]string, 0, len(nodeMap))
	for id := range nodeMap {
		ids = append(ids, id)
	}
	errRows, err := s.pool.Query(ctx,
		`SELECT DISTINCT entity_id FROM entity_events
		 WHERE entity_id = ANY($1) AND event_name = 'helix.error'`, ids)
	if err != nil {
		return nil, fmt.Errorf("error events query: %w", err)
	}
	defer errRows.Close()
	for errRows.Next() {
		var id string
		if err := errRows.Scan(&id); err == nil {
			if n, ok := nodeMap[id]; ok {
				n.HasError = true
			}
		}
	}

	// Build edge list from parent_ids of nodes in the result set.
	nodes := make([]GraphNode, 0, len(nodeMap))
	var edges []GraphEdge
	for _, n := range nodeMap {
		nodes = append(nodes, *n)
		for _, pid := range n.ParentIDs {
			if _, ok := nodeMap[pid]; ok {
				edges = append(edges, GraphEdge{Source: pid, Target: n.ID})
			}
		}
	}
	if edges == nil {
		edges = []GraphEdge{}
	}

	return &EntityGraph{Nodes: nodes, Edges: edges}, nil
}

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
	start := time.Now()
	_, err = s.pool.Exec(ctx, `
		INSERT INTO entities
			(id, instrument_id, trace_id, timestamp_ns, parent_ids, metadata)
		SELECT $1, $2, $3, $4, $5, $6
		WHERE NOT EXISTS (
			SELECT 1 FROM entities WHERE id = $1 AND instrument_id = $2
			AND (cardinality(parent_ids) > 0 OR cardinality($5::text[]) = 0)
		)`,
		e.ID, e.InstrumentID, e.TraceID, e.TimestampNs, parentIDs, meta,
	)
	s.recordWrite("entities", err, start)
	return err
}

// maxOperationsPerPair is the maximum number of entity_operations rows retained
// for a given (entity_id, operation) pair. When a new row pushes the count above
// this limit the oldest rows are pruned so only the most recent attempts remain.
const maxOperationsPerPair = 10

// WriteEntityOperation inserts one entity_operation row.
// If the target entity does not yet exist (e.g. the operation was submitted
// before the creation span arrived, or the entity was never formally tracked),
// a minimal placeholder entity row is created so the Entity Inspector always
// has something to display.
// After each insert, rows beyond maxOperationsPerPair for the same
// (entity_id, operation) pair are deleted so high-retry scenarios do not
// accumulate unbounded history — the most recent attempts are kept.
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

	// Atomic dedup: claim the trace_id in the seen-set first.
	// ON CONFLICT DO NOTHING on a regular PRIMARY KEY is atomic — no race window.
	tag, err := s.pool.Exec(ctx,
		`INSERT INTO operation_trace_seen (trace_id) VALUES ($1) ON CONFLICT DO NOTHING`,
		op.TraceID,
	)
	if err != nil {
		return fmt.Errorf("trace dedup: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return nil // already processed, skip silently
	}

	start := time.Now()
	_, err = s.pool.Exec(ctx, `
		INSERT INTO entity_operations
			(entity_id, instrument_id, operation, trace_id, timestamp_ns, metadata)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		op.EntityID, op.InstrumentID, op.Operation, op.TraceID, op.TimestampNs, meta,
	)
	s.recordWrite("entity_operations", err, start)
	if err != nil {
		return err
	}

	// Prune oldest rows beyond the retention limit for this (entity_id, operation) pair.
	// The subquery returns the created_at of the Nth most recent row; anything older is deleted.
	// If fewer than maxOperationsPerPair rows exist the subquery returns no rows and no
	// deletion occurs.
	_, err = s.pool.Exec(ctx, `
		DELETE FROM entity_operations
		WHERE entity_id = $1 AND operation = $2
		  AND created_at < (
		      SELECT created_at
		      FROM entity_operations
		      WHERE entity_id = $1 AND operation = $2
		      ORDER BY created_at DESC
		      LIMIT 1 OFFSET $3
		  )`,
		op.EntityID, op.Operation, maxOperationsPerPair-1,
	)
	return err
}

// WriteEntityEvent inserts one entity_event row.
func (s *Store) WriteEntityEvent(ctx context.Context, ev EntityEvent) error {
	meta, err := json.Marshal(ev.Metadata)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}
	start := time.Now()
	_, err = s.pool.Exec(ctx, `
		INSERT INTO entity_events
			(instrument_id, entity_id, event_name, timestamp_ns, metadata)
		VALUES ($1, $2, $3, $4, $5)`,
		ev.InstrumentID, ev.EntityID, ev.EventName, ev.TimestampNs, meta,
	)
	s.recordWrite("entity_events", err, start)
	return err
}
