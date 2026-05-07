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
	DurationNs   int64
	Metadata     map[string]string
}

// EntityOperationRow is one row returned by the entity operations read API.
type EntityOperationRow struct {
	Operation   string            `json:"operation"`
	TraceID     string            `json:"trace_id"`
	TimestampNs int64             `json:"timestamp_ns"`
	DurationNs  int64             `json:"duration_ns"`
	Metadata    map[string]string `json:"metadata"`
}

// EntityEvent is one row written to the entity_events hypertable.
type EntityEvent struct {
	InstrumentID string
	EntityID     string
	TraceID      string
	EventName    string
	TimestampNs  int64
	Metadata     map[string]string
}

// dbMetrics is the subset of gateway metrics used by the DB store.
type dbMetrics interface {
	DBWriteRecord(table, status string, dur time.Duration)
	DBQueryRecord(query, status string, dur time.Duration)
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

func (s *Store) recordQuery(query string, err error, start time.Time) {
	if s.m == nil {
		return
	}
	status := "success"
	if err != nil {
		status = "failed"
	}
	s.m.DBQueryRecord(query, status, time.Since(start))
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
	// Anchor each CTE on WHERE id = $1 so dedup only touches the specific
	// entity's rows (uses idx_entities_id) rather than scanning the whole table.
	// Error flag is folded into the main query to eliminate a second round-trip.
	start := time.Now()
	rows, err := s.pool.Query(ctx, `
		WITH RECURSIVE
		ancestors(id, instrument_id, trace_id, timestamp_ns, parent_ids, metadata, depth) AS (
			-- Subquery isolates DISTINCT ON + ORDER BY from the UNION so PostgreSQL
			-- does not misparse the ORDER BY as applying to the whole recursive query.
			SELECT id, instrument_id, trace_id, timestamp_ns, parent_ids, metadata, 0
			FROM (
				SELECT DISTINCT ON (id)
					id, instrument_id, trace_id, timestamp_ns, parent_ids, metadata
				FROM entities
				WHERE id = $1
				ORDER BY id,
					array_length(parent_ids, 1) DESC NULLS LAST,
					trace_id NULLS LAST,
					created_at ASC
			) t
			UNION
			SELECT e.id, e.instrument_id, e.trace_id, e.timestamp_ns, e.parent_ids, e.metadata, a.depth + 1
			FROM entities e
			INNER JOIN ancestors a ON e.id = ANY(a.parent_ids)
			WHERE a.depth < $2
		),
		descendants(id, instrument_id, trace_id, timestamp_ns, parent_ids, metadata, depth) AS (
			SELECT id, instrument_id, trace_id, timestamp_ns, parent_ids, metadata, 0
			FROM (
				SELECT DISTINCT ON (id)
					id, instrument_id, trace_id, timestamp_ns, parent_ids, metadata
				FROM entities
				WHERE id = $1
				ORDER BY id,
					array_length(parent_ids, 1) DESC NULLS LAST,
					trace_id NULLS LAST,
					created_at ASC
			) t
			UNION
			SELECT e.id, e.instrument_id, e.trace_id, e.timestamp_ns, e.parent_ids, e.metadata, d.depth + 1
			FROM entities e
			INNER JOIN descendants d ON e.parent_ids @> ARRAY[d.id]
			WHERE d.depth < 2
		)
		SELECT
			c.id,
			c.instrument_id,
			COALESCE(c.trace_id, '') AS trace_id,
			c.timestamp_ns,
			c.parent_ids,
			c.metadata,
			EXISTS(
				SELECT 1 FROM entity_events ee
				WHERE ee.entity_id = c.id AND ee.event_name = 'helix.error'
			) AS has_error
		FROM (
			SELECT id, instrument_id, trace_id, timestamp_ns, parent_ids, metadata
			FROM ancestors
			UNION
			SELECT id, instrument_id, trace_id, timestamp_ns, parent_ids, metadata
			FROM descendants
		) c
		LIMIT 500`,
		entityID, maxDepth,
	)
	s.recordQuery("entity_graph", err, start)
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
		if err := rows.Scan(&n.ID, &n.InstrumentID, &n.TraceID, &n.TimestampNs, &parents, &metaRaw, &n.HasError); err != nil {
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
			(entity_id, instrument_id, operation, trace_id, timestamp_ns, duration_ns, metadata)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		op.EntityID, op.InstrumentID, op.Operation, op.TraceID, op.TimestampNs, op.DurationNs, meta,
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

// RawEntityRow is one entity row with its full metadata — used by the monitor binning API.
type RawEntityRow struct {
	ID          string
	TimestampNs int64
	Metadata    map[string]string
}

// QueryEntitiesRaw fetches entities for one instrument within [fromNs, toNs].
// metaFilter is a validated SQL fragment (e.g. "metadata ? 'key1' AND metadata ? 'key2'")
// injected into the WHERE clause after key validation in the monitor package.
// created_at ≈ timestamp_ns (both are wall-clock ingestion time), so filtering on
// created_at alone is sufficient and lets TimescaleDB use chunk pruning directly.
func (s *Store) QueryEntitiesRaw(ctx context.Context, instrument string, fromNs, toNs int64, metaFilter string) ([]RawEntityRow, error) {
	fromTime := time.Unix(0, fromNs)
	toTime := time.Unix(0, toNs)

	q := `SELECT id, timestamp_ns, metadata
		FROM entities
		WHERE instrument_id = $1
		  AND created_at BETWEEN $2 AND $3`
	if metaFilter != "" {
		q += " AND " + metaFilter
	}

	start := time.Now()
	rows, err := s.pool.Query(ctx, q, instrument, fromTime, toTime)
	s.recordQuery("entities_raw", err, start)
	if err != nil {
		return nil, fmt.Errorf("query entities raw: %w", err)
	}
	defer rows.Close()

	var result []RawEntityRow
	for rows.Next() {
		var (
			row     RawEntityRow
			metaRaw []byte
		)
		if err := rows.Scan(&row.ID, &row.TimestampNs, &metaRaw); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		if err := json.Unmarshal(metaRaw, &row.Metadata); err != nil {
			row.Metadata = map[string]string{}
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}
	return result, nil
}

// QueryEntityOperations returns all operations for one entity, ordered by start time.
func (s *Store) QueryEntityOperations(ctx context.Context, entityID string) ([]EntityOperationRow, error) {
	start := time.Now()
	rows, err := s.pool.Query(ctx, `
		SELECT 'creation' AS operation, COALESCE(trace_id, ''), timestamp_ns, 0 AS duration_ns, metadata
		FROM entities
		WHERE id = $1 AND trace_id IS NOT NULL
		UNION ALL
		SELECT operation, COALESCE(trace_id, ''), timestamp_ns, COALESCE(duration_ns, 0), metadata
		FROM entity_operations
		WHERE entity_id = $1
		ORDER BY timestamp_ns ASC`,
		entityID,
	)
	s.recordQuery("entity_operations", err, start)
	if err != nil {
		return nil, fmt.Errorf("query entity operations: %w", err)
	}
	defer rows.Close()

	var result []EntityOperationRow
	for rows.Next() {
		var (
			row     EntityOperationRow
			metaRaw []byte
		)
		if err := rows.Scan(&row.Operation, &row.TraceID, &row.TimestampNs, &row.DurationNs, &metaRaw); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		if err := json.Unmarshal(metaRaw, &row.Metadata); err != nil {
			row.Metadata = map[string]string{}
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}
	return result, nil
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
			(instrument_id, entity_id, trace_id, event_name, timestamp_ns, metadata)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		ev.InstrumentID, ev.EntityID, ev.TraceID, ev.EventName, ev.TimestampNs, meta,
	)
	s.recordWrite("entity_events", err, start)
	return err
}
