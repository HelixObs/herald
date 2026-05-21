// Package db is the herald's TimescaleDB persistence layer.
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

	"github.com/HelixObs/herald/internal/notifier/fingerprint"
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

// dbMetrics is the subset of herald metrics used by the DB store.
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
	cfg, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("parse db config: %w", err)
	}
	cfg.MaxConns = 40
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
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

// ── Notification DB ───────────────────────────────────────────────────────────

// NotificationIssue is one row from notification_issues.
type NotificationIssue struct {
	InstrumentID      string
	ErrorFingerprint  string
	GithubRepo        string
	GithubIssueNumber int
	EntityCount       int
	FirstSeenAt       time.Time
	LastUpdatedAt     time.Time
	RecentEntityIDs   []string
}

// Silence is one row from notification_silences.
type Silence struct {
	ID             int       `json:"id"`
	InstrumentID   string    `json:"instrument_id"`
	EventType      string    `json:"event_type,omitempty"`
	Fingerprint    string    `json:"fingerprint,omitempty"`
	SilencedBy     string    `json:"silenced_by"`
	SilencedAt     time.Time `json:"silenced_at"`
	ExpiresAt      time.Time `json:"expires_at"`
	Reason         string    `json:"reason,omitempty"`
	GithubIssueURL string    `json:"github_issue_url,omitempty"`
}

// FindNotificationIssue returns the persisted record for a fingerprint, or nil if none exists.
func (s *Store) FindNotificationIssue(ctx context.Context, instrumentID, fingerprint, repo string) (*NotificationIssue, error) {
	var rec NotificationIssue
	err := s.pool.QueryRow(ctx, `
		SELECT github_issue_number, entity_count, first_seen_at, COALESCE(recent_entity_ids, '{}')
		FROM notification_issues
		WHERE instrument_id = $1 AND error_fingerprint = $2 AND github_repo = $3`,
		instrumentID, fingerprint, repo,
	).Scan(&rec.GithubIssueNumber, &rec.EntityCount, &rec.FirstSeenAt, &rec.RecentEntityIDs)
	if err != nil && err.Error() == "no rows in result set" {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &rec, nil
}

// UpsertNotificationIssue creates or updates a notification_issues row.
// first_seen_at is set only on INSERT — never overwritten.
// entityID is prepended to recent_entity_ids; the list is capped at 10.
func (s *Store) UpsertNotificationIssue(ctx context.Context, instrumentID, fingerprint, repo string, issueNum int, entityID string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO notification_issues
			(instrument_id, error_fingerprint, github_repo, github_issue_number, entity_count, first_seen_at, last_updated_at, recent_entity_ids)
		VALUES ($1, $2, $3, $4, 1, now(), now(), ARRAY[$5])
		ON CONFLICT (instrument_id, error_fingerprint, github_repo) DO UPDATE
			SET github_issue_number = EXCLUDED.github_issue_number,
			    entity_count        = notification_issues.entity_count + 1,
			    last_updated_at     = now(),
			    recent_entity_ids   = (ARRAY[$5] || notification_issues.recent_entity_ids)[1:10]`,
		instrumentID, fingerprint, repo, issueNum, entityID,
	)
	return err
}

// DeleteNotificationIssue removes a dedup record (used when an issue is deleted on GitHub).
func (s *Store) DeleteNotificationIssue(ctx context.Context, instrumentID, fingerprint, repo string) error {
	_, err := s.pool.Exec(ctx, `
		DELETE FROM notification_issues
		WHERE instrument_id = $1 AND error_fingerprint = $2 AND github_repo = $3`,
		instrumentID, fingerprint, repo,
	)
	return err
}

// ActiveSilences returns all non-expired silence rules for an instrument.
func (s *Store) ActiveSilences(ctx context.Context, instrumentID string) ([]Silence, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, instrument_id, COALESCE(event_type,''), COALESCE(fingerprint,''),
		       silenced_by, silenced_at, expires_at, COALESCE(reason,'')
		FROM notification_silences
		WHERE instrument_id = $1 AND expires_at > now()
		ORDER BY silenced_at DESC`,
		instrumentID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var silences []Silence
	for rows.Next() {
		var s Silence
		if err := rows.Scan(&s.ID, &s.InstrumentID, &s.EventType, &s.Fingerprint,
			&s.SilencedBy, &s.SilencedAt, &s.ExpiresAt, &s.Reason); err != nil {
			return nil, err
		}
		silences = append(silences, s)
	}
	return silences, rows.Err()
}

// CreateSilence inserts a new silence rule and returns its ID.
func (s *Store) CreateSilence(ctx context.Context, silence Silence) (int, error) {
	var id int
	err := s.pool.QueryRow(ctx, `
		INSERT INTO notification_silences
			(instrument_id, event_type, fingerprint, silenced_by, expires_at, reason)
		VALUES ($1, NULLIF($2,''), NULLIF($3,''), $4, $5, NULLIF($6,''))
		RETURNING id`,
		silence.InstrumentID, silence.EventType, silence.Fingerprint,
		silence.SilencedBy, silence.ExpiresAt, silence.Reason,
	).Scan(&id)
	return id, err
}

// DeleteSilence removes a silence rule by ID.
func (s *Store) DeleteSilence(ctx context.Context, id int) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM notification_silences WHERE id = $1`, id)
	return err
}

// ListSilences returns all silences for an instrument (including expired ones),
// with a GitHub issue URL where one exists for the silence's fingerprint.
func (s *Store) ListSilences(ctx context.Context, instrumentID string) ([]Silence, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT ns.id, ns.instrument_id,
		       COALESCE(ns.event_type,''), COALESCE(ns.fingerprint,''),
		       ns.silenced_by, ns.silenced_at, ns.expires_at, COALESCE(ns.reason,''),
		       COALESCE(ni.github_repo, ''), COALESCE(ni.github_issue_number, 0)
		FROM notification_silences ns
		LEFT JOIN notification_issues ni
		       ON ni.instrument_id = ns.instrument_id
		      AND ni.error_fingerprint = ns.fingerprint
		WHERE ns.instrument_id = $1
		ORDER BY ns.expires_at DESC`,
		instrumentID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var silences []Silence
	for rows.Next() {
		var (
			sl        Silence
			ghRepo    string
			ghIssueNo int
		)
		if err := rows.Scan(&sl.ID, &sl.InstrumentID, &sl.EventType, &sl.Fingerprint,
			&sl.SilencedBy, &sl.SilencedAt, &sl.ExpiresAt, &sl.Reason,
			&ghRepo, &ghIssueNo); err != nil {
			return nil, err
		}
		if ghRepo != "" && ghIssueNo > 0 {
			sl.GithubIssueURL = fmt.Sprintf("https://github.com/%s/issues/%d", ghRepo, ghIssueNo)
		}
		silences = append(silences, sl)
	}
	return silences, rows.Err()
}

// QueryInstruments returns all distinct instrument IDs that have sent data.
func (s *Store) QueryInstruments(ctx context.Context) ([]string, error) {
	start := time.Now()
	rows, err := s.pool.Query(ctx,
		`SELECT DISTINCT instrument_id FROM entities ORDER BY instrument_id LIMIT 100`,
	)
	s.recordQuery("instruments", err, start)
	if err != nil {
		return nil, fmt.Errorf("query instruments: %w", err)
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		result = append(result, id)
	}
	return result, rows.Err()
}

// AlertRow is one grouped helix.error alert returned by QueryAlerts.
type AlertRow struct {
	GroupKey        string            `json:"group_key"`
	Fingerprint     string            `json:"fingerprint"`
	Metadata        map[string]string `json:"metadata"`
	OccurrenceCount int               `json:"occurrence_count"`
	FirstSeen       time.Time         `json:"first_seen"`
	LastSeen        time.Time         `json:"last_seen"`
	EntityIDs       []string          `json:"entity_ids"`
}

// QueryAlerts returns unsilenced helix.error events for an instrument from the
// last 7 days, grouped by metadata content so identical errors are collapsed.
// Alerts matched by an active silence rule are excluded.
func (s *Store) QueryAlerts(ctx context.Context, instrumentID string) ([]AlertRow, error) {
	// Fetch active silences first so we can filter in Go (fingerprint normalisation
	// can't be replicated in SQL).
	silences, err := s.ActiveSilences(ctx, instrumentID)
	if err != nil {
		return nil, fmt.Errorf("query silences for alert filter: %w", err)
	}

	start := time.Now()
	rows, err := s.pool.Query(ctx, `
		WITH errors AS (
			SELECT entity_id, metadata, created_at, md5(metadata::text) AS grp
			FROM entity_events
			WHERE instrument_id = $1
			  AND event_name = 'helix.error'
			  AND created_at > now() - INTERVAL '7 days'
		)
		SELECT grp, metadata, COUNT(*) AS occurrence_count,
		       MIN(created_at) AS first_seen, MAX(created_at) AS last_seen,
		       COALESCE((array_agg(entity_id ORDER BY created_at DESC))[1:5], '{}') AS entity_ids
		FROM errors
		GROUP BY grp, metadata
		ORDER BY last_seen DESC
		LIMIT 50`,
		instrumentID,
	)
	s.recordQuery("alerts", err, start)
	if err != nil {
		return nil, fmt.Errorf("query alerts: %w", err)
	}
	defer rows.Close()

	var result []AlertRow
	for rows.Next() {
		var (
			row     AlertRow
			metaRaw []byte
		)
		if err := rows.Scan(&row.GroupKey, &metaRaw, &row.OccurrenceCount, &row.FirstSeen, &row.LastSeen, &row.EntityIDs); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		if err := json.Unmarshal(metaRaw, &row.Metadata); err != nil {
			row.Metadata = map[string]string{}
		}
		if row.EntityIDs == nil {
			row.EntityIDs = []string{}
		}
		msg := row.Metadata["message"]
		if msg == "" {
			msg = row.Metadata["exception.message"]
		}
		row.Fingerprint = fingerprint.Compute(instrumentID, "helix.error", msg, row.Metadata["stage"])
		if alertSilenced(silences, "helix.error", row.Fingerprint) {
			continue
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}
	return result, nil
}

// alertSilenced mirrors the silence.Store.IsSilenced logic without the cache layer.
func alertSilenced(silences []Silence, eventType, fp string) bool {
	for _, sl := range silences {
		if sl.EventType == "" {
			return true // instrument-wide silence
		}
		if sl.EventType == eventType {
			if sl.Fingerprint == "" {
				return true // all events of this type
			}
			if sl.Fingerprint == fp {
				return true // specific fingerprint
			}
		}
	}
	return false
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
