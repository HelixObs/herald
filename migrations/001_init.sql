-- HelixObs gateway schema
-- Applies automatically on first container start via /docker-entrypoint-initdb.d

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ── Entities ──────────────────────────────────────────────────────────────────
-- One row per entity span processed by the gateway.
-- parent_ids is a TEXT[] so the recursive CTE provenance query can use
-- the GIN index for "find all children of entity X" lookups.
-- metadata holds domain-specific span attributes (helix.chime.*, etc.) as JSONB.

CREATE TABLE IF NOT EXISTS entities (
    id            TEXT        NOT NULL,
    instrument_id TEXT        NOT NULL,
    trace_id      TEXT,
    timestamp_ns  BIGINT      NOT NULL,
    parent_ids    TEXT[]      NOT NULL DEFAULT '{}',
    metadata      JSONB       NOT NULL DEFAULT '{}',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

SELECT create_hypertable('entities', 'created_at',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists       => TRUE);

ALTER TABLE entities SET (timescaledb.compress, timescaledb.compress_orderby = 'created_at DESC');
SELECT add_compression_policy('entities', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_retention_policy('entities',   INTERVAL '90 days', if_not_exists => TRUE);

-- Lookup index used by the WHERE NOT EXISTS deduplication in WriteEntity.
-- TimescaleDB hypertables cannot have a unique index that omits the
-- partitioning column (created_at), so deduplication is application-level.
CREATE INDEX IF NOT EXISTS idx_entities_id
    ON entities (id, instrument_id);

-- Most common query pattern: all entities for an instrument in a time window.
CREATE INDEX IF NOT EXISTS idx_entities_instrument_time
    ON entities (instrument_id, created_at DESC);

-- Provenance traversal: "find all entities whose parent_ids contain X".
CREATE INDEX IF NOT EXISTS idx_entities_parents
    ON entities USING GIN (parent_ids);

-- Metadata queries: WHERE (metadata->>'snr')::float > 20
CREATE INDEX IF NOT EXISTS idx_entities_metadata
    ON entities USING GIN (metadata);


-- ── Entity operations ────────────────────────────────────────────────────────
-- One row per independent operation performed on an existing entity after its
-- creation trace has ended.  Examples: hdf5-conversion, registration, replication.
-- Each operation has its own OTel trace, so operators can drill into each one
-- independently, but they are linked to the originating entity by entity_id.

CREATE TABLE IF NOT EXISTS entity_operations (
    entity_id     TEXT        NOT NULL,
    instrument_id TEXT        NOT NULL,
    operation     TEXT        NOT NULL,
    trace_id      TEXT,
    timestamp_ns  BIGINT      NOT NULL,
    metadata      JSONB       NOT NULL DEFAULT '{}',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

SELECT create_hypertable('entity_operations', 'created_at',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists       => TRUE);

SELECT add_retention_policy('entity_operations', INTERVAL '90 days', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_entity_operations_entity_time
    ON entity_operations (entity_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_entity_operations_op_time
    ON entity_operations (instrument_id, operation, created_at DESC);


-- ── Entity events ─────────────────────────────────────────────────────────────
-- One row per helix.* span event extracted by the gateway interceptor.
-- Covers helix.error, helix.event.rfi_flagged, helix.event.candidate_promoted,
-- helix.event.voevent_issued, helix.event.archived, etc.
-- The event's own timestamp (from the OTel span event) is stored as timestamp_ns
-- so Grafana time-series queries reflect when the event actually occurred.

CREATE TABLE IF NOT EXISTS entity_events (
    instrument_id TEXT        NOT NULL,
    entity_id     TEXT        NOT NULL,
    event_name    TEXT        NOT NULL,
    timestamp_ns  BIGINT      NOT NULL,
    metadata      JSONB       NOT NULL DEFAULT '{}',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

SELECT create_hypertable('entity_events', 'created_at',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists       => TRUE);

SELECT add_retention_policy('entity_events', INTERVAL '90 days', if_not_exists => TRUE);

-- Full timeline for one entity (ordered by event timestamp).
CREATE INDEX IF NOT EXISTS idx_entity_events_entity_time
    ON entity_events (entity_id, created_at DESC);

-- All events of a given type for an instrument — used by Grafana dashboards
-- and the pre-built alerting rules.
CREATE INDEX IF NOT EXISTS idx_entity_events_name_time
    ON entity_events (instrument_id, event_name, created_at DESC);
