-- Atomic deduplication for entity_operations.
--
-- entity_operations is a hypertable and cannot carry a unique index on trace_id
-- alone (TimescaleDB requires the partition column in every unique index).
-- This regular table acts as a seen-set: WriteEntityOperation inserts here with
-- ON CONFLICT DO NOTHING before touching entity_operations.
-- RowsAffected == 0 means the trace was already processed → skip the insert.

CREATE TABLE IF NOT EXISTS operation_trace_seen (
    trace_id   TEXT        PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
