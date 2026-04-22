-- Sherlock usage records — financial-grade log of every investigation.
-- Prometheus counters are ephemeral; this table is the durable cost ledger.

CREATE TABLE IF NOT EXISTS sherlock_usage (
    id            TEXT          NOT NULL DEFAULT gen_random_uuid()::text,
    session_id    TEXT          NOT NULL,
    instrument_id TEXT          NOT NULL DEFAULT '',
    entity_id     TEXT          NOT NULL DEFAULT '',
    model         TEXT          NOT NULL,
    tokens_input  INT           NOT NULL DEFAULT 0,
    tokens_output INT           NOT NULL DEFAULT 0,
    cost_usd      NUMERIC(10,6) NOT NULL DEFAULT 0,
    duration_ms   INT           NOT NULL DEFAULT 0,
    tool_calls    INT           NOT NULL DEFAULT 0,
    successful    BOOLEAN       NOT NULL DEFAULT true,
    created_at    TIMESTAMPTZ   NOT NULL DEFAULT now()
);

SELECT create_hypertable('sherlock_usage', 'created_at',
    chunk_time_interval => INTERVAL '30 days',
    if_not_exists       => TRUE);

CREATE INDEX IF NOT EXISTS idx_sherlock_usage_session
    ON sherlock_usage (session_id);
CREATE INDEX IF NOT EXISTS idx_sherlock_usage_instrument_time
    ON sherlock_usage (instrument_id, created_at DESC);
