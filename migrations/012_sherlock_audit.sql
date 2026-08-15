-- Sherlock audit log — write-only record of every exchange, regardless of
-- interface or outcome. Never a retrieval source for Sherlock itself: the
-- agent loop must never query this table. Operators paste raw, unsanitized
-- material into conversations, so a retrievable audit log would be a live
-- path for that material to leak back into future answers — the same
-- reasoning the original Herodotus design applied to its own audit log.
--
-- Distinct from sherlock_sessions (011): sessions is read+write, meant to
-- be read back to resume context. This table is write-only by contract,
-- not just by convention.

CREATE TABLE IF NOT EXISTS sherlock_audit (
    id            TEXT          NOT NULL DEFAULT gen_random_uuid()::text,
    ts            TIMESTAMPTZ   NOT NULL DEFAULT now(),
    session_id    TEXT          NOT NULL,   -- UUID (web) or slack:{channel}:{thread_ts}
    interface     TEXT          NOT NULL DEFAULT 'web' CHECK (interface IN ('web', 'slack')),
    operator_id   TEXT          NOT NULL DEFAULT 'unknown',
    operator_name TEXT          NOT NULL DEFAULT '',
    instrument_id TEXT,
    entity_id     TEXT,                     -- null for general chat
    profile       TEXT          NOT NULL DEFAULT 'full',  -- 'kb_only' | 'full' — capability profiles not yet built
    kb_version    TEXT,                     -- Herodotus tag loaded at answer time — null until kb.py exists
    question      TEXT          NOT NULL,
    response      TEXT          NOT NULL,
    tools_used    TEXT[]        NOT NULL DEFAULT '{}',
    model         TEXT,
    cost_usd      NUMERIC(10,6) NOT NULL DEFAULT 0,
    latency_ms    INT           NOT NULL DEFAULT 0,
    filter_hit    BOOLEAN       NOT NULL DEFAULT false,   -- output guardrail fired — guardrail.py not yet built
    flagged       BOOLEAN       NOT NULL DEFAULT false,
    flag_reason   TEXT
);

SELECT create_hypertable('sherlock_audit', 'ts',
    chunk_time_interval => INTERVAL '30 days',
    if_not_exists       => TRUE);

CREATE INDEX IF NOT EXISTS idx_sherlock_audit_flagged
    ON sherlock_audit (flagged) WHERE flagged = TRUE;
CREATE INDEX IF NOT EXISTS idx_sherlock_audit_operator
    ON sherlock_audit (operator_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_sherlock_audit_instrument_time
    ON sherlock_audit (instrument_id, ts DESC);
