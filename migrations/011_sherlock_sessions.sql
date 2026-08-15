-- Sherlock session state — durable backing store for in-progress and
-- resumable conversations, read-through/write-through from sessions.py's
-- in-memory hot cache.
--
-- Unlike sherlock_usage / instrument_memory, this is NOT a hypertable: rows
-- are mutated in place (UPSERT on id) as a conversation progresses, not
-- appended once and left alone, so time-based partitioning doesn't fit.
--
-- id is app-supplied, not generated here: a UUID for web sessions, or
-- slack:{channel_id}:{thread_ts} for Slack sessions, so a session's identity
-- is deterministic from the Slack thread it belongs to.
--
-- github_token is deliberately never persisted here — it stays in the
-- in-memory Session only and is lost on cache eviction, same guarantee
-- sessions.py already documents for it today.

CREATE TABLE IF NOT EXISTS sherlock_sessions (
    id            TEXT        PRIMARY KEY,
    interface     TEXT        NOT NULL CHECK (interface IN ('web', 'slack')),
    entity_id     TEXT        NOT NULL DEFAULT '',
    instrument_id TEXT        NOT NULL DEFAULT '',
    history       JSONB       NOT NULL DEFAULT '[]',
    turn_count    INT         NOT NULL DEFAULT 0,
    input_tokens  INT         NOT NULL DEFAULT 0,
    output_tokens INT         NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_sherlock_sessions_updated
    ON sherlock_sessions (updated_at);
