-- Notification issue dedup map: one GitHub issue per error fingerprint per instrument.
CREATE TABLE IF NOT EXISTS notification_issues (
    instrument_id       TEXT        NOT NULL,
    error_fingerprint   TEXT        NOT NULL,
    github_repo         TEXT        NOT NULL,
    github_issue_number INT         NOT NULL,
    entity_count        INT         NOT NULL DEFAULT 1,
    first_seen_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    recent_entity_ids   TEXT[]      NOT NULL DEFAULT '{}',
    PRIMARY KEY (instrument_id, error_fingerprint, github_repo)
);

-- Silence rules: suppress notifications for a scoped window of time.
-- event_type  NULL = all event types for this instrument.
-- fingerprint NULL = all fingerprints of this event type.
CREATE TABLE IF NOT EXISTS notification_silences (
    id             SERIAL PRIMARY KEY,
    instrument_id  TEXT        NOT NULL,
    event_type     TEXT,
    fingerprint    TEXT,
    silenced_by    TEXT        NOT NULL,
    silenced_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at     TIMESTAMPTZ NOT NULL,
    reason         TEXT
);

CREATE INDEX IF NOT EXISTS idx_silences_active
    ON notification_silences(instrument_id, expires_at);
