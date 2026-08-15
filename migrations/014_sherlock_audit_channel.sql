-- Add the Slack channel (resolved to a human-readable name, not the raw
-- Slack channel ID) to sherlock_audit, so it's directly filterable instead
-- of only recoverable by parsing session_id (slack:{channel}:{thread_ts})
-- for Slack sessions, and not at all for web sessions.

ALTER TABLE sherlock_audit ADD COLUMN IF NOT EXISTS channel TEXT;
