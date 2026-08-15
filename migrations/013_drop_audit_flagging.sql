-- Drop the operator-review "flagged" mechanism from sherlock_audit — added
-- in 012 without a real workflow behind it (no UI, no trigger an operator
-- would actually use). Removed rather than left half-built; re-added once
-- there's an actual story for how flagging should work.
--
-- filter_hit is unrelated and untouched — that's the guardrail's own
-- signal for whether it redacted something, not an operator-review field.

ALTER TABLE sherlock_audit DROP COLUMN IF EXISTS flagged;
ALTER TABLE sherlock_audit DROP COLUMN IF EXISTS flag_reason;

DROP INDEX IF EXISTS idx_sherlock_audit_flagged;
