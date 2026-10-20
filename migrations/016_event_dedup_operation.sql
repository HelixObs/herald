-- Record which operation emitted each deduplicated event, and dedup per operation.
--
-- Notification fingerprints include the operation name (the emitting span's
-- name), and the alerts list recomputes fingerprints from this table, so it
-- needs the operation too. The operation is also part of the primary key, so
-- the same message on one entity from two operations stays two rows instead
-- of one row labelled with whichever operation came last.
--
-- Rows written before this migration keep operation = '' until they age out;
-- the alerts list only looks back 7 days.
--
-- Safe to re-run: the herald init container applies every migration on each
-- deploy.
ALTER TABLE entity_event_dedup ADD COLUMN IF NOT EXISTS operation TEXT NOT NULL DEFAULT '';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY (c.conkey)
        WHERE c.conrelid = 'entity_event_dedup'::regclass
          AND c.contype = 'p'
          AND a.attname = 'operation'
    ) THEN
        ALTER TABLE entity_event_dedup DROP CONSTRAINT IF EXISTS entity_event_dedup_pkey;
        ALTER TABLE entity_event_dedup
            ADD CONSTRAINT entity_event_dedup_pkey
            PRIMARY KEY (entity_id, event_name, normalised_message, operation);
    END IF;
END
$$;
