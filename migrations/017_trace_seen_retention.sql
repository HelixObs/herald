-- Keep 30 days of operation_trace_seen.
--
-- The seen-set only has to catch a span delivered twice, which happens within
-- minutes of the first delivery (exporter retries). A later run of the same
-- operation starts a new trace, so it is never caught here. Unpruned, the
-- table grew past 97M rows and its primary key outgrew the database cache,
-- which made every operation write read from disk.
--
-- The first run empties the table rather than deleting the backlog row by row.
-- The created_at index marks that as done.
--
-- Safe to re-run: the herald init container applies every migration on each
-- deploy.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE tablename = 'operation_trace_seen'
          AND indexname = 'idx_operation_trace_seen_created_at'
    ) THEN
        TRUNCATE operation_trace_seen;
        CREATE INDEX idx_operation_trace_seen_created_at
            ON operation_trace_seen (created_at);
    END IF;
END
$$;

CREATE OR REPLACE PROCEDURE prune_operation_trace_seen(job_id INT, config JSONB)
LANGUAGE SQL AS $$
    DELETE FROM operation_trace_seen
    WHERE created_at < now() - (config->>'keep')::INTERVAL;
$$;

SELECT add_job('prune_operation_trace_seen', INTERVAL '1 hour',
               config => '{"keep": "30 days"}')
WHERE NOT EXISTS (
    SELECT 1 FROM timescaledb_information.jobs
    WHERE proc_name = 'prune_operation_trace_seen'
);
