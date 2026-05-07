-- Index for has_error EXISTS subquery in QueryEntityGraph.
-- The query does: WHERE entity_id = $1 AND event_name = 'helix.error'
-- The existing idx_entity_events_entity_time only covers entity_id,
-- forcing a scan of all events per entity to find errors.
CREATE INDEX IF NOT EXISTS idx_entity_events_entity_name
    ON entity_events (entity_id, event_name);
