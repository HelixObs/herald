-- GIN index on entities.parent_ids for the descendants CTE (@> containment check).
-- Without this, each recursive step does a full sequential scan of the entities table.
CREATE INDEX IF NOT EXISTS entities_parent_ids_gin ON entities USING GIN (parent_ids);

-- Composite index on entity_events for the has_error EXISTS subquery in QueryEntityGraph.
CREATE INDEX IF NOT EXISTS entity_events_entity_event_idx ON entity_events (entity_id, event_name);
