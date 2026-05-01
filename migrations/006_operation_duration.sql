-- Add duration_ns to entity_operations so the gateway can store how long each
-- operation span lasted. NULL for rows written before this migration.
ALTER TABLE entity_operations
    ADD COLUMN IF NOT EXISTS duration_ns BIGINT;
