ALTER TABLE {s}.batches ADD COLUMN parent_id bigint;

CREATE INDEX batches_parent ON {s}.batches (parent_id, id) WHERE parent_id IS NOT NULL;

CREATE INDEX batches_unfinished ON {s}.batches (parent_id) WHERE parent_id IS NOT NULL AND finished_at IS NULL;
