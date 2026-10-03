ALTER TABLE {p}batches ADD COLUMN parent_id INTEGER;

CREATE INDEX {p}batches_parent ON {p}batches (parent_id, id) WHERE parent_id IS NOT NULL;

CREATE INDEX {p}batches_unfinished ON {p}batches (parent_id) WHERE parent_id IS NOT NULL AND finished_at IS NULL;
