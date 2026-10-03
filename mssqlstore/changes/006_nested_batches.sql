ALTER TABLE {p}batches ADD parent_id BIGINT NULL;

CREATE INDEX batches_parent ON {p}batches (parent_id, id);

CREATE INDEX batches_unfinished ON {p}batches (parent_id, finished_at);
