ALTER TABLE {p}batches ADD COLUMN parent_id BIGINT NULL, ALGORITHM = INSTANT;

ALTER TABLE {p}batches ADD INDEX batches_parent (parent_id, id), ADD INDEX batches_unfinished (parent_id, finished_at),
	ALGORITHM = INPLACE, LOCK = NONE;
