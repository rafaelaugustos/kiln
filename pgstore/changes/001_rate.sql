ALTER TABLE {s}.jobs ADD COLUMN granted boolean NOT NULL DEFAULT false;

ALTER TABLE {s}.limits
	ADD COLUMN rate int NOT NULL DEFAULT 0,
	ADD COLUMN per_us bigint NOT NULL DEFAULT 0,
	ADD COLUMN burst int NOT NULL DEFAULT 0,
	ADD COLUMN tat timestamptz;

CREATE INDEX IF NOT EXISTS jobs_granted ON {s}.jobs (limit_key) WHERE state = 'throttled' AND granted;
