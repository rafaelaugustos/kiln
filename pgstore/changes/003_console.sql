ALTER TABLE {s}.jobs ADD COLUMN progress smallint NOT NULL DEFAULT 0;

ALTER TABLE {s}.archive ADD COLUMN progress smallint NOT NULL DEFAULT 0;

CREATE SEQUENCE {s}.log_ids;

CREATE TABLE {s}.logs (
	job_id bigint NOT NULL,
	seq bigint NOT NULL DEFAULT nextval('{s}.log_ids'),
	attempt int NOT NULL,
	at timestamptz NOT NULL DEFAULT now(),
	text text NOT NULL,
	PRIMARY KEY (job_id, seq)
);
