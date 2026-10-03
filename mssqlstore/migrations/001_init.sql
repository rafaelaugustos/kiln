CREATE SEQUENCE {p}job_ids AS BIGINT START WITH 1 INCREMENT BY 1 CACHE 1000;

CREATE SEQUENCE {p}batch_ids AS BIGINT START WITH 1 INCREMENT BY 1 CACHE 100;

CREATE TABLE {p}jobs (
	id BIGINT NOT NULL,
	state VARCHAR(10) NOT NULL,
	queue NVARCHAR(255) COLLATE Latin1_General_100_BIN2 NOT NULL,
	kind NVARCHAR(255) COLLATE Latin1_General_100_BIN2 NOT NULL,
	priority SMALLINT NOT NULL DEFAULT 0,
	attempt INT NOT NULL DEFAULT 0,
	max_attempts INT NOT NULL,
	claim INT NOT NULL DEFAULT 0,
	timeout_ms BIGINT NOT NULL DEFAULT 0,
	deps_pending INT NOT NULL DEFAULT 0,
	run_at DATETIME2(6) NOT NULL,
	created_at DATETIME2(6) NOT NULL,
	attempted_at DATETIME2(6) NULL,
	finalized_at DATETIME2(6) NULL,
	server NVARCHAR(255) COLLATE Latin1_General_100_BIN2 NULL,
	cancel_requested BIT NOT NULL DEFAULT 0,
	granted BIT NOT NULL DEFAULT 0,
	batch_id BIGINT NULL,
	after_batch BIGINT NULL,
	parents NVARCHAR(MAX) NULL,
	recurring_id NVARCHAR(255) COLLATE Latin1_General_100_BIN2 NULL,
	unique_key VARBINARY(64) NULL,
	limit_key NVARCHAR(255) COLLATE Latin1_General_100_BIN2 NULL,
	args NVARCHAR(MAX) NOT NULL,
	meta NVARCHAR(MAX) NULL,
	tags NVARCHAR(MAX) NULL,
	history NVARCHAR(MAX) NULL,
	PRIMARY KEY CLUSTERED (id) WITH (OPTIMIZE_FOR_SEQUENTIAL_KEY = ON)
);

CREATE INDEX jobs_fetch ON {p}jobs (state, queue, priority DESC, id);

CREATE INDEX jobs_due ON {p}jobs (state, run_at) INCLUDE (attempt);

CREATE INDEX jobs_running ON {p}jobs (state, server) INCLUDE (claim, cancel_requested, attempted_at);

CREATE INDEX jobs_throttled ON {p}jobs (state, limit_key, priority DESC, id) INCLUDE (queue, granted);

CREATE INDEX jobs_granted ON {p}jobs (state, granted, limit_key);

CREATE INDEX jobs_failed ON {p}jobs (state, finalized_at);

CREATE INDEX jobs_state ON {p}jobs (state);

CREATE INDEX jobs_batch ON {p}jobs (batch_id);

CREATE INDEX jobs_limit ON {p}jobs (limit_key);

CREATE TABLE {p}archive (
	id BIGINT NOT NULL,
	state VARCHAR(10) NOT NULL,
	queue NVARCHAR(255) COLLATE Latin1_General_100_BIN2 NOT NULL,
	kind NVARCHAR(255) COLLATE Latin1_General_100_BIN2 NOT NULL,
	priority SMALLINT NOT NULL,
	attempt INT NOT NULL,
	max_attempts INT NOT NULL,
	claim INT NOT NULL,
	timeout_ms BIGINT NOT NULL,
	run_at DATETIME2(6) NOT NULL,
	created_at DATETIME2(6) NOT NULL,
	attempted_at DATETIME2(6) NULL,
	finalized_at DATETIME2(6) NOT NULL,
	server NVARCHAR(255) COLLATE Latin1_General_100_BIN2 NULL,
	batch_id BIGINT NULL,
	after_batch BIGINT NULL,
	parents NVARCHAR(MAX) NULL,
	recurring_id NVARCHAR(255) COLLATE Latin1_General_100_BIN2 NULL,
	unique_key VARBINARY(64) NULL,
	limit_key NVARCHAR(255) COLLATE Latin1_General_100_BIN2 NULL,
	args NVARCHAR(MAX) NOT NULL,
	meta NVARCHAR(MAX) NULL,
	tags NVARCHAR(MAX) NULL,
	history NVARCHAR(MAX) NULL,
	[output] NVARCHAR(MAX) NULL,
	PRIMARY KEY CLUSTERED (id)
);

CREATE INDEX archive_state ON {p}archive (state, finalized_at);

CREATE INDEX archive_batch ON {p}archive (batch_id);

CREATE INDEX archive_limit ON {p}archive (limit_key);

CREATE TABLE {p}deps (
	batch BIT NOT NULL,
	parent_id BIGINT NOT NULL,
	job_id BIGINT NOT NULL,
	mask SMALLINT NOT NULL,
	resolved BIT NOT NULL DEFAULT 0,
	PRIMARY KEY CLUSTERED (batch, parent_id, job_id)
);

CREATE INDEX deps_job ON {p}deps (job_id);

CREATE INDEX deps_open ON {p}deps (batch, resolved, parent_id);

CREATE TABLE {p}uniques (
	unique_key VARBINARY(64) NOT NULL,
	job_id BIGINT NOT NULL,
	expires_at DATETIME2(6) NULL,
	PRIMARY KEY CLUSTERED (unique_key)
);

CREATE INDEX uniques_expires ON {p}uniques (expires_at);

CREATE TABLE {p}limits (
	limit_key NVARCHAR(255) COLLATE Latin1_General_100_BIN2 NOT NULL,
	[max] INT NOT NULL,
	active INT NOT NULL DEFAULT 0,
	rate INT NOT NULL DEFAULT 0,
	per_us BIGINT NOT NULL DEFAULT 0,
	burst INT NOT NULL DEFAULT 0,
	tat DATETIME2(6) NULL,
	admit_tat DATETIME2(6) NULL,
	declared_at DATETIME2(6) NULL,
	PRIMARY KEY CLUSTERED (limit_key)
);

CREATE TABLE {p}batches (
	id BIGINT NOT NULL DEFAULT (NEXT VALUE FOR {p}batch_ids),
	description NVARCHAR(MAX) NOT NULL,
	meta NVARCHAR(MAX) NULL,
	total BIGINT NOT NULL DEFAULT 0,
	sealed BIT NOT NULL DEFAULT 0,
	created_at DATETIME2(6) NOT NULL,
	finished_at DATETIME2(6) NULL,
	PRIMARY KEY CLUSTERED (id)
);

CREATE INDEX batches_open ON {p}batches (finished_at, sealed);

CREATE TABLE {p}recurring (
	id NVARCHAR(255) COLLATE Latin1_General_100_BIN2 NOT NULL,
	spec NVARCHAR(255) NOT NULL,
	location NVARCHAR(255) NOT NULL,
	template NVARCHAR(MAX) NOT NULL,
	misfire SMALLINT NOT NULL,
	overlap BIT NOT NULL,
	paused BIT NOT NULL DEFAULT 0,
	next_run_at DATETIME2(6) NULL,
	last_run_at DATETIME2(6) NULL,
	last_job_id BIGINT NULL,
	created_at DATETIME2(6) NOT NULL,
	updated_at DATETIME2(6) NOT NULL,
	version BIGINT NOT NULL,
	PRIMARY KEY CLUSTERED (id)
);

CREATE INDEX recurring_due ON {p}recurring (paused, next_run_at);

CREATE TABLE {p}servers (
	id NVARCHAR(255) COLLATE Latin1_General_100_BIN2 NOT NULL,
	host NVARCHAR(255) NULL,
	pid INT NULL,
	version NVARCHAR(255) NULL,
	queues NVARCHAR(MAX) NULL,
	kinds NVARCHAR(MAX) NULL,
	workers INT NULL,
	running INT NULL,
	started_at DATETIME2(6) NULL,
	heartbeat_at DATETIME2(6) NOT NULL,
	PRIMARY KEY CLUSTERED (id)
);

CREATE TABLE {p}leases (
	name NVARCHAR(255) COLLATE Latin1_General_100_BIN2 NOT NULL,
	holder NVARCHAR(255) COLLATE Latin1_General_100_BIN2 NOT NULL,
	expires_at DATETIME2(6) NOT NULL,
	acquired_at DATETIME2(6) NOT NULL,
	PRIMARY KEY CLUSTERED (name)
);

CREATE TABLE {p}queues (
	name NVARCHAR(255) COLLATE Latin1_General_100_BIN2 NOT NULL,
	paused BIT NOT NULL DEFAULT 0,
	updated_at DATETIME2(6) NOT NULL,
	PRIMARY KEY CLUSTERED (name)
);

CREATE TABLE {p}stats (
	bucket DATETIME2(0) NOT NULL,
	server NVARCHAR(255) COLLATE Latin1_General_100_BIN2 NOT NULL,
	succeeded BIGINT NOT NULL DEFAULT 0,
	failed BIGINT NOT NULL DEFAULT 0,
	deleted BIGINT NOT NULL DEFAULT 0,
	retried BIGINT NOT NULL DEFAULT 0,
	PRIMARY KEY CLUSTERED (bucket, server)
);

CREATE TABLE {p}schema_changes (
	name NVARCHAR(255) NOT NULL,
	applied_at DATETIME2(6) NOT NULL,
	PRIMARY KEY CLUSTERED (name)
);
