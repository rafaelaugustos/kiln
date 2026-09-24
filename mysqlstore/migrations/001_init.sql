CREATE TABLE IF NOT EXISTS {p}jobs (
	id BIGINT NOT NULL,
	state ENUM('awaiting', 'scheduled', 'throttled', 'enqueued', 'processing', 'succeeded', 'failed', 'deleted') NOT NULL,
	queue VARCHAR(255) NOT NULL,
	kind VARCHAR(255) NOT NULL,
	priority SMALLINT NOT NULL DEFAULT 0,
	attempt INT NOT NULL DEFAULT 0,
	max_attempts INT NOT NULL,
	claim INT NOT NULL DEFAULT 0,
	timeout_ms BIGINT NOT NULL DEFAULT 0,
	deps_pending INT NOT NULL DEFAULT 0,
	run_at DATETIME(6) NOT NULL,
	created_at DATETIME(6) NOT NULL,
	attempted_at DATETIME(6) NULL,
	finalized_at DATETIME(6) NULL,
	server VARCHAR(255) NULL,
	cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
	batch_id BIGINT NULL,
	after_batch BIGINT NULL,
	parents JSON NULL,
	recurring_id VARCHAR(255) NULL,
	unique_key VARBINARY(64) NULL,
	limit_key VARCHAR(255) NULL,
	args LONGBLOB NOT NULL,
	meta JSON NULL,
	tags JSON NULL,
	history JSON NULL,
	PRIMARY KEY (id),
	KEY jobs_fetch (state, queue, priority DESC, id),
	KEY jobs_due (state, run_at, id, attempt),
	KEY jobs_running (state, server),
	KEY jobs_throttled (state, limit_key, priority DESC, id),
	KEY jobs_failed (state, finalized_at, id),
	KEY jobs_state (state),
	KEY jobs_batch (batch_id),
	KEY jobs_limit (limit_key)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_bin;

CREATE TABLE IF NOT EXISTS {p}archive (
	id BIGINT NOT NULL,
	state ENUM('awaiting', 'scheduled', 'throttled', 'enqueued', 'processing', 'succeeded', 'failed', 'deleted') NOT NULL,
	queue VARCHAR(255) NOT NULL,
	kind VARCHAR(255) NOT NULL,
	priority SMALLINT NOT NULL,
	attempt INT NOT NULL,
	max_attempts INT NOT NULL,
	claim INT NOT NULL,
	timeout_ms BIGINT NOT NULL,
	run_at DATETIME(6) NOT NULL,
	created_at DATETIME(6) NOT NULL,
	attempted_at DATETIME(6) NULL,
	finalized_at DATETIME(6) NOT NULL,
	server VARCHAR(255) NULL,
	batch_id BIGINT NULL,
	after_batch BIGINT NULL,
	parents JSON NULL,
	recurring_id VARCHAR(255) NULL,
	unique_key VARBINARY(64) NULL,
	limit_key VARCHAR(255) NULL,
	args LONGBLOB NOT NULL,
	meta JSON NULL,
	tags JSON NULL,
	history JSON NULL,
	output LONGBLOB NULL,
	PRIMARY KEY (id),
	KEY archive_state (state, finalized_at, id),
	KEY archive_batch (batch_id),
	KEY archive_limit (limit_key)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_bin;

CREATE TABLE IF NOT EXISTS {p}deps (
	batch BOOLEAN NOT NULL,
	parent_id BIGINT NOT NULL,
	job_id BIGINT NOT NULL,
	mask SMALLINT NOT NULL,
	resolved BOOLEAN NOT NULL DEFAULT FALSE,
	PRIMARY KEY (batch, parent_id, job_id),
	KEY deps_job (job_id),
	KEY deps_open (batch, resolved, parent_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_bin;

CREATE TABLE IF NOT EXISTS {p}uniques (
	unique_key VARBINARY(64) NOT NULL,
	job_id BIGINT NOT NULL,
	expires_at DATETIME(6) NULL,
	PRIMARY KEY (unique_key),
	KEY uniques_expires (expires_at)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_bin;

CREATE TABLE IF NOT EXISTS {p}limits (
	limit_key VARCHAR(255) NOT NULL,
	max INT NOT NULL,
	active INT NOT NULL DEFAULT 0,
	declared_at DATETIME(6) NULL,
	PRIMARY KEY (limit_key)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_bin;

CREATE TABLE IF NOT EXISTS {p}batches (
	id BIGINT NOT NULL AUTO_INCREMENT,
	description TEXT NOT NULL,
	meta JSON NULL,
	total BIGINT NOT NULL DEFAULT 0,
	sealed BOOLEAN NOT NULL DEFAULT FALSE,
	created_at DATETIME(6) NOT NULL,
	finished_at DATETIME(6) NULL,
	PRIMARY KEY (id),
	KEY batches_open (finished_at, sealed)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_bin;

CREATE TABLE IF NOT EXISTS {p}recurring (
	id VARCHAR(255) NOT NULL,
	spec VARCHAR(255) NOT NULL,
	location VARCHAR(255) NOT NULL,
	template JSON NOT NULL,
	misfire SMALLINT NOT NULL,
	overlap BOOLEAN NOT NULL,
	paused BOOLEAN NOT NULL DEFAULT FALSE,
	next_run_at DATETIME(6) NULL,
	last_run_at DATETIME(6) NULL,
	last_job_id BIGINT NULL,
	created_at DATETIME(6) NOT NULL,
	updated_at DATETIME(6) NOT NULL,
	version BIGINT NOT NULL,
	PRIMARY KEY (id),
	KEY recurring_due (paused, next_run_at)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_bin;

CREATE TABLE IF NOT EXISTS {p}servers (
	id VARCHAR(255) NOT NULL,
	host VARCHAR(255) NULL,
	pid INT NULL,
	version VARCHAR(255) NULL,
	queues JSON NULL,
	kinds JSON NULL,
	workers INT NULL,
	running INT NULL,
	started_at DATETIME(6) NULL,
	heartbeat_at DATETIME(6) NOT NULL,
	PRIMARY KEY (id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_bin;

CREATE TABLE IF NOT EXISTS {p}leases (
	name VARCHAR(255) NOT NULL,
	holder VARCHAR(255) NOT NULL,
	expires_at DATETIME(6) NOT NULL,
	acquired_at DATETIME(6) NOT NULL,
	PRIMARY KEY (name)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_bin;

CREATE TABLE IF NOT EXISTS {p}queues (
	name VARCHAR(255) NOT NULL,
	paused BOOLEAN NOT NULL DEFAULT FALSE,
	updated_at DATETIME(6) NOT NULL,
	PRIMARY KEY (name)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_bin;

CREATE TABLE IF NOT EXISTS {p}stats (
	bucket DATETIME NOT NULL,
	server VARCHAR(255) NOT NULL,
	succeeded BIGINT NOT NULL DEFAULT 0,
	failed BIGINT NOT NULL DEFAULT 0,
	deleted BIGINT NOT NULL DEFAULT 0,
	retried BIGINT NOT NULL DEFAULT 0,
	PRIMARY KEY (bucket, server)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_bin;

CREATE TABLE IF NOT EXISTS {p}sequences (
	name VARCHAR(32) NOT NULL,
	last_id BIGINT NOT NULL,
	PRIMARY KEY (name)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_bin;

INSERT IGNORE INTO {p}sequences (name, last_id) VALUES ('jobs', 0);
