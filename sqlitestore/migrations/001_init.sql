CREATE TABLE {p}jobs (
	id INTEGER PRIMARY KEY,
	state TEXT NOT NULL,
	queue TEXT NOT NULL,
	kind TEXT NOT NULL,
	priority INTEGER NOT NULL DEFAULT 0,
	attempt INTEGER NOT NULL DEFAULT 0,
	max_attempts INTEGER NOT NULL,
	claim INTEGER NOT NULL DEFAULT 0,
	timeout_ms INTEGER NOT NULL DEFAULT 0,
	deps_pending INTEGER NOT NULL DEFAULT 0,
	run_at INTEGER NOT NULL,
	created_at INTEGER NOT NULL,
	attempted_at INTEGER,
	finalized_at INTEGER,
	server TEXT,
	cancel_requested INTEGER NOT NULL DEFAULT 0,
	batch_id INTEGER,
	after_batch INTEGER,
	parents TEXT,
	recurring_id TEXT,
	unique_key BLOB,
	limit_key TEXT,
	args TEXT NOT NULL,
	meta TEXT,
	tags TEXT,
	history TEXT
);

CREATE INDEX {p}jobs_fetch ON {p}jobs (queue, priority DESC, id) WHERE state = 'enqueued';

CREATE INDEX {p}jobs_due ON {p}jobs (run_at) WHERE state = 'scheduled';

CREATE INDEX {p}jobs_running ON {p}jobs (server) WHERE state = 'processing';

CREATE INDEX {p}jobs_throttled ON {p}jobs (limit_key, priority DESC, id) WHERE state = 'throttled';

CREATE INDEX {p}jobs_failed ON {p}jobs (finalized_at) WHERE state = 'failed';

CREATE INDEX {p}jobs_awaiting ON {p}jobs (id) WHERE state = 'awaiting';

CREATE INDEX {p}jobs_batch ON {p}jobs (batch_id) WHERE batch_id IS NOT NULL;

CREATE INDEX {p}jobs_limit ON {p}jobs (limit_key) WHERE limit_key IS NOT NULL;

CREATE TABLE {p}archive (
	id INTEGER PRIMARY KEY,
	state TEXT NOT NULL,
	queue TEXT NOT NULL,
	kind TEXT NOT NULL,
	priority INTEGER NOT NULL,
	attempt INTEGER NOT NULL,
	max_attempts INTEGER NOT NULL,
	claim INTEGER NOT NULL,
	timeout_ms INTEGER NOT NULL,
	run_at INTEGER NOT NULL,
	created_at INTEGER NOT NULL,
	attempted_at INTEGER,
	finalized_at INTEGER NOT NULL,
	server TEXT,
	batch_id INTEGER,
	after_batch INTEGER,
	parents TEXT,
	recurring_id TEXT,
	unique_key BLOB,
	limit_key TEXT,
	args TEXT NOT NULL,
	meta TEXT,
	tags TEXT,
	history TEXT,
	output TEXT
);

CREATE INDEX {p}archive_state ON {p}archive (state, finalized_at);

CREATE INDEX {p}archive_batch ON {p}archive (batch_id) WHERE batch_id IS NOT NULL;

CREATE INDEX {p}archive_limit ON {p}archive (limit_key) WHERE limit_key IS NOT NULL;

CREATE TABLE {p}deps (
	batch INTEGER NOT NULL,
	parent_id INTEGER NOT NULL,
	job_id INTEGER NOT NULL,
	mask INTEGER NOT NULL,
	resolved INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (batch, parent_id, job_id)
) WITHOUT ROWID;

CREATE INDEX {p}deps_job ON {p}deps (job_id);

CREATE INDEX {p}deps_open ON {p}deps (batch, parent_id) WHERE resolved = 0;

CREATE TABLE {p}uniques (
	unique_key BLOB PRIMARY KEY,
	job_id INTEGER NOT NULL,
	expires_at INTEGER
) WITHOUT ROWID;

CREATE INDEX {p}uniques_expires ON {p}uniques (expires_at) WHERE expires_at IS NOT NULL;

CREATE TABLE {p}limits (
	limit_key TEXT PRIMARY KEY,
	max INTEGER NOT NULL,
	active INTEGER NOT NULL DEFAULT 0
) WITHOUT ROWID;

CREATE TABLE {p}batches (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	description TEXT NOT NULL,
	meta TEXT,
	total INTEGER NOT NULL DEFAULT 0,
	sealed INTEGER NOT NULL DEFAULT 0,
	created_at INTEGER NOT NULL,
	finished_at INTEGER
);

CREATE INDEX {p}batches_idle ON {p}batches (id) WHERE finished_at IS NULL AND sealed;

CREATE TABLE {p}recurring (
	id TEXT PRIMARY KEY,
	spec TEXT NOT NULL,
	location TEXT NOT NULL,
	template TEXT NOT NULL,
	misfire INTEGER NOT NULL,
	overlap INTEGER NOT NULL,
	paused INTEGER NOT NULL DEFAULT 0,
	next_run_at INTEGER,
	last_run_at INTEGER,
	last_job_id INTEGER,
	created_at INTEGER NOT NULL,
	updated_at INTEGER NOT NULL,
	version INTEGER NOT NULL
) WITHOUT ROWID;

CREATE INDEX {p}recurring_due ON {p}recurring (next_run_at) WHERE NOT paused;

CREATE TABLE {p}servers (
	id TEXT PRIMARY KEY,
	host TEXT,
	pid INTEGER,
	version TEXT,
	queues TEXT,
	kinds TEXT,
	workers INTEGER,
	running INTEGER,
	started_at INTEGER,
	heartbeat_at INTEGER NOT NULL
) WITHOUT ROWID;

CREATE TABLE {p}leases (
	name TEXT PRIMARY KEY,
	holder TEXT NOT NULL,
	expires_at INTEGER NOT NULL,
	acquired_at INTEGER NOT NULL
) WITHOUT ROWID;

CREATE TABLE {p}queues (
	name TEXT PRIMARY KEY,
	paused INTEGER NOT NULL DEFAULT 0,
	updated_at INTEGER NOT NULL
) WITHOUT ROWID;

CREATE TABLE {p}stats (
	bucket INTEGER NOT NULL,
	server TEXT NOT NULL,
	succeeded INTEGER NOT NULL DEFAULT 0,
	failed INTEGER NOT NULL DEFAULT 0,
	deleted INTEGER NOT NULL DEFAULT 0,
	retried INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (bucket, server)
) WITHOUT ROWID;

CREATE TABLE {p}sequences (
	name TEXT PRIMARY KEY,
	last_id INTEGER NOT NULL
) WITHOUT ROWID;

INSERT INTO {p}sequences (name, last_id) VALUES ('jobs', 0);
