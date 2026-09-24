CREATE TYPE {s}.state AS ENUM ('awaiting', 'scheduled', 'throttled', 'enqueued', 'processing', 'succeeded', 'failed', 'deleted');

CREATE SEQUENCE {s}.job_ids;
CREATE SEQUENCE {s}.batch_ids;

CREATE TABLE {s}.jobs (
	id bigint PRIMARY KEY DEFAULT nextval('{s}.job_ids'),
	state {s}.state NOT NULL,
	queue text NOT NULL,
	kind text NOT NULL,
	priority smallint NOT NULL DEFAULT 0,
	attempt int NOT NULL DEFAULT 0,
	max_attempts int NOT NULL,
	claim int NOT NULL DEFAULT 0,
	timeout_ms bigint NOT NULL DEFAULT 0,
	deps_pending int NOT NULL DEFAULT 0,
	run_at timestamptz NOT NULL,
	created_at timestamptz NOT NULL DEFAULT now(),
	attempted_at timestamptz,
	finalized_at timestamptz,
	server text,
	cancel_requested boolean NOT NULL DEFAULT false,
	batch_id bigint,
	after_batch bigint,
	parents bigint[],
	recurring_id text,
	unique_key bytea,
	limit_key text,
	args json NOT NULL,
	meta jsonb,
	tags text[],
	history jsonb
) WITH (
	fillfactor = 85,
	autovacuum_vacuum_scale_factor = 0,
	autovacuum_vacuum_threshold = 5000,
	autovacuum_vacuum_cost_delay = 0,
	autovacuum_analyze_scale_factor = 0.05
);

CREATE INDEX jobs_fetch ON {s}.jobs (queue, priority DESC, id) WHERE state = 'enqueued';
CREATE INDEX jobs_due ON {s}.jobs (run_at) WHERE state = 'scheduled';
CREATE INDEX jobs_running ON {s}.jobs (server) WHERE state = 'processing';
CREATE INDEX jobs_throttled ON {s}.jobs (limit_key, priority DESC, id) WHERE state = 'throttled';
CREATE INDEX jobs_failed ON {s}.jobs (finalized_at DESC, id DESC) WHERE state = 'failed';
CREATE INDEX jobs_awaiting ON {s}.jobs (id) WHERE state = 'awaiting';
CREATE INDEX jobs_batch ON {s}.jobs (batch_id) WHERE batch_id IS NOT NULL;
CREATE INDEX jobs_limit ON {s}.jobs (limit_key, state) WHERE limit_key IS NOT NULL;
CREATE INDEX jobs_stuck ON {s}.jobs (id) WHERE state = 'awaiting' AND deps_pending <= 0;
CREATE INDEX jobs_retries ON {s}.jobs (id) WHERE state = 'scheduled' AND attempt > 0;

CREATE TABLE {s}.archive (
	id bigint PRIMARY KEY,
	state {s}.state NOT NULL,
	queue text NOT NULL,
	kind text NOT NULL,
	priority smallint NOT NULL,
	attempt int NOT NULL,
	max_attempts int NOT NULL,
	claim int NOT NULL,
	timeout_ms bigint NOT NULL,
	run_at timestamptz NOT NULL,
	created_at timestamptz NOT NULL,
	attempted_at timestamptz,
	finalized_at timestamptz NOT NULL,
	server text,
	batch_id bigint,
	after_batch bigint,
	parents bigint[],
	recurring_id text,
	unique_key bytea,
	limit_key text,
	args json NOT NULL,
	meta jsonb,
	tags text[],
	history jsonb,
	output json
) WITH (autovacuum_vacuum_scale_factor = 0.05, autovacuum_analyze_scale_factor = 0.05);

CREATE INDEX archive_state ON {s}.archive (state, finalized_at DESC, id DESC);
CREATE INDEX archive_batch ON {s}.archive (batch_id) WHERE batch_id IS NOT NULL;
CREATE INDEX archive_limit ON {s}.archive (limit_key) WHERE limit_key IS NOT NULL;

CREATE TABLE {s}.deps (
	batch boolean NOT NULL,
	parent_id bigint NOT NULL,
	job_id bigint NOT NULL,
	mask smallint NOT NULL,
	resolved boolean NOT NULL DEFAULT false,
	PRIMARY KEY (batch, parent_id, job_id)
);

CREATE INDEX deps_job ON {s}.deps (job_id);
CREATE INDEX deps_open ON {s}.deps (batch, parent_id, job_id) WHERE NOT resolved;

CREATE TABLE {s}.uniques (
	key bytea PRIMARY KEY,
	job_id bigint NOT NULL,
	expires_at timestamptz
);

CREATE INDEX uniques_expires ON {s}.uniques (expires_at) WHERE expires_at IS NOT NULL;

CREATE TABLE {s}.limits (
	key text PRIMARY KEY,
	max int NOT NULL,
	active int NOT NULL DEFAULT 0 CHECK (active >= 0)
) WITH (fillfactor = 50);

CREATE TABLE {s}.batches (
	id bigint PRIMARY KEY DEFAULT nextval('{s}.batch_ids'),
	description text NOT NULL DEFAULT '',
	meta jsonb,
	total bigint NOT NULL DEFAULT 0,
	sealed boolean NOT NULL DEFAULT false,
	created_at timestamptz NOT NULL DEFAULT now(),
	finished_at timestamptz
);

CREATE TABLE {s}.recurring (
	id text PRIMARY KEY,
	spec text NOT NULL,
	location text NOT NULL,
	template jsonb NOT NULL,
	misfire smallint NOT NULL,
	overlap boolean NOT NULL,
	paused boolean NOT NULL DEFAULT false,
	next_run_at timestamptz,
	last_run_at timestamptz,
	last_job_id bigint,
	created_at timestamptz NOT NULL,
	updated_at timestamptz NOT NULL,
	version bigint NOT NULL
);

CREATE INDEX recurring_due ON {s}.recurring (next_run_at) WHERE NOT paused;

CREATE TABLE {s}.servers (
	id text PRIMARY KEY,
	host text,
	pid int,
	version text,
	queues text[],
	kinds text[],
	workers int,
	running int,
	started_at timestamptz,
	heartbeat_at timestamptz NOT NULL
);

CREATE TABLE {s}.leases (
	name text PRIMARY KEY,
	holder text NOT NULL,
	expires_at timestamptz NOT NULL,
	acquired_at timestamptz NOT NULL
);

CREATE TABLE {s}.queues (
	name text PRIMARY KEY,
	paused boolean NOT NULL DEFAULT false,
	updated_at timestamptz NOT NULL
);

CREATE TABLE {s}.stats (
	bucket timestamptz NOT NULL,
	server text NOT NULL,
	succeeded bigint NOT NULL DEFAULT 0,
	failed bigint NOT NULL DEFAULT 0,
	deleted bigint NOT NULL DEFAULT 0,
	retried bigint NOT NULL DEFAULT 0,
	PRIMARY KEY (bucket, server)
);

CREATE FUNCTION {s}.push(h jsonb, e jsonb) RETURNS jsonb LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
	SELECT CASE
		WHEN e IS NULL THEN h
		WHEN jsonb_array_length(coalesce(h, '[]')) >= 16 THEN (h - 0) || e
		ELSE coalesce(h, '[]') || e
	END
$$;

CREATE FUNCTION {s}.entry(st {s}.state, attempt int, reason text, err text, trace text, server text) RETURNS jsonb LANGUAGE sql STABLE PARALLEL SAFE AS $$
	SELECT CASE WHEN coalesce(reason, '') = '' THEN NULL ELSE jsonb_strip_nulls(jsonb_build_object(
		'at', now(), 'state', st, 'attempt', attempt, 'reason', reason,
		'error', nullif(err, ''), 'trace', nullif(trace, ''), 'server', server
	)) END
$$;

CREATE FUNCTION {s}.ready(run_at timestamptz, limit_key text) RETURNS {s}.state LANGUAGE sql VOLATILE AS $$
	SELECT CASE
		WHEN run_at > clock_timestamp() THEN 'scheduled'::{s}.state
		WHEN limit_key IS NOT NULL THEN 'throttled'::{s}.state
		ELSE 'enqueued'::{s}.state
	END
$$;

CREATE FUNCTION {s}.bit(st {s}.state) RETURNS smallint LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
	SELECT CASE st WHEN 'succeeded' THEN 1 WHEN 'failed' THEN 2 WHEN 'deleted' THEN 4 ELSE 0 END::smallint
$$;

CREATE FUNCTION {s}.raise(code text, msg text) RETURNS boolean LANGUAGE plpgsql AS $$
BEGIN
	RAISE EXCEPTION USING ERRCODE = code, MESSAGE = msg;
END
$$;
