CREATE TABLE pgqueue.jobs (
    id             uuid PRIMARY KEY DEFAULT uuidv7(),
    unique_key     text,
    queue          text NOT NULL,
    name           text NOT NULL,
    payload        jsonb NOT NULL DEFAULT 'null',
    status         text NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued', 'running', 'aborting', 'complete', 'failed', 'aborted')),
    priority       smallint NOT NULL DEFAULT 0,
    group_key      text,
    attempts       integer NOT NULL DEFAULT 0,
    max_attempts   integer NOT NULL DEFAULT 1,
    timeout_ms     bigint,
    heartbeat_ms   bigint,
    retry_delay_ms bigint NOT NULL DEFAULT 0,
    backoff        jsonb NOT NULL DEFAULT '{"type":"none"}',
    ttl_ms         bigint,
    scheduled_at   timestamptz NOT NULL DEFAULT now(),
    enqueued_at    timestamptz NOT NULL DEFAULT now(),
    started_at     timestamptz,
    touched_at     timestamptz,
    completed_at   timestamptz,
    expires_at     timestamptz,
    result         jsonb,
    error          text,
    meta           jsonb NOT NULL DEFAULT '{}',
    worker_id      uuid,
    kind           text NOT NULL DEFAULT 'job' CHECK (kind IN ('job', 'cron')),
    cron_expr      text,
    retried_at     timestamptz
);

CREATE UNIQUE INDEX jobs_unique_key_idx ON pgqueue.jobs (queue, unique_key)
    WHERE unique_key IS NOT NULL AND status IN ('queued', 'running', 'aborting');
CREATE INDEX jobs_dequeue_idx ON pgqueue.jobs (queue, priority, scheduled_at, id)
    WHERE status = 'queued';
CREATE INDEX jobs_dequeue_name_idx ON pgqueue.jobs (queue, name, priority, scheduled_at, id)
    WHERE status = 'queued';
CREATE INDEX jobs_queued_group_order_idx ON pgqueue.jobs
    (queue, group_key, priority, scheduled_at, id)
    WHERE status = 'queued' AND group_key IS NOT NULL;
CREATE UNIQUE INDEX jobs_running_group_unique_idx ON pgqueue.jobs (queue, group_key)
    WHERE status IN ('running', 'aborting') AND group_key IS NOT NULL;
CREATE INDEX jobs_running_idx ON pgqueue.jobs (queue)
    WHERE status IN ('running', 'aborting');
CREATE INDEX jobs_expires_idx ON pgqueue.jobs (expires_at)
    WHERE expires_at IS NOT NULL;
CREATE INDEX jobs_name_status_idx ON pgqueue.jobs (queue, name, status);
CREATE INDEX jobs_dashboard_status_page_idx ON pgqueue.jobs
    (queue, kind, status, enqueued_at DESC, id DESC);
CREATE INDEX jobs_dashboard_name_page_idx ON pgqueue.jobs
    (queue, kind, name, status, enqueued_at DESC, id DESC);
CREATE INDEX jobs_dashboard_ready_idx ON pgqueue.jobs (queue, scheduled_at, id)
    WHERE status = 'queued';
CREATE INDEX jobs_dashboard_failure_idx ON pgqueue.jobs
    (queue, completed_at DESC, id DESC) WHERE status = 'failed';
CREATE INDEX jobs_dashboard_execution_idx ON pgqueue.jobs (queue, status)
    WHERE status IN ('running', 'aborting');

-- Cron occurrence identity outlives the job row so result retention (including
-- immediate deletion) cannot make a completed or aborted occurrence eligible
-- for enqueue again. Claims only need to survive the scheduler's maximum
-- backfill grace, then the sweeper removes them.
CREATE TABLE pgqueue.cron_occurrences (
    queue        text NOT NULL,
    unique_key   text NOT NULL,
    scheduled_at timestamptz NOT NULL,
    expires_at   timestamptz NOT NULL,
    PRIMARY KEY (queue, unique_key, scheduled_at)
);

CREATE INDEX cron_occurrences_expiry_idx
    ON pgqueue.cron_occurrences (queue, expires_at);

CREATE TABLE pgqueue.cron_schedules (
    queue          text NOT NULL,
    unique_key     text NOT NULL,
    name           text NOT NULL,
    expression     text NOT NULL,
    definition     jsonb NOT NULL,
    revision       bigint NOT NULL CHECK (revision >= 0),
    misfire_policy text NOT NULL CHECK (misfire_policy IN ('skip', 'fire_once')),
    grace_ms       bigint CHECK (grace_ms IS NULL OR grace_ms >= 0),
    next_run_at    timestamptz NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (queue, unique_key)
);

CREATE INDEX cron_schedules_due_idx
    ON pgqueue.cron_schedules (queue, next_run_at, unique_key);

CREATE TABLE pgqueue.workers (
    id           uuid PRIMARY KEY,
    queue        text NOT NULL,
    stats        jsonb NOT NULL DEFAULT '{}',
    metadata     jsonb,
    started_at   timestamptz NOT NULL DEFAULT now(),
    heartbeat_at timestamptz NOT NULL DEFAULT now(),
    expires_at   timestamptz NOT NULL,
    accepting    boolean NOT NULL DEFAULT true
);

CREATE INDEX workers_queue_idx ON pgqueue.workers (queue, expires_at);
CREATE INDEX workers_dashboard_page_idx ON pgqueue.workers (queue, started_at, id);

-- A running or aborting job is stuck when its per-attempt timeout (plus the
-- sweep grace) has lapsed, its heartbeat deadline (plus the grace) has lapsed,
-- or — with neither deadline configured — the grace has passed and its owning
-- worker's lease has expired. Centralized so the sweeper's stuck scan, its
-- phase-one abort, the abandoned-retry guard, and the worker's late-finish
-- guard can never disagree on stuckness.
CREATE FUNCTION pgqueue.job_is_stuck(j pgqueue.jobs, grace_ms bigint)
RETURNS boolean
LANGUAGE sql
STABLE
AS $$
    SELECT (j.timeout_ms IS NOT NULL
            AND j.started_at + ((j.timeout_ms + grace_ms) * interval '1 millisecond') < now())
        OR (j.heartbeat_ms IS NOT NULL
            AND j.touched_at + ((j.heartbeat_ms + grace_ms) * interval '1 millisecond') < now())
        OR (j.timeout_ms IS NULL
            AND j.heartbeat_ms IS NULL
            AND j.touched_at + (grace_ms * interval '1 millisecond') < now()
            AND NOT EXISTS (
                SELECT 1 FROM pgqueue.workers w
                WHERE w.id = j.worker_id
                  AND w.queue = j.queue
                  AND w.expires_at > now()))
$$;
