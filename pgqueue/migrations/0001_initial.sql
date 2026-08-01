CREATE TABLE pgqueue.jobs (
    id             uuid PRIMARY KEY DEFAULT uuidv7(),
    -- 255 bytes is `MAX_INDEXED_KEY_BYTES`, the bound `JobRequest::validate`
    -- and `validate_queue_name` hold every Rust writer to (a cron name caps at
    -- 250 only because its derived dedupe key is `cron:{name}` — the stored
    -- columns share one limit). The database repeats it because foreign SQL
    -- writers exist by design (see the enqueue-lock fallback in `database.rs`),
    -- and without it the failure was deferred: the dedupe index is partial, so
    -- a terminal row carrying an oversized key landed silently, and the first
    -- `retry_job_occurrence` to copy that key onto a live row raised `54000`
    -- from inside the B-tree — permanently, for that job. `queue` and `name`
    -- sit in full indexes, so their oversized writes at least failed at
    -- insert, but only past the ~2704-byte tuple limit and with the same
    -- internals error. `octet_length`, not `length`: the Rust limits are byte
    -- lengths. `queue` and `name` must also be non-empty, exactly as the
    -- validators require; an empty dedupe key has no Rust-side rule, so it
    -- gets none here.
    dedupe_key     text
        CHECK (dedupe_key IS NULL OR octet_length(dedupe_key) <= 255),
    queue          text NOT NULL CHECK (octet_length(queue) BETWEEN 1 AND 255),
    name           text NOT NULL CHECK (octet_length(name) BETWEEN 1 AND 255),
    payload        jsonb NOT NULL DEFAULT 'null',
    status         text NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued', 'running', 'aborting', 'complete', 'failed', 'aborted')),
    priority       smallint NOT NULL DEFAULT 0,
    attempts       integer NOT NULL DEFAULT 0,
    max_attempts   integer NOT NULL DEFAULT 1,
    -- NULL = no timeout. Zero and negative values have no encoding — the macro
    -- already reads `timeout_ms = 0` as "unlimited" — and `JobRow::timeout`
    -- would decode either as a zero-length deadline, which cancels every
    -- attempt before its handler runs a statement.
    --
    -- The upper bound is `MAX_DURATION_MS`, the same window `validate_duration`
    -- holds every Rust writer to. Unbounded, `pgqueue.job_is_stuck` computed
    -- `timeout_ms + grace_ms` in bigint over *every* active row of a queue, so
    -- one row near `bigint`'s ceiling raised `22003` and took stuck-job
    -- recovery down for that whole queue, permanently.
    timeout_ms     bigint
        CHECK (timeout_ms IS NULL OR timeout_ms BETWEEN 1 AND 3153600000000),
    retry_delay_ms bigint NOT NULL DEFAULT 0,
    -- The `JobRetryBackoff` tag. A value this build cannot decode is claimed
    -- and then dropped by `Decode` (see `job.rs`), because the dequeue
    -- statement has already committed by the time the client reads the row;
    -- refusing to store one keeps that fallback for rows that predate this
    -- check or come from a newer version, rather than a way to write new ones.
    -- `COALESCE`, not a bare `IN`: `->>` answers NULL for a missing key and for
    -- every non-object, and a NULL predicate is not FALSE, so `{"delay":5}`
    -- and a bare `null` would both satisfy the check they are written to fail.
    backoff        jsonb NOT NULL DEFAULT '{"type":"none"}'
        CHECK (COALESCE(backoff ->> 'type', '') IN ('none', 'exponential')),
    -- NULL = keep forever, 0 = delete on finish. A negative value has no
    -- encoding, and `JobRetention::from_result_ttl_ms` would decode it as a
    -- live retention rather than an immediate delete. The upper bound is
    -- `MAX_DURATION_MS` for the same reason as `timeout_ms` above: finishing a
    -- row whose retention was near `bigint`'s ceiling raised `22008` from the
    -- `now() + (result_ttl_ms * interval '1 millisecond')` that finish and
    -- abort both compute.
    result_ttl_ms  bigint
        CHECK (result_ttl_ms IS NULL OR result_ttl_ms BETWEEN 0 AND 3153600000000),
    scheduled_at   timestamptz NOT NULL DEFAULT clock_timestamp(),
    enqueued_at    timestamptz NOT NULL DEFAULT clock_timestamp(),
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

CREATE UNIQUE INDEX jobs_dedupe_key_idx ON pgqueue.jobs (queue, dedupe_key)
    WHERE dedupe_key IS NOT NULL AND status IN ('queued', 'running', 'aborting');
CREATE INDEX jobs_dequeue_idx ON pgqueue.jobs (queue, priority, scheduled_at, id)
    WHERE status = 'queued';
CREATE INDEX jobs_dequeue_name_idx ON pgqueue.jobs (queue, name, priority, scheduled_at, id)
    WHERE status = 'queued';
CREATE INDEX jobs_expires_idx ON pgqueue.jobs (queue, expires_at, id)
    WHERE expires_at IS NOT NULL;
CREATE INDEX jobs_page_idx ON pgqueue.jobs (queue, enqueued_at DESC, id DESC);
CREATE INDEX jobs_dashboard_status_page_idx ON pgqueue.jobs
    (queue, kind, status, enqueued_at DESC, id DESC);
CREATE INDEX jobs_dashboard_name_page_idx ON pgqueue.jobs
    (queue, kind, name, status, enqueued_at DESC, id DESC);
-- The job-name typeahead filters on a case-folded prefix. `text_pattern_ops`
-- lets the planner turn `starts_with(lower(name), prefix)` into a range scan;
-- without it the prefix is only a filter, so every keystroke reads every
-- retained row of all six status partitions.
CREATE INDEX jobs_dashboard_name_prefix_idx ON pgqueue.jobs
    (queue, kind, status, lower(name) text_pattern_ops, enqueued_at DESC, id DESC);
CREATE INDEX jobs_dashboard_ready_idx ON pgqueue.jobs (queue, scheduled_at, id)
    WHERE status = 'queued';
CREATE INDEX jobs_dashboard_failure_idx ON pgqueue.jobs
    (queue, completed_at DESC, id DESC) WHERE status = 'failed';
-- Active jobs: the sweeper scans them per queue, the dashboard probes single
-- statuses. Both fit one partial index.
CREATE INDEX jobs_active_idx ON pgqueue.jobs (queue, status)
    WHERE status IN ('running', 'aborting');

-- Cron occurrence identity outlives the job row so result retention (including
-- immediate deletion) cannot make a completed or aborted occurrence eligible
-- for enqueue again. Claims only need to survive the scheduler's maximum
-- backfill grace, then the sweeper removes them.
CREATE TABLE pgqueue.cron_occurrences (
    queue        text NOT NULL,
    dedupe_key   text NOT NULL,
    scheduled_at timestamptz NOT NULL,
    expires_at   timestamptz NOT NULL,
    PRIMARY KEY (queue, dedupe_key, scheduled_at)
);

CREATE INDEX cron_occurrences_expiry_idx
    ON pgqueue.cron_occurrences (queue, expires_at);

CREATE TABLE pgqueue.cron_schedules (
    queue          text NOT NULL,
    dedupe_key     text NOT NULL,
    name           text NOT NULL,
    expression     text NOT NULL,
    definition     jsonb NOT NULL,
    revision       bigint NOT NULL CHECK (revision >= 0),
    misfire_policy text NOT NULL CHECK (misfire_policy IN ('skip', 'fire_once')),
    grace_ms       bigint CHECK (grace_ms IS NULL OR grace_ms >= 0),
    next_run_at    timestamptz NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (queue, dedupe_key)
);

-- No due-time index: every access to this table is by its primary key
-- `(queue, dedupe_key)`, and the `next_run_at <= now()` test is a filter on an
-- already-located row. An index on `next_run_at` would only add write cost to
-- the two statements that advance it on every tick.

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

CREATE INDEX workers_queue_idx ON pgqueue.workers (queue, expires_at, id);
CREATE INDEX workers_dashboard_page_idx ON pgqueue.workers (queue, started_at, id);

-- Two independent, *additive* reasons an attempt is recoverable:
--
--   1. Its configured timeout elapsed. This bounds an attempt even while its
--      worker is alive and healthy.
--   2. Its owner is provably gone — the `pgqueue.workers` lease that covered
--      the attempt lapsed, and has stayed lapsed for the liveness grace.
--
-- Gating (2) on the absence of a timeout made the two mutually exclusive, so
-- setting `timeout_ms` *weakened* crash recovery: a SIGKILLed worker's hour-long
-- attempt stayed `running` for the full hour, holding its dedupe key (which
-- silently deduplicates every re-enqueue and cron occurrence) and leaving
-- `abort_job` stranded in `aborting` with nobody alive to finish it.
--
-- The grace in (2) is measured from the *lease*, not from the attempt. Both
-- clocks are needed: `COALESCE(touched_at, started_at)` is the only one a
-- leaseless consumer has, but on its own it made a long attempt sweepable the
-- instant its owner missed one heartbeat window — a workers-row lock wait, a
-- pool stall, a GC pause or a failover was enough to cancel and re-run work
-- that was still in flight, where before it had the whole `timeout + grace`.
-- Waiting `grace_ms` past the lease's `expires_at` gives a stalled heartbeat the
-- same cushion a slow finish gets. `expires_at > now()` implies
-- `expires_at + grace_ms > now()` for a non-negative grace, so this single
-- predicate still carries the "no live lease" requirement; `sweep_grace` is
-- validated non-negative before it ever reaches here.
--
-- Worker rows are purged on the same grace (see `Sweeper::purge_worker_leases`)
-- so a lease that has just lapsed is still on disk to be seen — a deleted row
-- is indistinguishable from one that lapsed an hour ago.
--
-- This opens no double-execution hole: every caller re-checks for a live lease
-- and guards on `attempts`/`worker_id`, so a resurrected owner's `finish` is a
-- no-op.
--
-- The lease is a *parameter*, not a lookup inside the body. `inline_function`
-- refuses to inline any SQL function whose body has a sublink, and this one is
-- applied to every `running`/`aborting` row of a queue by
-- `Sweeper::recover_stuck_jobs` — unbounded by the sweep batch size, and with an
-- `ORDER BY` that forbids an early exit. As an opaque call it built the whole
-- `pgqueue.jobs` tuple as a composite datum per row and re-ran the lease lookup
-- per row: measured over 20,000 active rows and 50 leases at 180 ms / 20,484
-- buffers, against 6.6 ms / 460 for the same predicate inlined over a hashed
-- `LEFT JOIN`. `pgqueue.workers.id` is the primary key, so that join yields at
-- most one row and a NULL `lease_expires_at` is exactly `NOT EXISTS`.
--
-- `COALESCE(lease_expires_at, '-infinity')` rather than `IS NULL OR ...` so the
-- parameter is used once: `inline_function` declines when a parameter used more
-- than once is passed an expensive argument, and the callers that are already
-- keyed by id pass a correlated subquery (an UPDATE's target table cannot be
-- referenced from a join in its own FROM clause).
CREATE FUNCTION pgqueue.job_is_stuck(
    j                pgqueue.jobs,
    grace_ms         bigint,
    lease_expires_at timestamptz
)
RETURNS boolean
LANGUAGE sql
STABLE
AS $$
    SELECT (j.timeout_ms IS NOT NULL
            AND j.started_at + ((j.timeout_ms + grace_ms) * interval '1 millisecond') < now())
        OR (COALESCE(j.touched_at, j.started_at)
                + (grace_ms * interval '1 millisecond') < now()
            AND COALESCE(lease_expires_at, '-infinity')
                + (grace_ms * interval '1 millisecond') <= now())
$$;

-- The dashboard's keyset pagination, in one place. Both the job listing and the
-- job-name typeahead page through jobs the same way, so the access strategy —
-- per-status laterals riding jobs_dashboard_status_page_idx, newest first —
-- has a single definition instead of two SQL blocks that must be edited
-- together. Inlined by the planner, so callers keep the index scan.
--
-- `p_queue` and `p_kind` select the partition to page through and `p_statuses`
-- names the statuses to union, so all three are required: a NULL `p_statuses`
-- drives `unnest` to zero rows rather than skipping the filter. `p_name`,
-- `p_prefix` and the cursor pair are the optional ones — pass NULL to skip
-- them. `p_limit` bounds each status's lateral; callers that need a global
-- bound re-apply it to the result.
CREATE FUNCTION pgqueue.job_page_keys(
    p_queue     text,
    p_kind      text,
    p_statuses  text[],
    p_name      text,
    p_prefix    text,
    p_cursor_at timestamptz,
    p_cursor_id uuid,
    p_limit     bigint
)
RETURNS TABLE (enqueued_at timestamptz, id uuid, name text)
LANGUAGE sql
STABLE
AS $$
    SELECT candidate.enqueued_at, candidate.id, candidate.name
    FROM unnest(p_statuses) AS requested(status)
    CROSS JOIN LATERAL (
        SELECT j.enqueued_at, j.id, j.name
        FROM pgqueue.jobs j
        WHERE j.queue = p_queue
          AND j.kind = p_kind
          AND j.status = requested.status
          AND (p_name IS NULL OR j.name = p_name)
          AND (p_prefix IS NULL OR starts_with(lower(j.name), lower(p_prefix)))
          AND (p_cursor_at IS NULL OR (j.enqueued_at, j.id) < (p_cursor_at, p_cursor_id))
        ORDER BY j.enqueued_at DESC, j.id DESC
        LIMIT p_limit
    ) candidate
$$;
