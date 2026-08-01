//! Database-backed cleanup and recovery with advisory-lock leadership.

use std::collections::HashSet;
use std::sync::{Arc, LazyLock};

use serde_json::Value;
use sqlx::Connection;
use sqlx::postgres::PgConnection;
use uuid::Uuid;

use crate::Error;
use crate::database::{Database, DatabaseStuckJob};
use crate::job::{JobStatus, duration_to_ms};

pub(crate) const SWEPT: &str = "swept";
const SWEPT_RESULT: &str = "pgqueue:swept";

/// Consecutive failed passes a leader tolerates before releasing leadership.
///
/// Leadership is revalidated only by a liveness probe on the dedicated
/// leadership connection, and that can outlive the pool the passes run on — a
/// network change that blocks new flows but not established ones, say — so a
/// leader whose passes keep failing must eventually surrender the lock or
/// sweeping stops cluster-wide for as long as it stays up. Three passes is the
/// balance: one would trade leadership away on any transient error, flapping it
/// between healthy processes, while three consecutive failures — three minutes
/// at the default sweep interval — is a sustained outage on recovery work that
/// is already delayed by grace periods, not a blip.
const MAX_LEADER_SWEEP_FAILURES: u32 = 3;

static SWEPT_MARKER: LazyLock<Value> = LazyLock::new(|| Value::String(SWEPT_RESULT.to_string()));

/// The result of one sweep pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweeperReport {
    /// Whether this process held sweep leadership.
    pub leader: bool,
    /// Expired terminal jobs removed.
    pub purged_jobs: u64,
    /// Stuck jobs asked to abort in phase one.
    pub cancelling: Vec<Uuid>,
    /// Stuck jobs recovered in phase two.
    pub swept: Vec<Uuid>,
    /// The operations that filled their batch, for a follow-up pass that skips
    /// the ones already drained.
    pub unfinished: SweepOperations,
}

impl SweeperReport {
    /// Whether at least one bounded operation filled its batch and may have
    /// more work. Derived from [`SweeperReport::unfinished`] so the two can
    /// never disagree.
    pub fn more_work(&self) -> bool {
        self.unfinished.any()
    }
}

/// Which of a sweep pass's bounded operations run. [`Sweeper::sweep`] runs
/// [`SweepOperations::ALL`]; a drain pass repeats only the ones that filled
/// their batch.
///
/// The `Default` value is [`SweepOperations::NONE`], not `ALL` — it exists so
/// [`SweeperReport`] can derive `Default`. Handing it to
/// [`Sweeper::sweep_operations`] is not a full sweep: leadership is acquired
/// first, so the pass takes the queue's sweep lock away from every other
/// process for this [`Sweeper`]'s lifetime and then performs no operation at
/// all, reporting `leader: true` with no more work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SweepOperations {
    /// Purge expired terminal job rows.
    pub expired_jobs: bool,
    /// Purge expired cron occurrence claims.
    pub cron_occurrences: bool,
    /// Purge expired worker leases.
    pub workers: bool,
    /// Recover stuck running and aborting jobs.
    pub stuck_jobs: bool,
}

impl SweepOperations {
    /// Every operation.
    pub const ALL: Self = Self {
        expired_jobs: true,
        cron_occurrences: true,
        workers: true,
        stuck_jobs: true,
    };

    /// No operation. Reported by a pass that drained everything.
    pub const NONE: Self = Self {
        expired_jobs: false,
        cron_occurrences: false,
        workers: false,
        stuck_jobs: false,
    };

    /// Whether any operation is selected.
    pub fn any(self) -> bool {
        self.expired_jobs || self.cron_occurrences || self.workers || self.stuck_jobs
    }
}

impl Default for SweepOperations {
    fn default() -> Self {
        Self::NONE
    }
}

/// The `result` marker written by sweeper-initiated aborts. Bound on every
/// finish and retry, so it is allocated once and bound by reference.
pub(crate) fn swept_marker() -> &'static Value {
    &SWEPT_MARKER
}

pub(crate) fn is_swept_marked(error: Option<&str>, result: Option<&Value>) -> bool {
    error == Some(SWEPT) && result.and_then(Value::as_str) == Some(SWEPT_RESULT)
}

/// Cluster-coordinated sweeper that purges expired rows and recovers stuck jobs.
///
/// Holds its advisory leadership lock on a dedicated connection until it either
/// releases or fails [`MAX_LEADER_SWEEP_FAILURES`] passes in a row — the passes
/// run on the shared pool, which can die while the leadership connection stays
/// healthy, and a leader that cannot sweep must not lock its peers out. Call
/// [`Sweeper::release`] on graceful shutdown; dropping without releasing closes
/// the connection in the background, which also frees the session-scoped
/// advisory lock.
///
/// [`Sweeper::sweep`] is cancellation-safe: dropping the future — which is what
/// a `select!` on a shutdown token does — leaves this process either holding
/// leadership on a connection it can still name, or holding nothing at all.
pub struct Sweeper {
    database: Arc<Database>,
    conn: Option<PgConnection>,
    failed_passes: u32,
}

impl Sweeper {
    pub(crate) fn new(database: Arc<Database>) -> Self {
        Self {
            database,
            conn: None,
            failed_passes: 0,
        }
    }

    /// Runs one sweep pass. Acquires leadership if not already held; when
    /// another process is the leader, returns
    /// `SweeperReport { leader: false, .. }`.
    pub async fn sweep(&mut self) -> Result<SweeperReport, Error> {
        self.sweep_operations(SweepOperations::ALL).await
    }

    /// Runs one sweep pass over `operations` only. Draining a backlog repeats
    /// just the operations that filled their batch
    /// ([`SweeperReport::unfinished`]) instead of re-issuing every statement.
    pub async fn sweep_operations(
        &mut self,
        operations: SweepOperations,
    ) -> Result<SweeperReport, Error> {
        let result = self.sweep_pass(operations).await;
        match &result {
            Ok(_) => self.failed_passes = 0,
            // Only failures *while holding the lock* count: an error without
            // leadership left nothing held, and the next pass re-contends
            // anyway. Releasing goes through `release`, which closes the
            // detached connection — never pools it — because it carries the
            // session-scoped lock; the process re-contends on a later pass once
            // it can acquire from its pool again, and until then a healthy peer
            // is free to take over.
            Err(_) if self.is_leader() => {
                self.failed_passes += 1;
                if self.failed_passes >= MAX_LEADER_SWEEP_FAILURES {
                    tracing::warn!(
                        queue = %self.database.name(), failures = self.failed_passes,
                        "released sweep leadership after consecutive failed passes"
                    );
                    self.failed_passes = 0;
                    self.release().await;
                }
            }
            Err(_) => {}
        }
        result
    }

    async fn sweep_pass(&mut self, operations: SweepOperations) -> Result<SweeperReport, Error> {
        if !self.ensure_leadership().await? {
            return Ok(SweeperReport::default());
        }

        let batch_size = self.database.sweep_batch_size();
        let grace_ms = duration_to_ms(self.database.sweep_grace());
        let mut report = SweeperReport {
            leader: true,
            ..SweeperReport::default()
        };

        if operations.expired_jobs {
            report.purged_jobs = self.purge_expired_jobs(batch_size).await?;
            report.unfinished.expired_jobs = report.purged_jobs == batch_size as u64;
        }
        if operations.cron_occurrences {
            let purged = self.purge_cron_occurrences(batch_size).await?;
            report.unfinished.cron_occurrences = purged == batch_size as u64;
        }
        if operations.workers {
            let purged = self.purge_worker_leases(batch_size, grace_ms).await?;
            report.unfinished.workers = purged == batch_size as u64;
        }
        if operations.stuck_jobs {
            let stuck = self
                .recover_stuck_jobs(batch_size, grace_ms, &mut report)
                .await?;
            report.unfinished.stuck_jobs = stuck == batch_size as usize;
        }

        Ok(report)
    }

    /// Deletes one batch of expired terminal job rows.
    async fn purge_expired_jobs(&self, batch_size: i64) -> Result<u64, Error> {
        let database = &self.database;
        Ok(sqlx::query(
            r#"
            WITH expired AS (
                SELECT id FROM pgqueue.jobs
                WHERE queue = $1
                  AND status IN ('complete', 'failed', 'aborted')
                  AND expires_at <= now()
                ORDER BY expires_at, id
                LIMIT $2
                FOR UPDATE SKIP LOCKED
            )
            DELETE FROM pgqueue.jobs AS jobs
            USING expired
            WHERE jobs.id = expired.id
            "#,
        )
        .bind(database.name())
        .bind(batch_size)
        .execute(database.pool())
        .await?
        .rows_affected())
    }

    /// Deletes one batch of expired cron occurrence claims.
    async fn purge_cron_occurrences(&self, batch_size: i64) -> Result<u64, Error> {
        let database = &self.database;
        Ok(sqlx::query(
            r#"
            WITH expired AS (
                SELECT queue, dedupe_key, scheduled_at
                FROM pgqueue.cron_occurrences
                WHERE queue = $1 AND expires_at <= now()
                ORDER BY expires_at, dedupe_key, scheduled_at
                LIMIT $2
                FOR UPDATE SKIP LOCKED
            )
            DELETE FROM pgqueue.cron_occurrences AS occurrences
            USING expired
            WHERE occurrences.queue = expired.queue
              AND occurrences.dedupe_key = expired.dedupe_key
              AND occurrences.scheduled_at = expired.scheduled_at
            "#,
        )
        .bind(database.name())
        .bind(batch_size)
        .execute(database.pool())
        .await?
        .rows_affected())
    }

    /// Deletes one batch of worker leases that have been expired for longer
    /// than the liveness grace.
    ///
    /// The grace is what makes `pgqueue.job_is_stuck`'s dead-owner branch
    /// enforceable: that branch waits `sweep_grace` past `expires_at` before
    /// declaring the attempt recoverable, and a deleted row is
    /// indistinguishable from one that lapsed an hour ago. Nothing else reads
    /// these rows without an `expires_at > now()` filter, so retaining them a
    /// little longer changes no liveness answer.
    async fn purge_worker_leases(&self, batch_size: i64, grace_ms: i64) -> Result<u64, Error> {
        let database = &self.database;
        Ok(sqlx::query(
            r#"
            WITH expired AS (
                SELECT id FROM pgqueue.workers
                WHERE queue = $1
                  -- The grace is moved onto `now()` rather than onto the
                  -- column so `workers_queue_idx (queue, expires_at, id)` can
                  -- serve the range, not just the ordering.
                  AND expires_at <= now() - ($3::bigint * interval '1 millisecond')
                ORDER BY expires_at, id
                LIMIT $2
                FOR UPDATE SKIP LOCKED
            )
            DELETE FROM pgqueue.workers AS workers
            USING expired
            WHERE workers.id = expired.id
            "#,
        )
        .bind(database.name())
        .bind(batch_size)
        .bind(grace_ms)
        .execute(database.pool())
        .await?
        .rows_affected())
    }

    /// Asks stuck running jobs to abort (phase one) and recovers rows whose
    /// worker never finished the abort (phase two). Returns how many stuck rows
    /// this batch examined.
    async fn recover_stuck_jobs(
        &self,
        batch_size: i64,
        grace_ms: i64,
        report: &mut SweeperReport,
    ) -> Result<usize, Error> {
        let database = &self.database;
        let stuck = sqlx::query_as::<_, DatabaseStuckJob>(
            r#"
            SELECT
                j.id,
                j.name,
                j.status,
                j.attempts,
                j.max_attempts,
                j.retry_delay_ms,
                j.backoff,
                j.error,
                j.result,
                j.worker_id
            -- The lease is joined rather than looked up inside
            -- `pgqueue.job_is_stuck`, which is what lets that function inline:
            -- this scan is the one place the predicate is applied to every
            -- active row of the queue, unbounded by `batch_size`, and the
            -- `ORDER BY` below gives the executor no early exit.
            -- `pgqueue.workers.id` is the primary key, so the join adds no rows.
            FROM pgqueue.jobs AS j
            LEFT JOIN pgqueue.workers AS lease
                ON lease.id = j.worker_id AND lease.queue = j.queue
            WHERE j.queue = $1
              AND j.status IN ('running', 'aborting')
              AND pgqueue.job_is_stuck(j, $2, lease.expires_at)
              AND (
                  j.status = 'running'
                  OR lease.expires_at IS NULL
                  OR lease.expires_at <= now()
                  OR (
                      j.dedupe_key IS NULL
                      AND NOT (
                          j.error IS NOT DISTINCT FROM $4
                          AND j.result IS NOT DISTINCT FROM $5
                          AND j.attempts < j.max_attempts
                      )
                  )
              )
            ORDER BY j.touched_at, j.id
            LIMIT $3
            "#,
        )
        .bind(database.name())
        .bind(grace_ms)
        .bind(batch_size)
        .bind(SWEPT)
        .bind(swept_marker())
        .fetch_all(database.pool())
        .await?;
        let examined = stuck.len();

        let (running, aborting): (Vec<DatabaseStuckJob>, Vec<DatabaseStuckJob>) = stuck
            .into_iter()
            .partition(|job| job.status == JobStatus::Running);

        // Phase one marks every stuck running job in a single statement; the
        // per-row attempts/worker/stuckness guards ride along through unnest.
        if !running.is_empty() {
            let ids = running.iter().map(|job| job.id).collect::<Vec<_>>();
            let attempts = running.iter().map(|job| job.attempts).collect::<Vec<_>>();
            let worker_ids = running.iter().map(|job| job.worker_id).collect::<Vec<_>>();
            let marked = sqlx::query_scalar::<_, Uuid>(
                r#"
                UPDATE pgqueue.jobs AS j
                SET status = 'aborting', error = $5, result = $6
                FROM unnest($1::uuid[], $3::int[], $4::uuid[])
                    AS stuck(id, attempts, worker_id)
                WHERE j.id = stuck.id
                  AND j.queue = $2
                  AND j.status = 'running'
                  AND j.attempts = stuck.attempts
                  AND j.worker_id IS NOT DISTINCT FROM stuck.worker_id
                  -- A subquery, not a join: an UPDATE's target table cannot be
                  -- referenced from its own FROM clause's join conditions. It
                  -- costs one index lookup per row of a batch this statement is
                  -- already keyed by, and the predicate still inlines.
                  AND pgqueue.job_is_stuck(j, $7, (
                      SELECT lease.expires_at FROM pgqueue.workers AS lease
                      WHERE lease.id = j.worker_id AND lease.queue = j.queue))
                RETURNING j.id
                "#,
            )
            .bind(&ids)
            .bind(database.name())
            .bind(&attempts)
            .bind(&worker_ids as &[Option<Uuid>])
            .bind(SWEPT)
            .bind(swept_marker())
            .bind(grace_ms)
            .fetch_all(database.pool())
            .await?
            .into_iter()
            .collect::<HashSet<_>>();

            for job in &running {
                if marked.contains(&job.id) {
                    tracing::warn!(
                        job.id = %job.id, job.name = %job.name, queue = %database.name(),
                        "stuck job asked to abort"
                    );
                    report.cancelling.push(job.id);
                }
            }
        }

        // Phase two recovers the whole batch in two statements rather than one
        // (sometimes two) per row: a worker that died holding hundreds of jobs
        // must not spend the sweep's drain budget on round trips.
        let (retryable, terminal): (Vec<&DatabaseStuckJob>, Vec<&DatabaseStuckJob>) =
            aborting.iter().partition(|job| {
                is_swept_marked(job.error.as_deref(), job.result.as_ref()) && job.retryable()
            });
        let names = aborting
            .iter()
            .map(|job| (job.id, job.name.as_str()))
            .collect::<std::collections::HashMap<_, _>>();

        let retried = database.retry_swept_abandoned_batch(&retryable).await?;
        let aborted = database.abort_stuck_abandoned_batch(&terminal).await?;
        for (id, retried) in retried
            .iter()
            .map(|id| (id, true))
            .chain(aborted.iter().map(|id| (id, false)))
        {
            tracing::warn!(
                job.id = %id, job.name = names.get(id).copied().unwrap_or_default(),
                queue = %database.name(), retried, "swept stuck job"
            );
            report.swept.push(*id);
        }

        Ok(examined)
    }

    /// Whether this sweeper currently holds the leadership lock.
    pub fn is_leader(&self) -> bool {
        self.conn.is_some()
    }

    /// Releases leadership and closes the dedicated connection.
    pub async fn release(&mut self) {
        if let Some(conn) = self.conn.take() {
            let _ = conn.close().await;
        }
    }

    async fn ensure_leadership(&mut self) -> Result<bool, Error> {
        if let Some(conn) = self.conn.as_mut() {
            match sqlx::query_scalar::<_, i32>("SELECT 1::integer")
                .fetch_one(&mut *conn)
                .await
            {
                // The probe is a liveness check, so any answer at all proves
                // the leadership connection — and the advisory lock it holds —
                // is still there. Matching on the value would add an arm no
                // test can reach.
                Ok(_) => return Ok(true),
                Err(error) => tracing::warn!(
                    queue = %self.database.name(), %error,
                    "lost sweep leadership connection"
                ),
            }
            self.release().await;
        }

        // The acquisition runs behind a guard because `pg_try_advisory_lock` is
        // session-scoped, unlike every other advisory lock here: the lock
        // outlives the statement that took it, so between the server granting it
        // and `detach()` below there is a window where this future may be
        // dropped — `sweep_loop` drops it on every worker shutdown. A plain
        // `PoolConnection` would then be *released to the pool still holding
        // leadership*: sqlx runs no reset on release, `self.conn` is `None`, and
        // so neither `release` nor `Drop` can ever free it. Sweeping would stop
        // cluster-wide, silently, until that connection happened to be reaped.
        let mut acquisition = LeadershipAcquisition::new(self.database.pool().acquire().await?);
        let locked = sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_lock($1)")
            .bind(self.database.sweep_lock_key())
            .fetch_one(acquisition.connection())
            .await?;
        if locked {
            self.conn = Some(acquisition.into_leader());
            tracing::debug!(queue = %self.database.name(), "acquired sweep leadership");
            Ok(true)
        } else {
            acquisition.into_pool();
            Ok(false)
        }
    }
}

/// Owns the pooled connection a leadership acquisition runs on, so that no path
/// out of the acquisition can return that connection to the pool while the
/// server might be holding the session-scoped lock on it.
///
/// Both deliberate outcomes consume the guard: leadership takes the connection
/// (`into_leader`), and a refusal — where `pg_try_advisory_lock` answered
/// `false`, so no lock exists — hands it back (`into_pool`). Everything else is
/// a drop: cancellation, or a query error that leaves it unknowable whether the
/// server took the lock. Those close the connection, which is the only way to
/// free a session lock this process can no longer name.
struct LeadershipAcquisition(Option<sqlx::pool::PoolConnection<sqlx::Postgres>>);

impl LeadershipAcquisition {
    fn new(conn: sqlx::pool::PoolConnection<sqlx::Postgres>) -> Self {
        Self(Some(conn))
    }

    /// The connection the acquisition statement runs on.
    fn connection(&mut self) -> &mut PgConnection {
        match self.0.as_deref_mut() {
            Some(conn) => conn,
            // Unreachable: the connection is taken only by the two consuming
            // methods below and by `Drop`, none of which can run while this
            // borrow is alive.
            None => unreachable!("the acquisition guard owns its connection"),
        }
    }

    /// Takes the connection out to hold leadership on for the sweeper's life.
    fn into_leader(mut self) -> PgConnection {
        match self.0.take() {
            Some(conn) => conn.detach(),
            // Unreachable for the same reason as `connection`.
            None => unreachable!("the acquisition guard owns its connection"),
        }
    }

    /// Returns the connection to the pool, having taken no lock.
    fn into_pool(mut self) {
        drop(self.0.take());
    }
}

impl Drop for LeadershipAcquisition {
    fn drop(&mut self) {
        if let Some(conn) = self.0.take() {
            close_in_background(conn.detach());
        }
    }
}

/// Closes `conn` without awaiting it. With a runtime to hand that is a graceful
/// close; without one, dropping the connection closes its socket, which ends the
/// session — and so releases its advisory lock — either way.
fn close_in_background(conn: PgConnection) {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.spawn(async move {
            let _ = conn.close().await;
        });
    }
}

impl Drop for Sweeper {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            close_in_background(conn);
        }
    }
}
