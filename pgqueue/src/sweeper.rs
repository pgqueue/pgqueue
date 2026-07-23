//! Database-backed cleanup and recovery with advisory-lock leadership.

use std::collections::HashSet;
use std::sync::{Arc, LazyLock};

use serde_json::Value;
use sqlx::Connection;
use sqlx::postgres::PgConnection;
use uuid::Uuid;

use crate::Error;
use crate::database::{Database, DatabaseStuckJob};
use crate::job::{JobRetryBackoff, JobStatus, duration_to_ms};

pub(crate) const SWEPT: &str = "swept";
const SWEPT_RESULT: &str = "pgqueue:swept";

static SWEPT_MARKER: LazyLock<Value> = LazyLock::new(|| Value::String(SWEPT_RESULT.to_string()));

/// The outcome of one sweep pass.
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
    /// At least one bounded operation filled its batch and may have more work.
    pub more_work: bool,
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
/// Holds its advisory leadership lock on a dedicated connection for its whole
/// lifetime. Call [`Sweeper::release`] on graceful shutdown; dropping without
/// releasing closes the connection in the background, which also frees the
/// session-scoped advisory lock.
pub struct Sweeper {
    database: Arc<Database>,
    conn: Option<PgConnection>,
}

impl Sweeper {
    pub(crate) fn new(database: Arc<Database>) -> Self {
        Self {
            database,
            conn: None,
        }
    }

    /// Runs one sweep pass. Acquires leadership if not already held; when
    /// another process is the leader, returns
    /// `SweeperReport { leader: false, .. }`.
    pub async fn sweep(&mut self) -> Result<SweeperReport, Error> {
        if !self.ensure_leadership().await? {
            return Ok(SweeperReport::default());
        }

        let database = &self.database;
        let mut report = SweeperReport {
            leader: true,
            ..SweeperReport::default()
        };
        let batch_size = database.sweep_batch_size();

        report.purged_jobs = sqlx::query!(
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
            database.name(),
            batch_size,
        )
        .execute(database.pool())
        .await?
        .rows_affected();
        report.more_work |= report.purged_jobs == batch_size as u64;

        let purged_claims = sqlx::query!(
            r#"
            WITH expired AS (
                SELECT queue, unique_key, scheduled_at
                FROM pgqueue.cron_occurrences
                WHERE queue = $1 AND expires_at <= now()
                ORDER BY expires_at, unique_key, scheduled_at
                LIMIT $2
                FOR UPDATE SKIP LOCKED
            )
            DELETE FROM pgqueue.cron_occurrences AS occurrences
            USING expired
            WHERE occurrences.queue = expired.queue
              AND occurrences.unique_key = expired.unique_key
              AND occurrences.scheduled_at = expired.scheduled_at
            "#,
            database.name(),
            batch_size,
        )
        .execute(database.pool())
        .await?
        .rows_affected();
        report.more_work |= purged_claims == batch_size as u64;

        let purged_workers = sqlx::query!(
            r#"
            WITH expired AS (
                SELECT id FROM pgqueue.workers
                WHERE expires_at <= now()
                ORDER BY expires_at, id
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            )
            DELETE FROM pgqueue.workers AS workers
            USING expired
            WHERE workers.id = expired.id
            "#,
            batch_size,
        )
        .execute(database.pool())
        .await?
        .rows_affected();
        report.more_work |= purged_workers == batch_size as u64;

        let grace_ms = duration_to_ms(database.sweep_grace());
        let stuck = sqlx::query_as!(
            DatabaseStuckJob,
            r#"
            SELECT
                j.id,
                j.name,
                j.status AS "status: JobStatus",
                j.attempts,
                j.max_attempts,
                j.retry_delay_ms,
                j.backoff AS "backoff: JobRetryBackoff",
                j.error,
                j.result,
                j.worker_id
            FROM pgqueue.jobs AS j
            WHERE j.queue = $1
              AND j.status IN ('running', 'aborting')
              AND pgqueue.job_is_stuck(j, $2)
              AND (
                  j.status = 'running'
                  OR NOT EXISTS (
                      SELECT 1 FROM pgqueue.workers w
                      WHERE w.id = j.worker_id AND w.queue = j.queue
                        AND w.expires_at > now()
                  )
                  OR (
                      j.unique_key IS NULL AND j.group_key IS NULL
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
            database.name(),
            grace_ms,
            batch_size,
            SWEPT,
            swept_marker(),
        )
        .fetch_all(database.pool())
        .await?;
        report.more_work |= stuck.len() == batch_size as usize;

        let (running, aborting): (Vec<DatabaseStuckJob>, Vec<DatabaseStuckJob>) = stuck
            .into_iter()
            .partition(|job| job.status == JobStatus::Running);

        // Phase one marks every stuck running job in a single statement; the
        // per-row attempts/worker/stuckness guards ride along through unnest.
        if !running.is_empty() {
            let ids = running.iter().map(|job| job.id).collect::<Vec<_>>();
            let attempts = running.iter().map(|job| job.attempts).collect::<Vec<_>>();
            let worker_ids = running.iter().map(|job| job.worker_id).collect::<Vec<_>>();
            let marked = sqlx::query_scalar!(
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
                  AND pgqueue.job_is_stuck(j, $7)
                RETURNING j.id AS "id!"
                "#,
                &ids,
                database.name(),
                &attempts,
                &worker_ids as &[Option<Uuid>],
                SWEPT,
                swept_marker(),
                grace_ms,
            )
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

        for job in aborting {
            let sweeper_marked = is_swept_marked(job.error.as_deref(), job.result.as_ref());
            let recovered = if sweeper_marked && job.retryable() {
                database.retry_swept_abandoned(&job).await?
            } else {
                database
                    .finish_stuck_abandoned(&job, JobStatus::Aborted, None, None)
                    .await?
            };
            if recovered {
                tracing::warn!(
                    job.id = %job.id, job.name = %job.name, queue = %database.name(),
                    retried = sweeper_marked && job.retryable(), "swept stuck job"
                );
                report.swept.push(job.id);
            }
        }

        Ok(report)
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
            match sqlx::query_scalar!(r#"SELECT 1::integer AS "alive!""#)
                .fetch_one(&mut *conn)
                .await
            {
                Ok(1) => return Ok(true),
                Ok(_) => tracing::warn!(
                    queue = %self.database.name(),
                    "sweep leadership connection returned an invalid health response"
                ),
                Err(error) => tracing::warn!(
                    queue = %self.database.name(), %error,
                    "lost sweep leadership connection"
                ),
            }
            self.release().await;
        }

        let mut conn = self.database.pool().acquire().await?;
        let locked = sqlx::query_scalar!(
            r#"SELECT pg_try_advisory_lock($1) AS "locked!""#,
            self.database.sweep_lock_key(),
        )
        .fetch_one(&mut *conn)
        .await?;
        if locked {
            self.conn = Some(conn.detach());
            tracing::debug!(queue = %self.database.name(), "acquired sweep leadership");
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl Drop for Sweeper {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take()
            && let Ok(handle) = tokio::runtime::Handle::try_current()
        {
            handle.spawn(async move {
                let _ = conn.close().await;
            });
        }
    }
}
