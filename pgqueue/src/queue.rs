//! The Postgres-backed queue: connection, notifications, lifecycle
//! transitions, sweeping, and introspection.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::Deserialize;
use serde_json::Value;
use sqlx::postgres::{PgListener, PgPool, PgPoolOptions};
use tokio::sync::{broadcast, watch};
use uuid::Uuid;

use crate::Error;
use crate::database::{Database, DatabaseConnectOptions, DatabaseEnqueueOutcome};
use crate::job::{EnqueueOutcome, JobFilter, JobRequest, JobRow, JobStatus};
use crate::sweeper::Sweeper;
use crate::worker::WorkerInfo;

/// Current and retained job counts for one queue.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct QueueCounts {
    /// Jobs ready to run now.
    pub queued: i64,
    /// Jobs currently running or finishing abort cleanup.
    pub running: i64,
    /// Jobs queued for a future execution time.
    pub scheduled: i64,
    /// Retained jobs that exhausted their attempts.
    pub failed: i64,
    /// Retained jobs aborted before completion.
    pub aborted: i64,
}

/// Snapshot of a queue: gauges plus live workers.
#[derive(Debug, Clone, serde::Serialize)]
pub struct QueueInfo {
    /// Queue name.
    pub name: String,
    /// Current and retained job counts.
    #[serde(flatten)]
    pub counts: QueueCounts,
    /// Workers with unexpired heartbeats.
    pub workers: Vec<WorkerInfo>,
}

/// How queue connections handle the embedded `pgqueue` migrations.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum MigrationMode {
    /// Validate applied migrations and apply any pending migrations.
    #[default]
    Apply,
    /// Validate versions and checksums without executing DDL.
    Validate,
    /// Skip all schema checks. Intended only for externally managed schemas.
    Skip,
}

/// Counters accumulated by this queue handle since start.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct QueueStats {
    /// Jobs finished successfully.
    pub complete: u64,
    /// Jobs that exhausted their attempts.
    pub failed: u64,
    /// Retries scheduled.
    pub retried: u64,
    /// Jobs aborted.
    pub aborted: u64,
}

/// The counters behind every [`QueueStats`] snapshot, shared by queue handles
/// and workers so the fields and their assembly exist exactly once.
#[derive(Default)]
pub(crate) struct QueueCounters {
    complete: AtomicU64,
    failed: AtomicU64,
    retried: AtomicU64,
    aborted: AtomicU64,
}

impl QueueCounters {
    pub(crate) fn record_complete(&self) {
        self.complete.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_failed(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_retry(&self) {
        self.retried.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_abort(&self) {
        self.aborted.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> QueueStats {
        QueueStats {
            complete: self.complete.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            retried: self.retried.load(Ordering::Relaxed),
            aborted: self.aborted.load(Ordering::Relaxed),
        }
    }
}

/// A job-finished notification from this queue's completion channel.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct QueueDoneEvent {
    pub(crate) id: Uuid,
    pub(crate) status: JobStatus,
}

/// One PostgreSQL listener fanned out to every subscriber on this queue handle.
pub(crate) struct QueueNotifyListener {
    wakeup: broadcast::Sender<()>,
    done: broadcast::Sender<QueueDoneEvent>,
    health: watch::Sender<Option<String>>,
    task: tokio::task::JoinHandle<()>,
}

async fn connect_notify_listener(
    pool: &PgPool,
    notify_channel: &str,
    done_channel: &str,
) -> Result<PgListener, sqlx::Error> {
    let mut listener = PgListener::connect_with(pool).await?;
    listener.listen_all([notify_channel, done_channel]).await?;
    Ok(listener)
}

impl QueueNotifyListener {
    pub(crate) async fn start(database: &Database) -> Result<Self, Error> {
        // LISTEN is held for this queue handle's lifetime. Keep it outside the
        // query pool so independently constructed queues cannot reserve every
        // slot of a shared pool.
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with((*database.pool().connect_options()).clone())
            .await?;
        let notify_channel = database.notify_channel().to_string();
        let done_channel = database.done_channel().to_string();
        let listener = connect_notify_listener(&pool, &notify_channel, &done_channel).await?;

        let (wakeup, _) = broadcast::channel(16);
        let (done, _) = broadcast::channel(256);
        let (health, _) = watch::channel(None);
        let queue_name = database.name().to_string();
        let wakeup_tx = wakeup.clone();
        let done_tx = done.clone();
        let health_tx = health.clone();
        let task = tokio::spawn(async move {
            let mut listener = Some(listener);
            loop {
                // PgListener absorbs simple drops itself. A surfaced error
                // requires a fresh subscription; polling fallbacks cover
                // notifications lost while that subscription is rebuilt.
                let Some(active_listener) = listener.as_mut() else {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    match connect_notify_listener(&pool, &notify_channel, &done_channel).await {
                        Ok(reconnected) => {
                            listener = Some(reconnected);
                            health_tx.send_replace(None);
                            let _ = wakeup_tx.send(());
                        }
                        Err(error) => {
                            health_tx.send_replace(Some(error.to_string()));
                            tracing::warn!(
                                queue = %queue_name,
                                %error,
                                "notification listener reconnect failed"
                            );
                        }
                    }
                    continue;
                };
                match active_listener.recv().await {
                    Ok(notification) => {
                        health_tx.send_replace(None);
                        if notification.channel() == done_channel {
                            match serde_json::from_str::<QueueDoneEvent>(notification.payload()) {
                                Ok(event) => {
                                    let _ = done_tx.send(event);
                                }
                                Err(error) => tracing::warn!(
                                    queue = %queue_name,
                                    %error,
                                    "malformed done notification"
                                ),
                            }
                        }
                        let _ = wakeup_tx.send(());
                    }
                    Err(error) => {
                        health_tx.send_replace(Some(error.to_string()));
                        listener.take();
                        tracing::warn!(queue = %queue_name, %error, "notification listener error");
                    }
                }
            }
        });

        Ok(Self {
            wakeup,
            done,
            health,
            task,
        })
    }

    pub(crate) fn subscribe_wakeup(&self) -> broadcast::Receiver<()> {
        self.wakeup.subscribe()
    }

    pub(crate) fn subscribe_done(&self) -> broadcast::Receiver<QueueDoneEvent> {
        self.done.subscribe()
    }

    pub(crate) fn subscribe_health(&self) -> watch::Receiver<Option<String>> {
        self.health.subscribe()
    }
}

impl Drop for QueueNotifyListener {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// A handle to one named queue in the fixed `pgqueue` Postgres schema.
///
/// Cheap to clone (internally an `Arc`); clones share the connection pool and
/// stat counters. Obtain one with [`Queue::connect`] or [`Queue::builder`].
#[derive(Clone)]
pub struct Queue {
    database: Arc<Database>,
}

/// Low-level consumer bound to one worker identity.
///
/// Most applications should use [`crate::Worker`]. This capability-oriented
/// API exists for custom consumers that need to run the queue protocol
/// themselves without passing forgeable row snapshots back to [`Queue`]. A
/// custom consumer must call [`Consumer::heartbeat`] before dequeueing and keep
/// that lease alive while attempts run. Without a lease, attempts that have no
/// timeout or heartbeat deadline become sweepable after the queue's sweep
/// grace.
#[derive(Clone)]
pub struct Consumer {
    queue: Queue,
    worker_id: Uuid,
}

/// One dequeued attempt owned by a [`Consumer`].
pub struct Attempt {
    queue: Queue,
    row: JobRow,
}

impl Consumer {
    /// The worker identity written onto dequeued attempts and heartbeats.
    pub fn worker_id(&self) -> Uuid {
        self.worker_id
    }

    /// Dequeues up to `limit` due jobs and returns guarded attempt capabilities.
    /// Call [`Consumer::heartbeat`] first and refresh the lease until every
    /// returned attempt has been finished or retried.
    pub async fn dequeue(&self, limit: i64) -> Result<Vec<Attempt>, Error> {
        Ok(self
            .queue
            .database
            .dequeue(limit, self.worker_id)
            .await?
            .into_iter()
            .map(|row| Attempt {
                queue: self.queue.clone(),
                row,
            })
            .collect())
    }

    /// Upserts this consumer's worker lease and introspection metadata. Custom
    /// consumers must refresh it before `ttl` elapses while attempts are live.
    pub async fn heartbeat(
        &self,
        stats: Value,
        metadata: Option<Value>,
        ttl: Duration,
    ) -> Result<(), Error> {
        self.queue
            .database
            .write_worker_info(self.worker_id, stats, metadata, ttl, true)
            .await
    }
}

impl Attempt {
    /// The immutable row snapshot returned by dequeue.
    pub fn row(&self) -> &JobRow {
        &self.row
    }

    /// Refreshes this attempt's heartbeat under its attempt/worker fence.
    pub async fn touch(&self) -> Result<(), Error> {
        self.queue.database.touch_attempt(&self.row).await
    }

    /// Moves this attempt to a terminal state if it still owns the row.
    ///
    /// The capability is borrowed so callers can retry after a transient
    /// infrastructure error or apply a fallback after a refused transition.
    pub async fn finish(
        &self,
        status: JobStatus,
        result: Option<Value>,
        error: Option<&str>,
    ) -> Result<bool, Error> {
        self.queue
            .database
            .finish(&self.row, status, result, error)
            .await
    }

    /// Requeues this failed attempt if it still owns the row and may retry.
    ///
    /// The capability is borrowed so callers can retry after a transient
    /// infrastructure error, finish an exhausted final attempt as failed, or
    /// acknowledge an abort that landed mid-attempt by finishing as aborted.
    pub async fn retry(&self, error: &str) -> Result<bool, Error> {
        self.queue.database.retry(&self.row, error).await
    }
}

impl std::fmt::Debug for Consumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Consumer")
            .field("queue", &self.queue.name())
            .field("worker_id", &self.worker_id)
            .finish()
    }
}

impl std::fmt::Debug for Attempt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Attempt")
            .field("id", &self.row.id)
            .field("attempts", &self.row.attempts)
            .field("worker_id", &self.row.worker_id)
            .finish_non_exhaustive()
    }
}

/// Configures and connects a [`Queue`].
pub struct QueueBuilder {
    url: String,
    pool: Option<PgPool>,
    name: String,
    max_connections: u32,
    min_connections: u32,
    priorities: (i16, i16),
    sweep_grace: Duration,
    sweep_batch_size: u32,
    migration_mode: MigrationMode,
}

impl QueueBuilder {
    /// Queue name; jobs are namespaced within the `pgqueue` schema. Names must be
    /// non-empty, at most 255 bytes, contain no control characters, and not be
    /// the dot segments `.` or `..`.
    /// Default `"default"`.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Use an existing pool instead of connecting from the URL. A lazily
    /// started notification listener opens one additional connection without
    /// occupying a slot in this pool.
    pub fn pool(mut self, pool: PgPool) -> Self {
        self.pool = Some(pool);
        self
    }

    /// Pool sizing (ignored when [`QueueBuilder::pool`] is used). Defaults:
    /// 2..=10. A lazily started notification listener opens one additional
    /// connection outside this pool.
    pub fn connections(mut self, min: u32, max: u32) -> Self {
        self.min_connections = min;
        self.max_connections = max;
        self
    }

    /// Restrict dequeues from this handle to a priority range (inclusive).
    /// Default: all priorities.
    pub fn priorities(mut self, low: i16, high: i16) -> Self {
        self.priorities = (low, high);
        self
    }

    /// Extra time past a job's `timeout` before the sweeper declares it stuck,
    /// giving its worker a window to finalize normally. Default 5s.
    pub fn sweep_grace(mut self, grace: Duration) -> Self {
        self.sweep_grace = grace;
        self
    }

    /// Maximum rows handled by one bounded sweeper operation. Default 500.
    pub fn sweep_batch_size(mut self, size: u32) -> Self {
        self.sweep_batch_size = size;
        self
    }

    /// Controls whether connecting applies, validates, or skips migrations.
    /// Default [`MigrationMode::Apply`].
    pub fn migration_mode(mut self, mode: MigrationMode) -> Self {
        self.migration_mode = mode;
        self
    }

    /// Connects, verifies the server is PostgreSQL 18+, and handles migrations
    /// according to [`QueueBuilder::migration_mode`].
    pub async fn connect(self) -> Result<Queue, Error> {
        Ok(Queue {
            database: Arc::new(
                Database::connect(DatabaseConnectOptions {
                    url: self.url,
                    pool: self.pool,
                    name: self.name,
                    priorities: self.priorities,
                    sweep_grace: self.sweep_grace,
                    sweep_batch_size: self.sweep_batch_size,
                    max_connections: self.max_connections,
                    min_connections: self.min_connections,
                    migration_mode: self.migration_mode,
                })
                .await?,
            ),
        })
    }
}

impl Queue {
    /// Connects to queue `default` in the `pgqueue` schema and applies
    /// migrations. Use [`Queue::builder`] to customize the queue or pool.
    pub async fn connect(url: &str) -> Result<Queue, Error> {
        Queue::builder(url).connect().await
    }

    /// Starts configuring a queue connection.
    pub fn builder(url: &str) -> QueueBuilder {
        QueueBuilder {
            url: url.to_string(),
            pool: None,
            name: "default".to_string(),
            max_connections: 10,
            min_connections: 2,
            priorities: (i16::MIN, i16::MAX),
            sweep_grace: Duration::from_secs(5),
            sweep_batch_size: 500,
            migration_mode: MigrationMode::Apply,
        }
    }

    /// This queue's name.
    pub fn name(&self) -> &str {
        self.database.name()
    }

    /// The underlying connection pool.
    pub fn pool(&self) -> &PgPool {
        self.database.pool()
    }

    /// Creates a low-level consumer bound to `worker_id`.
    pub fn consumer(&self, worker_id: Uuid) -> Consumer {
        Consumer {
            queue: self.clone(),
            worker_id,
        }
    }

    pub(crate) fn database(&self) -> Arc<Database> {
        Arc::clone(&self.database)
    }

    /// The lazily-started notification listener for this queue handle. The first
    /// caller opens one LISTEN connection outside the query pool; enqueue-only
    /// processes never pay for it.
    pub(crate) async fn notify_listener(&self) -> Result<&QueueNotifyListener, Error> {
        self.database.notify_listener().await
    }

    /// Enqueues an untyped job (the dynamic escape hatch; typed enqueue via
    /// the `#[pgqueue::job]` machinery calls this).
    ///
    /// A unique-key collision returns the existing live job's id.
    pub async fn enqueue_raw(&self, job: JobRequest) -> Result<EnqueueOutcome<Uuid>, Error> {
        self.enqueue_raw_delayed(job, None).await
    }

    /// Enqueues an untyped job inside a caller-owned transaction.
    ///
    /// The row and notification become visible only when the caller commits.
    /// Unique-key advisory locks remain held until that commit.
    pub async fn enqueue_raw_in(
        &self,
        transaction: &mut sqlx::PgTransaction<'_>,
        job: JobRequest,
    ) -> Result<EnqueueOutcome<Uuid>, Error> {
        self.enqueue_raw_delayed_in(transaction, job, None).await
    }

    pub(crate) async fn enqueue_raw_delayed(
        &self,
        job: JobRequest,
        delay: Option<Duration>,
    ) -> Result<EnqueueOutcome<Uuid>, Error> {
        raw_enqueue_outcome(
            self.database
                .enqueue_raw_delayed_outcome(job, delay)
                .await?,
        )
    }

    pub(crate) async fn enqueue_raw_delayed_in(
        &self,
        transaction: &mut sqlx::PgTransaction<'_>,
        job: JobRequest,
        delay: Option<Duration>,
    ) -> Result<EnqueueOutcome<Uuid>, Error> {
        raw_enqueue_outcome(
            self.database
                .enqueue_raw_delayed_in_outcome(transaction, job, delay)
                .await?,
        )
    }

    pub(crate) async fn enqueue_raw_delayed_outcome(
        &self,
        job: JobRequest,
        delay: Option<Duration>,
    ) -> Result<DatabaseEnqueueOutcome, Error> {
        self.database.enqueue_raw_delayed_outcome(job, delay).await
    }

    pub(crate) async fn enqueue_raw_delayed_in_outcome(
        &self,
        transaction: &mut sqlx::PgTransaction<'_>,
        job: JobRequest,
        delay: Option<Duration>,
    ) -> Result<DatabaseEnqueueOutcome, Error> {
        self.database
            .enqueue_raw_delayed_in_outcome(transaction, job, delay)
            .await
    }

    /// Requests an abort. Queued jobs finish as `aborted` immediately; running
    /// jobs move to `aborting` and are canceled by their worker's abort loop.
    /// Queued jobs with delete-immediately retention remain observable until
    /// the next sweep so result waiters can resolve the aborted outcome.
    /// Returns `false` if the job wasn't queued or running.
    pub async fn abort(&self, id: Uuid, reason: &str) -> Result<bool, Error> {
        self.database.abort(id, reason).await
    }

    /// Creates a fresh occurrence of a terminal job with one more attempt.
    /// The terminal row remains unchanged so existing handles keep observing
    /// its result. A terminal occurrence can be retried once; returns `false`
    /// if it is not terminal, was already retried, or its unique key already
    /// belongs to a live occurrence.
    ///
    /// ```no_run
    /// # use pgqueue::{Error, Queue};
    /// # use uuid::Uuid;
    /// # async fn retry(queue: &Queue, id: Uuid) -> Result<(), Error> {
    /// let enqueued = queue.retry_job(id, "manual retry").await?;
    /// assert!(enqueued);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn retry_job(&self, id: Uuid, reason: &str) -> Result<bool, Error> {
        Ok(self.retry_job_occurrence(id, reason).await?.is_some())
    }

    /// Creates a fresh occurrence of a terminal job and returns its new ID.
    ///
    /// Unlike [`Queue::retry_job`], this exposes the new occurrence so callers
    /// can fetch or wait on it. Returns `None` under the same conditions that
    /// make `retry_job` return `false`.
    ///
    /// ```no_run
    /// # use pgqueue::{Error, Queue};
    /// # use uuid::Uuid;
    /// # async fn retry(queue: &Queue, failed_id: Uuid) -> Result<(), Error> {
    /// if let Some(retry_id) = queue
    ///     .retry_job_occurrence(failed_id, "manual retry")
    ///     .await?
    /// {
    ///     let retry = queue.job(retry_id).await?;
    ///     assert!(retry.is_some());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn retry_job_occurrence(
        &self,
        id: Uuid,
        reason: &str,
    ) -> Result<Option<Uuid>, Error> {
        self.database.retry_job_occurrence(id, reason).await
    }

    pub(crate) async fn touch_attempt(&self, job: &JobRow) -> Result<(), Error> {
        self.database.touch_attempt(job).await
    }

    /// Fetches one job by id.
    pub async fn job(&self, id: Uuid) -> Result<Option<JobRow>, Error> {
        self.database.job(id).await
    }

    /// Lists jobs for this queue, newest first, with optional filters.
    pub async fn jobs_page(&self, filter: JobFilter) -> Result<Vec<JobRow>, Error> {
        let limit = filter.limit()?;
        let before = filter.before;
        self.database
            .jobs_page(
                filter.status.map(JobStatus::as_str),
                filter.name.as_deref(),
                limit,
                before,
            )
            .await
    }

    /// Current queued/running/scheduled and retained failure counts.
    pub async fn counts(&self) -> Result<QueueCounts, Error> {
        self.database.counts().await
    }

    /// Job counts plus live workers — the dashboard's queue snapshot.
    pub async fn info(&self) -> Result<QueueInfo, Error> {
        let (counts, workers) = tokio::try_join!(self.database.counts(), self.database.workers())?;
        Ok(QueueInfo {
            name: self.database.name().to_string(),
            counts,
            workers,
        })
    }

    /// Counters accumulated by this handle since creation.
    pub fn stats(&self) -> QueueStats {
        self.database.stats()
    }

    /// Creates a sweeper for this queue. At most one sweeper per queue is
    /// running across all processes (advisory-lock leadership); the rest no-op.
    pub fn sweeper(&self) -> Sweeper {
        self.database.sweeper()
    }
}

fn raw_enqueue_outcome(outcome: DatabaseEnqueueOutcome) -> Result<EnqueueOutcome<Uuid>, Error> {
    match outcome {
        DatabaseEnqueueOutcome::Inserted(id) => Ok(EnqueueOutcome::Enqueued(id)),
        DatabaseEnqueueOutcome::Deduplicated { id, .. } => Ok(EnqueueOutcome::Deduplicated(id)),
    }
}

impl std::fmt::Debug for Queue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Queue")
            .field("name", &self.database.name())
            .finish_non_exhaustive()
    }
}
