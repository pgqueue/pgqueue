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
use crate::database::{Database, DatabaseConnectOptions, DatabaseEnqueueResult};
use crate::job::{EnqueueResult, JobFilter, JobRequest, JobRow, JobStatus};
use crate::sweeper::Sweeper;
use crate::worker::WorkerInfo;

/// Current and retained job counts for one queue.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, sqlx::FromRow)]
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

/// Reconnect delay bounds for the notification listener. The delay starts low
/// so a momentary blip (a terminated backend, a restart) is healed almost
/// immediately, doubles on every failed attempt so a long outage settles at
/// one connection attempt — and one warn — per cap interval instead of two per
/// second, and resets once a subscription is re-established. The cap costs
/// only push latency: while the listener is down, worker wakeups are carried
/// by `poll_interval` and result waits by [`crate::JobHandle::wait`]'s polling
/// fallback, both of which cover a gap of any length.
const LISTENER_RECONNECT_INITIAL_DELAY: Duration = Duration::from_millis(500);
const LISTENER_RECONNECT_MAX_DELAY: Duration = Duration::from_secs(5);

async fn connect_notify_listener(
    pool: &PgPool,
    notify_channel: &str,
    done_channel: &str,
) -> Result<PgListener, sqlx::Error> {
    let mut listener = PgListener::connect_with(pool).await?;
    listener.listen_all([notify_channel, done_channel]).await?;
    listener.eager_reconnect(false);
    Ok(listener)
}

impl QueueNotifyListener {
    /// Never fails: a listener that cannot connect yet starts disconnected and
    /// heals through the same reconnect loop that covers a listener lost later.
    /// Starting it any other way would make a momentary refusal — the dedicated
    /// LISTEN connection lives outside the query pool, so it can be refused
    /// while the pool is perfectly usable — permanent for this queue handle.
    pub(crate) fn start(database: &Database) -> Self {
        // LISTEN is held for this queue handle's lifetime. Keep it outside the
        // query pool so independently constructed queues cannot reserve every
        // slot of a shared pool. Lazy so construction cannot fail on a refused
        // connection.
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_lazy_with((*database.pool().connect_options()).clone());
        let notify_channel = database.notify_channel().to_string();
        let done_channel = database.done_channel().to_string();

        let (wakeup, _) = broadcast::channel(16);
        let (done, _) = broadcast::channel(256);
        let (health, _) = watch::channel(None);
        let queue_name = database.name().to_string();
        let wakeup_tx = wakeup.clone();
        let done_tx = done.clone();
        let health_tx = health.clone();
        let task = tokio::spawn(async move {
            // `None` sends the loop straight to its reconnect arm, so the
            // first subscription and every later one share one code path.
            let mut listener =
                match connect_notify_listener(&pool, &notify_channel, &done_channel).await {
                    Ok(listener) => Some(listener),
                    Err(error) => {
                        health_tx.send_replace(Some(error.to_string()));
                        tracing::warn!(
                            queue = %queue_name, %error,
                            "notification listener unavailable at start; retrying in the background"
                        );
                        None
                    }
                };
            let mut reconnect_delay = LISTENER_RECONNECT_INITIAL_DELAY;
            loop {
                // PgListener absorbs simple drops itself. A surfaced error
                // requires a fresh subscription; polling fallbacks cover
                // notifications lost while that subscription is rebuilt.
                let Some(active_listener) = listener.as_mut() else {
                    tokio::time::sleep(reconnect_delay).await;
                    match connect_notify_listener(&pool, &notify_channel, &done_channel).await {
                        Ok(reconnected) => {
                            reconnect_delay = LISTENER_RECONNECT_INITIAL_DELAY;
                            listener = Some(reconnected);
                            health_tx.send_replace(None);
                            let _ = wakeup_tx.send(());
                        }
                        Err(error) => {
                            reconnect_delay =
                                (reconnect_delay * 2).min(LISTENER_RECONNECT_MAX_DELAY);
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
                match active_listener.try_recv().await {
                    Ok(Some(notification)) => {
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
                    Ok(None) => {
                        health_tx
                            .send_replace(Some("notification listener disconnected".to_string()));
                        listener.take();
                        tracing::warn!(
                            queue = %queue_name,
                            "notification listener disconnected"
                        );
                    }
                    Err(error) => {
                        health_tx.send_replace(Some(error.to_string()));
                        listener.take();
                        tracing::warn!(queue = %queue_name, %error, "notification listener error");
                    }
                }
            }
        });

        Self {
            wakeup,
            done,
            health,
            task,
        }
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
/// that lease alive while attempts run. Without a live lease, an attempt becomes
/// sweepable a sweep grace past its last heartbeat — a timeout, however long,
/// buys no slack against that, since the two recovery triggers are additive.
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
    ///
    /// A live, accepting lease is required: call [`Consumer::heartbeat`] first
    /// and refresh it until every returned attempt has been finished or
    /// retried. Without one this claims nothing and returns an empty vector,
    /// because the sweeper would otherwise treat the claim as abandoned and
    /// hand the job to another consumer while it is still running.
    ///
    /// Claims are taken with `FOR UPDATE SKIP LOCKED`, so concurrent consumers
    /// never wait on each other: a dequeue either claims rows or reports none.
    pub async fn dequeue(&self, limit: i64) -> Result<Vec<Attempt>, Error> {
        Ok(self
            .queue
            .database
            .dequeue_consumer(limit, self.worker_id)
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
    ///
    /// `ttl` must be greater than zero. A zero one writes a lease that has
    /// already expired by the time any later transaction reads it, and
    /// [`Consumer::dequeue`] requires a live lease — so every subsequent claim
    /// would come back empty, indistinguishable from an empty queue.
    ///
    /// `stats` and `metadata` must not nest containers more than 127 levels
    /// deep. `serde_json` stops deserializing at 128, and every live lease of
    /// the queue is decoded in one statement, so a deeper document would be
    /// stored happily and then fail [`Queue::info`] and the dashboard's worker
    /// views for the whole queue until the lease expired.
    ///
    /// Neither may contain a NUL, which `jsonb` cannot store at all. Both are
    /// refused as [`Error::Config`], because a heartbeat loop is expected to
    /// retry a transient error and neither of these ever becomes storable: it
    /// would spin without renewing the lease until the sweeper reclaimed every
    /// attempt the caller has claimed.
    pub async fn heartbeat(
        &self,
        stats: Value,
        metadata: Option<Value>,
        ttl: Duration,
    ) -> Result<(), Error> {
        crate::job::validate_nonzero_duration("worker lease TTL", ttl)?;
        self.queue
            .database
            .write_worker_info(
                self.worker_id,
                stats,
                metadata,
                ttl,
                crate::database::LeaseIntake::Reopen,
            )
            .await
    }
}

impl Attempt {
    /// The immutable job row snapshot returned by dequeue.
    pub fn job(&self) -> &JobRow {
        &self.row
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

    /// How long the sweeper waits past each recovery trigger before declaring
    /// an attempt stuck, giving its worker a window to finalize normally.
    /// Default 5s.
    ///
    /// It applies to both triggers: past a job's `timeout`, and past the expiry
    /// of the `pgqueue.workers` lease that covers the attempt. The second is
    /// what absorbs a heartbeat that stalled without the worker dying, so raise
    /// this on deployments where a lock wait, a pool stall or a GC pause can
    /// outlast a worker's lease TTL — otherwise a still-running attempt is
    /// cancelled and re-run. Expired worker leases are retained this long for
    /// the same reason.
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

    pub(crate) fn database(&self) -> &Database {
        &self.database
    }

    /// A shared handle for the few callers that must outlive this `Queue`
    /// borrow; everything on a request or dequeue path wants [`Self::database`].
    pub(crate) fn database_handle(&self) -> Arc<Database> {
        Arc::clone(&self.database)
    }

    /// The lazily-started notification listener for this queue handle. The first
    /// caller opens one LISTEN connection outside the query pool; enqueue-only
    /// processes never pay for it.
    pub(crate) fn notify_listener(&self) -> &QueueNotifyListener {
        self.database.notify_listener()
    }

    /// Enqueues an untyped job: the dynamic escape hatch under the typed
    /// `#[pgqueue::job]` API, useful when the job name is only known at
    /// runtime.
    ///
    /// A dedupe-key collision returns the existing live job's id.
    pub async fn enqueue_raw(&self, job: JobRequest) -> Result<EnqueueResult<Uuid>, Error> {
        raw_enqueue_result(self.database.enqueue_raw_delayed_result(job, None).await?)
    }

    /// Enqueues an untyped job inside a caller-owned transaction.
    ///
    /// The row and notification become visible only when the caller commits.
    /// Dedupe-key advisory locks remain held until that commit.
    ///
    /// PostgreSQL's default `READ COMMITTED` isolation is required to observe a
    /// dedupe-key owner that commits while this call waits for its lock. At
    /// `REPEATABLE READ` or `SERIALIZABLE`, retry the whole transaction if such
    /// a concurrent owner is outside the caller's snapshot.
    pub async fn enqueue_raw_in(
        &self,
        transaction: &mut sqlx::PgTransaction<'_>,
        job: JobRequest,
    ) -> Result<EnqueueResult<Uuid>, Error> {
        raw_enqueue_result(
            self.database
                .enqueue_raw_delayed_in_result(transaction, job, None)
                .await?,
        )
    }

    /// Requests an abort. Queued jobs finish as `aborted` immediately; running
    /// jobs move to `aborting` and are canceled by their worker's abort loop.
    /// A job the sweeper has already marked for stuck-job recovery is claimed
    /// the same way: the pending recovery retry becomes this abort, so the job
    /// finishes `aborted` instead of running again.
    /// Queued jobs with delete-immediately retention remain observable until
    /// the next sweep so result waiters can resolve the aborted result.
    /// Returns `false` if the job wasn't queued or running (it is terminal,
    /// missing, or an abort is already pending).
    pub async fn abort_job(&self, job_id: Uuid, reason: &str) -> Result<bool, Error> {
        self.database.abort(job_id, reason).await
    }

    /// Creates a fresh occurrence of a terminal job with one more attempt.
    /// The terminal row remains unchanged so existing handles keep observing
    /// its result. A terminal occurrence can be retried once; returns `false`
    /// if it is not terminal, was already retried, or its dedupe key already
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
    ///     let retry = queue.fetch_job(retry_id).await?;
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

    /// Fetches one job by its job ID.
    pub async fn fetch_job(&self, job_id: Uuid) -> Result<Option<JobRow>, Error> {
        self.database.job(job_id).await
    }

    /// Lists jobs for this queue, newest first, with optional filters.
    ///
    /// The page *order* is index-backed; the `status` and `name` filters are
    /// not. They are applied to rows already in that order, so a filter whose
    /// matches sit far down the ordering reads every newer row first: the cost
    /// of a filtered page grows with retention rather than with the page size.
    /// The alternative is two more indexes on the queue's hot table, which
    /// every enqueue and every attempt state change would pay for; the
    /// dashboard does not need them, because its own listing pages through a
    /// kind-qualified strategy that the existing indexes serve in full.
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
    ///
    /// Each counter is index-served, so the cost tracks the rows actually
    /// counted — live, failed and aborted jobs — rather than the queue's total
    /// retained history. Retained `complete` rows are never read.
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

fn raw_enqueue_result(result: DatabaseEnqueueResult) -> Result<EnqueueResult<Uuid>, Error> {
    match result {
        DatabaseEnqueueResult::Inserted(id) => Ok(EnqueueResult::Enqueued(id)),
        DatabaseEnqueueResult::Deduplicated { id, .. } => Ok(EnqueueResult::Deduplicated(id)),
    }
}

impl std::fmt::Debug for Queue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Queue")
            .field("name", &self.database.name())
            .finish_non_exhaustive()
    }
}
