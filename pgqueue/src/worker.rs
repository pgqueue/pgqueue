//! The worker: dequeues jobs and runs their handlers with panic containment
//! and timeout enforcement, polls for aborts, fires cron jobs, sweeps the
//! queue, heartbeats worker info, and shuts down gracefully.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::Value;
use tokio::sync::{broadcast, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use uuid::Uuid;

use crate::Error;
#[cfg(feature = "dashboard")]
use crate::dashboard::{
    DashboardRuntime, DashboardServer, DashboardWorkerConfig, bind_dashboard,
    wait_for_dashboard_exit,
};
use crate::database::{Database, DatabaseCronAuthority, DatabaseCronScheduleOutcome};
use crate::job::{
    CronOptions, JobBuilder, JobContext, JobCronEntry, JobError, JobErrorKind, JobRow, JobStateMap,
    JobStatus, JobType, TypeErasedJobHandler, validate_duration,
};
use crate::queue::{Queue, QueueCounters};

const WORKER_INFO_TTL_MULTIPLIER: u32 = 3;
const HARD_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const SHUTDOWN_STEP_TIMEOUT: Duration = Duration::from_secs(1);
const FINALIZE_RETRY_INTERVAL: Duration = Duration::from_secs(1);
const DEQUEUE_RETRY_INITIAL_MAX_MS: u64 = 3;
const DEQUEUE_RETRY_MAX_MS: u64 = 100;

fn worker_info_ttl(timer: Duration) -> Duration {
    timer.saturating_mul(WORKER_INFO_TTL_MULTIPLIER)
}

/// A live worker row whose heartbeat has not expired.
#[derive(Debug, Clone, serde::Serialize)]
pub struct WorkerInfo {
    /// Worker identifier.
    pub id: Uuid,
    /// Queue processed by the worker.
    pub queue: String,
    /// Worker-local completion counters and uptime.
    pub stats: Value,
    /// Optional user metadata.
    pub metadata: Option<Value>,
    /// When this worker run began.
    pub started_at: DateTime<Utc>,
    /// Most recent heartbeat.
    pub heartbeat_at: DateTime<Utc>,
    /// When the worker is considered dead unless refreshed.
    pub expires_at: DateTime<Utc>,
}

/// Background subsystem represented in [`WorkerHealth`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerComponent {
    /// PostgreSQL notification listener.
    Notification,
    /// Job dequeue/fetch loop.
    Dequeue,
    /// Abort polling loop.
    Abort,
    /// Durable cron scheduler.
    Scheduler,
    /// Cleanup and stuck-job recovery.
    Sweeper,
    /// Worker lease and statistics heartbeat.
    WorkerInfo,
}

/// One currently failing worker subsystem.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct WorkerHealthFailure {
    /// Failing subsystem.
    pub component: WorkerComponent,
    /// Most recent error message.
    pub message: String,
    /// When this failure episode began.
    pub since: DateTime<Utc>,
}

/// Aggregate worker lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerHealthStatus {
    /// Built but not yet accepting work.
    Starting,
    /// Running with no known background failures.
    Ready,
    /// Running with one or more failing background subsystems.
    Degraded,
    /// The worker run has ended.
    Stopped,
}

/// Point-in-time worker health.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct WorkerHealthSnapshot {
    /// Aggregate lifecycle state.
    pub status: WorkerHealthStatus,
    /// Active component failures, ordered by component name.
    pub failures: Vec<WorkerHealthFailure>,
}

/// Cloneable observer for a worker's local health state.
#[derive(Clone)]
pub struct WorkerHealth {
    receiver: watch::Receiver<WorkerHealthSnapshot>,
}

impl WorkerHealth {
    /// Returns the latest health snapshot without waiting.
    pub fn snapshot(&self) -> WorkerHealthSnapshot {
        self.receiver.borrow().clone()
    }

    /// Waits for a health change and returns the new snapshot.
    pub async fn changed(&mut self) -> WorkerHealthSnapshot {
        let _ = self.receiver.changed().await;
        self.snapshot()
    }
}

impl std::fmt::Debug for WorkerHealth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("WorkerHealth")
            .field(&self.snapshot())
            .finish()
    }
}

struct WorkerHealthReporter {
    sender: watch::Sender<WorkerHealthSnapshot>,
    failures: Mutex<HashMap<WorkerComponent, WorkerHealthFailure>>,
    running: AtomicBool,
    stopped: AtomicBool,
}

impl WorkerHealthReporter {
    fn new() -> Self {
        let (sender, _) = watch::channel(WorkerHealthSnapshot {
            status: WorkerHealthStatus::Starting,
            failures: Vec::new(),
        });
        Self {
            sender,
            failures: Mutex::new(HashMap::new()),
            running: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
        }
    }

    fn subscribe(&self) -> WorkerHealth {
        WorkerHealth {
            receiver: self.sender.subscribe(),
        }
    }

    fn ready(&self) {
        self.running.store(true, Ordering::Release);
        self.publish();
    }

    fn failed(&self, component: WorkerComponent, error: &impl std::fmt::Display) {
        if let Ok(mut failures) = self.failures.lock() {
            let message = error.to_string();
            failures
                .entry(component)
                .and_modify(|failure| failure.message.clone_from(&message))
                .or_insert_with(|| WorkerHealthFailure {
                    component,
                    message,
                    since: Utc::now(),
                });
        }
        self.publish();
    }

    fn recovered(&self, component: WorkerComponent) {
        if let Ok(mut failures) = self.failures.lock() {
            failures.remove(&component);
        }
        self.publish();
    }

    fn stopped(&self) {
        self.stopped.store(true, Ordering::Release);
        self.publish();
    }

    fn publish(&self) {
        let mut failures = self
            .failures
            .lock()
            .map(|failures| failures.values().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        failures.sort_by_key(|failure| failure.component);
        let status = if self.stopped.load(Ordering::Acquire) {
            WorkerHealthStatus::Stopped
        } else if !failures.is_empty() {
            WorkerHealthStatus::Degraded
        } else if self.running.load(Ordering::Acquire) {
            WorkerHealthStatus::Ready
        } else {
            WorkerHealthStatus::Starting
        };
        self.sender
            .send_replace(WorkerHealthSnapshot { status, failures });
    }
}

/// Intervals for the worker's periodic loops.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerTimers {
    /// How often in-flight jobs are checked for abort requests. Default 1s.
    pub abort: Duration,
    /// How often cron jobs are (re-)scheduled. Default 1s.
    pub schedule: Duration,
    /// How often the sweeper purges expired rows and recovers stuck jobs.
    /// Default 60s.
    pub sweep: Duration,
    /// How often worker stats are heartbeated for the dashboard. Default 10s.
    pub worker_info: Duration,
}

impl Default for WorkerTimers {
    fn default() -> Self {
        Self {
            abort: Duration::from_secs(1),
            schedule: Duration::from_secs(1),
            sweep: Duration::from_secs(60),
            worker_info: Duration::from_secs(10),
        }
    }
}

fn validate_runtime_duration(
    name: &str,
    duration: Duration,
    require_nonzero: bool,
) -> Result<(), Error> {
    if require_nonzero && duration.is_zero() {
        return Err(Error::Config(format!("{name} must be greater than zero")));
    }
    validate_duration(name, duration)?;
    if tokio::time::Instant::now().checked_add(duration).is_none() {
        return Err(Error::Config(format!(
            "{name} is too large for the runtime clock"
        )));
    }
    Ok(())
}

/// Configures a [`Worker`]. Created by [`Worker::builder`].
pub struct WorkerBuilder {
    queue: Queue,
    handlers: HashMap<&'static str, TypeErasedJobHandler>,
    state: JobStateMap,
    concurrency: usize,
    timers: WorkerTimers,
    crons: Vec<(String, crate::job::JobRequest, CronOptions)>,
    burst: bool,
    max_burst_jobs: Option<usize>,
    dequeue_timeout: Option<Duration>,
    poll_interval: Duration,
    shutdown_grace: Duration,
    metadata: Option<Value>,
    #[cfg(feature = "dashboard")]
    dashboard: Option<DashboardServer>,
    error: Option<Error>,
}

impl WorkerBuilder {
    /// Registers a `#[pgqueue::job]` or `#[pgqueue::cron]` function:
    /// `.register(send_email)`. Cron jobs bring their schedule along — no
    /// separate scheduling call is needed.
    pub fn register<J: JobType>(mut self, _job: J) -> Self {
        let handler = J::erased();
        let name = handler.name();
        if self.handlers.insert(name, handler).is_some() && self.error.is_none() {
            self.error = Some(Error::Config(format!("job {name:?} registered twice")));
        }
        if let Some(schedule) = J::SCHEDULE {
            // Cron payloads are always `()` (the #[pgqueue::cron] contract),
            // which serializes to null.
            let mut template = crate::job::JobRequest::new(J::NAME, Value::Null);
            template.config = J::config();
            self.crons.push((
                schedule.to_string(),
                template,
                CronOptions {
                    revision: J::CRON_REVISION,
                    ..CronOptions::default()
                },
            ));
        }
        self
    }

    /// Schedules a job on a cron expression decided at runtime (5-field, or
    /// 6 with seconds), evaluated in UTC:
    /// `.cron(&expr_from_config, cleanup::job())`.
    ///
    /// Prefer `#[pgqueue::cron("...")]` + [`WorkerBuilder::register`] when the
    /// schedule is known at compile time; this method is the escape hatch for
    /// config-driven schedules. This shorthand uses revision 0 and the default
    /// skip policy; use [`WorkerBuilder::cron_with_options`] before changing a
    /// persisted definition.
    ///
    /// Cron jobs are unique: the enqueue is deduplicated on
    /// `cron:{job name}` (or the builder's explicit `unique_key`), so each
    /// occurrence publishes at most one live job row across current workers.
    /// Job execution remains at least once.
    ///
    /// The cron expression owns every occurrence's run time, so a builder
    /// carrying [`JobBuilder::delay`] or [`JobBuilder::at`] makes `build()`
    /// fail instead of silently ignoring the override.
    pub fn cron<J: JobType>(mut self, expr: &str, job: JobBuilder<J>) -> Self {
        match job.into_cron_template() {
            Ok(template) => self
                .crons
                .push((expr.to_string(), template, CronOptions::default())),
            Err(error) if self.error.is_none() => self.error = Some(error),
            Err(_) => {}
        }
        self
    }

    /// Schedules a config-driven cron job with an explicit durable revision
    /// and misfire policy. Increase the revision whenever the expression or
    /// job template changes; equal revisions with different definitions make
    /// worker startup fail. A template-only revision preserves the durable
    /// cursor; changing the expression starts at its next UTC occurrence.
    pub fn cron_with_options<J: JobType>(
        mut self,
        expr: &str,
        job: JobBuilder<J>,
        options: CronOptions,
    ) -> Self {
        match job.into_cron_template() {
            Ok(template) => self.crons.push((expr.to_string(), template, options)),
            Err(error) if self.error.is_none() => self.error = Some(error),
            Err(_) => {}
        }
        self
    }

    /// Shares a value with handlers via the [`crate::JobState`] extractor.
    pub fn state<T: Clone + Send + Sync + 'static>(mut self, value: T) -> Self {
        self.state.insert(value);
        self
    }

    /// Maximum jobs processed concurrently. Default 10. Values that do not fit
    /// PostgreSQL's `bigint` dequeue limit are rejected by [`WorkerBuilder::build`].
    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency.max(1);
        self
    }

    /// Overrides the periodic loop intervals.
    pub fn timers(mut self, timers: WorkerTimers) -> Self {
        self.timers = timers;
        self
    }

    /// Burst mode: drain currently due work and return instead of running
    /// forever. Future scheduled work, including delayed retries, is left due
    /// for a later worker run.
    /// Requires [`WorkerBuilder::dequeue_timeout`].
    pub fn burst(mut self, burst: bool) -> Self {
        self.burst = burst;
        self
    }

    /// In burst mode, stop after processing this many jobs even if the queue
    /// isn't drained. Requires [`WorkerBuilder::burst`]; `build()` rejects it
    /// otherwise.
    pub fn max_burst_jobs(mut self, max: usize) -> Self {
        self.max_burst_jobs = Some(max);
        self
    }

    /// How long an idle processor waits for work before declaring the queue
    /// drained (burst mode only).
    pub fn dequeue_timeout(mut self, timeout: Duration) -> Self {
        self.dequeue_timeout = Some(timeout);
        self
    }

    /// Fallback polling interval when notifications are quiet. Default 1s.
    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// How long in-flight jobs get to finish on shutdown before being
    /// cancelled and requeued. Default 30s.
    pub fn shutdown_grace(mut self, grace: Duration) -> Self {
        self.shutdown_grace = grace;
        self
    }

    /// Arbitrary metadata shown alongside this worker in the dashboard.
    pub fn metadata(mut self, metadata: Value) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Runs a configured dashboard server in this worker's process.
    ///
    /// Bind failures and dashboard task panics are worker infrastructure
    /// errors. The server starts and stops with [`Worker::run`] or
    /// [`Worker::run_until`]. A later call replaces the previous dashboard.
    ///
    /// The socket is bound before processing starts so address conflicts fail
    /// fast. Use `/health`, with the configured authentication, rather than a
    /// TCP-only readiness check.
    /// Multiple workers in one network namespace must use distinct dashboard
    /// addresses or enable the dashboard on only one worker.
    ///
    /// ```no_run
    /// # #[pgqueue::job]
    /// # async fn cleanup(_: ()) {}
    /// # async fn run(queue: pgqueue::Queue) -> anyhow::Result<()> {
    /// let dashboard = pgqueue::Dashboard::new([queue.clone()])
    ///     .basic_auth("admin", "secret")
    ///     .serve_on("127.0.0.1:8080".parse()?);
    /// pgqueue::Worker::builder(queue)
    ///     .register(cleanup)
    ///     .dashboard(dashboard)
    ///     .build()?
    ///     .run()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "dashboard")]
    pub fn dashboard(mut self, server: DashboardServer) -> Self {
        self.dashboard = Some(server);
        self
    }

    /// Validates the configuration and builds the worker.
    pub fn build(self) -> Result<Worker, Error> {
        if let Some(error) = self.error {
            return Err(error);
        }
        if self.handlers.is_empty() {
            return Err(Error::Config("no jobs registered on this worker".into()));
        }
        if self.burst && self.dequeue_timeout.is_none() {
            return Err(Error::Config(
                "burst mode requires WorkerBuilder::dequeue_timeout".into(),
            ));
        }
        if self.max_burst_jobs.is_some() && !self.burst {
            return Err(Error::Config(
                "max_burst_jobs requires WorkerBuilder::burst(true)".into(),
            ));
        }
        if i64::try_from(self.concurrency).is_err() {
            return Err(Error::Config(
                "worker concurrency does not fit PostgreSQL bigint".into(),
            ));
        }
        for (name, duration) in [
            ("abort timer", self.timers.abort),
            ("schedule timer", self.timers.schedule),
            ("sweep timer", self.timers.sweep),
            ("worker info timer", self.timers.worker_info),
            ("poll interval", self.poll_interval),
        ] {
            validate_runtime_duration(name, duration, true)?;
        }
        let worker_info_ttl = worker_info_ttl(self.timers.worker_info);
        validate_duration("worker info TTL", worker_info_ttl)?;
        validate_runtime_duration("shutdown grace", self.shutdown_grace, false)?;
        if let Some(timeout) = self.dequeue_timeout {
            validate_runtime_duration("dequeue timeout", timeout, true)?;
        }
        let mut crons = Vec::new();
        let mut cron_keys = HashSet::new();
        for (expr, template, options) in self.crons {
            if !self.handlers.contains_key(template.name.as_str()) {
                return Err(Error::Config(format!(
                    "cron job {:?} is not registered on this worker",
                    template.name
                )));
            }
            let entry = JobCronEntry::with_options(&expr, template, options)?;
            if !cron_keys.insert(entry.unique_key.clone()) {
                return Err(Error::Config(format!(
                    "cron unique key {:?} registered more than once",
                    entry.unique_key
                )));
            }
            crons.push(entry);
        }

        let health = WorkerHealthReporter::new();

        #[cfg(feature = "dashboard")]
        let dashboard = self
            .dashboard
            .map(|dashboard| dashboard.into_worker_dashboard(health.subscribe()))
            .transpose()?;

        let database = self.queue.database();
        Ok(Worker {
            inner: Arc::new(WorkerInner {
                queue: self.queue,
                database,
                handlers: self.handlers,
                state: Arc::new(self.state),
                concurrency: self.concurrency,
                timers: self.timers,
                crons,
                burst: self.burst,
                dequeue_timeout: self.dequeue_timeout,
                poll_interval: self.poll_interval,
                shutdown_grace: self.shutdown_grace,
                metadata: self.metadata,
                #[cfg(feature = "dashboard")]
                dashboard,
                id: Uuid::now_v7(),
                started: OnceLock::new(),
                counters: QueueCounters::default(),
                inflight: Mutex::new(HashMap::new()),
                burst_budget: self.max_burst_jobs.map(AtomicUsize::new),
                health,
            }),
        })
    }
}

/// A job-processing worker bound to one [`Queue`].
pub struct Worker {
    inner: Arc<WorkerInner>,
}

struct WorkerInner {
    queue: Queue,
    database: Arc<Database>,
    handlers: HashMap<&'static str, TypeErasedJobHandler>,
    state: Arc<JobStateMap>,
    concurrency: usize,
    timers: WorkerTimers,
    crons: Vec<JobCronEntry>,
    burst: bool,
    dequeue_timeout: Option<Duration>,
    poll_interval: Duration,
    shutdown_grace: Duration,
    metadata: Option<Value>,
    #[cfg(feature = "dashboard")]
    dashboard: Option<DashboardWorkerConfig>,
    id: Uuid,
    started: OnceLock<std::time::Instant>,
    counters: QueueCounters,
    inflight: Mutex<HashMap<Uuid, WorkerInflightJob>>,
    /// Remaining burst-mode job budget (only meaningful with max_burst_jobs).
    burst_budget: Option<AtomicUsize>,
    health: WorkerHealthReporter,
}

struct WorkerHealthStopGuard(Arc<WorkerInner>);

impl Drop for WorkerHealthStopGuard {
    fn drop(&mut self) {
        self.0.health.stopped();
    }
}

impl Worker {
    /// Starts configuring a worker for the given queue.
    pub fn builder(queue: Queue) -> WorkerBuilder {
        WorkerBuilder {
            queue,
            handlers: HashMap::new(),
            state: JobStateMap::default(),
            concurrency: 10,
            timers: WorkerTimers::default(),
            crons: Vec::new(),
            burst: false,
            max_burst_jobs: None,
            dequeue_timeout: None,
            poll_interval: Duration::from_secs(1),
            shutdown_grace: Duration::from_secs(30),
            metadata: None,
            #[cfg(feature = "dashboard")]
            dashboard: None,
            error: None,
        }
    }

    /// This worker's id (UUIDv7, minted at build time).
    pub fn id(&self) -> Uuid {
        self.inner.id
    }

    /// The queue this worker processes.
    pub fn queue(&self) -> &Queue {
        &self.inner.queue
    }

    /// Returns a cloneable observer that remains usable while `run` consumes
    /// the worker.
    pub fn health(&self) -> WorkerHealth {
        self.inner.health.subscribe()
    }

    /// Runs until `SIGINT`/`SIGTERM` (or the queue drains, in burst mode),
    /// then shuts down gracefully.
    pub async fn run(self) -> Result<(), Error> {
        let token = CancellationToken::new();
        let run = self.run_until(token.clone());
        tokio::pin!(run);
        tokio::select! {
            result = &mut run => result,
            _ = wait_for_shutdown_signal() => {
                token.cancel();
                run.await
            }
        }
    }

    /// Runs until `shutdown` is cancelled (or the queue drains, in burst
    /// mode). The embeddable, test-friendly entry point.
    ///
    /// Dropping this future starts the same graceful shutdown in a background
    /// task, so worker infrastructure and in-flight jobs are not abandoned.
    pub async fn run_until(self, shutdown: CancellationToken) -> Result<(), Error> {
        let dropped = CancellationToken::new();
        let drop_guard = dropped.clone().drop_guard();
        let result = tokio::spawn(self.run_until_inner(shutdown, dropped)).await?;
        drop_guard.disarm();
        result
    }

    async fn run_until_inner(
        self,
        shutdown: CancellationToken,
        dropped: CancellationToken,
    ) -> Result<(), Error> {
        let inner = self.inner;
        let _health_stop = WorkerHealthStopGuard(inner.clone());
        #[cfg(feature = "dashboard")]
        let bound_dashboard = bind_dashboard(inner.dashboard.as_ref()).await?;
        inner.started.get_or_init(std::time::Instant::now);

        tracing::info!(
            worker.id = %inner.id, queue = %inner.queue.name(),
            concurrency = inner.concurrency, burst = inner.burst, "worker starting"
        );
        if !inner.crons.is_empty() {
            let now = match inner.database.now().await {
                Ok(now) => now,
                Err(error) => {
                    inner.health.failed(WorkerComponent::Scheduler, &error);
                    return Err(error);
                }
            };
            let mut inactive = Vec::new();
            for entry in &inner.crons {
                let authority = tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => return Ok(()),
                    _ = dropped.cancelled() => return Ok(()),
                    authority = inner.database.reconcile_cron(entry, now) => authority?,
                };
                match authority {
                    DatabaseCronAuthority::Active => {}
                    DatabaseCronAuthority::Inactive { revision } => {
                        inactive.push(format!(
                            "cron {:?} local revision {} is below authority {revision}",
                            entry.unique_key, entry.options.revision
                        ));
                    }
                }
            }
            if inactive.is_empty() {
                inner.health.recovered(WorkerComponent::Scheduler);
            } else {
                inner
                    .health
                    .failed(WorkerComponent::Scheduler, &inactive.join("; "));
            }
        }
        write_worker_info(&inner, worker_info_ttl(inner.timers.worker_info)).await;

        let listener = match inner.database.notify_listener().await {
            Ok(listener) => {
                inner.health.recovered(WorkerComponent::Notification);
                listener
            }
            Err(error) => {
                inner.health.failed(WorkerComponent::Notification, &error);
                return Err(error);
            }
        };
        let wakeup = listener.subscribe_wakeup();
        let notification_health = listener.subscribe_health();
        let stop_intake = CancellationToken::new();
        let intake = Arc::new(WorkerIntake::new());
        let (fetcher_exit_tx, mut fetcher_exit) = tokio::sync::oneshot::channel();
        let fetch_inner = inner.clone();
        let fetch_intake = intake.clone();
        let fetch_stop = stop_intake.clone();
        let mut fetcher = Some(tokio::spawn(async move {
            fetch_loop(fetch_inner, fetch_intake, fetch_stop, wakeup).await;
            let _ = fetcher_exit_tx.send(());
        }));
        let mut processors = JoinSet::new();
        for _ in 0..inner.concurrency {
            processors.spawn(processor_loop(
                inner.clone(),
                intake.clone(),
                stop_intake.clone(),
            ));
        }

        let timer_token = CancellationToken::new();
        let mut timer_tasks = JoinSet::new();
        let timer_inner = inner.clone();
        let notification_token = timer_token.clone();
        timer_tasks.spawn(async move {
            notification_health_loop(timer_inner, notification_token, notification_health).await;
            "notification health loop"
        });
        let timer_inner = inner.clone();
        let abort_token = timer_token.clone();
        timer_tasks.spawn(async move {
            abort_loop(timer_inner, abort_token).await;
            "abort loop"
        });
        let timer_inner = inner.clone();
        let sweep_token = timer_token.clone();
        timer_tasks.spawn(async move {
            sweep_loop(timer_inner, sweep_token).await;
            "sweep loop"
        });
        let timer_inner = inner.clone();
        let worker_info_token = timer_token.clone();
        timer_tasks.spawn(async move {
            worker_info_loop(timer_inner, worker_info_token).await;
            "worker info loop"
        });
        if !inner.crons.is_empty() {
            let timer_inner = inner.clone();
            let schedule_token = timer_token.clone();
            timer_tasks.spawn(async move {
                schedule_loop(timer_inner, schedule_token).await;
                "schedule loop"
            });
        }
        inner.health.ready();

        #[cfg(feature = "dashboard")]
        let mut dashboard = bound_dashboard.map(DashboardRuntime::start);

        // Wait for a shutdown request, (burst) for every processor to drain,
        // or for a configured dashboard server to fail.
        let mut fetcher_stopped = false;
        #[cfg(feature = "dashboard")]
        let mut run_error = {
            let dashboard_exit = wait_for_dashboard_exit(&mut dashboard);
            tokio::pin!(dashboard_exit);

            tokio::select! {
                _ = wait_for_worker_shutdown(&shutdown, &dropped) => {
                    tracing::info!(worker.id = %inner.id, "shutdown requested");
                    None
                }
                result = wait_for_processors(&mut processors, inner.burst) => {
                    match result {
                        Ok(()) => {
                            tracing::info!(worker.id = %inner.id, "burst complete: queue drained");
                            None
                        }
                        Err(error) => Some(error),
                    }
                }
                _ = &mut fetcher_exit => {
                    fetcher_stopped = true;
                    None
                }
                error = wait_for_background_exit(&mut timer_tasks) => {
                    Some(error)
                }
                error = &mut dashboard_exit => {
                    tracing::error!(worker.id = %inner.id, %error, "dashboard server failed");
                    Some(error)
                }
            }
        };

        #[cfg(not(feature = "dashboard"))]
        let mut run_error = tokio::select! {
            _ = wait_for_worker_shutdown(&shutdown, &dropped) => {
                tracing::info!(worker.id = %inner.id, "shutdown requested");
                None
            }
            result = wait_for_processors(&mut processors, inner.burst) => {
                match result {
                    Ok(()) => {
                        tracing::info!(worker.id = %inner.id, "burst complete: queue drained");
                        None
                    }
                    Err(error) => Some(error),
                }
            }
            _ = &mut fetcher_exit => {
                fetcher_stopped = true;
                None
            }
            error = wait_for_background_exit(&mut timer_tasks) => {
                Some(error)
            }
        };

        if fetcher_stopped {
            let error = match fetcher.take() {
                Some(fetcher) => unexpected_task_exit("fetch loop", fetcher.await),
                None => Error::WorkerTask("fetch loop"),
            };
            tracing::error!(worker.id = %inner.id, %error, "worker infrastructure failed");
            run_error = Some(error);
        } else if let Some(error) = run_error.as_ref() {
            tracing::error!(worker.id = %inner.id, %error, "worker infrastructure failed");
        }

        // Graceful shutdown: stop taking work, give in-flight jobs the grace
        // period, then cancel them (they requeue) and hard-stop.
        let grace_deadline = tokio::time::Instant::now() + inner.shutdown_grace;
        intake.begin_shutdown();
        stop_intake.cancel();
        match tokio::time::timeout_at(grace_deadline, inner.database.stop_worker_intake(inner.id))
            .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::error!(worker.id = %inner.id, %error, "failed to close worker intake");
            }
            Err(_) => {
                tracing::warn!(worker.id = %inner.id, "worker intake close exceeded shutdown grace");
            }
        }
        // A fetcher may be between a committed dequeue and returning its rows
        // to Rust, so keep it alive while processors still own attempts. Its
        // caretaker heartbeats the lease while it drains committed rows. Once
        // processors are done, the outer timeout gives that drain the hard
        // shutdown bound before aborting it and letting the lease expire.
        let release_fetcher_lease = CancellationToken::new();
        let fetcher_abort = fetcher.as_ref().map(JoinHandle::abort_handle);
        let mut fetcher_caretaker = tokio::spawn(finish_fetcher_shutdown(
            inner.clone(),
            fetcher,
            release_fetcher_lease.clone(),
        ));
        if tokio::time::timeout_at(
            grace_deadline,
            join_all(&mut processors, &mut run_error, false),
        )
        .await
        .is_err()
        {
            tracing::warn!(worker.id = %inner.id, "grace period expired; cancelling in-flight jobs");
            for entry in inner
                .inflight
                .lock()
                .map(|m| m.values().cloned().collect::<Vec<_>>())
                .unwrap_or_default()
            {
                entry.token.cancel();
            }
            if tokio::time::timeout(
                HARD_SHUTDOWN_TIMEOUT,
                join_all(&mut processors, &mut run_error, false),
            )
            .await
            .is_err()
            {
                processors.abort_all();
                join_all(&mut processors, &mut run_error, true).await;
                if run_error.is_none() {
                    run_error = Some(Error::WorkerTask("processor shutdown timed out"));
                }
            }
        }
        timer_token.cancel();
        if tokio::time::timeout(
            SHUTDOWN_STEP_TIMEOUT,
            join_all(&mut timer_tasks, &mut run_error, false),
        )
        .await
        .is_err()
        {
            tracing::warn!(worker.id = %inner.id, "timer task shutdown timed out");
            timer_tasks.abort_all();
            join_all(&mut timer_tasks, &mut run_error, true).await;
            if run_error.is_none() {
                run_error = Some(Error::WorkerTask("timer shutdown timed out"));
            }
        }
        // No processor or timer can touch a job after this point. The
        // caretaker expires the lease once its fetch/drain side is also done.
        release_fetcher_lease.cancel();
        match tokio::time::timeout(HARD_SHUTDOWN_TIMEOUT, &mut fetcher_caretaker).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(error))) => {
                tracing::error!(worker.id = %inner.id, %error, "fetcher shutdown failed");
                run_error = run_error.or(Some(error));
            }
            Ok(Err(error)) => {
                tracing::error!(worker.id = %inner.id, %error, "fetcher caretaker failed");
                run_error = run_error.or(Some(Error::Task(error)));
            }
            Err(_) => {
                // Do not leave a detached fetcher or caretaker capable of
                // touching jobs or refreshing the lease after return.
                if let Some(fetcher_abort) = fetcher_abort {
                    fetcher_abort.abort();
                }
                fetcher_caretaker.abort();
                let _ = fetcher_caretaker.await;
                tracing::warn!(
                    worker.id = %inner.id,
                    "fetcher cleanup timed out; its worker lease will expire"
                );
                if run_error.is_none() {
                    run_error = Some(Error::WorkerTask("fetcher shutdown timed out"));
                }
            }
        }

        #[cfg(feature = "dashboard")]
        if let Some(dashboard) = dashboard.as_mut()
            && let Err(error) = dashboard.finish_shutdown().await
        {
            run_error = run_error.or(Some(error));
        }

        tracing::info!(worker.id = %inner.id, "worker stopped");
        match run_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

impl std::fmt::Debug for Worker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Worker")
            .field("id", &self.inner.id)
            .field("queue", &self.inner.queue.name())
            .field("concurrency", &self.inner.concurrency)
            .finish_non_exhaustive()
    }
}

async fn join_all<T: 'static>(
    set: &mut JoinSet<T>,
    run_error: &mut Option<Error>,
    ignore_cancellation: bool,
) {
    while let Some(result) = set.join_next().await {
        if let Err(error) = result {
            if ignore_cancellation && error.is_cancelled() {
                continue;
            }
            tracing::error!(%error, "worker task failed during shutdown");
            if run_error.is_none() {
                *run_error = Some(Error::Task(error));
            }
        }
    }
}

async fn wait_for_processors(set: &mut JoinSet<()>, burst: bool) -> Result<(), Error> {
    while let Some(result) = set.join_next().await {
        result?;
        if !burst {
            return Err(Error::WorkerTask("processor loop"));
        }
    }
    Ok(())
}

async fn wait_for_background_exit(set: &mut JoinSet<&'static str>) -> Error {
    match set.join_next().await {
        Some(Ok(name)) => Error::WorkerTask(name),
        Some(Err(error)) => Error::Task(error),
        None => Error::WorkerTask("background loops"),
    }
}

fn unexpected_task_exit(name: &'static str, result: Result<(), tokio::task::JoinError>) -> Error {
    match result {
        Ok(()) => Error::WorkerTask(name),
        Err(error) => Error::Task(error),
    }
}

async fn wait_for_worker_shutdown(shutdown: &CancellationToken, dropped: &CancellationToken) {
    tokio::select! {
        _ = shutdown.cancelled() => {}
        _ = dropped.cancelled() => {}
    }
}

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        let mut sigterm =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(signal) => signal,
                Err(error) => {
                    tracing::error!(%error, "failed to install SIGTERM handler");
                    let _ = tokio::signal::ctrl_c().await;
                    return;
                }
            };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

const UNHANDLED_JOB_WARNING_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Clone)]
struct WorkerInflightJob {
    token: CancellationToken,
    abort_reason: Arc<OnceLock<WorkerAbortReason>>,
    attempts: i32,
}

#[derive(Clone)]
enum WorkerAbortReason {
    User(String),
    Swept,
}

/// Removes the in-flight entry even if processing unwinds.
struct WorkerInflightJobGuard<'a> {
    inner: &'a WorkerInner,
    id: Uuid,
    abort_reason: Arc<OnceLock<WorkerAbortReason>>,
}

impl Drop for WorkerInflightJobGuard<'_> {
    fn drop(&mut self) {
        if let Ok(mut map) = self.inner.inflight.lock() {
            let owns_entry = map
                .get(&self.id)
                .is_some_and(|entry| Arc::ptr_eq(&entry.abort_reason, &self.abort_reason));
            if owns_entry {
                map.remove(&self.id);
            }
        }
    }
}

enum WorkerFetch {
    Job(Box<JobRow>),
    Stop,
    Drained,
}

enum WorkerAttemptResult {
    Success(Value),
    Errored(JobError),
    Cancelled,
}

enum WorkerProcessOutcome {
    Complete,
    Retried(JobError),
    Failed(JobError),
    Aborted(JobError),
    Requeued,
    Unconfirmed,
}

/// One processing slot: fetch → process, until stopped (or drained in burst).
/// In-process handoff between the worker's single fetcher and its processor
/// slots: one batched dequeue per wakeup instead of a thundering herd of
/// per-slot `dequeue(1)` transactions racing on the advisory lock.
struct WorkerIntake {
    buffer: Mutex<VecDeque<JobRow>>,
    /// Wakes processors when the buffer is refilled.
    refilled: tokio::sync::Notify,
    /// Wakes the fetcher when a processor goes idle (new demand).
    demand: tokio::sync::Notify,
    /// Processors currently waiting for work — the fetcher's batch size.
    idle: AtomicUsize,
    /// Monotonic demand and drain-proof generations. A burst processor can
    /// only drain after a valid underfilled fetch begun after its demand.
    demand_generation: AtomicU64,
    drained_generation: AtomicU64,
    /// Set under the buffer lock before shutdown so no buffered row can race
    /// from fetcher cleanup into a processor.
    stopping: AtomicBool,
}

impl WorkerIntake {
    fn new() -> Self {
        Self {
            buffer: Mutex::new(VecDeque::new()),
            refilled: tokio::sync::Notify::new(),
            demand: tokio::sync::Notify::new(),
            idle: AtomicUsize::new(0),
            demand_generation: AtomicU64::new(0),
            drained_generation: AtomicU64::new(0),
            stopping: AtomicBool::new(false),
        }
    }

    /// Claims one buffered job and withdraws this processor's demand while the
    /// buffer lock is held, giving the fetcher a coherent `(buffered, idle)`
    /// snapshot.
    fn claim(&self) -> Option<JobRow> {
        let mut buffer = self.buffer.lock().ok()?;
        if self.stopping.load(Ordering::Acquire) {
            return None;
        }
        let job = buffer.pop_front()?;
        self.idle.fetch_sub(1, Ordering::AcqRel);
        Some(job)
    }

    fn register_demand(&self) -> u64 {
        let _buffer = self.buffer.lock().ok();
        let generation = self.demand_generation.fetch_add(1, Ordering::AcqRel) + 1;
        self.idle.fetch_add(1, Ordering::AcqRel);
        generation
    }

    fn demand_is_drained(&self, generation: u64) -> bool {
        self.drained_generation.load(Ordering::Acquire) >= generation
    }

    fn withdraw_demand(&self) {
        let _buffer = self.buffer.lock().ok();
        self.idle.fetch_sub(1, Ordering::AcqRel);
    }

    fn begin_shutdown(&self) {
        if let Ok(_buffer) = self.buffer.lock() {
            self.stopping.store(true, Ordering::Release);
        }
    }
}

/// The worker's single dequeuer: fetches `idle`-sized batches on wakeup hints
/// (with an interval fallback — notifications can be lost across listener
/// reconnects) and hands jobs to processors through the intake buffer.
async fn fetch_loop(
    inner: Arc<WorkerInner>,
    intake: Arc<WorkerIntake>,
    stop: CancellationToken,
    mut wakeup: broadcast::Receiver<()>,
) {
    let mut registered_names = inner
        .handlers
        .keys()
        .map(|name| (*name).to_string())
        .collect::<Vec<_>>();
    registered_names.sort_unstable();
    // Tracks probes, not warnings: a probe that finds nothing must still
    // start the cooldown, or an idle worker would rescan job names on every
    // empty poll.
    let mut last_unhandled_probe: Option<tokio::time::Instant> = None;
    let mut retry_max_ms = DEQUEUE_RETRY_INITIAL_MAX_MS;
    loop {
        // Fill demand: batch size = processors currently waiting.
        loop {
            if stop.is_cancelled() {
                drain_on_shutdown(&inner, &intake).await;
                return;
            }
            let (want, demand_generation) = intake
                .buffer
                .lock()
                .map(|buffer| {
                    (
                        intake
                            .idle
                            .load(Ordering::Acquire)
                            .saturating_sub(buffer.len()),
                        intake.demand_generation.load(Ordering::Acquire),
                    )
                })
                .unwrap_or((0, 0));
            if want == 0 {
                break;
            }
            let probe_unhandled = last_unhandled_probe
                .is_none_or(|last| last.elapsed() >= UNHANDLED_JOB_WARNING_INTERVAL);
            let dequeue = inner
                .database
                .dequeue_worker(want as i64, inner.id, &registered_names, probe_unhandled)
                .await;
            if dequeue.is_ok() {
                inner.health.recovered(WorkerComponent::Dequeue);
            }
            match dequeue {
                Ok(result) if result.lock_contended => {
                    // Worker fetchers are opportunistic: unlike the public
                    // dequeue API, they must not form a connection-pinning
                    // convoy behind another process's queue lock. Small jitter
                    // keeps a many-worker wakeup from immediately colliding
                    // again while preserving outstanding processor demand.
                    if !wait_for_dequeue_retry(&stop, retry_max_ms).await {
                        drain_on_shutdown(&inner, &intake).await;
                        return;
                    }
                    retry_max_ms = (retry_max_ms * 2).min(DEQUEUE_RETRY_MAX_MS);
                }
                Ok(result)
                    if result.jobs.is_empty() && result.intake_open && !result.work_available =>
                {
                    retry_max_ms = DEQUEUE_RETRY_INITIAL_MAX_MS;
                    intake
                        .drained_generation
                        .fetch_max(demand_generation, Ordering::AcqRel);
                    intake.refilled.notify_waiters();
                    if probe_unhandled {
                        last_unhandled_probe = Some(tokio::time::Instant::now());
                        if !result.unhandled_names.is_empty() {
                            tracing::warn!(
                                worker.id = %inner.id,
                                queue = %inner.queue.name(),
                                job.names = ?result.unhandled_names,
                                "due jobs are queued with no handler registered on this worker"
                            );
                        }
                    }
                    break;
                }
                Ok(result) if result.jobs.is_empty() && result.intake_open => {
                    // `SKIP LOCKED` can produce an empty batch while a matching
                    // ready row is being inspected or updated elsewhere. Keep
                    // burst demand outstanding until a later fetch can make a
                    // definitive drain decision.
                    if probe_unhandled {
                        last_unhandled_probe = Some(tokio::time::Instant::now());
                        if !result.unhandled_names.is_empty() {
                            tracing::warn!(
                                worker.id = %inner.id,
                                queue = %inner.queue.name(),
                                job.names = ?result.unhandled_names,
                                "due jobs are queued with no handler registered on this worker"
                            );
                        }
                    }
                    if !wait_for_dequeue_retry(&stop, retry_max_ms).await {
                        drain_on_shutdown(&inner, &intake).await;
                        return;
                    }
                    retry_max_ms = (retry_max_ms * 2).min(DEQUEUE_RETRY_MAX_MS);
                }
                Ok(result) if result.jobs.is_empty() => {
                    retry_max_ms = DEQUEUE_RETRY_INITIAL_MAX_MS;
                    tracing::debug!(
                        worker.id = %inner.id,
                        "dequeue skipped while the worker intake lease is closed or expired"
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    break;
                }
                Ok(result) => {
                    retry_max_ms = DEQUEUE_RETRY_INITIAL_MAX_MS;
                    let fetched = result.jobs.len();
                    let work_available = result.work_available;
                    if let Ok(mut buffer) = intake.buffer.lock() {
                        buffer.extend(result.jobs);
                    }
                    intake.refilled.notify_waiters();
                    // A dequeue in flight when shutdown began can still return
                    // after intake was frozen. Rows enter shared state before
                    // any cleanup await, making task cancellation lossless.
                    if stop.is_cancelled() {
                        drain_on_shutdown(&inner, &intake).await;
                        return;
                    }
                    if fetched < want && !work_available {
                        intake
                            .drained_generation
                            .fetch_max(demand_generation, Ordering::AcqRel);
                        intake.refilled.notify_waiters();
                        break;
                    }
                }
                Err(error) => {
                    inner.health.failed(WorkerComponent::Dequeue, &error);
                    tracing::error!(worker.id = %inner.id, %error, "dequeue failed");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    break;
                }
            }
        }
        tokio::select! {
            _ = stop.cancelled() => {
                drain_on_shutdown(&inner, &intake).await;
                return;
            }
            _ = wakeup.recv() => {}
            _ = intake.demand.notified() => {}
            _ = tokio::time::sleep(inner.poll_interval) => {}
        }
    }
}

async fn wait_for_dequeue_retry(stop: &CancellationToken, max_ms: u64) -> bool {
    let delay_ms = 1 + u64::from(rand::random::<u8>()) % max_ms;
    tokio::select! {
        _ = stop.cancelled() => false,
        _ = tokio::time::sleep(Duration::from_millis(delay_ms)) => true,
    }
}

/// Keeps an intake-stopped fetcher's lease alive until it has drained every
/// committed row, then expires the lease once processor shutdown permits it.
async fn finish_fetcher_shutdown(
    inner: Arc<WorkerInner>,
    fetcher: Option<JoinHandle<()>>,
    release_lease: CancellationToken,
) -> Result<(), Error> {
    let mut heartbeat = tokio::time::interval(inner.timers.worker_info);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut fetch_error = None;
    if let Some(mut fetcher) = fetcher {
        loop {
            tokio::select! {
                biased;
                result = &mut fetcher => {
                    if let Err(error) = result {
                        fetch_error = Some(Error::Task(error));
                    }
                    break;
                }
                _ = heartbeat.tick() => refresh_fetcher_lease(&inner).await,
            }
        }
    }
    loop {
        tokio::select! {
            biased;
            _ = release_lease.cancelled() => break,
            _ = heartbeat.tick() => refresh_fetcher_lease(&inner).await,
        }
    }
    write_worker_info(&inner, Duration::ZERO).await;
    match fetch_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

async fn refresh_fetcher_lease(inner: &Arc<WorkerInner>) {
    if tokio::time::timeout(
        SHUTDOWN_STEP_TIMEOUT,
        write_worker_info(inner, worker_info_ttl(inner.timers.worker_info)),
    )
    .await
    .is_err()
    {
        tracing::warn!(worker.id = %inner.id, "fetcher lease heartbeat timed out");
    }
}

/// Requeues buffered-but-unclaimed jobs when the worker stops taking work.
async fn drain_on_shutdown(inner: &Arc<WorkerInner>, intake: &WorkerIntake) {
    loop {
        let job = intake
            .buffer
            .lock()
            .ok()
            .and_then(|buffer| buffer.front().cloned());
        let Some(job) = job else {
            return;
        };
        let settled = match inner.database.requeue_shutdown(&job).await {
            Ok(true) => true,
            Ok(false) => match inner
                .database
                .finish(&job, JobStatus::Aborted, None, None)
                .await
            {
                Ok(true) => {
                    inner.counters.record_abort();
                    true
                }
                Ok(false) => true,
                Err(error) => {
                    tracing::error!(job.id = %job.id, %error, "failed to finalize aborted buffered job during shutdown");
                    false
                }
            },
            Err(error) => {
                tracing::error!(job.id = %job.id, %error, "failed to requeue buffered job during shutdown");
                false
            }
        };
        if settled {
            if let Ok(mut buffer) = intake.buffer.lock()
                && buffer.front().is_some_and(|front| front.id == job.id)
            {
                buffer.pop_front();
            }
        } else {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

#[allow(deprecated)] // `try_update` is newer than the Rust 1.94 MSRV.
async fn processor_loop(
    inner: Arc<WorkerInner>,
    intake: Arc<WorkerIntake>,
    stop: CancellationToken,
) {
    loop {
        // Burst cap: reserve budget BEFORE fetching so `concurrency`
        // processors can't all slip past the check together.
        if inner.burst_budget.as_ref().is_some_and(|budget| {
            budget
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_err()
        }) {
            return;
        }
        match next_job(&inner, &intake, &stop).await {
            WorkerFetch::Job(job) => process(&inner, *job, &stop).await,
            WorkerFetch::Stop => return,
            WorkerFetch::Drained => {
                tracing::debug!(worker.id = %inner.id, "processor drained");
                return;
            }
        }
    }
}

/// Waits for a job from the intake buffer (the fetcher does all DB work).
async fn next_job(
    inner: &Arc<WorkerInner>,
    intake: &WorkerIntake,
    stop: &CancellationToken,
) -> WorkerFetch {
    let deadline = inner
        .burst
        .then(|| inner.dequeue_timeout)
        .flatten()
        .and_then(|t| tokio::time::Instant::now().checked_add(t));

    // Register demand once for the whole idle period (the counter is the
    // fetcher's batch size), pinging the fetcher so a refill between the
    // buffer check and the wait can't be missed.
    let demand_generation = intake.register_demand();
    intake.demand.notify_one();
    let mut deadline_elapsed = false;
    let outcome = loop {
        if stop.is_cancelled() {
            break WorkerFetch::Stop;
        }
        if let Some(job) = intake.claim() {
            break WorkerFetch::Job(Box::new(job));
        }
        if deadline_elapsed && intake.demand_is_drained(demand_generation) {
            break WorkerFetch::Drained;
        }
        tokio::select! {
            _ = stop.cancelled() => break WorkerFetch::Stop,
            _ = intake.refilled.notified() => {}
            // In-memory re-check fallback; the fetcher owns all DB polling.
            _ = tokio::time::sleep(inner.poll_interval) => {}
            _ = async {
                match (deadline, deadline_elapsed) {
                    (Some(deadline), false) => tokio::time::sleep_until(deadline).await,
                    _ => std::future::pending().await,
                }
            } => {
                deadline_elapsed = true;
            }
        }
    };
    if !matches!(outcome, WorkerFetch::Job(_)) {
        intake.withdraw_demand();
        // A processor that exits without taking a job returns its burst budget.
        if let Some(budget) = &inner.burst_budget {
            budget.fetch_add(1, Ordering::AcqRel);
        }
    }
    outcome
}

/// Runs one dequeued job through its handler and finalization.
async fn process(inner: &Arc<WorkerInner>, mut job: JobRow, stop: &CancellationToken) {
    let token = CancellationToken::new();
    let abort_reason = Arc::new(OnceLock::new());
    if let Ok(mut map) = inner.inflight.lock() {
        map.insert(
            job.id,
            WorkerInflightJob {
                token: token.clone(),
                abort_reason: abort_reason.clone(),
                attempts: job.attempts,
            },
        );
    }
    let _guard = WorkerInflightJobGuard {
        inner,
        id: job.id,
        abort_reason: abort_reason.clone(),
    };

    let ctx = JobContext::new(
        inner.queue.clone(),
        job.clone(),
        inner.id,
        inner.state.clone(),
        token.clone(),
    );
    // The context owns the full dequeue snapshot; finalization never reads the
    // payload, so move this copy into the handler instead of cloning it again.
    let payload = std::mem::take(&mut job.payload);
    let span = tracing::info_span!(
        "job.run",
        job.name = %job.name,
        job.id = %job.id,
        attempt = job.attempts,
        queue = %inner.queue.name(),
    );

    async {
        let end = run_attempt(inner, &job, payload, &ctx, &token).await;
        let outcome = finalize(inner, &job, end, &abort_reason, stop).await;
        match &outcome {
            WorkerProcessOutcome::Complete => inner.counters.record_complete(),
            WorkerProcessOutcome::Retried(_) | WorkerProcessOutcome::Requeued => {
                inner.counters.record_retry()
            }
            WorkerProcessOutcome::Failed(_) => inner.counters.record_failed(),
            WorkerProcessOutcome::Aborted(_) => inner.counters.record_abort(),
            WorkerProcessOutcome::Unconfirmed => {}
        }
        match &outcome {
            WorkerProcessOutcome::Complete => tracing::info!("job complete"),
            WorkerProcessOutcome::Retried(e) => {
                tracing::warn!(error = %e, "job attempt failed; retrying")
            }
            WorkerProcessOutcome::Failed(e) => tracing::error!(error = %e, "job failed"),
            WorkerProcessOutcome::Aborted(e) => tracing::warn!(error = %e, "job aborted"),
            WorkerProcessOutcome::Requeued => tracing::info!("job requeued for shutdown"),
            WorkerProcessOutcome::Unconfirmed => {
                tracing::warn!("job outcome was not confirmed by the database")
            }
        }
    }
    .instrument(span)
    .await;
}

/// Executes the handler in an owned task for panic containment, under the
/// job's timeout and cancellation token.
async fn run_attempt(
    inner: &Arc<WorkerInner>,
    job: &JobRow,
    payload: Value,
    ctx: &JobContext,
    token: &CancellationToken,
) -> WorkerAttemptResult {
    let Some(handler) = inner.handlers.get(job.name.as_str()).cloned() else {
        return WorkerAttemptResult::Errored(JobError::failed(format!(
            "no handler registered for job {:?}",
            job.name
        )));
    };

    let ctx = ctx.clone();
    let mut task = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
        handler.call(payload, ctx).await
    }));
    let timeout = job.timeout();
    tokio::select! {
        biased;
        _ = token.cancelled() => {
            task.abort();
            let _ = (&mut task).await;
            // An explicit shutdown/abort request wins even if the handler
            // happened to become ready in the same scheduler turn.
            WorkerAttemptResult::Cancelled
        }
        result = &mut task => classify_attempt_join(result, WorkerAttemptResult::Cancelled),
        _ = async {
            match timeout {
                Some(timeout) => tokio::time::sleep(timeout).await,
                None => std::future::pending().await,
            }
        } => {
            task.abort();
            let timed_out = WorkerAttemptResult::Errored(JobError::new(
                    JobErrorKind::Timeout,
                    format!("attempt exceeded {:?}", timeout.unwrap_or_default()),
                ));
            classify_attempt_join((&mut task).await, timed_out)
        }
    }
}

fn classify_attempt_join(
    result: Result<Result<Value, JobError>, tokio::task::JoinError>,
    cancelled: WorkerAttemptResult,
) -> WorkerAttemptResult {
    match result {
        Ok(Ok(value)) => WorkerAttemptResult::Success(value),
        Ok(Err(job_error)) => WorkerAttemptResult::Errored(job_error),
        Err(join_error) if join_error.is_panic() => WorkerAttemptResult::Errored(JobError::new(
            JobErrorKind::Panic,
            panic_message(join_error),
        )),
        Err(_) => cancelled,
    }
}

/// Applies the attempt's end state to the database. The in-flight guard and
/// worker ownership stay live while transient database errors are retried. On
/// shutdown, retrying stops so the worker lease can expire and make the row
/// recoverable by another process.
async fn finalize(
    inner: &Arc<WorkerInner>,
    job: &JobRow,
    end: WorkerAttemptResult,
    abort_reason: &OnceLock<WorkerAbortReason>,
    stop: &CancellationToken,
) -> WorkerProcessOutcome {
    loop {
        match try_finalize(inner, job, &end, abort_reason).await {
            Ok(outcome) => return outcome,
            Err(error) => {
                tracing::error!(%error, "failed to finalize job; retrying");
                tokio::select! {
                    _ = stop.cancelled() => return WorkerProcessOutcome::Unconfirmed,
                    _ = tokio::time::sleep(FINALIZE_RETRY_INTERVAL) => {}
                }
            }
        }
    }
}

async fn try_finalize(
    inner: &Arc<WorkerInner>,
    job: &JobRow,
    end: &WorkerAttemptResult,
    abort_reason: &OnceLock<WorkerAbortReason>,
) -> Result<WorkerProcessOutcome, Error> {
    let database = &inner.database;
    match end {
        WorkerAttemptResult::Success(value) => {
            finish_with_swept_fallback(
                database,
                job,
                JobStatus::Complete,
                Some(value.clone()),
                None,
                WorkerProcessOutcome::Complete,
            )
            .await
        }
        WorkerAttemptResult::Errored(error) => {
            if job.retryable() && error.kind.retryable() {
                match database.retry(job, &error.to_string()).await {
                    Ok(true) => Ok(WorkerProcessOutcome::Retried(error.clone())),
                    // Retry refused: the row moved to 'aborting' under us (a
                    // pending abort is never resurrected) or was swept.
                    // A sweeper abort is a retry request; a user abort is a
                    // terminal cancellation. The marker-guarded retry makes
                    // that distinction without trusting the reason string.
                    Ok(false) => retry_swept_or_refuse(database, job, error.clone()).await,
                    Err(db_error) => Err(db_error),
                }
            } else {
                let stored_error = error.to_string();
                finish_with_swept_fallback(
                    database,
                    job,
                    JobStatus::Failed,
                    None,
                    Some(&stored_error),
                    WorkerProcessOutcome::Failed(error.clone()),
                )
                .await
            }
        }
        WorkerAttemptResult::Cancelled => match abort_reason.get() {
            Some(WorkerAbortReason::Swept) if job.retryable() => {
                let error = JobError::new(JobErrorKind::Timeout, "swept");
                retry_swept_or_refuse(database, job, error).await
            }
            Some(abort_reason) => {
                let reason = match abort_reason {
                    WorkerAbortReason::Swept => "swept",
                    WorkerAbortReason::User(reason) => reason.as_str(),
                };
                let error = JobError::new(JobErrorKind::Aborted, reason);
                match database
                    .finish(job, JobStatus::Aborted, None, Some(reason))
                    .await
                {
                    Ok(true) => Ok(WorkerProcessOutcome::Aborted(error)),
                    Ok(false) => Ok(WorkerProcessOutcome::Unconfirmed),
                    Err(db_error) => Err(db_error),
                }
            }
            // Shutdown: requeue unconditionally. If an abort
            // raced shutdown (row now 'aborting'), retry is refused and the
            // sweeper finishes the abort later.
            None => match database.requeue_shutdown(job).await {
                Ok(true) => Ok(WorkerProcessOutcome::Requeued),
                Ok(false) => Ok(WorkerProcessOutcome::Unconfirmed),
                Err(db_error) => Err(db_error),
            },
        },
    }
}

async fn finish_with_swept_fallback(
    database: &Database,
    job: &JobRow,
    status: JobStatus,
    result: Option<Value>,
    error: Option<&str>,
    outcome: WorkerProcessOutcome,
) -> Result<WorkerProcessOutcome, Error> {
    // `Database::finish` already lets a handler complete through a sweeper's
    // grace window while never overwriting a user-requested abort.
    match database.finish(job, status, result, error).await {
        Ok(true) => Ok(outcome),
        Ok(false) => finish_aborted_fallback(database, job).await,
        Err(db_error) => Err(db_error),
    }
}

async fn retry_swept_or_refuse(
    database: &Database,
    job: &JobRow,
    error: JobError,
) -> Result<WorkerProcessOutcome, Error> {
    match database.retry_swept(job).await {
        Ok(true) => Ok(WorkerProcessOutcome::Retried(error)),
        Ok(false) => swept_retry_refusal_outcome(database, job, error).await,
        Err(db_error) => Err(db_error),
    }
}

async fn finish_aborted_fallback(
    database: &Database,
    job: &JobRow,
) -> Result<WorkerProcessOutcome, Error> {
    let aborted = JobError::new(JobErrorKind::Aborted, "abort requested during attempt");
    match database.finish(job, JobStatus::Aborted, None, None).await {
        Ok(true) => Ok(WorkerProcessOutcome::Aborted(aborted)),
        Ok(false) => {
            tracing::debug!("job already finalized elsewhere (likely swept)");
            Ok(WorkerProcessOutcome::Unconfirmed)
        }
        Err(db_error) => Err(db_error),
    }
}

async fn swept_retry_refusal_outcome(
    database: &Database,
    job: &JobRow,
    retry_error: JobError,
) -> Result<WorkerProcessOutcome, Error> {
    match database.job(job.id).await {
        Ok(Some(current))
            if current.attempts > job.attempts
                || matches!(current.status, JobStatus::Queued | JobStatus::Running) =>
        {
            Ok(WorkerProcessOutcome::Retried(retry_error))
        }
        Ok(Some(current)) if current.status == JobStatus::Aborted => {
            let error = JobError::new(
                JobErrorKind::Aborted,
                current.error.as_deref().unwrap_or("aborted"),
            );
            Ok(WorkerProcessOutcome::Aborted(error))
        }
        Ok(Some(_)) => finish_aborted_fallback(database, job).await,
        Ok(None) => Ok(WorkerProcessOutcome::Unconfirmed),
        Err(db_error) => Err(db_error),
    }
}

fn panic_message(join_error: tokio::task::JoinError) -> String {
    let payload = join_error.into_panic();
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "handler panicked".to_string()
    }
}

/// Cancels in-flight jobs whose rows moved to `aborting`/`aborted`.
async fn abort_loop(inner: Arc<WorkerInner>, token: CancellationToken) {
    let mut interval = tokio::time::interval(inner.timers.abort);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = token.cancelled() => return,
            _ = interval.tick() => {}
        }
        let ids: Vec<Uuid> = inner
            .inflight
            .lock()
            .map(|m| m.keys().copied().collect())
            .unwrap_or_default();
        if ids.is_empty() {
            inner.health.recovered(WorkerComponent::Abort);
            continue;
        }
        match inner.database.aborting_of(&ids).await {
            Ok(aborting) => {
                inner.health.recovered(WorkerComponent::Abort);
                for aborting in aborting {
                    let entry = inner
                        .inflight
                        .lock()
                        .ok()
                        .and_then(|m| m.get(&aborting.id).cloned());
                    if let Some(entry) = entry
                        && entry.attempts == aborting.attempts
                        && aborting.worker_id == Some(inner.id)
                    {
                        let reason = if aborting.swept {
                            WorkerAbortReason::Swept
                        } else {
                            WorkerAbortReason::User(
                                aborting.reason.unwrap_or_else(|| "aborted".to_string()),
                            )
                        };
                        let _ = entry.abort_reason.set(reason);
                        entry.token.cancel();
                    }
                }
            }
            Err(error) => {
                inner.health.failed(WorkerComponent::Abort, &error);
                tracing::warn!(%error, "abort poll failed");
            }
        }
    }
}

async fn notification_health_loop(
    inner: Arc<WorkerInner>,
    token: CancellationToken,
    mut health: watch::Receiver<Option<String>>,
) {
    loop {
        match health.borrow_and_update().clone() {
            Some(error) => inner.health.failed(WorkerComponent::Notification, &error),
            None => inner.health.recovered(WorkerComponent::Notification),
        }
        tokio::select! {
            _ = token.cancelled() => return,
            changed = health.changed() => {
                if changed.is_err() {
                    inner.health.failed(
                        WorkerComponent::Notification,
                        &"notification listener stopped",
                    );
                    token.cancelled().await;
                    return;
                }
            }
        }
    }
}

/// Advances durable cron cursors. Schedule rows are the authority; local
/// entries only act when their revision and canonical definition still match.
async fn schedule_loop(inner: Arc<WorkerInner>, token: CancellationToken) {
    let mut interval = tokio::time::interval(inner.timers.schedule);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut holder_warned = HashSet::new();
    loop {
        tokio::select! {
            _ = token.cancelled() => return,
            _ = interval.tick() => {}
        }
        let mut failed = Vec::new();
        for entry in &inner.crons {
            let outcome = tokio::select! {
                biased;
                _ = token.cancelled() => return,
                outcome = inner.database.schedule_cron(entry) => outcome,
            };
            match outcome {
                Ok(DatabaseCronScheduleOutcome::NotDue)
                | Ok(DatabaseCronScheduleOutcome::Contended) => {}
                Ok(DatabaseCronScheduleOutcome::Published { id, occurrence }) => {
                    holder_warned.remove(&entry.unique_key);
                    tracing::info!(
                        cron = %entry.template.name,
                        job.id = %id,
                        scheduled_at = %occurrence,
                        "published cron occurrence"
                    );
                }
                Ok(DatabaseCronScheduleOutcome::AlreadyPublished { occurrence }) => {
                    holder_warned.remove(&entry.unique_key);
                    tracing::debug!(
                        cron = %entry.template.name,
                        scheduled_at = %occurrence,
                        "cron occurrence was already published"
                    );
                }
                Ok(DatabaseCronScheduleOutcome::SkippedStale { occurrence }) => {
                    tracing::warn!(
                        cron = %entry.template.name,
                        scheduled_at = %occurrence,
                        "skipped stale cron occurrence"
                    );
                }
                Ok(DatabaseCronScheduleOutcome::SkippedHeld {
                    occurrence,
                    existing,
                }) => {
                    if holder_warned.insert(entry.unique_key.clone()) {
                        tracing::warn!(
                            cron = %entry.template.name,
                            scheduled_at = %occurrence,
                            unique_key = %entry.unique_key,
                            holder.scheduled_at = %existing.scheduled_at,
                            holder.kind = %existing.kind,
                            holder.name = %existing.name,
                            "cron unique key is held by another live job; occurrence skipped"
                        );
                    }
                }
                Ok(DatabaseCronScheduleOutcome::Inactive { revision }) => failed.push(format!(
                    "cron {:?} local revision {} is below or differs from authority {revision}",
                    entry.unique_key, entry.options.revision
                )),
                Err(error) => {
                    tracing::warn!(%error, cron = %entry.template.name, "cron scheduling failed");
                    failed.push(format!("{}: {error}", entry.template.name));
                }
            }
        }
        if failed.is_empty() {
            inner.health.recovered(WorkerComponent::Scheduler);
        } else {
            inner
                .health
                .failed(WorkerComponent::Scheduler, &failed.join("; "));
        }
    }
}

/// Runs the sweeper on its timer; leadership is advisory-lock coordinated.
async fn sweep_loop(inner: Arc<WorkerInner>, token: CancellationToken) {
    let mut sweeper = inner.database.sweeper();
    let mut interval = tokio::time::interval(inner.timers.sweep);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = token.cancelled() => {
                sweeper.release().await;
                return;
            }
            _ = interval.tick() => {}
        }
        const MAX_PASSES_PER_TICK: usize = 4;
        for _ in 0..MAX_PASSES_PER_TICK {
            match sweeper.sweep().await {
                Ok(report) if report.more_work => {
                    inner.health.recovered(WorkerComponent::Sweeper);
                    tokio::task::yield_now().await;
                }
                Ok(_) => {
                    inner.health.recovered(WorkerComponent::Sweeper);
                    break;
                }
                Err(error) => {
                    inner.health.failed(WorkerComponent::Sweeper, &error);
                    tracing::warn!(%error, "sweep failed");
                    break;
                }
            }
        }
    }
}

/// Heartbeats this worker's stats row for `Queue::info` / the dashboard.
async fn worker_info_loop(inner: Arc<WorkerInner>, token: CancellationToken) {
    let mut interval = tokio::time::interval(inner.timers.worker_info);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = token.cancelled() => return,
            _ = interval.tick() => {}
        }
        write_worker_info(&inner, worker_info_ttl(inner.timers.worker_info)).await;
    }
}

async fn write_worker_info(inner: &Arc<WorkerInner>, ttl: Duration) {
    let stats = match stats_json(inner) {
        Ok(stats) => stats,
        Err(error) => {
            inner.health.failed(WorkerComponent::WorkerInfo, &error);
            tracing::warn!(%error, "failed to serialize worker info");
            return;
        }
    };
    if let Err(error) = inner
        .database
        .write_worker_info(inner.id, stats, inner.metadata.clone(), ttl, false)
        .await
    {
        inner.health.failed(WorkerComponent::WorkerInfo, &error);
        tracing::warn!(%error, "failed to write worker info");
    } else {
        inner.health.recovered(WorkerComponent::WorkerInfo);
    }
}

fn stats_json(inner: &WorkerInner) -> Result<Value, Error> {
    let mut value = serde_json::to_value(inner.counters.snapshot())?;
    if let Value::Object(fields) = &mut value {
        fields.insert(
            "uptime_ms".into(),
            Value::from(
                inner
                    .started
                    .get()
                    .map(|started| started.elapsed().as_millis() as u64)
                    .unwrap_or_default(),
            ),
        );
    }
    Ok(value)
}

#[cfg(test)]
mod loop_tests {
    use super::*;

    #[tokio::test]
    async fn wait_for_processors_rejects_clean_exit_when_continuous() {
        let mut processors = JoinSet::new();
        processors.spawn(async {});

        let error = wait_for_processors(&mut processors, false)
            .await
            .unwrap_err();

        assert!(matches!(error, Error::WorkerTask("processor loop")));
    }

    #[tokio::test]
    async fn wait_for_processors_allows_clean_exits_when_burst() {
        let mut processors = JoinSet::new();
        processors.spawn(async {});
        processors.spawn(async {});

        wait_for_processors(&mut processors, true).await.unwrap();
    }

    #[tokio::test]
    async fn wait_for_processors_reports_panics() {
        let mut processors = JoinSet::new();
        processors.spawn(async { panic!("processor panic") });

        let error = wait_for_processors(&mut processors, false)
            .await
            .unwrap_err();

        assert!(matches!(error, Error::Task(error) if error.is_panic()));
    }

    #[tokio::test]
    async fn wait_for_background_exit_reports_loop_name() {
        let mut tasks = JoinSet::new();
        tasks.spawn(async { "test loop" });

        let error = wait_for_background_exit(&mut tasks).await;

        assert!(matches!(error, Error::WorkerTask("test loop")));
    }

    fn at(iso: &str) -> DateTime<Utc> {
        iso.parse().unwrap()
    }

    #[test]
    fn cron_validity_is_capped_by_the_successor_for_dense_schedules() {
        let next = at("2026-01-01T00:00:00Z");
        let every_second = entry("* * * * * *");
        // Every second: the successor bounds validity before the grace does.
        assert_eq!(
            every_second.publication_deadline(next, at("2026-01-01T00:00:01Z")),
            at("2026-01-01T00:00:01Z")
        );
        let every_five_seconds = entry("*/5 * * * * *");
        // Every five seconds: the minimum one-second grace applies, well
        // short of the full period.
        assert_eq!(
            every_five_seconds.publication_deadline(next, at("2026-01-01T00:00:05Z")),
            at("2026-01-01T00:00:01Z")
        );
    }

    #[test]
    fn cron_validity_grace_scales_with_sparse_periods_up_to_a_minute() {
        let next = at("2026-01-01T00:00:00Z");
        let minutely = entry("* * * * *");
        // Every minute: a fifth of the period.
        assert_eq!(
            minutely.publication_deadline(next, at("2026-01-01T00:01:00Z")),
            at("2026-01-01T00:00:12Z")
        );
        // Every five minutes: exactly the one-minute cap.
        let every_five_minutes = entry("*/5 * * * *");
        assert_eq!(
            every_five_minutes.publication_deadline(next, at("2026-01-01T00:05:00Z")),
            at("2026-01-01T00:01:00Z")
        );
        // Daily: still capped at one minute, never the full period.
        let daily = entry("0 0 * * *");
        assert_eq!(
            daily.publication_deadline(next, at("2026-01-02T00:00:00Z")),
            at("2026-01-01T00:01:00Z")
        );
    }

    fn entry(expr: &str) -> JobCronEntry {
        JobCronEntry::new(expr, crate::job::JobRequest::new("tick", Value::Null)).unwrap()
    }

    #[test]
    fn previous_cron_occurrence_finds_boundary_within_lookback() {
        let minutely = entry("* * * * *");
        assert_eq!(
            minutely
                .previous_occurrence(at("2026-01-01T00:05:07Z"))
                .unwrap(),
            at("2026-01-01T00:05:00Z")
        );
        // A boundary exactly at `now` counts: the strictly-after `next`
        // computation would otherwise skip it forever.
        assert_eq!(
            minutely
                .previous_occurrence(at("2026-01-01T00:05:00Z"))
                .unwrap(),
            at("2026-01-01T00:05:00Z")
        );
    }

    #[test]
    fn previous_cron_occurrence_finds_sparse_boundary_without_scanning() {
        let daily = entry("0 0 * * *");
        assert_eq!(
            daily
                .previous_occurrence(at("2026-01-01T12:00:00Z"))
                .unwrap(),
            at("2026-01-01T00:00:00Z")
        );
    }
}
