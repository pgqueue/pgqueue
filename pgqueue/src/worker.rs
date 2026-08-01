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
use crate::dashboard::{
    DashboardRuntime, DashboardServer, DashboardServerConfig, bind_dashboard,
    wait_for_dashboard_exit,
};
use crate::database::{
    Database, DatabaseAbortClaim, DatabaseCronAuthority, DatabaseCronScheduleResult, LeaseIntake,
};
use crate::job::{
    CronDefinition, CronOptions, JobBuilder, JobContext, JobCronEntry, JobDefinition, JobError,
    JobErrorKind, JobRow, JobStateMap, JobStatus, JobType, MAX_JSON_DEPTH, TypeErasedJobHandler,
    json_contains_nul, json_exceeds_depth, validate_duration, validate_nonzero_duration,
};
use crate::queue::{Queue, QueueCounters};
use crate::sweeper::SweepOperations;

const WORKER_INFO_TTL_MULTIPLIER: u32 = 3;
const HARD_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const SHUTDOWN_STEP_TIMEOUT: Duration = Duration::from_secs(1);
const FINALIZE_RETRY_INTERVAL: Duration = Duration::from_secs(1);
/// How long an aborted attempt is given to unwind before the worker finalizes
/// it anyway. A cooperative handler stops at its next `.await`, so this only
/// bounds handlers that block their runtime thread.
const ATTEMPT_ABORT_JOIN_GRACE: Duration = Duration::from_secs(1);
const DEFAULT_ABORT_GRACE: Duration = Duration::from_secs(1);
const MAX_SWEEP_DRAIN_TIME: Duration = Duration::from_secs(1);
const MAX_SWEEP_DRAIN_PASSES: usize = 16;
const DEQUEUE_RETRY_INITIAL_MAX_MS: u64 = 3;
const DEQUEUE_RETRY_MAX_MS: u64 = 100;

fn worker_info_ttl(timer: Duration) -> Duration {
    timer.saturating_mul(WORKER_INFO_TTL_MULTIPLIER)
}

/// A live worker row whose heartbeat has not expired.
#[derive(Debug, Clone, serde::Serialize, sqlx::FromRow)]
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
    /// Active component failures, in [`WorkerComponent`] declaration order —
    /// which groups them by subsystem, not alphabetically.
    pub failures: Vec<WorkerHealthFailure>,
}

/// Cloneable observer for a worker's local health state.
#[derive(Clone)]
pub struct WorkerHealth {
    receiver: watch::Receiver<WorkerHealthSnapshot>,
    closed: bool,
}

impl WorkerHealth {
    /// Returns the latest health snapshot without waiting.
    pub fn snapshot(&self) -> WorkerHealthSnapshot {
        self.receiver.borrow().clone()
    }

    /// Waits for a health change and returns the new snapshot.
    ///
    /// Stop waiting after observing [`WorkerHealthStatus::Stopped`]; sender
    /// closure returns the final snapshot once, and later calls remain pending.
    pub async fn changed(&mut self) -> WorkerHealthSnapshot {
        if self.closed {
            std::future::pending::<()>().await;
        }
        if self.receiver.changed().await.is_err() {
            self.closed = true;
        }
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
            closed: false,
        }
    }

    fn ready(&self) {
        self.running.store(true, Ordering::Release);
        self.publish();
    }

    fn failed(&self, component: WorkerComponent, error: &impl std::fmt::Display) {
        let mut failures = self.lock_failures();
        let message = error.to_string();
        failures
            .entry(component)
            .and_modify(|failure| failure.message.clone_from(&message))
            .or_insert_with(|| WorkerHealthFailure {
                component,
                message,
                since: Utc::now(),
            });
        self.publish_locked(&failures);
    }

    fn recovered(&self, component: WorkerComponent) {
        // Called after every successful dequeue, so the overwhelmingly common
        // case is "nothing was failing". Republishing an identical snapshot
        // would take the watch lock per dequeue to discover nothing changed.
        let mut failures = self.lock_failures();
        if failures.remove(&component).is_some() {
            self.publish_locked(&failures);
        }
    }

    fn stopped(&self) {
        self.stopped.store(true, Ordering::Release);
        self.publish();
    }

    fn publish(&self) {
        self.publish_locked(&self.lock_failures());
    }

    /// Acquires the failures map, recovering it if a panic poisoned the lock.
    /// Everything done under this lock is a plain map operation, so a panic
    /// cannot leave the map in a broken state and the poisoned data is still
    /// valid. Any other fallback points the wrong way: substituting an empty
    /// map would publish a `Ready` snapshot from the failure path itself, and
    /// skipping would freeze health at a possibly stale `Ready` and let
    /// `stopped` go unpublished.
    fn lock_failures(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<WorkerComponent, WorkerHealthFailure>> {
        match self.failures.lock() {
            Ok(failures) => failures,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Publishes the snapshot implied by `failures`, which the caller holds the
    /// lock on.
    ///
    /// Mutating the map and sending have to happen under one acquisition. Two
    /// components reporting concurrently could otherwise mutate in one order
    /// and send in the other, leaving the watch permanently contradicting the
    /// map — and nothing repairs that, because `recovered` short-circuits once
    /// the component is gone from the map, so a phantom `Degraded` would
    /// outlive every later recovery. No `await` happens under the lock.
    fn publish_locked(&self, failures: &HashMap<WorkerComponent, WorkerHealthFailure>) {
        let mut failures = failures.values().cloned().collect::<Vec<_>>();
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
        let next = WorkerHealthSnapshot { status, failures };
        self.sender.send_if_modified(|current| {
            if *current == next {
                return false;
            }
            *current = next;
            true
        });
    }
}

#[cfg(test)]
mod worker_health_reporter_tests {
    use std::sync::{Arc, Barrier};

    use super::*;

    /// Two components reporting concurrently must never leave the published
    /// snapshot disagreeing with the failure map. Nothing repairs such a
    /// disagreement: `recovered` short-circuits once the component is gone from
    /// the map, so a phantom `Degraded` would outlive every later recovery and
    /// keep a healthy worker out of a load balancer for the rest of its run.
    #[test]
    fn test_worker_health_snapshot_agrees_with_the_failure_map_under_concurrent_reports() {
        const ROUNDS: usize = 100_000;
        let reporter = Arc::new(WorkerHealthReporter::new());
        reporter.ready();
        // Three parties: both reporting threads and the observer between
        // rounds, so every round is checked while nothing else is running.
        let round_start = Arc::new(Barrier::new(3));
        let round_end = Arc::new(Barrier::new(3));
        let threads = [WorkerComponent::Dequeue, WorkerComponent::Sweeper].map(|component| {
            let reporter = Arc::clone(&reporter);
            let round_start = Arc::clone(&round_start);
            let round_end = Arc::clone(&round_end);
            std::thread::spawn(move || {
                for _ in 0..ROUNDS {
                    round_start.wait();
                    reporter.failed(component, &"transient");
                    reporter.recovered(component);
                    round_end.wait();
                }
            })
        });
        for round in 0..ROUNDS {
            round_start.wait();
            round_end.wait();
            let failures = reporter.failures.lock().unwrap();
            assert!(failures.is_empty(), "both components recovered");
            drop(failures);
            assert_eq!(
                reporter.sender.borrow().clone(),
                WorkerHealthSnapshot {
                    status: WorkerHealthStatus::Ready,
                    failures: Vec::new(),
                },
                "round {round} published a snapshot the failure map does not hold"
            );
        }
        for thread in threads {
            thread.join().unwrap();
        }
    }

    /// A panic under the failures lock must not invert health: reporting a
    /// failure while the lock is poisoned has to publish `Degraded`, never an
    /// empty-failures (`Ready`) snapshot on the failure path itself, and a
    /// later publish must not drop the recorded failures either.
    #[test]
    fn test_worker_health_reports_failures_after_lock_poisoning() {
        let reporter = Arc::new(WorkerHealthReporter::new());
        reporter.ready();
        reporter.failed(WorkerComponent::Sweeper, &"sweep failed");
        assert_eq!(
            reporter.sender.borrow().status,
            WorkerHealthStatus::Degraded
        );
        let poisoner = Arc::clone(&reporter);
        std::thread::spawn(move || {
            let _failures = poisoner.failures.lock().unwrap();
            panic!("poison the failures lock");
        })
        .join()
        .unwrap_err();
        assert!(reporter.failures.is_poisoned());
        reporter.failed(WorkerComponent::Dequeue, &"dequeue failed");
        let snapshot = reporter.sender.borrow().clone();
        assert_eq!(snapshot.status, WorkerHealthStatus::Degraded);
        assert_eq!(
            snapshot
                .failures
                .iter()
                .map(|failure| failure.component)
                .collect::<Vec<_>>(),
            [WorkerComponent::Dequeue, WorkerComponent::Sweeper]
        );
        reporter.stopped();
        let snapshot = reporter.sender.borrow().clone();
        assert_eq!(snapshot.status, WorkerHealthStatus::Stopped);
        assert_eq!(snapshot.failures.len(), 2);
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
    /// Default 60s. When a sweep fills its configured batch, the worker drains
    /// more batches — up to 16 passes, and for at most one second (or this
    /// interval, if shorter) — repeating only the operations that filled their
    /// batch, so a large backlog cannot monopolize the pool the worker dequeues
    /// and finalizes with.
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
    if require_nonzero {
        validate_nonzero_duration(name, duration)?;
    } else {
        validate_duration(name, duration)?;
    }
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
    abort_grace: Duration,
    shutdown_grace: Duration,
    metadata: Option<Value>,
    dashboard: Option<DashboardServer>,
    error: Option<Error>,
}

impl WorkerBuilder {
    /// Adds a generated handler to the registry unless its exact Rust type is
    /// already present. A shared database name on two distinct types remains a
    /// configuration error: rows are dispatched by that name, so silently
    /// choosing either handler would decode some payloads with the wrong type.
    fn ensure_handler<J: JobType>(&mut self) {
        let handler = J::erased();
        let name = handler.name();
        match self.handlers.get(name) {
            Some(existing) if existing.type_id() == handler.type_id() => {}
            Some(_) if self.error.is_none() => {
                self.error = Some(Error::Config(format!(
                    "job name {name:?} is used by multiple job types"
                )));
            }
            Some(_) => {}
            None => {
                self.handlers.insert(name, handler);
            }
        }
    }

    /// Registers a handler defined with `#[pgqueue::job]`.
    pub fn register_job<J: JobDefinition>(mut self, _job: J) -> Self {
        self.ensure_handler::<J>();
        self
    }

    /// Registers a handler and its compile-time `#[pgqueue::cron]` schedule.
    pub fn register_cron<J: CronDefinition>(mut self, _cron: J) -> Self {
        self.ensure_handler::<J>();
        // Cron payloads are always `()` (the #[pgqueue::cron] contract), which
        // serializes to null.
        let mut template = crate::job::JobRequest::new(J::NAME, Value::Null);
        template.config = J::config();
        self.crons.push((
            J::SCHEDULE.to_string(),
            template,
            CronOptions {
                revision: J::CRON_REVISION,
                ..CronOptions::default()
            },
        ));
        self
    }

    /// Schedules a job on a cron expression decided at runtime (5-field, or
    /// 6 with seconds), evaluated in UTC:
    /// `.schedule_cron(&expr_from_config, cleanup::job(()))`.
    ///
    /// The handler is registered by this call. When the schedule is known at
    /// compile time, prefer `#[pgqueue::cron("...")]` and
    /// [`WorkerBuilder::register_cron`]. This shorthand uses revision 0 and the
    /// default skip policy; use [`WorkerBuilder::schedule_cron_with_options`]
    /// before changing a persisted definition.
    ///
    /// Cron jobs are deduplicated on
    /// `cron:{job name}` (or the builder's explicit `dedupe_key`), so each
    /// occurrence publishes at most one live job row across current workers.
    /// Job execution remains at least once.
    ///
    /// The cron expression owns every occurrence's run time, so a builder
    /// carrying [`JobBuilder::delay`] or [`JobBuilder::at`] makes `build()`
    /// fail instead of silently ignoring the override.
    pub fn schedule_cron<J: JobDefinition>(self, expr: &str, job: JobBuilder<J>) -> Self {
        self.schedule_cron_with_options(expr, job, CronOptions::default())
    }

    /// Schedules a config-driven cron job with an explicit durable revision
    /// and misfire policy. Increase the revision whenever the expression or
    /// job template changes. A template-only revision preserves the durable
    /// cursor; changing the expression starts at its next UTC occurrence.
    ///
    /// Reusing a revision for a different definition is a deploy mistake, but
    /// it never stops the worker: the durable definition wins, this cron is
    /// disabled on this worker, and [`Worker::health`] reports
    /// [`WorkerComponent::Scheduler`] as failed while ordinary jobs keep
    /// flowing. Watch health (or the dashboard) to catch it.
    pub fn schedule_cron_with_options<J: JobDefinition>(
        mut self,
        expr: &str,
        job: JobBuilder<J>,
        options: CronOptions,
    ) -> Self {
        self.ensure_handler::<J>();
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

    /// How long a handler may react to a user abort before its task is forcibly
    /// stopped. Default 1s. Sweeper cancellations, attempt timeouts, and a job
    /// row deleted under a running attempt remain immediate: the row that
    /// granted the attempt its dedupe exclusivity is already gone or has been
    /// handed to another attempt.
    pub fn abort_grace(mut self, grace: Duration) -> Self {
        self.abort_grace = grace;
        self
    }

    /// How long in-flight handlers may finish, and their outcomes be recorded,
    /// after shutdown cancels their cooperative token. Transient failures
    /// writing an outcome are retried inside this window too, so a handler that
    /// succeeded during the drain is not lost to one database blip. When the
    /// grace period expires, the tasks are forcibly stopped and their attempts
    /// are requeued. Default 30s.
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
    /// fast. Use the intentionally unauthenticated `/health` endpoint rather
    /// than a TCP-only readiness check.
    /// Multiple workers in one network namespace must use distinct dashboard
    /// addresses or enable the dashboard on only one worker.
    ///
    /// ```no_run
    /// # #[pgqueue::job]
    /// # async fn cleanup(_: ()) {}
    /// # async fn run(queue: pgqueue::Queue) -> anyhow::Result<()> {
    /// let dashboard = pgqueue::Dashboard::new([queue.clone()])
    ///     .basic_auth("admin", "secret")
    ///     .serve_on("localhost", 8080);
    /// pgqueue::Worker::builder(queue)
    ///     .register_job(cleanup)
    ///     .dashboard(dashboard)
    ///     .run()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn dashboard(mut self, server: DashboardServer) -> Self {
        self.dashboard = Some(server);
        self
    }

    /// Validates, builds, and runs the worker until `SIGINT` or `SIGTERM` (or
    /// until the queue drains in burst mode).
    ///
    /// Use [`WorkerBuilder::build`] when the worker's id, queue, or health
    /// observer is needed before it starts.
    ///
    /// ```no_run
    /// # #[pgqueue::job]
    /// # async fn cleanup(_: ()) {}
    /// # async fn run(queue: pgqueue::Queue) -> Result<(), pgqueue::Error> {
    /// pgqueue::Worker::builder(queue)
    ///     .register_job(cleanup)
    ///     .run()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn run(self) -> Result<(), Error> {
        self.build()?.run().await
    }

    /// Validates, builds, and runs the worker until `shutdown` is cancelled
    /// (or until the queue drains in burst mode).
    ///
    /// Use [`WorkerBuilder::build`] when the worker's id, queue, or health
    /// observer is needed before it starts.
    pub async fn run_until(self, shutdown: CancellationToken) -> Result<(), Error> {
        self.build()?.run_until(shutdown).await
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
        // `pgqueue.workers.metadata` is `jsonb`, which cannot hold `\0`. The
        // lease write reports its failure through health and a log rather than
        // to a caller, so metadata carrying one leaves a worker that starts,
        // holds no lease, and — because dequeueing requires a live accepting
        // lease — processes nothing for as long as it runs. `JobRequest` refuses
        // the same byte on the enqueue side.
        if self
            .metadata
            .as_ref()
            .is_some_and(|metadata| json_exceeds_depth(metadata, MAX_JSON_DEPTH))
        {
            return Err(Error::Config(format!(
                "worker metadata must not nest deeper than {MAX_JSON_DEPTH} levels"
            )));
        }
        if self.metadata.as_ref().is_some_and(json_contains_nul) {
            return Err(Error::Config("worker metadata must not contain NUL".into()));
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
        validate_runtime_duration("abort grace", self.abort_grace, false)?;
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
            if !cron_keys.insert(entry.dedupe_key.clone()) {
                return Err(Error::Config(format!(
                    "cron dedupe key {:?} registered more than once",
                    entry.dedupe_key
                )));
            }
            crons.push(entry);
        }

        let health = WorkerHealthReporter::new();

        let dashboard = self
            .dashboard
            .map(|dashboard| dashboard.into_server_config(Some(health.subscribe())))
            .transpose()?;

        let database = self.queue.database_handle();
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
                abort_grace: self.abort_grace,
                shutdown_grace: self.shutdown_grace,
                metadata: self.metadata,
                dashboard,
                id: Uuid::now_v7(),
                started: OnceLock::new(),
                counters: QueueCounters::default(),
                inflight: Mutex::new(HashMap::new()),
                burst_budget: self.max_burst_jobs.map(AtomicUsize::new),
                intake_open: AtomicBool::new(true),
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
    abort_grace: Duration,
    shutdown_grace: Duration,
    metadata: Option<Value>,
    dashboard: Option<DashboardServerConfig>,
    id: Uuid,
    started: OnceLock<std::time::Instant>,
    counters: QueueCounters,
    /// In-flight attempts, keyed by job id *and* attempt number: recovery can
    /// take a row from a live attempt and this worker can then re-claim it as
    /// the next attempt, so the same id is briefly in flight twice. Keying by
    /// id alone let the newcomer overwrite its predecessor's entry, and the
    /// displaced attempt was never asked about again.
    inflight: Mutex<HashMap<(Uuid, i32), WorkerInflightJob>>,
    /// Remaining burst-mode job budget (only meaningful with max_burst_jobs).
    burst_budget: Option<AtomicUsize>,
    /// Whether this worker still takes new work. Every lease write reads it, so
    /// a lease *created* by a heartbeat — the worker's first, or a replacement
    /// for one the sweeper purged while the worker was stalled — starts in the
    /// state the worker is actually in rather than defaulting to accepting.
    intake_open: AtomicBool,
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
            abort_grace: DEFAULT_ABORT_GRACE,
            shutdown_grace: Duration::from_secs(30),
            metadata: None,
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
        let bound_dashboard = match before_shutdown(
            &shutdown,
            &dropped,
            bind_dashboard(inner.dashboard.as_ref()),
        )
        .await
        {
            Some(bound) => bound?,
            None => return Ok(()),
        };
        inner.started.get_or_init(std::time::Instant::now);

        tracing::info!(
            worker.id = %inner.id, queue = %inner.queue.name(),
            concurrency = inner.concurrency, burst = inner.burst, "worker starting"
        );
        let mut cron_state = CronSchedulingState::default();
        if !inner.crons.is_empty() {
            match before_shutdown(&shutdown, &dropped, reconcile_crons(&inner)).await {
                Some(reconciled) => cron_state = reconciled,
                None => return Ok(()),
            }
        }
        let mut cron_holder_warned = HashSet::new();
        if inner.burst
            && !inner.crons.is_empty()
            && before_shutdown(
                &shutdown,
                &dropped,
                schedule_burst_crons(&inner, &mut cron_holder_warned, &mut cron_state),
            )
            .await
            .is_none()
        {
            return Ok(());
        }
        if before_shutdown(
            &shutdown,
            &dropped,
            write_worker_info(&inner, worker_info_ttl(inner.timers.worker_info)),
        )
        .await
        .is_none()
        {
            // The one startup step where "cancelled" does not mean "did not
            // happen": the future is client-side but the INSERT is server-side,
            // so losing the race mid-statement still leaves a committed, live,
            // accepting lease behind. Retire it rather than advertising a
            // worker that is already gone for a full TTL.
            retire_startup_lease(&inner).await;
            return Ok(());
        }

        // The lease is durable from here on. `WorkerShutdown` retires it during
        // an ordinary shutdown, but it does not exist yet, so every early
        // return below has to retire it or this worker keeps advertising itself
        // as live and accepting until the lease TTL expires.
        let listener = inner.database.notify_listener();
        let wakeup = listener.subscribe_wakeup();
        let notification_health = listener.subscribe_health();
        let stop_intake = CancellationToken::new();
        let cooperative_shutdown = CancellationToken::new();
        let force_shutdown = CancellationToken::new();
        let intake = Arc::new(WorkerIntake::new());
        let (fetcher_exit_tx, mut fetcher_exit) = tokio::sync::oneshot::channel();
        if shutdown.is_cancelled() || dropped.is_cancelled() {
            retire_startup_lease(&inner).await;
            return Ok(());
        }
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
                cooperative_shutdown.clone(),
                force_shutdown.clone(),
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
        if !inner.burst && !inner.crons.is_empty() {
            let timer_inner = inner.clone();
            let schedule_token = timer_token.clone();
            timer_tasks.spawn(async move {
                schedule_loop(timer_inner, schedule_token, cron_holder_warned, cron_state).await;
                "schedule loop"
            });
        }
        inner.health.ready();

        let mut dashboard = bound_dashboard.map(DashboardRuntime::start);

        // Wait for a shutdown request, (burst) for every processor to drain,
        // or for a configured dashboard server to fail.
        let mut fetcher_stopped = false;
        // Scoped so the dashboard borrow ends before shutdown stops the server.
        let mut run_error = {
            let dashboard_exit = wait_for_dashboard_exit(&mut dashboard);
            tokio::pin!(dashboard_exit);

            tokio::select! {
            _ = wait_for_shutdown_or_drop(&shutdown, &dropped) => {
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

        if fetcher_stopped {
            // `fetcher` is `Some` from its spawn above and this is the only
            // `take` before `WorkerShutdown` is built, so the handle is always
            // here to join.
            let error = match fetcher.take() {
                Some(fetcher) => unexpected_task_exit("fetch loop", fetcher.await),
                None => unreachable!("the fetch loop handle is taken exactly once"),
            };
            tracing::error!(worker.id = %inner.id, %error, "worker infrastructure failed");
            run_error = Some(error);
        } else if let Some(error) = run_error.as_ref() {
            tracing::error!(worker.id = %inner.id, %error, "worker infrastructure failed");
        }

        WorkerShutdown {
            intake,
            stop_intake,
            cooperative_shutdown,
            force_shutdown,
            timer_token,
            fetcher,
            processors,
            timer_tasks,
        }
        .run(&inner, &mut run_error)
        .await;

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

/// The tasks and cancellation tokens the shutdown sequence owns.
///
/// Kept apart from `run_until_inner` so the five shutdown phases — close
/// intake, drain processors, stop timers, retire the fetcher, and report —
/// can be read and changed without reasoning about startup's early returns.
struct WorkerShutdown {
    intake: Arc<WorkerIntake>,
    stop_intake: CancellationToken,
    cooperative_shutdown: CancellationToken,
    force_shutdown: CancellationToken,
    timer_token: CancellationToken,
    fetcher: Option<JoinHandle<()>>,
    processors: JoinSet<()>,
    timer_tasks: JoinSet<&'static str>,
}

impl WorkerShutdown {
    async fn run(mut self, inner: &Arc<WorkerInner>, run_error: &mut Option<Error>) {
        // Graceful shutdown: stop taking work, signal cooperative cancellation,
        // then force-stop any attempts that outlive the grace period.
        let grace_deadline = tokio::time::Instant::now() + inner.shutdown_grace;
        self.close_intake(inner, grace_deadline).await;

        // A fetcher may be between a committed dequeue and returning its rows
        // to Rust, so keep it alive while processors still own attempts. Its
        // caretaker heartbeats the lease while it drains committed rows. Once
        // processors are done, the outer timeout gives that drain the hard
        // shutdown bound before aborting it and letting the lease expire.
        let release_fetcher_lease = CancellationToken::new();
        let fetcher_abort = self.fetcher.as_ref().map(JoinHandle::abort_handle);
        let fetcher_caretaker = tokio::spawn(finish_fetcher_shutdown(
            inner.clone(),
            self.fetcher.take(),
            release_fetcher_lease.clone(),
        ));

        self.drain_processors(inner, grace_deadline, run_error)
            .await;
        self.stop_timers(inner, run_error).await;

        // No processor or timer can mutate a job after this point. The
        // caretaker expires the lease once its fetch/drain side is also done.
        release_fetcher_lease.cancel();
        retire_fetcher(inner, fetcher_caretaker, fetcher_abort, run_error).await;
    }

    /// Phase one: refuse new work locally and durably.
    async fn close_intake(&self, inner: &Arc<WorkerInner>, grace_deadline: tokio::time::Instant) {
        // Before the durable close, so a heartbeat that recreates a purged
        // lease during the drain recreates it closed rather than advertising
        // this worker as taking work again.
        inner.intake_open.store(false, Ordering::Release);
        self.intake.begin_shutdown();
        self.stop_intake.cancel();
        self.cooperative_shutdown.cancel();
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
    }

    /// Phase two: let attempts finish, then force-stop and finally abort them.
    async fn drain_processors(
        &mut self,
        inner: &Arc<WorkerInner>,
        grace_deadline: tokio::time::Instant,
        run_error: &mut Option<Error>,
    ) {
        if tokio::time::timeout_at(
            grace_deadline,
            join_all(&mut self.processors, run_error, false),
        )
        .await
        .is_ok()
        {
            return;
        }
        tracing::warn!(worker.id = %inner.id, "grace period expired; force-stopping in-flight jobs");
        self.force_shutdown.cancel();
        if tokio::time::timeout(
            HARD_SHUTDOWN_TIMEOUT,
            join_all(&mut self.processors, run_error, false),
        )
        .await
        .is_err()
        {
            self.processors.abort_all();
            join_all(&mut self.processors, run_error, true).await;
            if run_error.is_none() {
                *run_error = Some(Error::WorkerTask("processor shutdown timed out"));
            }
        }
    }

    /// Phase three: stop the abort, sweep, schedule, and heartbeat loops.
    async fn stop_timers(&mut self, inner: &Arc<WorkerInner>, run_error: &mut Option<Error>) {
        self.timer_token.cancel();
        if tokio::time::timeout(
            SHUTDOWN_STEP_TIMEOUT,
            join_all(&mut self.timer_tasks, run_error, false),
        )
        .await
        .is_err()
        {
            tracing::warn!(worker.id = %inner.id, "timer task shutdown timed out");
            self.timer_tasks.abort_all();
            join_all(&mut self.timer_tasks, run_error, true).await;
            if run_error.is_none() {
                *run_error = Some(Error::WorkerTask("timer shutdown timed out"));
            }
        }
    }
}

/// Phase four: wait for the fetcher caretaker to drain and release the lease,
/// making sure nothing is left detached that could still touch a job row.
async fn retire_fetcher(
    inner: &Arc<WorkerInner>,
    mut caretaker: JoinHandle<Result<(), Error>>,
    fetcher_abort: Option<tokio::task::AbortHandle>,
    run_error: &mut Option<Error>,
) {
    match tokio::time::timeout(HARD_SHUTDOWN_TIMEOUT, &mut caretaker).await {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(error))) => {
            tracing::error!(worker.id = %inner.id, %error, "fetcher shutdown failed");
            *run_error = run_error.take().or(Some(error));
        }
        Ok(Err(error)) => {
            tracing::error!(worker.id = %inner.id, %error, "fetcher caretaker failed");
            *run_error = run_error.take().or(Some(Error::Task(error)));
        }
        Err(_) => {
            // Do not leave a detached fetcher or caretaker capable of
            // mutating jobs or refreshing the lease after return.
            if let Some(fetcher_abort) = fetcher_abort {
                fetcher_abort.abort();
            }
            caretaker.abort();
            let _ = caretaker.await;
            tracing::warn!(
                worker.id = %inner.id,
                "fetcher cleanup timed out; its worker lease will expire"
            );
            if run_error.is_none() {
                *run_error = Some(Error::WorkerTask("fetcher shutdown timed out"));
            }
        }
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

/// Resolves when a component is asked to stop, either by its caller's token or
/// by its owning handle being dropped.
pub(crate) async fn wait_for_shutdown_or_drop(
    shutdown: &CancellationToken,
    dropped: &CancellationToken,
) {
    tokio::select! {
        _ = shutdown.cancelled() => {}
        _ = dropped.cancelled() => {}
    }
}

/// Runs one startup step unless the worker is asked to stop first. `None` means
/// the step did not run to completion and the caller must unwind.
///
/// Startup is a sequence of these, so spelling the race out at each step would
/// repeat it once per step — and dropping either branch by mistake would leave
/// startup unresponsive to `run_until` cancellation or to a dropped handle,
/// which no compiler check catches.
async fn before_shutdown<T>(
    shutdown: &CancellationToken,
    dropped: &CancellationToken,
    step: impl Future<Output = T>,
) -> Option<T> {
    tokio::select! {
        biased;
        _ = wait_for_shutdown_or_drop(shutdown, dropped) => None,
        value = step => Some(value),
    }
}

/// Undoes the startup heartbeat when the worker stops before its fetcher — and
/// with it [`WorkerShutdown`] — exists. Without this, `Queue::info`, the
/// dashboard worker page, and `has_live_workers` all report a live worker that
/// is already gone, and the dequeue path's `accepting` check still lets it
/// claim jobs, until its lease expires.
///
/// Both steps are bounded like every other shutdown database call: `run_until`
/// awaits this, so a wedged backend would otherwise hang it forever.
async fn retire_startup_lease(inner: &Arc<WorkerInner>) {
    inner.intake_open.store(false, Ordering::Release);
    match tokio::time::timeout(
        SHUTDOWN_STEP_TIMEOUT,
        inner.database.stop_worker_intake(inner.id),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            tracing::error!(
                worker.id = %inner.id, %error,
                "failed to close worker intake while stopping during startup"
            );
        }
        Err(_) => {
            tracing::warn!(
                worker.id = %inner.id,
                "worker intake close timed out while stopping during startup"
            );
        }
    }
    if tokio::time::timeout(
        SHUTDOWN_STEP_TIMEOUT,
        write_worker_info(inner, Duration::ZERO),
    )
    .await
    .is_err()
    {
        tracing::warn!(
            worker.id = %inner.id,
            "worker lease expiry timed out while stopping during startup"
        );
    }
}

pub(crate) async fn wait_for_shutdown_signal() {
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
    cooperative: CancellationToken,
    force: CancellationToken,
    finished: CancellationToken,
    abort_reason: Arc<OnceLock<WorkerAbortReason>>,
}

#[derive(Clone)]
enum WorkerAbortReason {
    User(String),
    Swept,
    Missing,
    /// The row is no longer this attempt's: recovery requeued it, or it is
    /// running again one attempt further on.
    Superseded,
}

impl WorkerInflightJob {
    /// Asks an attempt to stop. A user abort gets `grace` to clean up
    /// cooperatively; sweeper recovery, a re-claimed row, and a deleted row do
    /// not, because the row that granted the attempt its dedupe exclusivity is
    /// already gone or has been handed to another attempt.
    ///
    /// The reason is recorded once, so the first one to arrive is the one the
    /// attempt is finished under. An immediate reason still has to force-stop
    /// the handler even when it lost that race, though: a user abort already
    /// under way leaves the attempt running for the whole `grace`, and if the
    /// row is deleted or handed to another attempt in that window — which
    /// `Database::abort_stuck_abandoned_batch` does to an `aborting` row whose
    /// `result_ttl_ms` is `0` — nothing in the database guards its writes any
    /// more. Returning early there was the difference between the immediacy
    /// this and [`WorkerBuilder::abort_grace`] document and a handler that kept
    /// producing side effects to the end of the grace.
    fn request_abort(&self, reason: WorkerAbortReason, grace: Duration) {
        let immediate = matches!(
            reason,
            WorkerAbortReason::Swept | WorkerAbortReason::Missing | WorkerAbortReason::Superseded
        );
        if self.abort_reason.set(reason).is_err() && !immediate {
            return;
        }
        self.cooperative.cancel();
        if immediate || grace.is_zero() {
            self.force.cancel();
            return;
        }

        let force = self.force.clone();
        let finished = self.finished.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = finished.cancelled() => {}
                _ = tokio::time::sleep(grace) => force.cancel(),
            }
        });
    }
}

/// Acquires the in-flight map, recovering it if a panic poisoned the lock.
/// Everything done under this lock is a plain map operation — insert, remove,
/// key collection, and get-and-clone — so a panic cannot leave the map in a
/// broken state and the poisoned data is still valid. Skipping instead would
/// silently degrade aborts with no health signal: an attempt that fails to
/// register can never be aborted, a finished attempt that fails to deregister
/// leaves a stale claim the abort poll asks the database about forever, and a
/// poll that reads an empty snapshot skips the database entirely while
/// reporting the abort component healthy.
fn lock_inflight(
    inflight: &Mutex<HashMap<(Uuid, i32), WorkerInflightJob>>,
) -> std::sync::MutexGuard<'_, HashMap<(Uuid, i32), WorkerInflightJob>> {
    match inflight.lock() {
        Ok(map) => map,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// Removes the in-flight entry even if processing unwinds.
struct WorkerInflightJobGuard<'a> {
    inflight: &'a Mutex<HashMap<(Uuid, i32), WorkerInflightJob>>,
    /// The `(id, attempts)` key this attempt registered under. A dequeue only
    /// ever hands out `attempts + 1`, so the key names this attempt alone and
    /// removing it can never take a later attempt's entry with it.
    key: (Uuid, i32),
    finished: CancellationToken,
}

impl Drop for WorkerInflightJobGuard<'_> {
    fn drop(&mut self) {
        self.finished.cancel();
        lock_inflight(self.inflight).remove(&self.key);
    }
}

#[cfg(test)]
mod worker_inflight_tests {
    use super::*;

    fn entry() -> WorkerInflightJob {
        WorkerInflightJob {
            cooperative: CancellationToken::new(),
            force: CancellationToken::new(),
            finished: CancellationToken::new(),
            abort_reason: Arc::new(OnceLock::new()),
        }
    }

    /// A panic under the inflight lock must not degrade abort handling: a new
    /// attempt must still register (an unregistered attempt can never be
    /// aborted), the abort poll must still see and look up registered
    /// attempts, and a finished attempt's guard must still deregister it (a
    /// stale entry is a claim every poll asks the database about forever) —
    /// all with no health signal that anything was lost.
    #[test]
    fn test_worker_inflight_registry_survives_lock_poisoning() {
        let inflight = Arc::new(Mutex::new(HashMap::new()));
        let poisoner = Arc::clone(&inflight);
        std::thread::spawn(move || {
            let _map = poisoner.lock().unwrap();
            panic!("poison the inflight lock");
        })
        .join()
        .unwrap_err();
        assert!(inflight.is_poisoned());
        // Registration (`process`'s insert) must still land.
        let key = (Uuid::now_v7(), 1);
        lock_inflight(&inflight).insert(key, entry());
        // The abort poll's claims snapshot and per-claim lookup must still
        // see the attempt.
        assert_eq!(
            lock_inflight(&inflight).keys().copied().collect::<Vec<_>>(),
            [key]
        );
        assert!(lock_inflight(&inflight).get(&key).cloned().is_some());
        // The finished attempt's guard must still deregister it.
        drop(WorkerInflightJobGuard {
            inflight: &inflight,
            key,
            finished: CancellationToken::new(),
        });
        assert!(
            lock_inflight(&inflight).is_empty(),
            "a finished attempt must be deregistered even after poisoning"
        );
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

enum WorkerProcessResult {
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
/// per-slot `dequeue(1)` statements, each taking a pooled connection and a
/// round trip of its own to claim one row. The dequeue takes no advisory lock —
/// it is a single statement whose candidates are `FOR UPDATE ... SKIP LOCKED`,
/// so concurrent claims never block each other — which is exactly why the cost
/// being saved here is the per-claim round trip and connection, not lock
/// contention.
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

    /// Acquires the intake buffer, recovering it if a panic poisoned the lock.
    /// Everything done under this lock is a single non-panicking deque or
    /// atomic operation, so a panic cannot leave the buffer in a broken state
    /// and the poisoned data is still valid. Skipping instead would silently
    /// wedge the worker: rows in the buffer are already claimed in the
    /// database, so a `claim` that stops handing them out strands them until
    /// lease-expiry recovery, and a fetcher that stops seeing demand never
    /// dequeues again — all while health keeps reporting `Ready`.
    fn lock_buffer(&self) -> std::sync::MutexGuard<'_, VecDeque<JobRow>> {
        match self.buffer.lock() {
            Ok(buffer) => buffer,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Claims one buffered job and withdraws this processor's demand while the
    /// buffer lock is held, giving the fetcher a coherent `(buffered, idle)`
    /// snapshot.
    fn claim(&self) -> Option<JobRow> {
        let mut buffer = self.lock_buffer();
        if self.stopping.load(Ordering::Acquire) {
            return None;
        }
        let job = buffer.pop_front()?;
        self.idle.fetch_sub(1, Ordering::AcqRel);
        Some(job)
    }

    fn register_demand(&self) -> u64 {
        let _buffer = self.lock_buffer();
        let generation = self.demand_generation.fetch_add(1, Ordering::AcqRel) + 1;
        self.idle.fetch_add(1, Ordering::AcqRel);
        generation
    }

    /// The fetcher's demand snapshot: unmet demand (idle processors minus
    /// already-buffered rows) and the demand generation it was taken at,
    /// coherent under the buffer lock.
    fn demand_snapshot(&self) -> (usize, u64) {
        let buffer = self.lock_buffer();
        (
            self.idle
                .load(Ordering::Acquire)
                .saturating_sub(buffer.len()),
            self.demand_generation.load(Ordering::Acquire),
        )
    }

    fn demand_is_drained(&self, generation: u64) -> bool {
        self.drained_generation.load(Ordering::Acquire) >= generation
    }

    fn withdraw_demand(&self) {
        let _buffer = self.lock_buffer();
        self.idle.fetch_sub(1, Ordering::AcqRel);
    }

    fn begin_shutdown(&self) {
        let _buffer = self.lock_buffer();
        self.stopping.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod worker_intake_tests {
    use crate::job::JobRetryBackoff;

    use super::*;

    fn buffered_job() -> JobRow {
        JobRow {
            id: Uuid::now_v7(),
            dedupe_key: None,
            queue: "default".to_string(),
            name: "buffered".to_string(),
            payload: Value::Null,
            status: JobStatus::Running,
            priority: 0,
            attempts: 1,
            max_attempts: 1,
            timeout_ms: None,
            retry_delay_ms: 0,
            backoff: JobRetryBackoff::None,
            result_ttl_ms: None,
            scheduled_at: Utc::now(),
            enqueued_at: Utc::now(),
            started_at: None,
            touched_at: None,
            completed_at: None,
            expires_at: None,
            result: None,
            error: None,
            meta: Value::Null,
            worker_id: None,
        }
    }

    /// A panic under the intake lock must not wedge the worker. Rows in the
    /// buffer are already claimed in the database, so a `claim` that stops
    /// handing them out strands them until lease-expiry recovery, and a fetcher
    /// that stops seeing demand never dequeues again — all while the worker
    /// keeps running and reports itself healthy.
    #[test]
    fn test_worker_intake_keeps_moving_jobs_after_lock_poisoning() {
        let intake = Arc::new(WorkerIntake::new());
        // One processor goes idle; the fetcher buffers a row for it.
        let generation = intake.register_demand();
        intake.buffer.lock().unwrap().push_back(buffered_job());
        let poisoner = Arc::clone(&intake);
        std::thread::spawn(move || {
            let _buffer = poisoner.buffer.lock().unwrap();
            panic!("poison the intake lock");
        })
        .join()
        .unwrap_err();
        assert!(intake.buffer.is_poisoned());
        // The processor must still receive the buffered, database-claimed row,
        // and taking it must keep the fetcher's demand snapshot coherent.
        let job = intake.claim();
        assert!(
            job.is_some(),
            "a buffered, database-claimed job must remain claimable"
        );
        assert_eq!(intake.demand_snapshot(), (0, generation));
        // The fetcher must keep seeing new demand, not a permanent zero.
        let next_generation = intake.register_demand();
        assert_eq!(next_generation, generation + 1);
        assert_eq!(intake.demand_snapshot(), (1, next_generation));
        // The fetcher must still buffer rows it dequeued, and shutdown must
        // still freeze intake so no buffered row can race from fetcher cleanup
        // into a processor.
        intake.lock_buffer().push_back(buffered_job());
        intake.begin_shutdown();
        assert!(
            intake.claim().is_none(),
            "intake must stop handing out jobs once shutdown began"
        );
        assert_eq!(
            intake.lock_buffer().len(),
            1,
            "the frozen row stays buffered for the shutdown drain to requeue"
        );
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
            let (want, demand_generation) = intake.demand_snapshot();
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
            if let Ok(result) = &dequeue
                && unhandled_probe_completed(probe_unhandled, result.jobs.len(), want)
            {
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
            match dequeue {
                Ok(result)
                    if result.jobs.is_empty() && result.intake_open && !result.work_available =>
                {
                    retry_max_ms = DEQUEUE_RETRY_INITIAL_MAX_MS;
                    intake
                        .drained_generation
                        .fetch_max(demand_generation, Ordering::AcqRel);
                    intake.refilled.notify_waiters();
                    break;
                }
                Ok(result) if result.jobs.is_empty() && result.intake_open => {
                    // `SKIP LOCKED` can produce an empty batch while a matching
                    // ready row is being inspected or updated elsewhere. Keep
                    // burst demand outstanding until a later fetch can make a
                    // definitive drain decision.
                    //
                    // Once the backoff has saturated, hand back to the outer
                    // `select!` rather than retrying at `DEQUEUE_RETRY_MAX_MS`
                    // forever. The claim uses `SKIP LOCKED` and the availability
                    // probe does not, so one `queued`, due, name-matching row
                    // held under a row lock by an unrelated open transaction
                    // reports work that no claim can ever take — and that pinned
                    // every idle worker in the fleet in this loop at ~22x its
                    // configured `poll_interval`, for as long as the lock was
                    // held. `retry_max_ms` is deliberately left saturated so
                    // later passes re-check once per `poll_interval` instead of
                    // climbing the ramp again; it is reset by every arm that
                    // learns something definitive. Demand stays outstanding and
                    // `drained_generation` is untouched, so a burst processor
                    // still cannot conclude a drain from this — and a processor
                    // going idle notifies `demand`, so nothing waits out a poll
                    // interval that had work to hand it.
                    if retry_max_ms >= DEQUEUE_RETRY_MAX_MS {
                        break;
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
                    if !sleep_unless_stopped(&stop, Duration::from_millis(100)).await {
                        drain_on_shutdown(&inner, &intake).await;
                        return;
                    }
                    break;
                }
                Ok(result) => {
                    retry_max_ms = DEQUEUE_RETRY_INITIAL_MAX_MS;
                    let fetched = result.jobs.len();
                    let work_available = result.work_available;
                    intake.lock_buffer().extend(result.jobs);
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
                    if !sleep_unless_stopped(&stop, Duration::from_secs(1)).await {
                        drain_on_shutdown(&inner, &intake).await;
                        return;
                    }
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

fn unhandled_probe_completed(requested: bool, fetched: usize, wanted: usize) -> bool {
    requested && fetched < wanted
}

async fn wait_for_dequeue_retry(stop: &CancellationToken, max_ms: u64) -> bool {
    let delay_ms = 1 + u64::from(rand::random::<u8>()) % max_ms;
    sleep_unless_stopped(stop, Duration::from_millis(delay_ms)).await
}

/// Sleeps for `duration`, returning `false` early if `stop` is cancelled
/// first, so the fetch loop's backoffs never delay shutdown.
async fn sleep_unless_stopped(stop: &CancellationToken, duration: Duration) -> bool {
    tokio::select! {
        _ = stop.cancelled() => false,
        _ = tokio::time::sleep(duration) => true,
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
        // Take the row rather than cloning it: these carry full payloads, and
        // shutdown is the worst moment to allocate a copy per iteration.
        let Some(job) = intake.lock_buffer().pop_front() else {
            return;
        };
        let settled = match inner.database.requeue_shutdown(&job, "cancelled").await {
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
        if !settled {
            intake.lock_buffer().push_front(job);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

async fn processor_loop(
    inner: Arc<WorkerInner>,
    intake: Arc<WorkerIntake>,
    stop: CancellationToken,
    cooperative_shutdown: CancellationToken,
    force_shutdown: CancellationToken,
) {
    loop {
        // Burst cap: reserve budget BEFORE fetching so `concurrency`
        // processors can't all slip past the check together.
        if inner.burst_budget.as_ref().is_some_and(|budget| {
            budget
                .try_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_err()
        }) {
            return;
        }
        match next_job(&inner, &intake, &stop).await {
            WorkerFetch::Job(job) => {
                process(&inner, *job, &cooperative_shutdown, &force_shutdown).await
            }
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
    let refill = intake.refilled.notified();
    tokio::pin!(refill);
    let result = loop {
        // Register with Notify before inspecting the buffer. `notify_waiters`
        // does not retain a permit, so constructing the future inside select
        // after `claim` would leave a lost-wakeup window.
        refill.as_mut().enable();
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
            _ = &mut refill => {
                refill.set(intake.refilled.notified());
            }
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
    if !matches!(result, WorkerFetch::Job(_)) {
        intake.withdraw_demand();
        // A processor that exits without taking a job returns its burst budget.
        if let Some(budget) = &inner.burst_budget {
            budget.fetch_add(1, Ordering::AcqRel);
        }
    }
    result
}

/// Runs one dequeued job through its handler and finalization.
async fn process(
    inner: &Arc<WorkerInner>,
    mut job: JobRow,
    cooperative_shutdown: &CancellationToken,
    force_shutdown: &CancellationToken,
) {
    let cooperative = cooperative_shutdown.child_token();
    let force = force_shutdown.child_token();
    let finished = CancellationToken::new();
    let abort_reason = Arc::new(OnceLock::new());
    let key = (job.id, job.attempts);
    lock_inflight(&inner.inflight).insert(
        key,
        WorkerInflightJob {
            cooperative: cooperative.clone(),
            force: force.clone(),
            finished: finished.clone(),
            abort_reason: abort_reason.clone(),
        },
    );
    let _guard = WorkerInflightJobGuard {
        inflight: &inner.inflight,
        key,
        finished,
    };

    let ctx = JobContext::new(
        inner.queue.clone(),
        job.clone(),
        inner.id,
        inner.state.clone(),
        cooperative,
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
        let end = run_attempt(inner, &job, payload, &ctx, &force).await;
        let result = finalize(
            inner,
            &job,
            end,
            &abort_reason,
            force_shutdown,
            cooperative_shutdown,
        )
        .await;
        match &result {
            WorkerProcessResult::Complete => inner.counters.record_complete(),
            WorkerProcessResult::Retried(_) | WorkerProcessResult::Requeued => {
                inner.counters.record_retry()
            }
            WorkerProcessResult::Failed(_) => inner.counters.record_failed(),
            WorkerProcessResult::Aborted(_) => inner.counters.record_abort(),
            WorkerProcessResult::Unconfirmed => {}
        }
        match &result {
            WorkerProcessResult::Complete => tracing::info!("job complete"),
            WorkerProcessResult::Retried(e) => {
                tracing::warn!(error = %e, "job attempt failed; retrying")
            }
            WorkerProcessResult::Failed(e) => tracing::error!(error = %e, "job failed"),
            WorkerProcessResult::Aborted(e) => tracing::warn!(error = %e, "job aborted"),
            WorkerProcessResult::Requeued => tracing::info!("job requeued for shutdown"),
            WorkerProcessResult::Unconfirmed => {
                tracing::warn!("job result was not confirmed by the database")
            }
        }
    }
    .instrument(span)
    .await;
}

/// Executes the handler in an owned task for panic containment, under the
/// job's timeout and force-stop token.
async fn run_attempt(
    inner: &Arc<WorkerInner>,
    job: &JobRow,
    payload: Value,
    ctx: &JobContext,
    force: &CancellationToken,
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
        _ = force.cancelled() => {
            let _ = join_after_abort(&mut task).await;
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
            // The select is biased, so reaching this arm means the handler was
            // not ready: the attempt really did exceed its limit, and a late
            // success must not overwrite that. A panic seen while unwinding is
            // the one outcome more informative than the timeout.
            match join_after_abort(&mut task).await {
                Some(Err(join_error)) if join_error.is_panic() => WorkerAttemptResult::Errored(
                    JobError::new(JobErrorKind::Panic, panic_message(join_error)),
                ),
                // A handler that was already past its last yield point when the
                // deadline fired runs to completion regardless of `abort`, so
                // its error is in hand. Reporting a synthetic timeout instead
                // would throw away the only actionable diagnosis the attempt
                // produced — the same reason a panic outranks the timeout.
                Some(Ok(Err(job_error))) => handler_errored(job_error),
                _ => WorkerAttemptResult::Errored(JobError::new(
                    JobErrorKind::Timeout,
                    format!("attempt exceeded {:?}", timeout.unwrap_or_default()),
                )),
            }
        }
    }
}

/// Aborts the handler task and waits a bounded time for it to unwind.
///
/// `JoinHandle::abort` only takes effect at the task's next yield point, so a
/// handler that blocks its runtime thread (a synchronous client, `std::fs`, a
/// CPU-bound loop) never completes. Waiting for it without a bound would pin
/// the job row, the processor slot, and — for cron jobs — every future
/// occurrence. Returns `None` when the task did not settle in time.
async fn join_after_abort(
    task: &mut tokio_util::task::AbortOnDropHandle<Result<Value, JobError>>,
) -> Option<Result<Result<Value, JobError>, tokio::task::JoinError>> {
    task.abort();
    match tokio::time::timeout(ATTEMPT_ABORT_JOIN_GRACE, task).await {
        Ok(result) => Some(result),
        Err(_) => {
            tracing::warn!(
                grace = ?ATTEMPT_ABORT_JOIN_GRACE,
                "handler did not yield after abort; finalizing without it. A handler that \
                 blocks its runtime thread cannot be cancelled — use spawn_blocking."
            );
            None
        }
    }
}

/// Rebuilds a handler's own [`JobError`] through [`JobError::new`], the one
/// place that substitutes the NUL PostgreSQL `text` cannot store (`22021`).
///
/// The constructor alone is not enough: `JobError`'s fields are public and
/// `IntoJobResult` is a public trait, so an error can reach this point without
/// ever having been through one — a struct literal, a deserialized error, a
/// user's own `IntoJobResult`. Storing such a message fails identically forever
/// while `finalize` retries once a second, so the attempt keeps its processor
/// slot and its row stays `running` under a healthy lease that nothing can
/// recover. Every handler-supplied error crosses into finalization here or in
/// [`classify_attempt_join`], and both go through this.
fn handler_errored(error: JobError) -> WorkerAttemptResult {
    WorkerAttemptResult::Errored(JobError::new(error.kind, error.message))
}

fn classify_attempt_join(
    result: Result<Result<Value, JobError>, tokio::task::JoinError>,
    cancelled: WorkerAttemptResult,
) -> WorkerAttemptResult {
    match result {
        // PostgreSQL `jsonb` cannot represent `\0` (`22P05`), so storing such a
        // result is not a transient failure that finalization could retry its
        // way out of — it fails identically forever, holding the processor slot
        // and, once the lease lapses, handing the sweeper a job whose next
        // attempt wedges the same way. The attempt is failed here instead, at
        // the one place a handler's value becomes a success, so the failure is
        // reported as what it is: a result that cannot be encoded.
        Ok(Ok(value)) if json_contains_nul(&value) => WorkerAttemptResult::Errored(JobError::new(
            JobErrorKind::Decode,
            "result encode: a job result must not contain NUL",
        )),
        // Same shape, mirror-image cause: `jsonb` stores nesting `serde_json`
        // cannot decode, so `validate_finalization` refuses it and `finalize`
        // would retry that refusal once a second forever. Fail the attempt here
        // instead, for the same reason and at the same place.
        Ok(Ok(value)) if json_exceeds_depth(&value, MAX_JSON_DEPTH) => {
            WorkerAttemptResult::Errored(JobError::new(
                JobErrorKind::Decode,
                format!(
                    "result encode: a job result must not nest deeper than {MAX_JSON_DEPTH} levels"
                ),
            ))
        }
        Ok(Ok(value)) => WorkerAttemptResult::Success(value),
        Ok(Err(job_error)) => handler_errored(job_error),
        Err(join_error) if join_error.is_panic() => WorkerAttemptResult::Errored(JobError::new(
            JobErrorKind::Panic,
            panic_message(join_error),
        )),
        Err(_) => cancelled,
    }
}

/// Applies the attempt's end state to the database. The in-flight guard and
/// worker ownership stay live while transient database errors are retried.
///
/// The retry is bounded by `force_shutdown`, not by the intake stop: closing
/// intake is shutdown's *first* durable act, so binding it to that token gave
/// every attempt finishing during the drain exactly zero retries — one pool
/// timeout and a job that had already succeeded was left `running`, then swept
/// to `aborted` with its result thrown away. The worker lease is deliberately
/// held alive for the whole drain, so nothing can recover the row while a
/// retry is in flight anyway. `drain_processors` caps the total at the
/// shutdown grace plus [`HARD_SHUTDOWN_TIMEOUT`] and aborts past it, so
/// retrying through the grace cannot hang shutdown.
async fn finalize(
    inner: &Arc<WorkerInner>,
    job: &JobRow,
    end: WorkerAttemptResult,
    abort_reason: &OnceLock<WorkerAbortReason>,
    force_shutdown: &CancellationToken,
    cooperative_shutdown: &CancellationToken,
) -> WorkerProcessResult {
    loop {
        match try_finalize(inner, job, &end, abort_reason, cooperative_shutdown).await {
            Ok(result) => return result,
            Err(error) => {
                tracing::error!(%error, "failed to finalize job; retrying");
                tokio::select! {
                    _ = force_shutdown.cancelled() => return WorkerProcessResult::Unconfirmed,
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
    cooperative_shutdown: &CancellationToken,
) -> Result<WorkerProcessResult, Error> {
    let database = &inner.database;
    match end {
        WorkerAttemptResult::Success(value) => {
            finish_with_swept_fallback(
                database,
                job,
                JobStatus::Complete,
                Some(value.clone()),
                None,
                WorkerProcessResult::Complete,
            )
            .await
        }
        WorkerAttemptResult::Errored(error) => {
            // Shutdown cancels every handler's cooperative token at the start
            // of the grace window, and the documented reaction is to return
            // after bounded cleanup. That return is a shutdown, not a failed
            // attempt: spend no attempt on it and let another worker run the
            // job, exactly as a handler force-stopped at grace expiry does.
            // The handler's message is kept so the reason stays visible.
            if cooperative_shutdown.is_cancelled() && abort_reason.get().is_none() {
                let stored_error = error.to_string();
                return match database.requeue_shutdown(job, &stored_error).await {
                    Ok(true) => Ok(WorkerProcessResult::Requeued),
                    Ok(false) => Ok(WorkerProcessResult::Unconfirmed),
                    Err(db_error) => Err(db_error),
                };
            }
            if job.retryable() && error.kind.retryable() {
                let stored_error = error.to_string();
                match database.retry(job, &stored_error).await {
                    Ok(true) => Ok(WorkerProcessResult::Retried(error.clone())),
                    // Retry refused: the row moved to 'aborting' under us (a
                    // pending abort is never resurrected) or was swept.
                    // A sweeper abort is a retry request; a user abort is a
                    // terminal cancellation. The marker-guarded retry makes
                    // that distinction without trusting the reason string.
                    //
                    // The handler's error is carried into that retry exactly as
                    // it is into the one above: the attempt failed for a real,
                    // reportable reason, and the sweeper losing the race to it
                    // must not replace that reason with the `swept` marker for
                    // the whole backoff window and the next attempt.
                    Ok(false) => {
                        retry_swept_or_refuse(database, job, error.clone(), Some(&stored_error))
                            .await
                    }
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
                    WorkerProcessResult::Failed(error.clone()),
                )
                .await
            }
        }
        WorkerAttemptResult::Cancelled => match abort_reason.get() {
            Some(WorkerAbortReason::Swept) if job.retryable() => {
                // No handler error to record: the attempt ended because the
                // sweeper took it away, and the marker already on the row says
                // exactly that.
                let error = JobError::new(JobErrorKind::Timeout, "swept");
                retry_swept_or_refuse(database, job, error, None).await
            }
            Some(abort_reason) => {
                let reason = match abort_reason {
                    WorkerAbortReason::Swept => "swept",
                    WorkerAbortReason::User(reason) => reason.as_str(),
                    WorkerAbortReason::Missing => {
                        "job row was deleted while the attempt was running"
                    }
                    // The row is another attempt's now. Every write path guards
                    // on `(attempts, worker_id)`, so recording anything here
                    // would be refused; report it unconfirmed and leave the row
                    // to its owner.
                    WorkerAbortReason::Superseded => {
                        return Ok(WorkerProcessResult::Unconfirmed);
                    }
                };
                let error = JobError::new(JobErrorKind::Aborted, reason);
                match database
                    .finish(job, JobStatus::Aborted, None, Some(reason))
                    .await
                {
                    Ok(true) => Ok(WorkerProcessResult::Aborted(error)),
                    Ok(false) => Ok(WorkerProcessResult::Unconfirmed),
                    Err(db_error) => Err(db_error),
                }
            }
            // Shutdown: requeue unconditionally. If an abort
            // raced shutdown (row now 'aborting'), retry is refused and the
            // sweeper finishes the abort later.
            None => match database.requeue_shutdown(job, "cancelled").await {
                Ok(true) => Ok(WorkerProcessResult::Requeued),
                Ok(false) => Ok(WorkerProcessResult::Unconfirmed),
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
    process_result: WorkerProcessResult,
) -> Result<WorkerProcessResult, Error> {
    // `Database::finish` already lets a handler complete through a sweeper's
    // grace window while never overwriting a user-requested abort.
    match database.finish(job, status, result, error).await {
        Ok(true) => Ok(process_result),
        Ok(false) => finish_aborted_fallback(database, job).await,
        Err(db_error) => Err(db_error),
    }
}

async fn retry_swept_or_refuse(
    database: &Database,
    job: &JobRow,
    error: JobError,
    stored_error: Option<&str>,
) -> Result<WorkerProcessResult, Error> {
    match database.retry_swept(job, stored_error).await {
        Ok(true) => Ok(WorkerProcessResult::Retried(error)),
        Ok(false) => swept_retry_refusal_result(database, job, error).await,
        Err(db_error) => Err(db_error),
    }
}

async fn finish_aborted_fallback(
    database: &Database,
    job: &JobRow,
) -> Result<WorkerProcessResult, Error> {
    let aborted = JobError::new(JobErrorKind::Aborted, "abort requested during attempt");
    match database.finish(job, JobStatus::Aborted, None, None).await {
        Ok(true) => Ok(WorkerProcessResult::Aborted(aborted)),
        Ok(false) => {
            tracing::debug!("job already finalized elsewhere (likely swept)");
            Ok(WorkerProcessResult::Unconfirmed)
        }
        Err(db_error) => Err(db_error),
    }
}

async fn swept_retry_refusal_result(
    database: &Database,
    job: &JobRow,
    retry_error: JobError,
) -> Result<WorkerProcessResult, Error> {
    match database.job(job.id).await {
        Ok(Some(current))
            if current.attempts > job.attempts
                || matches!(current.status, JobStatus::Queued | JobStatus::Running) =>
        {
            Ok(WorkerProcessResult::Retried(retry_error))
        }
        Ok(Some(current)) if current.status == JobStatus::Aborted => {
            let error = JobError::new(
                JobErrorKind::Aborted,
                current.error.as_deref().unwrap_or("aborted"),
            );
            Ok(WorkerProcessResult::Aborted(error))
        }
        Ok(Some(_)) => finish_aborted_fallback(database, job).await,
        Ok(None) => Ok(WorkerProcessResult::Unconfirmed),
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

/// Cancels in-flight attempts whose rows moved to `aborting`/`aborted`, were
/// taken away by recovery — requeued, or re-claimed as a later attempt — or
/// disappeared.
async fn abort_loop(inner: Arc<WorkerInner>, token: CancellationToken) {
    let mut interval = tokio::time::interval(inner.timers.abort);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = token.cancelled() => return,
            _ = interval.tick() => {}
        }
        let claims: Vec<DatabaseAbortClaim> = lock_inflight(&inner.inflight)
            .keys()
            .map(|(id, attempts)| DatabaseAbortClaim {
                id: *id,
                attempts: *attempts,
            })
            .collect();
        if claims.is_empty() {
            inner.health.recovered(WorkerComponent::Abort);
            continue;
        }
        // Cancellable mid-poll, like every other timer loop: `stop_timers`
        // gives all of them one second *together*, and a statement that is
        // already on the wire cannot be hurried. Without this, a poll caught by
        // a lock wait or a slow round trip turned an otherwise clean shutdown
        // into `Error::WorkerTask("timer shutdown timed out")`. Dropping this
        // read costs nothing: it is a `SELECT`, and by the time the timer token
        // is cancelled the processors have already drained.
        let poll = tokio::select! {
            biased;
            _ = token.cancelled() => return,
            poll = inner.database.aborting_of(&claims, inner.id) => poll,
        };
        match poll {
            Ok(poll) => {
                inner.health.recovered(WorkerComponent::Abort);
                for aborting in poll.aborting {
                    let entry = lock_inflight(&inner.inflight)
                        .get(&(aborting.id, aborting.attempts))
                        .cloned();
                    // No worker check here: `aborting_of` reports a row whose
                    // `worker_id` is not this worker's as superseded, so
                    // everything that reaches `aborting` is already ours.
                    if let Some(entry) = entry {
                        let reason = if aborting.swept {
                            WorkerAbortReason::Swept
                        } else {
                            WorkerAbortReason::User(
                                aborting.reason.unwrap_or_else(|| "aborted".to_string()),
                            )
                        };
                        entry.request_abort(reason, inner.abort_grace);
                    }
                }
                for claim in poll.missing {
                    let entry = lock_inflight(&inner.inflight)
                        .get(&(claim.id, claim.attempts))
                        .cloned();
                    if let Some(entry) = entry {
                        entry.request_abort(WorkerAbortReason::Missing, inner.abort_grace);
                        tracing::warn!(
                            job.id = %claim.id,
                            "in-flight job row was deleted; cancelling its handler"
                        );
                    }
                }
                for claim in poll.superseded {
                    // The lookup is by `(id, attempts)`, so a row this worker
                    // re-claimed in the meantime is a different entry: the
                    // attempt that lost the row is cancelled and its live
                    // successor is left alone.
                    let entry = lock_inflight(&inner.inflight)
                        .get(&(claim.id, claim.attempts))
                        .cloned();
                    if let Some(entry) = entry {
                        entry.request_abort(WorkerAbortReason::Superseded, inner.abort_grace);
                        tracing::warn!(
                            job.id = %claim.id,
                            attempt = claim.attempts,
                            "in-flight job row is no longer this attempt's; \
                             cancelling its handler"
                        );
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
async fn schedule_loop(
    inner: Arc<WorkerInner>,
    token: CancellationToken,
    mut holder_warned: HashSet<String>,
    mut state: CronSchedulingState,
) {
    let mut interval = tokio::time::interval(inner.timers.schedule);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = token.cancelled() => return,
            _ = interval.tick() => {}
        }
        tokio::select! {
            biased;
            _ = token.cancelled() => return,
            _ = schedule_crons_once(&inner, &mut holder_warned, &mut state, None) => {}
        }
    }
}

/// Schedules every cron occurrence due when a burst starts, without admitting
/// recurrences that become due while the queue is draining.
async fn schedule_burst_crons(
    inner: &Arc<WorkerInner>,
    holder_warned: &mut HashSet<String>,
    state: &mut CronSchedulingState,
) {
    let through = match inner.database.now().await {
        Ok(through) => through,
        Err(error) => {
            let mut failures = state.failures();
            failures.push(format!("could not read burst cron boundary: {error}"));
            inner
                .health
                .failed(WorkerComponent::Scheduler, &failures.join("; "));
            return;
        }
    };
    while schedule_crons_once(inner, holder_warned, state, Some(through)).await {}
}

/// What startup reconciliation decided about this worker's crons.
#[derive(Default)]
struct CronSchedulingState {
    /// Dedupe keys this worker must not schedule.
    disabled: HashSet<String>,
    /// Dedupe keys that need reconciling again for a reason that may pass on a
    /// later attempt — a reconciliation that failed transiently, or a schedule
    /// row that went missing under a running worker — so every scheduling pass
    /// retries them first.
    unreconciled: HashSet<String>,
    /// Why each permanently rejected cron was disabled. Held apart from
    /// [`Self::rejected`] and never cleared: a disabled cron is never
    /// re-evaluated, so nothing can ever re-add its failure, and folding it in
    /// with the retryable ones let an *unrelated* transient cron recovering
    /// erase it and report the worker healthy.
    disabled_reasons: Vec<String>,
    /// Reconciliation failures that a later attempt may clear. Discarded and
    /// re-collected whenever those crons are retried.
    rejected: Vec<String>,
}

impl CronSchedulingState {
    /// Every reconciliation failure still in force, permanent ones first.
    fn failures(&self) -> Vec<String> {
        self.disabled_reasons
            .iter()
            .chain(&self.rejected)
            .cloned()
            .collect()
    }
}

/// Reconciles every registered cron against the durable schedule rows.
///
/// A cron problem never stops the worker. A superseded revision is the normal
/// state of a not-yet-upgraded process during a rolling deploy, so it is logged
/// and skipped without touching health; a rejected definition is a deploy
/// mistake, so it degrades `Scheduler` health while ordinary jobs keep flowing.
async fn reconcile_crons(inner: &Arc<WorkerInner>) -> CronSchedulingState {
    let mut state = CronSchedulingState::default();
    reconcile_crons_into(inner, &mut state, None).await;
    let failures = state.failures();
    if failures.is_empty() {
        inner.health.recovered(WorkerComponent::Scheduler);
    } else {
        inner
            .health
            .failed(WorkerComponent::Scheduler, &failures.join("; "));
    }
    state
}

/// Reconciles the registered crons — all of them, or only `retry_keys` when the
/// scheduling loop is retrying earlier failures — recording the outcome in
/// `state`. Leaves health alone; the caller owns that.
async fn reconcile_crons_into(
    inner: &Arc<WorkerInner>,
    state: &mut CronSchedulingState,
    retry_keys: Option<&HashSet<String>>,
) {
    let selected = || {
        inner
            .crons
            .iter()
            .filter(|entry| retry_keys.is_none_or(|keys| keys.contains(&entry.dedupe_key)))
    };
    for entry in selected() {
        // One clock reading per entry. `reconcile_cron` is a round trip of its
        // own, so a reading shared across the loop is already stale by the time
        // a large registry reaches its last entries — and a cursor computed
        // from a stale clock lands in the past, making the cron instantly "due"
        // with an occurrence its own misfire policy then has to skip.
        let now = match inner.database.now().await {
            Ok(now) => now,
            Err(error) => {
                // Infrastructure, not a definition problem: a pool timeout or a
                // failover during a rolling restart must not silently stop every
                // cron for the rest of this process's life, so these stay pending
                // and the scheduling loop retries them.
                tracing::error!(
                    cron = %entry.template.name,
                    dedupe_key = %entry.dedupe_key,
                    %error,
                    "cron reconciliation could not read the database clock"
                );
                state
                    .rejected
                    .push(format!("{}: {error}", entry.dedupe_key));
                state.unreconciled.insert(entry.dedupe_key.clone());
                continue;
            }
        };
        match inner.database.reconcile_cron(entry, now).await {
            Ok(DatabaseCronAuthority::Active) => {
                state.unreconciled.remove(&entry.dedupe_key);
            }
            Ok(DatabaseCronAuthority::Inactive { revision }) => {
                tracing::info!(
                    cron = %entry.template.name,
                    dedupe_key = %entry.dedupe_key,
                    local.revision = entry.options.revision,
                    authority.revision = revision,
                    "cron superseded by a higher revision; not scheduled by this worker"
                );
                state.unreconciled.remove(&entry.dedupe_key);
                state.disabled.insert(entry.dedupe_key.clone());
            }
            // A rejected *definition* is a deploy mistake that no retry can
            // fix, so it disables the cron. Anything else is treated as
            // transient and retried on the next scheduling pass.
            Err(error) => {
                let permanent = matches!(error, Error::Config(_));
                tracing::error!(
                    cron = %entry.template.name,
                    dedupe_key = %entry.dedupe_key,
                    %error,
                    permanent,
                    "cron reconciliation failed"
                );
                let reason = format!("{}: {error}", entry.dedupe_key);
                if permanent {
                    state.unreconciled.remove(&entry.dedupe_key);
                    state.disabled.insert(entry.dedupe_key.clone());
                    state.disabled_reasons.push(reason);
                } else {
                    state.unreconciled.insert(entry.dedupe_key.clone());
                    state.rejected.push(reason);
                }
            }
        }
    }
}

async fn schedule_crons_once(
    inner: &Arc<WorkerInner>,
    holder_warned: &mut HashSet<String>,
    state: &mut CronSchedulingState,
    through: Option<DateTime<Utc>>,
) -> bool {
    // Retry any cron whose reconciliation hit a transient failure before
    // scheduling, so a blip at startup costs one pass rather than every
    // occurrence for the lifetime of the process.
    if !state.unreconciled.is_empty() {
        // Only the retryable failures are discarded here; `disabled_reasons`
        // survives, so a permanently rejected cron keeps degrading health even
        // while an unrelated transient one recovers.
        state.rejected.clear();
        let retry_keys = state.unreconciled.clone();
        reconcile_crons_into(inner, state, Some(&retry_keys)).await;
    }
    let mut failed = state.failures();
    let candidates: Vec<String> = inner
        .crons
        .iter()
        .filter(|entry| {
            !state.disabled.contains(&entry.dedupe_key)
                && !state.unreconciled.contains(&entry.dedupe_key)
        })
        .map(|entry| entry.dedupe_key.clone())
        .collect();
    // One read for the whole registry instead of a transaction per cron, and
    // none at all for a worker whose crons are every one of them disabled —
    // the permanent state of a worker a higher revision has superseded. The
    // pre-filter is an optimisation only, so a failure falls back to asking
    // every cron directly rather than skipping the pass.
    let due = if candidates.is_empty() {
        HashSet::new()
    } else {
        match inner.database.due_crons(&candidates, through).await {
            Ok(due) => due,
            Err(error) => {
                tracing::debug!(%error, "cron due-check failed; scheduling every cron directly");
                candidates.iter().cloned().collect()
            }
        }
    };
    let mut advance_again = false;
    for entry in &inner.crons {
        if !due.contains(&entry.dedupe_key) {
            continue;
        }
        match inner.database.schedule_cron(entry, through).await {
            Ok(DatabaseCronScheduleResult::NotDue) | Ok(DatabaseCronScheduleResult::Contended) => {}
            Ok(DatabaseCronScheduleResult::Published { id, occurrence }) => {
                advance_again |= through.is_some();
                holder_warned.remove(&entry.dedupe_key);
                tracing::info!(
                    cron = %entry.template.name,
                    job.id = %id,
                    scheduled_at = %occurrence,
                    "published cron occurrence"
                );
            }
            Ok(DatabaseCronScheduleResult::AlreadyPublished { occurrence }) => {
                advance_again |= through.is_some();
                holder_warned.remove(&entry.dedupe_key);
                tracing::debug!(
                    cron = %entry.template.name,
                    scheduled_at = %occurrence,
                    "cron occurrence was already published"
                );
            }
            Ok(DatabaseCronScheduleResult::SkippedStale { occurrence }) => {
                advance_again |= through.is_some();
                tracing::warn!(
                    cron = %entry.template.name,
                    scheduled_at = %occurrence,
                    "skipped stale cron occurrence"
                );
            }
            Ok(DatabaseCronScheduleResult::SkippedHeld {
                occurrence,
                existing,
            }) => {
                advance_again |= through.is_some();
                if holder_warned.insert(entry.dedupe_key.clone()) {
                    tracing::warn!(
                        cron = %entry.template.name,
                        scheduled_at = %occurrence,
                        dedupe_key = %entry.dedupe_key,
                        holder.scheduled_at = %existing.scheduled_at,
                        holder.kind = %existing.kind,
                        holder.name = %existing.name,
                        "cron dedupe key is held by another live job; occurrence skipped"
                    );
                }
            }
            // Another worker published a higher revision while this one was
            // running. Expected mid-deploy: stop scheduling it and stay healthy.
            Ok(DatabaseCronScheduleResult::Inactive { revision }) => {
                state.disabled.insert(entry.dedupe_key.clone());
                tracing::info!(
                    cron = %entry.template.name,
                    dedupe_key = %entry.dedupe_key,
                    local.revision = entry.options.revision,
                    authority.revision = revision,
                    "cron superseded by a higher revision; not scheduled by this worker"
                );
            }
            // Queued for reconciliation, exactly like a transient
            // reconciliation failure: `reconcile_crons` runs once, at startup,
            // so `state.unreconciled` is the only thing that can rewrite a
            // schedule row this worker lost underneath it — and a lost row is
            // what `schedule_cron` reports as "was not reconciled", every tick,
            // forever. This cannot spin: a definition the database genuinely
            // refuses comes back from reconciliation as `Error::Config` and
            // lands in `state.disabled`.
            Err(error) => {
                tracing::warn!(%error, cron = %entry.template.name, "cron scheduling failed");
                state.unreconciled.insert(entry.dedupe_key.clone());
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
    advance_again
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
        let drain_until =
            tokio::time::Instant::now() + inner.timers.sweep.min(MAX_SWEEP_DRAIN_TIME);
        // The sweeper shares the worker's pool with dequeues and finalization,
        // so a drain is bounded by passes as well as wall clock, and each pass
        // repeats only the operations that filled their batch.
        let mut operations = SweepOperations::ALL;
        for pass in 1..=MAX_SWEEP_DRAIN_PASSES {
            // A pass issues several statements against the shared pool and can
            // outlast the shutdown budget on a loaded database. Without a
            // cancellation point *inside* the pass, an ordinary shutdown that
            // lands mid-sweep would exhaust that budget and report a timer
            // shutdown failure for an otherwise clean stop.
            let swept = tokio::select! {
                biased;
                _ = token.cancelled() => {
                    sweeper.release().await;
                    return;
                }
                swept = sweeper.sweep_operations(operations) => swept,
            };
            match swept {
                Ok(report) if report.more_work() => {
                    inner.health.recovered(WorkerComponent::Sweeper);
                    if pass == MAX_SWEEP_DRAIN_PASSES {
                        tracing::debug!("sweep drain pass budget exhausted");
                        break;
                    }
                    if tokio::time::Instant::now() >= drain_until {
                        tracing::debug!("sweep drain time budget exhausted");
                        break;
                    }
                    operations = report.unfinished;
                    tokio::select! {
                        biased;
                        _ = token.cancelled() => {
                            sweeper.release().await;
                            return;
                        }
                        _ = tokio::task::yield_now() => {}
                    }
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
        // Also cancellable mid-write (see `abort_loop`). A heartbeat is an
        // idempotent upsert of this worker's own lease, and shutdown retires
        // that lease immediately afterwards, so abandoning one in flight loses
        // nothing — while waiting for it to land can spend the whole
        // timer-shutdown budget on a write whose result no longer matters.
        tokio::select! {
            biased;
            _ = token.cancelled() => return,
            _ = write_worker_info(&inner, worker_info_ttl(inner.timers.worker_info)) => {}
        }
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
    // Never `Reopen`: a worker's heartbeat is not a request for work, so it
    // must leave a lease `close_intake` already closed alone. `Open`/`Closed`
    // only decide what a lease this write has to *create* starts as.
    let intake = if inner.intake_open.load(Ordering::Acquire) {
        LeaseIntake::Open
    } else {
        LeaseIntake::Closed
    };
    if let Err(error) = inner
        .database
        .write_worker_info(inner.id, stats, inner.metadata.clone(), ttl, intake)
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

    /// The timeout arm keeps a settled handler's error instead of the deadline,
    /// so it reaches finalization without passing through `classify_attempt_join`
    /// — both routes have to sanitize, and they share one constructor so they
    /// cannot drift apart.
    #[test]
    fn test_handler_error_loses_its_nul_on_every_route_into_finalization() {
        let raw = JobError {
            kind: JobErrorKind::Timeout,
            message: "bad\u{0}input".to_string(),
        };
        for result in [
            handler_errored(raw.clone()),
            classify_attempt_join(Ok(Err(raw)), WorkerAttemptResult::Cancelled),
        ] {
            match result {
                WorkerAttemptResult::Errored(error) => {
                    assert_eq!(error.message, "bad\u{fffd}input");
                    assert_eq!(error.kind, JobErrorKind::Timeout, "the kind is preserved");
                }
                _ => panic!("a handler error must fail the attempt"),
            }
        }
    }

    #[test]
    fn test_unhandled_probe_completion_tracks_every_underfilled_scanned_batch() {
        assert!(unhandled_probe_completed(true, 0, 4));
        assert!(unhandled_probe_completed(true, 2, 4));
        assert!(!unhandled_probe_completed(true, 4, 4));
        assert!(!unhandled_probe_completed(false, 0, 4));
    }

    #[tokio::test]
    async fn test_worker_health_ignores_unchanged_snapshots() {
        let reporter = WorkerHealthReporter::new();
        let mut health = reporter.subscribe();

        reporter.recovered(WorkerComponent::Notification);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), health.changed())
                .await
                .is_err()
        );

        reporter.ready();
        let snapshot = tokio::time::timeout(Duration::from_secs(1), health.changed())
            .await
            .unwrap();
        assert_eq!(snapshot.status, WorkerHealthStatus::Ready);
    }

    #[tokio::test]
    async fn test_worker_health_reports_channel_close_once_without_spinning() {
        let reporter = WorkerHealthReporter::new();
        let mut health = reporter.subscribe();
        drop(reporter);

        let snapshot = tokio::time::timeout(Duration::from_secs(1), health.changed())
            .await
            .unwrap();
        assert_eq!(snapshot.status, WorkerHealthStatus::Starting);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), health.changed())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_wait_for_processors_rejects_clean_exit_when_continuous() {
        let mut processors = JoinSet::new();
        processors.spawn(async {});

        let error = wait_for_processors(&mut processors, false)
            .await
            .unwrap_err();

        assert!(matches!(error, Error::WorkerTask("processor loop")));
    }

    #[tokio::test]
    async fn test_wait_for_processors_allows_clean_exits_when_burst() {
        let mut processors = JoinSet::new();
        processors.spawn(async {});
        processors.spawn(async {});

        wait_for_processors(&mut processors, true).await.unwrap();
    }

    #[tokio::test]
    async fn test_wait_for_processors_reports_panics() {
        let mut processors = JoinSet::new();
        processors.spawn(async { panic!("processor panic") });

        let error = wait_for_processors(&mut processors, false)
            .await
            .unwrap_err();

        assert!(matches!(error, Error::Task(error) if error.is_panic()));
    }

    #[tokio::test]
    async fn test_wait_for_background_exit_reports_loop_name() {
        let mut tasks = JoinSet::new();
        tasks.spawn(async { "test loop" });

        let error = wait_for_background_exit(&mut tasks).await;

        assert!(matches!(error, Error::WorkerTask("test loop")));
    }

    fn at(iso: &str) -> DateTime<Utc> {
        iso.parse().unwrap()
    }

    #[test]
    fn test_cron_validity_is_capped_by_the_successor_for_dense_schedules() {
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
    fn test_cron_validity_grace_scales_with_sparse_periods_up_to_a_minute() {
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
    fn test_previous_cron_occurrence_finds_boundary_within_lookback() {
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
    fn test_previous_cron_occurrence_finds_sparse_boundary_without_scanning() {
        let daily = entry("0 0 * * *");
        assert_eq!(
            daily
                .previous_occurrence(at("2026-01-01T12:00:00Z"))
                .unwrap(),
            at("2026-01-01T00:00:00Z")
        );
    }
}
