//! Background and cron job processing backed by PostgreSQL 18+.
//!
//! `pgqueue` is an opinionated job queue for tokio applications: jobs are plain
//! `async fn`s annotated with [`macro@job`], enqueued with full type safety, and
//! processed by [`Worker`]s that coordinate through a single Postgres database
//! using `FOR UPDATE SKIP LOCKED` and `LISTEN`/`NOTIFY`.
//!
//! ```no_run
//! #[derive(serde::Serialize, serde::Deserialize)]
//! struct SendEmail { to: String, body: String }
//!
//! #[pgqueue::job]
//! async fn send_email(args: SendEmail) -> anyhow::Result<()> {
//!     println!("emailing {}", args.to);
//!     Ok(())
//! }
//!
//! # async fn run() -> anyhow::Result<()> {
//! let queue = pgqueue::Queue::connect(&std::env::var("DATABASE_URL")?).await?;
//! queue.enqueue(send_email::job(SendEmail { to: "a@b.c".into(), body: "hi".into() })).await?;
//! pgqueue::Worker::builder(queue).register_job(send_email).run().await?;
//! # Ok(())
//! # }
//! ```

// Macro expansions use this stable path when invoked from this package, while
// downstream crates use the dependency name resolved from their Cargo.toml.
extern crate self as pgqueue;

use uuid::Uuid;

/// Infrastructure failure returned by queue and worker operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A database operation failed.
    #[error(transparent)]
    Db(#[from] sqlx::Error),

    /// JSON (de)serialization of a payload, result, or metadata failed.
    #[error(transparent)]
    Serde(#[from] serde_json::Error),

    /// Applying or validating the embedded SQLx migrations failed.
    #[error(transparent)]
    Migration(#[from] sqlx::migrate::MigrateError),

    /// Invalid queue, job, or worker configuration.
    #[error("configuration error: {0}")]
    Config(String),

    /// A dedupe-key decision raced with a writer bypassing the enqueue
    /// advisory lock (SQL writing `pgqueue.jobs` directly): the key's live
    /// holder appeared and vanished mid-operation, leaving no job to report
    /// as the collision winner. Transient, unlike [`Error::Config`] — retry
    /// the operation.
    #[error("dedupe race: {0}")]
    DedupeRace(String),

    /// An internal asynchronous task panicked or was cancelled.
    #[error("task error: {0}")]
    Task(#[from] tokio::task::JoinError),

    /// A worker infrastructure task stopped unexpectedly or could not stop
    /// within its hard shutdown bound.
    #[error("worker task failed: {0}")]
    WorkerTask(&'static str),

    /// A dashboard could not bind or its server task panicked.
    #[error("dashboard server error: {0}")]
    Dashboard(std::io::Error),

    /// The job does not exist (deleted, expired, or never enqueued).
    #[error("job not found: {0}")]
    JobNotFound(Uuid),

    /// The job completed, but retention deleted its result before it could
    /// be read.
    #[error("job {0} completed but its result was already deleted")]
    ResultExpired(Uuid),

    /// A job waited on via `enqueue_and_wait` or `wait` finished unsuccessfully.
    #[error("job failed: {0}")]
    Job(#[from] JobError),

    /// Waiting for a job result exceeded the caller's deadline.
    #[error("timed out waiting for job result")]
    WaitTimeout,
}

mod dashboard;
mod database;
mod job;
mod queue;
mod sweeper;
mod worker;

pub use dashboard::{Dashboard, DashboardServer, DashboardServerHandle};

pub use job::{
    CronDefinition, CronMisfirePolicy, CronOptions, EnqueueResult, FromJobContext, JobBuilder,
    JobConfig, JobContext, JobCursor, JobDefinition, JobError, JobErrorKind, JobFilter, JobHandle,
    JobRequest, JobRetention, JobRetryBackoff, JobRow, JobState, JobStatus, JobType,
};
pub use queue::{
    Attempt, Consumer, MigrationMode, Queue, QueueBuilder, QueueCounts, QueueInfo, QueueStats,
};
pub use sweeper::{SweepOperations, Sweeper, SweeperReport};
pub use worker::{
    Worker, WorkerBuilder, WorkerComponent, WorkerHealth, WorkerHealthFailure,
    WorkerHealthSnapshot, WorkerHealthStatus, WorkerInfo, WorkerTimers,
};

/// Marks an `async fn` as a cron job handler run on a schedule. The first
/// attribute argument is a UTC cron expression whose syntax is checked at
/// compile time; an expression with no future occurrence disables that cron on
/// the worker and degrades [`WorkerComponent::Scheduler`] health rather than
/// stopping the worker.
/// Cron functions take no payload — every parameter is an extractor.
///
/// Every attribute [`macro@job`] accepts is accepted here too, plus:
///
/// | Attribute | Default | Meaning |
/// | --- | --- | --- |
/// | `revision = N` | `0` | Coordinates this definition across workers. Raise it when the schedule or the options change; the highest revision wins and workers on older ones stop scheduling this cron. Reusing a revision for a different definition is rejected. |
///
/// `name` is capped at 250 bytes rather than 255, because a cron's dedupe key
/// is the derived `cron:{name}`.
///
/// ```no_run
/// #[pgqueue::cron("*/5 * * * *")]
/// async fn cleanup(ctx: pgqueue::JobContext) -> anyhow::Result<u64> {
///     Ok(ctx.queue().counts().await?.queued as u64)
/// }
///
/// #[pgqueue::cron(
///     "0 * * * *",
///     revision = 1,
///     name = "collect_hourly_metrics",
///     max_attempts = 2,
///     timeout_ms = 120_000,
///     result_ttl_ms = 604_800_000,
///     retry_delay_ms = 1_000,
///     max_backoff_ms = 60_000,
///     priority = 10,
/// )]
/// async fn collect_metrics() -> anyhow::Result<()> {
///     Ok(())
/// }
/// # async fn run(queue: pgqueue::Queue) -> anyhow::Result<()> {
/// // Register the handler and its embedded schedule:
/// pgqueue::Worker::builder(queue).register_cron(cleanup).run().await?;
/// # Ok(())
/// # }
/// ```
pub use pgqueue_macros::cron;
/// Marks an `async fn` as a job handler.
///
/// The first parameter is the job's payload — use `_: ()` for a job that takes
/// none — and every parameter after it is an extractor ([`JobState`],
/// [`JobContext`]). The expansion adds `::job(args)` for building an enqueue
/// request, `::call(..)` for invoking the handler directly, and a [`JobType`]
/// implementation carrying the configuration below.
///
/// | Attribute | Default | Meaning |
/// | --- | --- | --- |
/// | `name = "..."` | the function's name, with any `r#` stripped | Name stored with the job, at most 255 bytes. Keep it stable until every job published under the old name has finished. |
/// | `max_attempts = N` | `1` | Total attempts, including the first run. |
/// | `timeout_ms = N` | `10_000` | Maximum duration of one attempt. `0` disables the timeout; an attempt is still recovered once its worker's lease has been expired for the queue's sweep grace. |
/// | `result_ttl_ms = N` | `600_000` | How long a finished job's row is retained. `0` deletes it as it finishes. |
/// | `retry_delay_ms = N` | `0` | Base delay before a retry. |
/// | `max_backoff_ms = N` | disabled | Exponential backoff capped at `N`. Requires a non-zero `retry_delay_ms`. |
/// | `priority = N` | `0` | Dequeue priority as an `i16`, lower values first. |
///
/// Durations are milliseconds and must not exceed the maximum a queue supports;
/// an out-of-range one fails the build rather than the enqueue.
///
/// ```no_run
/// #[derive(serde::Serialize, serde::Deserialize)]
/// struct Email { address: String }
///
/// #[pgqueue::job(
///     name = "deliver_email",
///     max_attempts = 5,
///     timeout_ms = 30_000,
///     result_ttl_ms = 3_600_000,
///     retry_delay_ms = 500,
///     max_backoff_ms = 60_000,
///     priority = -10,
/// )]
/// async fn send_email(args: Email) -> anyhow::Result<String> {
///     Ok(args.address)
/// }
/// # async fn run(queue: pgqueue::Queue) -> anyhow::Result<()> {
/// queue.enqueue(send_email::job(Email { address: "user@example.com".into() })).await?;
/// # Ok(())
/// # }
/// ```
pub use pgqueue_macros::job;

/// Support machinery for macro-generated code. Not part of the public API;
/// anything here may change without notice.
#[doc(hidden)]
pub mod __private {
    use serde::Serialize;
    use serde::de::DeserializeOwned;
    pub use serde_json::Value;

    pub use crate::job::MAX_DURATION_MS;
    pub use crate::job::{IntoJobResult, JobHandlerFuture, TypeErasedJobHandler};
    use crate::{JobError, JobErrorKind};

    /// Deserializes a stored payload into the handler's argument type.
    pub fn decode_payload<T: DeserializeOwned>(payload: Value) -> Result<T, JobError> {
        serde_json::from_value(payload)
            .map_err(|e| JobError::new(JobErrorKind::Decode, format!("payload decode: {e}")))
    }

    /// Normalizes and serializes a handler's return value.
    pub fn encode_result<R>(result: R) -> Result<Value, JobError>
    where
        R: IntoJobResult,
        R::Output: Serialize,
    {
        let output = result.into_job_result()?;
        serde_json::to_value(output)
            .map_err(|e| JobError::new(JobErrorKind::Decode, format!("result encode: {e}")))
    }
}

/// Raw-protocol access for this crate's own integration tests, which compile as
/// a separate crate and so can only reach the crate through its public API.
///
/// Gated behind the non-default, internal `_test` feature so ordinary builds —
/// including every release build of a downstream crate — never expose it. Not
/// semver-stable, and never for application code: it takes a caller-supplied
/// [`JobRow`] and bypasses the [`Consumer`] and [`Attempt`] guards, so a job
/// claimed here has no worker lease, and the sweeper will treat it as abandoned
/// and hand it to a second worker while it is still running.
#[cfg(feature = "_test")]
#[doc(hidden)]
pub mod __test_support {
    use std::time::Duration;

    use serde_json::Value;
    use uuid::Uuid;

    use crate::{Error, JobRow, JobStatus, Queue};

    /// Returns the completion channel used by a queue.
    pub fn done_channel(queue: &str) -> String {
        crate::database::done_channel(queue)
    }

    /// Returns the advisory-lock namespace used by dedupe enqueues.
    pub fn dedupe_enqueue_lock_key(database: &str) -> i32 {
        crate::database::dedupe_enqueue_lock_key(database)
    }

    /// Returns the advisory lock used for one queue's sweep leadership.
    pub fn sweep_lock_key(database: &str, queue: &str) -> i64 {
        crate::database::sweep_lock_key(database, queue)
    }

    /// The statement the `/health` liveness probe runs, so a plan-shape test
    /// can pin the shipped SQL rather than a copy of it.
    pub fn health_probe_sql() -> &'static str {
        crate::dashboard::HEALTH_PROBE_SQL
    }

    /// The statement every open dashboard polls per queue, for the same reason.
    pub fn dashboard_signals_sql() -> &'static str {
        crate::dashboard::DASHBOARD_SIGNALS_SQL
    }

    /// Raw dequeue that does not require a worker lease.
    pub async fn dequeue(queue: &Queue, limit: i64, worker_id: Uuid) -> Result<Vec<JobRow>, Error> {
        queue.database().dequeue_unleased(limit, worker_id).await
    }

    /// Raw access to the worker dequeue protocol.
    pub async fn dequeue_worker(
        queue: &Queue,
        limit: i64,
        worker_id: Uuid,
        registered_names: &[String],
        probe_unhandled: bool,
    ) -> Result<Vec<JobRow>, Error> {
        Ok(queue
            .database()
            .dequeue_worker(limit, worker_id, registered_names, probe_unhandled)
            .await?
            .jobs)
    }

    /// The diagnostic half of the worker dequeue protocol: whether work the
    /// caller can run is waiting, and the queued names it has no handler for.
    pub async fn dequeue_worker_probe(
        queue: &Queue,
        limit: i64,
        worker_id: Uuid,
        registered_names: &[String],
        probe_unhandled: bool,
    ) -> Result<(bool, Vec<String>), Error> {
        let batch = queue
            .database()
            .dequeue_worker(limit, worker_id, registered_names, probe_unhandled)
            .await?;
        Ok((batch.work_available, batch.unhandled_names))
    }

    /// Raw access to guarded finalization.
    pub async fn finish(
        queue: &Queue,
        job: &JobRow,
        status: JobStatus,
        result: Option<Value>,
        error: Option<&str>,
    ) -> Result<bool, Error> {
        queue.database().finish(job, status, result, error).await
    }

    /// Raw access to guarded retry.
    pub async fn retry(queue: &Queue, job: &JobRow, error: &str) -> Result<bool, Error> {
        queue.database().retry(job, error).await
    }

    /// Raw access to worker lease rows, reopening intake the way a
    /// [`Consumer`](crate::Consumer) heartbeat does.
    pub async fn write_worker_info(
        queue: &Queue,
        worker_id: Uuid,
        stats: Value,
        metadata: Option<Value>,
        ttl: Duration,
    ) -> Result<(), Error> {
        queue
            .database()
            .write_worker_info(
                worker_id,
                stats,
                metadata,
                ttl,
                crate::database::LeaseIntake::Reopen,
            )
            .await
    }

    /// Raw access to worker lease rows the way a worker writes its own: never
    /// reopening a closed lease, and creating a missing one in whichever intake
    /// state the worker is currently in.
    pub async fn write_worker_lease(
        queue: &Queue,
        worker_id: Uuid,
        stats: Value,
        metadata: Option<Value>,
        ttl: Duration,
        accepting: bool,
    ) -> Result<(), Error> {
        let intake = if accepting {
            crate::database::LeaseIntake::Open
        } else {
            crate::database::LeaseIntake::Closed
        };
        queue
            .database()
            .write_worker_info(worker_id, stats, metadata, ttl, intake)
            .await
    }

    /// The notification listener's health watch: `None` while subscribed, the
    /// latest error while disconnected. Every failed reconnect re-sends, so a
    /// test can observe the retry cadence. Starts the listener if the first
    /// caller.
    pub fn listener_health(queue: &Queue) -> tokio::sync::watch::Receiver<Option<String>> {
        queue.database().notify_listener().subscribe_health()
    }
}

#[cfg(test)]
mod tests {
    use crate::JobErrorKind;

    #[test]
    fn test_private_helpers_round_trip_and_surface_errors() {
        let value = crate::__private::encode_result(Ok::<u32, String>(7)).unwrap();
        assert_eq!(value, serde_json::json!(7));

        let decoded: u32 = crate::__private::decode_payload(serde_json::json!(7)).unwrap();
        assert_eq!(decoded, 7);

        // JSON object keys must be strings, so a tuple-keyed map cannot be
        // encoded: the encode error path.
        type BadKeys = std::collections::HashMap<(u32, u32), u32>;
        let bad: BadKeys = [((1, 2), 3)].into_iter().collect();
        let err = crate::__private::encode_result(Ok::<BadKeys, String>(bad)).unwrap_err();
        assert_eq!(err.kind, JobErrorKind::Decode);
        assert!(err.message.contains("result encode"), "{}", err.message);

        // And the decode error path.
        let err = crate::__private::decode_payload::<u32>(serde_json::json!("nope")).unwrap_err();
        assert_eq!(err.kind, JobErrorKind::Decode);
    }
}
