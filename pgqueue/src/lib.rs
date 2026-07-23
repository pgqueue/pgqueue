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
//! pgqueue::Worker::builder(queue).register(send_email).build()?.run().await?;
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

    /// An internal asynchronous task panicked or was cancelled.
    #[error("task error: {0}")]
    Task(#[from] tokio::task::JoinError),

    /// A worker infrastructure task stopped unexpectedly or could not stop
    /// within its hard shutdown bound.
    #[error("worker task failed: {0}")]
    WorkerTask(&'static str),

    /// The worker-hosted dashboard could not bind or its server task panicked.
    #[cfg(feature = "dashboard")]
    #[error("dashboard server error: {0}")]
    Dashboard(std::io::Error),

    /// The job does not exist (deleted, expired, or never enqueued).
    #[error("job not found: {0}")]
    JobNotFound(Uuid),

    /// The job completed, but retention deleted its result before it could
    /// be read.
    #[error("job {0} completed but its result was already deleted")]
    ResultExpired(Uuid),

    /// The job is not owned by the caller's active attempt.
    #[error("job cannot be touched by this attempt: {0}")]
    JobNotTouchable(Uuid),

    /// A job waited on via `apply` or `wait` finished unsuccessfully.
    #[error("job failed: {0}")]
    Job(#[from] JobError),

    /// Waiting for a job result exceeded the caller's deadline.
    #[error("timed out waiting for job result")]
    WaitTimeout,
}

#[cfg(feature = "dashboard")]
mod dashboard;
mod database;
mod job;
mod queue;
mod sweeper;
mod worker;

#[cfg(feature = "dashboard")]
pub use dashboard::{Dashboard, DashboardServer, DashboardServerHandle};

pub use job::{
    CronMisfirePolicy, CronOptions, EnqueueOutcome, FromJobContext, JobBuilder, JobConfig,
    JobContext, JobCursor, JobError, JobErrorKind, JobFilter, JobHandle, JobRequest, JobRetention,
    JobRetryBackoff, JobRow, JobState, JobStatus, JobType,
};
pub use queue::{
    Attempt, Consumer, MigrationMode, Queue, QueueBuilder, QueueCounts, QueueInfo, QueueStats,
};
pub use sweeper::{Sweeper, SweeperReport};
pub use worker::{
    Worker, WorkerBuilder, WorkerComponent, WorkerHealth, WorkerHealthFailure,
    WorkerHealthSnapshot, WorkerHealthStatus, WorkerInfo, WorkerTimers,
};

/// Marks an `async fn` as a cron job handler run on a schedule. The first
/// attribute argument is the UTC cron expression (compile-time validated);
/// cron functions take no payload — every parameter is an extractor.
///
/// ```no_run
/// #[pgqueue::cron("*/5 * * * *")]
/// async fn cleanup(ctx: pgqueue::JobContext) -> anyhow::Result<u64> {
///     Ok(ctx.queue().counts().await?.queued as u64)
/// }
/// # async fn run(queue: pgqueue::Queue) -> anyhow::Result<()> {
/// // The schedule registers with the job itself:
/// pgqueue::Worker::builder(queue).register(cleanup).build()?.run().await?;
/// # Ok(())
/// # }
/// ```
pub use pgqueue_macros::cron;
/// Marks an `async fn` as a job handler. See the crate-level documentation.
pub use pgqueue_macros::job;

/// Support machinery for macro-generated code. Not part of the public API;
/// anything here may change without notice.
#[doc(hidden)]
pub mod __private {
    use std::time::Duration;

    use serde::Serialize;
    use serde::de::DeserializeOwned;
    pub use serde_json::Value;
    use uuid::Uuid;

    pub use crate::job::{IntoJobResult, JobHandlerFuture, TypeErasedJobHandler};
    use crate::{Error, JobRow, JobStatus, Queue};
    use crate::{JobError, JobErrorKind};

    /// Returns the completion channel used by a queue.
    pub fn done_channel(queue: &str) -> String {
        crate::database::done_channel(queue)
    }

    /// Returns the advisory-lock namespace used by dequeues.
    pub fn dequeue_lock_key(database: &str) -> i32 {
        crate::database::dequeue_lock_key(database)
    }

    /// Returns the advisory lock used for one queue's sweep leadership.
    pub fn sweep_lock_key(database: &str, queue: &str) -> i64 {
        crate::database::sweep_lock_key(database, queue)
    }

    /// Test/support access to the raw protocol. Applications should use
    /// `Queue::consumer` and its opaque attempts instead.
    pub async fn dequeue(queue: &Queue, limit: i64, worker_id: Uuid) -> Result<Vec<JobRow>, Error> {
        queue.database().dequeue(limit, worker_id).await
    }

    /// Test/support access to guarded finalization.
    pub async fn finish(
        queue: &Queue,
        job: &JobRow,
        status: JobStatus,
        result: Option<Value>,
        error: Option<&str>,
    ) -> Result<bool, Error> {
        queue.database().finish(job, status, result, error).await
    }

    /// Test/support access to guarded retry.
    pub async fn retry(queue: &Queue, job: &JobRow, error: &str) -> Result<bool, Error> {
        queue.database().retry(job, error).await
    }

    /// Test/support access to the legacy unguarded touch behavior.
    pub async fn touch(queue: &Queue, id: Uuid) -> Result<(), Error> {
        queue.database().touch(id).await
    }

    /// Test/support access to worker lease rows.
    pub async fn write_worker_info(
        queue: &Queue,
        worker_id: Uuid,
        stats: Value,
        metadata: Option<Value>,
        ttl: Duration,
    ) -> Result<(), Error> {
        queue
            .database()
            .write_worker_info(worker_id, stats, metadata, ttl, true)
            .await
    }

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

#[cfg(test)]
mod tests {
    use crate::JobErrorKind;

    #[test]
    fn private_helpers_round_trip_and_surface_errors() {
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
