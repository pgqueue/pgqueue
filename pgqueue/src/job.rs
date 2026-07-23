//! Jobs: definitions, configuration, context, handlers, enqueue requests,
//! stored rows, result handles, and cron scheduling.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use cron_schedule::Cron;
use cron_schedule::parser::{CronParser, Seconds};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::Error;
use crate::database::DatabaseEnqueueOutcome;
use crate::queue::{Queue, QueueDoneEvent};

// One hundred years is beyond a useful queue delay while remaining safe for
// SQL date arithmetic and runtime clocks.
const MAX_DURATION: Duration = Duration::from_millis(3_153_600_000_000);

pub(crate) fn validate_duration(field: &str, duration: Duration) -> Result<(), Error> {
    if duration > MAX_DURATION {
        return Err(Error::Config(format!(
            "{field} exceeds the maximum supported duration of {MAX_DURATION:?}"
        )));
    }
    Ok(())
}

pub(crate) fn duration_to_ms(duration: Duration) -> i64 {
    i64::try_from(duration.as_nanos().div_ceil(1_000_000)).unwrap_or(i64::MAX)
}

/// How long a finished job's row (and result) is kept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobRetention {
    /// Keep the row for this long after it finishes, then the sweeper purges it.
    For(Duration),
    /// Keep the row forever. Reused unique keys and cron schedules retain one
    /// row per occurrence, so high-frequency recurring jobs should normally
    /// use a finite retention period.
    Forever,
    /// Delete the row as soon as a worker finishes it (no result retrieval).
    /// A queued job aborted before execution remains until the next sweep so
    /// waiters can observe its aborted outcome.
    DeleteImmediately,
}

impl JobRetention {
    /// Encoding for the `ttl_ms` column: `NULL` = forever, `0` = delete now.
    pub(crate) fn as_ttl_ms(self) -> Option<i64> {
        match self {
            JobRetention::For(d) => Some(duration_to_ms(d).max(1)),
            JobRetention::Forever => None,
            JobRetention::DeleteImmediately => Some(0),
        }
    }

    pub(crate) fn from_ttl_ms(ttl_ms: Option<i64>) -> Self {
        match ttl_ms {
            None => JobRetention::Forever,
            Some(0) => JobRetention::DeleteImmediately,
            Some(ms) => JobRetention::For(Duration::from_millis(ms.max(0) as u64)),
        }
    }
}

/// Retry delay growth strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum JobRetryBackoff {
    /// Every retry waits exactly `retry_delay`.
    None,
    /// Exponential backoff with full jitter: the nth retry waits a uniformly
    /// random duration in `[0, min(max, retry_delay * 2^(n-1))]`.
    Exponential {
        /// Upper bound for the un-jittered delay; `None` = unbounded.
        ///
        /// `default` is load-bearing: a `with` attribute disables serde's
        /// implicit missing-`Option`-is-`None` handling, and a stored backoff
        /// of `{"type":"exponential"}` must decode rather than poison every
        /// dequeue batch that selects its row.
        #[serde(rename = "max_ms", with = "opt_duration_ms", default)]
        max: Option<Duration>,
    },
}

impl JobRetryBackoff {
    /// Computes the delay before the next attempt. `attempts` is the number of
    /// attempts already made (>= 1 when retrying).
    pub(crate) fn next_delay(self, retry_delay: Duration, attempts: u32) -> Duration {
        match self {
            JobRetryBackoff::None => retry_delay.min(MAX_DURATION),
            JobRetryBackoff::Exponential { max } => {
                let capped = exponential_delay_bound(retry_delay, attempts, max);
                // Full jitter: a uniformly random delay up to the exponential
                // bound, so simultaneous retries spread out instead of
                // stampeding together.
                capped.mul_f64(rand::random::<f64>())
            }
        }
    }
}

fn exponential_delay_bound(
    retry_delay: Duration,
    attempts: u32,
    max: Option<Duration>,
) -> Duration {
    let exp = attempts.saturating_sub(1).min(63);
    let mut delay = retry_delay.min(MAX_DURATION);
    for _ in 0..exp {
        delay = delay.saturating_mul(2).min(MAX_DURATION);
        if delay == MAX_DURATION {
            break;
        }
    }
    max.map_or(delay, |max| delay.min(max)).min(MAX_DURATION)
}

impl sqlx::Type<sqlx::Postgres> for JobRetryBackoff {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <sqlx::types::Json<Self> as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <sqlx::types::Json<Self> as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for JobRetryBackoff {
    fn decode(
        value: sqlx::postgres::PgValueRef<'r>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
        Ok(<sqlx::types::Json<Self> as sqlx::Decode<sqlx::Postgres>>::decode(value)?.0)
    }
}

mod opt_duration_ms {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &Option<Duration>, s: S) -> Result<S::Ok, S::Error> {
        match v {
            Some(d) => {
                let millis = u64::try_from(d.as_nanos().div_ceil(1_000_000))
                    .map_err(|_| serde::ser::Error::custom("duration does not fit in u64 ms"))?;
                s.serialize_some(&millis)
            }
            None => s.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
        Ok(Option::<u64>::deserialize(d)?.map(Duration::from_millis))
    }
}

/// Per-job configuration, set by the `#[pgqueue::job]` attribute and
/// overridable per enqueue.
#[derive(Debug, Clone, PartialEq)]
pub struct JobConfig {
    /// Maximum attempts allowed (1 = no retries).
    pub max_attempts: u32,
    /// Per-attempt wall-clock limit enforced by the worker; `None` = unlimited.
    pub timeout: Option<Duration>,
    /// If set, the job must call `JobContext::touch()` at least this often or
    /// the sweeper considers it stuck.
    pub heartbeat: Option<Duration>,
    /// How long the finished row is retained.
    pub retention: JobRetention,
    /// Base delay before a retry.
    pub retry_delay: Duration,
    /// How the retry delay grows across attempts.
    pub backoff: JobRetryBackoff,
    /// Dequeue priority; lower values are dequeued first.
    pub priority: i16,
}

impl JobConfig {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        if self.max_attempts == 0 {
            return Err(Error::Config(
                "job max_attempts must allow at least one attempt".into(),
            ));
        }
        if self.max_attempts >= i32::MAX as u32 {
            return Err(Error::Config(format!(
                "job max_attempts must not exceed {}",
                i32::MAX - 1
            )));
        }
        if let Some(timeout) = self.timeout {
            if timeout.is_zero() {
                return Err(Error::Config(
                    "job timeout must be greater than zero or None".into(),
                ));
            }
            validate_duration("job timeout", timeout)?;
        }
        if let Some(heartbeat) = self.heartbeat {
            if heartbeat.is_zero() {
                return Err(Error::Config(
                    "job heartbeat must be greater than zero".into(),
                ));
            }
            validate_duration("job heartbeat", heartbeat)?;
        }
        if let JobRetention::For(ttl) = self.retention {
            validate_duration("job retention", ttl)?;
        }
        validate_duration("job retry delay", self.retry_delay)?;
        if let JobRetryBackoff::Exponential { max: Some(max) } = self.backoff {
            validate_duration("job backoff maximum", max)?;
        }
        Ok(())
    }
}

impl Default for JobConfig {
    /// 1 attempt, 10s timeout, 10min result retention, immediate retries,
    /// priority 0.
    fn default() -> Self {
        Self {
            max_attempts: 1,
            timeout: Some(Duration::from_secs(10)),
            heartbeat: None,
            retention: JobRetention::For(Duration::from_secs(600)),
            retry_delay: Duration::ZERO,
            backoff: JobRetryBackoff::None,
            priority: 0,
        }
    }
}

#[cfg(test)]
mod config_tests {
    use super::*;

    #[test]
    fn retention_maps_to_ttl_ms() {
        assert_eq!(JobRetention::Forever.as_ttl_ms(), None);
        assert_eq!(JobRetention::DeleteImmediately.as_ttl_ms(), Some(0));
        assert_eq!(
            JobRetention::For(Duration::from_secs(1)).as_ttl_ms(),
            Some(1000)
        );
        // Sub-millisecond retention still rounds up to 1ms (0 would mean delete).
        assert_eq!(
            JobRetention::For(Duration::from_micros(10)).as_ttl_ms(),
            Some(1)
        );

        assert_eq!(JobRetention::from_ttl_ms(None), JobRetention::Forever);
        assert_eq!(
            JobRetention::from_ttl_ms(Some(0)),
            JobRetention::DeleteImmediately
        );
        assert_eq!(
            JobRetention::from_ttl_ms(Some(1500)),
            JobRetention::For(Duration::from_millis(1500))
        );
    }

    #[test]
    fn backoff_serde_round_trip() {
        let none = serde_json::to_value(JobRetryBackoff::None).unwrap();
        assert_eq!(none, serde_json::json!({"type": "none"}));
        assert_eq!(
            serde_json::from_value::<JobRetryBackoff>(none).unwrap(),
            JobRetryBackoff::None
        );

        let capped = JobRetryBackoff::Exponential {
            max: Some(Duration::from_secs(60)),
        };
        let json = serde_json::to_value(capped).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"type": "exponential", "max_ms": 60000})
        );
        assert_eq!(
            serde_json::from_value::<JobRetryBackoff>(json).unwrap(),
            capped
        );

        let uncapped = JobRetryBackoff::Exponential { max: None };
        let json = serde_json::to_value(uncapped).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"type": "exponential", "max_ms": null})
        );
        assert_eq!(
            serde_json::from_value::<JobRetryBackoff>(json).unwrap(),
            uncapped
        );

        // A stored value may omit the key entirely (written by an external
        // client); it must decode instead of poisoning dequeue batches.
        assert_eq!(
            serde_json::from_value::<JobRetryBackoff>(serde_json::json!({"type": "exponential"}))
                .unwrap(),
            uncapped
        );

        assert!(
            serde_json::from_value::<JobRetryBackoff>(serde_json::json!({"type": "bogus"}))
                .is_err()
        );
    }

    #[test]
    fn backoff_none_is_flat() {
        let d = Duration::from_millis(250);
        for attempts in [0, 1, 5, 100] {
            assert_eq!(JobRetryBackoff::None.next_delay(d, attempts), d);
        }
    }

    #[test]
    fn backoff_exponential_respects_bounds() {
        let base = Duration::from_millis(100);
        let max = Duration::from_secs(1);
        let backoff = JobRetryBackoff::Exponential { max: Some(max) };
        for attempts in 1..=20 {
            let un_jittered = base
                .saturating_mul(2u32.saturating_pow(attempts - 1))
                .min(max);
            for _ in 0..10 {
                let delay = backoff.next_delay(base, attempts);
                assert!(
                    delay <= un_jittered,
                    "attempt {attempts}: {delay:?} > {un_jittered:?}"
                );
            }
        }
        // Uncapped growth doubles each attempt (jitter only shrinks it).
        let uncapped = JobRetryBackoff::Exponential { max: None };
        assert!(uncapped.next_delay(base, 4) <= base * 8);
        // Huge attempt counts must not overflow.
        assert!(uncapped.next_delay(MAX_DURATION, u32::MAX) <= MAX_DURATION);
    }

    #[test]
    fn exponential_bound_keeps_growing_past_u32_multiplier_range() {
        let base = Duration::from_millis(1);
        assert_eq!(
            exponential_delay_bound(base, 34, None),
            Duration::from_millis(1u64 << 33)
        );
        assert_eq!(exponential_delay_bound(base, u32::MAX, None), MAX_DURATION);
        assert_eq!(
            exponential_delay_bound(base, 34, Some(Duration::from_secs(2))),
            Duration::from_secs(2)
        );
    }

    #[test]
    fn job_config_defaults_match_documented_values() {
        let cfg = JobConfig::default();
        assert_eq!(cfg.max_attempts, 1);
        assert_eq!(cfg.timeout, Some(Duration::from_secs(10)));
        assert_eq!(cfg.heartbeat, None);
        assert_eq!(cfg.retention, JobRetention::For(Duration::from_secs(600)));
        assert_eq!(cfg.retry_delay, Duration::ZERO);
        assert_eq!(cfg.backoff, JobRetryBackoff::None);
        assert_eq!(cfg.priority, 0);
    }

    #[test]
    fn job_config_rejects_unrepresentable_values() {
        let config = JobConfig {
            max_attempts: 0,
            ..JobConfig::default()
        };
        assert!(config.validate().is_err());
        let config = JobConfig {
            max_attempts: i32::MAX as u32,
            ..JobConfig::default()
        };
        assert!(config.validate().is_err());
        let config = JobConfig {
            heartbeat: Some(Duration::ZERO),
            ..JobConfig::default()
        };
        assert!(config.validate().is_err());
        let config = JobConfig {
            timeout: Some(Duration::ZERO),
            ..JobConfig::default()
        };
        assert!(config.validate().is_err());
        let config = JobConfig {
            timeout: Some(Duration::MAX),
            ..JobConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn huge_backoff_durations_fail_instead_of_wrapping() {
        let error = serde_json::to_value(JobRetryBackoff::Exponential {
            max: Some(Duration::MAX),
        })
        .unwrap_err();
        assert!(error.to_string().contains("does not fit"), "{error}");
    }

    #[test]
    fn duration_to_ms_saturates() {
        assert_eq!(duration_to_ms(Duration::from_secs(2)), 2000);
        assert_eq!(duration_to_ms(Duration::from_nanos(1)), 1);
        assert_eq!(duration_to_ms(Duration::from_micros(1_500)), 2);
        assert_eq!(duration_to_ms(Duration::MAX), i64::MAX);
    }
}

/// The reason a single job attempt did not complete successfully.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobErrorKind {
    /// The handler returned an error.
    Failed,
    /// The attempt exceeded the job's `timeout`.
    Timeout,
    /// The job was aborted (by a user, the sweeper, or worker shutdown).
    Aborted,
    /// The handler panicked.
    Panic,
    /// A context extractor failed (e.g. missing `JobState<T>`).
    Extract,
    /// The stored payload could not be deserialized into the handler's type.
    Decode,
}

impl JobErrorKind {
    const ALL: [Self; 6] = [
        Self::Failed,
        Self::Timeout,
        Self::Aborted,
        Self::Panic,
        Self::Extract,
        Self::Decode,
    ];

    fn as_str(self) -> &'static str {
        match self {
            Self::Failed => "failed",
            Self::Timeout => "timeout",
            Self::Aborted => "aborted",
            Self::Panic => "panic",
            Self::Extract => "extract",
            Self::Decode => "decode",
        }
    }

    /// Whether a later attempt could plausibly succeed. Decode and extract
    /// failures are deterministic — the stored payload and the worker's
    /// registrations do not change between attempts — so retrying them only
    /// burns the job's backoff schedule.
    pub(crate) fn retryable(self) -> bool {
        !matches!(self, Self::Decode | Self::Extract)
    }
}

/// The outcome of a failed job attempt, stored in the job's `error` column.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, serde::Serialize, serde::Deserialize)]
#[error("{kind}: {message}", kind = self.kind.as_str())]
pub struct JobError {
    /// What category of failure occurred.
    pub kind: JobErrorKind,
    /// Human-readable detail (error display, panic message, ...).
    pub message: String,
}

impl JobError {
    /// A handler failure, from anything displayable (the common case).
    pub fn failed(err: impl std::fmt::Display) -> Self {
        Self::new(JobErrorKind::Failed, err)
    }

    /// Builds a [`JobError`] of the given kind.
    pub fn new(kind: JobErrorKind, err: impl std::fmt::Display) -> Self {
        Self {
            kind,
            message: err.to_string(),
        }
    }

    /// Reconstructs a [`JobError`] from the `error` column (the inverse of
    /// its `Display`). Unrecognized text becomes a plain `Failed` error.
    pub(crate) fn from_stored(text: &str) -> Self {
        for kind in JobErrorKind::ALL {
            let prefix = kind.as_str();
            if let Some(message) = text
                .strip_prefix(prefix)
                .and_then(|message| message.strip_prefix(": "))
            {
                return Self {
                    kind,
                    message: message.to_string(),
                };
            }
        }
        Self {
            kind: JobErrorKind::Failed,
            message: text.to_string(),
        }
    }
}

#[cfg(test)]
mod job_error_tests {
    use super::*;

    #[test]
    fn job_error_display_includes_kind_and_message() {
        let err = JobError::failed("boom");
        assert_eq!(err.to_string(), "failed: boom");
        let err = JobError::new(JobErrorKind::Timeout, "10s elapsed");
        assert_eq!(err.to_string(), "timeout: 10s elapsed");
        let err = JobError::new(JobErrorKind::Aborted, "user");
        assert_eq!(err.to_string(), "aborted: user");
        let err = JobError::new(JobErrorKind::Panic, "oops");
        assert_eq!(err.to_string(), "panic: oops");
        let err = JobError::new(JobErrorKind::Extract, "missing state");
        assert_eq!(err.to_string(), "extract: missing state");
        let err = JobError::new(JobErrorKind::Decode, "bad json");
        assert_eq!(err.to_string(), "decode: bad json");
    }

    #[test]
    fn job_error_round_trips_through_the_error_column() {
        for kind in JobErrorKind::ALL {
            let original = JobError::new(kind, "some detail");
            assert_eq!(JobError::from_stored(&original.to_string()), original);
        }
        // Unrecognized text (e.g. "swept", "cancelled") becomes Failed.
        let swept = JobError::from_stored("swept");
        assert_eq!(swept.kind, JobErrorKind::Failed);
        assert_eq!(swept.message, "swept");
    }

    #[test]
    fn job_error_round_trips_through_json() {
        let err = JobError::new(JobErrorKind::Timeout, "slow");
        let json = serde_json::to_string(&err).unwrap();
        let back: JobError = serde_json::from_str(&json).unwrap();
        assert_eq!(err, back);
    }
}

/// Filter for [`Queue::jobs_page`](Queue::jobs_page).
#[derive(Debug, Clone, Default)]
pub struct JobFilter {
    /// Only jobs with this status.
    pub status: Option<JobStatus>,
    /// Only jobs with this handler name.
    pub name: Option<String>,
    /// Page size (default 50, maximum 1000).
    pub limit: Option<i64>,
    /// Return rows older than this cursor.
    pub before: Option<JobCursor>,
}

impl JobFilter {
    pub(crate) fn limit(&self) -> Result<i64, Error> {
        let limit = self.limit.unwrap_or(50);
        if !(1..=1000).contains(&limit) {
            return Err(Error::Config(
                "job page limit must be between 1 and 1000".into(),
            ));
        }
        Ok(limit)
    }
}

/// Stable cursor for newest-first job pagination.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct JobCursor {
    /// Enqueue timestamp of the last row in the previous page.
    pub enqueued_at: DateTime<Utc>,
    /// Job id used to make the timestamp ordering deterministic.
    pub id: Uuid,
}

impl From<&JobRow> for JobCursor {
    fn from(job: &JobRow) -> Self {
        Self {
            enqueued_at: job.enqueued_at,
            id: job.id,
        }
    }
}

/// Lifecycle state of a job.
///
/// `Queued -> Running -> {Complete, Failed, Aborted}`, with retries moving a
/// job back to `Queued` and aborts of running jobs passing through `Aborting`.
///
/// ```
/// assert_eq!(pgqueue::JobStatus::Running.as_str(), "running");
/// ```
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize, sqlx::Type,
)]
#[serde(rename_all = "lowercase")]
#[sqlx(type_name = "text", rename_all = "lowercase")]
pub enum JobStatus {
    /// Waiting to be picked up (possibly scheduled in the future).
    Queued,
    /// Currently running on a worker.
    Running,
    /// Abort requested while running; the worker will cancel it.
    Aborting,
    /// Finished successfully (terminal).
    Complete,
    /// Exhausted its attempts with an error (terminal).
    Failed,
    /// Aborted before completion (terminal).
    Aborted,
}

impl JobStatus {
    /// The lowercase string stored in the database.
    pub fn as_str(self) -> &'static str {
        match self {
            JobStatus::Queued => "queued",
            JobStatus::Running => "running",
            JobStatus::Aborting => "aborting",
            JobStatus::Complete => "complete",
            JobStatus::Failed => "failed",
            JobStatus::Aborted => "aborted",
        }
    }

    /// Whether this status is terminal (`complete`, `failed`, or `aborted`).
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            JobStatus::Complete | JobStatus::Failed | JobStatus::Aborted
        )
    }
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for JobStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "queued" => Ok(JobStatus::Queued),
            "running" => Ok(JobStatus::Running),
            "aborting" => Ok(JobStatus::Aborting),
            "complete" => Ok(JobStatus::Complete),
            "failed" => Ok(JobStatus::Failed),
            "aborted" => Ok(JobStatus::Aborted),
            other => Err(format!("unknown job status: {other}")),
        }
    }
}

/// A fully-typed snapshot of one row in the jobs table.
#[derive(Debug, Clone, serde::Serialize)]
pub struct JobRow {
    /// Primary key (UUIDv7, time-ordered).
    pub id: Uuid,
    /// Dedupe identity; `None` = no dedupe.
    pub unique_key: Option<String>,
    /// Queue name.
    pub queue: String,
    /// Registered handler name.
    pub name: String,
    /// JSON payload.
    pub payload: Value,
    /// Current lifecycle state.
    pub status: JobStatus,
    /// Dequeue priority; lower first.
    pub priority: i16,
    /// Concurrency group; at most one running job per group.
    pub group_key: Option<String>,
    /// Attempts made so far (incremented at dequeue).
    pub attempts: i32,
    /// Maximum attempts allowed.
    pub max_attempts: i32,
    /// Per-attempt timeout in milliseconds.
    pub timeout_ms: Option<i64>,
    /// Heartbeat interval in milliseconds.
    pub heartbeat_ms: Option<i64>,
    /// Base retry delay in milliseconds.
    pub retry_delay_ms: i64,
    /// Retry backoff strategy.
    pub backoff: JobRetryBackoff,
    /// Result retention in milliseconds (`NULL` forever, `0` delete now).
    pub ttl_ms: Option<i64>,
    /// Earliest execution time.
    pub scheduled_at: DateTime<Utc>,
    /// When the job was enqueued.
    pub enqueued_at: DateTime<Utc>,
    /// When the current/last attempt started.
    pub started_at: Option<DateTime<Utc>>,
    /// Last heartbeat (set at dequeue, updated by `touch()`).
    pub touched_at: Option<DateTime<Utc>>,
    /// When the job reached a terminal status.
    pub completed_at: Option<DateTime<Utc>>,
    /// When the sweeper may purge this terminal row.
    pub expires_at: Option<DateTime<Utc>>,
    /// Serialized handler return value (terminal, successful jobs).
    pub result: Option<Value>,
    /// Last error recorded for this job.
    pub error: Option<String>,
    /// Arbitrary user metadata.
    pub meta: Value,
    /// Worker currently/last processing this job.
    pub worker_id: Option<Uuid>,
}

impl JobRow {
    /// Whether the job has attempts remaining (`max_attempts > attempts`).
    pub fn retryable(&self) -> bool {
        self.max_attempts > self.attempts
    }

    /// Per-attempt timeout as a [`Duration`].
    pub fn timeout(&self) -> Option<Duration> {
        self.timeout_ms
            .map(|ms| Duration::from_millis(ms.max(0) as u64))
    }

    /// Heartbeat interval as a [`Duration`].
    pub fn heartbeat(&self) -> Option<Duration> {
        self.heartbeat_ms
            .map(|ms| Duration::from_millis(ms.max(0) as u64))
    }

    /// Result retention policy.
    pub fn retention(&self) -> JobRetention {
        JobRetention::from_ttl_ms(self.ttl_ms)
    }

    /// Delay before the next retry attempt, applying this job's backoff.
    pub(crate) fn next_retry_delay(&self) -> Duration {
        let base = Duration::from_millis(self.retry_delay_ms.max(0) as u64);
        self.backoff.next_delay(base, self.attempts.max(0) as u32)
    }
}

#[cfg(test)]
mod job_status_tests {
    use super::*;

    #[test]
    fn status_round_trips_and_classifies() {
        for status in [
            JobStatus::Queued,
            JobStatus::Running,
            JobStatus::Aborting,
            JobStatus::Complete,
            JobStatus::Failed,
            JobStatus::Aborted,
        ] {
            assert_eq!(status.as_str().parse::<JobStatus>().unwrap(), status);
            assert_eq!(status.to_string(), status.as_str());
        }
        assert!("bogus".parse::<JobStatus>().is_err());
        assert!(JobStatus::Complete.is_terminal());
        assert!(JobStatus::Failed.is_terminal());
        assert!(JobStatus::Aborted.is_terminal());
        assert!(!JobStatus::Queued.is_terminal());
        assert!(!JobStatus::Running.is_terminal());
        assert!(!JobStatus::Aborting.is_terminal());
    }
}

#[derive(Default)]
pub(crate) struct JobStateMap {
    values: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl JobStateMap {
    pub(crate) fn insert<T: Clone + Send + Sync + 'static>(&mut self, value: T) {
        self.values.insert(TypeId::of::<T>(), Box::new(value));
    }

    fn get<T: Clone + Send + Sync + 'static>(&self) -> Option<&T> {
        self.values.get(&TypeId::of::<T>())?.downcast_ref()
    }
}

impl std::fmt::Debug for JobStateMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobStateMap")
            .field("len", &self.values.len())
            .finish()
    }
}

/// Extractor for shared worker state registered via [`crate::WorkerBuilder::state`].
///
/// `JobState<Mailer>` resolves to a clone of the `Mailer` the worker was built
/// with. A missing value fails the job attempt with an extraction error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JobState<T>(pub T);

/// Everything a running job can see: its row snapshot, the queue, shared
/// worker state, and a cancellation token that fires on abort or shutdown.
///
/// Cheap to clone. Extract it by adding a `ctx: JobContext` parameter to a
/// `#[pgqueue::job]` function.
#[derive(Clone)]
pub struct JobContext {
    inner: Arc<JobContextInner>,
}

struct JobContextInner {
    queue: Queue,
    job: JobRow,
    worker_id: Uuid,
    state: Arc<JobStateMap>,
    cancel: CancellationToken,
}

impl JobContext {
    pub(crate) fn new(
        queue: Queue,
        job: JobRow,
        worker_id: Uuid,
        state: Arc<JobStateMap>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            inner: Arc::new(JobContextInner {
                queue,
                job,
                worker_id,
                state,
                cancel,
            }),
        }
    }

    /// Snapshot of this job's row as it was dequeued.
    pub fn job(&self) -> &JobRow {
        &self.inner.job
    }

    /// The current attempt number (1 on the first run).
    pub fn attempt(&self) -> u32 {
        self.inner.job.attempts.max(0) as u32
    }

    /// The id of the worker processing this job.
    pub fn worker_id(&self) -> Uuid {
        self.inner.worker_id
    }

    /// The queue this job came from (enqueue follow-up jobs through it).
    pub fn queue(&self) -> &Queue {
        &self.inner.queue
    }

    /// A token cancelled when the job is aborted or the worker shuts down.
    /// Long-running handlers should `select!` on it at natural pause points.
    /// Cancelling the returned token does not cancel the job attempt.
    pub fn cancellation(&self) -> CancellationToken {
        self.inner.cancel.child_token()
    }

    /// Records a heartbeat. Jobs configured with `heartbeat_ms`
    /// must call this at least that often or the sweeper will consider them
    /// stuck.
    pub async fn touch(&self) -> Result<(), Error> {
        self.inner.queue.touch_attempt(&self.inner.job).await
    }
}

impl std::fmt::Debug for JobContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobContext")
            .field("job", &self.inner.job.id)
            .field("name", &self.inner.job.name)
            .field("worker_id", &self.inner.worker_id)
            .finish_non_exhaustive()
    }
}

/// Types that can be extracted from a [`JobContext`] — the trait behind every
/// `#[pgqueue::job]` function parameter after the payload.
pub trait FromJobContext: Sized + Send {
    /// Extracts `Self`, or fails the attempt with a
    /// [`JobErrorKind::Extract`] error.
    fn from_context(ctx: &JobContext) -> Result<Self, JobError>;
}

impl FromJobContext for JobContext {
    fn from_context(ctx: &JobContext) -> Result<Self, JobError> {
        Ok(ctx.clone())
    }
}

impl<T: Clone + Send + Sync + 'static> FromJobContext for JobState<T> {
    fn from_context(ctx: &JobContext) -> Result<Self, JobError> {
        ctx.inner
            .state
            .get::<T>()
            .cloned()
            .map(JobState)
            .ok_or_else(|| {
                JobError::new(
                    JobErrorKind::Extract,
                    format!(
                        "no state of type `{}` registered on this worker (WorkerBuilder::state)",
                        std::any::type_name::<T>()
                    ),
                )
            })
    }
}

#[cfg(test)]
mod context_tests {
    use super::*;

    #[test]
    fn state_map_indexes_values_by_type() {
        let mut state = JobStateMap::default();
        assert!(state.get::<String>().is_none());
        state.insert("hello".to_string());
        state.insert(42u32);
        assert_eq!(state.get::<String>().map(String::as_str), Some("hello"));
        assert_eq!(state.get::<u32>(), Some(&42));
        state.insert("world".to_string());
        assert_eq!(state.get::<String>().map(String::as_str), Some("world"));
        assert!(format!("{state:?}").contains("len"));
    }
}

/// A job type generated by the `#[pgqueue::job]` attribute macro.
///
/// You never implement this by hand: annotate an `async fn` and the macro
/// produces a unit struct implementing it, plus a typed `::job(args)` enqueue
/// constructor and a `::call(...)` test helper.
pub trait JobType: Copy + Send + Sync + 'static {
    /// The payload: the first parameter of the annotated function.
    type Args: Serialize + DeserializeOwned + Send + 'static;
    /// The success value: the `Ok` side of the function's return type.
    type Output: Serialize + DeserializeOwned + Send + 'static;

    /// The registry/database name of this job.
    const NAME: &'static str;

    /// The cron schedule this job runs on, if it was defined with
    /// `#[pgqueue::cron]`. `None` for ordinary jobs. Workers automatically
    /// schedule jobs with a `SCHEDULE` when they are registered.
    const SCHEDULE: Option<&'static str> = None;

    /// Monotonic revision for a compile-time cron schedule. Increase it when
    /// changing the schedule or canonical job template in a rolling deploy.
    /// A template-only revision preserves the durable schedule cursor; changing
    /// the expression starts at its next UTC occurrence.
    const CRON_REVISION: u64 = 0;

    /// The configuration from the attribute arguments (`max_attempts`,
    /// `timeout_ms`, and related options).
    fn config() -> JobConfig;

    /// The type-erased handler stored in the worker registry.
    fn erased() -> TypeErasedJobHandler;
}

/// How a durable cron schedule handles an occurrence missed while no current
/// scheduler was able to publish it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CronMisfirePolicy {
    /// Skip stale occurrences. `None` preserves the adaptive default of one
    /// fifth of the schedule period, clamped to 1..=60 seconds. An explicit
    /// grace is always capped by the next occurrence.
    Skip { grace: Option<Duration> },
    /// Publish only the most recent missed occurrence, provided its successor
    /// is still in the future.
    FireOnce,
}

impl Default for CronMisfirePolicy {
    fn default() -> Self {
        Self::Skip { grace: None }
    }
}

impl CronMisfirePolicy {
    pub(crate) fn validate(self) -> Result<(), Error> {
        if let Self::Skip { grace: Some(grace) } = self {
            validate_duration("cron misfire grace", grace)?;
        }
        Ok(())
    }

    pub(crate) fn kind(self) -> &'static str {
        match self {
            Self::Skip { .. } => "skip",
            Self::FireOnce => "fire_once",
        }
    }

    pub(crate) fn grace_ms(self) -> Option<i64> {
        match self {
            Self::Skip { grace } => grace.map(duration_to_ms),
            Self::FireOnce => None,
        }
    }
}

/// Durable cron registration options.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CronOptions {
    /// Monotonically increasing definition revision. Higher revisions replace
    /// lower ones; changing a schedule without increasing it is rejected. A
    /// template-only revision preserves the durable cursor, while changing the
    /// expression starts at its next UTC occurrence.
    pub revision: u64,
    /// Missed-occurrence behavior.
    pub misfire: CronMisfirePolicy,
}

/// Boxed future returned by an erased handler.
pub type JobHandlerFuture = Pin<Box<dyn Future<Output = Result<Value, JobError>> + Send>>;

type JobHandlerFn = dyn Fn(Value, JobContext) -> JobHandlerFuture + Send + Sync;

/// Normalizes `#[pgqueue::job]` return types into a serializable result.
///
/// Implemented for `Result<T: Serialize, E: Display + 'static>` (the
/// idiomatic form, including `anyhow::Result<T>`) and for `()` (infallible
/// jobs). A returned [`JobError`] keeps its original [`JobErrorKind`].
pub trait IntoJobResult {
    /// The success value stored in the job's `result` column.
    type Output: Serialize + DeserializeOwned + Send + 'static;

    /// Converts the handler return value into the attempt outcome.
    fn into_job_result(self) -> Result<Self::Output, JobError>;
}

impl<T, E> IntoJobResult for Result<T, E>
where
    T: Serialize + DeserializeOwned + Send + 'static,
    E: std::fmt::Display + 'static,
{
    type Output = T;

    fn into_job_result(self) -> Result<T, JobError> {
        self.map_err(|error| {
            let error_any = &error as &dyn std::any::Any;
            error_any
                .downcast_ref::<JobError>()
                .cloned()
                .unwrap_or_else(|| JobError::failed(error))
        })
    }
}

impl IntoJobResult for () {
    type Output = ();

    fn into_job_result(self) -> Result<(), JobError> {
        Ok(())
    }
}

/// A type-erased job handler: decodes the JSON payload, extracts context
/// parameters, runs the user function, and encodes the result.
#[derive(Clone)]
pub struct TypeErasedJobHandler {
    name: &'static str,
    config: JobConfig,
    call: Arc<JobHandlerFn>,
}

impl TypeErasedJobHandler {
    /// Wraps the macro-generated closure for job type `J`.
    pub fn new<J: JobType>(
        call: impl Fn(Value, JobContext) -> JobHandlerFuture + Send + Sync + 'static,
    ) -> Self {
        Self {
            name: J::NAME,
            config: J::config(),
            call: Arc::new(call),
        }
    }

    /// The registry name.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// The job's compile-time configuration.
    pub fn config(&self) -> &JobConfig {
        &self.config
    }

    /// Invokes the handler.
    pub(crate) fn call(&self, payload: Value, ctx: JobContext) -> JobHandlerFuture {
        (self.call)(payload, ctx)
    }
}

impl std::fmt::Debug for TypeErasedJobHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypeErasedJobHandler")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod handler_tests {
    use super::*;

    #[derive(Clone, Copy)]
    struct Noop;

    impl JobType for Noop {
        type Args = ();
        type Output = ();
        const NAME: &'static str = "noop";

        fn config() -> JobConfig {
            JobConfig::default()
        }

        fn erased() -> TypeErasedJobHandler {
            TypeErasedJobHandler::new::<Self>(|_payload, _ctx| Box::pin(async { Ok(Value::Null) }))
        }
    }

    #[test]
    fn erased_handler_exposes_name_and_config() {
        let handler = Noop::erased();
        assert_eq!(handler.name(), "noop");
        assert_eq!(*handler.config(), JobConfig::default());
        assert!(format!("{handler:?}").contains("noop"));
    }

    #[test]
    fn job_results_normalize_successes_and_failures() {
        let ok: Result<u32, std::io::Error> = Ok(7);
        assert_eq!(ok.into_job_result().unwrap(), 7);

        let err: Result<u32, String> = Err("boom".to_string());
        let job_err = err.into_job_result().unwrap_err();
        assert_eq!(job_err.kind, JobErrorKind::Failed);
        assert_eq!(job_err.message, "boom");

        let err: Result<u32, JobError> =
            Err(JobError::new(JobErrorKind::Decode, "invalid payload"));
        let job_err = err.into_job_result().unwrap_err();
        assert_eq!(job_err.kind, JobErrorKind::Decode);
        assert_eq!(job_err.message, "invalid payload");
        assert!(().into_job_result().is_ok());
    }
}

/// A registered cron job: a parsed schedule plus the job template to enqueue.
pub(crate) struct JobCronEntry {
    pub cron: Cron,
    /// The source expression stored with scheduled occurrences.
    pub expr: String,
    /// The dedupe key every occurrence fires under (also set on the template).
    pub unique_key: String,
    pub template: JobRequest,
    pub options: CronOptions,
    pub definition: Value,
}

/// Parses a cron expression: standard 5-field, with an optional leading
/// seconds field (6 fields) for sub-minute schedules.
pub(crate) fn parse_cron(expr: &str) -> Result<Cron, Error> {
    CronParser::builder()
        .seconds(Seconds::Optional)
        .build()
        .parse(expr)
        .map_err(|e| Error::Config(format!("invalid cron expression {expr:?}: {e}")))
}

impl JobCronEntry {
    /// Builds an entry, defaulting the dedupe key to `cron:{name}`.
    #[cfg(test)]
    pub(crate) fn new(expr: &str, template: JobRequest) -> Result<Self, Error> {
        Self::with_options(expr, template, CronOptions::default())
    }

    pub(crate) fn with_options(
        expr: &str,
        mut template: JobRequest,
        options: CronOptions,
    ) -> Result<Self, Error> {
        let cron = parse_cron(expr)?;
        options.misfire.validate()?;
        i64::try_from(options.revision)
            .map_err(|_| Error::Config("cron revision must fit PostgreSQL bigint".into()))?;
        let unique_key = template
            .unique_key
            .clone()
            .unwrap_or_else(|| format!("cron:{}", template.name));
        template.unique_key = Some(unique_key.clone());
        template.validate()?;
        let definition = serde_json::json!({
            "payload": template.payload.clone(),
            "max_attempts": template.config.max_attempts,
            "timeout_ms": template.config.timeout.map(duration_to_ms),
            "heartbeat_ms": template.config.heartbeat.map(duration_to_ms),
            "ttl_ms": template.config.retention.as_ttl_ms(),
            "retry_delay_ms": duration_to_ms(template.config.retry_delay),
            "backoff": template.config.backoff,
            "priority": template.config.priority,
            "group_key": template.group_key.clone(),
            "meta": template.meta.clone(),
        });
        Ok(Self {
            cron,
            expr: expr.to_string(),
            unique_key,
            template,
            options,
            definition,
        })
    }

    /// The next fire time strictly after `now`.
    pub(crate) fn next_occurrence(&self, now: DateTime<Utc>) -> Result<DateTime<Utc>, Error> {
        // The cron parser carries `now`'s sub-second component into its result, but a
        // cron occurrence is a whole-second instant. Truncate so every worker
        // and every tick computes the identical timestamp for an occurrence —
        // the schedule dedupe compares these values for equality.
        self.cron
            .find_next_occurrence(&chrono::SubsecRound::trunc_subsecs(now, 0), false)
            .map_err(|e| Error::Config(format!("cron occurrence: {e}")))
    }

    pub(crate) fn previous_occurrence(&self, now: DateTime<Utc>) -> Result<DateTime<Utc>, Error> {
        self.cron
            .find_previous_occurrence(&chrono::SubsecRound::trunc_subsecs(now, 0), true)
            .map_err(|e| Error::Config(format!("cron occurrence: {e}")))
    }

    pub(crate) fn publication_deadline(
        &self,
        occurrence: DateTime<Utc>,
        successor: DateTime<Utc>,
    ) -> DateTime<Utc> {
        let grace = match self.options.misfire {
            CronMisfirePolicy::Skip { grace: Some(grace) } => {
                chrono::Duration::try_milliseconds(duration_to_ms(grace))
                    .unwrap_or(chrono::Duration::MAX)
            }
            CronMisfirePolicy::Skip { grace: None } => ((successor - occurrence) / 5)
                .clamp(chrono::Duration::seconds(1), chrono::Duration::seconds(60)),
            CronMisfirePolicy::FireOnce => successor - occurrence,
        };
        successor.min(occurrence.checked_add_signed(grace).unwrap_or(successor))
    }

    /// The job to enqueue for the occurrence at `at`.
    pub(crate) fn job_for(&self, at: DateTime<Utc>) -> JobRequest {
        let mut job = self.template.clone();
        job.scheduled_at = Some(at);
        job
    }
}

impl std::fmt::Debug for JobCronEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobCronEntry")
            .field("cron", &self.cron.to_string())
            .field("job", &self.template.name)
            .finish()
    }
}

#[cfg(test)]
mod cron_entry_tests {
    use super::*;

    #[test]
    fn next_occurrence_is_identical_when_now_has_subseconds() {
        let entry = JobCronEntry::new("0 0 * * *", JobRequest::new("tick", Value::Null)).unwrap();
        let base: DateTime<Utc> = "2026-07-18T23:38:17Z".parse().unwrap();
        let early = entry.next_occurrence(base).unwrap();
        let late = entry
            .next_occurrence(base + chrono::Duration::microseconds(545_375))
            .unwrap();
        assert_eq!(early, late);
        assert_eq!(
            early,
            "2026-07-19T00:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
    }

    #[test]
    fn publication_deadline_is_identical_when_graces_share_canonical_milliseconds() {
        let entry_with_grace = |grace| {
            JobCronEntry::with_options(
                "0 * * * *",
                JobRequest::new("tick", Value::Null),
                CronOptions {
                    misfire: CronMisfirePolicy::Skip { grace: Some(grace) },
                    ..CronOptions::default()
                },
            )
            .unwrap()
        };
        let submillisecond = entry_with_grace(Duration::from_micros(1_500));
        let milliseconds = entry_with_grace(Duration::from_millis(2));
        let occurrence: DateTime<Utc> = "2026-01-01T00:00:00Z".parse().unwrap();
        let successor: DateTime<Utc> = "2026-01-01T01:00:00Z".parse().unwrap();

        assert_eq!(
            submillisecond.options.misfire.grace_ms(),
            milliseconds.options.misfire.grace_ms()
        );
        assert_eq!(
            submillisecond.publication_deadline(occurrence, successor),
            milliseconds.publication_deadline(occurrence, successor)
        );
        assert_eq!(
            submillisecond.publication_deadline(occurrence, successor),
            occurrence + chrono::Duration::milliseconds(2)
        );
    }
}

#[cfg(test)]
mod cron_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_five_and_six_field_expressions() {
        assert!(parse_cron("*/5 * * * *").is_ok());
        assert!(parse_cron("30 */5 * * * *").is_ok());
        assert!(parse_cron("not a cron").is_err());
        assert!(parse_cron("99 * * * *").is_err());
    }

    #[test]
    fn entry_defaults_unique_key_and_schedules() {
        let entry =
            JobCronEntry::new("0 * * * *", JobRequest::new("cleanup", json!(null))).unwrap();
        assert_eq!(entry.unique_key, "cron:cleanup");
        assert_eq!(entry.template.unique_key.as_deref(), Some("cron:cleanup"));
        assert!(format!("{entry:?}").contains("cleanup"));

        let now = "2026-01-01T10:15:00Z".parse::<DateTime<Utc>>().unwrap();
        let next = entry.next_occurrence(now).unwrap();
        assert_eq!(
            next,
            "2026-01-01T11:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );

        let job = entry.job_for(next);
        assert_eq!(job.scheduled_at, Some(next));
        assert_eq!(job.name, "cleanup");
    }

    #[test]
    fn impossible_schedule_surfaces_an_error() {
        let entry = JobCronEntry::new("0 0 30 2 *", JobRequest::new("never", json!(null))).unwrap();
        let err = entry.next_occurrence(Utc::now()).unwrap_err();
        assert!(err.to_string().contains("cron occurrence"), "{err}");
    }

    #[test]
    fn explicit_unique_key_is_preserved() {
        let mut template = JobRequest::new("cleanup", json!(null));
        template.unique_key = Some("custom".into());
        let entry = JobCronEntry::new("0 * * * *", template).unwrap();
        assert_eq!(entry.unique_key, "custom");
        assert_eq!(entry.template.unique_key.as_deref(), Some("custom"));
    }

    #[test]
    fn derived_unique_key_is_validated() {
        let error = JobCronEntry::new("0 * * * *", JobRequest::new("x".repeat(251), json!(null)))
            .unwrap_err();
        assert!(error.to_string().contains("unique key"), "{error}");
    }
}

const MAX_INDEXED_KEY_BYTES: usize = 255;

/// An untyped enqueue request: the dynamic escape hatch under the typed
/// `JobBuilder` API, useful when the job name is only known at runtime.
#[derive(Debug, Clone)]
pub struct JobRequest {
    /// Registered handler name.
    pub name: String,
    /// JSON payload passed to the handler.
    pub payload: Value,
    /// Execution configuration.
    pub config: JobConfig,
    /// Dedupe identity (unique per queue among live rows), at most 255 bytes so
    /// it remains safe to store in PostgreSQL's B-tree index. Terminal
    /// occurrences retain the key for history and result lookup.
    pub unique_key: Option<String>,
    /// Earliest execution time; `None` = now.
    pub scheduled_at: Option<DateTime<Utc>>,
    /// At most one job per group runs at a time. Group keys are limited to 255
    /// bytes so activating a job cannot exceed PostgreSQL's B-tree entry limit.
    pub group_key: Option<String>,
    /// Arbitrary user metadata stored on the row.
    pub meta: Value,
}

impl JobRequest {
    /// A new request for `name` with the given payload and default config.
    pub fn new(name: impl Into<String>, payload: Value) -> Self {
        Self {
            name: name.into(),
            payload,
            config: JobConfig::default(),
            unique_key: None,
            scheduled_at: None,
            group_key: None,
            meta: Value::Object(serde_json::Map::new()),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        if self.name.is_empty() {
            return Err(Error::Config("job name must not be empty".into()));
        }
        if self.name.len() > 255 {
            return Err(Error::Config(
                "job name must not be longer than 255 bytes".into(),
            ));
        }
        for (field, value) in [
            ("job name", Some(self.name.as_str())),
            ("unique key", self.unique_key.as_deref()),
            ("group key", self.group_key.as_deref()),
        ] {
            if value.is_some_and(|value| value.contains('\0')) {
                return Err(Error::Config(format!("{field} must not contain NUL")));
            }
        }
        for (field, value) in [
            ("unique key", self.unique_key.as_deref()),
            ("group key", self.group_key.as_deref()),
        ] {
            if value.is_some_and(|value| value.len() > MAX_INDEXED_KEY_BYTES) {
                return Err(Error::Config(format!(
                    "{field} must not be longer than {MAX_INDEXED_KEY_BYTES} bytes"
                )));
            }
        }
        self.config.validate()
    }
}

/// A typed, not-yet-enqueued job: `my_job::job(args)` with optional per-call
/// overrides, consumed by [`Queue::enqueue`].
///
/// Defaults come from the job's `#[pgqueue::job(...)]` attribute; every
/// builder method overrides just this enqueue.
#[must_use = "a JobBuilder does nothing until passed to Queue::enqueue"]
pub struct JobBuilder<J: JobType> {
    args: J::Args,
    config: JobConfig,
    unique_key: Option<String>,
    scheduled_at: Option<DateTime<Utc>>,
    delay: Option<Duration>,
    group_key: Option<String>,
    meta: Value,
    _job: PhantomData<J>,
}

impl<J: JobType> JobBuilder<J> {
    /// Starts a builder from the job's compile-time configuration. Generated
    /// code calls this as `my_job::job(args)`.
    pub fn new(args: J::Args) -> Self {
        Self {
            args,
            config: J::config(),
            unique_key: None,
            scheduled_at: None,
            delay: None,
            group_key: None,
            meta: Value::Object(serde_json::Map::new()),
            _job: PhantomData,
        }
    }

    /// Dedupe identity: at most one live (non-terminal) job per
    /// `(queue, unique_key)`. Enqueueing a duplicate returns
    /// `Ok(EnqueueOutcome::Deduplicated(handle))`.
    pub fn unique_key(mut self, key: impl Into<String>) -> Self {
        self.unique_key = Some(key.into());
        self
    }

    /// Runs no earlier than the given time.
    pub fn at(mut self, when: DateTime<Utc>) -> Self {
        self.scheduled_at = Some(when);
        self.delay = None;
        self
    }

    /// Runs no earlier than `delay` from now.
    pub fn delay(mut self, delay: Duration) -> Self {
        self.scheduled_at = None;
        self.delay = Some(delay);
        self
    }

    /// Concurrency group: at most one job per group runs at a time.
    pub fn group_key(mut self, group: impl Into<String>) -> Self {
        self.group_key = Some(group.into());
        self
    }

    /// Overrides the dequeue priority (lower runs first).
    pub fn priority(mut self, priority: i16) -> Self {
        self.config.priority = priority;
        self
    }

    /// Overrides the maximum attempts allowed.
    pub fn max_attempts(mut self, max_attempts: u32) -> Self {
        self.config.max_attempts = max_attempts;
        self
    }

    /// Overrides the per-attempt timeout. Must be greater than zero.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.config.timeout = Some(timeout);
        self
    }

    /// Overrides the heartbeat interval.
    pub fn heartbeat(mut self, heartbeat: Duration) -> Self {
        self.config.heartbeat = Some(heartbeat);
        self
    }

    /// Overrides how long the finished row is retained.
    pub fn retention(mut self, retention: JobRetention) -> Self {
        self.config.retention = retention;
        self
    }

    /// Overrides the base retry delay.
    pub fn retry_delay(mut self, delay: Duration) -> Self {
        self.config.retry_delay = delay;
        self
    }

    /// Overrides the retry backoff strategy.
    pub fn backoff(mut self, backoff: JobRetryBackoff) -> Self {
        self.config.backoff = backoff;
        self
    }

    /// Attaches arbitrary JSON metadata to the row.
    pub fn meta(mut self, meta: Value) -> Self {
        self.meta = meta;
        self
    }

    /// Converts the builder into a cron template. Rejects `delay()`/`at()`
    /// instead of dropping them: the cron expression overwrites every
    /// occurrence's `scheduled_at`, so a scheduling override can never take
    /// effect.
    pub(crate) fn into_cron_template(self) -> Result<JobRequest, Error> {
        let (job, delay) = self.into_parts()?;
        if delay.is_some() || job.scheduled_at.is_some() {
            return Err(Error::Config(format!(
                "cron job {:?} cannot use delay() or at(): the cron expression schedules every occurrence",
                job.name
            )));
        }
        job.validate()?;
        Ok(job)
    }

    pub(crate) fn into_parts(self) -> Result<(JobRequest, Option<Duration>), Error> {
        let job = JobRequest {
            name: J::NAME.to_string(),
            payload: serde_json::to_value(&self.args)?,
            config: self.config,
            unique_key: self.unique_key,
            scheduled_at: self.scheduled_at,
            group_key: self.group_key,
            meta: self.meta,
        };
        Ok((job, self.delay))
    }

    fn has_unique_key(&self) -> bool {
        self.unique_key.is_some()
    }

    fn deletes_immediately(&self) -> bool {
        self.config.retention == JobRetention::DeleteImmediately
    }

    fn into_validated_parts(self) -> Result<(JobRequest, Option<Duration>), Error> {
        let (job, delay) = self.into_parts()?;
        job.validate()?;
        if let Some(delay) = delay {
            validate_duration("job delay", delay)?;
        }
        Ok((job, delay))
    }
}

/// Result of publishing a job with an optional unique key.
///
/// Both variants contain a handle. A deduplicated publish points at the live
/// job that already owns the key; it does not provide exactly-once execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnqueueOutcome<H> {
    /// A new job row was inserted.
    Enqueued(H),
    /// A live job already owned the request's unique key.
    Deduplicated(H),
}

impl<H> EnqueueOutcome<H> {
    /// Whether this publish inserted a new row.
    pub fn is_enqueued(&self) -> bool {
        matches!(self, Self::Enqueued(_))
    }

    /// Whether this publish reused a live row with the same unique key.
    pub fn is_deduplicated(&self) -> bool {
        matches!(self, Self::Deduplicated(_))
    }

    /// Borrows the new or existing job identity.
    pub fn handle(&self) -> &H {
        match self {
            Self::Enqueued(handle) | Self::Deduplicated(handle) => handle,
        }
    }

    /// Returns the new or existing job identity.
    pub fn into_handle(self) -> H {
        match self {
            Self::Enqueued(handle) | Self::Deduplicated(handle) => handle,
        }
    }
}

impl Queue {
    /// Enqueues a typed job: `queue.enqueue(my_job::job(args)).await?`.
    ///
    /// A unique-key collision returns [`EnqueueOutcome::Deduplicated`] with a
    /// typed handle to the existing job. It is an error when that row belongs
    /// to a different job type.
    pub async fn enqueue<J: JobType>(
        &self,
        job: JobBuilder<J>,
    ) -> Result<EnqueueOutcome<JobHandle<J>>, Error> {
        let (new_job, delay) = job.into_parts()?;
        let retention = new_job.config.retention;
        let outcome = self.enqueue_raw_delayed_outcome(new_job, delay).await?;
        typed_enqueue_outcome::<J>(self, outcome, retention)
    }

    /// Enqueues a typed job as part of a caller-owned PostgreSQL transaction.
    ///
    /// The job and its notification become visible only if the caller commits.
    /// Unique-key advisory locks remain held until that commit, so applications
    /// should acquire their own locks and publish unique jobs in a consistent
    /// order across transactions.
    pub async fn enqueue_in<J: JobType>(
        &self,
        transaction: &mut sqlx::PgTransaction<'_>,
        job: JobBuilder<J>,
    ) -> Result<EnqueueOutcome<JobHandle<J>>, Error> {
        let (new_job, delay) = job.into_parts()?;
        let retention = new_job.config.retention;
        let outcome = self
            .enqueue_raw_delayed_in_outcome(transaction, new_job, delay)
            .await?;
        typed_enqueue_outcome::<J>(self, outcome, retention)
    }

    /// Enqueues a job and waits for its typed result (request/response).
    ///
    /// If the builder carries a `unique_key` that deduplicates against a live
    /// job, `apply` waits on that existing job instead. Failures surface as
    /// [`Error::Job`]; `None` timeout waits forever.
    ///
    /// The job's retention must keep the row around long enough to read the
    /// result. `JobRetention::DeleteImmediately` is rejected before enqueue.
    pub async fn apply<J: JobType>(
        &self,
        job: JobBuilder<J>,
        timeout: Option<Duration>,
    ) -> Result<J::Output, Error> {
        let (new_job, delay) = job.into_parts()?;
        if new_job.config.retention == JobRetention::DeleteImmediately {
            return Err(Error::Config(
                "apply requires result retention; DeleteImmediately removes the result before it can be read"
                    .into(),
            ));
        }
        let retention = new_job.config.retention;
        let handle: JobHandle<J> = match self.enqueue_raw_delayed_outcome(new_job, delay).await? {
            DatabaseEnqueueOutcome::Inserted(id) => JobHandle::new(id, self.clone(), retention),
            DatabaseEnqueueOutcome::Deduplicated {
                id,
                name,
                retention,
            } => {
                if retention == JobRetention::DeleteImmediately {
                    return Err(Error::Config(
                        "apply cannot wait on the existing deduplicated job because it deletes its result immediately"
                            .into(),
                    ));
                }
                if name != J::NAME {
                    return Err(Error::Config(format!(
                        "unique key belongs to job {name:?}, not {:?}",
                        J::NAME
                    )));
                }
                JobHandle::new(id, self.clone(), retention)
            }
        };
        handle.wait(timeout).await
    }

    /// Enqueues a batch and waits for every result, preserving order.
    ///
    /// Per-job failures come back as `Err(JobError)` items; infrastructure
    /// problems (or the batch-wide `timeout`) fail the whole call.
    pub async fn map<J: JobType>(
        &self,
        jobs: Vec<JobBuilder<J>>,
        timeout: Option<Duration>,
    ) -> Result<Vec<Result<J::Output, JobError>>, Error> {
        if jobs.iter().any(JobBuilder::has_unique_key) {
            return Err(Error::Config("map does not support unique_key jobs".into()));
        }
        if jobs.iter().any(JobBuilder::deletes_immediately) {
            return Err(Error::Config(
                "map requires result retention; DeleteImmediately removes results before they can be read"
                    .into(),
            ));
        }
        let operation = async {
            let jobs = jobs
                .into_iter()
                .map(JobBuilder::into_validated_parts)
                .collect::<Result<Vec<_>, _>>()?;
            let mut transaction = self.pool().begin().await?;
            let mut handles = Vec::with_capacity(jobs.len());
            for (job, delay) in jobs {
                let retention = job.config.retention;
                let outcome = self
                    .enqueue_raw_delayed_in_outcome(&mut transaction, job, delay)
                    .await?;
                let handle = match outcome {
                    DatabaseEnqueueOutcome::Inserted(id) => {
                        JobHandle::<J>::new(id, self.clone(), retention)
                    }
                    _ => {
                        return Err(Error::Config(
                            "map enqueue was unexpectedly deduplicated".into(),
                        ));
                    }
                };
                handles.push(handle);
            }
            transaction.commit().await?;

            const MAX_WAITERS: usize = 64;
            let mut waiters = tokio::task::JoinSet::new();
            let result_len = handles.len();
            let mut handles = handles.into_iter().enumerate();
            for (index, handle) in handles.by_ref().take(MAX_WAITERS) {
                waiters.spawn(async move { (index, handle.wait(None).await) });
            }
            let mut results = Vec::new();
            results.resize_with(result_len, || None);
            while let Some(waiter) = waiters.join_next().await {
                let (index, result) = waiter?;
                results[index] = Some(match result {
                    Ok(output) => Ok(output),
                    Err(Error::Job(job_error)) => Err(job_error),
                    Err(other) => return Err(other),
                });
                if let Some((next_index, handle)) = handles.next() {
                    waiters.spawn(async move { (next_index, handle.wait(None).await) });
                }
            }
            Ok(results.into_iter().flatten().collect())
        };
        match timeout {
            Some(t) => tokio::time::timeout(t, operation)
                .await
                .map_err(|_| Error::WaitTimeout)?,
            None => operation.await,
        }
    }
}

fn typed_enqueue_outcome<J: JobType>(
    queue: &Queue,
    outcome: DatabaseEnqueueOutcome,
    inserted_retention: JobRetention,
) -> Result<EnqueueOutcome<JobHandle<J>>, Error> {
    match outcome {
        DatabaseEnqueueOutcome::Inserted(id) => Ok(EnqueueOutcome::Enqueued(JobHandle::new(
            id,
            queue.clone(),
            inserted_retention,
        ))),
        DatabaseEnqueueOutcome::Deduplicated {
            id,
            name,
            retention,
        } => {
            if name != J::NAME {
                return Err(Error::Config(format!(
                    "unique key belongs to job {name:?}, not {:?}",
                    J::NAME
                )));
            }
            Ok(EnqueueOutcome::Deduplicated(JobHandle::new(
                id,
                queue.clone(),
                retention,
            )))
        }
    }
}

/// A reference to an enqueued job.
#[derive(Clone)]
pub struct JobHandle<J: JobType> {
    pub(crate) id: Uuid,
    pub(crate) queue: Queue,
    pub(super) retention: JobRetention,
    _job: PhantomData<fn() -> J>,
}

impl<J: JobType> JobHandle<J> {
    fn new(id: Uuid, queue: Queue, retention: JobRetention) -> Self {
        Self {
            id,
            queue,
            retention,
            _job: PhantomData,
        }
    }

    /// The job's id (UUIDv7).
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// Fetches the job's current row.
    pub async fn refresh(&self) -> Result<JobRow, Error> {
        self.queue
            .job(self.id)
            .await?
            .ok_or(Error::JobNotFound(self.id))
    }

    /// Requests an abort (see [`Queue::abort`]).
    pub async fn abort(&self, reason: &str) -> Result<bool, Error> {
        self.queue.abort(self.id, reason).await
    }

    /// Waits for the job to finish and deserializes its result.
    ///
    /// Resolution is push-based (the queue's completion NOTIFY channel) with
    /// a polling fallback, so results arrive promptly even if a notification
    /// is lost. Failures surface as [`Error::Job`]; `None` waits forever.
    /// Delete-immediately jobs have no durable result and cannot be waited on,
    /// except for a queued abort that is still present as a terminal row.
    pub async fn wait(&self, timeout: Option<Duration>) -> Result<J::Output, Error> {
        Ok(serde_json::from_value(self.wait_value(timeout).await?)?)
    }

    /// Like [`JobHandle::wait`] but returns the raw JSON result.
    pub async fn wait_value(&self, timeout: Option<Duration>) -> Result<Value, Error> {
        if self.retention == JobRetention::DeleteImmediately {
            // Queued aborts intentionally remain until sweep, so a caller that
            // already aborted may still read that terminal outcome. Running or
            // deleted rows cannot provide a reliable result.
            if let Some(row) = self.queue.job(self.id).await?
                && row.status.is_terminal()
            {
                return resolve(row);
            }
            return Err(Error::Config(
                "wait requires result retention; DeleteImmediately jobs have no durable result"
                    .into(),
            ));
        }
        match timeout {
            Some(t) => tokio::time::timeout(t, self.wait_inner())
                .await
                .map_err(|_| Error::WaitTimeout)?,
            None => self.wait_inner().await,
        }
    }

    async fn wait_inner(&self) -> Result<Value, Error> {
        // The fallback poll only matters when a notification was lost, so it
        // backs off: short waits stay snappy while long waits settle at the
        // maximum instead of hammering the pool (Queue::map spawns one waiter
        // per job) even though completions arrive push-based.
        const INITIAL_POLL_INTERVAL: Duration = Duration::from_millis(250);
        const MAX_POLL_INTERVAL: Duration = Duration::from_secs(2);

        // Subscribe before the first status check so a finish landing in
        // between can't be missed.
        let mut done = self.queue.notify_listener().await?.subscribe_done();
        let mut poll_interval = INITIAL_POLL_INTERVAL;
        'poll: loop {
            let row = self.queue.job(self.id).await?;
            let missing = match row {
                Some(row) if row.status.is_terminal() => return resolve(row),
                Some(_) => false,
                // A delete-immediately finish commits the row deletion and
                // NOTIFY atomically, but listener delivery can lag this read.
                // Give the already-subscribed receiver one poll interval to
                // observe that terminal event before declaring the ID absent.
                None => true,
            };
            let poll_deadline = tokio::time::sleep(poll_interval);
            poll_interval = (poll_interval * 2).min(MAX_POLL_INTERVAL);
            tokio::pin!(poll_deadline);
            loop {
                tokio::select! {
                    biased;
                    _ = &mut poll_deadline => {
                        if missing {
                            return Err(Error::JobNotFound(self.id));
                        }
                        continue 'poll;
                    },
                    event = done.recv() => match event {
                        // Fast path: our job finished. Re-fetch for its result;
                        // if retention already removed the row, resolve from
                        // the event alone.
                        Ok(event) if event.id == self.id => {
                            if let Some(row) = self.queue.job(self.id).await? {
                                if row.status.is_terminal() {
                                    return resolve(row);
                                }
                            } else {
                                return resolve_deleted(event);
                            }
                        }
                        // A foreign completion does not require a database read.
                        Ok(_) => continue,
                        // Lagged/closed channels retain the polling fallback.
                        Err(_) => {
                            poll_deadline.as_mut().await;
                            if missing {
                                return Err(Error::JobNotFound(self.id));
                            }
                            continue 'poll;
                        }
                    },
                }
            }
        }
    }
}

impl<J: JobType> std::fmt::Debug for JobHandle<J> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobHandle")
            .field("id", &self.id)
            .field("job", &J::NAME)
            .finish_non_exhaustive()
    }
}

fn resolve_deleted(event: QueueDoneEvent) -> Result<Value, Error> {
    match event.status {
        // The completed row was purged (retention expiry) between the
        // notification and the re-fetch: the result is unrecoverable, which
        // must not masquerade as a successful null result.
        JobStatus::Complete => Err(Error::ResultExpired(event.id)),
        JobStatus::Failed => Err(Error::Job(JobError::new(
            JobErrorKind::Failed,
            "job failed and was deleted",
        ))),
        JobStatus::Aborted | JobStatus::Aborting => Err(Error::Job(JobError::new(
            JobErrorKind::Aborted,
            "job was aborted and deleted",
        ))),
        JobStatus::Queued | JobStatus::Running => Err(Error::Config(format!(
            "job emitted a non-terminal {} completion event",
            event.status
        ))),
    }
}

fn resolve(row: JobRow) -> Result<Value, Error> {
    match row.status {
        JobStatus::Complete => Ok(row.result.unwrap_or(Value::Null)),
        // Aborts store the raw reason (e.g. "aborted from ui"), not a
        // JobError rendering — classify by status.
        JobStatus::Aborted | JobStatus::Aborting => Err(Error::Job(JobError::new(
            JobErrorKind::Aborted,
            row.error.as_deref().unwrap_or("aborted"),
        ))),
        _ => Err(Error::Job(
            row.error
                .as_deref()
                .map(JobError::from_stored)
                .unwrap_or_else(|| JobError::failed(format!("job {}", row.status))),
        )),
    }
}

#[cfg(test)]
mod api_tests {
    use super::*;

    #[test]
    fn new_job_uses_expected_defaults() {
        let job = JobRequest::new("send_email", serde_json::json!({"to": "a@b.c"}));
        assert_eq!(job.name, "send_email");
        assert_eq!(job.config, JobConfig::default());
        assert!(job.unique_key.is_none());
        assert!(job.scheduled_at.is_none());
        assert!(job.group_key.is_none());
        assert_eq!(job.meta, serde_json::json!({}));
    }

    #[test]
    fn new_job_rejects_oversized_indexed_keys() {
        for field in ["unique", "group"] {
            let mut job = JobRequest::new("bounded", Value::Null);
            if field == "unique" {
                job.unique_key = Some("x".repeat(MAX_INDEXED_KEY_BYTES + 1));
            } else {
                job.group_key = Some("x".repeat(MAX_INDEXED_KEY_BYTES + 1));
            }
            let error = job.validate().unwrap_err();
            assert!(error.to_string().contains("255 bytes"), "{error}");
        }
    }
}
