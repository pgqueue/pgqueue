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
use crate::database::DatabaseEnqueueResult;
use crate::queue::{Queue, QueueDoneEvent};

// One hundred years is beyond a useful queue delay while remaining safe for
// SQL date arithmetic and runtime clocks.
//
// `#[pgqueue::job]` emits a compile-time assertion against `MAX_DURATION_MS`
// (re-exported through `__private`), so lowering this bound turns an
// out-of-range attribute literal into a build failure rather than a runtime one.
#[doc(hidden)]
pub const MAX_DURATION_MS: u64 = 3_153_600_000_000;
const MAX_DURATION: Duration = Duration::from_millis(MAX_DURATION_MS);

/// PostgreSQL's `timestamptz` floor — `4714-11-24 00:00:00 BC` UTC — as a Unix
/// timestamp in seconds.
///
/// There is deliberately no matching ceiling: PostgreSQL's is 294277 AD, which
/// `DateTime<Utc>` cannot represent.
pub(crate) const MIN_TIMESTAMPTZ_SECONDS: i64 = -210_866_803_200;

pub(crate) fn validate_duration(field: &str, duration: Duration) -> Result<(), Error> {
    if duration > MAX_DURATION {
        return Err(Error::Config(format!(
            "{field} exceeds the maximum supported duration of {MAX_DURATION:?}"
        )));
    }
    Ok(())
}

/// [`validate_duration`] for fields where zero is meaningless rather than
/// "immediately", so every such field rejects it the same way.
pub(crate) fn validate_nonzero_duration(field: &str, duration: Duration) -> Result<(), Error> {
    if duration.is_zero() {
        return Err(Error::Config(format!("{field} must be greater than zero")));
    }
    validate_duration(field, duration)
}

/// Milliseconds for `duration`, rounding up, or `None` when it does not fit.
/// The rounding rule lives here so every conversion agrees; callers pick how to
/// handle a duration too large to represent.
pub(crate) fn duration_to_ms_checked(duration: Duration) -> Option<i64> {
    i64::try_from(duration.as_nanos().div_ceil(1_000_000)).ok()
}

pub(crate) fn duration_to_ms(duration: Duration) -> i64 {
    duration_to_ms_checked(duration).unwrap_or(i64::MAX)
}

/// Whether a job has attempts remaining. Shared by every row shape that
/// carries an attempt counter so the retry policy has one definition.
pub(crate) fn attempts_remaining(attempts: i32, max_attempts: i32) -> bool {
    max_attempts > attempts
}

/// Delay before the next retry, applying `backoff` to `retry_delay_ms`. Shared
/// so worker-driven and sweeper-driven retries can never diverge.
pub(crate) fn retry_delay_for(
    retry_delay_ms: i64,
    backoff: &JobRetryBackoff,
    attempts: i32,
) -> Duration {
    let base = Duration::from_millis(retry_delay_ms.max(0) as u64);
    backoff.next_delay(base, attempts.max(0) as u32)
}

/// How long a finished job's row (and result) is kept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobRetention {
    /// Keep the row for this long after it finishes, then the sweeper purges it.
    For(Duration),
    /// Keep the row forever. Reused dedupe keys and cron schedules retain one
    /// row per occurrence, so high-frequency recurring jobs should normally
    /// use a finite retention period.
    Forever,
    /// Delete the row as soon as a worker finishes it (no result retrieval).
    /// A queued job aborted before execution remains until the next sweep so
    /// waiters can observe its aborted result.
    DeleteImmediately,
}

impl JobRetention {
    /// Encoding for the `result_ttl_ms` column: `NULL` = forever, `0` = delete now.
    pub(crate) fn as_result_ttl_ms(self) -> Option<i64> {
        match self {
            JobRetention::For(d) => Some(duration_to_ms(d).max(1)),
            JobRetention::Forever => None,
            JobRetention::DeleteImmediately => Some(0),
        }
    }

    pub(crate) fn from_result_ttl_ms(result_ttl_ms: Option<i64>) -> Self {
        match result_ttl_ms {
            None => JobRetention::Forever,
            // A negative TTL has no encoding — the column now rejects one — but
            // decoding must not turn a row written by hand before that check
            // into a *live* zero-length retention, which is the one reading
            // that keeps the row instead of deleting it.
            Some(ms) if ms <= 0 => JobRetention::DeleteImmediately,
            Some(ms) => JobRetention::For(Duration::from_millis(ms as u64)),
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
    /// random duration in `[0, min(max, retry_delay * 2^(n-1))]`. This
    /// strategy requires a non-zero `retry_delay`.
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
    /// A strategy this build cannot read decodes as [`JobRetryBackoff::None`],
    /// the flat `retry_delay`, rather than failing the row.
    ///
    /// The `backoff` column now refuses one (see the migration), but decoding
    /// must not turn a row written by hand before that check — or by a newer
    /// version carrying a variant this build has never heard of — into an error
    /// that poisons its whole batch. The dequeue statement commits server-side
    /// (`running`, `attempts + 1`, `worker_id` set) *before* the client decodes
    /// what it returned, so a refusal here strands every healthy job claimed
    /// alongside the unreadable one, leaving them for a sweeper that re-claims
    /// the same batch and loses it again, burning an attempt each cycle. It
    /// also fails `Queue::jobs_page` and the dashboard listing for the whole
    /// queue — the two places an operator would look to find the bad row.
    fn decode(
        value: sqlx::postgres::PgValueRef<'r>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
        // `Json` borrows the raw bytes, so keep them for the warning: `Decode`
        // sees one column, not the row it belongs to, and the stored text is
        // the only thing that identifies which strategy was unreadable.
        //
        // Binary-format `jsonb` — what every query here uses — arrives with a
        // one-byte version header (currently 1) in front of the JSON text, and
        // `as_str` returns the wire bytes verbatim. `Json::decode` strips that
        // header before parsing; without the same strip the warning glued a
        // stray U+0001 to the front of the very string it exists to show. No
        // JSON text begins with a control character, so this can only ever take
        // the header off.
        let raw = value.as_str().unwrap_or("<binary>");
        let raw = raw.strip_prefix('\u{1}').unwrap_or(raw).to_owned();
        match <sqlx::types::Json<Self> as sqlx::Decode<sqlx::Postgres>>::decode(value) {
            Ok(json) => Ok(json.0),
            Err(error) => {
                tracing::warn!(
                    backoff = %raw,
                    %error,
                    "unreadable job backoff; retrying with the flat retry delay"
                );
                Ok(JobRetryBackoff::None)
            }
        }
    }
}

mod opt_duration_ms {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &Option<Duration>, s: S) -> Result<S::Ok, S::Error> {
        match v {
            Some(d) => {
                // Unlike `duration_to_ms`, a stored backoff cap must not
                // silently saturate: a value that cannot round-trip is an error.
                let millis = super::duration_to_ms_checked(*d)
                    .and_then(|ms| u64::try_from(ms).ok())
                    .ok_or_else(|| serde::ser::Error::custom("duration does not fit in u64 ms"))?;
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
            validate_nonzero_duration("job timeout", timeout)?;
        }
        if let JobRetention::For(ttl) = self.retention {
            validate_duration("job retention", ttl)?;
        }
        validate_duration("job retry delay", self.retry_delay)?;
        if let JobRetryBackoff::Exponential { max: Some(max) } = self.backoff {
            validate_nonzero_duration("job backoff maximum", max)?;
        }
        if matches!(self.backoff, JobRetryBackoff::Exponential { .. }) && self.retry_delay.is_zero()
        {
            return Err(Error::Config(
                "exponential job backoff requires a non-zero retry delay".into(),
            ));
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
    fn test_retention_maps_to_result_ttl_ms() {
        assert_eq!(JobRetention::Forever.as_result_ttl_ms(), None);
        assert_eq!(JobRetention::DeleteImmediately.as_result_ttl_ms(), Some(0));
        assert_eq!(
            JobRetention::For(Duration::from_secs(1)).as_result_ttl_ms(),
            Some(1000)
        );
        // Sub-millisecond retention still rounds up to 1ms (0 would mean delete).
        assert_eq!(
            JobRetention::For(Duration::from_micros(10)).as_result_ttl_ms(),
            Some(1)
        );

        assert_eq!(
            JobRetention::from_result_ttl_ms(None),
            JobRetention::Forever
        );
        assert_eq!(
            JobRetention::from_result_ttl_ms(Some(0)),
            JobRetention::DeleteImmediately
        );
        assert_eq!(
            JobRetention::from_result_ttl_ms(Some(1500)),
            JobRetention::For(Duration::from_millis(1500))
        );
    }

    #[test]
    fn test_backoff_serde_round_trip() {
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
    fn test_backoff_none_is_flat() {
        let d = Duration::from_millis(250);
        for attempts in [0, 1, 5, 100] {
            assert_eq!(JobRetryBackoff::None.next_delay(d, attempts), d);
        }
    }

    #[test]
    fn test_backoff_exponential_respects_bounds() {
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
    fn test_exponential_bound_keeps_growing_past_u32_multiplier_range() {
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
    fn test_job_config_defaults_match_documented_values() {
        let cfg = JobConfig::default();
        assert_eq!(cfg.max_attempts, 1);
        assert_eq!(cfg.timeout, Some(Duration::from_secs(10)));
        assert_eq!(cfg.retention, JobRetention::For(Duration::from_secs(600)));
        assert_eq!(cfg.retry_delay, Duration::ZERO);
        assert_eq!(cfg.backoff, JobRetryBackoff::None);
        assert_eq!(cfg.priority, 0);
    }

    #[test]
    fn test_job_config_rejects_unrepresentable_values() {
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
            timeout: Some(Duration::ZERO),
            ..JobConfig::default()
        };
        assert!(config.validate().is_err());
        let config = JobConfig {
            timeout: Some(Duration::MAX),
            ..JobConfig::default()
        };
        assert!(config.validate().is_err());
        let config = JobConfig {
            retry_delay: Duration::ZERO,
            backoff: JobRetryBackoff::Exponential { max: None },
            ..JobConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_huge_backoff_durations_fail_instead_of_wrapping() {
        let error = serde_json::to_value(JobRetryBackoff::Exponential {
            max: Some(Duration::MAX),
        })
        .unwrap_err();
        assert!(error.to_string().contains("does not fit"), "{error}");
    }

    #[test]
    fn test_duration_to_ms_saturates() {
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
    /// The payload could not be deserialized, or the handler result could not
    /// be serialized.
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

/// The result of a failed job attempt, stored in the job's `error` column.
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
    ///
    /// A NUL in the message becomes U+FFFD, so the substitution is visible
    /// rather than silent. This message is stored verbatim in the job's `error`
    /// column and PostgreSQL `text` cannot hold `\0` (`22021`) — an
    /// `anyhow::bail!("bad\0input")` would otherwise leave the attempt
    /// unfinalizable: every write of it fails, the worker retries the write
    /// forever, and the processor slot never comes back.
    pub fn new(kind: JobErrorKind, err: impl std::fmt::Display) -> Self {
        let message = err.to_string();
        Self {
            kind,
            message: if message.contains('\0') {
                message.replace('\0', "\u{fffd}")
            } else {
                message
            },
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
    fn test_job_error_display_includes_kind_and_message() {
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
    fn test_job_error_round_trips_through_the_error_column() {
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
    fn test_job_error_round_trips_through_json() {
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
#[derive(Debug, Clone, serde::Serialize, sqlx::FromRow)]
pub struct JobRow {
    /// Primary key (UUIDv7, time-ordered).
    pub id: Uuid,
    /// Dedupe identity; `None` = no dedupe.
    pub dedupe_key: Option<String>,
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
    /// Attempts made so far (incremented at dequeue).
    pub attempts: i32,
    /// Maximum attempts allowed.
    pub max_attempts: i32,
    /// Per-attempt timeout in milliseconds.
    pub timeout_ms: Option<i64>,
    /// Base retry delay in milliseconds.
    pub retry_delay_ms: i64,
    /// Retry backoff strategy.
    pub backoff: JobRetryBackoff,
    /// Result retention in milliseconds (`NULL` forever, `0` delete now).
    pub result_ttl_ms: Option<i64>,
    /// Earliest execution time.
    pub scheduled_at: DateTime<Utc>,
    /// When the job was enqueued.
    pub enqueued_at: DateTime<Utc>,
    /// When the current/last attempt started.
    pub started_at: Option<DateTime<Utc>>,
    /// Last lifecycle update for the current attempt.
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
        attempts_remaining(self.attempts, self.max_attempts)
    }

    /// Per-attempt timeout as a [`Duration`]; `None` = unlimited.
    ///
    /// Zero and negative are not encodings the column accepts, and
    /// `#[pgqueue::job(timeout_ms = 0)]` already means "no timeout", so a row
    /// written by hand before that check reads the same way here. Saturating to
    /// `Duration::ZERO` instead picked the one reading that cancels every
    /// attempt before its handler runs a statement.
    pub fn timeout(&self) -> Option<Duration> {
        self.timeout_ms
            .filter(|ms| *ms > 0)
            .map(|ms| Duration::from_millis(ms as u64))
    }

    /// Result retention policy.
    pub fn retention(&self) -> JobRetention {
        JobRetention::from_result_ttl_ms(self.result_ttl_ms)
    }

    /// Delay before the next retry attempt, applying this job's backoff.
    pub(crate) fn next_retry_delay(&self) -> Duration {
        retry_delay_for(self.retry_delay_ms, &self.backoff, self.attempts)
    }
}

#[cfg(test)]
mod job_status_tests {
    use super::*;

    #[test]
    fn test_status_round_trips_and_classifies() {
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
/// worker state, and cooperative cancellation.
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

    /// A token cancelled when the worker begins shutdown or observes a user
    /// abort or missing job row. Long-running handlers should `select!` on it
    /// at natural pause points and return after bounded cleanup.
    ///
    /// Shutdown allows up to the worker's configured `shutdown_grace`; a user
    /// abort allows up to
    /// [`WorkerBuilder::abort_grace`](crate::WorkerBuilder::abort_grace).
    /// The task is forcibly stopped when that bound expires. Attempt timeouts,
    /// sweeper recovery, and a job row deleted under a running attempt stop the
    /// task immediately, so this token is a cooperative cleanup opportunity
    /// rather than an unconditional guarantee.
    /// Cancelling the returned child token does not cancel the job attempt.
    pub fn cancellation(&self) -> CancellationToken {
        self.inner.cancel.child_token()
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
    fn test_state_map_indexes_values_by_type() {
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

/// A job type generated by the `#[pgqueue::job]` or `#[pgqueue::cron]`
/// attribute macro.
///
/// You never implement this by hand: annotate an `async fn` and the macro
/// produces a unit struct implementing it, plus a typed enqueue constructor and
/// a `::call(...)` test helper.
pub trait JobType: Copy + Send + Sync + 'static {
    /// The payload: the first parameter of the annotated function.
    type Args: Serialize + DeserializeOwned + Send + 'static;
    /// The success value: the `Ok` side of the function's return type.
    type Output: Serialize + DeserializeOwned + Send + 'static;

    /// The registry/database name of this job.
    const NAME: &'static str;

    /// The configuration from the attribute arguments (`max_attempts`,
    /// `timeout_ms`, and related options).
    fn config() -> JobConfig;

    /// The type-erased handler stored in the worker registry.
    fn erased() -> TypeErasedJobHandler;
}

/// Marker implemented by job types generated with [`macro@crate::job`].
///
/// It distinguishes ordinary job definitions from compile-time cron
/// definitions when configuring a worker.
pub trait JobDefinition: JobType {}

/// A compile-time cron definition generated with [`macro@crate::cron`].
pub trait CronDefinition: JobType {
    /// The UTC cron expression checked by the macro.
    const SCHEDULE: &'static str;

    /// Monotonic revision for the durable cron definition.
    const CRON_REVISION: u64;
}

/// How a durable cron schedule handles an occurrence missed while no current
/// scheduler was able to publish it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CronMisfirePolicy {
    /// Skip stale occurrences. `None` preserves the adaptive default of one
    /// fifth of the schedule period, clamped to 1..=60 seconds. An explicit
    /// grace is always capped by the next occurrence.
    Skip {
        /// Maximum non-zero age at which a missed occurrence may still be
        /// published.
        grace: Option<Duration>,
    },
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
            validate_nonzero_duration("cron misfire grace", grace)?;
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

    /// Converts the handler return value into the attempt result.
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
            // Rebuilt rather than cloned: `JobError`'s fields are public, so a
            // handler's own error may never have passed through a constructor —
            // a struct literal or a deserialized one carries whatever message it
            // was given, NUL included. Only the kind is preserved verbatim.
            if let Some(job_error) = error_any.downcast_ref::<JobError>() {
                return JobError::new(job_error.kind, &job_error.message);
            }
            if let Some(error) = error_any.downcast_ref::<anyhow::Error>() {
                if let Some(job_error) = error
                    .chain()
                    .find_map(|cause| cause.downcast_ref::<JobError>())
                {
                    return JobError::new(job_error.kind, &job_error.message);
                }
                return JobError::failed(format!("{error:#}"));
            }
            JobError::failed(error)
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
    type_id: TypeId,
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
            type_id: TypeId::of::<J>(),
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

    /// The generated Rust type that owns this handler.
    pub(crate) fn type_id(&self) -> TypeId {
        self.type_id
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
    fn test_erased_handler_exposes_name_and_config() {
        let handler = Noop::erased();
        assert_eq!(handler.name(), "noop");
        assert_eq!(*handler.config(), JobConfig::default());
        assert!(format!("{handler:?}").contains("noop"));
    }

    #[test]
    fn test_job_results_normalize_successes_and_failures() {
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

    #[test]
    fn test_job_result_preserves_job_error_wrapped_by_anyhow() {
        let wrapped = anyhow::Error::new(JobError::new(JobErrorKind::Timeout, "too slow"))
            .context("handler context");
        let result: Result<(), anyhow::Error> = Err(wrapped);

        let error = result.into_job_result().unwrap_err();

        assert_eq!(error.kind, JobErrorKind::Timeout);
        assert_eq!(error.message, "too slow");
    }

    #[test]
    fn test_job_result_preserves_anyhow_cause_chain() {
        let wrapped = anyhow::Error::new(std::io::Error::other("connection closed"))
            .context("publish job")
            .context("handler failed");
        let result: Result<(), anyhow::Error> = Err(wrapped);

        let error = result.into_job_result().unwrap_err();

        assert_eq!(error.kind, JobErrorKind::Failed);
        assert_eq!(
            error.message,
            "handler failed: publish job: connection closed"
        );
    }
}

/// A registered cron job: a parsed schedule plus the job template to enqueue.
pub(crate) struct JobCronEntry {
    pub cron: Cron,
    /// The source expression stored with scheduled occurrences.
    pub expr: String,
    /// The dedupe key every occurrence fires under (also set on the template).
    pub dedupe_key: String,
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
        let dedupe_key = template
            .dedupe_key
            .clone()
            .unwrap_or_else(|| format!("cron:{}", template.name));
        template.dedupe_key = Some(dedupe_key.clone());
        template.validate()?;
        let definition = serde_json::json!({
            "payload": template.payload.clone(),
            "max_attempts": template.config.max_attempts,
            "timeout_ms": template.config.timeout.map(duration_to_ms),
            "result_ttl_ms": template.config.retention.as_result_ttl_ms(),
            "retry_delay_ms": duration_to_ms(template.config.retry_delay),
            "backoff": template.config.backoff,
            "priority": template.config.priority,
            "meta": template.meta.clone(),
        });
        Ok(Self {
            cron,
            expr: expr.to_string(),
            dedupe_key,
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
            // "Publish only the most recent missed occurrence, provided its
            // successor is still in the future": the whole period is the grace.
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
    fn test_cron_misfire_policy_rejects_zero_grace() {
        let error = CronMisfirePolicy::Skip {
            grace: Some(Duration::ZERO),
        }
        .validate()
        .unwrap_err();

        assert!(error.to_string().contains("greater than zero"), "{error}");
    }

    #[test]
    fn test_next_occurrence_is_identical_when_now_has_subseconds() {
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
    fn test_publication_deadline_is_identical_when_graces_share_canonical_milliseconds() {
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
    fn test_parses_five_and_six_field_expressions() {
        assert!(parse_cron("*/5 * * * *").is_ok());
        assert!(parse_cron("30 */5 * * * *").is_ok());
        assert!(parse_cron("not a cron").is_err());
        assert!(parse_cron("99 * * * *").is_err());
    }

    #[test]
    fn test_entry_defaults_dedupe_key_and_schedules() {
        let entry =
            JobCronEntry::new("0 * * * *", JobRequest::new("cleanup", json!(null))).unwrap();
        assert_eq!(entry.dedupe_key, "cron:cleanup");
        assert_eq!(entry.template.dedupe_key.as_deref(), Some("cron:cleanup"));
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
    fn test_impossible_schedule_surfaces_an_error() {
        let entry = JobCronEntry::new("0 0 30 2 *", JobRequest::new("never", json!(null))).unwrap();
        let err = entry.next_occurrence(Utc::now()).unwrap_err();
        assert!(err.to_string().contains("cron occurrence"), "{err}");
    }

    #[test]
    fn test_explicit_dedupe_key_is_preserved() {
        let mut template = JobRequest::new("cleanup", json!(null));
        template.dedupe_key = Some("custom".into());
        let entry = JobCronEntry::new("0 * * * *", template).unwrap();
        assert_eq!(entry.dedupe_key, "custom");
        assert_eq!(entry.template.dedupe_key.as_deref(), Some("custom"));
    }

    #[test]
    fn test_derived_dedupe_key_is_validated() {
        let error = JobCronEntry::new("0 * * * *", JobRequest::new("x".repeat(251), json!(null)))
            .unwrap_err();
        assert!(error.to_string().contains("dedupe key"), "{error}");
    }
}

const MAX_INDEXED_KEY_BYTES: usize = 255;

/// Whether a JSON document carries a NUL in any string or object key.
///
/// PostgreSQL's `jsonb` cannot represent `\0`, so such a document is an error
/// on this side of the wire rather than a database one, whichever end of a job
/// it came from. An enqueued payload carrying one raises `22P05`, which on
/// `Queue::enqueue_in`/`enqueue_raw_in` aborts the *caller's* transaction and
/// destroys their whole unit of work; a handler result carrying one leaves an
/// attempt that can never be finalized (see `classify_attempt_join`).
pub(crate) fn json_contains_nul(value: &Value) -> bool {
    match value {
        Value::String(text) => text.contains('\0'),
        Value::Array(items) => items.iter().any(json_contains_nul),
        Value::Object(fields) => fields
            .iter()
            .any(|(key, value)| key.contains('\0') || json_contains_nul(value)),
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    }
}

/// The deepest container nesting this crate will write to `jsonb`.
///
/// `jsonb` itself tolerates far more, but `serde_json`'s deserializer stops at
/// 128 nested containers, and every read of `payload`, `meta`, `result` and
/// worker `metadata` goes through it. A document nested any deeper is therefore
/// one PostgreSQL stores happily and this crate can never decode again.
pub(crate) const MAX_JSON_DEPTH: usize = 127;

/// Whether `value` nests containers more than `budget` levels deep.
///
/// The damage from writing one is not confined to its own row: the dequeue
/// statement commits `status = 'running'`, `attempts + 1` and `worker_id`
/// server-side *before* the client decodes the returned rows, so a single
/// undecodable row fails the whole batch's decode and strands every healthy job
/// claimed alongside it — each with an attempt spent and nobody processing it.
/// `fetch_job`, `jobs_page` and the dashboard listing fail for the whole queue
/// for as long as the row is retained. So it is refused here, before anything is
/// written, the way `json_contains_nul` refuses a NUL.
///
/// The walk is bounded by the same budget it is checking, so it cannot itself
/// recurse past the limit it exists to enforce.
pub(crate) fn json_exceeds_depth(value: &Value, budget: usize) -> bool {
    match value {
        Value::Array(items) => {
            budget == 0
                || items
                    .iter()
                    .any(|item| json_exceeds_depth(item, budget - 1))
        }
        Value::Object(fields) => {
            budget == 0
                || fields
                    .values()
                    .any(|field| json_exceeds_depth(field, budget - 1))
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => false,
    }
}

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
    /// Dedupe identity shared by at most one live row per queue, at most 255
    /// bytes so it remains safe to store in PostgreSQL's B-tree index.
    /// Terminal occurrences retain the key for history and result lookup.
    pub dedupe_key: Option<String>,
    /// Earliest execution time; `None` = now.
    pub scheduled_at: Option<DateTime<Utc>>,
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
            dedupe_key: None,
            scheduled_at: None,
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
            ("dedupe key", self.dedupe_key.as_deref()),
        ] {
            if value.is_some_and(|value| value.contains('\0')) {
                return Err(Error::Config(format!("{field} must not contain NUL")));
            }
        }
        for (field, value) in [("job payload", &self.payload), ("job meta", &self.meta)] {
            // Depth first: it is the bounded walk, so `json_contains_nul`'s
            // unbounded recursion never sees a document deep enough to matter.
            if json_exceeds_depth(value, MAX_JSON_DEPTH) {
                return Err(Error::Config(format!(
                    "{field} must not nest deeper than {MAX_JSON_DEPTH} levels"
                )));
            }
            if json_contains_nul(value) {
                return Err(Error::Config(format!("{field} must not contain NUL")));
            }
        }
        if self
            .dedupe_key
            .as_deref()
            .is_some_and(|key| key.len() > MAX_INDEXED_KEY_BYTES)
        {
            return Err(Error::Config(format!(
                "dedupe key must not be longer than {MAX_INDEXED_KEY_BYTES} bytes"
            )));
        }
        // `delay` is bounded by the same window (see `validate_duration`), and
        // `at()` is the same instant expressed absolutely, so accepting one and
        // refusing the other would be arbitrary.
        if let Some(scheduled_at) = self.scheduled_at {
            if scheduled_at.timestamp() < MIN_TIMESTAMPTZ_SECONDS {
                return Err(Error::Config(
                    "job schedule time is below PostgreSQL's supported timestamp range".into(),
                ));
            }
            if chrono::Duration::from_std(MAX_DURATION)
                .ok()
                .and_then(|window| Utc::now().checked_add_signed(window))
                .is_some_and(|horizon| scheduled_at > horizon)
            {
                return Err(Error::Config(format!(
                    "job schedule time exceeds the maximum supported duration of {MAX_DURATION:?} \
                     from now"
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
    dedupe_key: Option<String>,
    scheduled_at: Option<DateTime<Utc>>,
    delay: Option<Duration>,
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
            dedupe_key: None,
            scheduled_at: None,
            delay: None,
            meta: Value::Object(serde_json::Map::new()),
            _job: PhantomData,
        }
    }

    /// Dedupe identity: at most one live (non-terminal) job per
    /// `(queue, dedupe_key)`. Enqueueing a duplicate returns
    /// `Ok(EnqueueResult::Deduplicated(handle))`.
    pub fn dedupe_key(mut self, key: impl Into<String>) -> Self {
        self.dedupe_key = Some(key.into());
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
            dedupe_key: self.dedupe_key,
            scheduled_at: self.scheduled_at,
            meta: self.meta,
        };
        Ok((job, self.delay))
    }
}

/// Result of publishing a job with an optional dedupe key.
///
/// Both variants contain the new or existing job identity. Typed enqueue
/// methods store a [`JobHandle`], while raw enqueue methods store the job's
/// [`Uuid`]. A deduplicated publish points at the live job that already owns
/// the key; it does not provide exactly-once execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnqueueResult<H> {
    /// A new job row was inserted.
    Enqueued(H),
    /// A live job already owned the request's dedupe key.
    Deduplicated(H),
}

impl<H> EnqueueResult<H> {
    /// Whether this publish inserted a new row.
    pub fn is_enqueued(&self) -> bool {
        matches!(self, Self::Enqueued(_))
    }

    /// Whether this publish reused a live row with the same dedupe key.
    pub fn is_deduplicated(&self) -> bool {
        matches!(self, Self::Deduplicated(_))
    }

    fn value(&self) -> &H {
        match self {
            Self::Enqueued(handle) | Self::Deduplicated(handle) => handle,
        }
    }

    fn into_value(self) -> H {
        match self {
            Self::Enqueued(handle) | Self::Deduplicated(handle) => handle,
        }
    }
}

impl EnqueueResult<Uuid> {
    /// Returns the new or existing job's id.
    pub fn job_id(&self) -> Uuid {
        *self.value()
    }

    /// Consumes the result and returns the new or existing job's id.
    pub fn into_job_id(self) -> Uuid {
        self.into_value()
    }
}

impl Queue {
    /// Enqueues a typed job: `queue.enqueue(my_job::job(args)).await?`.
    ///
    /// A dedupe-key collision returns [`EnqueueResult::Deduplicated`] with a
    /// typed handle to the existing job. It is an error when that row belongs
    /// to a different job type.
    pub async fn enqueue<J: JobType>(
        &self,
        job: JobBuilder<J>,
    ) -> Result<EnqueueResult<JobHandle<J>>, Error> {
        let (new_job, delay) = job.into_parts()?;
        let retention = new_job.config.retention;
        let result = self
            .database()
            .enqueue_raw_delayed_result(new_job, delay)
            .await?;
        typed_enqueue_result::<J>(self, result, retention)
    }

    /// Enqueues a typed job as part of a caller-owned PostgreSQL transaction.
    ///
    /// The job and its notification become visible only if the caller commits.
    /// Dedupe-key advisory locks remain held until that commit, so applications
    /// should acquire their own locks and publish jobs with dedupe keys in a
    /// consistent order across transactions.
    ///
    /// PostgreSQL's default `READ COMMITTED` isolation is required to observe a
    /// dedupe-key owner that commits while this call waits for its lock. At
    /// `REPEATABLE READ` or `SERIALIZABLE`, retry the whole transaction if such
    /// a concurrent owner is outside the caller's snapshot.
    pub async fn enqueue_in<J: JobType>(
        &self,
        transaction: &mut sqlx::PgTransaction<'_>,
        job: JobBuilder<J>,
    ) -> Result<EnqueueResult<JobHandle<J>>, Error> {
        let (new_job, delay) = job.into_parts()?;
        let retention = new_job.config.retention;
        let result = self
            .database()
            .enqueue_raw_delayed_in_result(transaction, new_job, delay)
            .await?;
        typed_enqueue_result::<J>(self, result, retention)
    }

    /// Enqueues a job and waits for its typed result (request/response).
    ///
    /// If the builder carries a `dedupe_key` that deduplicates against a live
    /// job, `enqueue_and_wait` waits on that existing job instead. Failures surface as
    /// [`Error::Job`]; `None` timeout waits forever.
    ///
    /// The job's retention must keep the row around long enough to read the
    /// result. `JobRetention::DeleteImmediately` is rejected before enqueue.
    pub async fn enqueue_and_wait<J: JobType>(
        &self,
        job: JobBuilder<J>,
        timeout: Option<Duration>,
    ) -> Result<J::Output, Error> {
        let (new_job, delay) = job.into_parts()?;
        if new_job.config.retention == JobRetention::DeleteImmediately {
            return Err(Error::Config(
                "enqueue_and_wait requires result retention; DeleteImmediately removes the result before it can be read"
                    .into(),
            ));
        }
        let retention = new_job.config.retention;
        let handle: JobHandle<J> = match self
            .database()
            .enqueue_raw_delayed_result(new_job, delay)
            .await?
        {
            DatabaseEnqueueResult::Inserted(id) => JobHandle::new(id, self.clone(), retention),
            DatabaseEnqueueResult::Deduplicated {
                id,
                name,
                retention,
            } => {
                if retention == JobRetention::DeleteImmediately {
                    return Err(Error::Config(
                        "enqueue_and_wait cannot wait on the existing deduplicated job because it deletes its result immediately"
                            .into(),
                    ));
                }
                if name != J::NAME {
                    return Err(Error::Config(format!(
                        "dedupe key belongs to job {name:?}, not {:?}",
                        J::NAME
                    )));
                }
                JobHandle::new(id, self.clone(), retention)
            }
        };
        handle.wait(timeout).await
    }
}

fn typed_enqueue_result<J: JobType>(
    queue: &Queue,
    result: DatabaseEnqueueResult,
    inserted_retention: JobRetention,
) -> Result<EnqueueResult<JobHandle<J>>, Error> {
    match result {
        DatabaseEnqueueResult::Inserted(id) => Ok(EnqueueResult::Enqueued(JobHandle::new(
            id,
            queue.clone(),
            inserted_retention,
        ))),
        DatabaseEnqueueResult::Deduplicated {
            id,
            name,
            retention,
        } => {
            if name != J::NAME {
                return Err(Error::Config(format!(
                    "dedupe key belongs to job {name:?}, not {:?}",
                    J::NAME
                )));
            }
            Ok(EnqueueResult::Deduplicated(JobHandle::new(
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
    pub async fn fetch_job(&self) -> Result<JobRow, Error> {
        self.queue
            .fetch_job(self.id)
            .await?
            .ok_or(Error::JobNotFound(self.id))
    }

    /// Requests an abort (see [`Queue::abort_job`]).
    pub async fn abort(&self, reason: &str) -> Result<bool, Error> {
        self.queue.abort_job(self.id, reason).await
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
            // already aborted may still read that terminal result. Running or
            // deleted rows cannot provide a reliable result.
            if let Some(row) = self.queue.fetch_job(self.id).await?
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
        // maximum instead of hammering the pool even though completions arrive
        // push-based.
        const INITIAL_POLL_INTERVAL: Duration = Duration::from_millis(250);
        const MAX_POLL_INTERVAL: Duration = Duration::from_secs(2);

        // Subscribe before the first status check so a finish landing in
        // between can't be missed. The listener needs its own connection
        // outside the query pool, so it can be refused while the queue is
        // perfectly reachable; it reconnects in the background and the poll
        // loop below carries the wait until it does.
        let mut done = self.queue.notify_listener().subscribe_done();
        let mut poll_interval = INITIAL_POLL_INTERVAL;
        'poll: loop {
            let row = self.queue.fetch_job(self.id).await?;
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
                            if let Some(row) = self.queue.fetch_job(self.id).await? {
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

impl<J: JobType> EnqueueResult<JobHandle<J>> {
    /// Returns the new or existing job's id.
    ///
    /// ```no_run
    /// # #[pgqueue::job]
    /// # async fn cleanup(_: ()) {}
    /// # async fn enqueue(queue: pgqueue::Queue) -> Result<(), pgqueue::Error> {
    /// let result = queue.enqueue(cleanup::job(())).await?;
    /// assert_eq!(result.job_id(), result.job_handle().id());
    /// # Ok(())
    /// # }
    /// ```
    pub fn job_id(&self) -> Uuid {
        self.job_handle().id()
    }

    /// Borrows the new or existing job handle.
    pub fn job_handle(&self) -> &JobHandle<J> {
        self.value()
    }

    /// Consumes the result and returns the new or existing job handle.
    pub fn into_job_handle(self) -> JobHandle<J> {
        self.into_value()
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
    // Every caller guards on `JobStatus::is_terminal`, so `Aborting` — which is
    // not terminal — never arrives here and gets no arm of its own.
    match row.status {
        JobStatus::Complete => Ok(row.result.unwrap_or(Value::Null)),
        // Aborts store the raw reason (e.g. "aborted from ui"), not a
        // JobError rendering — classify by status.
        JobStatus::Aborted => Err(Error::Job(JobError::new(
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
    fn test_new_job_uses_expected_defaults() {
        let job = JobRequest::new("send_email", serde_json::json!({"to": "a@b.c"}));
        assert_eq!(job.name, "send_email");
        assert_eq!(job.config, JobConfig::default());
        assert!(job.dedupe_key.is_none());
        assert!(job.scheduled_at.is_none());
        assert_eq!(job.meta, serde_json::json!({}));
    }

    #[test]
    fn test_new_job_rejects_an_oversized_dedupe_key() {
        let mut job = JobRequest::new("bounded", Value::Null);
        job.dedupe_key = Some("x".repeat(MAX_INDEXED_KEY_BYTES + 1));
        let error = job.validate().unwrap_err();
        assert!(error.to_string().contains("255 bytes"), "{error}");
    }

    /// `jsonb` cannot store `\0`, so a payload carrying one used to reach
    /// SQL and raise `22P05` — which inside `Queue::enqueue_in` aborts the
    /// *caller's* transaction and destroys their whole unit of work over an
    /// input error that is detectable here.
    #[test]
    fn test_new_job_rejects_a_nul_anywhere_in_the_payload_or_meta() {
        for (field, nul) in [
            ("job payload", serde_json::json!("bad\0value")),
            ("job payload", serde_json::json!({ "k": ["ok", "bad\0"] })),
            ("job payload", serde_json::json!({ "bad\0key": 1 })),
            ("job meta", serde_json::json!({ "trace": "bad\0" })),
        ] {
            let mut job = JobRequest::new("nul", Value::Null);
            if field == "job payload" {
                job.payload = nul.clone();
            } else {
                job.meta = nul.clone();
            }
            let error = job.validate().unwrap_err();
            assert_eq!(
                error.to_string(),
                format!("configuration error: {field} must not contain NUL")
            );
        }

        // Values that merely *contain* the escape's neighbours still pass.
        let mut job = JobRequest::new("fine", serde_json::json!({ "k": ["ü", "\u{1}"] }));
        job.meta = serde_json::json!({ "nested": { "deep": [null, 1, true] } });
        job.validate().unwrap();
    }

    /// An absolute schedule must fit both pgqueue's delay window and
    /// PostgreSQL's timestamp representation.
    #[test]
    fn test_new_job_bounds_an_absolute_schedule_time() {
        let window = chrono::Duration::from_std(MAX_DURATION).unwrap();
        let mut job = JobRequest::new("scheduled", Value::Null);

        job.scheduled_at = DateTime::from_timestamp(MIN_TIMESTAMPTZ_SECONDS, 0);
        job.validate().unwrap();

        job.scheduled_at = DateTime::from_timestamp(MIN_TIMESTAMPTZ_SECONDS - 1, 0);
        let error = job.validate().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("below PostgreSQL's supported timestamp range"),
            "{error}"
        );

        job.scheduled_at = Some(Utc::now() + window - chrono::Duration::days(1));
        job.validate().unwrap();

        job.scheduled_at = Some(Utc::now() + window + chrono::Duration::days(1));
        let error = job.validate().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("job schedule time exceeds the maximum supported duration"),
            "{error}"
        );

        // A time in the past is meaningful ("run now"): cron publishes missed
        // occurrences that way.
        job.scheduled_at = Some(Utc::now() - window);
        job.validate().unwrap();
    }

    /// A negative TTL has no encoding, but decoding one as a live zero-length
    /// retention is the single reading that *keeps* the row rather than
    /// deleting it, so it inverts the caller's intent.
    #[test]
    fn test_retention_decodes_a_negative_ttl_as_an_immediate_delete() {
        assert_eq!(
            JobRetention::from_result_ttl_ms(Some(-1)),
            JobRetention::DeleteImmediately
        );
        assert_eq!(
            JobRetention::from_result_ttl_ms(Some(i64::MIN)),
            JobRetention::DeleteImmediately
        );
    }

    #[test]
    fn test_resolve_deleted_preserves_failed_terminal_result() {
        let id = Uuid::now_v7();
        let error = resolve_deleted(QueueDoneEvent {
            id,
            status: JobStatus::Failed,
        })
        .unwrap_err();

        let Error::Job(error) = error else {
            panic!("deleted failed row should resolve to a job error");
        };
        assert_eq!(error.kind, JobErrorKind::Failed);
        assert_eq!(error.message, "job failed and was deleted");
    }
}
