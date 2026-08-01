//! PostgreSQL persistence shared by queues, workers, and the dashboard.

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::postgres::{PgPool, PgPoolOptions};
use uuid::Uuid;

use crate::Error;
use crate::job::{
    CronMisfirePolicy, JobCronEntry, JobCursor, JobRequest, JobRetention, JobRetryBackoff, JobRow,
    JobStatus, MAX_JSON_DEPTH, duration_to_ms, json_contains_nul, json_exceeds_depth,
    validate_duration,
};
use crate::queue::{MigrationMode, QueueCounters, QueueCounts, QueueNotifyListener, QueueStats};
use crate::sweeper::{SWEPT, Sweeper, is_swept_marked, swept_marker};
use crate::worker::WorkerInfo;

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

/// PostgreSQL `undefined_table`, raised for a missing schema too.
const UNDEFINED_TABLE: &str = "42P01";

#[derive(sqlx::FromRow)]
struct AppliedMigration {
    version: i64,
    checksum: Vec<u8>,
    success: bool,
}

#[derive(sqlx::FromRow)]
struct DatabaseServer {
    version: i32,
    database: String,
}

async fn validate_migrations(pool: &PgPool) -> Result<(), Error> {
    let applied = match sqlx::query_as::<_, AppliedMigration>(
        r#"
        SELECT version, checksum, success
        FROM pgqueue._sqlx_migrations
        ORDER BY version
        "#,
    )
    .fetch_all(pool)
    .await
    {
        Ok(applied) => applied,
        // No history table: nothing has ever been migrated here.
        Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some(UNDEFINED_TABLE) => {
            return Err(Error::Config(
                "database is missing pgqueue migrations; run once with MigrationMode::Apply".into(),
            ));
        }
        Err(error) => return Err(error.into()),
    };

    for row in &applied {
        if !row.success {
            return Err(Error::Migration(sqlx::migrate::MigrateError::Dirty(
                row.version,
            )));
        }
    }

    let expected = MIGRATOR
        .iter()
        .filter(|migration| !migration.migration_type.is_down_migration())
        .collect::<Vec<_>>();
    for row in &applied {
        let Some(migration) = expected
            .iter()
            .find(|migration| migration.version == row.version)
        else {
            return Err(Error::Migration(
                sqlx::migrate::MigrateError::VersionMissing(row.version),
            ));
        };
        if migration.checksum.as_ref() != row.checksum.as_slice() {
            return Err(Error::Migration(
                sqlx::migrate::MigrateError::VersionMismatch(row.version),
            ));
        }
    }
    if let Some(missing) = expected
        .iter()
        .find(|migration| !applied.iter().any(|row| row.version == migration.version))
    {
        return Err(Error::Config(format!(
            "database is missing pgqueue migration {} ({})",
            missing.version, missing.description
        )));
    }
    Ok(())
}

// Advisory locks use distinct two-key namespaces. Hash collisions only add
// serialization; table constraints remain the source of truth.
const DEDUPE_ENQUEUE_LOCK_MASK: i32 = 1 << 29;

/// FNV-1a over a byte stream; the one stable hash used for advisory-lock
/// keys, channel names, and asset fingerprints.
pub(crate) fn stable_hash(bytes: impl IntoIterator<Item = u8>) -> u64 {
    bytes.into_iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x100_0000_01b3)
    })
}

fn channel_name(queue: &str, suffix: &str) -> String {
    let full = format!("pgqueue_{queue}{suffix}");
    // Hash the queue and suffix NUL-separated (queue names reject control
    // characters) so a queue named "{x}_done" cannot share a channel with
    // queue "{x}"'s done channel.
    let hash = stable_hash(format!("{queue}\0{suffix}").bytes());
    // PostgreSQL identifiers are at most 63 bytes: 46 bytes, `_`, and 16 hex digits.
    let cut = (0..=46)
        .rev()
        .find(|&index| index <= full.len() && full.is_char_boundary(index))
        .unwrap_or(0);
    format!("{}_{hash:016x}", &full[..cut])
}

pub(crate) fn done_channel(queue: &str) -> String {
    channel_name(queue, "_done")
}

#[cfg(test)]
mod channel_name_tests {
    use super::*;

    #[test]
    fn test_channel_name_differs_when_queue_name_embeds_done_suffix() {
        assert_ne!(channel_name("jobs_done", ""), channel_name("jobs", "_done"));
    }

    #[test]
    fn test_channel_name_stays_within_postgres_identifier_limit() {
        let name = channel_name(&"q".repeat(300), "_done");
        assert!(name.len() <= 63, "channel name too long: {name}");
    }
}

pub(crate) fn dedupe_enqueue_lock_key(database: &str) -> i32 {
    stable_hash(database.bytes()) as i32 ^ DEDUPE_ENQUEUE_LOCK_MASK
}

pub(crate) fn sweep_lock_key(database: &str, queue: &str) -> i64 {
    stable_hash(format!("{database}:sweep:{queue}").bytes()) as i64
}

fn validate_queue_name(queue: &str) -> Result<(), Error> {
    if queue.is_empty() {
        return Err(Error::Config("queue name must not be empty".into()));
    }
    if matches!(queue, "." | "..") {
        return Err(Error::Config(
            "queue name must not be a dot segment (`.` or `..`)".into(),
        ));
    }
    if queue.len() > 255 {
        return Err(Error::Config(
            "queue name must not be longer than 255 bytes".into(),
        ));
    }
    if queue.chars().any(char::is_control) {
        return Err(Error::Config(
            "queue name must not contain control characters".into(),
        ));
    }
    Ok(())
}

/// Refuses a finalization value PostgreSQL can never store, or that this crate
/// could never read back.
///
/// A NUL is permanently invalid, not a transient failure: `jsonb` raises
/// `22P05` and `text` raises `22021`, so the attempt stays `running` and the
/// caller — which [`Attempt::finish`](crate::Attempt::finish) and
/// [`Attempt::retry`](crate::Attempt::retry) explicitly invite to "retry after
/// a transient infrastructure error" — spins forever. Every other writer on
/// this side of the wire already refuses one (see `json_contains_nul`); these
/// are the two the public consumer API reaches.
///
/// Excessive nesting is refused for the mirror-image reason: `jsonb` accepts it
/// and `serde_json` cannot decode it, so the row would be written successfully
/// and then poison every read of the queue it lands in (see
/// `json_exceeds_depth`).
///
/// Refused before a connection is taken, so it cannot be mistaken for pool
/// exhaustion either.
fn validate_finalization(result: Option<&Value>, error: Option<&str>) -> Result<(), Error> {
    if result.is_some_and(|result| json_exceeds_depth(result, MAX_JSON_DEPTH)) {
        return Err(Error::Config(format!(
            "job result must not nest deeper than {MAX_JSON_DEPTH} levels"
        )));
    }
    if result.is_some_and(json_contains_nul) {
        return Err(Error::Config("job result must not contain NUL".into()));
    }
    if error.is_some_and(|error| error.contains('\0')) {
        return Err(Error::Config("job error must not contain NUL".into()));
    }
    Ok(())
}

/// Database state scoped to one named queue.
pub(crate) struct Database {
    pool: PgPool,
    name: String,
    dedupe_enqueue_lock_key: i32,
    sweep_lock_key: i64,
    priorities: (i16, i16),
    sweep_grace: Duration,
    sweep_batch_size: i64,
    notify_channel: String,
    done_channel: String,
    counters: QueueCounters,
    notify_listener: std::sync::OnceLock<QueueNotifyListener>,
}

pub(crate) struct DatabaseConnectOptions {
    pub(crate) url: String,
    pub(crate) pool: Option<PgPool>,
    pub(crate) name: String,
    pub(crate) max_connections: u32,
    pub(crate) min_connections: u32,
    pub(crate) priorities: (i16, i16),
    pub(crate) sweep_grace: Duration,
    pub(crate) sweep_batch_size: u32,
    pub(crate) migration_mode: MigrationMode,
}

pub(crate) enum DatabaseEnqueueResult {
    Inserted(Uuid),
    Deduplicated {
        id: Uuid,
        name: String,
        retention: JobRetention,
    },
}

/// The live row a cron upsert conflicted with.
#[derive(sqlx::FromRow)]
pub(crate) struct DatabaseCronConflict {
    pub(crate) scheduled_at: DateTime<Utc>,
    pub(crate) kind: String,
    pub(crate) name: String,
}

pub(crate) enum DatabaseCronAuthority {
    Active,
    Inactive { revision: i64 },
}

pub(crate) enum DatabaseCronScheduleResult {
    NotDue,
    Contended,
    Inactive {
        revision: i64,
    },
    Published {
        id: Uuid,
        occurrence: DateTime<Utc>,
    },
    AlreadyPublished {
        occurrence: DateTime<Utc>,
    },
    SkippedStale {
        occurrence: DateTime<Utc>,
    },
    SkippedHeld {
        occurrence: DateTime<Utc>,
        existing: DatabaseCronConflict,
    },
}

pub(crate) struct DatabaseAbortingAttempt {
    pub(crate) id: Uuid,
    pub(crate) attempts: i32,
    pub(crate) reason: Option<String>,
    pub(crate) swept: bool,
}

/// One in-flight attempt as its worker knows it, for [`Database::aborting_of`]
/// to compare against the row.
#[derive(Clone, Copy)]
pub(crate) struct DatabaseAbortClaim {
    pub(crate) id: Uuid,
    pub(crate) attempts: i32,
}

pub(crate) struct DatabaseAbortPoll {
    pub(crate) aborting: Vec<DatabaseAbortingAttempt>,
    /// Claims whose row is gone. Reported as the claim, not just the id, so the
    /// caller can name the one attempt that lost its row: the same id can be
    /// in flight under two attempt numbers at once.
    pub(crate) missing: Vec<DatabaseAbortClaim>,
    /// Claims whose row is still there but no longer theirs.
    pub(crate) superseded: Vec<DatabaseAbortClaim>,
}

#[derive(sqlx::FromRow)]
pub(crate) struct DatabaseStuckJob {
    pub(crate) id: Uuid,
    pub(crate) name: String,
    pub(crate) status: JobStatus,
    pub(crate) attempts: i32,
    pub(crate) max_attempts: i32,
    pub(crate) retry_delay_ms: i64,
    pub(crate) backoff: JobRetryBackoff,
    pub(crate) worker_id: Option<Uuid>,
    pub(crate) error: Option<String>,
    pub(crate) result: Option<Value>,
}

impl DatabaseStuckJob {
    pub(crate) fn retryable(&self) -> bool {
        crate::job::attempts_remaining(self.attempts, self.max_attempts)
    }

    pub(crate) fn next_retry_delay(&self) -> Duration {
        crate::job::retry_delay_for(self.retry_delay_ms, &self.backoff, self.attempts)
    }
}

#[derive(Clone, Copy)]
struct AttemptGuard<'a> {
    id: Uuid,
    queue: &'a str,
    attempts: i32,
    worker_id: Option<Uuid>,
}

impl<'a> From<&'a JobRow> for AttemptGuard<'a> {
    fn from(job: &'a JobRow) -> Self {
        Self {
            id: job.id,
            queue: &job.queue,
            attempts: job.attempts,
            worker_id: job.worker_id,
        }
    }
}

pub(crate) struct DatabaseDequeueBatch {
    pub(crate) jobs: Vec<JobRow>,
    pub(crate) intake_open: bool,
    /// A matching job is still ready after this batch. This remains true for
    /// rows skipped because another transaction currently holds their row
    /// lock, so burst workers cannot mistake transient lock contention for a
    /// drained queue.
    pub(crate) work_available: bool,
    pub(crate) unhandled_names: Vec<String>,
}

#[derive(Debug, PartialEq, Eq, sqlx::FromRow)]
struct DatabaseDequeueProbe {
    intake_open: bool,
    work_available: bool,
    unhandled_names: Vec<String>,
}

#[derive(sqlx::FromRow)]
struct ExistingJob {
    id: Uuid,
    name: String,
    result_ttl_ms: Option<i64>,
}

/// The collision answer for a dedupe key an existing live job holds.
fn deduplicated(row: ExistingJob) -> DatabaseEnqueueResult {
    DatabaseEnqueueResult::Deduplicated {
        id: row.id,
        name: row.name,
        retention: JobRetention::from_result_ttl_ms(row.result_ttl_ms),
    }
}

#[derive(sqlx::FromRow)]
struct CronAuthority {
    name: String,
    expression: String,
    /// Whether the stored `definition` equals the one this worker registered.
    ///
    /// Compared server-side, not in Rust, because `jsonb` equality is the only
    /// equality this value has. `jsonb` stores numbers as `numeric`, so a
    /// `serde_json` float in exponent form comes back expanded and re-parses as
    /// `Number::PosInt` where it went in as `Number::Float` — and `serde_json`'s
    /// `PartialEq` calls those unequal. A cron whose payload or meta carried a
    /// float of 1e16 or larger therefore conflicted with the definition this
    /// same call had just written, and was disabled permanently with a
    /// revision-conflict error no revision bump can clear.
    definition_matches: bool,
    revision: i64,
    misfire_policy: String,
    grace_ms: Option<i64>,
}

#[derive(sqlx::FromRow)]
struct ObservedCron {
    name: String,
    expression: String,
    /// Server-side `jsonb` equality, for the reason on [`CronAuthority`].
    definition_matches: bool,
    revision: i64,
    misfire_policy: String,
    grace_ms: Option<i64>,
    next_run_at: DateTime<Utc>,
    now: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct AbortPollRow {
    id: Uuid,
    status: JobStatus,
    attempts: i32,
    worker_id: Option<Uuid>,
    error: Option<String>,
    result: Option<Value>,
}

#[derive(sqlx::FromRow)]
struct AbortResult {
    status: String,
}

#[derive(sqlx::FromRow)]
struct FinishResult {
    finished: bool,
}

#[derive(sqlx::FromRow)]
struct RequeueResult {
    requeued: bool,
}

/// What a lease write does to `pgqueue.workers.accepting`.
///
/// The row a heartbeat updates and the row it creates need different answers.
/// A worker's own heartbeat must never reopen intake it already closed, so it
/// leaves an existing flag alone — but it still creates a lease whenever one is
/// missing (its first, or a replacement for one the sweeper purged after the
/// worker stalled past its TTL), and that new row has to start in the state the
/// caller is actually in. Defaulting it to `accepting` republished a
/// shutting-down worker as open for business: `accepting` is read by the two
/// claim paths ([`Database::dequeue_inner`] and its underfilled-batch probe),
/// so the recreated lease let a worker that had already closed intake keep
/// claiming new jobs it would then have to abandon.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LeaseIntake {
    /// Take work: create the lease accepting, and reopen one that was closed.
    /// A [`crate::Consumer`] heartbeat is its request for work, so it reopens.
    Reopen,
    /// Take work, but never undo a close: create the lease accepting and leave
    /// an existing flag as it stands.
    Open,
    /// Stopped taking work: create the lease closed and leave a closed one
    /// closed.
    Closed,
}

impl LeaseIntake {
    /// Whether an existing lease's `accepting` flag is forced back on.
    fn reopens(self) -> bool {
        matches!(self, LeaseIntake::Reopen)
    }

    /// The `accepting` value a lease created by this write starts with.
    fn accepts_when_created(self) -> bool {
        !matches!(self, LeaseIntake::Closed)
    }
}

fn resolve_post_commit_probe(
    queue: &str,
    worker_id: Uuid,
    jobs_claimed: usize,
    probe: Result<DatabaseDequeueProbe, sqlx::Error>,
) -> Result<DatabaseDequeueProbe, Error> {
    match probe {
        Ok(probe) => Ok(probe),
        Err(error) if jobs_claimed == 0 => Err(error.into()),
        Err(error) => {
            tracing::warn!(
                queue,
                worker.id = %worker_id,
                job.count = jobs_claimed,
                %error,
                "post-commit dequeue probe failed; returning the committed batch"
            );
            // The claim is already durable. Hand it to processors, keep their
            // demand outstanding, and defer availability and handler
            // diagnostics rather than orphaning the attempt under lease.
            Ok(DatabaseDequeueProbe {
                intake_open: true,
                work_available: true,
                unhandled_names: Vec::new(),
            })
        }
    }
}

#[cfg(test)]
mod dequeue_probe_tests {
    use super::*;

    #[test]
    fn test_resolve_post_commit_probe_preserves_successful_metadata() {
        let expected = DatabaseDequeueProbe {
            intake_open: false,
            work_available: true,
            unhandled_names: vec!["missing".into()],
        };

        let actual = resolve_post_commit_probe("default", Uuid::nil(), 0, Ok(expected)).unwrap();

        assert_eq!(
            actual,
            DatabaseDequeueProbe {
                intake_open: false,
                work_available: true,
                unhandled_names: vec!["missing".into()],
            }
        );
    }

    #[test]
    fn test_resolve_post_commit_probe_returns_conservative_metadata_when_jobs_were_claimed() {
        let actual =
            resolve_post_commit_probe("default", Uuid::nil(), 1, Err(sqlx::Error::PoolClosed))
                .unwrap();

        assert_eq!(
            actual,
            DatabaseDequeueProbe {
                intake_open: true,
                work_available: true,
                unhandled_names: Vec::new(),
            }
        );
    }

    #[test]
    fn test_resolve_post_commit_probe_propagates_error_when_no_jobs_were_claimed() {
        let error =
            resolve_post_commit_probe("default", Uuid::nil(), 0, Err(sqlx::Error::PoolClosed))
                .unwrap_err();

        assert!(matches!(error, Error::Db(sqlx::Error::PoolClosed)));
    }
}

/// Which rows [`Database::requeue_guarded`] may reclaim.
#[derive(Clone, Copy)]
struct DatabaseRequeueGuards {
    /// Reclaim the row while it is still `running`.
    allow_running: bool,
    /// Reclaim an `aborting` row bearing the sweeper's markers.
    allow_swept_abort: bool,
    /// Refund the attempt and close the worker's intake (shutdown requeue).
    refund_attempt: bool,
}

impl Database {
    pub(crate) async fn connect(options: DatabaseConnectOptions) -> Result<Self, Error> {
        validate_queue_name(&options.name)?;
        if options.priorities.0 > options.priorities.1 {
            return Err(Error::Config(
                "queue priority range must have low <= high".into(),
            ));
        }
        validate_duration("sweep grace", options.sweep_grace)?;
        if options.sweep_batch_size == 0 {
            return Err(Error::Config(
                "sweep batch size must be greater than zero".into(),
            ));
        }
        if options.pool.is_none() {
            if options.max_connections == 0 {
                return Err(Error::Config(
                    "queue max_connections must be greater than zero".into(),
                ));
            }
            if options.min_connections > options.max_connections {
                return Err(Error::Config(
                    "queue min_connections must not exceed max_connections".into(),
                ));
            }
        }

        let pool = match options.pool {
            Some(pool) => pool,
            None => {
                PgPoolOptions::new()
                    .min_connections(options.min_connections)
                    .max_connections(options.max_connections)
                    .connect(&options.url)
                    .await?
            }
        };

        let server = sqlx::query_as::<_, DatabaseServer>(
            "SELECT current_setting('server_version_num')::int AS version, current_database() AS database"
        )
        .fetch_one(&pool)
        .await?;
        if server.version < 180_000 {
            return Err(Error::Config(format!(
                "pgqueue requires PostgreSQL 18+; server_version_num = {}",
                server.version
            )));
        }

        match options.migration_mode {
            MigrationMode::Apply => MIGRATOR.run(&pool).await.map_err(Error::Migration)?,
            MigrationMode::Validate => validate_migrations(&pool).await?,
            MigrationMode::Skip => {}
        }

        Ok(Self {
            notify_channel: channel_name(&options.name, ""),
            done_channel: done_channel(&options.name),
            dedupe_enqueue_lock_key: dedupe_enqueue_lock_key(&server.database),
            sweep_lock_key: sweep_lock_key(&server.database, &options.name),
            pool,
            name: options.name,
            priorities: options.priorities,
            sweep_grace: options.sweep_grace,
            sweep_batch_size: i64::from(options.sweep_batch_size),
            counters: QueueCounters::default(),
            notify_listener: std::sync::OnceLock::new(),
        })
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub(crate) fn sweep_lock_key(&self) -> i64 {
        self.sweep_lock_key
    }

    pub(crate) fn sweep_grace(&self) -> Duration {
        self.sweep_grace
    }

    pub(crate) fn sweep_batch_size(&self) -> i64 {
        self.sweep_batch_size
    }

    pub(crate) fn notify_channel(&self) -> &str {
        &self.notify_channel
    }

    pub(crate) fn done_channel(&self) -> &str {
        &self.done_channel
    }

    pub(crate) fn notify_listener(&self) -> &QueueNotifyListener {
        self.notify_listener
            .get_or_init(|| QueueNotifyListener::start(self))
    }

    pub(crate) fn sweeper(self: &std::sync::Arc<Self>) -> Sweeper {
        Sweeper::new(std::sync::Arc::clone(self))
    }

    pub(crate) fn stats(&self) -> QueueStats {
        self.counters.snapshot()
    }

    fn ensure_owns(&self, job: &JobRow) -> Result<(), Error> {
        if job.queue == self.name {
            return Ok(());
        }
        Err(Error::Config(format!(
            "job {} belongs to queue {:?}, not {:?}",
            job.id, job.queue, self.name
        )))
    }

    pub(crate) async fn enqueue_raw_delayed_result(
        &self,
        job: JobRequest,
        delay: Option<Duration>,
    ) -> Result<DatabaseEnqueueResult, Error> {
        // Before a connection is taken, on both branches. Behind `pool.begin()`
        // the dedupe path answered identical invalid input with whatever the
        // pool said — `Error::Db(PoolTimedOut)` under load — while the keyless
        // path answered `Error::Config`, so a permanently invalid job looked
        // retryable purely because it carried a dedupe key.
        job.validate()?;
        if let Some(delay) = delay {
            validate_duration("job delay", delay)?;
        }
        if job.dedupe_key.is_some() {
            let mut transaction = self.pool.begin().await?;
            let result = self
                .enqueue_raw_delayed_in_result(&mut transaction, job, delay)
                .await?;
            transaction.commit().await?;
            return Ok(result);
        }

        let backoff = serde_json::to_value(job.config.backoff)?;
        let id = self
            .insert_job(
                &job,
                &backoff,
                job.config.timeout.map(duration_to_ms),
                duration_to_ms(job.config.retry_delay),
                job.config.retention.as_result_ttl_ms(),
                delay.map(duration_to_ms),
                &self.pool,
            )
            .await?;
        // `insert_job`'s only conflict target is the partial dedupe-key index,
        // whose predicate excludes keyless rows, so this insert always returns.
        match id {
            Some(id) => Ok(DatabaseEnqueueResult::Inserted(id)),
            None => unreachable!("a keyless insert has no conflict target"),
        }
    }

    pub(crate) async fn enqueue_raw_delayed_in_result(
        &self,
        transaction: &mut sqlx::PgTransaction<'_>,
        job: JobRequest,
        delay: Option<Duration>,
    ) -> Result<DatabaseEnqueueResult, Error> {
        job.validate()?;
        if let Some(delay) = delay {
            validate_duration("job delay", delay)?;
        }
        let backoff = serde_json::to_value(job.config.backoff)?;
        let timeout_ms = job.config.timeout.map(duration_to_ms);
        let retry_delay_ms = duration_to_ms(job.config.retry_delay);
        let result_ttl_ms = job.config.retention.as_result_ttl_ms();
        let delay_ms = delay.map(duration_to_ms);

        if let Some(dedupe_key) = job.dedupe_key.as_deref() {
            sqlx::query(
                "SELECT pg_advisory_xact_lock($1, hashtext(length($2)::text || ':' || $2 || $3))",
            )
            .bind(self.dedupe_enqueue_lock_key)
            .bind(&self.name)
            .bind(dedupe_key)
            .execute(&mut **transaction)
            .await?;

            // The advisory transaction lock serializes enqueue decisions. A
            // plain read deliberately avoids pinning the existing row against
            // worker finalization for the caller transaction's lifetime.
            if let Some(row) = self.live_dedupe_owner(dedupe_key, transaction).await? {
                return Ok(deduplicated(row));
            }
        }

        let id = self
            .insert_job(
                &job,
                &backoff,
                timeout_ms,
                retry_delay_ms,
                result_ttl_ms,
                delay_ms,
                &mut **transaction,
            )
            .await?;
        match (id, job.dedupe_key.as_deref()) {
            (Some(id), _) => Ok(DatabaseEnqueueResult::Inserted(id)),
            // The insert's only conflict target is the partial dedupe-key index,
            // and the guarded read above found no such row — but they are two
            // statements, and the advisory lock they run under binds only
            // writers that take it. Anything writing `pgqueue.jobs` directly
            // (application SQL, a backfill, an ops script) can commit a
            // conflicting row in between and leave `DO NOTHING` nothing to
            // return. That is an ordinary dedupe collision as far as the caller
            // is concerned, so re-read the holder and report it as one, exactly
            // as `schedule_cron` does.
            (None, Some(dedupe_key)) => {
                match self.live_dedupe_owner(dedupe_key, transaction).await? {
                    Some(row) => Ok(deduplicated(row)),
                    // The row that blocked the insert left the live statuses
                    // again before it could be named. Nothing here can name a
                    // job to deduplicate against, so the caller retries —
                    // which is why this is `DedupeRace`, not `Config`: the
                    // request itself is valid.
                    None => Err(Error::DedupeRace(format!(
                        "dedupe key {dedupe_key:?} was taken by a writer that did not take the \
                         enqueue lock, and released again before it could be reported; retry the \
                         enqueue"
                    ))),
                }
            }
            // Unreachable: a keyless insert matches no conflict target, so it
            // always returns its row.
            (None, None) => unreachable!("a keyless insert has no conflict target"),
        }
    }

    /// The live job holding `dedupe_key`, if any.
    async fn live_dedupe_owner(
        &self,
        dedupe_key: &str,
        transaction: &mut sqlx::PgTransaction<'_>,
    ) -> Result<Option<ExistingJob>, Error> {
        Ok(sqlx::query_as::<_, ExistingJob>(
            r#"
            SELECT id, name, result_ttl_ms FROM pgqueue.jobs
            WHERE queue = $1 AND dedupe_key = $2
              AND status IN ('queued', 'running', 'aborting')
            "#,
        )
        .bind(&self.name)
        .bind(dedupe_key)
        .fetch_optional(&mut **transaction)
        .await?)
    }

    pub(crate) async fn reconcile_cron(
        &self,
        entry: &JobCronEntry,
        now: DateTime<Utc>,
    ) -> Result<DatabaseCronAuthority, Error> {
        let revision = i64::try_from(entry.options.revision)
            .map_err(|_| Error::Config("cron revision must fit PostgreSQL bigint".into()))?;
        let next_run_at = entry.next_occurrence(now)?;
        let policy = entry.options.misfire.kind();
        let grace_ms = entry.options.misfire.grace_ms();
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            r#"
            INSERT INTO pgqueue.cron_schedules (
                queue, dedupe_key, name, expression, definition, revision,
                misfire_policy, grace_ms, next_run_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (queue, dedupe_key) DO UPDATE SET
                name = EXCLUDED.name,
                expression = EXCLUDED.expression,
                definition = EXCLUDED.definition,
                revision = EXCLUDED.revision,
                misfire_policy = EXCLUDED.misfire_policy,
                grace_ms = EXCLUDED.grace_ms,
                next_run_at = CASE
                    WHEN pgqueue.cron_schedules.expression = EXCLUDED.expression
                    THEN pgqueue.cron_schedules.next_run_at
                    ELSE EXCLUDED.next_run_at
                END,
                updated_at = now()
            WHERE pgqueue.cron_schedules.revision < EXCLUDED.revision
            "#,
        )
        .bind(&self.name)
        .bind(&entry.dedupe_key)
        .bind(&entry.template.name)
        .bind(&entry.expr)
        .bind(&entry.definition)
        .bind(revision)
        .bind(policy)
        .bind(grace_ms)
        .bind(next_run_at)
        .execute(&mut *tx)
        .await?;
        let authority = sqlx::query_as::<_, CronAuthority>(
            r#"
            SELECT name, expression, revision, misfire_policy, grace_ms,
                   definition = $3::jsonb AS definition_matches
            FROM pgqueue.cron_schedules
            WHERE queue = $1 AND dedupe_key = $2
            "#,
        )
        .bind(&self.name)
        .bind(&entry.dedupe_key)
        .bind(&entry.definition)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;

        if authority.revision > revision {
            return Ok(DatabaseCronAuthority::Inactive {
                revision: authority.revision,
            });
        }
        if authority.revision != revision
            || authority.name != entry.template.name
            || authority.expression != entry.expr
            || !authority.definition_matches
            || authority.misfire_policy != policy
            || authority.grace_ms != grace_ms
        {
            return Err(Error::Config(format!(
                "cron {:?} revision {} conflicts with the stored definition",
                entry.dedupe_key, revision
            )));
        }
        Ok(DatabaseCronAuthority::Active)
    }

    /// The live job holding `dedupe_key` in this queue, if one does.
    async fn dedupe_key_holder(
        &self,
        dedupe_key: &str,
        executor: &mut sqlx::PgConnection,
    ) -> Result<Option<DatabaseCronConflict>, Error> {
        Ok(sqlx::query_as::<_, DatabaseCronConflict>(
            r#"
            SELECT scheduled_at, kind, name FROM pgqueue.jobs
            WHERE queue = $1 AND dedupe_key = $2
              AND status IN ('queued', 'running', 'aborting')
            "#,
        )
        .bind(&self.name)
        .bind(dedupe_key)
        .fetch_optional(executor)
        .await?)
    }

    /// The subset of `dedupe_keys` a scheduling pass has anything to do for:
    /// the schedules that are due by `through`, plus every key with no schedule
    /// row at all. `None` uses the database's current time.
    /// A missing row is not skippable — [`Database::schedule_cron`] is where it
    /// becomes the error that degrades the worker's health and queues the key
    /// for reconciliation.
    ///
    /// One pooled statement per tick stands in for one transaction per cron per
    /// tick: `schedule_cron` opens a transaction and, on the overwhelmingly
    /// common `NotDue` path, rolls it straight back, so an idle registry spent
    /// `BEGIN`/`SELECT`/`ROLLBACK` per cron per worker per tick to learn
    /// nothing. This is only a pre-filter — `schedule_cron` re-reads the row
    /// under `FOR UPDATE SKIP LOCKED` and decides for itself, so a key that
    /// stops being due in between is refused there exactly as before.
    pub(crate) async fn due_crons(
        &self,
        dedupe_keys: &[String],
        through: Option<DateTime<Utc>>,
    ) -> Result<std::collections::HashSet<String>, Error> {
        Ok(sqlx::query_scalar::<_, String>(
            r#"
            SELECT k.dedupe_key
            FROM unnest($2::text[]) AS k(dedupe_key)
            LEFT JOIN pgqueue.cron_schedules s
                ON s.queue = $1 AND s.dedupe_key = k.dedupe_key
            WHERE s.dedupe_key IS NULL
               OR s.next_run_at <= COALESCE($3, now())
            "#,
        )
        .bind(&self.name)
        .bind(dedupe_keys)
        .bind(through)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .collect())
    }

    pub(crate) async fn schedule_cron(
        &self,
        entry: &JobCronEntry,
        through: Option<DateTime<Utc>>,
    ) -> Result<DatabaseCronScheduleResult, Error> {
        let revision = i64::try_from(entry.options.revision)
            .map_err(|_| Error::Config("cron revision must fit PostgreSQL bigint".into()))?;
        let policy = entry.options.misfire.kind();
        let grace_ms = entry.options.misfire.grace_ms();
        let mut tx = self.pool.begin().await?;
        let observed = sqlx::query_as::<_, ObservedCron>(
            r#"
            SELECT name, expression, revision, misfire_policy, grace_ms,
                   next_run_at, now() AS now,
                   definition = $3::jsonb AS definition_matches
            FROM pgqueue.cron_schedules
            WHERE queue = $1 AND dedupe_key = $2
            "#,
        )
        .bind(&self.name)
        .bind(&entry.dedupe_key)
        .bind(&entry.definition)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(observed) = observed else {
            tx.rollback().await?;
            return Err(Error::Config(format!(
                "cron schedule {:?} was not reconciled",
                entry.dedupe_key
            )));
        };
        if observed.revision != revision
            || observed.name != entry.template.name
            || observed.expression != entry.expr
            || !observed.definition_matches
            || observed.misfire_policy != policy
            || observed.grace_ms != grace_ms
        {
            tx.rollback().await?;
            return Ok(DatabaseCronScheduleResult::Inactive {
                revision: observed.revision,
            });
        }
        if observed.next_run_at > through.unwrap_or(observed.now) {
            tx.rollback().await?;
            return Ok(DatabaseCronScheduleResult::NotDue);
        }

        // A continuous scheduler must not let one locked row stall every cron,
        // so it skips contention and tries again on its next tick. A burst has
        // a finite scheduling boundary and no later tick: wait for rows that
        // were due at that boundary, then re-check the predicate after the lock
        // is acquired. If another scheduler advanced the cursor while we
        // waited, PostgreSQL re-evaluates the predicate and returns no row.
        let due = if let Some(through) = through {
            sqlx::query_scalar::<_, DateTime<Utc>>(
                r#"
                SELECT next_run_at
                FROM pgqueue.cron_schedules
                WHERE queue = $1 AND dedupe_key = $2
                  AND revision = $3 AND definition = $4
                  AND next_run_at <= $5
                FOR UPDATE
                "#,
            )
            .bind(&self.name)
            .bind(&entry.dedupe_key)
            .bind(revision)
            .bind(&entry.definition)
            .bind(through)
            .fetch_optional(&mut *tx)
            .await?
        } else {
            sqlx::query_scalar::<_, DateTime<Utc>>(
                r#"
                SELECT next_run_at
                FROM pgqueue.cron_schedules
                WHERE queue = $1 AND dedupe_key = $2
                  AND revision = $3 AND definition = $4
                  AND next_run_at <= now()
                FOR UPDATE SKIP LOCKED
                "#,
            )
            .bind(&self.name)
            .bind(&entry.dedupe_key)
            .bind(revision)
            .bind(&entry.definition)
            .fetch_optional(&mut *tx)
            .await?
        };
        let Some(due) = due else {
            tx.rollback().await?;
            return Ok(if through.is_some() {
                DatabaseCronScheduleResult::NotDue
            } else {
                DatabaseCronScheduleResult::Contended
            });
        };

        let stored_occurrence = due;
        sqlx::query(
            "SELECT pg_advisory_xact_lock($1, hashtext(length($2)::text || ':' || $2 || $3))",
        )
        .bind(self.dedupe_enqueue_lock_key)
        .bind(&self.name)
        .bind(&entry.dedupe_key)
        .execute(&mut *tx)
        .await?;
        // The dedupe-key lock may have been held by a long caller-owned
        // transaction. Use wall-clock database time after that wait so an
        // occurrence cannot be published after its grace or successor.
        let current = sqlx::query_scalar::<_, DateTime<Utc>>("SELECT clock_timestamp()")
            .fetch_one(&mut *tx)
            .await?;
        // Burst scheduling chooses an occurrence at its fixed boundary. The
        // actual clock still decides whether lock waiting made that occurrence
        // stale; importantly, it never moves the burst forward into a later
        // recurrence.
        let scheduling_time = through.unwrap_or(current);
        let (occurrence, successor, publish) = match entry.options.misfire {
            CronMisfirePolicy::Skip { .. } => {
                self.skip_catch_up(entry, stored_occurrence, scheduling_time)?
            }
            CronMisfirePolicy::FireOnce => {
                let occurrence = entry.previous_occurrence(scheduling_time)?;
                let successor = entry.next_occurrence(occurrence)?;
                (occurrence, successor, true)
            }
        };
        let publish = publish && current < entry.publication_deadline(occurrence, successor);
        let next_run_at = if publish {
            successor
        } else {
            entry.next_occurrence(scheduling_time)?
        };
        let claim_expires_at = successor.max(current + chrono::Duration::seconds(1));

        let claimed = sqlx::query_scalar::<_, bool>(
            r#"
            INSERT INTO pgqueue.cron_occurrences (
                queue, dedupe_key, scheduled_at, expires_at
            ) VALUES ($1, $2, $3, $4)
            ON CONFLICT DO NOTHING
            RETURNING true
            "#,
        )
        .bind(&self.name)
        .bind(&entry.dedupe_key)
        .bind(occurrence)
        .bind(claim_expires_at)
        .fetch_optional(&mut *tx)
        .await?
        .unwrap_or(false);

        let result = if !claimed {
            DatabaseCronScheduleResult::AlreadyPublished { occurrence }
        } else if !publish {
            DatabaseCronScheduleResult::SkippedStale { occurrence }
        } else if let Some(holder) = self.dedupe_key_holder(&entry.dedupe_key, &mut tx).await? {
            DatabaseCronScheduleResult::SkippedHeld {
                occurrence,
                existing: holder,
            }
        } else {
            let job = entry.job_for(occurrence);
            let backoff = serde_json::to_value(job.config.backoff)?;
            let inserted = sqlx::query_scalar::<_, Uuid>(
                r#"
                WITH inserted AS (
                    INSERT INTO pgqueue.jobs (
                        queue, name, payload, dedupe_key, priority,
                        max_attempts, timeout_ms, retry_delay_ms,
                        backoff, result_ttl_ms, scheduled_at, enqueued_at, meta, kind, cron_expr
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8,
                            $9, $10, $11, clock_timestamp(), $12, 'cron', $13)
                    ON CONFLICT (queue, dedupe_key) WHERE dedupe_key IS NOT NULL
                        AND status IN ('queued', 'running', 'aborting') DO NOTHING
                    RETURNING id
                )
                SELECT id, pg_notify($14, 'enqueue') IS NULL AS notified
                FROM inserted
                "#,
            )
            .bind(&self.name)
            .bind(&job.name)
            .bind(&job.payload)
            .bind(&job.dedupe_key)
            .bind(job.config.priority)
            .bind(job.config.max_attempts as i32)
            .bind(job.config.timeout.map(duration_to_ms))
            .bind(duration_to_ms(job.config.retry_delay))
            .bind(&backoff)
            .bind(job.config.retention.as_result_ttl_ms())
            .bind(occurrence)
            .bind(&job.meta)
            .bind(&entry.expr)
            .bind(&self.notify_channel)
            .fetch_optional(&mut *tx)
            .await?;
            // The only conflict target is the partial dedupe-key index over
            // `queued`/`running`/`aborting`, and the query just above found no
            // such row — but the two are separate statements in one READ
            // COMMITTED transaction, and the advisory lock they run under binds
            // only writers that take it. Anything writing `pgqueue.jobs`
            // directly (application SQL, a backfill, an ops script) can commit a
            // conflicting row in between, leaving `DO NOTHING` nothing to
            // return. Re-read the holder and report it, exactly as the branch
            // above does: this runs in the worker's schedule loop, where a panic
            // takes the whole worker down instead of degrading the scheduler.
            // Reporting it as `SkippedStale` would point the operator at misfire
            // grace instead of at the live holder, and unlike `SkippedHeld` that
            // warning is not de-duplicated, so it would repeat every tick.
            match inserted {
                Some(id) => DatabaseCronScheduleResult::Published { id, occurrence },
                None => match self.dedupe_key_holder(&entry.dedupe_key, &mut tx).await? {
                    Some(holder) => DatabaseCronScheduleResult::SkippedHeld {
                        occurrence,
                        existing: holder,
                    },
                    // The row that blocked the insert left the live statuses
                    // again before it could be named. Rolling back releases this
                    // occurrence's claim too, so the next tick republishes it —
                    // and `DedupeRace`, not `Config`, keeps the scheduler's
                    // "`Config` is permanent" taxonomy intact.
                    None => {
                        tx.rollback().await?;
                        return Err(Error::DedupeRace(format!(
                            "cron {:?} lost its dedupe key to a writer that did not take the \
                             enqueue lock; the occurrence will be retried",
                            entry.dedupe_key
                        )));
                    }
                },
            }
        };

        // `FOR UPDATE` above pinned this row for the rest of the transaction,
        // and it already matched this revision and definition, so the primary
        // key alone identifies it and the update always lands. Re-stating the
        // revision/definition guards here would only add an outcome that cannot
        // occur and so can never be tested.
        sqlx::query(
            r#"
            UPDATE pgqueue.cron_schedules
            SET next_run_at = $3, updated_at = now()
            WHERE queue = $1 AND dedupe_key = $2
            "#,
        )
        .bind(&self.name)
        .bind(&entry.dedupe_key)
        .bind(next_run_at)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(result)
    }

    /// Which occurrence a [`CronMisfirePolicy::Skip`] schedule publishes now:
    /// `(occurrence, its successor, whether to publish it)`.
    ///
    /// The durable cursor is the first candidate. When it is more than one
    /// period stale — a restart, a leader handover, or a deploy gap — refusing
    /// it and jumping straight to the next occurrence silently threw away the
    /// *most recent* occurrence even while it was still well inside its own
    /// grace, so every catch-up cost one extra occurrence with no job row, no
    /// claim, and no `SkippedStale` warning. So fall back to that occurrence
    /// when its own publication deadline has not passed.
    ///
    /// This terminates: the fallback is strictly newer than the stored cursor
    /// and its successor is strictly after `current`, and the claim row keeps a
    /// concurrent scheduler from publishing it twice.
    fn skip_catch_up(
        &self,
        entry: &JobCronEntry,
        stored_occurrence: DateTime<Utc>,
        current: DateTime<Utc>,
    ) -> Result<(DateTime<Utc>, DateTime<Utc>, bool), Error> {
        let successor = entry.next_occurrence(stored_occurrence)?;
        if current < entry.publication_deadline(stored_occurrence, successor) {
            return Ok((stored_occurrence, successor, true));
        }
        let recent = entry.previous_occurrence(current)?;
        if recent > stored_occurrence {
            let recent_successor = entry.next_occurrence(recent)?;
            if current < entry.publication_deadline(recent, recent_successor) {
                return Ok((recent, recent_successor, true));
            }
        }
        Ok((stored_occurrence, successor, false))
    }

    /// Inserts a plain (non-cron) job and emits its enqueue notification as
    /// one statement, so the keyless path costs a single round trip.
    #[allow(clippy::too_many_arguments)]
    async fn insert_job<'e>(
        &self,
        job: &JobRequest,
        backoff: &Value,
        timeout_ms: Option<i64>,
        retry_delay_ms: i64,
        result_ttl_ms: Option<i64>,
        delay_ms: Option<i64>,
        executor: impl sqlx::PgExecutor<'e>,
    ) -> Result<Option<Uuid>, Error> {
        let row = sqlx::query_scalar::<_, Uuid>(
            r#"
            WITH inserted AS (
                INSERT INTO pgqueue.jobs (
                    queue, name, payload, dedupe_key, priority, max_attempts,
                    timeout_ms, retry_delay_ms, backoff, result_ttl_ms,
                    scheduled_at, enqueued_at, meta, kind, cron_expr
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                        COALESCE(
                            $11,
                            statement_timestamp() + ($13::bigint * interval '1 millisecond'),
                            statement_timestamp()
                        ),
                        statement_timestamp(), $12, 'job', NULL)
                ON CONFLICT (queue, dedupe_key) WHERE dedupe_key IS NOT NULL
                    AND status IN ('queued', 'running', 'aborting') DO NOTHING
                RETURNING id
            )
            SELECT id, pg_notify($14, 'enqueue') IS NULL AS notified
            FROM inserted
            "#,
        )
        .bind(&self.name)
        .bind(&job.name)
        .bind(&job.payload)
        .bind(&job.dedupe_key)
        .bind(job.config.priority)
        .bind(job.config.max_attempts as i32)
        .bind(timeout_ms)
        .bind(retry_delay_ms)
        .bind(backoff)
        .bind(result_ttl_ms)
        .bind(job.scheduled_at)
        .bind(&job.meta)
        .bind(delay_ms)
        .bind(&self.notify_channel)
        .fetch_optional(executor)
        .await?;
        Ok(row)
    }
}

impl Database {
    pub(crate) async fn jobs_page(
        &self,
        status: Option<&str>,
        name: Option<&str>,
        limit: i64,
        before: Option<JobCursor>,
    ) -> Result<Vec<JobRow>, Error> {
        let (before_enqueued_at, before_id) = before
            .map(|cursor| (Some(cursor.enqueued_at), Some(cursor.id)))
            .unwrap_or((None, None));
        Ok(sqlx::query_as::<_, JobRow>(
            r#"
            SELECT id, dedupe_key, queue, name, payload,
                   status, priority, attempts,
                   max_attempts, timeout_ms, retry_delay_ms,
                   backoff, result_ttl_ms, scheduled_at,
                   enqueued_at, started_at, touched_at, completed_at, expires_at,
                   result, error, meta, worker_id
            FROM pgqueue.jobs
            WHERE queue = $1
              AND ($2::text IS NULL OR status = $2)
              AND ($3::text IS NULL OR name = $3)
              AND ($5::timestamptz IS NULL OR (enqueued_at, id) < ($5, $6))
            ORDER BY enqueued_at DESC, id DESC
            LIMIT $4
            "#,
        )
        .bind(&self.name)
        .bind(status)
        .bind(name)
        .bind(limit)
        .bind(before_enqueued_at)
        .bind(before_id)
        .fetch_all(&self.pool)
        .await?)
    }

    /// Five independent scalar aggregates, not five `FILTER`s over one scan.
    /// A shared `FROM pgqueue.jobs WHERE queue = $1` is a sequential scan of
    /// the queue's whole retained history — overwhelmingly `complete` rows,
    /// which no counter here reports — so its cost grew with throughput times
    /// retention, and was unbounded under `JobRetention::Forever`. Split, each
    /// counter carries its own status predicate and is served by an existing
    /// index: `jobs_dashboard_ready_idx` for the two `queued` halves,
    /// `jobs_active_idx` for `running`, `jobs_dashboard_failure_idx` for
    /// `failed`, and a skip scan of `jobs_dashboard_name_prefix_idx` for
    /// `aborted`. One statement is one snapshot and one `now()`, so the halves
    /// still partition the `queued` rows exactly as the single scan did.
    pub(crate) async fn counts(&self) -> Result<QueueCounts, Error> {
        Ok(sqlx::query_as::<_, QueueCounts>(
            r#"
            SELECT
                (SELECT COUNT(*) FROM pgqueue.jobs
                  WHERE queue = $1 AND status = 'queued'
                    AND scheduled_at <= now()) AS queued,
                (SELECT COUNT(*) FROM pgqueue.jobs
                  WHERE queue = $1 AND status IN ('running', 'aborting')) AS running,
                (SELECT COUNT(*) FROM pgqueue.jobs
                  WHERE queue = $1 AND status = 'queued'
                    AND scheduled_at > now()) AS scheduled,
                (SELECT COUNT(*) FROM pgqueue.jobs
                  WHERE queue = $1 AND status = 'failed') AS failed,
                (SELECT COUNT(*) FROM pgqueue.jobs
                  WHERE queue = $1 AND status = 'aborted') AS aborted
            "#,
        )
        .bind(&self.name)
        .fetch_one(&self.pool)
        .await?)
    }

    pub(crate) async fn workers(&self) -> Result<Vec<WorkerInfo>, Error> {
        Ok(sqlx::query_as::<_, WorkerInfo>(
            r#"
            SELECT id, queue, stats, metadata, started_at, heartbeat_at, expires_at
            FROM pgqueue.workers
            WHERE queue = $1 AND expires_at > now()
            ORDER BY started_at
            "#,
        )
        .bind(&self.name)
        .fetch_all(&self.pool)
        .await?)
    }

    pub(crate) async fn write_worker_info(
        &self,
        worker_id: Uuid,
        stats: Value,
        metadata: Option<Value>,
        ttl: Duration,
        intake: LeaseIntake,
    ) -> Result<(), Error> {
        validate_duration("worker info TTL", ttl)?;
        // Guarded here rather than in the builder alone, because `Consumer::
        // heartbeat` is a public writer of both columns and a document nested
        // past what `serde_json` can read back poisons far more than its own
        // row: `workers` decodes every live lease of the queue in one statement,
        // so `Queue::info` and both dashboard worker views fail for the whole
        // queue for as long as the lease is renewed.
        //
        // A NUL is refused for the same reason `validate_finalization` refuses
        // one: `jsonb` cannot hold it, so the write raises `22P05` — an
        // `Error::Db` indistinguishable from the transient failures a heartbeat
        // loop is built to retry. Spinning on it renews nothing, so every
        // attempt the caller has claimed is reclaimed by the sweeper once the
        // lease expires. Depth first, because that walk is the bounded one and
        // it keeps `json_contains_nul`'s unbounded recursion safe.
        for (field, value) in [
            ("worker stats", Some(&stats)),
            ("worker metadata", metadata.as_ref()),
        ] {
            if value.is_some_and(|value| json_exceeds_depth(value, MAX_JSON_DEPTH)) {
                return Err(Error::Config(format!(
                    "{field} must not nest deeper than {MAX_JSON_DEPTH} levels"
                )));
            }
            if value.is_some_and(json_contains_nul) {
                return Err(Error::Config(format!("{field} must not contain NUL")));
            }
        }
        let written = sqlx::query_scalar::<_, Uuid>(
            r#"
            INSERT INTO pgqueue.workers (id, queue, stats, metadata, expires_at, accepting)
            VALUES ($1, $2, $3, $5, now() + ($4::bigint * interval '1 millisecond'), $7)
            ON CONFLICT (id) DO UPDATE SET
                stats = $3, metadata = $5, heartbeat_at = now(),
                expires_at = now() + ($4::bigint * interval '1 millisecond'),
                accepting = CASE WHEN $6 THEN true ELSE pgqueue.workers.accepting END
            WHERE pgqueue.workers.queue = EXCLUDED.queue
            RETURNING id
            "#,
        )
        .bind(worker_id)
        .bind(&self.name)
        .bind(stats)
        .bind(duration_to_ms(ttl))
        .bind(metadata)
        .bind(intake.reopens())
        .bind(intake.accepts_when_created())
        .fetch_optional(&self.pool)
        .await?;
        if written.is_none() {
            return Err(Error::Config(format!(
                "worker id {worker_id} already belongs to a different queue"
            )));
        }
        Ok(())
    }

    pub(crate) async fn stop_worker_intake(&self, worker_id: Uuid) -> Result<(), Error> {
        sqlx::query(
            r#"
            UPDATE pgqueue.workers SET accepting = false, heartbeat_at = now()
            WHERE id = $1 AND queue = $2
            "#,
        )
        .bind(worker_id)
        .bind(&self.name)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Reads the rows behind `worker_id`'s in-flight attempts and sorts them
    /// into the three states that end an attempt early.
    ///
    /// A row whose `(attempts, worker_id)` no longer match the claim is
    /// reported as superseded: recovery took the attempt away — by requeueing
    /// the row, which clears `worker_id`, or by letting a later dequeue claim
    /// it with `attempts + 1` — so the row is queued or running for someone
    /// else. That state is neither `aborting` nor missing, and every write the
    /// displaced attempt could still make is guarded out by the same pair, so
    /// it has to be cancelled here or it keeps its processor slot until it
    /// returns on its own — never, when its timeout is disabled.
    ///
    /// Claims are matched against the rows without consuming them: the same id
    /// can arrive under two attempt numbers when this worker re-claimed a row
    /// recovery had taken from it, and the second claim must be answered from
    /// the same row as the first rather than reported missing.
    pub(crate) async fn aborting_of(
        &self,
        claims: &[DatabaseAbortClaim],
        worker_id: Uuid,
    ) -> Result<DatabaseAbortPoll, Error> {
        let ids = claims.iter().map(|claim| claim.id).collect::<Vec<_>>();
        let rows = sqlx::query_as::<_, AbortPollRow>(
            r#"
            SELECT id, status, attempts, worker_id, error, result FROM pgqueue.jobs
            WHERE id = ANY($1) AND queue = $2
            "#,
        )
        .bind(&ids)
        .bind(&self.name)
        .fetch_all(&self.pool)
        .await?;
        let present = rows
            .into_iter()
            .map(|row| (row.id, row))
            .collect::<std::collections::HashMap<_, _>>();
        let mut aborting = Vec::new();
        let mut missing = Vec::new();
        let mut superseded = Vec::new();
        for claim in claims {
            match present.get(&claim.id) {
                None => missing.push(*claim),
                Some(row) if row.attempts != claim.attempts || row.worker_id != Some(worker_id) => {
                    superseded.push(*claim);
                }
                Some(row) if matches!(row.status, JobStatus::Aborting | JobStatus::Aborted) => {
                    aborting.push(DatabaseAbortingAttempt {
                        swept: is_swept_marked(row.error.as_deref(), row.result.as_ref()),
                        id: row.id,
                        attempts: row.attempts,
                        reason: row.error.clone(),
                    });
                }
                // Still running as claimed: nothing to signal.
                Some(_) => {}
            }
        }
        Ok(DatabaseAbortPoll {
            aborting,
            missing,
            superseded,
        })
    }

    pub(crate) async fn now(&self) -> Result<DateTime<Utc>, Error> {
        Ok(sqlx::query_scalar::<_, DateTime<Utc>>("SELECT now()")
            .fetch_one(&self.pool)
            .await?)
    }

    async fn notify(
        &self,
        tx: &mut sqlx::PgTransaction<'_>,
        channel: &str,
        payload: &str,
    ) -> Result<(), Error> {
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(channel)
            .bind(payload)
            .execute(&mut **tx)
            .await?;
        Ok(())
    }
}

impl Database {
    /// Requeues an attempt the sweeper marked for abort, on behalf of the
    /// worker that still owns it. The sweeper's own recovery of an abandoned
    /// attempt goes through [`Database::retry_swept_abandoned_batch`], which
    /// carries the extra stuckness and dead-owner guards that path needs.
    ///
    /// `error` is what the attempt ended with, when it ended with something the
    /// operator needs to see: a handler failure that raced the sweeper's abort
    /// is still a real failure, and storing it is what keeps the retry-backoff
    /// window and the next attempt from reporting the sweeper's internal
    /// `swept` marker as the reason. `None` — the attempt the sweeper itself
    /// ended — keeps that marker, which is the accurate reason there.
    pub(crate) async fn retry_swept(
        &self,
        job: &JobRow,
        error: Option<&str>,
    ) -> Result<bool, Error> {
        let guards = DatabaseRequeueGuards {
            allow_running: false,
            allow_swept_abort: true,
            refund_attempt: false,
        };
        let updated = self
            .requeue_guarded(
                AttemptGuard::from(job),
                error,
                job.next_retry_delay(),
                guards,
            )
            .await?;
        if updated {
            self.counters.record_retry();
        }
        Ok(updated)
    }

    pub(crate) async fn job(&self, id: Uuid) -> Result<Option<JobRow>, Error> {
        Ok(sqlx::query_as::<_, JobRow>(
            r#"
            SELECT id, dedupe_key, queue, name, payload,
                   status, priority, attempts,
                   max_attempts, timeout_ms, retry_delay_ms,
                   backoff, result_ttl_ms, scheduled_at,
                   enqueued_at, started_at, touched_at, completed_at, expires_at,
                   result, error, meta, worker_id
            FROM pgqueue.jobs WHERE id = $1 AND queue = $2
            "#,
        )
        .bind(id)
        .bind(&self.name)
        .fetch_optional(&self.pool)
        .await?)
    }

    /// A sweeper-marked `aborting` row is claimed too: the sweeper's pending
    /// retry would otherwise run the job again with the abort silently
    /// dropped. Storing the reason and clearing the marker is what converts
    /// that retry intent into a user abort — every downstream requeue guard
    /// keys on the marker pair, so the row can only finish `aborted` from
    /// here. A row already `aborting` for a user abort carries no marker and
    /// is left alone.
    pub(crate) async fn abort(&self, id: Uuid, reason: &str) -> Result<bool, Error> {
        let payload = format!(r#"{{"id":"{id}","status":"aborted"}}"#);
        let row = sqlx::query_as::<_, AbortResult>(
            r#"
            WITH updated AS (
                UPDATE pgqueue.jobs
                SET status = CASE WHEN status = 'queued' THEN 'aborted' ELSE 'aborting' END,
                    error = $2, touched_at = now(),
                    result = CASE WHEN status = 'aborting' THEN NULL ELSE result END,
                    completed_at = CASE WHEN status = 'queued' THEN now() ELSE completed_at END,
                    expires_at = CASE WHEN status = 'queued' AND result_ttl_ms IS NOT NULL
                        THEN now() + (result_ttl_ms * interval '1 millisecond') ELSE expires_at END
                WHERE id = $1 AND queue = $3
                  AND (status IN ('queued', 'running')
                       OR (status = 'aborting' AND error = $6 AND result = $7))
                RETURNING status
            )
            SELECT status,
                   (CASE WHEN status = 'aborted' THEN pg_notify($4, $5) END) IS NULL
                       AS notify_skipped
            FROM updated
            "#,
        )
        .bind(id)
        .bind(reason)
        .bind(&self.name)
        .bind(&self.done_channel)
        .bind(payload)
        .bind(SWEPT)
        .bind(swept_marker())
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(false);
        };
        if row.status == "aborted" {
            self.counters.record_abort();
        }
        tracing::debug!(job.id = %id, status = %row.status, queue = %self.name, "abort requested");
        Ok(true)
    }

    pub(crate) async fn retry_job_occurrence(
        &self,
        id: Uuid,
        reason: &str,
    ) -> Result<Option<Uuid>, Error> {
        // A cron occurrence's dedupe key belongs to the schedule loop's
        // dedupe: carrying it onto a manual retry would collide with the
        // next scheduled occurrence and silently refuse the retry, so cron
        // retries run as keyless one-offs.
        let mut tx = self.pool.begin().await?;
        let new_id = sqlx::query_scalar::<_, Uuid>(
            r#"
            WITH source AS MATERIALIZED (
                UPDATE pgqueue.jobs SET retried_at = now()
                WHERE id = $1 AND queue = $3
                  AND status IN ('complete', 'failed', 'aborted') AND retried_at IS NULL
                RETURNING queue, name, payload,
                          CASE WHEN kind = 'cron' THEN NULL
                               ELSE dedupe_key END AS dedupe_key,
                          priority, attempts, timeout_ms, retry_delay_ms, backoff,
                          result_ttl_ms, meta, kind, cron_expr
            ), locked AS MATERIALIZED (
                SELECT pg_advisory_xact_lock($4,
                    hashtext(length(queue)::text || ':' || queue || dedupe_key))
                FROM source WHERE dedupe_key IS NOT NULL
            ), wall_clock AS MATERIALIZED (
                SELECT clock_timestamp() AS current
                FROM source LEFT JOIN locked ON true
            )
            INSERT INTO pgqueue.jobs (
                queue, name, payload, dedupe_key, priority, attempts,
                max_attempts, timeout_ms, retry_delay_ms, backoff,
                result_ttl_ms, scheduled_at, enqueued_at, meta, error, kind, cron_expr
            )
            SELECT queue, name, payload, dedupe_key, priority, attempts,
                   attempts + 1, timeout_ms, retry_delay_ms, backoff,
                   result_ttl_ms, wall_clock.current, wall_clock.current, meta, $2, kind, cron_expr
            FROM source JOIN wall_clock ON true
            ON CONFLICT (queue, dedupe_key) WHERE dedupe_key IS NOT NULL
                AND status IN ('queued', 'running', 'aborting') DO NOTHING
            RETURNING id
            "#,
        )
        .bind(id)
        .bind(reason)
        .bind(&self.name)
        .bind(self.dedupe_enqueue_lock_key)
        .fetch_optional(&mut *tx)
        .await?;
        if new_id.is_some() {
            self.notify(&mut tx, &self.notify_channel, "enqueue")
                .await?;
            tx.commit().await?;
            self.counters.record_retry();
        } else {
            tx.rollback().await?;
        }
        Ok(new_id)
    }
}

impl Database {
    /// Claims jobs for a custom consumer. Like the worker path, this requires a
    /// live, accepting `pgqueue.workers` lease for `worker_id`: without one the
    /// sweeper would treat the claim as abandoned and hand the job to someone
    /// else while it is still running.
    pub(crate) async fn dequeue_consumer(
        &self,
        limit: i64,
        worker_id: Uuid,
    ) -> Result<Vec<JobRow>, Error> {
        Ok(self
            .dequeue_inner(limit, worker_id, true, None, false)
            .await?
            .jobs)
    }

    /// Claims jobs without requiring a lease. Only [`crate::__test_support`]
    /// reaches this; every supported entry point goes through a lease-checked
    /// path.
    #[cfg(feature = "_test")]
    pub(crate) async fn dequeue_unleased(
        &self,
        limit: i64,
        worker_id: Uuid,
    ) -> Result<Vec<JobRow>, Error> {
        Ok(self
            .dequeue_inner(limit, worker_id, false, None, false)
            .await?
            .jobs)
    }

    pub(crate) async fn dequeue_worker(
        &self,
        limit: i64,
        worker_id: Uuid,
        registered_names: &[String],
        probe_unhandled: bool,
    ) -> Result<DatabaseDequeueBatch, Error> {
        self.dequeue_inner(
            limit,
            worker_id,
            true,
            Some(registered_names),
            probe_unhandled,
        )
        .await
    }

    async fn dequeue_inner(
        &self,
        limit: i64,
        worker_id: Uuid,
        require_open_intake: bool,
        registered_names: Option<&[String]>,
        probe_unhandled: bool,
    ) -> Result<DatabaseDequeueBatch, Error> {
        if limit <= 0 {
            return Err(Error::Config(
                "dequeue limit must be greater than zero".into(),
            ));
        }

        let mut jobs = sqlx::query_as::<_, JobRow>(
            r#"
            WITH candidates AS (
                SELECT job.id FROM pgqueue.jobs job
                WHERE job.queue = $1 AND job.status = 'queued'
                  AND job.scheduled_at <= now()
                  AND job.priority BETWEEN $2 AND $3
                  AND ($7::text[] IS NULL OR job.name = ANY($7))
                ORDER BY job.priority, job.scheduled_at, job.id
                LIMIT $4
                FOR UPDATE OF job SKIP LOCKED
            ), updated AS (
                UPDATE pgqueue.jobs job
                SET status = 'running', attempts = job.attempts + 1,
                    started_at = now(), touched_at = now(), worker_id = $5
                FROM candidates
                WHERE job.id = candidates.id AND job.queue = $1
                  AND job.status = 'queued'
                  AND job.scheduled_at <= now()
                  AND job.priority BETWEEN $2 AND $3
                  AND ($7::text[] IS NULL OR job.name = ANY($7))
                  AND (NOT $6 OR EXISTS (
                      SELECT 1 FROM pgqueue.workers worker
                      WHERE worker.id = $5 AND worker.queue = $1
                        AND worker.accepting AND worker.expires_at > now()
                  ))
                RETURNING job.id, job.dedupe_key, job.queue, job.name,
                          job.payload, job.status, job.priority,
                          job.attempts, job.max_attempts, job.timeout_ms,
                          job.retry_delay_ms, job.backoff,
                          job.result_ttl_ms, job.scheduled_at, job.enqueued_at,
                          job.started_at, job.touched_at, job.completed_at,
                          job.expires_at, job.result, job.error, job.meta,
                          job.worker_id
            )
            SELECT id, dedupe_key, queue, name, payload,
                   status, priority, attempts,
                   max_attempts, timeout_ms, retry_delay_ms,
                   backoff, result_ttl_ms, scheduled_at,
                   enqueued_at, started_at, touched_at, completed_at, expires_at,
                   result, error, meta, worker_id
            FROM updated
            "#,
        )
        .bind(&self.name)
        .bind(self.priorities.0)
        .bind(self.priorities.1)
        .bind(limit)
        .bind(worker_id)
        .bind(require_open_intake)
        .bind(registered_names)
        .fetch_all(&self.pool)
        .await?;

        // The underfilled-batch probes are their own statement, run after the
        // claim has committed: they need no consistency with the batch, and
        // folding them into the statement above would keep its transaction —
        // and the `FOR UPDATE` row locks it holds — open across two more scans
        // before the claim commits. The unhandled-names scan is the expensive
        // part, so it only runs when the caller's rate-limited warning is due.
        //
        // Only the worker fetch loop consumes the probe: it drives demand from
        // `work_available` and warns on `unhandled_names`, both of which are
        // defined in terms of the handler names it registered. A caller with no
        // registered names (the custom-consumer path) would pay a second round
        // trip per dequeue for a `work_available` that is structurally false —
        // and, on an empty batch, turn a failure of that purely diagnostic
        // query into a hard error.
        let batch_underfilled = i64::try_from(jobs.len()).is_ok_and(|fetched| fetched < limit);
        let probe = if let Some(names) = registered_names.filter(|_| batch_underfilled) {
            let probe = sqlx::query_as::<_, DatabaseDequeueProbe>(
                r#"
                SELECT
                    EXISTS (
                        SELECT 1 FROM pgqueue.workers
                        WHERE id = $2 AND queue = $1
                          AND accepting AND expires_at > now()
                    ) AS intake_open,
                    EXISTS (
                        SELECT 1 FROM pgqueue.jobs job
                        WHERE job.queue = $1 AND job.status = 'queued'
                          AND job.scheduled_at <= now()
                          AND job.priority BETWEEN $4 AND $5
                          AND job.name = ANY($3)
                    ) AS work_available,
                    CASE WHEN $6 THEN ARRAY(
                        SELECT name FROM (
                            SELECT DISTINCT name FROM pgqueue.jobs
                            WHERE queue = $1 AND status = 'queued'
                              AND scheduled_at <= now()
                              AND priority BETWEEN $4 AND $5
                              AND NOT (name = ANY($3))
                        ) unhandled ORDER BY name LIMIT 10
                    ) ELSE ARRAY[]::text[] END AS unhandled_names
                "#,
            )
            .bind(&self.name)
            .bind(worker_id)
            .bind(names)
            .bind(self.priorities.0)
            .bind(self.priorities.1)
            .bind(probe_unhandled)
            .fetch_one(&self.pool)
            .await;
            resolve_post_commit_probe(&self.name, worker_id, jobs.len(), probe)?
        } else {
            DatabaseDequeueProbe {
                intake_open: true,
                work_available: false,
                unhandled_names: Vec::new(),
            }
        };

        jobs.sort_by(|a, b| {
            (a.priority, a.scheduled_at, a.id).cmp(&(b.priority, b.scheduled_at, b.id))
        });
        Ok(DatabaseDequeueBatch {
            jobs,
            intake_open: probe.intake_open,
            work_available: probe.work_available,
            unhandled_names: probe.unhandled_names,
        })
    }

    pub(crate) async fn finish(
        &self,
        job: &JobRow,
        status: JobStatus,
        result: Option<Value>,
        error: Option<&str>,
    ) -> Result<bool, Error> {
        self.ensure_owns(job)?;
        validate_finalization(result.as_ref(), error)?;
        self.finish_with_guards(AttemptGuard::from(job), status, &result, error)
            .await
    }

    /// Requeues a batch of abandoned, sweeper-marked attempts in one statement.
    /// The per-row attempt/worker/stuckness guards and retry delays ride along
    /// through `unnest`, exactly as phase one's abort marking does.
    pub(crate) async fn retry_swept_abandoned_batch(
        &self,
        jobs: &[&DatabaseStuckJob],
    ) -> Result<Vec<Uuid>, Error> {
        if jobs.is_empty() {
            return Ok(Vec::new());
        }
        let ids = jobs.iter().map(|job| job.id).collect::<Vec<_>>();
        let attempts = jobs.iter().map(|job| job.attempts).collect::<Vec<_>>();
        let worker_ids = jobs.iter().map(|job| job.worker_id).collect::<Vec<_>>();
        let delays = jobs
            .iter()
            .map(|job| duration_to_ms(job.next_retry_delay()))
            .collect::<Vec<_>>();
        let requeued = sqlx::query_scalar::<_, Uuid>(
            r#"
            WITH requested AS (
                SELECT *
                FROM unnest($1::uuid[], $2::integer[], $3::uuid[], $4::bigint[])
                    AS t(id, attempts, worker_id, delay_ms)
            ),
            requeued AS (
                UPDATE pgqueue.jobs j
                SET status = 'queued',
                    scheduled_at = CASE WHEN r.delay_ms = 0 THEN j.scheduled_at
                        ELSE now() + (r.delay_ms * interval '1 millisecond') END,
                    completed_at = NULL, started_at = NULL,
                    -- The attempt is nobody's from here on. Clearing the owner
                    -- is what tells a presumed-dead worker that is in fact
                    -- still running the handler that the attempt was taken
                    -- from it: `attempts` is unchanged, so `aborting_of` has
                    -- nothing else to see the loss by, and the attempt would
                    -- keep its processor slot — and keep producing side
                    -- effects — until it returned on its own. A queued row
                    -- advertising an owner is wrong for the dashboard too.
                    worker_id = NULL,
                    touched_at = now(), expires_at = NULL, result = NULL
                FROM requested r
                WHERE j.id = r.id AND j.queue = $5
                  AND j.status = 'aborting' AND j.error = $6 AND j.result = $7
                  AND j.attempts = r.attempts
                  AND j.worker_id IS NOT DISTINCT FROM r.worker_id
                  AND j.attempts < j.max_attempts
                  -- A subquery, not a join: an UPDATE's target table cannot be
                  -- referenced from its own FROM clause's join conditions.
                  AND pgqueue.job_is_stuck(j, $8::bigint, (
                      SELECT lease.expires_at FROM pgqueue.workers AS lease
                      WHERE lease.id = j.worker_id AND lease.queue = j.queue))
                  AND NOT EXISTS (
                      SELECT 1 FROM pgqueue.workers w
                      WHERE w.id = j.worker_id AND w.queue = j.queue
                        AND w.expires_at > now())
                RETURNING j.id
            )
            -- The lateral keeps the wakeup inside this statement's transaction,
            -- so it is emitted exactly when the requeue commits. Its arguments
            -- are constant, so the planner evaluates the function scan once for
            -- the whole batch rather than per row; one wakeup is enough, because
            -- every idle fetcher re-polls on it.
            SELECT requeued.id
            FROM requeued
            CROSS JOIN LATERAL pg_notify($9, 'enqueue') AS notified
            "#,
        )
        .bind(&ids)
        .bind(&attempts)
        .bind(&worker_ids as &[Option<Uuid>])
        .bind(&delays)
        .bind(&self.name)
        .bind(SWEPT)
        .bind(swept_marker())
        .bind(duration_to_ms(self.sweep_grace))
        .bind(&self.notify_channel)
        .fetch_all(&self.pool)
        .await?;
        for _ in &requeued {
            self.counters.record_retry();
        }
        Ok(requeued)
    }

    /// Aborts a batch of abandoned attempts in one statement. Rows whose
    /// retention deletes immediately are removed instead of updated, matching
    /// the single-row finish path.
    pub(crate) async fn abort_stuck_abandoned_batch(
        &self,
        jobs: &[&DatabaseStuckJob],
    ) -> Result<Vec<Uuid>, Error> {
        if jobs.is_empty() {
            return Ok(Vec::new());
        }
        let ids = jobs.iter().map(|job| job.id).collect::<Vec<_>>();
        let attempts = jobs.iter().map(|job| job.attempts).collect::<Vec<_>>();
        let worker_ids = jobs.iter().map(|job| job.worker_id).collect::<Vec<_>>();
        let finished = sqlx::query_scalar::<_, Uuid>(
            r#"
            WITH requested AS (
                SELECT *
                FROM unnest($1::uuid[], $2::integer[], $3::uuid[])
                    AS t(id, attempts, worker_id)
            ),
            candidate AS (
                SELECT j.id, j.result_ttl_ms
                FROM pgqueue.jobs j
                JOIN requested r ON r.id = j.id
                WHERE j.queue = $4
                  AND j.status IN ('running', 'aborting')
                  AND j.attempts = r.attempts
                  AND j.worker_id IS NOT DISTINCT FROM r.worker_id
                  -- A subquery rather than a join, like the sibling batch
                  -- statements: one index lookup per row of a batch already
                  -- keyed by id, and no outer join for `FOR UPDATE OF j` to
                  -- interact with.
                  AND pgqueue.job_is_stuck(j, $5::bigint, (
                      SELECT lease.expires_at FROM pgqueue.workers AS lease
                      WHERE lease.id = j.worker_id AND lease.queue = j.queue))
                  AND (
                      j.dedupe_key IS NULL
                      OR NOT EXISTS (
                          SELECT 1 FROM pgqueue.workers w
                          WHERE w.id = j.worker_id AND w.queue = j.queue
                            AND w.expires_at > now())
                  )
                FOR UPDATE OF j
            ),
            deleted AS (
                DELETE FROM pgqueue.jobs
                WHERE id IN (SELECT id FROM candidate WHERE result_ttl_ms = 0)
                RETURNING id
            ),
            updated AS (
                UPDATE pgqueue.jobs j
                SET status = 'aborted', result = NULL,
                    completed_at = now(), touched_at = now(),
                    expires_at = CASE WHEN j.result_ttl_ms IS NULL THEN NULL
                                      ELSE now() + (j.result_ttl_ms * interval '1 millisecond') END
                FROM candidate c
                WHERE j.id = c.id AND c.result_ttl_ms IS DISTINCT FROM 0
                RETURNING j.id
            ),
            finished AS (
                SELECT id FROM deleted UNION ALL SELECT id FROM updated
            )
            SELECT finished.id
            FROM finished
            CROSS JOIN LATERAL
                pg_notify($6, '{"id":"' || finished.id || '","status":"aborted"}') AS notified
            "#,
        )
        .bind(&ids)
        .bind(&attempts)
        .bind(&worker_ids as &[Option<Uuid>])
        .bind(&self.name)
        .bind(duration_to_ms(self.sweep_grace))
        .bind(&self.done_channel)
        .fetch_all(&self.pool)
        .await?;
        for id in &finished {
            self.counters.record_abort();
            tracing::debug!(job.id = %id, status = "aborted", queue = %self.name, "finished");
        }
        Ok(finished)
    }

    async fn finish_with_guards(
        &self,
        attempt: AttemptGuard<'_>,
        status: JobStatus,
        result: &Option<Value>,
        error: Option<&str>,
    ) -> Result<bool, Error> {
        if !status.is_terminal() {
            return Err(Error::Config(
                "finish requires a terminal job status".into(),
            ));
        }
        if attempt.queue != self.name {
            return Err(Error::Config(format!(
                "job {} belongs to queue {:?}, not {:?}",
                attempt.id, attempt.queue, self.name
            )));
        }
        // An owner may still finish an attempt the sweeper marked `aborting`
        // underneath it, so the guard accepts that row too: unconditionally
        // when the owner is itself reporting the abort, and otherwise only
        // while it still carries the sweeper's markers. Folding both into one
        // predicate keeps finishing a swept attempt to a single round trip.
        let owner_reports_abort = status == JobStatus::Aborted;
        let status = status.as_str();
        let payload = format!(r#"{{"id":"{}","status":"{status}"}}"#, attempt.id);

        // One statement: the guarded candidate is locked once, rows with an
        // immediate-delete retention are removed instead of updated, and the
        // done notification fires only when a row actually finished.
        let row = sqlx::query_as::<_, FinishResult>(
            r#"
            WITH candidate AS (
                SELECT j.id, j.result_ttl_ms FROM pgqueue.jobs j
                WHERE j.id = $1 AND j.queue = $7
                  AND (j.status = 'running'
                       OR (j.status = 'aborting'
                           AND ($8 OR (j.error = $9 AND j.result = $10))))
                  AND j.attempts = $5 AND j.worker_id IS NOT DISTINCT FROM $6
                FOR UPDATE
            ),
            deleted AS (
                DELETE FROM pgqueue.jobs
                WHERE id IN (SELECT id FROM candidate WHERE result_ttl_ms = 0)
                RETURNING id
            ),
            updated AS (
                UPDATE pgqueue.jobs j
                SET status = $2, result = $3,
                    error = CASE WHEN $2 = 'complete' THEN $4 ELSE COALESCE($4, j.error) END,
                    completed_at = now(), touched_at = now(),
                    expires_at = CASE WHEN j.result_ttl_ms IS NULL THEN NULL
                                      ELSE now() + (j.result_ttl_ms * interval '1 millisecond') END
                FROM candidate c
                WHERE j.id = c.id AND c.result_ttl_ms IS DISTINCT FROM 0
                RETURNING j.id
            ),
            finished AS (
                SELECT id FROM deleted UNION ALL SELECT id FROM updated
            )
            SELECT EXISTS (SELECT 1 FROM finished) AS finished,
                   (SELECT pg_notify($11, $12) FROM finished) IS NULL AS notify_skipped
            "#,
        )
        .bind(attempt.id)
        .bind(status)
        .bind(result)
        .bind(error)
        .bind(attempt.attempts)
        .bind(attempt.worker_id)
        .bind(&self.name)
        .bind(owner_reports_abort)
        .bind(SWEPT)
        .bind(swept_marker())
        .bind(&self.done_channel)
        .bind(payload)
        .fetch_one(&self.pool)
        .await?;
        if !row.finished {
            return Ok(false);
        }

        match status {
            "complete" => self.counters.record_complete(),
            "failed" => self.counters.record_failed(),
            _ => self.counters.record_abort(),
        }
        tracing::debug!(job.id = %attempt.id, status, queue = %self.name, "finished");
        Ok(true)
    }

    pub(crate) async fn retry(&self, job: &JobRow, error: &str) -> Result<bool, Error> {
        self.ensure_owns(job)?;
        validate_finalization(None, Some(error))?;
        if !job.retryable() {
            return Ok(false);
        }
        let delay = job.next_retry_delay();
        let retried = self
            .retry_with(AttemptGuard::from(job), error, delay, false, false)
            .await?;
        if retried {
            self.counters.record_retry();
            tracing::debug!(
                job.id = %job.id, attempt = job.attempts,
                delay_ms = duration_to_ms(delay), queue = %self.name,
                "retry scheduled"
            );
        }
        Ok(retried)
    }

    /// Requeues an attempt the worker gave up on at shutdown, refunding the
    /// attempt. `error` is stored so the reason the attempt ended stays visible.
    pub(crate) async fn requeue_shutdown(&self, job: &JobRow, error: &str) -> Result<bool, Error> {
        let retried = self
            .retry_with(AttemptGuard::from(job), error, Duration::ZERO, true, true)
            .await?;
        if retried {
            self.counters.record_retry();
        }
        Ok(retried)
    }

    async fn retry_with(
        &self,
        attempt: AttemptGuard<'_>,
        error: &str,
        delay: Duration,
        refund_attempt: bool,
        allow_swept: bool,
    ) -> Result<bool, Error> {
        let guards = DatabaseRequeueGuards {
            allow_running: true,
            allow_swept_abort: allow_swept,
            refund_attempt,
        };
        self.requeue_guarded(attempt, Some(error), delay, guards)
            .await
    }

    /// Puts the job back to `queued` under the given guards, as one
    /// statement: the guarded update, the shutdown intake close, and the
    /// enqueue notification travel together so every requeue on the worker
    /// hot path costs a single round trip. `error` replaces the stored error
    /// when given; a `None` keeps the sweeper's marker in place.
    async fn requeue_guarded(
        &self,
        attempt: AttemptGuard<'_>,
        error: Option<&str>,
        delay: Duration,
        guards: DatabaseRequeueGuards,
    ) -> Result<bool, Error> {
        if attempt.queue != self.name {
            return Err(Error::Config(format!(
                "job {} belongs to queue {:?}, not {:?}",
                attempt.id, attempt.queue, self.name
            )));
        }
        let row = sqlx::query_as::<_, RequeueResult>(
            r#"
            WITH requeued AS (
                UPDATE pgqueue.jobs j
                SET status = 'queued',
                    max_attempts = CASE WHEN $7
                        THEN LEAST(max_attempts::bigint + 1, 2147483647)::integer
                        ELSE max_attempts END,
                    scheduled_at = CASE WHEN $2::bigint = 0 THEN scheduled_at
                        ELSE now() + ($2::bigint * interval '1 millisecond') END,
                    error = COALESCE($3, j.error),
                    completed_at = NULL, started_at = NULL,
                    -- The guard below reads the pre-update row, so clearing the
                    -- owner here is safe — and required: a `queued` row that
                    -- still names the worker that gave the attempt up is wrong
                    -- for `JobRow::worker_id` and for the dashboard, which
                    -- renders it as the job's owner. Matches
                    -- `retry_swept_abandoned_batch`.
                    worker_id = NULL,
                    touched_at = now(), expires_at = NULL, result = NULL
                WHERE j.id = $1 AND j.queue = $6
                  AND (($8 AND j.status = 'running')
                       OR ($9 AND j.status = 'aborting'
                           AND j.error = $10 AND j.result = $11))
                  AND j.attempts = $4 AND j.worker_id IS NOT DISTINCT FROM $5
                  AND ($7 OR j.attempts < j.max_attempts)
                RETURNING j.id
            ),
            intake_closed AS (
                UPDATE pgqueue.workers w
                SET accepting = false, heartbeat_at = now()
                WHERE $7 AND w.id = $5 AND w.queue = $6
                RETURNING w.id
            )
            SELECT EXISTS (SELECT 1 FROM requeued) AS requeued,
                   (SELECT pg_notify($12, 'enqueue') FROM requeued) IS NULL
                       AS notify_skipped,
                   EXISTS (SELECT 1 FROM intake_closed) AS intake_closed
            "#,
        )
        .bind(attempt.id)
        .bind(duration_to_ms(delay))
        .bind(error)
        .bind(attempt.attempts)
        .bind(attempt.worker_id)
        .bind(&self.name)
        .bind(guards.refund_attempt)
        .bind(guards.allow_running)
        .bind(guards.allow_swept_abort)
        .bind(SWEPT)
        .bind(swept_marker())
        .bind(&self.notify_channel)
        .fetch_one(&self.pool)
        .await?;
        Ok(row.requeued)
    }
}
