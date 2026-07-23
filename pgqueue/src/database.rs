//! PostgreSQL persistence shared by queues, workers, and the dashboard.

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::postgres::{PgPool, PgPoolOptions};
use tokio::sync::OnceCell;
use uuid::Uuid;

use crate::Error;
use crate::job::{
    CronMisfirePolicy, JobCronEntry, JobCursor, JobRequest, JobRetention, JobRetryBackoff, JobRow,
    JobStatus, duration_to_ms, validate_duration,
};
use crate::queue::{MigrationMode, QueueCounters, QueueCounts, QueueNotifyListener, QueueStats};
use crate::sweeper::{SWEPT, Sweeper, is_swept_marked, swept_marker};
use crate::worker::WorkerInfo;

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

async fn validate_migrations(pool: &PgPool) -> Result<(), Error> {
    let applied = sqlx::query!(
        r#"
        SELECT version, checksum, success
        FROM pgqueue._sqlx_migrations
        ORDER BY version
        "#,
    )
    .fetch_all(pool)
    .await?;

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
const DEQUEUE_LOCK_MASK: i32 = i32::MIN;
const UNIQUE_ENQUEUE_LOCK_MASK: i32 = 1 << 29;

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
    fn channel_name_differs_when_queue_name_embeds_done_suffix() {
        assert_ne!(channel_name("jobs_done", ""), channel_name("jobs", "_done"));
    }

    #[test]
    fn channel_name_stays_within_postgres_identifier_limit() {
        let name = channel_name(&"q".repeat(300), "_done");
        assert!(name.len() <= 63, "channel name too long: {name}");
    }
}

pub(crate) fn dequeue_lock_key(database: &str) -> i32 {
    stable_hash(database.bytes()) as i32 ^ DEQUEUE_LOCK_MASK
}

fn unique_enqueue_lock_key(database: &str) -> i32 {
    stable_hash(database.bytes()) as i32 ^ UNIQUE_ENQUEUE_LOCK_MASK
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

/// Database state scoped to one named queue.
pub(crate) struct Database {
    pool: PgPool,
    name: String,
    dequeue_lock_key: i32,
    unique_enqueue_lock_key: i32,
    sweep_lock_key: i64,
    priorities: (i16, i16),
    sweep_grace: Duration,
    sweep_batch_size: i64,
    notify_channel: String,
    done_channel: String,
    counters: QueueCounters,
    notify_listener: OnceCell<QueueNotifyListener>,
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

pub(crate) enum DatabaseEnqueueOutcome {
    Inserted(Uuid),
    Deduplicated {
        id: Uuid,
        name: String,
        retention: JobRetention,
    },
}

/// The live row a cron upsert conflicted with.
pub(crate) struct DatabaseCronConflict {
    pub(crate) scheduled_at: DateTime<Utc>,
    pub(crate) kind: String,
    pub(crate) name: String,
}

pub(crate) enum DatabaseCronAuthority {
    Active,
    Inactive { revision: i64 },
}

pub(crate) enum DatabaseCronScheduleOutcome {
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
    pub(crate) worker_id: Option<Uuid>,
    pub(crate) reason: Option<String>,
    pub(crate) swept: bool,
}

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
        self.max_attempts > self.attempts
    }

    pub(crate) fn next_retry_delay(&self) -> Duration {
        let base = Duration::from_millis(self.retry_delay_ms.max(0) as u64);
        self.backoff.next_delay(base, self.attempts.max(0) as u32)
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
    pub(crate) lock_contended: bool,
}

/// Which rows [`Database::requeue_guarded`] may reclaim.
#[derive(Clone, Copy)]
struct DatabaseRequeueGuards {
    /// Reclaim the row while it is still `running`.
    allow_running: bool,
    /// Reclaim an `aborting` row bearing the sweeper's markers.
    allow_swept_abort: bool,
    /// Additionally require the row to be stuck with a dead worker lease.
    require_stuck: bool,
    /// Refund the attempt and close the worker's intake (shutdown requeue).
    refund_attempt: bool,
}

#[derive(Clone, Copy)]
enum DatabaseFinishMode {
    Owned,
    SweptOwner,
    Abandoned,
}

impl DatabaseFinishMode {
    fn guards(self, status: JobStatus) -> (bool, bool, bool) {
        match self {
            Self::Owned => (true, status == JobStatus::Aborted, false),
            Self::SweptOwner => (true, true, true),
            Self::Abandoned => (false, true, false),
        }
    }
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

        let server = sqlx::query!(
            "SELECT current_setting('server_version_num')::int AS \"version!\", current_database() AS \"database!\""
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
            dequeue_lock_key: dequeue_lock_key(&server.database),
            unique_enqueue_lock_key: unique_enqueue_lock_key(&server.database),
            sweep_lock_key: sweep_lock_key(&server.database, &options.name),
            pool,
            name: options.name,
            priorities: options.priorities,
            sweep_grace: options.sweep_grace,
            sweep_batch_size: i64::from(options.sweep_batch_size),
            counters: QueueCounters::default(),
            notify_listener: OnceCell::new(),
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

    pub(crate) async fn notify_listener(&self) -> Result<&QueueNotifyListener, Error> {
        self.notify_listener
            .get_or_try_init(|| QueueNotifyListener::start(self))
            .await
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

    pub(crate) async fn enqueue_raw_delayed_outcome(
        &self,
        job: JobRequest,
        delay: Option<Duration>,
    ) -> Result<DatabaseEnqueueOutcome, Error> {
        if job.unique_key.is_some() {
            let mut transaction = self.pool.begin().await?;
            let outcome = self
                .enqueue_raw_delayed_in_outcome(&mut transaction, job, delay)
                .await?;
            transaction.commit().await?;
            return Ok(outcome);
        }

        job.validate()?;
        if let Some(delay) = delay {
            validate_duration("job delay", delay)?;
        }
        let backoff = serde_json::to_value(job.config.backoff)?;
        let id = self
            .insert_job(
                &job,
                &backoff,
                job.config.timeout.map(duration_to_ms),
                job.config.heartbeat.map(duration_to_ms),
                duration_to_ms(job.config.retry_delay),
                job.config.retention.as_ttl_ms(),
                delay.map(duration_to_ms),
                &self.pool,
            )
            .await?
            .ok_or_else(|| Error::Config("keyless job insert returned no row".into()))?;
        Ok(DatabaseEnqueueOutcome::Inserted(id))
    }

    pub(crate) async fn enqueue_raw_delayed_in_outcome(
        &self,
        transaction: &mut sqlx::PgTransaction<'_>,
        job: JobRequest,
        delay: Option<Duration>,
    ) -> Result<DatabaseEnqueueOutcome, Error> {
        job.validate()?;
        if let Some(delay) = delay {
            validate_duration("job delay", delay)?;
        }
        let backoff = serde_json::to_value(job.config.backoff)?;
        let timeout_ms = job.config.timeout.map(duration_to_ms);
        let heartbeat_ms = job.config.heartbeat.map(duration_to_ms);
        let retry_delay_ms = duration_to_ms(job.config.retry_delay);
        let ttl_ms = job.config.retention.as_ttl_ms();
        let delay_ms = delay.map(duration_to_ms);

        if let Some(unique_key) = job.unique_key.as_deref() {
            sqlx::query!(
                "SELECT pg_advisory_xact_lock($1, hashtext(length($2)::text || ':' || $2 || $3))",
                self.unique_enqueue_lock_key,
                self.name,
                unique_key,
            )
            .execute(&mut **transaction)
            .await?;

            // The advisory transaction lock serializes enqueue decisions. A
            // plain read deliberately avoids pinning the existing row against
            // worker finalization for the caller transaction's lifetime.
            if let Some(row) = sqlx::query!(
                r#"
                SELECT id, name, ttl_ms FROM pgqueue.jobs
                WHERE queue = $1 AND unique_key = $2
                  AND status IN ('queued', 'running', 'aborting')
                "#,
                self.name,
                unique_key,
            )
            .fetch_optional(&mut **transaction)
            .await?
            {
                return Ok(DatabaseEnqueueOutcome::Deduplicated {
                    id: row.id,
                    name: row.name,
                    retention: JobRetention::from_ttl_ms(row.ttl_ms),
                });
            }
        }

        let id = self
            .insert_job(
                &job,
                &backoff,
                timeout_ms,
                heartbeat_ms,
                retry_delay_ms,
                ttl_ms,
                delay_ms,
                &mut **transaction,
            )
            .await?;
        match id {
            Some(id) => Ok(DatabaseEnqueueOutcome::Inserted(id)),
            None => Err(Error::Config(
                "job enqueue conflicted after acquiring its unique-key lock".into(),
            )),
        }
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
        sqlx::query!(
            r#"
            INSERT INTO pgqueue.cron_schedules (
                queue, unique_key, name, expression, definition, revision,
                misfire_policy, grace_ms, next_run_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (queue, unique_key) DO UPDATE SET
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
            self.name,
            entry.unique_key,
            entry.template.name,
            entry.expr,
            entry.definition,
            revision,
            policy,
            grace_ms,
            next_run_at,
        )
        .execute(&mut *tx)
        .await?;
        let authority = sqlx::query!(
            r#"
            SELECT name, expression, definition, revision, misfire_policy, grace_ms
            FROM pgqueue.cron_schedules
            WHERE queue = $1 AND unique_key = $2
            "#,
            self.name,
            entry.unique_key,
        )
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
            || authority.definition != entry.definition
            || authority.misfire_policy != policy
            || authority.grace_ms != grace_ms
        {
            return Err(Error::Config(format!(
                "cron {:?} revision {} conflicts with the stored definition",
                entry.unique_key, revision
            )));
        }
        Ok(DatabaseCronAuthority::Active)
    }

    pub(crate) async fn schedule_cron(
        &self,
        entry: &JobCronEntry,
    ) -> Result<DatabaseCronScheduleOutcome, Error> {
        let revision = i64::try_from(entry.options.revision)
            .map_err(|_| Error::Config("cron revision must fit PostgreSQL bigint".into()))?;
        let policy = entry.options.misfire.kind();
        let grace_ms = entry.options.misfire.grace_ms();
        let mut tx = self.pool.begin().await?;
        let observed = sqlx::query!(
            r#"
            SELECT name, expression, definition, revision, misfire_policy, grace_ms,
                   next_run_at, now() AS "now!"
            FROM pgqueue.cron_schedules
            WHERE queue = $1 AND unique_key = $2
            "#,
            self.name,
            entry.unique_key,
        )
        .fetch_optional(&mut *tx)
        .await?;
        let Some(observed) = observed else {
            tx.rollback().await?;
            return Err(Error::Config(format!(
                "cron schedule {:?} was not reconciled",
                entry.unique_key
            )));
        };
        if observed.revision != revision
            || observed.name != entry.template.name
            || observed.expression != entry.expr
            || observed.definition != entry.definition
            || observed.misfire_policy != policy
            || observed.grace_ms != grace_ms
        {
            tx.rollback().await?;
            return Ok(DatabaseCronScheduleOutcome::Inactive {
                revision: observed.revision,
            });
        }
        if observed.next_run_at > observed.now {
            tx.rollback().await?;
            return Ok(DatabaseCronScheduleOutcome::NotDue);
        }

        let due = sqlx::query!(
            r#"
            SELECT next_run_at
            FROM pgqueue.cron_schedules
            WHERE queue = $1 AND unique_key = $2
              AND revision = $3 AND definition = $4
              AND next_run_at <= now()
            FOR UPDATE SKIP LOCKED
            "#,
            self.name,
            entry.unique_key,
            revision,
            entry.definition,
        )
        .fetch_optional(&mut *tx)
        .await?;
        let Some(due) = due else {
            tx.rollback().await?;
            return Ok(DatabaseCronScheduleOutcome::Contended);
        };

        let stored_occurrence = due.next_run_at;
        sqlx::query!(
            "SELECT pg_advisory_xact_lock($1, hashtext(length($2)::text || ':' || $2 || $3))",
            self.unique_enqueue_lock_key,
            self.name,
            entry.unique_key,
        )
        .execute(&mut *tx)
        .await?;
        // The unique-key lock may have been held by a long caller-owned
        // transaction. Use wall-clock database time after that wait so an
        // occurrence cannot be published after its grace or successor.
        let current = sqlx::query_scalar!(r#"SELECT clock_timestamp() AS "now!""#)
            .fetch_one(&mut *tx)
            .await?;
        let (occurrence, successor, publish) = match entry.options.misfire {
            CronMisfirePolicy::Skip { .. } => {
                let successor = entry.next_occurrence(stored_occurrence)?;
                let deadline = entry.publication_deadline(stored_occurrence, successor);
                (stored_occurrence, successor, current < deadline)
            }
            CronMisfirePolicy::FireOnce => {
                let occurrence = entry.previous_occurrence(current)?;
                let successor = entry.next_occurrence(occurrence)?;
                (occurrence, successor, current < successor)
            }
        };
        let next_run_at = if publish {
            successor
        } else {
            entry.next_occurrence(current)?
        };
        let claim_expires_at = successor.max(current + chrono::Duration::seconds(1));

        let claimed = sqlx::query_scalar!(
            r#"
            INSERT INTO pgqueue.cron_occurrences (
                queue, unique_key, scheduled_at, expires_at
            ) VALUES ($1, $2, $3, $4)
            ON CONFLICT DO NOTHING
            RETURNING true AS "claimed!"
            "#,
            self.name,
            entry.unique_key,
            occurrence,
            claim_expires_at,
        )
        .fetch_optional(&mut *tx)
        .await?
        .unwrap_or(false);

        let mut outcome = if !claimed {
            DatabaseCronScheduleOutcome::AlreadyPublished { occurrence }
        } else if !publish {
            DatabaseCronScheduleOutcome::SkippedStale { occurrence }
        } else if let Some(holder) = sqlx::query!(
            r#"
            SELECT scheduled_at, kind, name FROM pgqueue.jobs
            WHERE queue = $1 AND unique_key = $2
              AND status IN ('queued', 'running', 'aborting')
            "#,
            self.name,
            entry.unique_key,
        )
        .fetch_optional(&mut *tx)
        .await?
        {
            DatabaseCronScheduleOutcome::SkippedHeld {
                occurrence,
                existing: DatabaseCronConflict {
                    scheduled_at: holder.scheduled_at,
                    kind: holder.kind,
                    name: holder.name,
                },
            }
        } else {
            let job = entry.job_for(occurrence);
            let backoff = serde_json::to_value(job.config.backoff)?;
            let inserted = sqlx::query!(
                r#"
                WITH inserted AS (
                    INSERT INTO pgqueue.jobs (
                        queue, name, payload, unique_key, priority, group_key,
                        max_attempts, timeout_ms, heartbeat_ms, retry_delay_ms,
                        backoff, ttl_ms, scheduled_at, meta, kind, cron_expr
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                            $11, $12, $13, $14, 'cron', $15)
                    ON CONFLICT (queue, unique_key) WHERE unique_key IS NOT NULL
                        AND status IN ('queued', 'running', 'aborting') DO NOTHING
                    RETURNING id
                )
                SELECT id AS "id!", pg_notify($16, 'enqueue') IS NULL AS "notified!"
                FROM inserted
                "#,
                self.name,
                job.name,
                job.payload,
                job.unique_key,
                job.config.priority,
                job.group_key,
                job.config.max_attempts as i32,
                job.config.timeout.map(duration_to_ms),
                job.config.heartbeat.map(duration_to_ms),
                duration_to_ms(job.config.retry_delay),
                backoff,
                job.config.retention.as_ttl_ms(),
                occurrence,
                job.meta,
                entry.expr,
                self.notify_channel,
            )
            .fetch_optional(&mut *tx)
            .await?
            .map(|row| row.id);
            match inserted {
                Some(id) => DatabaseCronScheduleOutcome::Published { id, occurrence },
                None => DatabaseCronScheduleOutcome::SkippedStale { occurrence },
            }
        };

        let advanced = sqlx::query_scalar!(
            r#"
            UPDATE pgqueue.cron_schedules
            SET next_run_at = $4, updated_at = now()
            WHERE queue = $1 AND unique_key = $2
              AND revision = $3 AND definition = $5
            RETURNING true AS "advanced!"
            "#,
            self.name,
            entry.unique_key,
            revision,
            next_run_at,
            entry.definition,
        )
        .fetch_optional(&mut *tx)
        .await?
        .unwrap_or(false);
        if !advanced {
            tx.rollback().await?;
            outcome = DatabaseCronScheduleOutcome::Inactive { revision };
            return Ok(outcome);
        }
        tx.commit().await?;
        Ok(outcome)
    }

    /// Inserts a plain (non-cron) job and emits its enqueue notification as
    /// one statement, so the keyless path costs a single round trip.
    #[allow(clippy::too_many_arguments)]
    async fn insert_job<'e>(
        &self,
        job: &JobRequest,
        backoff: &Value,
        timeout_ms: Option<i64>,
        heartbeat_ms: Option<i64>,
        retry_delay_ms: i64,
        ttl_ms: Option<i64>,
        delay_ms: Option<i64>,
        executor: impl sqlx::PgExecutor<'e>,
    ) -> Result<Option<Uuid>, Error> {
        let row = sqlx::query!(
            r#"
            WITH inserted AS (
                INSERT INTO pgqueue.jobs (
                    queue, name, payload, unique_key, priority, group_key, max_attempts,
                    timeout_ms, heartbeat_ms, retry_delay_ms, backoff, ttl_ms,
                    scheduled_at, meta, kind, cron_expr
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                        COALESCE($13, now() + ($15::bigint * interval '1 millisecond'), now()),
                        $14, 'job', NULL)
                ON CONFLICT (queue, unique_key) WHERE unique_key IS NOT NULL
                    AND status IN ('queued', 'running', 'aborting') DO NOTHING
                RETURNING id
            )
            SELECT id AS "id!", pg_notify($16, 'enqueue') IS NULL AS "notified!"
            FROM inserted
            "#,
            self.name,
            job.name,
            job.payload,
            job.unique_key,
            job.config.priority,
            job.group_key,
            job.config.max_attempts as i32,
            timeout_ms,
            heartbeat_ms,
            retry_delay_ms,
            backoff,
            ttl_ms,
            job.scheduled_at,
            job.meta,
            delay_ms,
            self.notify_channel,
        )
        .fetch_optional(executor)
        .await?;
        Ok(row.map(|row| row.id))
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
        Ok(sqlx::query_as!(
            JobRow,
            r#"
            SELECT id, unique_key, queue, name, payload,
                   status AS "status: JobStatus", priority, group_key, attempts,
                   max_attempts, timeout_ms, heartbeat_ms, retry_delay_ms,
                   backoff AS "backoff: JobRetryBackoff", ttl_ms, scheduled_at,
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
            self.name,
            status,
            name,
            limit,
            before_enqueued_at,
            before_id,
        )
        .fetch_all(&self.pool)
        .await?)
    }

    pub(crate) async fn counts(&self) -> Result<QueueCounts, Error> {
        let row = sqlx::query!(
            r#"
            SELECT
                COUNT(*) FILTER (WHERE status = 'queued' AND scheduled_at <= now()) AS "queued!",
                COUNT(*) FILTER (WHERE status IN ('running', 'aborting')) AS "running!",
                COUNT(*) FILTER (WHERE status = 'queued' AND scheduled_at > now()) AS "scheduled!",
                COUNT(*) FILTER (WHERE status = 'failed') AS "failed!",
                COUNT(*) FILTER (WHERE status = 'aborted') AS "aborted!"
            FROM pgqueue.jobs WHERE queue = $1
            "#,
            self.name,
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(QueueCounts {
            queued: row.queued,
            running: row.running,
            scheduled: row.scheduled,
            failed: row.failed,
            aborted: row.aborted,
        })
    }

    pub(crate) async fn workers(&self) -> Result<Vec<WorkerInfo>, Error> {
        Ok(sqlx::query_as!(
            WorkerInfo,
            r#"
            SELECT id, queue, stats, metadata, started_at, heartbeat_at, expires_at
            FROM pgqueue.workers
            WHERE queue = $1 AND expires_at > now()
            ORDER BY started_at
            "#,
            self.name,
        )
        .fetch_all(&self.pool)
        .await?)
    }

    pub(crate) async fn write_worker_info(
        &self,
        worker_id: Uuid,
        stats: Value,
        metadata: Option<Value>,
        ttl: Duration,
        reopen_intake: bool,
    ) -> Result<(), Error> {
        validate_duration("worker info TTL", ttl)?;
        let written = sqlx::query_scalar!(
            r#"
            INSERT INTO pgqueue.workers (id, queue, stats, metadata, expires_at)
            VALUES ($1, $2, $3, $5, now() + ($4::bigint * interval '1 millisecond'))
            ON CONFLICT (id) DO UPDATE SET
                stats = $3, metadata = $5, heartbeat_at = now(),
                expires_at = now() + ($4::bigint * interval '1 millisecond'),
                accepting = CASE WHEN $6 THEN true ELSE pgqueue.workers.accepting END
            WHERE pgqueue.workers.queue = EXCLUDED.queue
            RETURNING id
            "#,
            worker_id,
            self.name,
            stats,
            duration_to_ms(ttl),
            metadata,
            reopen_intake,
        )
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
        sqlx::query!(
            r#"
            UPDATE pgqueue.workers SET accepting = false, heartbeat_at = now()
            WHERE id = $1 AND queue = $2
            "#,
            worker_id,
            self.name,
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub(crate) async fn aborting_of(
        &self,
        ids: &[Uuid],
    ) -> Result<Vec<DatabaseAbortingAttempt>, Error> {
        let rows = sqlx::query!(
            r#"
            SELECT id, attempts, worker_id, error, result FROM pgqueue.jobs
            WHERE id = ANY($1) AND queue = $2 AND status IN ('aborting', 'aborted')
            "#,
            ids,
            self.name,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| DatabaseAbortingAttempt {
                swept: is_swept_marked(row.error.as_deref(), row.result.as_ref()),
                id: row.id,
                attempts: row.attempts,
                worker_id: row.worker_id,
                reason: row.error,
            })
            .collect())
    }

    pub(crate) async fn now(&self) -> Result<DateTime<Utc>, Error> {
        Ok(sqlx::query_scalar!("SELECT now() AS \"now!\"")
            .fetch_one(&self.pool)
            .await?)
    }

    async fn notify(
        &self,
        tx: &mut sqlx::PgTransaction<'_>,
        channel: &str,
        payload: &str,
    ) -> Result<(), Error> {
        sqlx::query!("SELECT pg_notify($1, $2)", channel, payload)
            .execute(&mut **tx)
            .await?;
        Ok(())
    }
}

impl Database {
    pub(crate) async fn retry_swept(&self, job: &JobRow) -> Result<bool, Error> {
        let guards = DatabaseRequeueGuards {
            allow_running: false,
            allow_swept_abort: true,
            require_stuck: false,
            refund_attempt: false,
        };
        let updated = self
            .requeue_guarded(
                AttemptGuard::from(job),
                None,
                job.next_retry_delay(),
                guards,
            )
            .await?;
        if updated {
            self.counters.record_retry();
        }
        Ok(updated)
    }

    pub(crate) async fn retry_swept_abandoned(
        &self,
        job: &DatabaseStuckJob,
    ) -> Result<bool, Error> {
        let guards = DatabaseRequeueGuards {
            allow_running: false,
            allow_swept_abort: true,
            require_stuck: true,
            refund_attempt: false,
        };
        let attempt = AttemptGuard {
            id: job.id,
            queue: &self.name,
            attempts: job.attempts,
            worker_id: job.worker_id,
        };
        let updated = self
            .requeue_guarded(attempt, None, job.next_retry_delay(), guards)
            .await?;
        if updated {
            self.counters.record_retry();
        }
        Ok(updated)
    }

    pub(crate) async fn job(&self, id: Uuid) -> Result<Option<JobRow>, Error> {
        Ok(sqlx::query_as!(
            JobRow,
            r#"
            SELECT id, unique_key, queue, name, payload,
                   status AS "status: JobStatus", priority, group_key, attempts,
                   max_attempts, timeout_ms, heartbeat_ms, retry_delay_ms,
                   backoff AS "backoff: JobRetryBackoff", ttl_ms, scheduled_at,
                   enqueued_at, started_at, touched_at, completed_at, expires_at,
                   result, error, meta, worker_id
            FROM pgqueue.jobs WHERE id = $1 AND queue = $2
            "#,
            id,
            self.name,
        )
        .fetch_optional(&self.pool)
        .await?)
    }

    pub(crate) async fn abort(&self, id: Uuid, reason: &str) -> Result<bool, Error> {
        let payload = format!(r#"{{"id":"{id}","status":"aborted"}}"#);
        let row = sqlx::query!(
            r#"
            WITH updated AS (
                UPDATE pgqueue.jobs
                SET status = CASE WHEN status = 'queued' THEN 'aborted' ELSE 'aborting' END,
                    error = $2, touched_at = now(),
                    completed_at = CASE WHEN status = 'queued' THEN now() ELSE completed_at END,
                    expires_at = CASE WHEN status = 'queued' AND ttl_ms IS NOT NULL
                        THEN now() + (ttl_ms * interval '1 millisecond') ELSE expires_at END
                WHERE id = $1 AND queue = $3 AND status IN ('queued', 'running')
                RETURNING status
            )
            SELECT status AS "status!",
                   (CASE WHEN status = 'aborted' THEN pg_notify($4, $5) END) IS NULL
                       AS "notify_skipped!"
            FROM updated
            "#,
            id,
            reason,
            self.name,
            self.done_channel,
            payload,
        )
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
        // A cron occurrence's unique key belongs to the schedule loop's
        // dedupe: carrying it onto a manual retry would collide with the
        // next scheduled occurrence and silently refuse the retry, so cron
        // retries run as keyless one-offs.
        let mut tx = self.pool.begin().await?;
        let new_id = sqlx::query_scalar!(
            r#"
            WITH source AS MATERIALIZED (
                UPDATE pgqueue.jobs SET retried_at = now()
                WHERE id = $1 AND queue = $3
                  AND status IN ('complete', 'failed', 'aborted') AND retried_at IS NULL
                RETURNING queue, name, payload,
                          CASE WHEN kind = 'cron' THEN NULL
                               ELSE unique_key END AS unique_key,
                          priority, group_key,
                          attempts, timeout_ms, heartbeat_ms, retry_delay_ms, backoff,
                          ttl_ms, meta, kind, cron_expr
            ), locked AS MATERIALIZED (
                SELECT pg_advisory_xact_lock($4,
                    hashtext(length(queue)::text || ':' || queue || unique_key))
                FROM source WHERE unique_key IS NOT NULL
            )
            INSERT INTO pgqueue.jobs (
                queue, name, payload, unique_key, priority, group_key, attempts,
                max_attempts, timeout_ms, heartbeat_ms, retry_delay_ms, backoff,
                ttl_ms, scheduled_at, meta, error, kind, cron_expr
            )
            SELECT queue, name, payload, unique_key, priority, group_key, attempts,
                   attempts + 1, timeout_ms, heartbeat_ms, retry_delay_ms, backoff,
                   ttl_ms, now(), meta, $2, kind, cron_expr
            FROM source LEFT JOIN locked ON true
            ON CONFLICT (queue, unique_key) WHERE unique_key IS NOT NULL
                AND status IN ('queued', 'running', 'aborting') DO NOTHING
            RETURNING id
            "#,
            id,
            reason,
            self.name,
            self.unique_enqueue_lock_key,
        )
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

    pub(crate) async fn touch(&self, id: Uuid) -> Result<(), Error> {
        self.touch_guarded(id, None).await
    }

    pub(crate) async fn touch_attempt(&self, job: &JobRow) -> Result<(), Error> {
        self.touch_guarded(job.id, Some((job.attempts, job.worker_id)))
            .await
    }

    /// Refreshes a live job's heartbeat. `attempt` additionally pins the
    /// touch to one attempt (attempt counter plus worker), so a leaked
    /// context cannot keep a retried job alive.
    async fn touch_guarded(
        &self,
        id: Uuid,
        attempt: Option<(i32, Option<Uuid>)>,
    ) -> Result<(), Error> {
        let check_attempt = attempt.is_some();
        let (attempts, worker_id) = attempt.map_or((None, None), |(attempts, worker_id)| {
            (Some(attempts), worker_id)
        });
        let touched = sqlx::query_scalar!(
            r#"
            WITH touched AS (
                UPDATE pgqueue.jobs SET touched_at = now()
                WHERE id = $1 AND queue = $2 AND status IN ('running', 'aborting')
                  AND (NOT $5 OR (attempts = $3 AND worker_id IS NOT DISTINCT FROM $4))
                RETURNING true AS touched
            )
            SELECT touched AS "touched!" FROM touched
            UNION ALL
            SELECT false AS "touched!" FROM pgqueue.jobs
            WHERE id = $1 AND queue = $2 AND NOT EXISTS (SELECT 1 FROM touched)
            LIMIT 1
            "#,
            id,
            self.name,
            attempts,
            worker_id,
            check_attempt,
        )
        .fetch_optional(&self.pool)
        .await?;
        match touched {
            Some(true) => Ok(()),
            Some(false) => Err(Error::JobNotTouchable(id)),
            None => Err(Error::JobNotFound(id)),
        }
    }
}

impl Database {
    pub(crate) async fn dequeue(&self, limit: i64, worker_id: Uuid) -> Result<Vec<JobRow>, Error> {
        Ok(self
            .dequeue_inner(limit, worker_id, false, None, false, true)
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
            false,
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
        wait_for_lock: bool,
    ) -> Result<DatabaseDequeueBatch, Error> {
        if limit <= 0 {
            return Err(Error::Config(
                "dequeue limit must be greater than zero".into(),
            ));
        }

        let mut jobs = self
            .dequeue_ungrouped_fast(limit, worker_id, require_open_intake, registered_names)
            .await?;
        if jobs.is_empty() {
            let mut tx = self.pool.begin().await?;
            let lock_acquired = if wait_for_lock {
                sqlx::query!(
                    "SELECT pg_advisory_xact_lock($1, hashtext($2))",
                    self.dequeue_lock_key,
                    self.name,
                )
                .execute(&mut *tx)
                .await?;
                true
            } else {
                sqlx::query_scalar!(
                    r#"SELECT pg_try_advisory_xact_lock($1, hashtext($2)) AS "locked!""#,
                    self.dequeue_lock_key,
                    self.name,
                )
                .fetch_one(&mut *tx)
                .await?
            };
            if !lock_acquired {
                tx.rollback().await?;
                return Ok(DatabaseDequeueBatch {
                    jobs: Vec::new(),
                    intake_open: true,
                    work_available: true,
                    unhandled_names: Vec::new(),
                    lock_contended: true,
                });
            }

            jobs = sqlx::query_as!(
                JobRow,
                r#"
            WITH candidates AS (
                SELECT job.id FROM pgqueue.jobs job
                WHERE job.queue = $1 AND job.status = 'queued'
                  AND job.scheduled_at <= now() AND job.priority BETWEEN $2 AND $3
                  AND ($7::text[] IS NULL OR job.name = ANY($7))
                  AND (job.group_key IS NULL OR (
                        NOT EXISTS (
                            SELECT 1 FROM pgqueue.jobs running
                            WHERE running.queue = $1
                              AND running.status IN ('running', 'aborting')
                              AND running.group_key = job.group_key
                        )
                        AND NOT EXISTS (
                            SELECT 1 FROM pgqueue.jobs earlier
                            WHERE earlier.queue = $1 AND earlier.status = 'queued'
                              AND earlier.scheduled_at <= now()
                              AND earlier.group_key = job.group_key
                              AND (earlier.priority, earlier.scheduled_at, earlier.id) <
                                  (job.priority, job.scheduled_at, job.id)
                        )
                  ))
                ORDER BY job.priority, job.scheduled_at, job.id
                LIMIT $4
                FOR UPDATE OF job SKIP LOCKED
            ), updated AS (
                UPDATE pgqueue.jobs j
                SET status = 'running', attempts = j.attempts + 1,
                    started_at = now(), touched_at = now(), worker_id = $5
                FROM candidates WHERE j.id = candidates.id AND j.queue = $1
                  AND j.status = 'queued' AND j.scheduled_at <= now()
                  AND j.priority BETWEEN $2 AND $3
                  AND ($7::text[] IS NULL OR j.name = ANY($7))
                  AND (NOT $6 OR EXISTS (
                        SELECT 1 FROM pgqueue.workers w
                        WHERE w.id = $5 AND w.queue = $1
                          AND w.accepting AND w.expires_at > now()))
                RETURNING j.id, j.unique_key, j.queue, j.name, j.payload, j.status,
                          j.priority, j.group_key, j.attempts, j.max_attempts,
                          j.timeout_ms, j.heartbeat_ms, j.retry_delay_ms, j.backoff,
                          j.ttl_ms, j.scheduled_at, j.enqueued_at, j.started_at,
                          j.touched_at, j.completed_at, j.expires_at, j.result,
                          j.error, j.meta, j.worker_id
            )
            SELECT id, unique_key, queue, name, payload,
                   status AS "status: JobStatus", priority, group_key, attempts,
                   max_attempts, timeout_ms, heartbeat_ms, retry_delay_ms,
                   backoff AS "backoff: JobRetryBackoff", ttl_ms, scheduled_at,
                   enqueued_at, started_at, touched_at, completed_at, expires_at,
                   result, error, meta, worker_id
            FROM updated
            "#,
                self.name,
                self.priorities.0,
                self.priorities.1,
                limit,
                worker_id,
                require_open_intake,
                registered_names,
            )
            .fetch_all(&mut *tx)
            .await?;
            tx.commit().await?;
        }

        // The underfilled-batch probes run outside the dequeue transaction: they
        // need no consistency with the batch, and holding the per-queue
        // advisory lock across them would serialize other dequeuers behind
        // them. The unhandled-names scan is the expensive part, so it only
        // runs when the caller's rate-limited warning is due.
        let batch_underfilled = i64::try_from(jobs.len()).is_ok_and(|fetched| fetched < limit);
        let (intake_open, work_available, unhandled_names) =
            if require_open_intake && batch_underfilled {
                let names = registered_names.unwrap_or_default();
                let row = sqlx::query!(
                    r#"
                SELECT
                    EXISTS (
                        SELECT 1 FROM pgqueue.workers
                        WHERE id = $2 AND queue = $1
                          AND accepting AND expires_at > now()
                    ) AS "intake_open!",
                    EXISTS (
                        SELECT 1 FROM pgqueue.jobs job
                        WHERE job.queue = $1 AND job.status = 'queued'
                          AND job.scheduled_at <= now()
                          AND job.priority BETWEEN $4 AND $5
                          AND job.name = ANY($3)
                          AND (job.group_key IS NULL OR (
                                NOT EXISTS (
                                    SELECT 1 FROM pgqueue.jobs running
                                    WHERE running.queue = $1
                                      AND running.status IN ('running', 'aborting')
                                      AND running.group_key = job.group_key
                                )
                                AND NOT EXISTS (
                                    SELECT 1 FROM pgqueue.jobs earlier
                                    WHERE earlier.queue = $1
                                      AND earlier.status = 'queued'
                                      AND earlier.scheduled_at <= now()
                                      AND earlier.group_key = job.group_key
                                      AND (earlier.priority, earlier.scheduled_at, earlier.id) <
                                          (job.priority, job.scheduled_at, job.id)
                                )
                          ))
                    ) AS "work_available!",
                    CASE WHEN $6 THEN ARRAY(
                        SELECT name FROM (
                            SELECT DISTINCT name FROM pgqueue.jobs
                            WHERE queue = $1 AND status = 'queued'
                              AND scheduled_at <= now()
                              AND priority BETWEEN $4 AND $5
                              AND NOT (name = ANY($3))
                        ) unhandled ORDER BY name LIMIT 10
                    ) ELSE ARRAY[]::text[] END AS "unhandled_names!"
                "#,
                    self.name,
                    worker_id,
                    names,
                    self.priorities.0,
                    self.priorities.1,
                    probe_unhandled,
                )
                .fetch_one(&self.pool)
                .await?;
                (row.intake_open, row.work_available, row.unhandled_names)
            } else {
                (true, false, Vec::new())
            };

        jobs.sort_by(|a, b| {
            (a.priority, a.scheduled_at, a.id).cmp(&(b.priority, b.scheduled_at, b.id))
        });
        Ok(DatabaseDequeueBatch {
            jobs,
            intake_open,
            work_available,
            unhandled_names,
            lock_contended: false,
        })
    }

    /// Concurrent fast path for queues whose currently due, visible workload
    /// is entirely ungrouped. The stronger "no grouped candidate at all"
    /// predicate preserves global selection order while allowing independent
    /// dequeuers to make progress through `SKIP LOCKED` without the queue lock.
    async fn dequeue_ungrouped_fast(
        &self,
        limit: i64,
        worker_id: Uuid,
        require_open_intake: bool,
        registered_names: Option<&[String]>,
    ) -> Result<Vec<JobRow>, Error> {
        Ok(sqlx::query_as!(
            JobRow,
            r#"
            WITH candidates AS (
                SELECT job.id FROM pgqueue.jobs job
                WHERE job.queue = $1 AND job.status = 'queued'
                  AND job.group_key IS NULL
                  AND job.scheduled_at <= now()
                  AND job.priority BETWEEN $2 AND $3
                  AND ($7::text[] IS NULL OR job.name = ANY($7))
                  AND NOT EXISTS (
                      SELECT 1 FROM pgqueue.jobs grouped
                      WHERE grouped.queue = $1 AND grouped.status = 'queued'
                        AND grouped.group_key IS NOT NULL
                        AND grouped.scheduled_at <= now()
                        AND grouped.priority BETWEEN $2 AND $3
                        AND ($7::text[] IS NULL OR grouped.name = ANY($7))
                  )
                ORDER BY job.priority, job.scheduled_at, job.id
                LIMIT $4
                FOR UPDATE OF job SKIP LOCKED
            ), updated AS (
                UPDATE pgqueue.jobs job
                SET status = 'running', attempts = job.attempts + 1,
                    started_at = now(), touched_at = now(), worker_id = $5
                FROM candidates
                WHERE job.id = candidates.id AND job.queue = $1
                  AND job.status = 'queued' AND job.group_key IS NULL
                  AND job.scheduled_at <= now()
                  AND job.priority BETWEEN $2 AND $3
                  AND ($7::text[] IS NULL OR job.name = ANY($7))
                  AND (NOT $6 OR EXISTS (
                      SELECT 1 FROM pgqueue.workers worker
                      WHERE worker.id = $5 AND worker.queue = $1
                        AND worker.accepting AND worker.expires_at > now()
                  ))
                RETURNING job.id, job.unique_key, job.queue, job.name,
                          job.payload, job.status, job.priority, job.group_key,
                          job.attempts, job.max_attempts, job.timeout_ms,
                          job.heartbeat_ms, job.retry_delay_ms, job.backoff,
                          job.ttl_ms, job.scheduled_at, job.enqueued_at,
                          job.started_at, job.touched_at, job.completed_at,
                          job.expires_at, job.result, job.error, job.meta,
                          job.worker_id
            )
            SELECT id, unique_key, queue, name, payload,
                   status AS "status: JobStatus", priority, group_key, attempts,
                   max_attempts, timeout_ms, heartbeat_ms, retry_delay_ms,
                   backoff AS "backoff: JobRetryBackoff", ttl_ms, scheduled_at,
                   enqueued_at, started_at, touched_at, completed_at, expires_at,
                   result, error, meta, worker_id
            FROM updated
            "#,
            self.name,
            self.priorities.0,
            self.priorities.1,
            limit,
            worker_id,
            require_open_intake,
            registered_names,
        )
        .fetch_all(&self.pool)
        .await?)
    }

    pub(crate) async fn finish(
        &self,
        job: &JobRow,
        status: JobStatus,
        result: Option<Value>,
        error: Option<&str>,
    ) -> Result<bool, Error> {
        self.ensure_owns(job)?;
        let finished = self
            .finish_with_guards(
                AttemptGuard::from(job),
                status,
                &result,
                error,
                DatabaseFinishMode::Owned,
            )
            .await?;
        if finished || status == JobStatus::Aborted {
            return Ok(finished);
        }
        self.finish_with_guards(
            AttemptGuard::from(job),
            status,
            &result,
            error,
            DatabaseFinishMode::SweptOwner,
        )
        .await
    }

    pub(crate) async fn finish_stuck_abandoned(
        &self,
        job: &DatabaseStuckJob,
        status: JobStatus,
        result: Option<Value>,
        error: Option<&str>,
    ) -> Result<bool, Error> {
        self.finish_with_guards(
            AttemptGuard {
                id: job.id,
                queue: &self.name,
                attempts: job.attempts,
                worker_id: job.worker_id,
            },
            status,
            &result,
            error,
            DatabaseFinishMode::Abandoned,
        )
        .await
    }

    async fn finish_with_guards(
        &self,
        attempt: AttemptGuard<'_>,
        status: JobStatus,
        result: &Option<Value>,
        error: Option<&str>,
        mode: DatabaseFinishMode,
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
        let (allow_live_owner, allow_aborting, require_swept) = mode.guards(status);
        let status = status.as_str();
        let grace_ms = duration_to_ms(self.sweep_grace);
        let payload = format!(r#"{{"id":"{}","status":"{status}"}}"#, attempt.id);

        // An abandoned keyed attempt retains its active status while its owner
        // is live, preserving group and unique-key exclusivity. Unkeyed final
        // attempts can resolve promptly without enabling conflicting work.
        // One statement: the guarded candidate is locked once, rows with an
        // immediate-delete retention are removed instead of updated, and the
        // done notification fires only when a row actually finished.
        let row = sqlx::query!(
            r#"
            WITH candidate AS (
                SELECT j.id, j.ttl_ms FROM pgqueue.jobs j
                WHERE j.id = $1 AND j.queue = $7
                  AND (j.status = 'running' OR ($9 AND j.status = 'aborting'
                       AND (NOT $10 OR (j.error = $11 AND j.result = $12))))
                  AND j.attempts = $5 AND j.worker_id IS NOT DISTINCT FROM $6
                  AND ($8 OR (
                      pgqueue.job_is_stuck(j, $13::bigint)
                      AND (
                          (j.unique_key IS NULL AND j.group_key IS NULL)
                          OR NOT EXISTS (
                              SELECT 1 FROM pgqueue.workers w
                              WHERE w.id = j.worker_id AND w.queue = j.queue
                                AND w.expires_at > now()
                          )
                      )
                  ))
                FOR UPDATE
            ),
            deleted AS (
                DELETE FROM pgqueue.jobs
                WHERE id IN (SELECT id FROM candidate WHERE ttl_ms = 0)
                RETURNING id
            ),
            updated AS (
                UPDATE pgqueue.jobs j
                SET status = $2, result = $3,
                    error = CASE WHEN $2 = 'complete' THEN $4 ELSE COALESCE($4, j.error) END,
                    completed_at = now(), touched_at = now(),
                    expires_at = CASE WHEN j.ttl_ms IS NULL THEN NULL
                                      ELSE now() + (j.ttl_ms * interval '1 millisecond') END
                FROM candidate c
                WHERE j.id = c.id AND c.ttl_ms IS DISTINCT FROM 0
                RETURNING j.id
            ),
            finished AS (
                SELECT id FROM deleted UNION ALL SELECT id FROM updated
            )
            SELECT EXISTS (SELECT 1 FROM finished) AS "finished!",
                   (SELECT pg_notify($14, $15) FROM finished) IS NULL AS "notify_skipped!"
            "#,
            attempt.id,
            status,
            result.clone(),
            error,
            attempt.attempts,
            attempt.worker_id,
            self.name,
            allow_live_owner,
            allow_aborting,
            require_swept,
            SWEPT,
            swept_marker(),
            grace_ms,
            self.done_channel,
            payload,
        )
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

    pub(crate) async fn requeue_shutdown(&self, job: &JobRow) -> Result<bool, Error> {
        let retried = self
            .retry_with(
                AttemptGuard::from(job),
                "cancelled",
                Duration::ZERO,
                true,
                true,
            )
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
            require_stuck: false,
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
        let row = sqlx::query!(
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
                    touched_at = now(), expires_at = NULL, result = NULL
                WHERE j.id = $1 AND j.queue = $6
                  AND (($8 AND j.status = 'running')
                       OR ($9 AND j.status = 'aborting'
                           AND j.error = $10 AND j.result = $11))
                  AND j.attempts = $4 AND j.worker_id IS NOT DISTINCT FROM $5
                  AND ($7 OR j.attempts < j.max_attempts)
                  AND (NOT $12 OR (pgqueue.job_is_stuck(j, $13::bigint)
                       AND NOT EXISTS (
                            SELECT 1 FROM pgqueue.workers w
                            WHERE w.id = j.worker_id AND w.queue = j.queue
                              AND w.expires_at > now())))
                RETURNING j.id
            ),
            intake_closed AS (
                UPDATE pgqueue.workers w
                SET accepting = false, heartbeat_at = now()
                WHERE $7 AND w.id = $5 AND w.queue = $6
                RETURNING w.id
            )
            SELECT EXISTS (SELECT 1 FROM requeued) AS "requeued!",
                   (SELECT pg_notify($14, 'enqueue') FROM requeued) IS NULL
                       AS "notify_skipped!",
                   EXISTS (SELECT 1 FROM intake_closed) AS "intake_closed!"
            "#,
            attempt.id,
            duration_to_ms(delay),
            error,
            attempt.attempts,
            attempt.worker_id,
            self.name,
            guards.refund_attempt,
            guards.allow_running,
            guards.allow_swept_abort,
            SWEPT,
            swept_marker(),
            guards.require_stuck,
            duration_to_ms(self.sweep_grace),
            self.notify_channel,
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(row.requeued)
    }
}
