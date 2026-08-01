//! Integration tests for the Postgres queue core, against a real database.

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

use crate::{
    DEQUEUE_CLAIM, DEQUEUE_PROBE, Stats, hold_gate, install_statement_gate, leased_consumer,
    wait_for_lock_waiter,
};
use crate::{
    EnqueueResultTestExt, QueueProtocolTestExt, TestDb, backdate_job_liveness, new_job, wait_until,
    with_config,
};
use chrono::Utc;
use pgqueue::{
    EnqueueResult, Error, JobCursor, JobFilter, JobRetention, JobRetryBackoff, JobStatus,
    MigrationMode, Queue,
};
use serde_json::json;
use sqlx::{Connection, PgConnection, Row};
use uuid::Uuid;

async fn connect_with_validation(pool: PgPool) -> Result<Queue, Error> {
    Queue::builder("postgres://unused")
        .pool(pool)
        .migration_mode(MigrationMode::Validate)
        .connect()
        .await
}

#[sqlx::test(migrations = "./migrations")]
async fn test_connect_is_idempotent_and_migrations_rerun(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    // A second connect runs SQLx's migration check as a no-op.
    let again = db.another_queue(|b| b).await;
    assert_eq!(again.name(), db.queue.name());
}

#[sqlx::test(migrations = "./migrations")]
async fn test_migrations_install_the_current_jobs_and_workers_columns(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let job_columns = sqlx::query_scalar::<_, String>(
        "SELECT column_name FROM information_schema.columns
         WHERE table_schema = 'pgqueue' AND table_name = 'jobs'
           AND column_name IN ('cron_expr', 'dedupe_key', 'heartbeat_ms', 'kind', 'max_attempts',
                               'result_ttl_ms', 'retried_at', 'ttl_ms', 'unique_key')
         ORDER BY column_name",
    )
    .fetch_all(db.queue.pool())
    .await
    .unwrap();
    assert_eq!(
        job_columns,
        [
            "cron_expr",
            "dedupe_key",
            "kind",
            "max_attempts",
            "result_ttl_ms",
            "retried_at"
        ]
    );

    let worker_accepting = sqlx::query_scalar::<_, String>(
        "SELECT column_name FROM information_schema.columns
         WHERE table_schema = 'pgqueue' AND table_name = 'workers' AND column_name = 'accepting'",
    )
    .fetch_optional(db.queue.pool())
    .await
    .unwrap();
    assert_eq!(worker_accepting.as_deref(), Some("accepting"));
}

#[sqlx::test(migrations = "./migrations")]
async fn test_migrations_install_the_cron_occurrence_ledger_and_expiry_index(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let primary_key = sqlx::query_scalar::<_, String>(
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint
         WHERE conrelid = 'pgqueue.cron_occurrences'::regclass AND contype = 'p'",
    )
    .fetch_one(db.queue.pool())
    .await
    .unwrap();
    assert!(
        primary_key.contains("PRIMARY KEY (queue, dedupe_key, scheduled_at)"),
        "{primary_key}"
    );

    let expiry_index = sqlx::query_scalar::<_, String>(
        "SELECT indexdef FROM pg_indexes
         WHERE schemaname = 'pgqueue' AND indexname = 'cron_occurrences_expiry_idx'",
    )
    .fetch_one(db.queue.pool())
    .await
    .unwrap();
    assert!(expiry_index.contains("(queue, expires_at)"));
}

#[sqlx::test(migrations = "./migrations")]
async fn test_migrations_install_the_registry_filtered_dequeue_index(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let definition = sqlx::query_scalar::<_, String>(
        "SELECT indexdef FROM pg_indexes
         WHERE schemaname = 'pgqueue' AND indexname = 'jobs_dequeue_name_idx'",
    )
    .fetch_one(db.queue.pool())
    .await
    .unwrap();
    assert!(definition.contains("(queue, name, priority, scheduled_at, id)"));
    assert!(definition.contains("status = 'queued'"));
}

/// Every index on `jobs` is paid for by every enqueue and by every attempt
/// state change — which already rewrites five entries — so the set is pinned
/// here and adding one has to be a deliberate decision. `jobs_name_status_idx`
/// was not: no statement the crate issues ever selected it. `Queue::jobs_page`
/// documents that its `status` and `name` filters are deliberately *not*
/// index-backed for exactly this reason, and the dashboard's own listing pages
/// through the kind-qualified indexes below.
#[sqlx::test(migrations = "./migrations")]
async fn test_migrations_install_only_the_job_indexes_the_crate_reads(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let indexes = sqlx::query_scalar::<_, String>(
        "SELECT indexname FROM pg_indexes
         WHERE schemaname = 'pgqueue' AND tablename = 'jobs'
         ORDER BY indexname",
    )
    .fetch_all(db.queue.pool())
    .await
    .unwrap();
    assert_eq!(
        indexes,
        [
            "jobs_active_idx",
            "jobs_dashboard_failure_idx",
            "jobs_dashboard_name_page_idx",
            "jobs_dashboard_name_prefix_idx",
            "jobs_dashboard_ready_idx",
            "jobs_dashboard_status_page_idx",
            "jobs_dedupe_key_idx",
            "jobs_dequeue_idx",
            "jobs_dequeue_name_idx",
            "jobs_expires_idx",
            "jobs_page_idx",
            "jobs_pkey",
        ]
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_migrations_install_the_cron_registry_key(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let schedule_key = sqlx::query_scalar::<_, String>(
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint
         WHERE conrelid = 'pgqueue.cron_schedules'::regclass AND contype = 'p'",
    )
    .fetch_one(db.queue.pool())
    .await
    .unwrap();
    assert!(
        schedule_key.contains("PRIMARY KEY (queue, dedupe_key)"),
        "{schedule_key}"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_migrations_install_the_live_dedupe_key_index(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let (indexdef, predicate) = sqlx::query_as::<_, (String, String)>(
        "SELECT pg_get_indexdef(indexes.indexrelid), pg_get_expr(indexes.indpred, indexes.indrelid) \
         FROM pg_index indexes \
         JOIN pg_class index_class ON index_class.oid = indexes.indexrelid \
         JOIN pg_class table_class ON table_class.oid = indexes.indrelid \
         JOIN pg_namespace namespace ON namespace.oid = table_class.relnamespace \
         WHERE namespace.nspname = 'pgqueue' AND index_class.relname = 'jobs_dedupe_key_idx'",
    )
    .fetch_one(db.queue.pool())
    .await
    .unwrap();
    assert!(indexdef.contains("UNIQUE INDEX"));
    assert!(indexdef.contains("(queue, dedupe_key)"));
    assert!(predicate.contains("dedupe_key IS NOT NULL"), "{predicate}");
    for status in ["queued", "running", "aborting"] {
        assert!(
            predicate.contains(status),
            "live index predicate missing {status}: {predicate}"
        );
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn test_migrations_use_an_isolated_history_table(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let pgqueue_history = sqlx::query_scalar::<_, Option<String>>(
        "SELECT to_regclass('pgqueue._sqlx_migrations')::text",
    )
    .fetch_one(db.queue.pool())
    .await
    .unwrap();
    let public_history = sqlx::query_scalar::<_, Option<String>>(
        "SELECT to_regclass('public._sqlx_migrations')::text",
    )
    .fetch_one(db.queue.pool())
    .await
    .unwrap();
    assert!(pgqueue_history.is_some());
    assert!(public_history.is_none());
}

//noinspection SqlNoDataSourceInspection
#[sqlx::test(migrations = "./migrations")]
async fn test_migration_mode_validate_needs_no_schema_ddl_privilege(pool: PgPool) {
    let restricted = crate::pool_with_max(&pool, 1).await;
    sqlx::query("SET ROLE pg_read_all_data")
        .execute(&restricted)
        .await
        .unwrap();
    let can_create = sqlx::query_scalar::<_, bool>(
        r#"SELECT has_schema_privilege(current_user, 'pgqueue', 'CREATE')"#,
    )
    .fetch_one(&restricted)
    .await
    .unwrap();
    assert!(!can_create);

    let queue = Queue::builder("postgres://unused")
        .pool(restricted)
        .migration_mode(MigrationMode::Validate)
        .connect()
        .await
        .unwrap();
    assert_eq!(queue.name(), "default");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_migration_mode_validate_rejects_a_dirty_migration(pool: PgPool) {
    let changed =
        sqlx::query("UPDATE pgqueue._sqlx_migrations SET success = false WHERE version = 1")
            .execute(&pool)
            .await
            .unwrap();
    assert_eq!(changed.rows_affected(), 1);

    let error = connect_with_validation(pool).await.unwrap_err();
    assert!(
        matches!(
            error,
            Error::Migration(sqlx::migrate::MigrateError::Dirty(1))
        ),
        "{error}"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_migration_mode_validate_rejects_an_unknown_migration(pool: PgPool) {
    let inserted = sqlx::query(
        "INSERT INTO pgqueue._sqlx_migrations
             (version, description, success, checksum, execution_time)
         SELECT 999999, 'unknown', true, checksum, 0
         FROM pgqueue._sqlx_migrations WHERE version = 1",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(inserted.rows_affected(), 1);

    let error = connect_with_validation(pool).await.unwrap_err();
    assert!(
        matches!(
            error,
            Error::Migration(sqlx::migrate::MigrateError::VersionMissing(999999))
        ),
        "{error}"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_migration_mode_validate_rejects_a_modified_migration(pool: PgPool) {
    let changed = sqlx::query(
        "UPDATE pgqueue._sqlx_migrations SET checksum = checksum || decode('00', 'hex')
         WHERE version = 1",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(changed.rows_affected(), 1);

    let error = connect_with_validation(pool).await.unwrap_err();
    assert!(
        matches!(
            error,
            Error::Migration(sqlx::migrate::MigrateError::VersionMismatch(1))
        ),
        "{error}"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_migration_mode_validate_rejects_a_missing_migration(pool: PgPool) {
    let deleted = sqlx::query(
        "DELETE FROM pgqueue._sqlx_migrations
         WHERE version = (SELECT max(version) FROM pgqueue._sqlx_migrations)",
    )
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(deleted.rows_affected(), 1);

    let error = connect_with_validation(pool).await.unwrap_err();
    assert!(
        matches!(error, Error::Config(ref message) if message.starts_with("database is missing pgqueue migration")),
        "{error}"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_migration_mode_skip_does_not_require_the_queue_schema(pool: PgPool) {
    sqlx::query("DROP SCHEMA pgqueue CASCADE")
        .execute(&pool)
        .await
        .unwrap();

    Queue::builder("postgres://unused")
        .pool(pool)
        .migration_mode(MigrationMode::Skip)
        .connect()
        .await
        .unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn test_migration_mode_validate_rejects_a_missing_migration_history(pool: PgPool) {
    sqlx::query("DROP SCHEMA pgqueue CASCADE")
        .execute(&pool)
        .await
        .unwrap();

    let error = Queue::builder("postgres://unused")
        .pool(pool)
        .migration_mode(MigrationMode::Validate)
        .connect()
        .await
        .unwrap_err();
    assert!(
        matches!(error, Error::Config(ref message) if message.contains("missing pgqueue migrations")),
        "{error}"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_connect_rejects_unsafe_queue_configuration(pool: PgPool) {
    for name in ["", ".", "..", "bad\nname"] {
        let err = Queue::builder("postgres://unused")
            .pool(pool.clone())
            .name(name)
            .connect()
            .await
            .expect_err("queue name should be rejected");
        assert!(matches!(err, Error::Config(_)), "{name:?}: {err}");
    }
    let dotted = Queue::builder("postgres://unused")
        .pool(pool.clone())
        .name("jobs.v2")
        .connect()
        .await
        .unwrap();
    assert_eq!(dotted.name(), "jobs.v2");
    let builder = Queue::builder("postgres://unused")
        .pool(pool)
        .priorities(1, -1);
    assert!(matches!(builder.connect().await, Err(Error::Config(_))));
    for builder in [
        Queue::builder("postgres://unused").connections(2, 1),
        Queue::builder("postgres://unused").connections(0, 0),
    ] {
        assert!(matches!(builder.connect().await, Err(Error::Config(_))));
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn test_enqueue_rejects_values_that_break_database_arithmetic(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let invalid = [
        with_config("zero-max-attempts", |config| config.max_attempts = 0),
        with_config("too-many-max-attempts", |config| {
            config.max_attempts = u32::MAX
        }),
        with_config("zero-timeout", |config| {
            config.timeout = Some(Duration::ZERO)
        }),
        with_config("zero-delay-backoff", |config| {
            config.backoff = JobRetryBackoff::Exponential { max: None }
        }),
        with_config("zero-max-backoff", |config| {
            config.retry_delay = Duration::from_millis(1);
            config.backoff = JobRetryBackoff::Exponential {
                max: Some(Duration::ZERO),
            };
        }),
        with_config("huge-delay", |config| config.retry_delay = Duration::MAX),
        new_job("", |_| {}),
        new_job("nul", |job| job.dedupe_key = Some("bad\0key".into())),
        new_job("long-dedupe", |job| job.dedupe_key = Some("x".repeat(256))),
    ];
    for job in invalid {
        assert!(
            matches!(db.queue.enqueue_raw(job).await, Err(Error::Config(_))),
            "invalid job must fail before reaching PostgreSQL"
        );
    }
    assert_eq!(db.queue.counts().await.unwrap().queued, 0);
}

/// The 255-byte limits above bind only Rust writers, and foreign SQL writers
/// exist by design (the enqueue advisory lock is opt-in). Without the DDL
/// checks an oversized dedupe key was not even refused: the dedupe index is
/// partial, so a terminal row carrying one landed silently, and the first
/// retry to copy that key onto a live row failed forever with a raw B-tree
/// `index row size exceeds` error. The columns now refuse at insert, at the
/// same byte counts as the Rust validators.
#[sqlx::test(migrations = "./migrations")]
async fn test_length_checks_refuse_oversized_foreign_writes(pool: PgPool) {
    let insert = "INSERT INTO pgqueue.jobs (queue, name, dedupe_key, status, max_attempts)
                  VALUES ($1, $2, $3, $4, 1)";

    // At the limit every column is still accepted — the checks must not be
    // tighter than the validators — and an empty dedupe key stays legal
    // because no Rust writer refuses one.
    for dedupe_key in [Some("k".repeat(255)), Some(String::new()), None] {
        sqlx::query(insert)
            .bind("q".repeat(255))
            .bind("n".repeat(255))
            .bind(&dedupe_key)
            .bind("queued")
            .execute(&pool)
            .await
            .unwrap_or_else(|error| panic!("{dedupe_key:?} must be accepted: {error}"));
        sqlx::query("DELETE FROM pgqueue.jobs")
            .execute(&pool)
            .await
            .unwrap();
    }

    // One byte over — including 128 two-byte characters, because the limits
    // are `octet_length` bytes exactly as `str::len` counts them — and the
    // empty values `JobRequest::validate` and `validate_queue_name` refuse.
    // `status = 'failed'` on the dedupe cases is the write the partial index
    // used to wave through.
    let rejected = [
        ("q".repeat(256), "n".into(), None, "jobs_queue_check"),
        (String::new(), "n".into(), None, "jobs_queue_check"),
        ("q".into(), "n".repeat(256), None, "jobs_name_check"),
        ("q".into(), "é".repeat(128), None, "jobs_name_check"),
        ("q".into(), String::new(), None, "jobs_name_check"),
        (
            "q".into(),
            "n".into(),
            Some("k".repeat(256)),
            "jobs_dedupe_key_check",
        ),
        (
            "q".into(),
            "n".into(),
            Some("é".repeat(128)),
            "jobs_dedupe_key_check",
        ),
    ];
    for (queue, name, dedupe_key, constraint) in rejected {
        let error = sqlx::query(insert)
            .bind(&queue)
            .bind(&name)
            .bind(&dedupe_key)
            .bind(if dedupe_key.is_some() {
                "failed"
            } else {
                "queued"
            })
            .execute(&pool)
            .await
            .unwrap_err();
        assert_eq!(
            error
                .as_database_error()
                .and_then(|error| error.constraint()),
            Some(constraint),
            "queue {} / name {} / dedupe key {:?} must be refused: {error}",
            queue.len(),
            name.len(),
            dedupe_key.as_deref().map(str::len),
        );
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn test_enqueue_round_trips_all_fields(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let result = db
        .queue
        .enqueue_raw(new_job("send_email", |job| {
            job.payload = json!({"to": "a@b.c"});
            job.meta = json!({"trace": "xyz"});
            job.config.priority = -3;
            job.config.max_attempts = 5;
            job.config.timeout = Some(Duration::from_secs(30));
            job.config.retry_delay = Duration::from_millis(250);
            job.config.backoff = JobRetryBackoff::Exponential {
                max: Some(Duration::from_secs(60)),
            };
            job.config.retention = JobRetention::For(Duration::from_secs(3600));
        }))
        .await
        .unwrap();
    assert!(result.is_enqueued());
    let id = result.job_id();

    let row = db.queue.fetch_job(id).await.unwrap().expect("job exists");
    assert_eq!(row.id, id);
    assert_eq!(row.queue, "default");
    assert_eq!(row.name, "send_email");
    assert_eq!(row.payload, json!({"to": "a@b.c"}));
    assert_eq!(row.meta, json!({"trace": "xyz"}));
    assert_eq!(row.status, JobStatus::Queued);
    assert_eq!(row.priority, -3);
    assert_eq!(row.attempts, 0);
    assert_eq!(row.max_attempts, 5);
    assert_eq!(row.timeout(), Some(Duration::from_secs(30)));
    assert_eq!(row.retry_delay_ms, 250);
    assert_eq!(
        row.backoff,
        JobRetryBackoff::Exponential {
            max: Some(Duration::from_secs(60))
        }
    );
    assert_eq!(
        row.retention(),
        JobRetention::For(Duration::from_secs(3600))
    );
    assert!(row.retryable());
    assert!(row.started_at.is_none());
    assert!(row.completed_at.is_none());
    assert!(row.result.is_none());
    assert!(row.error.is_none());
    assert!(row.worker_id.is_none());
    assert!(row.dedupe_key.is_none());
}

#[sqlx::test(migrations = "./migrations")]
async fn test_enqueue_raw_in_obeys_the_caller_transaction(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let mut committed = db.queue.pool().begin().await.unwrap();
    let first = db
        .queue
        .enqueue_raw_in(&mut committed, new_job("committed-a", |_| {}))
        .await
        .unwrap()
        .into_job_id();
    let second = db
        .queue
        .enqueue_raw_in(&mut committed, new_job("committed-b", |_| {}))
        .await
        .unwrap()
        .into_job_id();
    assert!(db.queue.fetch_job(first).await.unwrap().is_none());
    assert!(db.queue.fetch_job(second).await.unwrap().is_none());
    committed.commit().await.unwrap();
    assert!(db.queue.fetch_job(first).await.unwrap().is_some());
    assert!(db.queue.fetch_job(second).await.unwrap().is_some());

    let mut rolled_back = db.queue.pool().begin().await.unwrap();
    let discarded = db
        .queue
        .enqueue_raw_in(&mut rolled_back, new_job("discarded", |_| {}))
        .await
        .unwrap()
        .into_job_id();
    rolled_back.rollback().await.unwrap();
    assert!(db.queue.fetch_job(discarded).await.unwrap().is_none());
}

/// The advisory lock the enqueue takes binds only writers that take it too, so
/// a row committed straight into `pgqueue.jobs` can claim the key between the
/// guarded read and the insert. `READ COMMITTED` sees that row on the next
/// statement, so the collision is reported the way every other collision is —
/// with the live holder's id.
#[sqlx::test(migrations = "./migrations")]
async fn test_enqueue_raw_in_deduplicates_against_a_dedupe_owner_committed_out_of_band(
    pool: PgPool,
) {
    const INSERT_GATE: i32 = 20_571;

    let control = TestDb::new(crate::pool_with_max(&pool, 3).await).await;
    let db = TestDb::new(crate::pool_with_max(&pool, 2).await).await;
    // Pause the caller transaction between its visibility read and its insert.
    crate::install_statement_gate(
        control.queue.pool(),
        "wait_at_job_insert",
        INSERT_GATE,
        "INSERT",
        "NEW.name = 'gated-insert'",
    )
    .await;
    let gate = crate::hold_gate(control.queue.pool(), INSERT_GATE, &control.database).await;

    let queue = db.queue.clone();
    let pool_for_caller = db.queue.pool().clone();
    let enqueue = tokio::spawn(async move {
        let mut caller = pool_for_caller.begin().await.unwrap();
        let result = queue
            .enqueue_raw_in(
                &mut caller,
                new_job("gated-insert", |job| {
                    job.dedupe_key = Some("snapshot:key".into());
                }),
            )
            .await;
        caller.rollback().await.unwrap();
        result
    });
    crate::wait_for_lock_waiter(
        &control,
        "%WITH inserted AS (%",
        "transactional enqueue did not reach its insert",
    )
    .await;

    // A row that takes the dedupe key without going through the enqueue
    // advisory lock: the caller's snapshot cannot see it, but the partial unique
    // index still refuses the insert.
    let owner = sqlx::query_scalar::<_, uuid::Uuid>(
        r#"INSERT INTO pgqueue.jobs (queue, name, payload, dedupe_key, status, max_attempts)
           VALUES ($1, 'out-of-band', 'null'::jsonb, 'snapshot:key', 'queued', 1)
           RETURNING id"#,
    )
    .bind(control.queue.name())
    .fetch_one(control.queue.pool())
    .await
    .unwrap();
    gate.rollback().await.unwrap();

    let result = enqueue
        .await
        .unwrap()
        .expect("a dedupe collision is not an error");
    assert!(result.is_deduplicated(), "the key was already taken");
    assert_eq!(
        result.into_job_id(),
        owner,
        "the collision must name the live holder of the key"
    );
}

/// The other half of the out-of-band collision above: the foreign row that
/// blocked the insert is gone again by the time the collision is re-read, so
/// there is no live holder to report. Nothing about the request is invalid —
/// retrying the same enqueue succeeds — so the failure must stay
/// distinguishable from a permanent configuration error.
#[sqlx::test(migrations = "./migrations")]
async fn test_enqueue_raw_reports_a_vanished_out_of_band_dedupe_owner_as_retryable(pool: PgPool) {
    const INSERT_GATE: i32 = 20_572;
    const CONFLICT_GATE: i32 = 20_573;

    let control = TestDb::new(crate::pool_with_max(&pool, 4).await).await;
    let db = TestDb::new(crate::pool_with_max(&pool, 2).await).await;
    // Pause the enqueue between its visibility read and its insert...
    install_statement_gate(
        control.queue.pool(),
        "wait_at_job_insert",
        INSERT_GATE,
        "INSERT",
        "NEW.name = 'gated-insert'",
    )
    .await;
    // ...and between its conflict decision and its collision re-read.
    crate::install_conflicted_insert_gate(control.queue.pool(), "wait_at_conflict", CONFLICT_GATE)
        .await;
    let insert_gate = hold_gate(control.queue.pool(), INSERT_GATE, &control.database).await;
    let conflict_gate = hold_gate(control.queue.pool(), CONFLICT_GATE, &control.database).await;

    let queue = db.queue.clone();
    let enqueue = tokio::spawn(async move {
        queue
            .enqueue_raw(new_job("gated-insert", |job| {
                job.dedupe_key = Some("vanishing:key".into());
            }))
            .await
    });
    wait_for_lock_waiter(
        &control,
        "%WITH inserted AS (%",
        "enqueue did not reach its insert",
    )
    .await;

    // A row that takes the dedupe key without the enqueue advisory lock...
    let owner = sqlx::query_scalar::<_, Uuid>(
        r#"INSERT INTO pgqueue.jobs (queue, name, payload, dedupe_key, status, max_attempts)
           VALUES ($1, 'out-of-band', 'null'::jsonb, 'vanishing:key', 'queued', 1)
           RETURNING id"#,
    )
    .bind(control.queue.name())
    .fetch_one(control.queue.pool())
    .await
    .unwrap();
    insert_gate.rollback().await.unwrap();
    crate::wait_for_advisory_waiter(
        control.queue.pool(),
        CONFLICT_GATE,
        "conflicted enqueue did not reach its collision re-read",
    )
    .await;
    // ...and releases it again before the enqueue can name it.
    sqlx::query("DELETE FROM pgqueue.jobs WHERE id = $1")
        .bind(owner)
        .execute(control.queue.pool())
        .await
        .unwrap();
    conflict_gate.rollback().await.unwrap();

    let error = enqueue.await.unwrap().unwrap_err();
    assert!(
        matches!(&error, Error::DedupeRace(message) if message.contains("released again")),
        "a transient dedupe race must not look like invalid input: {error:?}"
    );

    // The race is transient: the identical request enqueues cleanly afterwards.
    let retried = db
        .queue
        .enqueue_raw(new_job("gated-insert", |job| {
            job.dedupe_key = Some("vanishing:key".into());
        }))
        .await
        .unwrap();
    assert!(
        retried.is_enqueued(),
        "retrying the raced enqueue must succeed"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_transactional_dedupe_does_not_lock_the_existing_job_row(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let dedupe_job = || {
        new_job("deduplicated", |job| {
            job.dedupe_key = Some("transactional-dedupe".into())
        })
    };
    let id = db.queue.enqueue_raw(dedupe_job()).await.unwrap().unwrap();
    let active = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);

    let mut transaction = db.queue.pool().begin().await.unwrap();
    let result = db
        .queue
        .enqueue_raw_in(&mut transaction, dedupe_job())
        .await
        .unwrap();
    assert_eq!(result.job_id(), id);
    assert!(matches!(
        result,
        EnqueueResult::Deduplicated(existing) if existing == id
    ));

    let queue = db.queue.clone();
    let finishing = tokio::spawn(async move {
        queue
            .finish(&active, JobStatus::Complete, Some(json!("done")), None)
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_secs(1), finishing)
            .await
            .expect("dedupe read held a row lock until caller commit")
            .unwrap()
            .unwrap()
    );
    transaction.rollback().await.unwrap();
    assert_eq!(
        db.queue.fetch_job(id).await.unwrap().unwrap().status,
        JobStatus::Complete
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_enqueue_raw_in_returns_serialization_failure_for_invisible_dedupe_owner(
    pool: PgPool,
) {
    let db = TestDb::new(pool.clone()).await;
    let dedupe_job = || {
        new_job("snapshot-dedupe", |job| {
            job.dedupe_key = Some("snapshot-dedupe".into())
        })
    };
    let mut transaction = db.queue.pool().begin().await.unwrap();
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *transaction)
        .await
        .unwrap();
    sqlx::query_scalar::<_, i32>(r#"SELECT 1"#)
        .fetch_one(&mut *transaction)
        .await
        .unwrap();

    db.queue.enqueue_raw(dedupe_job()).await.unwrap();
    let error = db
        .queue
        .enqueue_raw_in(&mut transaction, dedupe_job())
        .await
        .unwrap_err();
    match error {
        Error::Db(sqlx::Error::Database(error)) => {
            assert_eq!(error.code().as_deref(), Some("40001"));
        }
        other => panic!("expected PostgreSQL serialization failure, got {other}"),
    }
    transaction.rollback().await.unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn test_enqueue_rounds_nonzero_fractional_milliseconds_up(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(new_job("precise", |job| {
            job.config.timeout = Some(Duration::from_micros(500));
            job.config.retry_delay = Duration::from_nanos(1);
            job.config.retention = JobRetention::For(Duration::from_micros(1_500));
            job.config.backoff = JobRetryBackoff::Exponential {
                max: Some(Duration::from_micros(500)),
            };
        }))
        .await
        .unwrap()
        .unwrap();

    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.timeout_ms, Some(1));
    assert_eq!(row.retry_delay_ms, 1);
    assert_eq!(row.result_ttl_ms, Some(2));
    assert_eq!(
        row.backoff,
        JobRetryBackoff::Exponential {
            max: Some(Duration::from_millis(1))
        }
    );
}

//noinspection SqlNoDataSourceInspection
#[sqlx::test(migrations = "./migrations")]
async fn test_stored_backoff_without_max_ms_still_decodes_and_dequeues(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(new_job("external", |_| {}))
        .await
        .unwrap()
        .unwrap();
    // An external client (ops script, manual UPDATE) may store the
    // exponential variant without a max_ms key; the row must not poison
    // every dequeue batch that selects it.
    sqlx::query(
        r#"UPDATE pgqueue.jobs SET backoff = '{"type":"exponential"}'::jsonb WHERE id = $1"#,
    )
    .bind(id)
    .execute(db.queue.pool())
    .await
    .unwrap();

    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.backoff, JobRetryBackoff::Exponential { max: None });
    let active = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);
    assert_eq!(active.id, id);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_missing_job_operations_return_their_documented_results(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    assert!(db.queue.fetch_job(Uuid::now_v7()).await.unwrap().is_none());
    assert!(!db.queue.abort_job(Uuid::now_v7(), "x").await.unwrap());
    assert!(!db.queue.retry_job(Uuid::now_v7(), "x").await.unwrap());
    assert!(matches!(
        db.queue.dequeue(0, Uuid::now_v7()).await,
        Err(Error::Config(_))
    ));
    assert!(matches!(
        db.queue
            .jobs_page(JobFilter {
                limit: Some(-1),
                ..JobFilter::default()
            })
            .await,
        Err(Error::Config(_))
    ));
    assert!(matches!(
        db.queue
            .jobs_page(JobFilter {
                limit: Some(1001),
                ..JobFilter::default()
            })
            .await,
        Err(Error::Config(_))
    ));
}

#[sqlx::test(migrations = "./migrations")]
async fn test_dedupe_key_dedupes_live_jobs_and_preserves_terminal_occurrences(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(new_job("cron_job", |job| {
            job.dedupe_key = Some("cron:cron_job".into());
        }))
        .await
        .unwrap()
        .expect("first enqueue");
    let t0 = db.queue.fetch_job(id).await.unwrap().unwrap().scheduled_at;
    let enqueue_at = |at| {
        new_job("cron_job", move |job| {
            job.dedupe_key = Some("cron:cron_job".into());
            job.scheduled_at = Some(at);
        })
    };
    // Live job with the same key: dedupe hit.
    assert!(
        db.queue
            .enqueue_raw(enqueue_at(t0 + chrono::Duration::seconds(5)))
            .await
            .unwrap()
            .is_none()
    );

    // Finish it, then re-enqueue with a later schedule: a new occurrence gets
    // a new ID while the first result remains addressable.
    let worker = Uuid::now_v7();
    let jobs = db.queue.dequeue(1, worker).await.unwrap();
    assert_eq!(jobs.len(), 1);
    db.queue
        .finish(&jobs[0], JobStatus::Complete, None, None)
        .await
        .unwrap();

    let second = db
        .queue
        .enqueue_raw(enqueue_at(t0 + chrono::Duration::seconds(1)))
        .await
        .unwrap()
        .expect("enqueue after terminal occurrence");
    assert_ne!(second, id, "each occurrence needs a stable ID");
    assert_eq!(
        db.queue.fetch_job(id).await.unwrap().unwrap().status,
        JobStatus::Complete
    );
    let row = db.queue.fetch_job(second).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Queued);
    assert_eq!(row.attempts, 0, "revive resets attempts");
    assert!(row.result.is_none());

    // Finish again; ordinary deduplicated jobs enqueue even with an earlier/equal
    // schedule. Only the cron-specific enqueue path applies occurrence ordering.
    sqlx::query("UPDATE pgqueue.jobs SET scheduled_at = now() WHERE id = $1")
        .bind(second)
        .execute(db.queue.pool())
        .await
        .unwrap();
    let jobs = db.queue.dequeue(1, worker).await.unwrap();
    assert_eq!(jobs.len(), 1, "revived occurrence is due by now");
    db.queue
        .finish(&jobs[0], JobStatus::Complete, None, None)
        .await
        .unwrap();
    let third = db
        .queue
        .enqueue_raw(enqueue_at(t0))
        .await
        .unwrap()
        .expect("terminal dedupe key can be reused regardless of schedule");
    assert_ne!(third, id);
    assert_ne!(third, second);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_concurrent_dedupe_enqueues_accept_exactly_one_live_occurrence(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let mut enqueues = tokio::task::JoinSet::new();
    for _ in 0..100 {
        let queue = db.queue.clone();
        enqueues.spawn(async move {
            queue
                .enqueue_raw(new_job("singleton", |job| {
                    job.dedupe_key = Some("contended-key".into());
                }))
                .await
                .unwrap()
        });
    }
    let mut accepted = Vec::new();
    while let Some(result) = enqueues.join_next().await {
        if let EnqueueResult::Enqueued(id) = result.unwrap() {
            accepted.push(id);
        }
    }
    assert_eq!(accepted.len(), 1);
    assert_eq!(db.queue.counts().await.unwrap().queued, 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_retry_job_occurrence_uses_wall_clock_after_dedupe_lock_contention(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(new_job("retry-contended", |job| {
            job.dedupe_key = Some("retry-contended".into());
        }))
        .await
        .unwrap()
        .unwrap();
    assert!(db.queue.abort_job(id, "terminal").await.unwrap());

    let mut lock_tx = db.queue.pool().begin().await.unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock($1, hashtext(length($2)::text || ':' || $2 || $3))")
        .bind(pgqueue::__test_support::dedupe_enqueue_lock_key(
            &db.database,
        ))
        .bind(db.queue.name())
        .bind("retry-contended")
        .execute(&mut *lock_tx)
        .await
        .unwrap();

    let queue = db.queue.clone();
    let mut retrying =
        tokio::spawn(async move { queue.retry_job_occurrence(id, "manual retry").await });
    crate::wait_for_dequeue_lock_waiter(&db.queue, true).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut retrying)
            .await
            .is_err(),
        "retry did not wait for the dedupe-key lock"
    );
    let released_at =
        sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>(r#"SELECT clock_timestamp()"#)
            .fetch_one(db.queue.pool())
            .await
            .unwrap();
    lock_tx.rollback().await.unwrap();

    let retry_id = tokio::time::timeout(Duration::from_secs(5), retrying)
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .unwrap();
    let row = db.queue.fetch_job(retry_id).await.unwrap().unwrap();
    assert!(row.scheduled_at >= released_at);
    assert!(row.enqueued_at >= released_at);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_aborted_future_scheduled_deduplicated_jobs_can_be_reenqueued(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    // A deduplicated job scheduled for tomorrow, aborted while still queued: the
    // terminal row keeps its future schedule.
    let id = db
        .queue
        .enqueue_raw(new_job("report", |job| {
            job.dedupe_key = Some("report:x".into());
            job.scheduled_at = Some(Utc::now() + chrono::Duration::days(1));
        }))
        .await
        .unwrap()
        .unwrap();
    assert!(db.queue.abort_job(id, "changed plans").await.unwrap());

    // Re-enqueueing the key to run now must create a new occurrence, not no-op
    // until tomorrow.
    let next = db
        .queue
        .enqueue_raw(new_job("report", |job| {
            job.dedupe_key = Some("report:x".into());
        }))
        .await
        .unwrap()
        .expect("dead future-scheduled key must be reusable");
    assert_ne!(next, id);
    assert_eq!(
        db.queue.fetch_job(id).await.unwrap().unwrap().status,
        JobStatus::Aborted
    );
    let row = db.queue.fetch_job(next).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Queued);
    assert!(row.scheduled_at <= Utc::now() + chrono::Duration::seconds(1));
}

#[sqlx::test(migrations = "./migrations")]
async fn test_dequeue_orders_by_priority_then_schedule(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    for priority in [1i16, -1, 0] {
        db.queue
            .enqueue_raw(with_config("prio", |c| c.priority = priority))
            .await
            .unwrap()
            .unwrap();
    }
    let jobs = db.queue.dequeue(10, Uuid::now_v7()).await.unwrap();
    let got: Vec<i16> = jobs.iter().map(|j| j.priority).collect();
    assert_eq!(got, vec![-1, 0, 1]);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_dequeue_marks_rows_active(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    db.queue.enqueue_raw(new_job("j", |_| {})).await.unwrap();
    let worker = Uuid::now_v7();
    let jobs = db.queue.dequeue(5, worker).await.unwrap();
    assert_eq!(jobs.len(), 1);
    let row = &jobs[0];
    assert_eq!(row.status, JobStatus::Running);
    assert_eq!(row.attempts, 1);
    assert_eq!(row.worker_id, Some(worker));
    assert!(row.started_at.is_some());
    assert!(row.touched_at.is_some());

    // Nothing left.
    assert!(db.queue.dequeue(5, worker).await.unwrap().is_empty());
}

#[sqlx::test(migrations = "./migrations")]
async fn test_consumer_heartbeat_and_finish_use_guarded_capabilities(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let worker_id = Uuid::now_v7();
    let consumer = db.queue.consumer(worker_id);
    consumer
        .heartbeat(
            json!({"complete": 0}),
            Some(json!({"kind": "custom"})),
            Duration::from_secs(30),
        )
        .await
        .unwrap();
    let id = db
        .queue
        .enqueue_raw(new_job("custom-consumer", |_| {}))
        .await
        .unwrap()
        .unwrap();

    let attempts = consumer.dequeue(1).await.unwrap();
    assert_eq!(attempts.len(), 1);
    let attempt = attempts.into_iter().next().unwrap();
    assert_eq!(attempt.job().id, id);
    assert_eq!(attempt.job().worker_id, Some(worker_id));
    assert!(
        attempt
            .finish(JobStatus::Complete, Some(json!("ok")), None)
            .await
            .unwrap()
    );

    let info = db.queue.info().await.unwrap();
    assert!(info.workers.iter().any(|worker| worker.id == worker_id));
    assert_eq!(
        db.queue.fetch_job(id).await.unwrap().unwrap().status,
        JobStatus::Complete
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_consumer_attempt_finish_can_retry_after_a_pool_timeout(pool: PgPool) {
    let constrained = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(1))
        .connect_with(pool.connect_options().as_ref().clone())
        .await
        .unwrap();
    let db = TestDb::new(constrained).await;
    let id = db
        .queue
        .enqueue_raw(new_job("retry-finish", |_| {}))
        .await
        .unwrap()
        .unwrap();
    let attempt = crate::leased_consumer(&db.queue, Uuid::now_v7())
        .await
        .dequeue(1)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    let connection = db.pool.acquire().await.unwrap();
    assert!(matches!(
        attempt
            .finish(JobStatus::Complete, Some(json!("ok")), None)
            .await,
        Err(Error::Db(sqlx::Error::PoolTimedOut))
    ));
    drop(connection);

    assert!(
        attempt
            .finish(JobStatus::Complete, Some(json!("ok")), None)
            .await
            .unwrap()
    );
    assert_eq!(
        db.queue.fetch_job(id).await.unwrap().unwrap().status,
        JobStatus::Complete
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_consumer_attempt_retry_refusal_leaves_the_attempt_finishable(pool: PgPool) {
    let db = TestDb::new(pool).await;
    let id = db
        .queue
        .enqueue_raw(with_config("refused-retry", |config| {
            config.max_attempts = 1;
        }))
        .await
        .unwrap()
        .unwrap();
    let attempt = crate::leased_consumer(&db.queue, Uuid::now_v7())
        .await
        .dequeue(1)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    assert!(!attempt.retry("attempt failed").await.unwrap());
    assert!(
        attempt
            .finish(JobStatus::Failed, None, Some("attempt failed"))
            .await
            .unwrap()
    );
    assert_eq!(
        db.queue.fetch_job(id).await.unwrap().unwrap().status,
        JobStatus::Failed
    );
}

/// A JSON document of `depth` nested objects wrapped around `null`.
fn nested_json(depth: usize) -> serde_json::Value {
    let mut value = serde_json::Value::Null;
    for _ in 0..depth {
        value = json!({ "a": value });
    }
    value
}

/// `json_contains_nul` guarded the one value `jsonb` cannot *hold*, but nothing
/// guarded the values `serde_json` cannot *read back*: its deserializer stops at
/// 128 nested containers, and every read of `payload`, `meta` and `result` goes
/// through it. So the crate accepted, wrote and acknowledged a document it could
/// never decode again.
///
/// The damage was not confined to the bad row. The dequeue statement commits
/// `status = 'running'`, `attempts + 1` and `worker_id` server-side *before* the
/// client decodes the returned rows, so one undecodable row failed the whole
/// batch's decode and stranded every healthy job claimed alongside it — each
/// with an attempt spent and nobody processing it. `fetch_job`, `jobs_page` and
/// the dashboard listing failed for the entire queue for as long as the row was
/// retained.
#[sqlx::test(migrations = "./migrations")]
async fn test_enqueue_refuses_json_nested_deeper_than_it_can_be_read_back(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;

    // 127 is the deepest `serde_json` decodes, so it must still be accepted:
    // an off-by-one here silently narrows the documented payload space.
    let deepest = nested_json(127);
    let id = db
        .queue
        .enqueue_raw(new_job("deep-ok", |job| {
            job.payload = deepest.clone();
            job.meta = deepest.clone();
        }))
        .await
        .unwrap()
        .unwrap();
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(
        row.payload, deepest,
        "the deepest legal payload must survive"
    );
    assert_eq!(row.meta, deepest);
    // The read path the poison row broke for the whole queue.
    let claimed = crate::leased_consumer(&db.queue, Uuid::now_v7())
        .await
        .dequeue(10)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);

    for (field, customize) in [
        (
            "job payload",
            Box::new(|job: &mut pgqueue::JobRequest| job.payload = nested_json(128))
                as Box<dyn FnOnce(&mut pgqueue::JobRequest)>,
        ),
        (
            "job meta",
            Box::new(|job: &mut pgqueue::JobRequest| job.meta = nested_json(200)),
        ),
    ] {
        let refused = db
            .queue
            .enqueue_raw(new_job("too-deep", customize))
            .await
            .unwrap_err();
        assert!(
            matches!(&refused, Error::Config(message)
                if *message == format!("{field} must not nest deeper than 127 levels")),
            "{field}: {refused:?}"
        );
    }
    // Refused before the write, not after it: nothing named `too-deep` exists.
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM pgqueue.jobs WHERE queue = $1 AND name = 'too-deep'"
        )
        .bind(db.queue.name())
        .fetch_one(db.queue.pool())
        .await
        .unwrap(),
        0,
        "a payload that can never be decoded must not reach the table"
    );

    // The same limit on the finalization value, which the consumer API reaches.
    let attempt = claimed.into_iter().next().unwrap();
    let refused = attempt
        .finish(JobStatus::Complete, Some(nested_json(128)), None)
        .await
        .unwrap_err();
    assert!(
        matches!(&refused, Error::Config(message)
            if message == "job result must not nest deeper than 127 levels"),
        "{refused:?}"
    );
    // Refused, not spent: the attempt is still the caller's to finalize.
    assert!(
        attempt
            .finish(JobStatus::Complete, Some(nested_json(127)), None)
            .await
            .unwrap()
    );
    assert_eq!(
        db.queue.fetch_job(id).await.unwrap().unwrap().result,
        Some(nested_json(127))
    );
}

/// The depth guard was added to the four writers that reach `jobs`, but
/// `Consumer::heartbeat` — the public lease writer of the custom-consumer API —
/// validated only its TTL, so `stats` and `metadata` went into
/// `pgqueue.workers.jsonb` unchecked. A NUL there *fails* the write (`22P05`);
/// excess depth *succeeds* and breaks the reads, and not only for the offending
/// lease: `Database::workers` decodes every live lease of the queue in one
/// statement, so `Queue::info` and both dashboard worker views answered
/// `ColumnDecode { source: "recursion limit exceeded" }` for the whole queue for
/// as long as the consumer kept renewing it.
#[sqlx::test(migrations = "./migrations")]
async fn test_consumer_heartbeat_refuses_json_nested_deeper_than_it_can_be_read_back(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let worker_id = Uuid::now_v7();
    let consumer = db.queue.consumer(worker_id);

    for (field, stats, metadata) in [
        ("worker stats", nested_json(128), None),
        ("worker metadata", json!({}), Some(nested_json(200))),
    ] {
        let refused = consumer
            .heartbeat(stats, metadata, Duration::from_secs(30))
            .await
            .unwrap_err();
        assert!(
            matches!(&refused, Error::Config(message)
                if *message == format!("{field} must not nest deeper than 127 levels")),
            "{field}: {refused:?}"
        );
    }
    // Refused before the write, not after it: no lease exists to poison the
    // queue's worker reads.
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM pgqueue.workers WHERE id = $1")
            .bind(worker_id)
            .fetch_one(db.queue.pool())
            .await
            .unwrap(),
        0,
        "a lease that can never be decoded must not reach the table"
    );

    // 127 is the deepest `serde_json` decodes, so it must still be accepted, and
    // the read that a deeper one broke must still answer.
    let deepest = nested_json(127);
    consumer
        .heartbeat(
            deepest.clone(),
            Some(deepest.clone()),
            Duration::from_secs(30),
        )
        .await
        .unwrap();
    let info = db.queue.info().await.unwrap();
    let worker = info
        .workers
        .iter()
        .find(|worker| worker.id == worker_id)
        .expect("the accepted lease is listed");
    assert_eq!(worker.stats, deepest);
    assert_eq!(worker.metadata, Some(deepest));
}

/// The other half of the same invariant, and the half the depth guard above left
/// out: `jsonb` cannot store a NUL at all, so one in `stats` or `metadata` came
/// back as `Error::Db` (`22P05`) — indistinguishable from the pool timeout a
/// heartbeat loop is documented to run through. Retrying it renews nothing, so
/// the loop spins until the lease expires and the sweeper reclaims every attempt
/// the consumer has claimed. Every other writer of these columns already refuses
/// one locally.
#[sqlx::test(migrations = "./migrations")]
async fn test_consumer_heartbeat_refuses_a_nul_in_its_lease_values(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let worker_id = Uuid::now_v7();
    let consumer = db.queue.consumer(worker_id);

    for (field, stats, metadata) in [
        ("worker stats", json!({"queue": "bu\0sy"}), None),
        (
            "worker metadata",
            json!({}),
            Some(json!({"host": ["a\0b"]})),
        ),
    ] {
        let refused = consumer
            .heartbeat(stats, metadata, Duration::from_secs(30))
            .await
            .unwrap_err();
        assert!(
            matches!(&refused, Error::Config(message)
                if *message == format!("{field} must not contain NUL")),
            "{field}: {refused:?}"
        );
    }
    // Refused before a connection is taken, so it cannot be read as pool
    // exhaustion either — and no lease was written.
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM pgqueue.workers WHERE id = $1")
            .bind(worker_id)
            .fetch_one(db.queue.pool())
            .await
            .unwrap(),
        0,
        "an unstorable lease must not reach the table"
    );

    // The same values without the NUL still write, so the guard refuses the
    // character and not the shape.
    consumer
        .heartbeat(
            json!({"queue": "busy"}),
            Some(json!({"host": ["ab"]})),
            Duration::from_secs(30),
        )
        .await
        .unwrap();
    let info = db.queue.info().await.unwrap();
    let worker = info
        .workers
        .iter()
        .find(|worker| worker.id == worker_id)
        .expect("the accepted lease is listed");
    assert_eq!(worker.stats, json!({"queue": "busy"}));
    assert_eq!(worker.metadata, Some(json!({"host": ["ab"]})));
}

/// A NUL is permanently invalid, not a transient failure: `jsonb` answers one
/// with `22P05` and `text` with `22021`, both `Error::Db` — indistinguishable
/// from the pool timeout these two methods explicitly invite the caller to
/// retry. A conforming retry loop therefore spun forever and the row never left
/// `running`. Every other writer on this side of the wire already refuses one;
/// the public consumer API was the one that did not.
#[sqlx::test(migrations = "./migrations")]
async fn test_consumer_attempt_refuses_a_nul_in_a_finalization_value(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(with_config("nul-finalization", |config| {
            config.max_attempts = 2;
        }))
        .await
        .unwrap()
        .unwrap();
    let attempt = crate::leased_consumer(&db.queue, Uuid::now_v7())
        .await
        .dequeue(1)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    for (field, result, error) in [
        ("job result", Some(json!({"log": ["do\0ne"]})), None),
        ("job error", None, Some("fai\0led")),
    ] {
        let refused = attempt
            .finish(JobStatus::Failed, result, error)
            .await
            .unwrap_err();
        assert!(
            matches!(&refused, Error::Config(message)
                if *message == format!("{field} must not contain NUL")),
            "{refused:?}"
        );
    }
    let refused = attempt.retry("fai\0led").await.unwrap_err();
    assert!(
        matches!(&refused, Error::Config(message)
            if message == "job error must not contain NUL"),
        "{refused:?}"
    );

    // Refused, not spent: the attempt is still the caller's to finalize.
    assert!(attempt.retry("failed").await.unwrap());
    assert_eq!(
        db.queue.fetch_job(id).await.unwrap().unwrap().status,
        JobStatus::Queued
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_consumer_attempt_cannot_finish_a_newer_attempt(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(with_config("fenced-consumer", |config| {
            config.max_attempts = 2;
            config.timeout = Some(Duration::from_millis(10));
        }))
        .await
        .unwrap()
        .unwrap();
    let first_worker = Uuid::now_v7();
    let first = crate::leased_consumer(&db.queue, first_worker)
        .await
        .dequeue(1)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    backdate_job_liveness(&db, id).await;
    // The sweeper only recovers an attempt whose owner is gone, so retire the
    // first consumer's lease before asking it to.
    crate::expire_worker(&db, first_worker).await;
    let mut sweeper = db.queue.sweeper();
    assert_eq!(sweeper.sweep().await.unwrap().cancelling, vec![id]);
    assert_eq!(sweeper.sweep().await.unwrap().swept, vec![id]);
    sweeper.release().await;

    let second = crate::leased_consumer(&db.queue, Uuid::now_v7())
        .await
        .dequeue(1)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(second.job().attempts, 2);
    assert!(
        !first
            .finish(JobStatus::Complete, Some(json!("stale")), None)
            .await
            .unwrap()
    );
    assert!(
        second
            .finish(JobStatus::Complete, Some(json!("fresh")), None)
            .await
            .unwrap()
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_dequeue_respects_limit_schedule_and_priority_range(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    for _ in 0..3 {
        db.queue.enqueue_raw(new_job("j", |_| {})).await.unwrap();
    }
    // A job scheduled in the future must not be dequeued.
    db.queue
        .enqueue_raw(new_job("future", |job| {
            job.scheduled_at = Some(Utc::now() + chrono::Duration::seconds(60));
        }))
        .await
        .unwrap();
    // A job outside a restricted handle's priority range must not be dequeued by it.
    db.queue
        .enqueue_raw(with_config("low", |c| c.priority = -10))
        .await
        .unwrap();

    let restricted = db.another_queue(|b| b.priorities(0, 10)).await;
    let worker = Uuid::now_v7();
    let first = restricted.dequeue(2, worker).await.unwrap();
    assert_eq!(first.len(), 2);
    let rest = restricted.dequeue(10, worker).await.unwrap();
    assert_eq!(rest.len(), 1, "third in-range job");
    assert!(restricted.dequeue(10, worker).await.unwrap().is_empty());

    // The unrestricted handle still sees the low-priority job.
    let low = db.queue.dequeue(10, worker).await.unwrap();
    assert_eq!(low.len(), 1);
    assert_eq!(low[0].priority, -10);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_concurrent_dequeues_get_disjoint_jobs(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    for _ in 0..4 {
        db.queue.enqueue_raw(new_job("j", |_| {})).await.unwrap();
    }
    let other = db.another_queue(|b| b).await;
    let (a, b) = tokio::join!(
        db.queue.dequeue(2, Uuid::now_v7()),
        other.dequeue(2, Uuid::now_v7()),
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    assert_eq!(a.len() + b.len(), 4);
    for job_a in &a {
        assert!(
            b.iter().all(|job_b| job_b.id != job_a.id),
            "SKIP LOCKED overlap"
        );
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn test_concurrent_dequeues_make_disjoint_progress_at_scale(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    for _ in 0..64 {
        db.queue
            .enqueue_raw(new_job("parallel", |_| {}))
            .await
            .unwrap()
            .unwrap();
    }

    let mut dequeues = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let queue = db.queue.clone();
        dequeues.spawn(async move { queue.dequeue(16, Uuid::now_v7()).await });
    }
    let mut ids = std::collections::HashSet::new();
    while let Some(result) = dequeues.join_next().await {
        for job in result.unwrap().unwrap() {
            assert!(ids.insert(job.id), "two dequeues returned the same job");
        }
    }
    assert_eq!(ids.len(), 64);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_finish_complete_stores_result_and_expiry(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(with_config("j", |c| {
            c.retention = JobRetention::For(Duration::from_secs(60))
        }))
        .await
        .unwrap()
        .unwrap();
    let active = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);

    assert!(
        db.queue
            .finish(&active, JobStatus::Complete, Some(json!(42)), None)
            .await
            .unwrap()
    );
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Complete);
    assert_eq!(row.result, Some(json!(42)));
    assert!(row.completed_at.is_some());
    let expires = row.expires_at.expect("expiry from retention");
    assert!(expires > Utc::now() + chrono::Duration::seconds(50));

    // Double-finish is refused (already terminal).
    assert!(
        !db.queue
            .finish(&active, JobStatus::Failed, None, Some("late"))
            .await
            .unwrap()
    );
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Complete);

    assert_eq!(db.queue.stats().complete, 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_finish_rejects_nonterminal_statuses(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    db.queue.enqueue_raw(new_job("j", |_| {})).await.unwrap();
    let active = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);
    let error = db
        .queue
        .finish(&active, JobStatus::Running, None, None)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Config(_)), "{error}");
    assert_eq!(
        db.queue.fetch_job(active.id).await.unwrap().unwrap().status,
        JobStatus::Running
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_finish_retention_forever_and_delete_immediately(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let forever = db
        .queue
        .enqueue_raw(with_config("keep", |c| c.retention = JobRetention::Forever))
        .await
        .unwrap()
        .unwrap();
    let ephemeral = db
        .queue
        .enqueue_raw(with_config("gone", |c| {
            c.retention = JobRetention::DeleteImmediately
        }))
        .await
        .unwrap()
        .unwrap();
    let active = db.queue.dequeue(2, Uuid::now_v7()).await.unwrap();
    let forever_row = active.iter().find(|j| j.id == forever).unwrap();
    let ephemeral_row = active.iter().find(|j| j.id == ephemeral).unwrap();

    db.queue
        .finish(forever_row, JobStatus::Complete, None, None)
        .await
        .unwrap();
    let row = db.queue.fetch_job(forever).await.unwrap().unwrap();
    assert!(row.expires_at.is_none(), "forever rows never expire");

    db.queue
        .finish(ephemeral_row, JobStatus::Complete, None, None)
        .await
        .unwrap();
    assert!(
        db.queue.fetch_job(ephemeral).await.unwrap().is_none(),
        "deleted on finish"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_finish_failed_counts_and_stores_error(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(new_job("j", |_| {}))
        .await
        .unwrap()
        .unwrap();
    let active = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);
    db.queue
        .finish(&active, JobStatus::Failed, None, Some("failed: boom"))
        .await
        .unwrap();
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Failed);
    assert_eq!(row.error.as_deref(), Some("failed: boom"));
    assert_eq!(db.queue.stats().failed, 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_retry_requeues_with_delay(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    db.queue
        .enqueue_raw(with_config("j", |c| {
            c.max_attempts = 3;
            c.retry_delay = Duration::from_millis(30_000);
        }))
        .await
        .unwrap();
    let row = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);

    assert!(db.queue.retry(&row, "failed: transient").await.unwrap());
    let updated = db.queue.fetch_job(row.id).await.unwrap().unwrap();
    assert_eq!(updated.status, JobStatus::Queued);
    assert_eq!(updated.error.as_deref(), Some("failed: transient"));
    assert_eq!(updated.attempts, 1, "attempts preserved across retry");
    assert!(
        updated.scheduled_at > Utc::now() + chrono::Duration::seconds(20),
        "retry delay applied"
    );
    assert!(updated.started_at.is_none());
    assert_eq!(db.queue.stats().retried, 1);

    // Retrying a job that is no longer running is refused.
    assert!(!db.queue.retry(&row, "again").await.unwrap());
    // And it is not dequeueable before its delay elapses.
    assert!(
        db.queue
            .dequeue(1, Uuid::now_v7())
            .await
            .unwrap()
            .is_empty()
    );
}

/// A requeued attempt belongs to nobody, so the row must stop naming the worker
/// that gave it up: `JobRow::worker_id` is public API and the dashboard's job
/// detail renders it as the owner, so a `queued` row carrying a stale — often
/// already exited — worker misattributes the job to whoever looks at it.
#[sqlx::test(migrations = "./migrations")]
async fn test_retry_clears_the_worker_id_on_the_requeued_row(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(with_config("j", |config| config.max_attempts = 3))
        .await
        .unwrap()
        .unwrap();
    let worker_id = Uuid::now_v7();
    let consumer = crate::leased_consumer(&db.queue, worker_id).await;
    let attempt = consumer.dequeue(1).await.unwrap().remove(0);
    assert_eq!(attempt.job().worker_id, Some(worker_id));

    assert!(attempt.retry("failed: transient").await.unwrap());

    let updated = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(updated.status, JobStatus::Queued);
    assert_eq!(
        updated.worker_id, None,
        "a requeued job must not advertise the worker that gave it up"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_retry_refuses_when_attempts_are_exhausted(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(with_config("j", |config| config.max_attempts = 1))
        .await
        .unwrap()
        .unwrap();
    let mut row = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);
    assert!(!row.retryable());
    row.max_attempts += 1;
    assert!(row.retryable(), "the caller snapshot can be modified");

    assert!(!db.queue.retry(&row, "failed: permanent").await.unwrap());
    let updated = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(updated.status, JobStatus::Running);
    assert_eq!(updated.attempts, 1);
    assert!(updated.error.is_none());
    assert_eq!(db.queue.stats().retried, 0);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_abort_queued_job_finishes_immediately(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(new_job("j", |_| {}))
        .await
        .unwrap()
        .unwrap();
    assert!(db.queue.abort_job(id, "not needed").await.unwrap());
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Aborted);
    assert_eq!(row.error.as_deref(), Some("not needed"));
    assert!(row.completed_at.is_some());
    assert!(
        row.expires_at.is_some(),
        "retention applies to aborted rows"
    );
    assert_eq!(db.queue.stats().aborted, 1);

    // A terminal job can't be aborted again.
    assert!(!db.queue.abort_job(id, "again").await.unwrap());
}

#[sqlx::test(migrations = "./migrations")]
async fn test_abort_queued_delete_immediately_survives_until_sweep(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(with_config("j", |config| {
            config.retention = JobRetention::DeleteImmediately
        }))
        .await
        .unwrap()
        .unwrap();
    assert!(db.queue.abort_job(id, "not needed").await.unwrap());
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Aborted);
    assert!(row.expires_at.is_some());

    let mut sweeper = db.queue.sweeper();
    assert_eq!(sweeper.sweep().await.unwrap().purged_jobs, 1);
    assert!(db.queue.fetch_job(id).await.unwrap().is_none());
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_abort_running_job_goes_through_aborting(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(new_job("j", |_| {}))
        .await
        .unwrap()
        .unwrap();
    let active = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);
    backdate_job_liveness(&db, id).await;
    let touched_before_abort = db
        .queue
        .fetch_job(id)
        .await
        .unwrap()
        .unwrap()
        .touched_at
        .unwrap();

    assert!(db.queue.abort_job(id, "stop it").await.unwrap());
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Aborting, "worker must cancel it");
    assert_eq!(
        db.queue.counts().await.unwrap().running,
        1,
        "aborting work still occupies a worker"
    );
    assert!(
        row.touched_at.unwrap() > touched_before_abort,
        "abort updates the job's lifecycle timestamp"
    );

    assert!(
        !db.queue
            .finish(&active, JobStatus::Complete, Some(json!("too late")), None)
            .await
            .unwrap(),
        "a public finish must not overwrite a committed abort"
    );
    assert_eq!(
        db.queue.fetch_job(id).await.unwrap().unwrap().status,
        JobStatus::Aborting
    );

    // The worker's abort loop then finishes it.
    assert!(
        db.queue
            .finish(&active, JobStatus::Aborted, None, Some("stop it"))
            .await
            .unwrap()
    );
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Aborted);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_retry_job_grants_terminal_jobs_one_more_attempt(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(new_job("j", |_| {}))
        .await
        .unwrap()
        .unwrap();

    // Not retryable while queued.
    assert!(!db.queue.retry_job(id, "from ui").await.unwrap());

    let active = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);
    db.queue
        .finish(&active, JobStatus::Failed, None, Some("boom"))
        .await
        .unwrap();

    assert!(db.queue.retry_job(id, "from ui").await.unwrap());
    assert_eq!(db.queue.stats().retried, 1);
    assert!(
        !db.queue.retry_job(id, "duplicate click").await.unwrap(),
        "one terminal occurrence can only be retried once"
    );
    let original = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(original.status, JobStatus::Failed);
    assert_eq!(original.error.as_deref(), Some("boom"));

    // It is immediately dequeueable and can succeed this time.
    let jobs = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap();
    assert_eq!(jobs.len(), 1);
    assert_ne!(jobs[0].id, id);
    assert_eq!(
        jobs[0].max_attempts, jobs[0].attempts,
        "the dequeue consumed exactly one added attempt"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_concurrent_manual_retries_enqueue_exactly_one_occurrence(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(new_job("j", |_| {}))
        .await
        .unwrap()
        .unwrap();
    assert!(db.queue.abort_job(id, "make terminal").await.unwrap());

    let mut retries = tokio::task::JoinSet::new();
    for _ in 0..20 {
        let queue = db.queue.clone();
        retries.spawn(async move { queue.retry_job(id, "raced retry").await.unwrap() });
    }
    let mut accepted = 0;
    while let Some(result) = retries.join_next().await {
        accepted += usize::from(result.unwrap());
    }
    assert_eq!(accepted, 1);
    assert_eq!(db.queue.counts().await.unwrap().queued, 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_jobs_page_filters_and_paginates(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    for i in 0..3 {
        db.queue
            .enqueue_raw(new_job("alpha", |j| j.payload = json!(i)))
            .await
            .unwrap();
    }
    db.queue.enqueue_raw(new_job("beta", |_| {})).await.unwrap();
    db.queue.dequeue(1, Uuid::now_v7()).await.unwrap(); // one starts running

    let all = db.queue.jobs_page(JobFilter::default()).await.unwrap();
    assert_eq!(all.len(), 4);

    let queued = db
        .queue
        .jobs_page(JobFilter {
            status: Some(JobStatus::Queued),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(queued.len(), 3);

    let alphas = db
        .queue
        .jobs_page(JobFilter {
            name: Some("alpha".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(alphas.len(), 3);

    let page = db
        .queue
        .jobs_page(JobFilter {
            limit: Some(2),
            before: Some(JobCursor::from(&all[2])),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(page.len(), 1, "cursor past the first three");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_jobs_page_orders_reused_dedupe_keys_by_latest_enqueue(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let deduplicated = || {
        new_job("deduplicated", |job| {
            job.dedupe_key = Some("key".into());
        })
    };
    let first_id = db.queue.enqueue_raw(deduplicated()).await.unwrap().unwrap();
    let active = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);
    db.queue
        .finish(&active, JobStatus::Complete, None, None)
        .await
        .unwrap();

    let newer_id = db
        .queue
        .enqueue_raw(new_job("plain", |_| {}))
        .await
        .unwrap()
        .unwrap();
    sqlx::query(
        "UPDATE pgqueue.jobs SET enqueued_at = now() - interval '1 second' \
         WHERE id IN ($1, $2)",
    )
    .bind(first_id)
    .bind(newer_id)
    .execute(db.queue.pool())
    .await
    .unwrap();
    let latest_id = db.queue.enqueue_raw(deduplicated()).await.unwrap().unwrap();
    assert_ne!(latest_id, first_id);

    let jobs = db.queue.jobs_page(JobFilter::default()).await.unwrap();
    assert_eq!(
        jobs[0].id, latest_id,
        "new occurrence is the newest activity"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_counts_split_queued_running_scheduled(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    db.queue.enqueue_raw(new_job("now", |_| {})).await.unwrap();
    db.queue.enqueue_raw(new_job("now2", |_| {})).await.unwrap();
    db.queue
        .enqueue_raw(new_job("later", |job| {
            job.scheduled_at = Some(Utc::now() + chrono::Duration::seconds(60));
        }))
        .await
        .unwrap();
    db.queue.dequeue(1, Uuid::now_v7()).await.unwrap();

    let counts = db.queue.counts().await.unwrap();
    assert_eq!(
        (
            counts.queued,
            counts.running,
            counts.scheduled,
            counts.failed,
            counts.aborted,
        ),
        (1, 1, 1, 0, 0)
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_worker_info_appears_until_ttl_expires(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let worker = Uuid::now_v7();
    db.queue
        .write_worker_info(
            worker,
            json!({"complete": 3}),
            Some(json!({"host": "test"})),
            Duration::from_secs(60),
        )
        .await
        .unwrap();

    let info = db.queue.info().await.unwrap();
    assert_eq!(info.name, "default");
    assert_eq!(info.workers.len(), 1);
    assert_eq!(info.workers[0].id, worker);
    assert_eq!(info.workers[0].stats, json!({"complete": 3}));
    assert_eq!(info.workers[0].metadata, Some(json!({"host": "test"})));

    // Re-upsert with zero TTL: immediately expired, hence invisible.
    db.queue
        .write_worker_info(worker, json!({}), None, Duration::ZERO)
        .await
        .unwrap();
    let info = db.queue.info().await.unwrap();
    assert!(info.workers.is_empty());
}

/// The `INSERT` half of the lease upsert has to honour the caller's intake
/// state. A worker whose row the sweeper purged — because it stalled past its
/// TTL — recreates the lease on its next heartbeat, and letting the column
/// default to `accepting` republished a shutting-down worker as live and taking
/// work. A live lease is what suppresses sweeper recovery, so that also stalls
/// recovery of the worker's own abandoned attempts for a whole fresh TTL.
#[sqlx::test(migrations = "./migrations")]
async fn test_worker_lease_is_created_closed_when_the_writer_stopped_taking_work(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let workers = db.queue.pool().clone();
    let accepting = |id: Uuid| {
        let workers = workers.clone();
        async move {
            sqlx::query_scalar::<_, bool>("SELECT accepting FROM pgqueue.workers WHERE id = $1")
                .bind(id)
                .fetch_optional(&workers)
                .await
                .unwrap()
        }
    };
    db.queue
        .enqueue_raw(new_job("drainable", |_| {}))
        .await
        .unwrap();

    // No row yet, so this takes the INSERT branch.
    let closed = Uuid::now_v7();
    db.queue
        .write_worker_lease(closed, json!({}), None, Duration::from_secs(60), false)
        .await
        .unwrap();
    assert_eq!(accepting(closed).await, Some(false));

    // And a worker that is still taking work creates an accepting lease.
    let open = Uuid::now_v7();
    db.queue
        .write_worker_lease(open, json!({}), None, Duration::from_secs(60), true)
        .await
        .unwrap();
    assert_eq!(accepting(open).await, Some(true));

    // A closed lease is live, which is what keeps the sweeper off the attempts
    // the worker is still draining, but it must not be advertised as a worker
    // that will take more work.
    let info = db.queue.info().await.unwrap();
    assert!(info.workers.iter().any(|worker| worker.id == closed));
    assert!(
        db.queue
            .consumer(closed)
            .dequeue(1)
            .await
            .unwrap()
            .is_empty(),
        "a closed lease must not satisfy the intake check"
    );

    // On the UPDATE branch a worker heartbeat leaves the flag alone in both
    // directions: it neither reopens what shutdown closed nor closes a lease a
    // consumer heartbeat reopened.
    db.queue
        .write_worker_lease(closed, json!({}), None, Duration::from_secs(60), true)
        .await
        .unwrap();
    assert_eq!(accepting(closed).await, Some(false));
    db.queue
        .write_worker_lease(open, json!({}), None, Duration::from_secs(60), false)
        .await
        .unwrap();
    assert_eq!(accepting(open).await, Some(true));

    // Only a consumer heartbeat reopens.
    db.queue
        .write_worker_info(closed, json!({}), None, Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(accepting(closed).await, Some(true));
}

#[sqlx::test(migrations = "./migrations")]
async fn test_sweep_purges_expired_jobs_and_workers(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(with_config("old", |c| {
            c.retention = JobRetention::For(Duration::from_millis(1))
        }))
        .await
        .unwrap()
        .unwrap();
    let active = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);
    db.queue
        .finish(&active, JobStatus::Complete, None, None)
        .await
        .unwrap();
    let worker_id = Uuid::now_v7();
    db.queue
        .write_worker_info(worker_id, json!({}), None, Duration::from_millis(1))
        .await
        .unwrap();
    sqlx::query("UPDATE pgqueue.jobs SET expires_at = now() - interval '1 second' WHERE id = $1")
        .bind(id)
        .execute(db.queue.pool())
        .await
        .unwrap();
    crate::expire_worker(&db, worker_id).await;
    sqlx::query(
        "INSERT INTO pgqueue.cron_occurrences (queue, dedupe_key, scheduled_at, expires_at)
         VALUES ($1, 'expired-claim', now() - interval '2 seconds', now() - interval '1 second')",
    )
    .bind(db.queue.name())
    .execute(db.queue.pool())
    .await
    .unwrap();

    let mut sweeper = db.queue.sweeper();
    let report = sweeper.sweep().await.unwrap();
    assert!(report.leader);
    assert_eq!(report.purged_jobs, 1);
    assert!(report.swept.is_empty());
    assert!(
        db.queue.fetch_job(id).await.unwrap().is_none(),
        "expired row purged"
    );
    assert!(db.queue.info().await.unwrap().workers.is_empty());
    let claim_exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (
             SELECT 1 FROM pgqueue.cron_occurrences
             WHERE queue = $1 AND dedupe_key = 'expired-claim'
         )",
    )
    .bind(db.queue.name())
    .fetch_one(db.queue.pool())
    .await
    .unwrap();
    assert!(
        !claim_exists,
        "expired cron occurrence claim was not purged"
    );
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_sweep_purges_only_expired_rows_for_its_queue(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let other = db.another_queue(|builder| builder.name("other")).await;
    let first_job = sqlx::query_scalar::<_, uuid::Uuid>(
        r#"
        INSERT INTO pgqueue.jobs (
            queue, name, payload, status, completed_at, expires_at
        ) VALUES ($1, 'expired-first', 'null', 'complete', now(),
                  now() - interval '1 second')
        RETURNING id
        "#,
    )
    .bind(db.queue.name())
    .fetch_one(&pool)
    .await
    .unwrap();
    let other_job = sqlx::query_scalar::<_, uuid::Uuid>(
        r#"
        INSERT INTO pgqueue.jobs (
            queue, name, payload, status, completed_at, expires_at
        ) VALUES ($1, 'expired-other', 'null', 'complete', now(),
                  now() - interval '1 second')
        RETURNING id
        "#,
    )
    .bind(other.name())
    .fetch_one(&pool)
    .await
    .unwrap();
    let first_worker = Uuid::now_v7();
    let other_worker = Uuid::now_v7();
    db.queue
        .write_worker_info(first_worker, json!({}), None, Duration::from_secs(1))
        .await
        .unwrap();
    other
        .write_worker_info(other_worker, json!({}), None, Duration::from_secs(1))
        .await
        .unwrap();
    sqlx::query(
        "UPDATE pgqueue.workers SET expires_at = now() - interval '1 second' WHERE id = ANY($1)",
    )
    .bind([first_worker, other_worker])
    .execute(&pool)
    .await
    .unwrap();

    let mut first_sweeper = db.queue.sweeper();
    let report = first_sweeper.sweep().await.unwrap();
    assert!(report.leader);
    assert_eq!(report.purged_jobs, 1);
    assert!(
        !sqlx::query_scalar::<_, bool>(
            r#"SELECT EXISTS (SELECT 1 FROM pgqueue.jobs WHERE id = $1)"#
        )
        .bind(first_job)
        .fetch_one(&pool)
        .await
        .unwrap()
    );
    assert!(
        sqlx::query_scalar::<_, bool>(
            r#"SELECT EXISTS (SELECT 1 FROM pgqueue.jobs WHERE id = $1)"#
        )
        .bind(other_job)
        .fetch_one(&pool)
        .await
        .unwrap()
    );
    assert!(
        !sqlx::query_scalar::<_, bool>(
            r#"SELECT EXISTS (SELECT 1 FROM pgqueue.workers WHERE id = $1)"#
        )
        .bind(first_worker)
        .fetch_one(&pool)
        .await
        .unwrap()
    );
    assert!(
        sqlx::query_scalar::<_, bool>(
            r#"SELECT EXISTS (SELECT 1 FROM pgqueue.workers WHERE id = $1)"#
        )
        .bind(other_worker)
        .fetch_one(&pool)
        .await
        .unwrap()
    );
    first_sweeper.release().await;

    let mut other_sweeper = other.sweeper();
    assert_eq!(other_sweeper.sweep().await.unwrap().purged_jobs, 1);
    assert!(
        !sqlx::query_scalar::<_, bool>(
            r#"SELECT EXISTS (SELECT 1 FROM pgqueue.jobs WHERE id = $1)"#
        )
        .bind(other_job)
        .fetch_one(&pool)
        .await
        .unwrap()
    );
    assert!(
        !sqlx::query_scalar::<_, bool>(
            r#"SELECT EXISTS (SELECT 1 FROM pgqueue.workers WHERE id = $1)"#
        )
        .bind(other_worker)
        .fetch_one(&pool)
        .await
        .unwrap()
    );
    other_sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_sweep_bounds_each_purge_batch_and_reports_more_work(pool: PgPool) {
    let db = TestDb::with(pool.clone(), |builder| builder.sweep_batch_size(2)).await;
    sqlx::query(
        r#"
        INSERT INTO pgqueue.jobs (
            queue, name, payload, status, completed_at, expires_at
        )
        SELECT $1, 'expired-batch', 'null'::jsonb, 'complete', now(),
               now() - interval '1 second'
        FROM generate_series(1, 5)
        "#,
    )
    .bind(db.queue.name())
    .execute(&pool)
    .await
    .unwrap();

    let mut sweeper = db.queue.sweeper();
    let first = sweeper.sweep().await.unwrap();
    assert_eq!(first.purged_jobs, 2);
    assert!(first.more_work());
    let second = sweeper.sweep().await.unwrap();
    assert_eq!(second.purged_jobs, 2);
    assert!(second.more_work());
    let third = sweeper.sweep().await.unwrap();
    assert_eq!(third.purged_jobs, 1);
    assert!(!third.more_work());
    sweeper.release().await;

    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"SELECT count(*) FROM pgqueue.jobs WHERE queue = $1 AND name = 'expired-batch'"#
        )
        .bind(db.queue.name())
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_sweep_marks_every_stuck_running_job_in_one_pass(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let mut ids = Vec::new();
    for name in ["stuck-a", "stuck-b", "stuck-c"] {
        let id = db
            .queue
            .enqueue_raw(with_config(name, |c| {
                c.timeout = Some(Duration::from_millis(20));
            }))
            .await
            .unwrap()
            .unwrap();
        ids.push(id);
    }
    db.queue.dequeue(3, Uuid::now_v7()).await.unwrap();
    sqlx::query("UPDATE pgqueue.jobs SET started_at = now() - interval '100 milliseconds'")
        .execute(db.queue.pool())
        .await
        .unwrap();

    let mut sweeper = db.queue.sweeper();
    let report = sweeper.sweep().await.unwrap();
    let mut cancelling = report.cancelling.clone();
    cancelling.sort();
    ids.sort();
    assert_eq!(cancelling, ids);
    for id in ids {
        let row = db.queue.fetch_job(id).await.unwrap().unwrap();
        assert_eq!(row.status, JobStatus::Aborting);
        assert_eq!(row.error.as_deref(), Some("swept"));
    }
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_sweep_retries_stuck_retryable_jobs(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(with_config("stuck", |c| {
            c.max_attempts = 3;
            c.timeout = Some(Duration::from_millis(20));
        }))
        .await
        .unwrap()
        .unwrap();
    db.queue.dequeue(1, Uuid::now_v7()).await.unwrap();

    sqlx::query(
        "UPDATE pgqueue.jobs SET started_at = now() - interval '100 milliseconds' \
         WHERE id = $1",
    )
    .bind(id)
    .execute(db.queue.pool())
    .await
    .unwrap();

    let mut sweeper = db.queue.sweeper();
    // Phase 1: the stuck job is asked to abort (its worker may still be
    // running it), never yanked straight back to 'queued'.
    let report = sweeper.sweep().await.unwrap();
    assert_eq!(report.cancelling, vec![id]);
    assert!(report.swept.is_empty());
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Aborting);
    assert_eq!(row.error.as_deref(), Some("swept"));

    // Phase 2 (next sweep, nobody reacted): requeued for retry.
    let report = sweeper.sweep().await.unwrap();
    assert_eq!(report.swept, vec![id]);
    assert_eq!(db.queue.stats().retried, 1);
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(
        row.status,
        JobStatus::Queued,
        "retryable stuck job requeued"
    );
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_public_finish_succeeds_through_the_sweeper_grace_window(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(with_config("slow", |c| {
            c.max_attempts = 3;
            c.timeout = Some(Duration::from_millis(20));
        }))
        .await
        .unwrap()
        .unwrap();
    let active = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);
    backdate_job_liveness(&db, id).await;

    let mut sweeper = db.queue.sweeper();
    assert_eq!(sweeper.sweep().await.unwrap().cancelling, vec![id]);
    assert_eq!(
        db.queue.fetch_job(id).await.unwrap().unwrap().status,
        JobStatus::Aborting
    );

    // The low-level consumer was slow, not dead: its successful result must
    // land through the same grace window worker-processed jobs get, instead
    // of being discarded and the job running twice.
    assert!(
        db.queue
            .finish(&active, JobStatus::Complete, Some(json!("done")), None)
            .await
            .unwrap(),
        "a swept-but-alive attempt finishes through the grace window"
    );
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Complete);
    assert_eq!(row.result, Some(json!("done")));
    assert!(
        sweeper.sweep().await.unwrap().swept.is_empty(),
        "nothing left to recover"
    );
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_sweep_waits_for_the_live_owner_before_requeueing(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(with_config("owned-stuck", |config| {
            config.max_attempts = 2;
            config.timeout = Some(Duration::from_millis(20));
        }))
        .await
        .unwrap()
        .unwrap();
    let worker = Uuid::now_v7();
    db.queue.dequeue(1, worker).await.unwrap();
    db.queue
        .write_worker_info(worker, json!({}), None, Duration::from_secs(30))
        .await
        .unwrap();
    backdate_job_liveness(&db, id).await;

    let mut sweeper = db.queue.sweeper();
    assert_eq!(sweeper.sweep().await.unwrap().cancelling, vec![id]);
    let report = sweeper.sweep().await.unwrap();
    assert!(report.swept.is_empty(), "the owner lease is still live");
    assert_eq!(
        db.queue.fetch_job(id).await.unwrap().unwrap().status,
        JobStatus::Aborting
    );

    db.queue
        .write_worker_info(worker, json!({}), None, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(sweeper.sweep().await.unwrap().swept, vec![id]);
    assert_eq!(
        db.queue.fetch_job(id).await.unwrap().unwrap().status,
        JobStatus::Queued
    );
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_sweep_keeps_a_keyed_stuck_job_owned_until_the_worker_lease_expires(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(new_job("stuck", |job| {
            job.dedupe_key = Some("stuck:once".into());
            job.config.max_attempts = 1;
            job.config.timeout = Some(Duration::from_millis(20));
        }))
        .await
        .unwrap()
        .unwrap();
    let worker = Uuid::now_v7();
    db.queue.dequeue(1, worker).await.unwrap();
    db.queue
        .write_worker_info(worker, json!({}), None, Duration::from_secs(30))
        .await
        .unwrap();
    backdate_job_liveness(&db, id).await;

    let mut sweeper = db.queue.sweeper();
    let report = sweeper.sweep().await.unwrap();
    assert_eq!(report.cancelling, vec![id]);
    let report = sweeper.sweep().await.unwrap();
    assert!(report.swept.is_empty(), "the owner lease is still live");
    assert_eq!(
        db.queue.fetch_job(id).await.unwrap().unwrap().status,
        JobStatus::Aborting
    );

    db.queue
        .write_worker_info(worker, json!({}), None, Duration::ZERO)
        .await
        .unwrap();
    let report = sweeper.sweep().await.unwrap();
    assert_eq!(report.swept, vec![id]);
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Aborted);
    assert_eq!(row.error.as_deref(), Some("swept"));
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_sweep_batch_skips_live_dedupe_blocker_and_recovers_unkeyed_job(pool: PgPool) {
    let db = TestDb::with(pool.clone(), |builder| builder.sweep_batch_size(1)).await;
    let exclusive = db
        .queue
        .enqueue_raw(new_job("exclusive", |job| {
            job.dedupe_key = Some("singleton".into());
            job.config.max_attempts = 1;
            job.config.timeout = Some(Duration::from_millis(20));
        }))
        .await
        .unwrap()
        .unwrap();
    let unkeyed = db
        .queue
        .enqueue_raw(with_config("unkeyed", |config| {
            config.max_attempts = 1;
            config.timeout = Some(Duration::from_millis(20));
        }))
        .await
        .unwrap()
        .unwrap();
    let worker = Uuid::now_v7();
    assert_eq!(db.queue.dequeue(2, worker).await.unwrap().len(), 2);
    db.queue
        .write_worker_info(worker, json!({}), None, Duration::from_secs(30))
        .await
        .unwrap();
    sqlx::query(
        "UPDATE pgqueue.jobs
         SET started_at = now()
             - CASE WHEN id = $1 THEN interval '2 seconds' ELSE interval '1 second' END,
             touched_at = now()
             - CASE WHEN id = $1 THEN interval '2 seconds' ELSE interval '1 second' END
         WHERE id IN ($1, $2)",
    )
    .bind(exclusive)
    .bind(unkeyed)
    .execute(db.queue.pool())
    .await
    .unwrap();

    let mut sweeper = db.queue.sweeper();
    assert_eq!(sweeper.sweep().await.unwrap().cancelling, vec![exclusive]);
    assert_eq!(sweeper.sweep().await.unwrap().cancelling, vec![unkeyed]);
    assert_eq!(sweeper.sweep().await.unwrap().swept, vec![unkeyed]);
    assert_eq!(
        db.queue.fetch_job(exclusive).await.unwrap().unwrap().status,
        JobStatus::Aborting
    );

    db.queue
        .write_worker_info(worker, json!({}), None, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(sweeper.sweep().await.unwrap().swept, vec![exclusive]);
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_user_abort_reason_cannot_forge_the_sweeper_marker(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(with_config("ab", |c| {
            c.max_attempts = 5; // retryable, but aborting jobs must not be retried
            c.timeout = Some(Duration::from_millis(20));
        }))
        .await
        .unwrap()
        .unwrap();
    db.queue.dequeue(1, Uuid::now_v7()).await.unwrap();
    db.queue.abort_job(id, "swept").await.unwrap();
    backdate_job_liveness(&db, id).await;

    let mut sweeper = db.queue.sweeper();
    let report = sweeper.sweep().await.unwrap();
    assert_eq!(report.swept, vec![id]);
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Aborted);
    assert_eq!(row.error.as_deref(), Some("swept"));
    sweeper.release().await;
}

/// The mirror image of the forgery test above: an abort requested while the
/// row sits in the sweeper-marked `aborting` window claims the row —
/// converting the sweeper's pending retry into a user abort — instead of
/// being dropped for that retry to run the job again.
#[sqlx::test(migrations = "./migrations")]
async fn test_user_abort_claims_a_sweeper_marked_aborting_job(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(with_config("stuck", |c| {
            c.max_attempts = 3;
            c.timeout = Some(Duration::from_millis(20));
        }))
        .await
        .unwrap()
        .unwrap();
    let active = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);
    backdate_job_liveness(&db, id).await;

    let mut sweeper = db.queue.sweeper();
    // Phase 1: the sweeper marks the stuck row 'aborting' for a retry.
    assert_eq!(sweeper.sweep().await.unwrap().cancelling, vec![id]);

    assert!(
        db.queue.abort_job(id, "operator said stop").await.unwrap(),
        "the abort claims the sweeper-marked row instead of being dropped"
    );
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Aborting);
    assert_eq!(row.error.as_deref(), Some("operator said stop"));
    assert!(row.result.is_none(), "the sweeper's marker is cleared");

    // A success landing after the claim must not overwrite the abort...
    assert!(
        !db.queue
            .finish(&active, JobStatus::Complete, Some(json!("too late")), None)
            .await
            .unwrap()
    );
    // ...and the next sweep finishes the abort instead of requeueing the job.
    assert_eq!(sweeper.sweep().await.unwrap().swept, vec![id]);
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Aborted);
    assert_eq!(row.error.as_deref(), Some("operator said stop"));
    assert_eq!(db.queue.stats().retried, 0, "the sweeper retry never ran");
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_sweep_leadership_is_exclusive_per_queue(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let other = db.another_queue(|b| b).await;

    let mut leader = db.queue.sweeper();
    let report = leader.sweep().await.unwrap();
    assert!(report.leader);
    assert!(leader.is_leader());

    let mut follower = other.sweeper();
    let report = follower.sweep().await.unwrap();
    assert!(!report.leader, "second sweeper must not get the lock");
    assert!(!follower.is_leader());

    // Leadership hands over once released.
    leader.release().await;
    assert!(!leader.is_leader());
    let report = follower.sweep().await.unwrap();
    assert!(report.leader);
    follower.release().await;
}

//noinspection SqlNoDataSourceInspection
#[sqlx::test(migrations = "./migrations")]
async fn test_sweep_leadership_recovers_after_its_backend_is_terminated(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let other = db.another_queue(|builder| builder).await;
    let mut stale = db.queue.sweeper();
    assert!(stale.sweep().await.unwrap().leader);

    let key = pgqueue::__test_support::sweep_lock_key(&db.database, db.queue.name()) as u64;
    let class_id = (key >> 32) as u32 as i64;
    let object_id = key as u32 as i64;
    let pid = sqlx::query_scalar::<_, i32>(
        "SELECT pid FROM pg_locks
         WHERE locktype = 'advisory' AND classid::bigint = $1 AND objid::bigint = $2
           AND objsubid = 1 AND granted",
    )
    .bind(class_id)
    .bind(object_id)
    .fetch_one(db.queue.pool())
    .await
    .unwrap();
    assert!(
        sqlx::query_scalar::<_, bool>("SELECT pg_terminate_backend($1)")
            .bind(pid)
            .fetch_one(db.queue.pool())
            .await
            .unwrap()
    );

    let mut replacement = other.sweeper();
    for _ in 0..20 {
        if replacement.sweep().await.unwrap().leader {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        replacement.is_leader(),
        "replacement never acquired leadership"
    );

    let report = stale.sweep().await.unwrap();
    assert!(
        !report.leader,
        "stale session must revalidate before sweeping"
    );
    assert!(!stale.is_leader());
    replacement.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_distinct_queue_names_do_not_share_sweep_leadership(pool: PgPool) {
    let db = TestDb::with(pool.clone(), |builder| builder.name("133665")).await;
    let other = db.another_queue(|builder| builder.name("27472")).await;
    let mut first = db.queue.sweeper();
    let mut second = other.sweeper();
    assert!(first.sweep().await.unwrap().leader);
    assert!(second.sweep().await.unwrap().leader);
    first.release().await;
    second.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_queues_are_isolated_by_name(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let other = db.another_queue(|b| b.name("other")).await;

    let id = db
        .queue
        .enqueue_raw(new_job("j", |_| {}))
        .await
        .unwrap()
        .unwrap();
    assert!(other.dequeue(10, Uuid::now_v7()).await.unwrap().is_empty());
    assert_eq!(other.counts().await.unwrap().queued, 0);
    assert_eq!(db.queue.counts().await.unwrap().queued, 1);

    // UUIDs are not authorization: every id-based operation must remain
    // scoped to the queue handle that owns the row.
    assert!(other.fetch_job(id).await.unwrap().is_none());
    assert!(!other.abort_job(id, "cross-queue").await.unwrap());
    assert!(!other.retry_job(id, "cross-queue").await.unwrap());

    let active = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);
    assert!(matches!(
        other.retry(&active, "cross-queue").await,
        Err(Error::Config(_))
    ));
    assert!(matches!(
        other
            .finish(&active, JobStatus::Complete, Some(json!(null)), None)
            .await,
        Err(Error::Config(_))
    ));
    assert_eq!(
        db.queue.fetch_job(id).await.unwrap().unwrap().status,
        JobStatus::Running
    );

    let worker_id = Uuid::now_v7();
    db.queue
        .write_worker_info(
            worker_id,
            json!({"owner": "default"}),
            None,
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    let err = other
        .write_worker_info(
            worker_id,
            json!({"owner": "other"}),
            None,
            Duration::from_secs(5),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Config(_)), "{err}");
    assert!(other.info().await.unwrap().workers.is_empty());
}

#[sqlx::test(migrations = "./migrations")]
async fn test_builder_accepts_external_pool(pool: PgPool) {
    crate::init_tracing();
    let queue = Queue::builder("ignored-when-pool-is-set")
        .pool(pool.clone())
        .connections(1, 2) // ignored, but exercised
        .connect()
        .await
        .unwrap();
    queue.enqueue_raw(new_job("j", |_| {})).await.unwrap();
    assert_eq!(queue.counts().await.unwrap().queued, 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_dropping_a_sweeper_releases_leadership(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let other = db.another_queue(|b| b).await;

    let mut leader = db.queue.sweeper();
    assert!(leader.sweep().await.unwrap().leader);
    drop(leader); // no explicit release: Drop must close the lock connection

    // Mutex-wrapped so the polling closure can re-borrow the sweeper mutably.
    let follower = tokio::sync::Mutex::new(other.sweeper());
    wait_until(
        Duration::from_secs(5),
        Duration::from_millis(50),
        "leadership never released",
        || async { follower.lock().await.sweep().await.unwrap().leader },
    )
    .await;
    follower.into_inner().release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_queue_accessors_and_debug_reflect_configuration(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    assert_eq!(db.queue.name(), "default");
    let debug = format!("{:?}", db.queue);
    assert!(debug.contains("Queue"));
    assert!(debug.contains("default"));
}

#[sqlx::test(migrations = "./migrations")]
async fn test_stale_attempts_cannot_finalize_newer_ones(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    db.queue
        .enqueue_raw(with_config("dup", |c| {
            c.max_attempts = 3;
            c.timeout = Some(Duration::from_millis(20));
        }))
        .await
        .unwrap()
        .unwrap();
    // Worker A dequeues attempt 1, then goes silent past its timeout.
    let attempt_a = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);
    backdate_job_liveness(&db, attempt_a.id).await;

    // The sweeper recovers the job (two passes) and worker B picks it up.
    let mut sweeper = db.queue.sweeper();
    sweeper.sweep().await.unwrap();
    sweeper.sweep().await.unwrap();
    sweeper.release().await;
    let attempt_b = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);
    assert_eq!(attempt_b.id, attempt_a.id);
    assert_eq!(attempt_b.attempts, 2);

    // Worker A wakes up: its stale attempt must not mutate the row.
    assert!(
        !db.queue
            .finish(&attempt_a, JobStatus::Complete, Some(json!("stale")), None)
            .await
            .unwrap(),
        "stale finish must be refused"
    );
    assert!(
        !db.queue.retry(&attempt_a, "stale").await.unwrap(),
        "stale retry must be refused"
    );
    let row = db.queue.fetch_job(attempt_a.id).await.unwrap().unwrap();
    assert_eq!(
        row.status,
        JobStatus::Running,
        "attempt 2 still owns the row"
    );
    assert_eq!(row.attempts, 2);
    assert!(row.result.is_none());

    // The current attempt finalizes normally.
    assert!(
        db.queue
            .finish(&attempt_b, JobStatus::Complete, Some(json!("fresh")), None)
            .await
            .unwrap()
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_failed_attempt_of_an_aborting_job_honors_the_abort(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(with_config("ab", |c| c.max_attempts = 5))
        .await
        .unwrap()
        .unwrap();
    let active = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);
    assert!(db.queue.abort_job(id, "user said stop").await.unwrap());

    // The attempt fails while the abort is pending: retry must refuse...
    assert!(!db.queue.retry(&active, "failed: transient").await.unwrap());
    // ...and finishing as aborted (error: None) preserves the abort reason.
    assert!(
        db.queue
            .finish(&active, JobStatus::Aborted, None, None)
            .await
            .unwrap()
    );
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Aborted);
    assert_eq!(row.error.as_deref(), Some("user said stop"));
}

#[sqlx::test(migrations = "./migrations")]
async fn test_dead_workers_unbounded_jobs_are_recovered(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(with_config("unbounded", |c| {
            c.max_attempts = 2;
            c.timeout = None; // no timeout, no heartbeat: no self-deadline
        }))
        .await
        .unwrap()
        .unwrap();
    // Dequeued by a worker that never heartbeats (crashed instantly).
    db.queue.dequeue(1, Uuid::now_v7()).await.unwrap();

    let mut sweeper = db.queue.sweeper();
    let report = sweeper.sweep().await.unwrap();
    assert_eq!(
        report.cancelling,
        vec![id],
        "dead-worker job detected as stuck"
    );
    let report = sweeper.sweep().await.unwrap();
    assert_eq!(report.swept, vec![id]);
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Queued, "recovered for retry");
    sweeper.release().await;
}

//noinspection SqlNoDataSourceInspection
#[sqlx::test(migrations = "./migrations")]
async fn test_leaseless_unbounded_jobs_are_recovered_after_the_sweep_grace(pool: PgPool) {
    // A nonzero grace (unlike the harness default) makes the no-deadline
    // arm's liveness window observable.
    let db = TestDb::with(pool.clone(), |builder| {
        builder.sweep_grace(Duration::from_secs(30))
    })
    .await;
    let id = db
        .queue
        .enqueue_raw(with_config("unbounded", |c| {
            c.max_attempts = 2;
            c.timeout = None;
        }))
        .await
        .unwrap()
        .unwrap();
    // Public dequeue: this consumer never writes a workers row.
    db.queue.dequeue(1, Uuid::now_v7()).await.unwrap();

    let backdate_lifecycle = |milliseconds: i64| {
        let pool = db.queue.pool().clone();
        async move {
            sqlx::query(
                "UPDATE pgqueue.jobs
                 SET touched_at = now() - $2::bigint * interval '1 millisecond'
                 WHERE id = $1",
            )
            .bind(id)
            .bind(milliseconds)
            .execute(&pool)
            .await
            .unwrap();
        }
    };

    let mut sweeper = db.queue.sweeper();
    let report = sweeper.sweep().await.unwrap();
    assert!(
        report.cancelling.is_empty(),
        "a freshly dequeued no-deadline job is inside the liveness grace"
    );

    // Past the grace with no worker lease, the attempt is recovered.
    backdate_lifecycle(60_000).await;
    assert_eq!(sweeper.sweep().await.unwrap().cancelling, vec![id]);
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_another_queues_worker_cannot_keep_an_unbounded_job_alive(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let other = db.another_queue(|builder| builder.name("other")).await;
    db.queue
        .enqueue_raw(with_config("unbounded", |config| {
            config.max_attempts = 2;
            config.timeout = None;
        }))
        .await
        .unwrap();
    let worker_id = Uuid::now_v7();
    let active = db.queue.dequeue(1, worker_id).await.unwrap().remove(0);
    other
        .write_worker_info(worker_id, json!({}), None, Duration::from_secs(30))
        .await
        .unwrap();

    let mut sweeper = db.queue.sweeper();
    let report = sweeper.sweep().await.unwrap();
    assert_eq!(report.cancelling, vec![active.id]);
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_live_workers_unbounded_jobs_are_not_swept(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    db.queue
        .enqueue_raw(with_config("unbounded", |c| c.timeout = None))
        .await
        .unwrap()
        .unwrap();
    let worker_id = Uuid::now_v7();
    db.queue.dequeue(1, worker_id).await.unwrap();
    // The worker has a live heartbeat row: its job is not stuck.
    db.queue
        .write_worker_info(worker_id, json!({}), None, Duration::from_secs(60))
        .await
        .unwrap();

    let mut sweeper = db.queue.sweeper();
    let report = sweeper.sweep().await.unwrap();
    assert!(report.cancelling.is_empty());
    assert!(report.swept.is_empty());
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_zero_delay_retries_keep_their_queue_position(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(with_config("j", |c| {
            c.max_attempts = 3;
            c.retry_delay = Duration::ZERO;
        }))
        .await
        .unwrap()
        .unwrap();
    let original = db.queue.fetch_job(id).await.unwrap().unwrap().scheduled_at;

    let active = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);
    assert!(db.queue.retry(&active, "failed: transient").await.unwrap());
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Queued);
    assert_eq!(
        row.scheduled_at, original,
        "zero-delay retry must not lose its place behind the backlog"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_consumer_dequeue_claims_nothing_without_a_live_lease(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    db.queue
        .enqueue_raw(new_job("leaseless-consumer", |_| {}))
        .await
        .unwrap()
        .unwrap();

    // No heartbeat: without a lease the sweeper would treat any claim as
    // abandoned and hand the job to someone else mid-flight, so refuse it.
    let attempts = db.queue.consumer(Uuid::now_v7()).dequeue(10).await.unwrap();
    assert!(attempts.is_empty());
    assert_eq!(db.queue.counts().await.unwrap().queued, 1);

    // With the documented heartbeat first, the same call claims the job.
    let attempts = crate::leased_consumer(&db.queue, Uuid::now_v7())
        .await
        .dequeue(10)
        .await
        .unwrap();
    assert_eq!(attempts.len(), 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_sweep_recovers_abandoned_jobs_in_one_statement_per_batch(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    // Statement-level triggers fire once per statement, so this counts round
    // trips rather than rows.
    sqlx::raw_sql(sqlx::AssertSqlSafe(
        "CREATE TABLE pgqueue.stmt_counter (n bigint NOT NULL);
         INSERT INTO pgqueue.stmt_counter VALUES (0);
         CREATE FUNCTION pgqueue.bump_stmt_counter() RETURNS trigger
         LANGUAGE plpgsql AS $$
         BEGIN
             UPDATE pgqueue.stmt_counter SET n = n + 1;
             RETURN NULL;
         END
         $$;
         CREATE TRIGGER count_job_updates
         AFTER UPDATE ON pgqueue.jobs
         FOR EACH STATEMENT
         EXECUTE FUNCTION pgqueue.bump_stmt_counter();"
            .to_string(),
    ))
    .execute(&pool)
    .await
    .unwrap();

    // Half retryable (requeued), half exhausted (aborted): both groups must be
    // recovered in one statement each, not one per row.
    sqlx::query(
        "INSERT INTO pgqueue.jobs
             (queue, name, status, kind, attempts, max_attempts, error, result,
              started_at, touched_at, worker_id)
         SELECT 'default', 'swept', 'aborting', 'job', 1,
                CASE WHEN g % 2 = 0 THEN 5 ELSE 1 END,
                'swept', '\"pgqueue:swept\"',
                now() - interval '1 hour', now() - interval '1 hour', gen_random_uuid()
         FROM generate_series(1, 40) AS g",
    )
    .execute(&pool)
    .await
    .unwrap();

    let mut sweeper = db.queue.sweeper();
    let report = sweeper.sweep().await.unwrap();
    sweeper.release().await;
    assert_eq!(report.swept.len(), 40);

    let statements = sqlx::query_scalar::<_, i64>("SELECT n FROM pgqueue.stmt_counter")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(
        statements <= 2,
        "40 abandoned jobs should cost one requeue statement and one abort statement, not {statements}"
    );
}

// `finish_with_guards` is the only statement opening with a bare `candidate`
// CTE; the sweeper's near-identical batch abort opens with `WITH requested AS`.

/// The underfilled-batch probe reports availability in terms of the handler
/// names the worker registered, so it is worthless to a consumer that has none
/// — and paying for it would double the statements an idle consumer issues and
/// turn a failure of that diagnostic query into a failed dequeue.
#[sqlx::test(migrations = "./migrations")]
async fn test_consumer_dequeue_does_not_run_the_worker_only_probe(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let Some(mut stats) = Stats::new(&db.database).await else {
        return;
    };
    let consumer = leased_consumer(&db.queue, Uuid::now_v7()).await;

    let claims = stats.since_now(DEQUEUE_CLAIM).await;
    let probes = stats.since_now(DEQUEUE_PROBE).await;
    for _ in 0..5 {
        assert!(consumer.dequeue(4).await.unwrap().is_empty());
    }

    assert_eq!(
        stats.delta(&claims).await,
        5,
        "one claim statement per dequeue"
    );
    assert_eq!(
        stats.delta(&probes).await,
        0,
        "the consumer path must not pay for the worker's availability probe"
    );
}

/// `JobRequest::validate` NUL-checked only `name` and `dedupe_key`, so a NUL in
/// `payload` or `meta` reached `jsonb` and raised `22P05` — and inside
/// `Queue::enqueue_raw_in` that poisons the caller's transaction, taking their
/// whole unit of work down with it. The same applies to an unbounded `at()` or
/// a Chrono timestamp below PostgreSQL's floor.
#[sqlx::test(migrations = "./migrations")]
async fn test_enqueue_in_a_caller_transaction_rejects_bad_input_without_aborting_it(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let horizon = chrono::Utc::now() + chrono::Duration::days(365 * 200);

    for (label, job) in [
        (
            "payload",
            new_job("nul_payload", |job| {
                job.payload = serde_json::json!({ "note": "bad\0value" });
            }),
        ),
        (
            "meta",
            new_job("nul_meta", |job| {
                job.meta = serde_json::json!({ "trace": "bad\0value" });
            }),
        ),
        (
            "scheduled_at",
            new_job("far_future", |job| {
                job.scheduled_at = Some(horizon);
            }),
        ),
        (
            "scheduled_at_before_postgres_floor",
            new_job("far_past", |job| {
                job.scheduled_at = Some(chrono::DateTime::<chrono::Utc>::MIN_UTC);
            }),
        ),
    ] {
        let mut transaction = pool.begin().await.unwrap();
        // A statement whose result proves the transaction is still usable
        // after the refused enqueue.
        sqlx::query("CREATE TEMPORARY TABLE repro_unit_of_work (n int) ON COMMIT DROP")
            .execute(&mut *transaction)
            .await
            .unwrap();

        let error = db
            .queue
            .enqueue_raw_in(&mut transaction, job)
            .await
            .expect_err(&format!("{label} must be refused before it reaches SQL"));
        assert!(
            matches!(error, pgqueue::Error::Config(_)),
            "{label}: {error:?}"
        );

        sqlx::query("INSERT INTO repro_unit_of_work VALUES (1)")
            .execute(&mut *transaction)
            .await
            .unwrap_or_else(|error| panic!("{label} aborted the caller's transaction: {error}"));
        transaction.commit().await.unwrap();
    }

    // And the same input is refused on the non-transactional path.
    for job in [
        new_job("nul_payload", |job| {
            job.payload = serde_json::json!(["ok", "bad\0"]);
        }),
        new_job("far_future", |job| {
            job.scheduled_at = Some(horizon);
        }),
        new_job("far_past", |job| {
            job.scheduled_at = Some(chrono::DateTime::<chrono::Utc>::MIN_UTC);
        }),
    ] {
        assert!(matches!(
            db.queue.enqueue_raw(job).await.unwrap_err(),
            pgqueue::Error::Config(_)
        ));
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM pgqueue.jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
}

/// A burst of same-key enqueues collapses onto one row only while that row is
/// live: once it completes, the partial dedupe index frees the key and the next
/// enqueue inserts a second row. Publishing the burst inside one transaction is
/// what makes "one row" an invariant rather than a bet on no worker finishing
/// the first row mid-burst — every later insert conflicts with the uncommitted
/// row the transaction itself made, which no worker can dequeue yet.
/// `examples/stress.rs` relies on exactly this to assert `dedupe_rows == 1`.
#[sqlx::test(migrations = "./migrations")]
async fn test_enqueue_in_deduplicates_against_the_uncommitted_row_of_its_own_transaction(
    pool: PgPool,
) {
    let db = TestDb::new(pool.clone()).await;
    let mut transaction = pool.begin().await.unwrap();

    let mut ids = Vec::new();
    for index in 0..5 {
        let result = db
            .queue
            .enqueue_raw_in(
                &mut transaction,
                new_job("burst", |job| job.dedupe_key = Some("singleton".into())),
            )
            .await
            .unwrap();
        assert_eq!(
            result.is_enqueued(),
            index == 0,
            "only the first insert of the burst may create a row"
        );
        ids.push(result.into_job_id());
    }
    assert!(
        ids.windows(2).all(|pair| pair[0] == pair[1]),
        "the burst must collapse onto one job id: {ids:?}"
    );

    transaction.commit().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM pgqueue.jobs WHERE queue = $1 AND dedupe_key = 'singleton'",
        )
        .bind(db.queue.name())
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
}

/// `result_ttl_ms` encodes NULL as "keep forever" and 0 as "delete on finish".
/// A negative value has no encoding, and the column now refuses one rather
/// than leaving a row that decodes as a *live* zero-length retention.
#[sqlx::test(migrations = "./migrations")]
async fn test_result_ttl_column_refuses_a_negative_retention(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let handle = db
        .queue
        .enqueue_raw(new_job("retained", |_| {}))
        .await
        .unwrap();

    let error = sqlx::query("UPDATE pgqueue.jobs SET result_ttl_ms = -1 WHERE id = $1")
        .bind(handle.job_id())
        .execute(&pool)
        .await
        .expect_err("a negative retention has no encoding");
    assert_eq!(
        error
            .as_database_error()
            .and_then(|error| error.constraint()),
        Some("jobs_result_ttl_ms_check"),
        "{error}"
    );

    // The two encodings that do exist are still accepted.
    for ttl in [None, Some(0i64), Some(1i64)] {
        sqlx::query("UPDATE pgqueue.jobs SET result_ttl_ms = $2 WHERE id = $1")
            .bind(handle.job_id())
            .bind(ttl)
            .execute(&pool)
            .await
            .unwrap();
    }
}

/// `retry_job` returning `false` because the dedupe key already belongs to a
/// live occurrence is documented public behaviour, but only the
/// `retried_at IS NULL` guard had ever been exercised — the keyed
/// `ON CONFLICT ... DO NOTHING` arm had not. It also has to leave the terminal
/// row untouched: the refusal rolls the whole transaction back, so the one
/// retry that row is entitled to survives for when the key is free again.
#[sqlx::test(migrations = "./migrations")]
async fn test_retry_job_refuses_a_terminal_occurrence_while_its_dedupe_key_is_live(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let keyed = |queue: &Queue| {
        let job = new_job("keyed_retry", |job| job.dedupe_key = Some("held".into()));
        let queue = queue.clone();
        async move { queue.enqueue_raw(job).await.unwrap().job_id() }
    };

    let terminal = keyed(&db.queue).await;
    assert!(
        db.queue
            .abort_job(terminal, "make it terminal")
            .await
            .unwrap()
    );
    // A fresh occurrence now owns the key.
    let holder = keyed(&db.queue).await;
    assert_ne!(holder, terminal);

    assert!(
        !db.queue.retry_job(terminal, "manual retry").await.unwrap(),
        "a terminal occurrence must not be retried while its dedupe key is held"
    );
    let retried_at: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT retried_at FROM pgqueue.jobs WHERE id = $1")
            .bind(terminal)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        retried_at.is_none(),
        "the refused retry must roll back, leaving the row retryable"
    );
    let keyed_rows: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pgqueue.jobs WHERE dedupe_key = 'held'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        keyed_rows, 2,
        "no occurrence may be inserted for a held key"
    );

    // Once the holder is terminal too, the retry that was refused lands.
    assert!(db.queue.abort_job(holder, "release the key").await.unwrap());
    assert!(db.queue.retry_job(terminal, "manual retry").await.unwrap());
}

/// `counts()` reports the two retained terminal states an operator watches, and
/// both filters had only ever been asserted as `0`.
#[sqlx::test(migrations = "./migrations")]
async fn test_counts_report_retained_failed_and_aborted_jobs(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let aborted = db
        .queue
        .enqueue_raw(new_job("counted_abort", |_| {}))
        .await
        .unwrap();
    assert!(
        db.queue
            .abort_job(aborted.job_id(), "counted")
            .await
            .unwrap()
    );

    let failed = db
        .queue
        .enqueue_raw(new_job("counted_failure", |_| {}))
        .await
        .unwrap();
    let owner = Uuid::now_v7();
    db.queue
        .write_worker_info(owner, serde_json::json!({}), None, Duration::from_secs(30))
        .await
        .unwrap();
    let claimed = db.queue.dequeue(1, owner).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert!(
        db.queue
            .finish(&claimed[0], JobStatus::Failed, None, Some("counted"))
            .await
            .unwrap()
    );

    let counts = db.queue.counts().await.unwrap();
    assert_eq!(
        (counts.failed, counts.aborted, counts.queued, counts.running),
        (1, 1, 0, 0),
        "{counts:?}"
    );
    assert_eq!(failed.job_id(), claimed[0].id);
}

/// Every one of the five counters is scoped to its own queue and ignores
/// retained `complete` rows. Only `queued` had ever been asserted across two
/// queues, so a counter that lost its `queue` predicate — each carries its own
/// now that they are separate aggregates — would have counted a neighbour's
/// jobs with the whole suite still green.
#[sqlx::test(migrations = "./migrations")]
async fn test_counts_are_scoped_to_one_queue_and_ignore_completed_jobs(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let empty = db.another_queue(|b| b.name("empty")).await;
    let worker = Uuid::now_v7();

    // One retained row per terminal state, then the two live ones. Each job is
    // claimed before the next is enqueued, so every claim is unambiguous.
    for (name, status) in [("done", JobStatus::Complete), ("boom", JobStatus::Failed)] {
        db.queue.enqueue_raw(new_job(name, |_| {})).await.unwrap();
        let claimed = db.queue.dequeue(1, worker).await.unwrap();
        assert_eq!(claimed.len(), 1);
        assert!(
            db.queue
                .finish(&claimed[0], status, None, Some("scoped"))
                .await
                .unwrap()
        );
    }

    let stopping = db
        .queue
        .enqueue_raw(new_job("stopping", |_| {}))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(db.queue.dequeue(1, worker).await.unwrap().len(), 1);
    assert!(db.queue.abort_job(stopping, "scoped").await.unwrap());

    db.queue.enqueue_raw(new_job("busy", |_| {})).await.unwrap();
    assert_eq!(db.queue.dequeue(1, worker).await.unwrap().len(), 1);

    let cancelled = db
        .queue
        .enqueue_raw(new_job("cancelled", |_| {}))
        .await
        .unwrap()
        .unwrap();
    assert!(db.queue.abort_job(cancelled, "scoped").await.unwrap());

    db.queue
        .enqueue_raw(new_job("ready", |_| {}))
        .await
        .unwrap();
    db.queue
        .enqueue_raw(new_job("later", |job| {
            job.scheduled_at = Some(Utc::now() + chrono::Duration::seconds(60));
        }))
        .await
        .unwrap();

    let counts = db.queue.counts().await.unwrap();
    assert_eq!(
        (
            counts.queued,
            counts.running,
            counts.scheduled,
            counts.failed,
            counts.aborted,
        ),
        (1, 2, 1, 1, 1),
        "the completed row counts nowhere and aborting still occupies a worker: {counts:?}"
    );

    let neighbour = empty.counts().await.unwrap();
    assert_eq!(
        (
            neighbour.queued,
            neighbour.running,
            neighbour.scheduled,
            neighbour.failed,
            neighbour.aborted,
        ),
        (0, 0, 0, 0, 0),
        "a queue with no rows of its own reports zeros, not its neighbour's \
         jobs and not NULL: {neighbour:?}"
    );
}

/// The version gate is the first thing `Queue::connect` does, and refusing an
/// older server is the whole reason it exists — every statement this library
/// ships assumes PostgreSQL 18 semantics. It had no test because the test
/// cluster is 18: this one shadows `current_setting` in its own database
/// (`pg_catalog` is only searched first when it is not named explicitly) so the
/// connect path sees a 17 server without needing one.
#[tokio::test]
async fn test_queue_connect_refuses_a_server_older_than_postgresql_18() {
    crate::init_tracing();
    let url = crate::fresh_database("pg_version").await;
    let (_, name) = url.rsplit_once('/').expect("database url has a path");

    let mut setup = PgConnection::connect(&url).await.unwrap();
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        r#"CREATE FUNCTION public.current_setting(setting text) RETURNS text
           LANGUAGE sql AS $$
               SELECT CASE WHEN setting = 'server_version_num' THEN '170004'
                           ELSE pg_catalog.current_setting(setting) END
           $$;
           ALTER DATABASE "{name}" SET search_path = public, pg_catalog;"#
    )))
    .execute(&mut setup)
    .await
    .expect("shadow current_setting");
    setup.close().await.unwrap();

    match Queue::connect(&url).await {
        Err(pgqueue::Error::Config(message)) => {
            assert!(
                message.contains("requires PostgreSQL 18+")
                    && message.contains("server_version_num = 170004"),
                "{message}"
            );
        }
        Err(other) => panic!("an old server must be refused as a config error: {other}"),
        Ok(_) => panic!("a PostgreSQL 17 server must be refused"),
    }

    // The refusal comes before the migrator, so nothing was installed.
    let mut check = PgConnection::connect(&url).await.unwrap();
    let installed: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgqueue')",
    )
    .fetch_one(&mut check)
    .await
    .unwrap();
    assert!(!installed, "a refused server must not be migrated");
    check.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// A NUL a handler produces must fail its attempt, not wedge the processor slot
// ---------------------------------------------------------------------------

/// `pgqueue.workers` is keyed by id alone, so the lease upsert guards its update
/// with `WHERE pgqueue.workers.queue = EXCLUDED.queue`: without it, a heartbeat
/// would move another queue's lease — and every attempt owned by it — under a
/// new queue name. A `Worker` mints `Uuid::now_v7()` for itself, so only the
/// low-level [`pgqueue::Consumer`] API, where the caller supplies the id, can
/// collide.
#[sqlx::test(migrations = "./migrations")]
async fn test_consumer_heartbeat_is_refused_when_the_worker_id_belongs_to_another_queue(
    pool: PgPool,
) {
    let db = TestDb::new(pool.clone()).await;
    let beta = db.another_queue(|builder| builder.name("beta")).await;
    let worker_id = Uuid::now_v7();
    let ttl = Duration::from_secs(30);

    db.queue
        .consumer(worker_id)
        .heartbeat(serde_json::json!({ "queue": "alpha" }), None, ttl)
        .await
        .expect("the first queue takes the id");

    let error = beta
        .consumer(worker_id)
        .heartbeat(serde_json::json!({ "queue": "beta" }), None, ttl)
        .await
        .expect_err("a second queue must not adopt a live worker id");
    match error {
        pgqueue::Error::Config(message) => {
            assert!(
                message.contains("already belongs to a different queue")
                    && message.contains(&worker_id.to_string()),
                "{message}"
            );
        }
        other => panic!("a cross-queue worker id must be a config error: {other}"),
    }

    let rows = sqlx::query("SELECT queue, stats FROM pgqueue.workers WHERE id = $1")
        .bind(worker_id)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("queue"), db.queue.name());
    assert_eq!(
        rows[0].get::<serde_json::Value, _>("stats"),
        serde_json::json!({ "queue": "alpha" }),
        "the refused heartbeat must not have written anything"
    );
}

// ---------------------------------------------------------------------------
// Dashboard authentication: cookie scanning and the guessing budget
// ---------------------------------------------------------------------------

/// The enqueue path takes an advisory lock, reads the live holder of the dedupe
/// key, and then inserts with `ON CONFLICT ... DO NOTHING RETURNING id`. That lock
/// only binds writers that take it, so anything writing `pgqueue.jobs` directly
/// — application SQL, a backfill, an ops script — can commit the key between the
/// two statements and leave the insert nothing to return.
///
/// The contract is that "a dedupe-key collision returns the existing live job's
/// id", but that case returned `Error::Config` telling the caller to retry the
/// transaction or switch to `READ COMMITTED` — advice that fits neither the
/// session (already `READ COMMITTED`) nor `Queue::enqueue_raw` (which owns the
/// transaction). `schedule_cron` has handled the identical race on the identical
/// statement all along by re-reading the holder; this is the enqueue path doing
/// the same.
///
/// A `BEFORE INSERT` trigger parks the library's insert *after* its guarded read
/// and *before* the conflict check, which is what makes the window a fixed point
/// rather than a race the test has to win.
#[sqlx::test(migrations = "./migrations")]
async fn test_enqueue_deduplicates_against_a_holder_committed_by_an_unlocked_writer(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    install_statement_gate(
        &pool,
        "enqueue_conflict_gate",
        4343,
        "INSERT",
        "NEW.name = 'gated'",
    )
    .await;
    let gate = hold_gate(&pool, 4343, &db.database).await;

    let queue = db.queue.clone();
    let enqueue = tokio::spawn(async move {
        queue
            .enqueue_raw(new_job("gated", |job| {
                job.dedupe_key = Some("contended-key".into())
            }))
            .await
    });
    wait_for_lock_waiter(
        &db,
        "%INSERT INTO pgqueue.jobs%",
        "the enqueue never reached its insert",
    )
    .await;

    // A writer that never takes the enqueue lock, committing the key in the
    // window the guarded read has already passed.
    let owner: Uuid = sqlx::query_scalar(
        "INSERT INTO pgqueue.jobs (queue, name, dedupe_key) VALUES ($1, 'outsider', $2)
         RETURNING id",
    )
    .bind(db.queue.name())
    .bind("contended-key")
    .fetch_one(db.queue.pool())
    .await
    .expect("outside writer takes the dedupe key");

    gate.rollback().await.unwrap();
    let result = enqueue.await.unwrap().expect("a collision is not an error");
    assert!(
        result.is_deduplicated(),
        "the key was taken, so nothing should have been enqueued"
    );
    assert_eq!(
        result.job_id(),
        owner,
        "the collision must name the job that holds the key"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM pgqueue.jobs WHERE queue = $1 AND dedupe_key = $2"
        )
        .bind(db.queue.name())
        .bind("contended-key")
        .fetch_one(db.queue.pool())
        .await
        .unwrap(),
        1,
        "the dedupe key still admits exactly one live job"
    );
}

// ---------------------------------------------------------------------------
// A superseded attempt must be cancelled, not left running beside its successor
// ---------------------------------------------------------------------------

/// `Consumer::heartbeat` takes the caller's TTL, and `Duration::ZERO` wrote
/// `expires_at = now()` — expired for every later transaction. `dequeue`
/// requires a live lease, so it then returned an empty `Vec` for a queue full
/// of ready work, with no error anywhere to say why. The upper bound was
/// checked all along; only zero slipped through.
#[sqlx::test(migrations = "./migrations")]
async fn test_consumer_heartbeat_refuses_a_zero_lease_ttl(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    db.queue
        .enqueue_raw(new_job("zero-ttl", |_| {}))
        .await
        .unwrap();
    let worker_id = Uuid::now_v7();
    let consumer = db.queue.consumer(worker_id);

    let error = consumer
        .heartbeat(serde_json::json!({}), None, Duration::ZERO)
        .await
        .expect_err("a zero TTL must not be accepted");
    match error {
        pgqueue::Error::Config(message) => {
            assert!(message.contains("must be greater than zero"), "{message}");
        }
        other => panic!("a zero lease TTL must be a config error: {other}"),
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM pgqueue.workers WHERE id = $1")
            .bind(worker_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        0,
        "the refused heartbeat must not have written a lease"
    );

    // The queue was never the problem: a real TTL claims the waiting job.
    consumer
        .heartbeat(serde_json::json!({}), None, Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(consumer.dequeue(10).await.unwrap().len(), 1);
}

// ---------------------------------------------------------------------------
// An idle cron registry must not cost a transaction per cron per tick
// ---------------------------------------------------------------------------

/// The dedupe path opened its transaction *before* validating, so an enqueue
/// whose input can never be accepted first queued for a connection: on a busy
/// pool the caller got `Error::Db(PoolTimedOut)` — a transient failure worth
/// retrying — for input that was permanently invalid. The keyless path
/// validated first and answered `Error::Config` for the very same job.
#[sqlx::test(migrations = "./migrations")]
async fn test_enqueue_rejects_invalid_input_before_taking_a_connection(pool: PgPool) {
    // Generous, because `connect_with` validates the new pool by acquiring a
    // connection under this same timeout: at 250 ms a loaded machine failed the
    // test at pool *creation*, on TCP connect and startup, before the enqueue
    // this is about ever ran. A regression still fails here — the invalid
    // enqueue waits out the timeout for the held connection and reports
    // `PoolTimedOut` instead of `Config` — it just takes longer to say so.
    let starved = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .connect_with(pool.connect_options().as_ref().clone())
        .await
        .unwrap();
    let db = TestDb::new(starved.clone()).await;
    // The pool's only connection, held for the rest of the test.
    let _held = starved.acquire().await.unwrap();

    for dedupe_key in [None, Some("held-key".to_string())] {
        let keyed = dedupe_key.is_some();
        let error = db
            .queue
            .enqueue_raw(new_job("bad\0name", |job| job.dedupe_key = dedupe_key))
            .await
            .expect_err("a NUL in a job name is never valid");
        match error {
            pgqueue::Error::Config(message) => {
                assert!(message.contains("must not contain NUL"), "{message}");
            }
            other => panic!("invalid input with dedupe_key={keyed} reported as {other}"),
        }
    }
}

// ---------------------------------------------------------------------------
// One unreadable `backoff` must not strand the batch it rode in on
// ---------------------------------------------------------------------------

/// Collects the `backoff` field of every event emitted while this is the
/// default subscriber. The decoder's warning is the only place an unreadable
/// value is ever named, so the warning is where the name has to be checked.
#[derive(Clone, Default)]
struct RecordedBackoffs(std::sync::Arc<std::sync::Mutex<Vec<String>>>);

impl RecordedBackoffs {
    fn recorded(&self) -> Vec<String> {
        self.0.lock().expect("recorded backoffs").clone()
    }

    fn record(&self, value: String) {
        self.0.lock().expect("recorded backoffs").push(value);
    }
}

impl tracing::field::Visit for RecordedBackoffs {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "backoff" {
            self.record(value.to_string());
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "backoff" {
            self.record(format!("{value:?}"));
        }
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::layer::Layer<S> for RecordedBackoffs {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        event.record(&mut self.clone());
    }
}

/// `PgValueRef::as_str` hands back the wire bytes verbatim, and binary-format
/// `jsonb` — what every query in this crate uses — puts a one-byte version
/// header in front of the JSON text. `Json::decode` strips it; the warning did
/// not, so the one string identifying the bad value arrived with a stray U+0001
/// glued to its front.
#[sqlx::test(migrations = "./migrations")]
async fn test_unreadable_backoff_warning_reports_the_stored_json(pool: PgPool) {
    use tracing_subscriber::layer::SubscriberExt;

    let db = TestDb::new(pool.clone()).await;
    let handle = db
        .queue
        .enqueue_raw(new_job("unreadable-backoff", |_| {}))
        .await
        .unwrap();
    // Accepted by the column's CHECK — the tag is one this build knows — and
    // still unreadable, because `max_ms` is not a number.
    sqlx::query("UPDATE pgqueue.jobs SET backoff = $2::jsonb WHERE id = $1")
        .bind(handle.job_id())
        .bind(r#"{"type": "exponential", "max_ms": "nope"}"#)
        .execute(&pool)
        .await
        .unwrap();

    let recorded = RecordedBackoffs::default();
    // Thread-local, so it takes precedence over the suite's global subscriber
    // for the read below and nothing else.
    let guard = tracing::subscriber::set_default(
        tracing_subscriber::registry::Registry::default().with(recorded.clone()),
    );
    let row = db
        .queue
        .fetch_job(handle.job_id())
        .await
        .unwrap()
        .expect("the row must decode despite the unreadable strategy");
    drop(guard);

    assert_eq!(row.backoff, JobRetryBackoff::None);
    let reported = recorded.recorded();
    assert_eq!(
        reported.len(),
        1,
        "the unreadable strategy must be reported exactly once: {reported:?}"
    );
    let reported = &reported[0];
    assert!(
        reported.starts_with('{') && !reported.contains('\u{1}'),
        "the warning must name the stored JSON, not the jsonb wire bytes: {reported:?}"
    );
    assert!(
        reported.contains(r#""max_ms": "nope""#),
        "the warning must name the value that could not be read: {reported:?}"
    );
}

/// The sibling `result_ttl_ms` column refuses a value it has no encoding for,
/// and `backoff` now does the same: the leniency above is for rows that
/// predate the check, not a licence to write new ones.
#[sqlx::test(migrations = "./migrations")]
async fn test_backoff_column_refuses_an_unknown_strategy(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let handle = db
        .queue
        .enqueue_raw(new_job("checked", |_| {}))
        .await
        .unwrap();

    for rejected in [
        r#"{"type":"linear"}"#,
        r#"{"delay":5}"#,
        r#""none""#,
        "null",
    ] {
        let error = sqlx::query("UPDATE pgqueue.jobs SET backoff = $2::jsonb WHERE id = $1")
            .bind(handle.job_id())
            .bind(rejected)
            .execute(&pool)
            .await
            .unwrap_err();
        assert_eq!(
            error
                .as_database_error()
                .and_then(|error| error.constraint()),
            Some("jobs_backoff_check"),
            "{rejected} must be refused: {error}"
        );
    }

    // Both strategies the crate does write are still accepted.
    for accepted in [
        r#"{"type":"none"}"#,
        r#"{"type":"exponential"}"#,
        r#"{"type":"exponential","max_ms":60000}"#,
    ] {
        sqlx::query("UPDATE pgqueue.jobs SET backoff = $2::jsonb WHERE id = $1")
            .bind(handle.job_id())
            .bind(accepted)
            .execute(&pool)
            .await
            .unwrap_or_else(|error| panic!("{accepted} must be accepted: {error}"));
    }
}

// ---------------------------------------------------------------------------
// `timeout_ms = 0` means unlimited everywhere else; it must here too
// ---------------------------------------------------------------------------

/// And the column refuses one from now on, for the same reason `result_ttl_ms`
/// refuses a negative retention.
#[sqlx::test(migrations = "./migrations")]
async fn test_timeout_column_refuses_a_non_positive_timeout(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let handle = db
        .queue
        .enqueue_raw(new_job("bounded", |_| {}))
        .await
        .unwrap();

    for rejected in [0i64, -1] {
        let error = sqlx::query("UPDATE pgqueue.jobs SET timeout_ms = $2 WHERE id = $1")
            .bind(handle.job_id())
            .bind(rejected)
            .execute(&pool)
            .await
            .unwrap_err();
        assert_eq!(
            error
                .as_database_error()
                .and_then(|error| error.constraint()),
            Some("jobs_timeout_ms_check"),
            "timeout_ms = {rejected} must be refused: {error}"
        );
    }

    // NULL is how "no timeout" is written, and a positive bound is a timeout.
    for accepted in [None, Some(1i64)] {
        sqlx::query("UPDATE pgqueue.jobs SET timeout_ms = $2 WHERE id = $1")
            .bind(handle.job_id())
            .bind(accepted)
            .execute(&pool)
            .await
            .unwrap();
    }
}

/// The lower bounds are only half of it. `pgqueue.job_is_stuck` adds
/// `timeout_ms` to the sweep grace in `bigint` and is applied to *every* active
/// row of a queue, and finish and abort both compute
/// `now() + result_ttl_ms * interval '1 millisecond'` — so a single row near
/// the type's ceiling answered `22003`/`22008` and took stuck-job recovery down
/// for that whole queue, permanently. Every Rust writer is already held to
/// `MAX_DURATION_MS` by `validate_duration`; these checks are the defence
/// against the writers that are not it.
#[sqlx::test(migrations = "./migrations")]
async fn test_duration_columns_refuse_a_value_past_the_api_maximum(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let handle = db
        .queue
        .enqueue_raw(new_job("bounded-duration", |_| {}))
        .await
        .unwrap();
    let maximum = i64::try_from(pgqueue::__private::MAX_DURATION_MS).unwrap();

    for (column, constraint, statement) in [
        (
            "timeout_ms",
            "jobs_timeout_ms_check",
            "UPDATE pgqueue.jobs SET timeout_ms = $2 WHERE id = $1",
        ),
        (
            "result_ttl_ms",
            "jobs_result_ttl_ms_check",
            "UPDATE pgqueue.jobs SET result_ttl_ms = $2 WHERE id = $1",
        ),
    ] {
        let error = sqlx::query(statement)
            .bind(handle.job_id())
            .bind(i64::MAX)
            .execute(&pool)
            .await
            .unwrap_err();
        assert_eq!(
            error
                .as_database_error()
                .and_then(|error| error.constraint()),
            Some(constraint),
            "{column} = i64::MAX must be refused: {error}"
        );
        // The API's own maximum still is not: the bound is the API's, not a
        // second, stricter policy nothing can reach.
        sqlx::query(statement)
            .bind(handle.job_id())
            .bind(maximum)
            .execute(&pool)
            .await
            .unwrap_or_else(|error| panic!("{column} = {maximum} must be accepted: {error}"));
    }

    // ...and with both at that maximum the arithmetic the sweeper runs over
    // every active row stays in range, which is the whole point of the bound.
    sqlx::query("UPDATE pgqueue.jobs SET started_at = now(), touched_at = now() WHERE id = $1")
        .bind(handle.job_id())
        .execute(&pool)
        .await
        .unwrap();
    let stuck = sqlx::query_scalar::<_, bool>(
        "SELECT pgqueue.job_is_stuck(j, $2::bigint, now()) FROM pgqueue.jobs j WHERE j.id = $1",
    )
    .bind(handle.job_id())
    .bind(maximum)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(!stuck);
}

// ---------------------------------------------------------------------------
// A NUL in a dashboard name filter is a malformed request, not a 500
// ---------------------------------------------------------------------------
