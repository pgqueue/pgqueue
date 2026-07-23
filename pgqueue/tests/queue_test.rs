//! Integration tests for the Postgres queue core, against a real database.

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

use crate::{
    EnqueueOutcomeTestExt, QueueProtocolTestExt, TestDb, backdate_job_liveness, new_job,
    wait_until, with_config,
};
use chrono::Utc;
use pgqueue::{
    EnqueueOutcome, Error, JobCursor, JobFilter, JobRetention, JobRetryBackoff, JobStatus,
    MigrationMode, Queue,
};
use serde_json::json;
use uuid::Uuid;

async fn connect_with_validation(pool: PgPool) -> Result<Queue, Error> {
    Queue::builder("postgres://unused")
        .pool(pool)
        .migration_mode(MigrationMode::Validate)
        .connect()
        .await
}

#[sqlx::test(migrations = "./migrations")]
async fn connect_is_idempotent_and_migrations_rerun(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    // A second connect runs SQLx's migration check as a no-op.
    let again = db.another_queue(|b| b).await;
    assert_eq!(again.name(), db.queue.name());
}

#[sqlx::test(migrations = "./migrations")]
async fn migrations_install_the_current_jobs_and_workers_columns(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let job_columns = sqlx::query_scalar!(
        "SELECT column_name AS \"column_name!\" FROM information_schema.columns WHERE table_schema = 'pgqueue' AND table_name = 'jobs' AND column_name IN ('cron_expr', 'kind', 'max_attempts', 'retried_at') ORDER BY column_name"
    )
    .fetch_all(db.queue.pool())
    .await
    .unwrap();
    assert_eq!(
        job_columns,
        ["cron_expr", "kind", "max_attempts", "retried_at"]
    );

    let worker_accepting = sqlx::query_scalar!(
        "SELECT column_name AS \"column_name!\" FROM information_schema.columns WHERE table_schema = 'pgqueue' AND table_name = 'workers' AND column_name = 'accepting'"
    )
    .fetch_optional(db.queue.pool())
    .await
    .unwrap();
    assert_eq!(worker_accepting.as_deref(), Some("accepting"));
}

#[sqlx::test(migrations = "./migrations")]
async fn migrations_install_the_cron_occurrence_ledger_and_expiry_index(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let primary_key = sqlx::query_scalar!(
        "SELECT pg_get_constraintdef(oid) AS \"definition!\" FROM pg_constraint WHERE conrelid = 'pgqueue.cron_occurrences'::regclass AND contype = 'p'"
    )
    .fetch_one(db.queue.pool())
    .await
    .unwrap();
    assert!(
        primary_key.contains("PRIMARY KEY (queue, unique_key, scheduled_at)"),
        "{primary_key}"
    );

    let expiry_index = sqlx::query_scalar!(
        "SELECT indexdef AS \"indexdef!\" FROM pg_indexes WHERE schemaname = 'pgqueue' AND indexname = 'cron_occurrences_expiry_idx'"
    )
    .fetch_one(db.queue.pool())
    .await
    .unwrap();
    assert!(expiry_index.contains("(queue, expires_at)"));
}

#[sqlx::test(migrations = "./migrations")]
async fn migrations_install_the_registry_filtered_dequeue_index(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let definition = sqlx::query_scalar!(
        "SELECT indexdef AS \"indexdef!\" FROM pg_indexes WHERE schemaname = 'pgqueue' AND indexname = 'jobs_dequeue_name_idx'"
    )
    .fetch_one(db.queue.pool())
    .await
    .unwrap();
    assert!(definition.contains("(queue, name, priority, scheduled_at, id)"));
    assert!(definition.contains("status = 'queued'"));
}

#[sqlx::test(migrations = "./migrations")]
async fn migrations_install_the_queued_group_order_index(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let definition = sqlx::query_scalar!(
        "SELECT indexdef AS \"indexdef!\" FROM pg_indexes WHERE schemaname = 'pgqueue' AND indexname = 'jobs_queued_group_order_idx'"
    )
    .fetch_one(db.queue.pool())
    .await
    .unwrap();
    assert!(definition.contains("(queue, group_key, priority, scheduled_at, id)"));
    assert!(definition.contains("status = 'queued'"));
    assert!(definition.contains("group_key IS NOT NULL"));
}

#[sqlx::test(migrations = "./migrations")]
async fn migrations_install_the_cron_registry_and_running_group_guard(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let schedule_key = sqlx::query_scalar!(
        "SELECT pg_get_constraintdef(oid) AS \"definition!\" FROM pg_constraint WHERE conrelid = 'pgqueue.cron_schedules'::regclass AND contype = 'p'"
    )
    .fetch_one(db.queue.pool())
    .await
    .unwrap();
    assert!(
        schedule_key.contains("PRIMARY KEY (queue, unique_key)"),
        "{schedule_key}"
    );

    let group_index = sqlx::query!(
        "SELECT pg_get_indexdef(indexes.indexrelid) AS \"indexdef!\", pg_get_expr(indexes.indpred, indexes.indrelid) AS \"predicate!\" FROM pg_index indexes JOIN pg_class index_class ON index_class.oid = indexes.indexrelid JOIN pg_class table_class ON table_class.oid = indexes.indrelid JOIN pg_namespace namespace ON namespace.oid = table_class.relnamespace WHERE namespace.nspname = 'pgqueue' AND index_class.relname = 'jobs_running_group_unique_idx'"
    )
    .fetch_one(db.queue.pool())
    .await
    .unwrap();
    assert!(group_index.indexdef.contains("UNIQUE INDEX"));
    assert!(group_index.indexdef.contains("(queue, group_key)"));
    assert!(group_index.predicate.contains("group_key IS NOT NULL"));
    for status in ["running", "aborting"] {
        assert!(
            group_index.predicate.contains(status),
            "{}",
            group_index.predicate
        );
    }

    let redundant = sqlx::query_scalar!("SELECT to_regclass('pgqueue.jobs_group_idx')::text")
        .fetch_one(db.queue.pool())
        .await
        .unwrap();
    assert!(redundant.is_none());
}

#[sqlx::test(migrations = "./migrations")]
async fn migrations_install_the_live_unique_key_index(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let row = sqlx::query!(
        "SELECT pg_get_indexdef(indexes.indexrelid) AS \"indexdef!\", pg_get_expr(indexes.indpred, indexes.indrelid) AS \"predicate!\" FROM pg_index indexes JOIN pg_class index_class ON index_class.oid = indexes.indexrelid JOIN pg_class table_class ON table_class.oid = indexes.indrelid JOIN pg_namespace namespace ON namespace.oid = table_class.relnamespace WHERE namespace.nspname = 'pgqueue' AND index_class.relname = 'jobs_unique_key_idx'"
    )
    .fetch_one(db.queue.pool())
    .await
    .unwrap();
    let (indexdef, predicate) = (row.indexdef, row.predicate);
    assert!(indexdef.contains("UNIQUE INDEX"));
    assert!(indexdef.contains("(queue, unique_key)"));
    assert!(predicate.contains("unique_key IS NOT NULL"), "{predicate}");
    for status in ["queued", "running", "aborting"] {
        assert!(
            predicate.contains(status),
            "live index predicate missing {status}: {predicate}"
        );
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn migrations_use_an_isolated_history_table(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let pgqueue_history =
        sqlx::query_scalar!("SELECT to_regclass('pgqueue._sqlx_migrations')::text")
            .fetch_one(db.queue.pool())
            .await
            .unwrap();
    let public_history = sqlx::query_scalar!("SELECT to_regclass('public._sqlx_migrations')::text")
        .fetch_one(db.queue.pool())
        .await
        .unwrap();
    assert!(pgqueue_history.is_some());
    assert!(public_history.is_none());
}

//noinspection SqlNoDataSourceInspection
#[sqlx::test(migrations = "./migrations")]
async fn migration_mode_validate_needs_no_schema_ddl_privilege(pool: PgPool) {
    let restricted = crate::pool_with_max(&pool, 1).await;
    sqlx::query!("SET ROLE pg_read_all_data")
        .execute(&restricted)
        .await
        .unwrap();
    let can_create = sqlx::query_scalar!(
        r#"SELECT has_schema_privilege(current_user, 'pgqueue', 'CREATE') AS "allowed!""#
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
async fn migration_mode_validate_rejects_a_dirty_migration(pool: PgPool) {
    let changed =
        sqlx::query!("UPDATE pgqueue._sqlx_migrations SET success = false WHERE version = 1")
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
async fn migration_mode_validate_rejects_an_unknown_migration(pool: PgPool) {
    let inserted = sqlx::query!(
        "INSERT INTO pgqueue._sqlx_migrations (version, description, success, checksum, execution_time) SELECT 999999, 'unknown', true, checksum, 0 FROM pgqueue._sqlx_migrations WHERE version = 1"
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
async fn migration_mode_validate_rejects_a_modified_migration(pool: PgPool) {
    let changed = sqlx::query!(
        "UPDATE pgqueue._sqlx_migrations SET checksum = checksum || decode('00', 'hex') WHERE version = 1"
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
async fn migration_mode_validate_rejects_a_missing_migration(pool: PgPool) {
    let deleted = sqlx::query!(
        "DELETE FROM pgqueue._sqlx_migrations WHERE version = (SELECT max(version) FROM pgqueue._sqlx_migrations)"
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
async fn migration_mode_skip_does_not_require_the_queue_schema(pool: PgPool) {
    sqlx::query!("DROP SCHEMA pgqueue CASCADE")
        .execute(&pool)
        .await
        .unwrap();

    Queue::builder("postgres://unused")
        .pool(pool.clone())
        .migration_mode(MigrationMode::Skip)
        .connect()
        .await
        .unwrap();
    let error = Queue::builder("postgres://unused")
        .pool(pool)
        .migration_mode(MigrationMode::Validate)
        .connect()
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Db(_)), "{error}");
}

#[sqlx::test(migrations = "./migrations")]
async fn connect_rejects_unsafe_queue_configuration(pool: PgPool) {
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
async fn enqueue_rejects_values_that_break_database_arithmetic(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let invalid = [
        with_config("zero-max-attempts", |config| config.max_attempts = 0),
        with_config("too-many-max-attempts", |config| {
            config.max_attempts = u32::MAX
        }),
        with_config("zero-heartbeat", |config| {
            config.heartbeat = Some(Duration::ZERO)
        }),
        with_config("zero-timeout", |config| {
            config.timeout = Some(Duration::ZERO)
        }),
        with_config("huge-delay", |config| config.retry_delay = Duration::MAX),
        new_job("", |_| {}),
        new_job("nul", |job| job.unique_key = Some("bad\0key".into())),
        new_job("long-unique", |job| job.unique_key = Some("x".repeat(256))),
        new_job("long-group", |job| job.group_key = Some("x".repeat(256))),
    ];
    for job in invalid {
        assert!(
            matches!(db.queue.enqueue_raw(job).await, Err(Error::Config(_))),
            "invalid job must fail before reaching PostgreSQL"
        );
    }
    assert_eq!(db.queue.counts().await.unwrap().queued, 0);
}

#[sqlx::test(migrations = "./migrations")]
async fn enqueue_round_trips_all_fields(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(new_job("send_email", |job| {
            job.payload = json!({"to": "a@b.c"});
            job.meta = json!({"trace": "xyz"});
            job.group_key = Some("g1".into());
            job.config.priority = -3;
            job.config.max_attempts = 5;
            job.config.timeout = Some(Duration::from_secs(30));
            job.config.heartbeat = Some(Duration::from_secs(7));
            job.config.retry_delay = Duration::from_millis(250);
            job.config.backoff = JobRetryBackoff::Exponential {
                max: Some(Duration::from_secs(60)),
            };
            job.config.retention = JobRetention::For(Duration::from_secs(3600));
        }))
        .await
        .unwrap()
        .expect("enqueued");

    let row = db.queue.job(id).await.unwrap().expect("job exists");
    assert_eq!(row.id, id);
    assert_eq!(row.queue, "default");
    assert_eq!(row.name, "send_email");
    assert_eq!(row.payload, json!({"to": "a@b.c"}));
    assert_eq!(row.meta, json!({"trace": "xyz"}));
    assert_eq!(row.status, JobStatus::Queued);
    assert_eq!(row.priority, -3);
    assert_eq!(row.group_key.as_deref(), Some("g1"));
    assert_eq!(row.attempts, 0);
    assert_eq!(row.max_attempts, 5);
    assert_eq!(row.timeout(), Some(Duration::from_secs(30)));
    assert_eq!(row.heartbeat(), Some(Duration::from_secs(7)));
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
    assert!(row.unique_key.is_none());
}

#[sqlx::test(migrations = "./migrations")]
async fn enqueue_raw_in_obeys_the_caller_transaction(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let mut committed = db.queue.pool().begin().await.unwrap();
    let first = db
        .queue
        .enqueue_raw_in(&mut committed, new_job("committed-a", |_| {}))
        .await
        .unwrap()
        .into_handle();
    let second = db
        .queue
        .enqueue_raw_in(&mut committed, new_job("committed-b", |_| {}))
        .await
        .unwrap()
        .into_handle();
    assert!(db.queue.job(first).await.unwrap().is_none());
    assert!(db.queue.job(second).await.unwrap().is_none());
    committed.commit().await.unwrap();
    assert!(db.queue.job(first).await.unwrap().is_some());
    assert!(db.queue.job(second).await.unwrap().is_some());

    let mut rolled_back = db.queue.pool().begin().await.unwrap();
    let discarded = db
        .queue
        .enqueue_raw_in(&mut rolled_back, new_job("discarded", |_| {}))
        .await
        .unwrap()
        .into_handle();
    rolled_back.rollback().await.unwrap();
    assert!(db.queue.job(discarded).await.unwrap().is_none());
}

#[sqlx::test(migrations = "./migrations")]
async fn transactional_dedupe_does_not_lock_the_existing_job_row(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let unique_job = || {
        new_job("unique", |job| {
            job.unique_key = Some("transactional-dedupe".into())
        })
    };
    let id = db.queue.enqueue_raw(unique_job()).await.unwrap().unwrap();
    let active = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);

    let mut transaction = db.queue.pool().begin().await.unwrap();
    let outcome = db
        .queue
        .enqueue_raw_in(&mut transaction, unique_job())
        .await
        .unwrap();
    assert!(matches!(
        outcome,
        EnqueueOutcome::Deduplicated(existing) if existing == id
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
        db.queue.job(id).await.unwrap().unwrap().status,
        JobStatus::Complete
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn enqueue_rounds_nonzero_fractional_milliseconds_up(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(new_job("precise", |job| {
            job.config.timeout = Some(Duration::from_micros(500));
            job.config.heartbeat = Some(Duration::from_micros(1_500));
            job.config.retry_delay = Duration::from_nanos(1);
            job.config.retention = JobRetention::For(Duration::from_micros(1_500));
            job.config.backoff = JobRetryBackoff::Exponential {
                max: Some(Duration::from_micros(500)),
            };
        }))
        .await
        .unwrap()
        .unwrap();

    let row = db.queue.job(id).await.unwrap().unwrap();
    assert_eq!(row.timeout_ms, Some(1));
    assert_eq!(row.heartbeat_ms, Some(2));
    assert_eq!(row.retry_delay_ms, 1);
    assert_eq!(row.ttl_ms, Some(2));
    assert_eq!(
        row.backoff,
        JobRetryBackoff::Exponential {
            max: Some(Duration::from_millis(1))
        }
    );
}

//noinspection SqlNoDataSourceInspection
#[sqlx::test(migrations = "./migrations")]
async fn stored_backoff_without_max_ms_still_decodes_and_dequeues(pool: PgPool) {
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
    sqlx::query!(
        r#"UPDATE pgqueue.jobs SET backoff = '{"type":"exponential"}'::jsonb WHERE id = $1"#,
        id
    )
    .execute(db.queue.pool())
    .await
    .unwrap();

    let row = db.queue.job(id).await.unwrap().unwrap();
    assert_eq!(row.backoff, JobRetryBackoff::Exponential { max: None });
    let active = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);
    assert_eq!(active.id, id);
}

#[sqlx::test(migrations = "./migrations")]
async fn missing_job_operations_return_their_documented_outcomes(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    assert!(db.queue.job(Uuid::now_v7()).await.unwrap().is_none());
    let err = db
        .queue
        .touch(Uuid::now_v7())
        .await
        .expect_err("touch missing");
    assert!(matches!(err, Error::JobNotFound(_)));
    assert!(!db.queue.abort(Uuid::now_v7(), "x").await.unwrap());
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
async fn unique_key_dedupes_live_jobs_and_preserves_terminal_occurrences(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(new_job("cron_job", |job| {
            job.unique_key = Some("cron:cron_job".into());
        }))
        .await
        .unwrap()
        .expect("first enqueue");
    let t0 = db.queue.job(id).await.unwrap().unwrap().scheduled_at;
    let enqueue_at = |at| {
        new_job("cron_job", move |job| {
            job.unique_key = Some("cron:cron_job".into());
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
        db.queue.job(id).await.unwrap().unwrap().status,
        JobStatus::Complete
    );
    let row = db.queue.job(second).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Queued);
    assert_eq!(row.attempts, 0, "revive resets attempts");
    assert!(row.result.is_none());

    // Finish again; ordinary unique jobs enqueue even with an earlier/equal
    // schedule. Only the cron-specific enqueue path applies occurrence ordering.
    sqlx::query!(
        "UPDATE pgqueue.jobs SET scheduled_at = now() WHERE id = $1",
        second
    )
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
        .expect("terminal unique key can be reused regardless of schedule");
    assert_ne!(third, id);
    assert_ne!(third, second);
}

#[sqlx::test(migrations = "./migrations")]
async fn concurrent_unique_enqueues_accept_exactly_one_live_occurrence(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let mut enqueues = tokio::task::JoinSet::new();
    for _ in 0..100 {
        let queue = db.queue.clone();
        enqueues.spawn(async move {
            queue
                .enqueue_raw(new_job("singleton", |job| {
                    job.unique_key = Some("contended-key".into());
                }))
                .await
                .unwrap()
        });
    }
    let mut accepted = Vec::new();
    while let Some(result) = enqueues.join_next().await {
        if let EnqueueOutcome::Enqueued(id) = result.unwrap() {
            accepted.push(id);
        }
    }
    assert_eq!(accepted.len(), 1);
    assert_eq!(db.queue.counts().await.unwrap().queued, 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn aborted_future_scheduled_unique_jobs_can_be_reenqueued(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    // A unique job scheduled for tomorrow, aborted while still queued: the
    // terminal row keeps its future schedule.
    let id = db
        .queue
        .enqueue_raw(new_job("report", |job| {
            job.unique_key = Some("report:x".into());
            job.scheduled_at = Some(Utc::now() + chrono::Duration::days(1));
        }))
        .await
        .unwrap()
        .unwrap();
    assert!(db.queue.abort(id, "changed plans").await.unwrap());

    // Re-enqueueing the key to run now must create a new occurrence, not no-op
    // until tomorrow.
    let next = db
        .queue
        .enqueue_raw(new_job("report", |job| {
            job.unique_key = Some("report:x".into());
        }))
        .await
        .unwrap()
        .expect("dead future-scheduled key must be reusable");
    assert_ne!(next, id);
    assert_eq!(
        db.queue.job(id).await.unwrap().unwrap().status,
        JobStatus::Aborted
    );
    let row = db.queue.job(next).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Queued);
    assert!(row.scheduled_at <= Utc::now() + chrono::Duration::seconds(1));
}

#[sqlx::test(migrations = "./migrations")]
async fn dequeue_orders_by_priority_then_schedule(pool: PgPool) {
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
async fn dequeue_marks_rows_active(pool: PgPool) {
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
async fn consumer_heartbeat_and_attempt_finish_use_guarded_capabilities(pool: PgPool) {
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
    assert_eq!(attempt.row().id, id);
    assert_eq!(attempt.row().worker_id, Some(worker_id));
    attempt.touch().await.unwrap();
    assert!(
        attempt
            .finish(JobStatus::Complete, Some(json!("ok")), None)
            .await
            .unwrap()
    );

    let info = db.queue.info().await.unwrap();
    assert!(info.workers.iter().any(|worker| worker.id == worker_id));
    assert_eq!(
        db.queue.job(id).await.unwrap().unwrap().status,
        JobStatus::Complete
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn consumer_attempt_finish_can_retry_after_a_pool_timeout(pool: PgPool) {
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
    let attempt = db
        .queue
        .consumer(Uuid::now_v7())
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
        db.queue.job(id).await.unwrap().unwrap().status,
        JobStatus::Complete
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn consumer_attempt_retry_refusal_leaves_the_attempt_finishable(pool: PgPool) {
    let db = TestDb::new(pool).await;
    let id = db
        .queue
        .enqueue_raw(with_config("refused-retry", |config| {
            config.max_attempts = 1;
        }))
        .await
        .unwrap()
        .unwrap();
    let attempt = db
        .queue
        .consumer(Uuid::now_v7())
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
        db.queue.job(id).await.unwrap().unwrap().status,
        JobStatus::Failed
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn consumer_attempt_cannot_touch_or_finish_a_newer_attempt(pool: PgPool) {
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
    let first = db
        .queue
        .consumer(Uuid::now_v7())
        .dequeue(1)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    backdate_job_liveness(&db, id).await;
    let mut sweeper = db.queue.sweeper();
    assert_eq!(sweeper.sweep().await.unwrap().cancelling, vec![id]);
    assert_eq!(sweeper.sweep().await.unwrap().swept, vec![id]);
    sweeper.release().await;

    let second = db
        .queue
        .consumer(Uuid::now_v7())
        .dequeue(1)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(second.row().attempts, 2);
    assert!(matches!(first.touch().await, Err(Error::JobNotTouchable(job)) if job == id));
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
async fn dequeue_respects_limit_schedule_and_priority_range(pool: PgPool) {
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
async fn priority_filter_preserves_group_ready_order(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let earlier = db
        .queue
        .enqueue_raw(new_job("earlier", |job| {
            job.group_key = Some("serial".into());
            job.config.priority = -1;
        }))
        .await
        .unwrap()
        .unwrap();
    let later = db
        .queue
        .enqueue_raw(new_job("later", |job| {
            job.group_key = Some("serial".into());
            job.config.priority = 0;
        }))
        .await
        .unwrap()
        .unwrap();
    let restricted = db.another_queue(|builder| builder.priorities(0, 10)).await;

    assert!(
        restricted
            .dequeue(1, Uuid::now_v7())
            .await
            .unwrap()
            .is_empty(),
        "a priority filter must not let a later group member overtake"
    );
    let first = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap();
    assert_eq!(first[0].id, earlier);
    db.queue
        .finish(&first[0], JobStatus::Complete, None, None)
        .await
        .unwrap();

    let second = restricted.dequeue(1, Uuid::now_v7()).await.unwrap();
    assert_eq!(second[0].id, later);
}

#[sqlx::test(migrations = "./migrations")]
async fn group_key_allows_one_active_job_per_group(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    for _ in 0..2 {
        db.queue
            .enqueue_raw(new_job("grouped", |job| job.group_key = Some("g".into())))
            .await
            .unwrap();
    }
    let worker = Uuid::now_v7();
    let first = db.queue.dequeue(10, worker).await.unwrap();
    assert_eq!(first.len(), 1, "only one running job per group");
    assert!(db.queue.dequeue(10, worker).await.unwrap().is_empty());

    db.queue
        .finish(&first[0], JobStatus::Complete, None, None)
        .await
        .unwrap();
    let second = db.queue.dequeue(10, worker).await.unwrap();
    assert_eq!(second.len(), 1);
    assert_ne!(second[0].id, first[0].id);
}

#[sqlx::test(migrations = "./migrations")]
async fn dequeue_limit_is_applied_after_group_deduplication(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    for _ in 0..3 {
        db.queue
            .enqueue_raw(new_job("crowded", |job| job.group_key = Some("g".into())))
            .await
            .unwrap();
    }
    for name in ["free-a", "free-b"] {
        db.queue.enqueue_raw(new_job(name, |_| {})).await.unwrap();
    }

    let jobs = db.queue.dequeue(3, Uuid::now_v7()).await.unwrap();
    assert_eq!(
        jobs.len(),
        3,
        "one crowded group must not consume the limit"
    );
    assert_eq!(
        jobs.iter()
            .filter(|job| job.group_key.as_deref() == Some("g"))
            .count(),
        1
    );
    assert_eq!(jobs.iter().filter(|job| job.group_key.is_none()).count(), 2);
}

#[sqlx::test(migrations = "./migrations")]
async fn dequeue_cursor_handles_a_large_tied_group_prefix(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    sqlx::query!(
        "INSERT INTO pgqueue.jobs \
         (queue, name, payload, group_key, scheduled_at) \
         SELECT $1, $2, 'null'::jsonb, \
                CASE WHEN n <= 900 THEN $3 ELSE $4 || n::text END, \
                now() - interval '1 second' \
         FROM generate_series(1, 1000) AS n",
        db.queue.name(),
        "tied",
        "crowded",
        "group-"
    )
    .execute(db.queue.pool())
    .await
    .unwrap();

    let jobs = db.queue.dequeue(32, Uuid::now_v7()).await.unwrap();
    assert_eq!(jobs.len(), 32);
    assert_eq!(
        jobs.iter()
            .filter(|job| job.group_key.as_deref() == Some("crowded"))
            .count(),
        1
    );
    assert_eq!(
        jobs.iter()
            .filter_map(|job| job.group_key.as_deref())
            .collect::<std::collections::HashSet<_>>()
            .len(),
        32
    );
}

//noinspection SqlNoDataSourceInspection
#[sqlx::test(migrations = "./migrations")]
async fn dequeue_serializes_grouped_jobs_on_the_advisory_lock(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    db.queue
        .enqueue_raw(new_job("locked", |job| {
            job.group_key = Some("serial".into())
        }))
        .await
        .unwrap()
        .unwrap();

    let mut lock_tx = db.queue.pool().begin().await.unwrap();
    sqlx::query!(
        "SELECT pg_advisory_xact_lock($1, hashtext($2))",
        pgqueue::__private::dequeue_lock_key(&db.database),
        db.queue.name()
    )
    .execute(&mut *lock_tx)
    .await
    .unwrap();

    let queue = db.queue.clone();
    let mut dequeuing = tokio::spawn(async move { queue.dequeue(1, Uuid::now_v7()).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(250), &mut dequeuing)
            .await
            .is_err(),
        "dequeuer bypassed the dequeue lock"
    );

    lock_tx.rollback().await.unwrap();
    let jobs = tokio::time::timeout(Duration::from_secs(5), dequeuing)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(jobs.len(), 1);
}

//noinspection SqlNoDataSourceInspection
#[sqlx::test(migrations = "./migrations")]
async fn dequeue_ungrouped_fast_path_bypasses_the_advisory_lock(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(new_job("unlocked", |_| {}))
        .await
        .unwrap()
        .unwrap();

    let mut lock_tx = db.queue.pool().begin().await.unwrap();
    sqlx::query!(
        "SELECT pg_advisory_xact_lock($1, hashtext($2))",
        pgqueue::__private::dequeue_lock_key(&db.database),
        db.queue.name()
    )
    .execute(&mut *lock_tx)
    .await
    .unwrap();

    let jobs = tokio::time::timeout(Duration::from_secs(1), db.queue.dequeue(1, Uuid::now_v7()))
        .await
        .expect("ungrouped dequeue should not wait for the queue lock")
        .unwrap();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].id, id);
    lock_tx.rollback().await.unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn concurrent_dequeues_get_disjoint_jobs(pool: PgPool) {
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
async fn concurrent_ungrouped_dequeues_make_disjoint_progress_at_scale(pool: PgPool) {
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
async fn finish_complete_stores_result_and_expiry(pool: PgPool) {
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
    let row = db.queue.job(id).await.unwrap().unwrap();
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
    let row = db.queue.job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Complete);

    assert_eq!(db.queue.stats().complete, 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn finish_rejects_nonterminal_statuses(pool: PgPool) {
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
        db.queue.job(active.id).await.unwrap().unwrap().status,
        JobStatus::Running
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn finish_retention_forever_and_delete_immediately(pool: PgPool) {
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
    let row = db.queue.job(forever).await.unwrap().unwrap();
    assert!(row.expires_at.is_none(), "forever rows never expire");

    db.queue
        .finish(ephemeral_row, JobStatus::Complete, None, None)
        .await
        .unwrap();
    assert!(
        db.queue.job(ephemeral).await.unwrap().is_none(),
        "deleted on finish"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn finish_failed_counts_and_stores_error(pool: PgPool) {
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
    let row = db.queue.job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Failed);
    assert_eq!(row.error.as_deref(), Some("failed: boom"));
    assert_eq!(db.queue.stats().failed, 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn retry_requeues_with_delay(pool: PgPool) {
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
    let updated = db.queue.job(row.id).await.unwrap().unwrap();
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

#[sqlx::test(migrations = "./migrations")]
async fn retry_refuses_when_attempts_are_exhausted(pool: PgPool) {
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
    let updated = db.queue.job(id).await.unwrap().unwrap();
    assert_eq!(updated.status, JobStatus::Running);
    assert_eq!(updated.attempts, 1);
    assert!(updated.error.is_none());
    assert_eq!(db.queue.stats().retried, 0);
}

#[sqlx::test(migrations = "./migrations")]
async fn abort_queued_job_finishes_immediately(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(new_job("j", |_| {}))
        .await
        .unwrap()
        .unwrap();
    assert!(db.queue.abort(id, "not needed").await.unwrap());
    let row = db.queue.job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Aborted);
    assert_eq!(row.error.as_deref(), Some("not needed"));
    assert!(row.completed_at.is_some());
    assert!(
        row.expires_at.is_some(),
        "retention applies to aborted rows"
    );
    assert_eq!(db.queue.stats().aborted, 1);

    // A terminal job can't be aborted again.
    assert!(!db.queue.abort(id, "again").await.unwrap());
}

#[sqlx::test(migrations = "./migrations")]
async fn abort_queued_delete_immediately_survives_until_sweep(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(with_config("j", |config| {
            config.retention = JobRetention::DeleteImmediately
        }))
        .await
        .unwrap()
        .unwrap();
    assert!(db.queue.abort(id, "not needed").await.unwrap());
    let row = db.queue.job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Aborted);
    assert!(row.expires_at.is_some());

    let mut sweeper = db.queue.sweeper();
    assert_eq!(sweeper.sweep().await.unwrap().purged_jobs, 1);
    assert!(db.queue.job(id).await.unwrap().is_none());
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn abort_running_job_goes_through_aborting(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(new_job("j", |_| {}))
        .await
        .unwrap()
        .unwrap();
    let active = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);
    backdate_job_liveness(&db, id).await;
    let touched_before_abort = db.queue.job(id).await.unwrap().unwrap().touched_at.unwrap();

    assert!(db.queue.abort(id, "stop it").await.unwrap());
    let row = db.queue.job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Aborting, "worker must cancel it");
    assert_eq!(
        db.queue.counts().await.unwrap().running,
        1,
        "aborting work still occupies a worker"
    );
    assert!(
        row.touched_at.unwrap() > touched_before_abort,
        "abort updates the job's last-touched timestamp"
    );
    backdate_job_liveness(&db, id).await;
    let touched_before_touch = db.queue.job(id).await.unwrap().unwrap().touched_at.unwrap();
    db.queue.touch(id).await.unwrap();
    let touched_while_aborting = db.queue.job(id).await.unwrap().unwrap().touched_at.unwrap();
    assert!(
        touched_while_aborting > touched_before_touch,
        "cleanup can keep an aborting attempt alive"
    );

    assert!(
        !db.queue
            .finish(&active, JobStatus::Complete, Some(json!("too late")), None)
            .await
            .unwrap(),
        "a public finish must not overwrite a committed abort"
    );
    assert_eq!(
        db.queue.job(id).await.unwrap().unwrap().status,
        JobStatus::Aborting
    );

    // The worker's abort loop then finishes it.
    assert!(
        db.queue
            .finish(&active, JobStatus::Aborted, None, Some("stop it"))
            .await
            .unwrap()
    );
    let row = db.queue.job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Aborted);
}

#[sqlx::test(migrations = "./migrations")]
async fn retry_job_grants_terminal_jobs_one_more_attempt(pool: PgPool) {
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
    let original = db.queue.job(id).await.unwrap().unwrap();
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
async fn concurrent_manual_retries_enqueue_exactly_one_occurrence(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(new_job("j", |_| {}))
        .await
        .unwrap()
        .unwrap();
    assert!(db.queue.abort(id, "make terminal").await.unwrap());

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
async fn touch_updates_heartbeat(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(new_job("j", |_| {}))
        .await
        .unwrap()
        .unwrap();
    db.queue.dequeue(1, Uuid::now_v7()).await.unwrap();
    backdate_job_liveness(&db, id).await;
    let before = db.queue.job(id).await.unwrap().unwrap().touched_at.unwrap();
    db.queue.touch(id).await.unwrap();
    let after = db.queue.job(id).await.unwrap().unwrap().touched_at.unwrap();
    assert!(after > before);
}

#[sqlx::test(migrations = "./migrations")]
async fn touch_distinguishes_an_inactive_job_from_a_missing_job(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(new_job("j", |_| {}))
        .await
        .unwrap()
        .unwrap();
    let row = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);
    assert!(
        db.queue
            .finish(&row, JobStatus::Complete, None, None)
            .await
            .unwrap()
    );

    assert!(matches!(
        db.queue.touch(id).await,
        Err(Error::JobNotTouchable(job_id)) if job_id == id
    ));
}

#[sqlx::test(migrations = "./migrations")]
async fn jobs_page_filters_and_paginates(pool: PgPool) {
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
async fn jobs_page_orders_reused_unique_keys_by_latest_enqueue(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let unique = || {
        new_job("unique", |job| {
            job.unique_key = Some("key".into());
        })
    };
    let first_id = db.queue.enqueue_raw(unique()).await.unwrap().unwrap();
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
    sqlx::query!(
        "UPDATE pgqueue.jobs SET enqueued_at = now() - interval '1 second' \
         WHERE id IN ($1, $2)",
        first_id,
        newer_id
    )
    .execute(db.queue.pool())
    .await
    .unwrap();
    let latest_id = db.queue.enqueue_raw(unique()).await.unwrap().unwrap();
    assert_ne!(latest_id, first_id);

    let jobs = db.queue.jobs_page(JobFilter::default()).await.unwrap();
    assert_eq!(
        jobs[0].id, latest_id,
        "new occurrence is the newest activity"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn counts_split_queued_running_scheduled(pool: PgPool) {
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
async fn worker_info_appears_until_ttl_expires(pool: PgPool) {
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

#[sqlx::test(migrations = "./migrations")]
async fn sweep_purges_expired_jobs_and_workers(pool: PgPool) {
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
    sqlx::query!(
        "UPDATE pgqueue.jobs SET expires_at = now() - interval '1 second' WHERE id = $1",
        id
    )
    .execute(db.queue.pool())
    .await
    .unwrap();
    crate::expire_worker(&db, worker_id).await;
    sqlx::query!(
        "INSERT INTO pgqueue.cron_occurrences (queue, unique_key, scheduled_at, expires_at) VALUES ($1, 'expired-claim', now() - interval '2 seconds', now() - interval '1 second')",
        db.queue.name(),
    )
    .execute(db.queue.pool())
    .await
    .unwrap();

    let mut sweeper = db.queue.sweeper();
    let report = sweeper.sweep().await.unwrap();
    assert!(report.leader);
    assert_eq!(report.purged_jobs, 1);
    assert!(report.swept.is_empty());
    assert!(
        db.queue.job(id).await.unwrap().is_none(),
        "expired row purged"
    );
    assert!(db.queue.info().await.unwrap().workers.is_empty());
    let claim_exists = sqlx::query_scalar!(
        "SELECT EXISTS (SELECT 1 FROM pgqueue.cron_occurrences WHERE queue = $1 AND unique_key = 'expired-claim') AS \"exists!\"",
        db.queue.name(),
    )
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
async fn sweep_bounds_each_purge_batch_and_reports_more_work(pool: PgPool) {
    let db = TestDb::with(pool.clone(), |builder| builder.sweep_batch_size(2)).await;
    sqlx::query!(
        r#"
        INSERT INTO pgqueue.jobs (
            queue, name, payload, status, completed_at, expires_at
        )
        SELECT $1, 'expired-batch', 'null'::jsonb, 'complete', now(),
               now() - interval '1 second'
        FROM generate_series(1, 5)
        "#,
        db.queue.name(),
    )
    .execute(&pool)
    .await
    .unwrap();

    let mut sweeper = db.queue.sweeper();
    let first = sweeper.sweep().await.unwrap();
    assert_eq!(first.purged_jobs, 2);
    assert!(first.more_work);
    let second = sweeper.sweep().await.unwrap();
    assert_eq!(second.purged_jobs, 2);
    assert!(second.more_work);
    let third = sweeper.sweep().await.unwrap();
    assert_eq!(third.purged_jobs, 1);
    assert!(!third.more_work);
    sweeper.release().await;

    assert_eq!(
        sqlx::query_scalar!(
            r#"SELECT count(*) AS "count!" FROM pgqueue.jobs WHERE queue = $1 AND name = 'expired-batch'"#,
            db.queue.name(),
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn sweep_marks_every_stuck_running_job_in_one_pass(pool: PgPool) {
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
    sqlx::query!("UPDATE pgqueue.jobs SET started_at = now() - interval '100 milliseconds'")
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
        let row = db.queue.job(id).await.unwrap().unwrap();
        assert_eq!(row.status, JobStatus::Aborting);
        assert_eq!(row.error.as_deref(), Some("swept"));
    }
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn sweep_retries_stuck_retryable_jobs(pool: PgPool) {
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

    sqlx::query!(
        "UPDATE pgqueue.jobs SET started_at = now() - interval '100 milliseconds' \
         WHERE id = $1",
        id
    )
    .execute(db.queue.pool())
    .await
    .unwrap();

    let mut sweeper = db.queue.sweeper();
    // Phase 1: the stuck job is asked to abort (its worker may still be
    // running it), never yanked straight back to 'queued'.
    let report = sweeper.sweep().await.unwrap();
    assert_eq!(report.cancelling, vec![id]);
    assert!(report.swept.is_empty());
    let row = db.queue.job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Aborting);
    assert_eq!(row.error.as_deref(), Some("swept"));

    // Phase 2 (next sweep, nobody reacted): requeued for retry.
    let report = sweeper.sweep().await.unwrap();
    assert_eq!(report.swept, vec![id]);
    assert_eq!(db.queue.stats().retried, 1);
    let row = db.queue.job(id).await.unwrap().unwrap();
    assert_eq!(
        row.status,
        JobStatus::Queued,
        "retryable stuck job requeued"
    );
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn public_finish_succeeds_through_the_sweeper_grace_window(pool: PgPool) {
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
        db.queue.job(id).await.unwrap().unwrap().status,
        JobStatus::Aborting
    );

    // The low-level consumer was slow, not dead: its successful outcome must
    // land through the same grace window worker-processed jobs get, instead
    // of being discarded and the job running twice.
    assert!(
        db.queue
            .finish(&active, JobStatus::Complete, Some(json!("done")), None)
            .await
            .unwrap(),
        "a swept-but-alive attempt finishes through the grace window"
    );
    let row = db.queue.job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Complete);
    assert_eq!(row.result, Some(json!("done")));
    assert!(
        sweeper.sweep().await.unwrap().swept.is_empty(),
        "nothing left to recover"
    );
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn sweep_waits_for_the_live_owner_before_requeueing(pool: PgPool) {
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
        db.queue.job(id).await.unwrap().unwrap().status,
        JobStatus::Aborting
    );

    db.queue
        .write_worker_info(worker, json!({}), None, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(sweeper.sweep().await.unwrap().swept, vec![id]);
    assert_eq!(
        db.queue.job(id).await.unwrap().unwrap().status,
        JobStatus::Queued
    );
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn sweep_keeps_exclusive_stuck_job_owned_until_worker_lease_expires(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(new_job("stuck", |job| {
            job.group_key = Some("serial".into());
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
        db.queue.job(id).await.unwrap().unwrap().status,
        JobStatus::Aborting
    );

    db.queue
        .write_worker_info(worker, json!({}), None, Duration::ZERO)
        .await
        .unwrap();
    let report = sweeper.sweep().await.unwrap();
    assert_eq!(report.swept, vec![id]);
    let row = db.queue.job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Aborted);
    assert_eq!(row.error.as_deref(), Some("swept"));
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn sweep_batch_skips_live_unique_blocker_and_recovers_unkeyed_job(pool: PgPool) {
    let db = TestDb::with(pool.clone(), |builder| builder.sweep_batch_size(1)).await;
    let exclusive = db
        .queue
        .enqueue_raw(new_job("exclusive", |job| {
            job.unique_key = Some("singleton".into());
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
    sqlx::query!(
        "UPDATE pgqueue.jobs SET started_at = now() - CASE WHEN id = $1 THEN interval '2 seconds' ELSE interval '1 second' END, touched_at = now() - CASE WHEN id = $1 THEN interval '2 seconds' ELSE interval '1 second' END WHERE id IN ($1, $2)",
        exclusive,
        unkeyed,
    )
    .execute(db.queue.pool())
    .await
    .unwrap();

    let mut sweeper = db.queue.sweeper();
    assert_eq!(sweeper.sweep().await.unwrap().cancelling, vec![exclusive]);
    assert_eq!(sweeper.sweep().await.unwrap().cancelling, vec![unkeyed]);
    assert_eq!(sweeper.sweep().await.unwrap().swept, vec![unkeyed]);
    assert_eq!(
        db.queue.job(exclusive).await.unwrap().unwrap().status,
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
async fn sweep_catches_missed_heartbeats_but_not_fresh_ones(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(with_config("hb", |c| {
            c.max_attempts = 2;
            c.timeout = None;
            c.heartbeat = Some(Duration::from_secs(60 * 60));
        }))
        .await
        .unwrap()
        .unwrap();
    db.queue.dequeue(1, Uuid::now_v7()).await.unwrap();

    let mut sweeper = db.queue.sweeper();
    // Fresh heartbeat: not stuck.
    db.queue.touch(id).await.unwrap();
    let report = sweeper.sweep().await.unwrap();
    assert!(report.swept.is_empty());

    // Backdate with the database clock instead of sleeping near the threshold;
    // the test remains deterministic even when the parallel suite is loaded.
    sqlx::query!(
        "UPDATE pgqueue.jobs SET touched_at = now() - interval '2 hours' WHERE id = $1",
        id
    )
    .execute(db.queue.pool())
    .await
    .unwrap();
    let report = sweeper.sweep().await.unwrap();
    assert_eq!(report.cancelling, vec![id]);
    let report = sweeper.sweep().await.unwrap();
    assert_eq!(report.swept, vec![id]);
    let row = db.queue.job(id).await.unwrap().unwrap();
    assert_eq!(
        row.status,
        JobStatus::Queued,
        "retryable heartbeat-stuck job requeued"
    );
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn sweep_does_not_abort_an_attempt_refreshed_while_transition_waits(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(with_config("heartbeat-race", |config| {
            config.max_attempts = 2;
            config.timeout = None;
            config.heartbeat = Some(Duration::from_secs(1));
        }))
        .await
        .unwrap()
        .unwrap();
    db.queue.dequeue(1, Uuid::now_v7()).await.unwrap();
    sqlx::query!(
        "UPDATE pgqueue.jobs SET touched_at = now() - interval '2 hours' WHERE id = $1",
        id
    )
    .execute(db.queue.pool())
    .await
    .unwrap();

    let mut lock = db.queue.pool().begin().await.unwrap();
    sqlx::query!("SELECT id FROM pgqueue.jobs WHERE id = $1 FOR UPDATE", id)
        .fetch_one(&mut *lock)
        .await
        .unwrap();

    let queue = db.queue.clone();
    let sweep = tokio::spawn(async move {
        let mut sweeper = queue.sweeper();
        let report = sweeper.sweep().await.unwrap();
        sweeper.release().await;
        report
    });
    crate::wait_for_lock_waiter(
        &db,
        "%UPDATE pgqueue.jobs AS j%SET status = 'aborting'%",
        "sweep transition did not wait for the row lock",
    )
    .await;

    sqlx::query!(
        "UPDATE pgqueue.jobs SET touched_at = now() WHERE id = $1",
        id
    )
    .execute(&mut *lock)
    .await
    .unwrap();
    lock.commit().await.unwrap();

    let report = sweep.await.unwrap();
    assert!(report.cancelling.is_empty());
    assert_eq!(
        db.queue.job(id).await.unwrap().unwrap().status,
        JobStatus::Running
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn sweep_grace_applies_to_heartbeat_deadlines(pool: PgPool) {
    let db = TestDb::with(pool.clone(), |builder| {
        builder.sweep_grace(Duration::from_secs(30))
    })
    .await;
    let id = db
        .queue
        .enqueue_raw(with_config("heartbeat-grace", |config| {
            config.max_attempts = 2;
            config.timeout = None;
            config.heartbeat = Some(Duration::from_secs(1));
        }))
        .await
        .unwrap()
        .unwrap();
    db.queue.dequeue(1, Uuid::now_v7()).await.unwrap();
    sqlx::query!(
        "UPDATE pgqueue.jobs SET touched_at = now() - interval '5 seconds' \
         WHERE id = $1",
        id
    )
    .execute(db.queue.pool())
    .await
    .unwrap();

    let mut sweeper = db.queue.sweeper();
    let report = sweeper.sweep().await.unwrap();
    assert!(
        report.cancelling.is_empty(),
        "heartbeat is still within grace"
    );

    sqlx::query!(
        "UPDATE pgqueue.jobs SET touched_at = now() - interval '60 seconds' \
         WHERE id = $1",
        id
    )
    .execute(db.queue.pool())
    .await
    .unwrap();
    let report = sweeper.sweep().await.unwrap();
    assert_eq!(report.cancelling, vec![id]);
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn user_abort_reason_cannot_forge_the_sweeper_marker(pool: PgPool) {
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
    db.queue.abort(id, "swept").await.unwrap();
    backdate_job_liveness(&db, id).await;

    let mut sweeper = db.queue.sweeper();
    let report = sweeper.sweep().await.unwrap();
    assert_eq!(report.swept, vec![id]);
    let row = db.queue.job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Aborted);
    assert_eq!(row.error.as_deref(), Some("swept"));
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn sweep_leadership_is_exclusive_per_queue(pool: PgPool) {
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
async fn sweep_leadership_recovers_after_its_backend_is_terminated(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let other = db.another_queue(|builder| builder).await;
    let mut stale = db.queue.sweeper();
    assert!(stale.sweep().await.unwrap().leader);

    let key = pgqueue::__private::sweep_lock_key(&db.database, db.queue.name()) as u64;
    let class_id = (key >> 32) as u32 as i64;
    let object_id = key as u32 as i64;
    let pid = sqlx::query_scalar!(
        "SELECT pid AS \"pid!\" FROM pg_locks WHERE locktype = 'advisory' AND classid::bigint = $1 AND objid::bigint = $2 AND objsubid = 1 AND granted",
        class_id,
        object_id
    )
    .fetch_one(db.queue.pool())
    .await
    .unwrap();
    assert!(
        sqlx::query_scalar!("SELECT pg_terminate_backend($1)", pid)
            .fetch_one(db.queue.pool())
            .await
            .unwrap()
            .unwrap_or(false)
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
async fn distinct_queue_names_do_not_share_sweep_leadership(pool: PgPool) {
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
async fn queues_are_isolated_by_name(pool: PgPool) {
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
    assert!(other.job(id).await.unwrap().is_none());
    assert!(!other.abort(id, "cross-queue").await.unwrap());
    assert!(!other.retry_job(id, "cross-queue").await.unwrap());
    assert!(matches!(other.touch(id).await, Err(Error::JobNotFound(_))));

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
        db.queue.job(id).await.unwrap().unwrap().status,
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
async fn builder_accepts_external_pool(pool: PgPool) {
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
async fn dropping_a_sweeper_releases_leadership(pool: PgPool) {
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
async fn queue_accessors_and_debug_reflect_configuration(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    assert_eq!(db.queue.name(), "default");
    let debug = format!("{:?}", db.queue);
    assert!(debug.contains("Queue"));
    assert!(debug.contains("default"));
}

#[sqlx::test(migrations = "./migrations")]
async fn stale_attempts_cannot_finalize_newer_ones(pool: PgPool) {
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

    // Worker A wakes up: its stale attempt must not touch the row.
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
    let row = db.queue.job(attempt_a.id).await.unwrap().unwrap();
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
async fn failed_attempt_of_an_aborting_job_honors_the_abort(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(with_config("ab", |c| c.max_attempts = 5))
        .await
        .unwrap()
        .unwrap();
    let active = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);
    assert!(db.queue.abort(id, "user said stop").await.unwrap());

    // The attempt fails while the abort is pending: retry must refuse...
    assert!(!db.queue.retry(&active, "failed: transient").await.unwrap());
    // ...and finishing as aborted (error: None) preserves the abort reason.
    assert!(
        db.queue
            .finish(&active, JobStatus::Aborted, None, None)
            .await
            .unwrap()
    );
    let row = db.queue.job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Aborted);
    assert_eq!(row.error.as_deref(), Some("user said stop"));
}

#[sqlx::test(migrations = "./migrations")]
async fn group_stays_exclusive_while_a_member_is_aborting(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    for _ in 0..2 {
        db.queue
            .enqueue_raw(new_job("g", |job| job.group_key = Some("serial".into())))
            .await
            .unwrap();
    }
    let first = db
        .queue
        .dequeue(10, Uuid::now_v7())
        .await
        .unwrap()
        .remove(0);
    assert!(db.queue.abort(first.id, "stop").await.unwrap());
    let row = db.queue.job(first.id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Aborting);

    // Still executing until its worker cancels it: the group stays closed.
    assert!(
        db.queue
            .dequeue(10, Uuid::now_v7())
            .await
            .unwrap()
            .is_empty(),
        "an aborting member still blocks its group"
    );

    // Once truly finished, the group opens.
    db.queue
        .finish(&first, JobStatus::Aborted, None, None)
        .await
        .unwrap();
    assert_eq!(db.queue.dequeue(10, Uuid::now_v7()).await.unwrap().len(), 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn dead_workers_unbounded_jobs_are_recovered(pool: PgPool) {
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
    let row = db.queue.job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Queued, "recovered for retry");
    sweeper.release().await;
}

//noinspection SqlNoDataSourceInspection
#[sqlx::test(migrations = "./migrations")]
async fn leaseless_unbounded_jobs_stay_alive_within_the_sweep_grace(pool: PgPool) {
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

    let backdate_touch = |milliseconds: i64| {
        let pool = db.queue.pool().clone();
        async move {
            sqlx::query!(
                "UPDATE pgqueue.jobs SET touched_at = now() - $2::bigint * interval '1 millisecond' WHERE id = $1",
                id,
                milliseconds
            )
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

    // Touching the job keeps a leaseless consumer alive across sweeps.
    backdate_touch(60_000).await;
    db.queue.touch(id).await.unwrap();
    assert!(sweeper.sweep().await.unwrap().cancelling.is_empty());

    // Untouched past the grace with no worker lease: recovered as before.
    backdate_touch(60_000).await;
    assert_eq!(sweeper.sweep().await.unwrap().cancelling, vec![id]);
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn another_queues_worker_cannot_keep_an_unbounded_job_alive(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let other = db.another_queue(|builder| builder.name("other")).await;
    db.queue
        .enqueue_raw(with_config("unbounded", |config| {
            config.max_attempts = 2;
            config.timeout = None;
            config.heartbeat = None;
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
async fn live_workers_unbounded_jobs_are_not_swept(pool: PgPool) {
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
async fn zero_delay_retries_keep_their_queue_position(pool: PgPool) {
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
    let original = db.queue.job(id).await.unwrap().unwrap().scheduled_at;

    let active = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap().remove(0);
    assert!(db.queue.retry(&active, "failed: transient").await.unwrap());
    let row = db.queue.job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Queued);
    assert_eq!(
        row.scheduled_at, original,
        "zero-delay retry must not lose its place behind the backlog"
    );
}
