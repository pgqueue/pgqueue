//! Durable cron registry, revision, misfire, and publication integration tests.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use pgqueue::{
    CronMisfirePolicy, CronOptions, Error, JobFilter, JobState, JobStatus, JobType, Worker,
    WorkerComponent, WorkerHealthStatus, WorkerTimers,
};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use crate::{EnqueueOutcomeTestExt, TestDb, wait_for_some, wait_until};

#[pgqueue::cron("* * * * * *", ttl_ms = 3_600_000, revision = 7)]
async fn tick(counter: JobState<Arc<AtomicU32>>) -> anyhow::Result<u32> {
    Ok(counter.0.fetch_add(1, Ordering::SeqCst) + 1)
}

#[pgqueue::cron("0 0 1 1 *")]
async fn yearly(counter: JobState<Arc<AtomicU32>>) -> anyhow::Result<u32> {
    Ok(counter.0.fetch_add(1, Ordering::SeqCst) + 1)
}

#[pgqueue::job(ttl_ms = 3_600_000)]
async fn dynamic_tick(_: (), counter: JobState<Arc<AtomicU32>>) -> anyhow::Result<u32> {
    Ok(counter.0.fetch_add(1, Ordering::SeqCst) + 1)
}

fn timers() -> WorkerTimers {
    WorkerTimers {
        abort: Duration::from_millis(50),
        schedule: Duration::from_millis(40),
        sweep: Duration::from_secs(60),
        worker_info: Duration::from_millis(100),
    }
}

fn dynamic_worker(
    queue: pgqueue::Queue,
    expression: &str,
    unique_key: &str,
    options: CronOptions,
    counter: Arc<AtomicU32>,
) -> Worker {
    Worker::builder(queue)
        .register(dynamic_tick)
        .cron_with_options(
            expression,
            dynamic_tick::job(()).unique_key(unique_key),
            options,
        )
        .state(counter)
        .timers(timers())
        .poll_interval(Duration::from_millis(20))
        .concurrency(2)
        .build()
        .unwrap()
}

fn skip_options(revision: u64, grace: Duration) -> CronOptions {
    CronOptions {
        revision,
        misfire: CronMisfirePolicy::Skip { grace: Some(grace) },
    }
}

async fn schedule_cursor(
    pool: &PgPool,
    queue: &str,
    unique_key: &str,
) -> Option<chrono::DateTime<chrono::Utc>> {
    sqlx::query_scalar!(
        "SELECT next_run_at FROM pgqueue.cron_schedules WHERE queue = $1 AND unique_key = $2",
        queue,
        unique_key,
    )
    .fetch_optional(pool)
    .await
    .unwrap()
}

async fn wait_for_schedule(
    pool: &PgPool,
    queue: &str,
    unique_key: &str,
) -> chrono::DateTime<chrono::Utc> {
    wait_for_some(
        Duration::from_secs(5),
        Duration::from_millis(10),
        "cron schedule was not reconciled",
        || schedule_cursor(pool, queue, unique_key),
    )
    .await
}

async fn stop_worker(shutdown: CancellationToken, run: tokio::task::JoinHandle<Result<(), Error>>) {
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("worker did not stop")
        .unwrap()
        .unwrap();
}

async fn register_dynamic_schedule(
    db: &TestDb,
    expression: &str,
    unique_key: &str,
    options: CronOptions,
    counter: Arc<AtomicU32>,
) {
    let worker = dynamic_worker(db.queue.clone(), expression, unique_key, options, counter);
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    wait_for_schedule(&db.pool, db.queue.name(), unique_key).await;
    stop_worker(shutdown, run).await;
}

async fn just_missed_schedule(
    pool: &PgPool,
    seconds_ago: i64,
) -> (String, chrono::DateTime<chrono::Utc>) {
    let now = sqlx::query_scalar!(r#"SELECT now() AS "now!""#)
        .fetch_one(pool)
        .await
        .unwrap();
    let occurrence =
        chrono::SubsecRound::trunc_subsecs(now - chrono::Duration::seconds(seconds_ago), 0);
    (
        format!("{} * * * * *", chrono::Timelike::second(&occurrence)),
        occurrence,
    )
}

async fn upcoming_schedule(
    pool: &PgPool,
    seconds_ahead: i64,
) -> (String, chrono::DateTime<chrono::Utc>) {
    let now = sqlx::query_scalar!(r#"SELECT now() AS "now!""#)
        .fetch_one(pool)
        .await
        .unwrap();
    let occurrence =
        chrono::SubsecRound::trunc_subsecs(now + chrono::Duration::seconds(seconds_ahead), 0);
    (
        format!("{} * * * * *", chrono::Timelike::second(&occurrence)),
        occurrence,
    )
}

#[test]
fn cron_attribute_exposes_schedule_and_revision() {
    assert_eq!(tick::SCHEDULE, Some("* * * * * *"));
    assert_eq!(tick::CRON_REVISION, 7);
    assert_eq!(tick::NAME, "tick");
}

#[sqlx::test(migrations = "./migrations")]
async fn cron_publishes_each_occurrence_once_across_workers(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let worker_a = Worker::builder(db.queue.clone())
        .register(tick)
        .state(counter.clone())
        .timers(timers())
        .poll_interval(Duration::from_millis(20))
        .build()
        .unwrap();
    let worker_b = Worker::builder(db.another_queue(|builder| builder).await)
        .register(tick)
        .state(counter.clone())
        .timers(timers())
        .poll_interval(Duration::from_millis(20))
        .build()
        .unwrap();
    let shutdown = CancellationToken::new();
    let run_a = tokio::spawn(worker_a.run_until(shutdown.clone()));
    let run_b = tokio::spawn(worker_b.run_until(shutdown.clone()));

    wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        "cron occurrences did not run",
        || async { counter.load(Ordering::SeqCst) >= 2 },
    )
    .await;
    shutdown.cancel();
    run_a.await.unwrap().unwrap();
    run_b.await.unwrap().unwrap();

    let fired = counter.load(Ordering::SeqCst);
    let completed = sqlx::query_scalar!(
        r#"SELECT count(*) AS "count!" FROM pgqueue.jobs WHERE queue = $1 AND name = 'tick' AND kind = 'cron' AND status = 'complete'"#,
        db.queue.name(),
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(completed, i64::from(fired));
}

#[sqlx::test(migrations = "./migrations")]
async fn cron_job_can_run_as_a_keyless_one_off(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let handle = db.queue.enqueue(yearly::job()).await.unwrap().unwrap();
    let worker = Worker::builder(db.queue.clone())
        .register(yearly)
        .state(counter.clone())
        .timers(timers())
        .poll_interval(Duration::from_millis(20))
        .burst(true)
        .dequeue_timeout(Duration::from_millis(200))
        .build()
        .unwrap();
    worker.run_until(CancellationToken::new()).await.unwrap();

    assert_eq!(handle.wait(Some(Duration::from_secs(2))).await.unwrap(), 1);
    assert!(handle.refresh().await.unwrap().unique_key.is_none());
}

#[sqlx::test(migrations = "./migrations")]
async fn cron_registry_does_not_speculatively_enqueue_future_jobs(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let worker = Worker::builder(db.queue.clone())
        .register(yearly)
        .state(Arc::new(AtomicU32::new(0)))
        .timers(timers())
        .build()
        .unwrap();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    let next = wait_for_schedule(&pool, db.queue.name(), "cron:yearly").await;
    let now = sqlx::query_scalar!(r#"SELECT now() AS "now!""#)
        .fetch_one(&pool)
        .await
        .unwrap();

    assert!(next > now);
    assert_eq!(db.queue.counts().await.unwrap().scheduled, 0);
    assert_eq!(
        sqlx::query_scalar!(
            r#"SELECT count(*) AS "count!" FROM pgqueue.jobs WHERE queue = $1 AND kind = 'cron'"#,
            db.queue.name(),
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    stop_worker(shutdown, run).await;
}

#[sqlx::test(migrations = "./migrations")]
async fn worker_builder_cron_runs_a_dynamic_schedule(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let worker = dynamic_worker(
        db.queue.clone(),
        "* * * * * *",
        "dynamic",
        CronOptions::default(),
        counter.clone(),
    );
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    wait_until(
        Duration::from_secs(3),
        Duration::from_millis(10),
        "dynamic cron did not run",
        || async { counter.load(Ordering::SeqCst) >= 1 },
    )
    .await;
    stop_worker(shutdown, run).await;
}

#[sqlx::test(migrations = "./migrations")]
async fn cron_skip_publishes_a_durable_cursor_within_grace(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let (expression, missed) = just_missed_schedule(&pool, 2).await;
    let options = skip_options(1, Duration::from_secs(10));
    register_dynamic_schedule(&db, &expression, "within-grace", options, counter.clone()).await;
    sqlx::query!(
        "UPDATE pgqueue.cron_schedules SET next_run_at = $3 WHERE queue = $1 AND unique_key = $2",
        db.queue.name(),
        "within-grace",
        missed,
    )
    .execute(&pool)
    .await
    .unwrap();

    let worker = dynamic_worker(
        db.queue.clone(),
        &expression,
        "within-grace",
        options,
        counter.clone(),
    );
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    wait_until(
        Duration::from_secs(3),
        Duration::from_millis(10),
        "durable occurrence was not published",
        || async { counter.load(Ordering::SeqCst) == 1 },
    )
    .await;
    stop_worker(shutdown, run).await;

    let scheduled_at = sqlx::query_scalar!(
        "SELECT scheduled_at FROM pgqueue.jobs WHERE queue = $1 AND unique_key = $2 AND kind = 'cron'",
        db.queue.name(),
        "within-grace",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(scheduled_at, missed);
}

#[sqlx::test(migrations = "./migrations")]
async fn cron_skip_advances_a_stale_cursor_without_publishing(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let (expression, missed) = just_missed_schedule(&pool, 3).await;
    let options = skip_options(1, Duration::from_secs(1));
    register_dynamic_schedule(&db, &expression, "stale-skip", options, counter.clone()).await;
    sqlx::query!(
        "UPDATE pgqueue.cron_schedules SET next_run_at = $3 WHERE queue = $1 AND unique_key = $2",
        db.queue.name(),
        "stale-skip",
        missed,
    )
    .execute(&pool)
    .await
    .unwrap();

    let worker = dynamic_worker(
        db.queue.clone(),
        &expression,
        "stale-skip",
        options,
        counter.clone(),
    );
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    wait_until(
        Duration::from_secs(3),
        Duration::from_millis(10),
        "stale cursor was not advanced",
        || async {
            schedule_cursor(&pool, db.queue.name(), "stale-skip")
                .await
                .is_some_and(|cursor| cursor > missed)
        },
    )
    .await;
    stop_worker(shutdown, run).await;

    assert_eq!(counter.load(Ordering::SeqCst), 0);
    assert_eq!(
        sqlx::query_scalar!(
            r#"SELECT count(*) AS "count!" FROM pgqueue.jobs WHERE queue = $1 AND unique_key = $2 AND kind = 'cron'"#,
            db.queue.name(),
            "stale-skip",
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar!(
            r#"SELECT count(*) AS "count!" FROM pgqueue.cron_occurrences WHERE queue = $1 AND unique_key = $2 AND scheduled_at = $3"#,
            db.queue.name(),
            "stale-skip",
            missed,
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn cron_fire_once_publishes_only_the_latest_missed_occurrence(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let (expression, latest) = just_missed_schedule(&pool, 2).await;
    let options = CronOptions {
        revision: 1,
        misfire: CronMisfirePolicy::FireOnce,
    };
    register_dynamic_schedule(&db, &expression, "fire-once", options, counter.clone()).await;
    let old_cursor = latest - chrono::Duration::hours(2);
    sqlx::query!(
        "UPDATE pgqueue.cron_schedules SET next_run_at = $3 WHERE queue = $1 AND unique_key = $2",
        db.queue.name(),
        "fire-once",
        old_cursor,
    )
    .execute(&pool)
    .await
    .unwrap();

    let worker = dynamic_worker(
        db.queue.clone(),
        &expression,
        "fire-once",
        options,
        counter.clone(),
    );
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    wait_until(
        Duration::from_secs(3),
        Duration::from_millis(10),
        "fire-once occurrence was not published",
        || async { counter.load(Ordering::SeqCst) == 1 },
    )
    .await;
    stop_worker(shutdown, run).await;

    let rows = db
        .queue
        .jobs_page(JobFilter {
            name: Some("dynamic_tick".into()),
            ..JobFilter::default()
        })
        .await
        .unwrap();
    let occurrences = rows
        .iter()
        .filter(|row| row.unique_key.as_deref() == Some("fire-once"))
        .collect::<Vec<_>>();
    assert_eq!(occurrences.len(), 1);
    assert_eq!(occurrences[0].scheduled_at, latest);
}

#[sqlx::test(migrations = "./migrations")]
async fn cron_template_revision_preserves_a_due_fire_once_cursor(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let (expression, latest) = just_missed_schedule(&pool, 2).await;
    let unique_key = "template-revision-fire-once";
    let initial_options = CronOptions {
        revision: 1,
        misfire: CronMisfirePolicy::FireOnce,
    };
    let initial = Worker::builder(db.queue.clone())
        .register(dynamic_tick)
        .cron_with_options(
            &expression,
            dynamic_tick::job(())
                .unique_key(unique_key)
                .meta(serde_json::json!({ "template": 1 })),
            initial_options,
        )
        .state(counter.clone())
        .timers(timers())
        .poll_interval(Duration::from_millis(20))
        .build()
        .unwrap();
    let initial_shutdown = CancellationToken::new();
    let initial_run = tokio::spawn(initial.run_until(initial_shutdown.clone()));
    wait_for_schedule(&pool, db.queue.name(), unique_key).await;
    stop_worker(initial_shutdown, initial_run).await;

    sqlx::query!(
        "UPDATE pgqueue.cron_schedules SET next_run_at = $3 WHERE queue = $1 AND unique_key = $2",
        db.queue.name(),
        unique_key,
        latest,
    )
    .execute(&pool)
    .await
    .unwrap();

    let revised = Worker::builder(db.queue.clone())
        .register(dynamic_tick)
        .cron_with_options(
            &expression,
            dynamic_tick::job(())
                .unique_key(unique_key)
                .meta(serde_json::json!({ "template": 2 })),
            CronOptions {
                revision: 2,
                misfire: CronMisfirePolicy::FireOnce,
            },
        )
        .state(counter.clone())
        .timers(timers())
        .poll_interval(Duration::from_millis(20))
        .build()
        .unwrap();
    let revised_shutdown = CancellationToken::new();
    let revised_run = tokio::spawn(revised.run_until(revised_shutdown.clone()));
    wait_until(
        Duration::from_secs(3),
        Duration::from_millis(10),
        "due occurrence was lost during a template-only revision",
        || async { counter.load(Ordering::SeqCst) == 1 },
    )
    .await;
    stop_worker(revised_shutdown, revised_run).await;

    let row = sqlx::query!(
        "SELECT scheduled_at, meta FROM pgqueue.jobs WHERE queue = $1 AND unique_key = $2 AND kind = 'cron'",
        db.queue.name(),
        unique_key,
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.scheduled_at, latest);
    assert_eq!(row.meta, serde_json::json!({ "template": 2 }));
}

#[sqlx::test(migrations = "./migrations")]
async fn cron_equal_revision_rejects_a_different_definition(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let first = dynamic_worker(
        db.queue.clone(),
        "0 * * * * *",
        "revision-conflict",
        skip_options(4, Duration::from_secs(1)),
        Arc::new(AtomicU32::new(0)),
    );
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(first.run_until(shutdown.clone()));
    wait_for_schedule(&pool, db.queue.name(), "revision-conflict").await;

    let conflicting = dynamic_worker(
        db.another_queue(|builder| builder).await,
        "30 * * * * *",
        "revision-conflict",
        skip_options(4, Duration::from_secs(1)),
        Arc::new(AtomicU32::new(0)),
    );
    let error = tokio::time::timeout(
        Duration::from_secs(3),
        conflicting.run_until(CancellationToken::new()),
    )
    .await
    .expect("conflicting worker did not fail startup")
    .unwrap_err();
    assert!(matches!(error, Error::Config(_)), "{error}");
    stop_worker(shutdown, run).await;
}

#[sqlx::test(migrations = "./migrations")]
async fn cron_higher_revision_takes_authority_and_degrades_lower_workers(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let lower = dynamic_worker(
        db.queue.clone(),
        "0 * * * * *",
        "revision-takeover",
        skip_options(1, Duration::from_secs(1)),
        Arc::new(AtomicU32::new(0)),
    );
    let lower_health = lower.health();
    let lower_shutdown = CancellationToken::new();
    let lower_run = tokio::spawn(lower.run_until(lower_shutdown.clone()));
    wait_for_schedule(&pool, db.queue.name(), "revision-takeover").await;

    let higher = dynamic_worker(
        db.another_queue(|builder| builder).await,
        "30 * * * * *",
        "revision-takeover",
        skip_options(2, Duration::from_secs(1)),
        Arc::new(AtomicU32::new(0)),
    );
    let higher_shutdown = CancellationToken::new();
    let higher_run = tokio::spawn(higher.run_until(higher_shutdown.clone()));
    wait_until(
        Duration::from_secs(3),
        Duration::from_millis(10),
        "higher cron revision did not take authority",
        || async {
            sqlx::query_scalar!(
                "SELECT revision FROM pgqueue.cron_schedules WHERE queue = $1 AND unique_key = $2",
                db.queue.name(),
                "revision-takeover",
            )
            .fetch_optional(&pool)
            .await
            .unwrap()
                == Some(2)
        },
    )
    .await;
    let revised_cursor = schedule_cursor(&pool, db.queue.name(), "revision-takeover")
        .await
        .unwrap();
    assert_eq!(
        chrono::Timelike::second(&revised_cursor),
        30,
        "changing the expression did not reset the durable cursor"
    );
    wait_until(
        Duration::from_secs(3),
        Duration::from_millis(10),
        "lower revision worker did not report degraded health",
        || async {
            let snapshot = lower_health.snapshot();
            snapshot.status == WorkerHealthStatus::Degraded
                && snapshot
                    .failures
                    .iter()
                    .any(|failure| failure.component == WorkerComponent::Scheduler)
        },
    )
    .await;

    stop_worker(lower_shutdown, lower_run).await;
    stop_worker(higher_shutdown, higher_run).await;
}

//noinspection SqlNoDataSourceInspection
#[sqlx::test(migrations = "./migrations")]
async fn cron_cursor_claim_and_job_insert_roll_back_together(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let (expression, missed) = just_missed_schedule(&pool, 1).await;
    let options = skip_options(1, Duration::from_secs(20));
    register_dynamic_schedule(
        &db,
        &expression,
        "atomic-publication",
        options,
        counter.clone(),
    )
    .await;
    sqlx::query!(
        "UPDATE pgqueue.cron_schedules SET next_run_at = $3 WHERE queue = $1 AND unique_key = $2",
        db.queue.name(),
        "atomic-publication",
        missed,
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query!(
        "ALTER TABLE pgqueue.jobs ADD CONSTRAINT reject_cron_insert_for_test CHECK (kind <> 'cron') NOT VALID"
    )
    .execute(&pool)
    .await
    .unwrap();

    let worker = dynamic_worker(
        db.queue.clone(),
        &expression,
        "atomic-publication",
        options,
        counter.clone(),
    );
    let health = worker.health();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    wait_until(
        Duration::from_secs(3),
        Duration::from_millis(10),
        "scheduler insert failure was not reported",
        || async { health.snapshot().status == WorkerHealthStatus::Degraded },
    )
    .await;
    assert_eq!(
        schedule_cursor(&pool, db.queue.name(), "atomic-publication").await,
        Some(missed)
    );
    assert_eq!(
        sqlx::query_scalar!(
            r#"SELECT count(*) AS "count!" FROM pgqueue.cron_occurrences WHERE queue = $1 AND unique_key = $2"#,
            db.queue.name(),
            "atomic-publication",
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );

    sqlx::query!("ALTER TABLE pgqueue.jobs DROP CONSTRAINT reject_cron_insert_for_test")
        .execute(&pool)
        .await
        .unwrap();
    wait_until(
        Duration::from_secs(3),
        Duration::from_millis(10),
        "rolled-back occurrence was not retried",
        || async { counter.load(Ordering::SeqCst) == 1 },
    )
    .await;
    wait_until(
        Duration::from_secs(3),
        Duration::from_millis(10),
        "scheduler health did not recover after publication succeeded",
        || async { health.snapshot().status == WorkerHealthStatus::Ready },
    )
    .await;
    stop_worker(shutdown, run).await;
    assert_eq!(
        sqlx::query_scalar!(
            r#"SELECT count(*) AS "count!" FROM pgqueue.cron_occurrences WHERE queue = $1 AND unique_key = $2 AND scheduled_at = $3"#,
            db.queue.name(),
            "atomic-publication",
            missed,
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn cron_foreign_live_holder_claims_and_skips_the_occurrence(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let (expression, missed) = just_missed_schedule(&pool, 2).await;
    let options = skip_options(1, Duration::from_secs(10));
    register_dynamic_schedule(&db, &expression, "foreign-holder", options, counter.clone()).await;
    let owner = db
        .queue
        .enqueue(
            yearly::job()
                .unique_key("foreign-holder")
                .delay(Duration::from_secs(60)),
        )
        .await
        .unwrap()
        .unwrap();
    sqlx::query!(
        "UPDATE pgqueue.cron_schedules SET next_run_at = $3 WHERE queue = $1 AND unique_key = $2",
        db.queue.name(),
        "foreign-holder",
        missed,
    )
    .execute(&pool)
    .await
    .unwrap();

    let worker = dynamic_worker(
        db.queue.clone(),
        &expression,
        "foreign-holder",
        options,
        counter.clone(),
    );
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    wait_until(
        Duration::from_secs(3),
        Duration::from_millis(10),
        "held occurrence was not advanced",
        || async {
            schedule_cursor(&pool, db.queue.name(), "foreign-holder")
                .await
                .is_some_and(|cursor| cursor > missed)
        },
    )
    .await;

    assert_eq!(counter.load(Ordering::SeqCst), 0);
    assert_eq!(owner.refresh().await.unwrap().status, JobStatus::Queued);
    assert_eq!(
        sqlx::query_scalar!(
            r#"SELECT count(*) AS "count!" FROM pgqueue.cron_occurrences WHERE queue = $1 AND unique_key = $2 AND scheduled_at = $3"#,
            db.queue.name(),
            "foreign-holder",
            missed,
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar!(
            r#"SELECT count(*) AS "count!" FROM pgqueue.jobs WHERE queue = $1 AND unique_key = $2 AND kind = 'cron'"#,
            db.queue.name(),
            "foreign-holder",
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    stop_worker(shutdown, run).await;
}

//noinspection SqlNoDataSourceInspection
#[sqlx::test(migrations = "./migrations")]
async fn cron_rechecks_skip_grace_after_waiting_for_the_unique_lock(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let (expression, occurrence) = upcoming_schedule(&pool, 2).await;
    let options = skip_options(1, Duration::from_millis(300));
    register_dynamic_schedule(&db, &expression, "lock-wait", options, counter.clone()).await;
    sqlx::query!(
        "UPDATE pgqueue.cron_schedules SET next_run_at = $3 WHERE queue = $1 AND unique_key = $2",
        db.queue.name(),
        "lock-wait",
        occurrence,
    )
    .execute(&pool)
    .await
    .unwrap();

    let mut transaction = db.queue.pool().begin().await.unwrap();
    db.queue
        .enqueue_in(
            &mut transaction,
            dynamic_tick::job(())
                .unique_key("lock-wait")
                .delay(Duration::from_secs(60)),
        )
        .await
        .unwrap();
    let worker = dynamic_worker(
        db.queue.clone(),
        &expression,
        "lock-wait",
        options,
        counter.clone(),
    );
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    let now = sqlx::query_scalar!(r#"SELECT clock_timestamp() AS "now!""#)
        .fetch_one(&pool)
        .await
        .unwrap();
    if let Ok(until_due) = (occurrence - now).to_std() {
        tokio::time::sleep(until_due).await;
    }
    crate::wait_for_dequeue_lock_waiter(&db.queue, true).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    transaction.rollback().await.unwrap();
    wait_until(
        Duration::from_secs(3),
        Duration::from_millis(10),
        "scheduler did not advance after the unique lock was released",
        || async {
            schedule_cursor(&pool, db.queue.name(), "lock-wait")
                .await
                .is_some_and(|cursor| cursor > occurrence)
        },
    )
    .await;
    stop_worker(shutdown, run).await;

    assert_eq!(counter.load(Ordering::SeqCst), 0);
    assert_eq!(
        sqlx::query_scalar!(
            r#"SELECT count(*) AS "count!" FROM pgqueue.jobs WHERE queue = $1 AND unique_key = $2"#,
            db.queue.name(),
            "lock-wait",
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn cron_lock_wait_observes_worker_shutdown(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let (expression, _) = upcoming_schedule(&pool, 30).await;
    let options = skip_options(1, Duration::from_secs(60));
    let mut transaction = db.queue.pool().begin().await.unwrap();
    db.queue
        .enqueue_in(
            &mut transaction,
            dynamic_tick::job(())
                .unique_key("shutdown-lock-wait")
                .delay(Duration::from_secs(60)),
        )
        .await
        .unwrap();

    let worker = dynamic_worker(
        db.queue.clone(),
        &expression,
        "shutdown-lock-wait",
        options,
        counter,
    );
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    wait_for_schedule(&pool, db.queue.name(), "shutdown-lock-wait").await;
    sqlx::query!(
        "UPDATE pgqueue.cron_schedules SET next_run_at = now() WHERE queue = $1 AND unique_key = $2",
        db.queue.name(),
        "shutdown-lock-wait",
    )
    .execute(&pool)
    .await
    .unwrap();
    crate::wait_for_dequeue_lock_waiter(&db.queue, true).await;

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(3), run)
        .await
        .expect("worker did not stop while the cron lock remained held")
        .unwrap()
        .unwrap();
    transaction.rollback().await.unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn cron_reconciliation_lock_wait_observes_worker_shutdown(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let (expression, _) = upcoming_schedule(&pool, 30).await;
    let options = skip_options(1, Duration::from_secs(60));
    let mut transaction = db.queue.pool().begin().await.unwrap();
    db.queue
        .enqueue_in(
            &mut transaction,
            dynamic_tick::job(())
                .unique_key("reconcile-shutdown-lock-wait")
                .delay(Duration::from_secs(60)),
        )
        .await
        .unwrap();

    let scheduler = dynamic_worker(
        db.queue.clone(),
        &expression,
        "reconcile-shutdown-lock-wait",
        options,
        counter.clone(),
    );
    let scheduler_shutdown = CancellationToken::new();
    let scheduler_run = tokio::spawn(scheduler.run_until(scheduler_shutdown.clone()));
    wait_for_schedule(&pool, db.queue.name(), "reconcile-shutdown-lock-wait").await;
    sqlx::query!(
        "UPDATE pgqueue.cron_schedules SET next_run_at = now() WHERE queue = $1 AND unique_key = $2",
        db.queue.name(),
        "reconcile-shutdown-lock-wait",
    )
    .execute(&pool)
    .await
    .unwrap();
    crate::wait_for_dequeue_lock_waiter(&db.queue, true).await;

    let starting = dynamic_worker(
        db.queue.clone(),
        &expression,
        "reconcile-shutdown-lock-wait",
        options,
        counter,
    );
    let starting_shutdown = CancellationToken::new();
    let starting_run = tokio::spawn(starting.run_until(starting_shutdown.clone()));
    crate::wait_for_lock_waiter(
        &db,
        "%INSERT INTO pgqueue.cron_schedules%",
        "starting worker did not wait on cron reconciliation",
    )
    .await;

    starting_shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(3), starting_run)
        .await
        .expect("starting worker did not stop while cron reconciliation was locked")
        .unwrap()
        .unwrap();
    scheduler_shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(3), scheduler_run)
        .await
        .expect("scheduler did not stop while the cron key remained locked")
        .unwrap()
        .unwrap();
    transaction.rollback().await.unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn cron_builder_rejects_manual_schedule_overrides(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let error = Worker::builder(db.queue)
        .register(dynamic_tick)
        .cron(
            "* * * * * *",
            dynamic_tick::job(()).delay(Duration::from_secs(1)),
        )
        .build()
        .unwrap_err();
    assert!(error.to_string().contains("cannot use delay"), "{error}");
}
