//! Durable cron registry, revision, misfire, and publication integration tests.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use pgqueue::{
    CronDefinition, CronMisfirePolicy, CronOptions, Error, JobFilter, JobState, JobStatus, JobType,
    Worker, WorkerComponent, WorkerHealthStatus, WorkerTimers,
};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use crate::{EnqueueResultTestExt, TestDb, pool_with_max, wait_for_some, wait_until};
use crate::{Stats, test_timers};
use pgqueue::Queue;

#[pgqueue::cron("* * * * * *", result_ttl_ms = 3_600_000, revision = 7)]
async fn tick(counter: JobState<Arc<AtomicU32>>) -> anyhow::Result<u32> {
    Ok(counter.0.fetch_add(1, Ordering::SeqCst) + 1)
}

#[pgqueue::cron("0 0 1 1 *")]
async fn yearly(counter: JobState<Arc<AtomicU32>>) -> anyhow::Result<u32> {
    Ok(counter.0.fetch_add(1, Ordering::SeqCst) + 1)
}

#[pgqueue::job(result_ttl_ms = 3_600_000)]
async fn dynamic_tick(_: (), counter: JobState<Arc<AtomicU32>>) -> anyhow::Result<u32> {
    Ok(counter.0.fetch_add(1, Ordering::SeqCst) + 1)
}

fn timers() -> WorkerTimers {
    WorkerTimers {
        schedule: Duration::from_millis(40),
        ..crate::test_timers()
    }
}

fn dynamic_worker(
    queue: pgqueue::Queue,
    expression: &str,
    dedupe_key: &str,
    options: CronOptions,
    counter: Arc<AtomicU32>,
) -> Worker {
    Worker::builder(queue)
        .schedule_cron_with_options(
            expression,
            dynamic_tick::job(()).dedupe_key(dedupe_key),
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
    dedupe_key: &str,
) -> Option<chrono::DateTime<chrono::Utc>> {
    sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>(
        "SELECT next_run_at FROM pgqueue.cron_schedules WHERE queue = $1 AND dedupe_key = $2",
    )
    .bind(queue)
    .bind(dedupe_key)
    .fetch_optional(pool)
    .await
    .unwrap()
}

/// Drags a schedule's durable cursor into the past so the cron is genuinely
/// due, and answers the cursor it wrote — which is what a worker that declines
/// to schedule the cron leaves untouched.
///
/// A cursor still in the future sits still whatever the worker does, so
/// "the cursor has not moved" only says something about supersession once the
/// cursor is one an entitled worker would move.
async fn backdate_schedule(
    pool: &PgPool,
    queue: &str,
    dedupe_key: &str,
) -> chrono::DateTime<chrono::Utc> {
    sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>(
        r#"UPDATE pgqueue.cron_schedules SET next_run_at = now() - interval '5 seconds'
           WHERE queue = $1 AND dedupe_key = $2
           RETURNING next_run_at"#,
    )
    .bind(queue)
    .bind(dedupe_key)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn cron_jobs_published(pool: &PgPool, queue: &str, dedupe_key: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(
        r#"SELECT count(*) FROM pgqueue.jobs
           WHERE queue = $1 AND dedupe_key = $2 AND kind = 'cron'"#,
    )
    .bind(queue)
    .bind(dedupe_key)
    .fetch_one(pool)
    .await
    .unwrap()
}

/// Runs a worker holding the authoritative revision until it moves the cursor
/// past `due_at`. The positive half of every supersession assertion: it shows
/// the occurrence the superseded worker left alone was there to be taken.
async fn assert_authority_advances(
    db: &TestDb,
    expression: &str,
    dedupe_key: &str,
    options: CronOptions,
    due_at: chrono::DateTime<chrono::Utc>,
) {
    let worker = dynamic_worker(
        db.another_queue(|builder| builder).await,
        expression,
        dedupe_key,
        options,
        Arc::new(AtomicU32::new(0)),
    );
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        "the authoritative worker did not take the due occurrence",
        || async {
            schedule_cursor(&db.pool, db.queue.name(), dedupe_key)
                .await
                .is_some_and(|cursor| cursor > due_at)
        },
    )
    .await;
    stop_worker(shutdown, run).await;
}

async fn wait_for_schedule(
    pool: &PgPool,
    queue: &str,
    dedupe_key: &str,
) -> chrono::DateTime<chrono::Utc> {
    wait_for_some(
        Duration::from_secs(5),
        Duration::from_millis(10),
        "cron schedule was not reconciled",
        || schedule_cursor(pool, queue, dedupe_key),
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

#[sqlx::test(migrations = "./migrations")]
async fn test_cron_startup_is_skipped_when_shutdown_is_pre_cancelled(pool: PgPool) {
    let constrained = pool_with_max(&pool, 1).await;
    let db = TestDb::new(constrained.clone()).await;
    let worker = dynamic_worker(
        db.queue.clone(),
        "0 0 1 1 *",
        "pre-cancelled-startup",
        CronOptions::default(),
        Arc::new(AtomicU32::new(0)),
    );
    let health = worker.health();
    let connection = constrained.acquire().await.unwrap();
    let shutdown = CancellationToken::new();
    shutdown.cancel();

    tokio::time::timeout(Duration::from_secs(1), worker.run_until(shutdown))
        .await
        .expect("pre-cancelled cron worker should stop promptly")
        .expect("pre-cancelled cron worker should stop cleanly");

    let snapshot = health.snapshot();
    assert_eq!(snapshot.status, WorkerHealthStatus::Stopped);
    assert!(snapshot.failures.is_empty());
    drop(connection);
    assert!(
        schedule_cursor(&pool, db.queue.name(), "pre-cancelled-startup")
            .await
            .is_none()
    );
}

async fn register_dynamic_schedule(
    db: &TestDb,
    expression: &str,
    dedupe_key: &str,
    options: CronOptions,
    counter: Arc<AtomicU32>,
) {
    let worker = dynamic_worker(db.queue.clone(), expression, dedupe_key, options, counter);
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    wait_for_schedule(&db.pool, db.queue.name(), dedupe_key).await;
    stop_worker(shutdown, run).await;
}

async fn just_missed_schedule(
    pool: &PgPool,
    seconds_ago: i64,
) -> (String, chrono::DateTime<chrono::Utc>) {
    let now = sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>(r#"SELECT now()"#)
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
    let now = sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>(r#"SELECT now()"#)
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
fn test_cron_attribute_exposes_schedule_and_revision() {
    assert_eq!(tick::SCHEDULE, "* * * * * *");
    assert_eq!(tick::CRON_REVISION, 7);
    assert_eq!(tick::NAME, "tick");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_cron_publishes_each_occurrence_once_across_workers(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let worker_a = Worker::builder(db.queue.clone())
        .register_cron(tick)
        .state(counter.clone())
        .timers(timers())
        .poll_interval(Duration::from_millis(20))
        .build()
        .unwrap();
    let worker_b = Worker::builder(db.another_queue(|builder| builder).await)
        .register_cron(tick)
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
    // A second publication of one occurrence would run the handler twice, so it
    // moves `published` and `fired` together: only counting the distinct
    // `scheduled_at` values can tell the duplicate apart from the next tick.
    let (published, occurrences, completed) = sqlx::query_as::<_, (i64, i64, i64)>(
        r#"SELECT count(*), count(DISTINCT scheduled_at),
                  count(*) FILTER (WHERE status = 'complete')
           FROM pgqueue.jobs
           WHERE queue = $1 AND name = 'tick' AND kind = 'cron'"#,
    )
    .bind(db.queue.name())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(completed, i64::from(fired));
    assert_eq!(
        published, occurrences,
        "two workers published the same occurrence more than once"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_cron_job_can_run_as_a_keyless_one_off(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let handle = db.queue.enqueue(yearly::job()).await.unwrap().unwrap();
    let worker = Worker::builder(db.queue.clone())
        .register_cron(yearly)
        .state(counter.clone())
        .timers(timers())
        .poll_interval(Duration::from_millis(20))
        .burst(true)
        .dequeue_timeout(Duration::from_millis(200))
        .build()
        .unwrap();
    worker.run_until(CancellationToken::new()).await.unwrap();

    assert_eq!(handle.wait(Some(Duration::from_secs(2))).await.unwrap(), 1);
    assert!(handle.fetch_job().await.unwrap().dedupe_key.is_none());
}

#[sqlx::test(migrations = "./migrations")]
async fn test_burst_worker_runs_due_cron_before_declaring_queue_drained(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let dedupe_key = "burst-due-cron";
    let options = CronOptions {
        revision: 1,
        misfire: CronMisfirePolicy::FireOnce,
    };
    let (expression, missed) = just_missed_schedule(&pool, 2).await;
    register_dynamic_schedule(&db, &expression, dedupe_key, options, counter.clone()).await;
    sqlx::query(
        "UPDATE pgqueue.cron_schedules SET next_run_at = $3 WHERE queue = $1 AND dedupe_key = $2",
    )
    .bind(db.queue.name())
    .bind(dedupe_key)
    .bind(missed)
    .execute(&pool)
    .await
    .unwrap();

    let worker = Worker::builder(db.queue.clone())
        .schedule_cron_with_options(
            &expression,
            dynamic_tick::job(()).dedupe_key(dedupe_key),
            options,
        )
        .state(counter.clone())
        .timers(timers())
        .poll_interval(Duration::from_millis(20))
        .concurrency(1)
        .burst(true)
        .dequeue_timeout(Duration::from_nanos(1))
        .build()
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        worker.run_until(CancellationToken::new()),
    )
    .await
    .expect("burst worker did not stop")
    .unwrap();

    assert_eq!(counter.load(Ordering::SeqCst), 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"SELECT count(*) FROM pgqueue.jobs
               WHERE queue = $1 AND dedupe_key = $2 AND status = 'complete'"#,
        )
        .bind(db.queue.name())
        .bind(dedupe_key)
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_burst_worker_leaves_cron_occurrences_after_its_start_boundary(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let dedupe_key = "burst-cron-boundary";
    let expression = "* * * * * *";
    let options = CronOptions {
        revision: 1,
        misfire: CronMisfirePolicy::FireOnce,
    };
    register_dynamic_schedule(&db, expression, dedupe_key, options, counter.clone()).await;
    // Registration can straddle a whole-second boundary. Start the assertion
    // from a clean occurrence ledger and force exactly one occurrence due.
    sqlx::query("DELETE FROM pgqueue.jobs WHERE queue = $1 AND dedupe_key = $2")
        .bind(db.queue.name())
        .bind(dedupe_key)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM pgqueue.cron_occurrences WHERE queue = $1 AND dedupe_key = $2")
        .bind(db.queue.name())
        .bind(dedupe_key)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE pgqueue.cron_schedules
         SET next_run_at = date_trunc('second', clock_timestamp()) - interval '1 second'
         WHERE queue = $1 AND dedupe_key = $2",
    )
    .bind(db.queue.name())
    .bind(dedupe_key)
    .execute(&pool)
    .await
    .unwrap();
    counter.store(0, Ordering::SeqCst);

    let worker = Worker::builder(db.queue.clone())
        .schedule_cron_with_options(
            expression,
            dynamic_tick::job(()).dedupe_key(dedupe_key),
            options,
        )
        .state(counter.clone())
        .timers(WorkerTimers {
            schedule: Duration::from_millis(20),
            ..crate::test_timers()
        })
        .poll_interval(Duration::from_millis(20))
        .concurrency(1)
        .burst(true)
        // Longer than the cron period: a continuous scheduler would keep
        // resetting this idle deadline with newly due occurrences.
        .dequeue_timeout(Duration::from_millis(1_500))
        .build()
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(4),
        worker.run_until(CancellationToken::new()),
    )
    .await
    .expect("burst worker kept scheduling future cron occurrences")
    .unwrap();

    assert_eq!(counter.load(Ordering::SeqCst), 1);
    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT next_run_at <= clock_timestamp()
             FROM pgqueue.cron_schedules WHERE queue = $1 AND dedupe_key = $2",
        )
        .bind(db.queue.name())
        .bind(dedupe_key)
        .fetch_one(&pool)
        .await
        .unwrap(),
        "an occurrence that became due after startup must remain for a later worker"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_burst_worker_waits_for_a_locked_due_cron_schedule(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let blocker_key = "burst-lock-blocker";
    let dedupe_key = "burst-locked-cron";
    let options = CronOptions {
        revision: 1,
        misfire: CronMisfirePolicy::FireOnce,
    };
    let (expression, missed) = just_missed_schedule(&pool, 2).await;
    register_dynamic_schedule(&db, &expression, blocker_key, options, counter.clone()).await;
    register_dynamic_schedule(&db, &expression, dedupe_key, options, counter.clone()).await;
    sqlx::query(
        "UPDATE pgqueue.cron_schedules SET next_run_at = $4
         WHERE queue = $1 AND dedupe_key IN ($2, $3)",
    )
    .bind(db.queue.name())
    .bind(blocker_key)
    .bind(dedupe_key)
    .bind(missed)
    .execute(&pool)
    .await
    .unwrap();

    // Park scheduling of the first cron on its dedupe advisory lock. Once it
    // reaches that lock, reconciliation of the whole registry is complete and
    // the second cron has not yet been visited, giving the test a deterministic
    // window to lock only the schedule path under test.
    let mut blocker = db.queue.pool().begin().await.unwrap();
    db.queue
        .enqueue_in(
            &mut blocker,
            dynamic_tick::job(())
                .dedupe_key(blocker_key)
                .delay(Duration::from_secs(60)),
        )
        .await
        .unwrap();

    let worker = Worker::builder(db.queue.clone())
        .schedule_cron_with_options(
            &expression,
            dynamic_tick::job(()).dedupe_key(blocker_key),
            options,
        )
        .schedule_cron_with_options(
            &expression,
            dynamic_tick::job(()).dedupe_key(dedupe_key),
            options,
        )
        .state(counter.clone())
        .timers(timers())
        .poll_interval(Duration::from_millis(20))
        .concurrency(1)
        .burst(true)
        .dequeue_timeout(Duration::from_millis(100))
        .build()
        .unwrap();
    let run = tokio::spawn(worker.run_until(CancellationToken::new()));
    crate::wait_for_dequeue_lock_waiter(&db.queue, true).await;

    let mut lock = pool.begin().await.unwrap();
    sqlx::query(
        "SELECT next_run_at FROM pgqueue.cron_schedules
         WHERE queue = $1 AND dedupe_key = $2 FOR UPDATE",
    )
    .bind(db.queue.name())
    .bind(dedupe_key)
    .fetch_one(&mut *lock)
    .await
    .unwrap();
    blocker.rollback().await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !run.is_finished(),
        "a dequeue timeout must not hide a due cron behind row-lock contention"
    );
    lock.rollback().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("burst worker did not finish after the schedule lock was released")
        .unwrap()
        .unwrap();

    assert_eq!(counter.load(Ordering::SeqCst), 2);
    assert_eq!(
        cron_jobs_published(&pool, db.queue.name(), dedupe_key).await,
        1
    );
    assert!(
        schedule_cursor(&pool, db.queue.name(), dedupe_key)
            .await
            .is_some_and(|cursor| cursor > missed)
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_cron_registry_does_not_speculatively_enqueue_future_jobs(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let worker = Worker::builder(db.queue.clone())
        .register_cron(yearly)
        .state(Arc::new(AtomicU32::new(0)))
        .timers(timers())
        .build()
        .unwrap();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    let next = wait_for_schedule(&pool, db.queue.name(), "cron:yearly").await;
    let now = sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>(r#"SELECT now()"#)
        .fetch_one(&pool)
        .await
        .unwrap();

    assert!(next > now);
    assert_eq!(db.queue.counts().await.unwrap().scheduled, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"SELECT count(*) FROM pgqueue.jobs WHERE queue = $1 AND kind = 'cron'"#
        )
        .bind(db.queue.name())
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    stop_worker(shutdown, run).await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_worker_builder_cron_runs_a_dynamic_schedule(pool: PgPool) {
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
async fn test_cron_skip_publishes_a_durable_cursor_within_grace(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let (expression, missed) = just_missed_schedule(&pool, 2).await;
    let options = skip_options(1, Duration::from_secs(10));
    register_dynamic_schedule(&db, &expression, "within-grace", options, counter.clone()).await;
    sqlx::query(
        "UPDATE pgqueue.cron_schedules SET next_run_at = $3 WHERE queue = $1 AND dedupe_key = $2",
    )
    .bind(db.queue.name())
    .bind("within-grace")
    .bind(missed)
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

    let scheduled_at = sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>(
        "SELECT scheduled_at FROM pgqueue.jobs
         WHERE queue = $1 AND dedupe_key = $2 AND kind = 'cron'",
    )
    .bind(db.queue.name())
    .bind("within-grace")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(scheduled_at, missed);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_cron_skip_advances_a_stale_cursor_without_publishing(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let (expression, missed) = just_missed_schedule(&pool, 3).await;
    let options = skip_options(1, Duration::from_secs(1));
    register_dynamic_schedule(&db, &expression, "stale-skip", options, counter.clone()).await;
    sqlx::query(
        "UPDATE pgqueue.cron_schedules SET next_run_at = $3 WHERE queue = $1 AND dedupe_key = $2",
    )
    .bind(db.queue.name())
    .bind("stale-skip")
    .bind(missed)
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
        sqlx::query_scalar::<_, i64>(
            r#"SELECT count(*) FROM pgqueue.jobs
               WHERE queue = $1 AND dedupe_key = $2 AND kind = 'cron'"#,
        )
        .bind(db.queue.name())
        .bind("stale-skip")
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"SELECT count(*) FROM pgqueue.cron_occurrences
               WHERE queue = $1 AND dedupe_key = $2 AND scheduled_at = $3"#,
        )
        .bind(db.queue.name())
        .bind("stale-skip")
        .bind(missed)
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_cron_fire_once_publishes_only_the_latest_missed_occurrence(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let (expression, latest) = just_missed_schedule(&pool, 2).await;
    let options = CronOptions {
        revision: 1,
        misfire: CronMisfirePolicy::FireOnce,
    };
    register_dynamic_schedule(&db, &expression, "fire-once", options, counter.clone()).await;
    let old_cursor = latest - chrono::Duration::hours(2);
    sqlx::query(
        "UPDATE pgqueue.cron_schedules SET next_run_at = $3 WHERE queue = $1 AND dedupe_key = $2",
    )
    .bind(db.queue.name())
    .bind("fire-once")
    .bind(old_cursor)
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
        .filter(|row| row.dedupe_key.as_deref() == Some("fire-once"))
        .collect::<Vec<_>>();
    assert_eq!(occurrences.len(), 1);
    assert_eq!(occurrences[0].scheduled_at, latest);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_cron_template_revision_preserves_a_due_fire_once_cursor(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let (expression, latest) = just_missed_schedule(&pool, 2).await;
    let dedupe_key = "template-revision-fire-once";
    let initial_options = CronOptions {
        revision: 1,
        misfire: CronMisfirePolicy::FireOnce,
    };
    let initial = Worker::builder(db.queue.clone())
        .schedule_cron_with_options(
            &expression,
            dynamic_tick::job(())
                .dedupe_key(dedupe_key)
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
    wait_for_schedule(&pool, db.queue.name(), dedupe_key).await;
    stop_worker(initial_shutdown, initial_run).await;

    sqlx::query(
        "UPDATE pgqueue.cron_schedules SET next_run_at = $3 WHERE queue = $1 AND dedupe_key = $2",
    )
    .bind(db.queue.name())
    .bind(dedupe_key)
    .bind(latest)
    .execute(&pool)
    .await
    .unwrap();

    let revised = Worker::builder(db.queue.clone())
        .schedule_cron_with_options(
            &expression,
            dynamic_tick::job(())
                .dedupe_key(dedupe_key)
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

    let (scheduled_at, meta) =
        sqlx::query_as::<_, (chrono::DateTime<chrono::Utc>, serde_json::Value)>(
            "SELECT scheduled_at, meta FROM pgqueue.jobs
             WHERE queue = $1 AND dedupe_key = $2 AND kind = 'cron'",
        )
        .bind(db.queue.name())
        .bind(dedupe_key)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(scheduled_at, latest);
    assert_eq!(meta, serde_json::json!({ "template": 2 }));
}

/// `reconcile_cron` writes the definition, immediately reads it back, and
/// compared the two with a Rust `!=` — but `jsonb` stores numbers as `numeric`,
/// so `serde_json`'s exponent form is expanded on the way out and re-parses as
/// `Number::PosInt` where it went in as `Number::Float`, and `serde_json`'s
/// `Number: PartialEq` calls those unequal. Any cron whose payload or meta
/// carried a float of 1e16 or larger therefore found the definition it had just
/// written itself to be in conflict, with no competing deploy anywhere:
/// `permanent=true`, so the cron never ran again, and the diagnostic told the
/// operator to bump a revision, which can never help. `jsonb` equality is the
/// only equality this value has, so the comparison belongs in PostgreSQL.
#[sqlx::test(migrations = "./migrations")]
async fn test_cron_reconciles_a_definition_carrying_a_float_jsonb_stores_as_an_integer(
    pool: PgPool,
) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let dedupe_key = "float-definition";
    let worker = Worker::builder(db.queue.clone())
        .schedule_cron_with_options(
            "* * * * * *",
            dynamic_tick::job(())
                .dedupe_key(dedupe_key)
                // `1e16` survives the round trip as `10000000000000000`, which
                // `jsonb` calls equal and `serde_json` does not. `1e15` renders
                // as `1000000000000000.0` and always compared equal, which is
                // why the control below is the same shape one exponent smaller.
                .meta(serde_json::json!({ "big": 1e16, "small": 1e15 })),
            skip_options(0, Duration::from_secs(1)),
        )
        .state(counter.clone())
        .timers(timers())
        .poll_interval(Duration::from_millis(20))
        .concurrency(2)
        .build()
        .unwrap();
    let health = worker.health();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        "the cron never published an occurrence",
        || async { cron_jobs_published(&pool, db.queue.name(), dedupe_key).await >= 1 },
    )
    .await;
    wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        "the cron occurrence never ran",
        || async { counter.load(Ordering::SeqCst) >= 1 },
    )
    .await;

    // A conflict is reported as a permanent scheduler failure, so a healthy
    // scheduler is what says the definition reconciled rather than merely that
    // some other worker published for it.
    let snapshot = health.snapshot();
    assert_eq!(
        snapshot.status,
        WorkerHealthStatus::Ready,
        "reconciliation reported a conflict against its own definition: {:?}",
        snapshot.failures
    );
    stop_worker(shutdown, run).await;

    // The stored form really is the expanded integer the Rust-side comparison
    // could never match, so this test is exercising the round trip it claims to.
    let stored = sqlx::query_scalar::<_, String>(
        "SELECT definition -> 'meta' ->> 'big' FROM pgqueue.cron_schedules
         WHERE queue = $1 AND dedupe_key = $2",
    )
    .bind(db.queue.name())
    .bind(dedupe_key)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(stored, "10000000000000000");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_cron_equal_revision_rejects_a_different_definition(pool: PgPool) {
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
    // A rejected cron definition disables that cron and degrades scheduler
    // health, but the worker keeps running so unrelated jobs still flow.
    let conflicting_health = conflicting.health();
    let conflicting_shutdown = CancellationToken::new();
    let conflicting_run = tokio::spawn(conflicting.run_until(conflicting_shutdown.clone()));
    wait_until(
        Duration::from_secs(3),
        Duration::from_millis(10),
        "conflicting worker did not report degraded scheduler health",
        || async {
            let snapshot = conflicting_health.snapshot();
            snapshot.status == WorkerHealthStatus::Degraded
                && snapshot
                    .failures
                    .iter()
                    .any(|failure| failure.component == WorkerComponent::Scheduler)
        },
    )
    .await;
    // The authority's schedule is untouched by the rejected definition.
    assert_eq!(
        chrono::Timelike::second(
            &schedule_cursor(&pool, db.queue.name(), "revision-conflict")
                .await
                .unwrap()
        ),
        0,
    );
    stop_worker(conflicting_shutdown, conflicting_run).await;
    stop_worker(shutdown, run).await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_cron_higher_revision_takes_authority_and_degrades_lower_workers(pool: PgPool) {
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
            sqlx::query_scalar::<_, i64>(
                "SELECT revision FROM pgqueue.cron_schedules WHERE queue = $1 AND dedupe_key = $2",
            )
            .bind(db.queue.name())
            .bind("revision-takeover")
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
    // Being superseded is the normal state of a not-yet-upgraded worker during
    // a rolling deploy: it stops scheduling that cron but stays healthy, so an
    // orchestrator probing `/health` does not restart a perfectly good process.
    //
    // Retire the authority and drag the cursor into the past first. Left where
    // the takeover put it the cursor is a minute away, so nothing was ever due
    // in the window below and "the cursor has not moved" held whether or not
    // the lower worker was still scheduling — the guard this asserts was never
    // reached at all.
    stop_worker(higher_shutdown, higher_run).await;
    let due_at = backdate_schedule(&pool, db.queue.name(), "revision-takeover").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let snapshot = lower_health.snapshot();
    assert_eq!(snapshot.status, WorkerHealthStatus::Ready, "{snapshot:?}");
    assert!(
        !snapshot
            .failures
            .iter()
            .any(|failure| failure.component == WorkerComponent::Scheduler),
        "{snapshot:?}"
    );
    // ...and it really has stopped advancing the superseded schedule, having
    // published nothing against a revision that is no longer its own.
    assert_eq!(
        schedule_cursor(&pool, db.queue.name(), "revision-takeover").await,
        Some(due_at),
        "a superseded worker advanced a cursor it no longer owns"
    );
    assert_eq!(
        cron_jobs_published(&pool, db.queue.name(), "revision-takeover").await,
        0,
        "a superseded worker published an occurrence"
    );
    stop_worker(lower_shutdown, lower_run).await;

    // And the occurrence really was there to take.
    assert_authority_advances(
        &db,
        "30 * * * * *",
        "revision-takeover",
        skip_options(2, Duration::from_secs(1)),
        due_at,
    )
    .await;
}

/// The other half of supersession: a worker that starts *after* a higher
/// revision has taken over is refused at reconciliation, by the UPSERT's
/// `revision < EXCLUDED.revision` guard, and never reaches scheduling at all.
/// That is every not-yet-restarted process in a rolling deploy, so it must stay
/// healthy, leave the authority's revision alone, and — the point of the whole
/// mechanism — publish nothing for a cron that is due right now.
#[sqlx::test(migrations = "./migrations")]
async fn test_cron_reconcile_refuses_a_worker_whose_revision_is_already_superseded(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    register_dynamic_schedule(
        &db,
        "30 * * * * *",
        "revision-superseded",
        skip_options(9, Duration::from_secs(1)),
        Arc::new(AtomicU32::new(0)),
    )
    .await;
    let due_at = backdate_schedule(&pool, db.queue.name(), "revision-superseded").await;

    let superseded = dynamic_worker(
        db.queue.clone(),
        "30 * * * * *",
        "revision-superseded",
        skip_options(8, Duration::from_secs(1)),
        Arc::new(AtomicU32::new(0)),
    );
    let health = superseded.health();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(superseded.run_until(shutdown.clone()));
    tokio::time::sleep(Duration::from_millis(300)).await;

    let snapshot = health.snapshot();
    assert_eq!(snapshot.status, WorkerHealthStatus::Ready, "{snapshot:?}");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT revision FROM pgqueue.cron_schedules WHERE queue = $1 AND dedupe_key = $2",
        )
        .bind(db.queue.name())
        .bind("revision-superseded")
        .fetch_one(&pool)
        .await
        .unwrap(),
        9,
        "an older revision overwrote the authority's definition"
    );
    assert_eq!(
        schedule_cursor(&pool, db.queue.name(), "revision-superseded").await,
        Some(due_at),
        "a superseded worker advanced a cursor it never owned"
    );
    assert_eq!(
        cron_jobs_published(&pool, db.queue.name(), "revision-superseded").await,
        0,
        "a superseded worker published an occurrence"
    );
    stop_worker(shutdown, run).await;

    assert_authority_advances(
        &db,
        "30 * * * * *",
        "revision-superseded",
        skip_options(9, Duration::from_secs(1)),
        due_at,
    )
    .await;
}

//noinspection SqlNoDataSourceInspection
#[sqlx::test(migrations = "./migrations")]
async fn test_cron_cursor_claim_and_job_insert_roll_back_together(pool: PgPool) {
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
    sqlx::query(
        "UPDATE pgqueue.cron_schedules SET next_run_at = $3 WHERE queue = $1 AND dedupe_key = $2",
    )
    .bind(db.queue.name())
    .bind("atomic-publication")
    .bind(missed)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "ALTER TABLE pgqueue.jobs
         ADD CONSTRAINT reject_cron_insert_for_test CHECK (kind <> 'cron') NOT VALID",
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
        sqlx::query_scalar::<_, i64>(
            r#"SELECT count(*) FROM pgqueue.cron_occurrences WHERE queue = $1 AND dedupe_key = $2"#
        )
        .bind(db.queue.name())
        .bind("atomic-publication")
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );

    sqlx::query("ALTER TABLE pgqueue.jobs DROP CONSTRAINT reject_cron_insert_for_test")
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
        sqlx::query_scalar::<_, i64>(
            r#"SELECT count(*) FROM pgqueue.cron_occurrences
               WHERE queue = $1 AND dedupe_key = $2 AND scheduled_at = $3"#,
        )
        .bind(db.queue.name())
        .bind("atomic-publication")
        .bind(missed)
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_cron_foreign_live_holder_claims_and_skips_the_occurrence(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let (expression, missed) = just_missed_schedule(&pool, 2).await;
    let options = skip_options(1, Duration::from_secs(10));
    register_dynamic_schedule(&db, &expression, "foreign-holder", options, counter.clone()).await;
    let owner = db
        .queue
        .enqueue(
            yearly::job()
                .dedupe_key("foreign-holder")
                .delay(Duration::from_secs(60)),
        )
        .await
        .unwrap()
        .unwrap();
    sqlx::query(
        "UPDATE pgqueue.cron_schedules SET next_run_at = $3 WHERE queue = $1 AND dedupe_key = $2",
    )
    .bind(db.queue.name())
    .bind("foreign-holder")
    .bind(missed)
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
    assert_eq!(owner.fetch_job().await.unwrap().status, JobStatus::Queued);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"SELECT count(*) FROM pgqueue.cron_occurrences
               WHERE queue = $1 AND dedupe_key = $2 AND scheduled_at = $3"#,
        )
        .bind(db.queue.name())
        .bind("foreign-holder")
        .bind(missed)
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"SELECT count(*) FROM pgqueue.jobs
               WHERE queue = $1 AND dedupe_key = $2 AND kind = 'cron'"#,
        )
        .bind(db.queue.name())
        .bind("foreign-holder")
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    stop_worker(shutdown, run).await;
}

/// The cron twin of the vanished out-of-band dedupe owner: a foreign writer
/// takes the cron's dedupe key between `schedule_cron`'s holder pre-check and
/// its insert, then releases it again before the holder can be re-read. The
/// occurrence claim rolls back with the error, so a later pass republishes it:
/// the failure is transient, and health must degrade and recover around it.
#[sqlx::test(migrations = "./migrations")]
async fn test_cron_retries_an_occurrence_lost_to_a_vanished_foreign_holder(pool: PgPool) {
    const INSERT_GATE: i32 = 20_574;
    const CONFLICT_GATE: i32 = 20_575;

    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let (expression, missed) = just_missed_schedule(&pool, 1).await;
    let options = skip_options(1, Duration::from_secs(60));
    register_dynamic_schedule(
        &db,
        &expression,
        "vanishing-holder",
        options,
        counter.clone(),
    )
    .await;
    sqlx::query(
        "UPDATE pgqueue.cron_schedules SET next_run_at = $3 WHERE queue = $1 AND dedupe_key = $2",
    )
    .bind(db.queue.name())
    .bind("vanishing-holder")
    .bind(missed)
    .execute(&pool)
    .await
    .unwrap();
    // Pause the publication between its holder pre-check and its insert...
    crate::install_statement_gate(
        &pool,
        "wait_at_cron_insert",
        INSERT_GATE,
        "INSERT",
        "NEW.kind = 'cron'",
    )
    .await;
    // ...and between its conflict decision and its holder re-read.
    crate::install_conflicted_insert_gate(&pool, "wait_at_cron_conflict", CONFLICT_GATE).await;
    let insert_gate = crate::hold_gate(&pool, INSERT_GATE, &db.database).await;
    let conflict_gate = crate::hold_gate(&pool, CONFLICT_GATE, &db.database).await;

    let worker = dynamic_worker(
        db.queue.clone(),
        &expression,
        "vanishing-holder",
        options,
        counter.clone(),
    );
    let health = worker.health();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    crate::wait_for_lock_waiter(
        &db,
        "%WITH inserted AS (%",
        "cron publication did not reach its insert",
    )
    .await;

    // A row that takes the cron's dedupe key without the enqueue advisory
    // lock...
    let holder = sqlx::query_scalar::<_, uuid::Uuid>(
        r#"INSERT INTO pgqueue.jobs (queue, name, payload, dedupe_key, status, max_attempts)
           VALUES ($1, 'out-of-band', 'null'::jsonb, 'vanishing-holder', 'queued', 1)
           RETURNING id"#,
    )
    .bind(db.queue.name())
    .fetch_one(&pool)
    .await
    .unwrap();
    insert_gate.rollback().await.unwrap();
    crate::wait_for_advisory_waiter(
        &pool,
        CONFLICT_GATE,
        "conflicted cron insert did not reach its holder re-read",
    )
    .await;
    // ...and releases it again before the holder can be named.
    sqlx::query("DELETE FROM pgqueue.jobs WHERE id = $1")
        .bind(holder)
        .execute(&pool)
        .await
        .unwrap();
    conflict_gate.rollback().await.unwrap();

    // The lost occurrence degrades scheduler health with the race error...
    let failure = wait_for_some(
        Duration::from_secs(3),
        Duration::from_millis(10),
        "the lost occurrence did not degrade scheduler health",
        || async {
            health.snapshot().failures.into_iter().find(|failure| {
                failure.component == WorkerComponent::Scheduler
                    && failure.message.contains("lost its dedupe key")
            })
        },
    )
    .await;
    assert!(
        failure.message.contains("dedupe race") && !failure.message.contains("configuration"),
        "a transient dedupe race must not look like a permanent misconfiguration: {}",
        failure.message
    );
    // ...and the rolled-back claim is republished by a later pass.
    wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        "the lost occurrence was not retried",
        || async { counter.load(Ordering::SeqCst) == 1 },
    )
    .await;
    wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        "scheduler health did not recover after the retry published",
        || async { health.snapshot().status == WorkerHealthStatus::Ready },
    )
    .await;
    stop_worker(shutdown, run).await;
    assert_eq!(
        cron_jobs_published(&pool, db.queue.name(), "vanishing-holder").await,
        1,
        "the retried occurrence must be published exactly once"
    );
}

//noinspection SqlNoDataSourceInspection
#[sqlx::test(migrations = "./migrations")]
async fn test_cron_rechecks_skip_grace_after_waiting_for_the_dedupe_lock(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let (expression, occurrence) = upcoming_schedule(&pool, 2).await;
    let options = skip_options(1, Duration::from_millis(300));
    register_dynamic_schedule(&db, &expression, "lock-wait", options, counter.clone()).await;
    sqlx::query(
        "UPDATE pgqueue.cron_schedules SET next_run_at = $3 WHERE queue = $1 AND dedupe_key = $2",
    )
    .bind(db.queue.name())
    .bind("lock-wait")
    .bind(occurrence)
    .execute(&pool)
    .await
    .unwrap();

    let mut transaction = db.queue.pool().begin().await.unwrap();
    db.queue
        .enqueue_in(
            &mut transaction,
            dynamic_tick::job(())
                .dedupe_key("lock-wait")
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

    let now = sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>(r#"SELECT clock_timestamp()"#)
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
        "scheduler did not advance after the dedupe lock was released",
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
        sqlx::query_scalar::<_, i64>(
            r#"SELECT count(*) FROM pgqueue.jobs WHERE queue = $1 AND dedupe_key = $2"#
        )
        .bind(db.queue.name())
        .bind("lock-wait")
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_cron_lock_wait_observes_worker_shutdown(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let (expression, _) = upcoming_schedule(&pool, 30).await;
    let options = skip_options(1, Duration::from_secs(60));
    let mut transaction = db.queue.pool().begin().await.unwrap();
    db.queue
        .enqueue_in(
            &mut transaction,
            dynamic_tick::job(())
                .dedupe_key("shutdown-lock-wait")
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
    sqlx::query(
        "UPDATE pgqueue.cron_schedules SET next_run_at = now()
         WHERE queue = $1 AND dedupe_key = $2",
    )
    .bind(db.queue.name())
    .bind("shutdown-lock-wait")
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
async fn test_cron_reconciliation_lock_wait_observes_worker_shutdown(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let (expression, _) = upcoming_schedule(&pool, 30).await;
    let options = skip_options(1, Duration::from_secs(60));
    let mut transaction = db.queue.pool().begin().await.unwrap();
    db.queue
        .enqueue_in(
            &mut transaction,
            dynamic_tick::job(())
                .dedupe_key("reconcile-shutdown-lock-wait")
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
    sqlx::query(
        "UPDATE pgqueue.cron_schedules SET next_run_at = now()
         WHERE queue = $1 AND dedupe_key = $2",
    )
    .bind(db.queue.name())
    .bind("reconcile-shutdown-lock-wait")
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
async fn test_cron_builder_rejects_manual_schedule_overrides(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let error = Worker::builder(db.queue)
        .schedule_cron(
            "* * * * * *",
            dynamic_tick::job(()).delay(Duration::from_secs(1)),
        )
        .build()
        .unwrap_err();
    assert!(error.to_string().contains("cannot use delay"), "{error}");
}

/// A cursor more than one period stale is correctly refused, but jumping
/// straight to `next_occurrence(now)` silently threw away the *most recent*
/// occurrence even while it was still well inside its own grace — no job row,
/// no claim row, and no `SkippedStale` warning for it. Every catch-up (restart,
/// leader handover, deploy gap) cost one extra occurrence.
#[sqlx::test(migrations = "./migrations")]
async fn test_cron_skip_publishes_the_recent_occurrence_when_the_cursor_is_stale(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    // Once a minute; `recent` fired 3 seconds ago and its 10s grace is open.
    let (expression, recent) = just_missed_schedule(&pool, 3).await;
    let options = skip_options(1, Duration::from_secs(10));
    register_dynamic_schedule(&db, &expression, "stale-catch-up", options, counter.clone()).await;
    // A whole period further back, so the stored cursor is genuinely stale.
    let stale = recent - chrono::Duration::seconds(60);
    sqlx::query(
        "UPDATE pgqueue.cron_schedules SET next_run_at = $3 WHERE queue = $1 AND dedupe_key = $2",
    )
    .bind(db.queue.name())
    .bind("stale-catch-up")
    .bind(stale)
    .execute(&pool)
    .await
    .unwrap();

    let worker = dynamic_worker(
        db.queue.clone(),
        &expression,
        "stale-catch-up",
        options,
        counter.clone(),
    );
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        "the still-publishable occurrence was discarded",
        || async { counter.load(Ordering::SeqCst) == 1 },
    )
    .await;
    stop_worker(shutdown, run).await;

    let scheduled_at = sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>(
        "SELECT scheduled_at FROM pgqueue.jobs
         WHERE queue = $1 AND dedupe_key = $2 AND kind = 'cron'",
    )
    .bind(db.queue.name())
    .bind("stale-catch-up")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        scheduled_at, recent,
        "catch-up must publish the most recent occurrence, not the stale cursor"
    );
    assert_eq!(
        schedule_cursor(&pool, db.queue.name(), "stale-catch-up").await,
        Some(recent + chrono::Duration::seconds(60)),
        "the cursor advances past the occurrence it just published",
    );
}

/// The catch-up fallback is bounded by the same grace as the stored cursor: a
/// recent occurrence that is *also* past its deadline must still be skipped,
/// or `Skip` would degrade into `FireOnce`.
#[sqlx::test(migrations = "./migrations")]
async fn test_cron_skip_discards_a_recent_occurrence_that_is_past_its_own_grace(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let (expression, recent) = just_missed_schedule(&pool, 3).await;
    // One second of grace, so the 3-second-old occurrence is stale as well.
    let options = skip_options(1, Duration::from_secs(1));
    register_dynamic_schedule(&db, &expression, "stale-both", options, counter.clone()).await;
    let stale = recent - chrono::Duration::seconds(60);
    sqlx::query(
        "UPDATE pgqueue.cron_schedules SET next_run_at = $3 WHERE queue = $1 AND dedupe_key = $2",
    )
    .bind(db.queue.name())
    .bind("stale-both")
    .bind(stale)
    .execute(&pool)
    .await
    .unwrap();

    let worker = dynamic_worker(
        db.queue.clone(),
        &expression,
        "stale-both",
        options,
        counter.clone(),
    );
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        "stale cursor was not advanced",
        || async {
            schedule_cursor(&pool, db.queue.name(), "stale-both")
                .await
                .is_some_and(|cursor| cursor > recent)
        },
    )
    .await;
    stop_worker(shutdown, run).await;

    assert_eq!(counter.load(Ordering::SeqCst), 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            r#"SELECT count(*) FROM pgqueue.jobs
               WHERE queue = $1 AND dedupe_key = $2 AND kind = 'cron'"#,
        )
        .bind(db.queue.name())
        .bind("stale-both")
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
}

/// `state.rejected` mixed permanent rejections — a reused revision with a
/// different definition, which disables the cron and is never re-evaluated —
/// with transient ones, and every scheduling pass cleared the whole vector
/// before re-reconciling only the retryable keys. So an *unrelated* cron
/// recovering from a database blip erased the permanent failure and the worker
/// reported itself Ready while a cron stayed silently disabled forever.
#[sqlx::test(migrations = "./migrations")]
async fn test_cron_health_keeps_a_permanent_rejection_when_a_transient_one_recovers(pool: PgPool) {
    let db = TestDb::new(pool_with_max(&pool, 10).await).await;

    // The authority establishes "mixed-permanent" at revision 4.
    let authority = dynamic_worker(
        db.queue.clone(),
        "0 * * * * *",
        "mixed-permanent",
        skip_options(4, Duration::from_secs(1)),
        Arc::new(AtomicU32::new(0)),
    );
    let authority_shutdown = CancellationToken::new();
    let authority_run = tokio::spawn(authority.run_until(authority_shutdown.clone()));
    wait_for_schedule(&pool, db.queue.name(), "mixed-permanent").await;
    stop_worker(authority_shutdown, authority_run).await;

    // A database blip that only affects "mixed-transient".
    sqlx::raw_sql(
        "CREATE FUNCTION pgqueue.repro_mixed_outage() RETURNS trigger
         LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION 'transient: terminating connection due to failover'; END $$;
         CREATE TRIGGER repro_mixed_outage
         BEFORE INSERT OR UPDATE ON pgqueue.cron_schedules
         FOR EACH ROW WHEN (NEW.dedupe_key = 'mixed-transient')
         EXECUTE FUNCTION pgqueue.repro_mixed_outage();",
    )
    .execute(&pool)
    .await
    .unwrap();

    // One worker, two crons: a permanently rejected definition (revision 4
    // reused for a different expression) and a transiently failing one.
    let counter = Arc::new(AtomicU32::new(0));
    let subject = Worker::builder(db.another_queue(|builder| builder).await)
        .schedule_cron_with_options(
            "30 * * * * *",
            dynamic_tick::job(()).dedupe_key("mixed-permanent"),
            skip_options(4, Duration::from_secs(1)),
        )
        // Never due, so the subject only ever reconciles: this is a test about
        // health reporting, and publishing on top of it would make shutdown
        // wait on attempts that have nothing to do with the assertion.
        .schedule_cron_with_options(
            "0 0 1 1 *",
            dynamic_tick::job(()).dedupe_key("mixed-transient"),
            skip_options(1, Duration::from_secs(1)),
        )
        .state(counter)
        .timers(timers())
        .poll_interval(Duration::from_millis(20))
        .shutdown_grace(Duration::from_secs(10))
        .build()
        .unwrap();
    let health = subject.health();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(subject.run_until(shutdown.clone()));

    let scheduler_failures = || {
        health
            .snapshot()
            .failures
            .into_iter()
            .filter(|failure| failure.component == WorkerComponent::Scheduler)
            .map(|failure| failure.message)
            .collect::<Vec<_>>()
    };
    wait_until(
        Duration::from_secs(10),
        Duration::from_millis(20),
        "both cron failures were never reported together",
        || async {
            scheduler_failures().iter().any(|message| {
                message.contains("mixed-permanent") && message.contains("mixed-transient")
            })
        },
    )
    .await;

    // The blip is over; only the transient cron can recover.
    sqlx::raw_sql("DROP TRIGGER repro_mixed_outage ON pgqueue.cron_schedules")
        .execute(&pool)
        .await
        .unwrap();
    wait_until(
        Duration::from_secs(10),
        Duration::from_millis(20),
        "the transient cron never reconciled after the blip cleared",
        || async {
            schedule_cursor(&pool, db.queue.name(), "mixed-transient")
                .await
                .is_some()
        },
    )
    .await;
    // Give the scheduling loop several passes to (wrongly) report recovery.
    wait_until(
        Duration::from_secs(10),
        Duration::from_millis(20),
        "the transient failure was never dropped from the health report",
        || async {
            scheduler_failures()
                .iter()
                .all(|message| !message.contains("mixed-transient"))
        },
    )
    .await;

    let snapshot = health.snapshot();
    assert_eq!(
        snapshot.status,
        WorkerHealthStatus::Degraded,
        "a permanently disabled cron must keep degrading health: {:?}",
        snapshot.failures
    );
    assert!(
        scheduler_failures()
            .iter()
            .any(|message| message.contains("mixed-permanent")),
        "the permanent rejection was erased by an unrelated recovery: {:?}",
        snapshot.failures
    );

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(20), run)
        .await
        .expect("subject worker did not stop")
        .unwrap()
        .unwrap();
}

/// Startup reconciliation read `database.now()` once and reused it for every
/// entry, but each `reconcile_cron` is a round trip of its own. With a large
/// registry the shared reading was seconds stale by the end, so the last crons
/// got a `next_run_at` already in the past: instantly "due" with a stale
/// occurrence their misfire policy then had to skip.
///
/// The trigger below makes each reconcile take ~0.4s, so four entries span more
/// than a second — which a single per-second cursor cannot cover.
#[sqlx::test(migrations = "./migrations")]
async fn test_cron_reconciliation_reads_the_clock_once_per_entry(pool: PgPool) {
    let db = TestDb::new(pool_with_max(&pool, 10).await).await;
    sqlx::raw_sql(
        "CREATE FUNCTION pgqueue.repro_slow_reconcile() RETURNS trigger
         LANGUAGE plpgsql AS $$
         BEGIN PERFORM pg_sleep(0.4); RETURN NEW; END $$;
         CREATE TRIGGER repro_slow_reconcile
         BEFORE INSERT ON pgqueue.cron_schedules
         FOR EACH ROW EXECUTE FUNCTION pgqueue.repro_slow_reconcile();",
    )
    .execute(&pool)
    .await
    .unwrap();

    let keys = ["clock-a", "clock-b", "clock-c", "clock-d"];
    let mut builder = Worker::builder(db.queue.clone());
    for key in keys {
        builder = builder.schedule_cron_with_options(
            "* * * * * *",
            dynamic_tick::job(()).dedupe_key(key),
            skip_options(1, Duration::from_secs(1)),
        );
    }
    let worker = builder
        .state(Arc::new(AtomicU32::new(0)))
        .timers(timers())
        .poll_interval(Duration::from_millis(20))
        .build()
        .unwrap();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    for key in keys {
        wait_for_schedule(&pool, db.queue.name(), key).await;
    }

    #[derive(sqlx::FromRow)]
    struct Cursor {
        dedupe_key: String,
        next_run_at: chrono::DateTime<chrono::Utc>,
        created_at: chrono::DateTime<chrono::Utc>,
    }
    let cursors = sqlx::query_as::<_, Cursor>(
        "SELECT dedupe_key, next_run_at, created_at FROM pgqueue.cron_schedules
         WHERE queue = $1 ORDER BY created_at",
    )
    .bind(db.queue.name())
    .fetch_all(&pool)
    .await
    .unwrap();
    stop_worker(shutdown, run).await;

    assert_eq!(cursors.len(), keys.len());
    assert!(
        cursors[0].next_run_at < cursors[keys.len() - 1].next_run_at,
        "every entry got the same stale cursor: {:?}",
        cursors
            .iter()
            .map(|row| (row.dedupe_key.clone(), row.next_run_at))
            .collect::<Vec<_>>()
    );
    // `reconcile_cron` reads the clock in a round trip of its own and only then
    // opens the transaction `created_at` comes from. For a one-second cron the
    // cursor therefore lands a hair *behind* `created_at` whenever that gap
    // crosses a second boundary — harmless, and not what this test is about, so
    // it is tolerated. The staleness the shared clock reading produced is a
    // whole reconcile per entry (~0.4s here), which this still catches.
    let tolerance = chrono::TimeDelta::milliseconds(100);
    for row in &cursors {
        assert!(
            row.next_run_at > row.created_at - tolerance,
            "{} was reconciled with a cursor already in the past: {} <= {}",
            row.dedupe_key,
            row.next_run_at,
            row.created_at
        );
    }
}

#[pgqueue::cron("* * * * * *")]
async fn repro_ticker() -> anyhow::Result<()> {
    Ok(())
}

/// A pool timeout or a failover during a rolling restart fails startup
/// reconciliation. Reconciliation is one-shot, so without a retry the cron
/// would stay disabled for the lifetime of the process even though the database
/// recovered seconds later.
#[sqlx::test(migrations = "./migrations")]
async fn test_cron_recovers_when_a_transient_reconcile_failure_clears(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;

    sqlx::raw_sql(
        "CREATE FUNCTION pgqueue.repro_cron_outage() RETURNS trigger
         LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION 'transient: terminating connection due to failover'; END $$;
         CREATE TRIGGER repro_cron_outage
         BEFORE INSERT OR UPDATE ON pgqueue.cron_schedules
         FOR EACH ROW EXECUTE FUNCTION pgqueue.repro_cron_outage();",
    )
    .execute(&pool)
    .await
    .unwrap();

    let shutdown = CancellationToken::new();
    let worker = Worker::builder(db.queue.clone())
        .register_cron(repro_ticker)
        .timers(WorkerTimers {
            schedule: Duration::from_millis(50),
            ..test_timers()
        })
        .build()
        .unwrap();
    let health = worker.health();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    wait_until(
        Duration::from_secs(10),
        Duration::from_millis(20),
        "scheduler never degraded",
        || async {
            health
                .snapshot()
                .failures
                .iter()
                .any(|failure| failure.component == WorkerComponent::Scheduler)
        },
    )
    .await;

    // The outage is over.
    sqlx::raw_sql("DROP TRIGGER repro_cron_outage ON pgqueue.cron_schedules")
        .execute(&pool)
        .await
        .unwrap();

    wait_until(
        Duration::from_secs(10),
        Duration::from_millis(20),
        "the cron never recovered after the database healed",
        || async {
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM pgqueue.jobs WHERE queue = $1")
                .bind(db.queue.name())
                .fetch_one(&pool)
                .await
                .unwrap()
                > 0
        },
    )
    .await;
    wait_until(
        Duration::from_secs(10),
        Duration::from_millis(20),
        "scheduler health never recovered",
        || async { health.snapshot().status == WorkerHealthStatus::Ready },
    )
    .await;

    shutdown.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), run).await;
}

// ---------------------------------------------------------------------------
// A refused LISTEN connection at startup
// ---------------------------------------------------------------------------

#[pgqueue::job(name = "repro_cron_tick", max_attempts = 1)]
async fn repro_cron_tick(_: ()) -> anyhow::Result<()> {
    Ok(())
}

/// A worker whose only work is one dynamic cron on `expression`.
fn cron_worker(queue: &Queue, expression: &str, dedupe_key: &str) -> Worker {
    Worker::builder(queue.clone())
        .schedule_cron(expression, repro_cron_tick::job(()).dedupe_key(dedupe_key))
        .timers(WorkerTimers {
            schedule: Duration::from_millis(40),
            ..test_timers()
        })
        .poll_interval(Duration::from_millis(20))
        .build()
        .unwrap()
}

async fn cron_job_count(pool: &PgPool, queue: &str, dedupe_key: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM pgqueue.jobs WHERE queue = $1 AND dedupe_key = $2",
    )
    .bind(queue)
    .bind(dedupe_key)
    .fetch_one(pool)
    .await
    .unwrap()
}

/// Moves the durable cursor to `due`, which a
/// [`pgqueue::CronMisfirePolicy::Skip`] schedule publishes as its occurrence
/// while it is inside the grace. Returns it so a rewind can name the same
/// instant twice.
async fn set_cron_due(
    pool: &PgPool,
    queue: &str,
    dedupe_key: &str,
    due: Option<chrono::DateTime<chrono::Utc>>,
) -> chrono::DateTime<chrono::Utc> {
    sqlx::query_scalar(
        "UPDATE pgqueue.cron_schedules
         SET next_run_at = COALESCE($3, date_trunc('second', now()) - interval '2 seconds')
         WHERE queue = $1 AND dedupe_key = $2
         RETURNING next_run_at",
    )
    .bind(queue)
    .bind(dedupe_key)
    .bind(due)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn cron_next_run_at(
    pool: &PgPool,
    queue: &str,
    dedupe_key: &str,
) -> Option<chrono::DateTime<chrono::Utc>> {
    sqlx::query_scalar(
        "SELECT next_run_at FROM pgqueue.cron_schedules WHERE queue = $1 AND dedupe_key = $2",
    )
    .bind(queue)
    .bind(dedupe_key)
    .fetch_optional(pool)
    .await
    .unwrap()
}

/// An occurrence is claimed in `pgqueue.cron_occurrences` before its job row is
/// written, and the claim is what makes publication idempotent across workers.
/// The arm that observes a claim already taken had no test, so nothing pinned
/// "an occurrence is published at most once" — the one guarantee a durable cron
/// registry exists to provide.
#[sqlx::test(migrations = "./migrations")]
async fn test_cron_publishes_an_occurrence_at_most_once_when_the_cursor_is_rewound(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let key = "repro-already-published";
    let worker = cron_worker(&db.queue, "0 0 3 * * *", key);
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    wait_until(
        Duration::from_secs(15),
        Duration::from_millis(10),
        "the cron was never reconciled",
        || async {
            cron_next_run_at(&pool, db.queue.name(), key)
                .await
                .is_some()
        },
    )
    .await;

    let due = set_cron_due(&pool, db.queue.name(), key, None).await;
    wait_until(
        Duration::from_secs(15),
        Duration::from_millis(10),
        "the due occurrence was never published",
        || async { cron_job_count(&pool, db.queue.name(), key).await == 1 },
    )
    .await;
    let occurrence: chrono::DateTime<chrono::Utc> = sqlx::query_scalar(
        "SELECT scheduled_at FROM pgqueue.cron_occurrences WHERE queue = $1 AND dedupe_key = $2",
    )
    .bind(db.queue.name())
    .bind(key)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        occurrence, due,
        "the cursor is what was claimed and published"
    );

    // Remove the published job, so a second publication of the same occurrence
    // would be visible rather than indistinguishable from the first — and so
    // nothing holds the dedupe key, which would refuse the insert for its own
    // reason.
    sqlx::query("DELETE FROM pgqueue.jobs WHERE queue = $1 AND dedupe_key = $2")
        .bind(db.queue.name())
        .bind(key)
        .execute(&pool)
        .await
        .unwrap();

    // The same instant again: within the misfire grace the cursor *is* the
    // occurrence, so this pass recomputes exactly the one already claimed.
    set_cron_due(&pool, db.queue.name(), key, Some(due)).await;
    wait_until(
        Duration::from_secs(15),
        Duration::from_millis(10),
        "the scheduler never ran against the rewound cursor",
        || async {
            cron_next_run_at(&pool, db.queue.name(), key)
                .await
                .is_some_and(|next| next > chrono::Utc::now())
        },
    )
    .await;
    assert_eq!(
        cron_job_count(&pool, db.queue.name(), key).await,
        0,
        "an occurrence whose claim already exists must not be published twice"
    );
    let claims: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pgqueue.cron_occurrences
         WHERE queue = $1 AND dedupe_key = $2 AND scheduled_at = $3",
    )
    .bind(db.queue.name())
    .bind(key)
    .bind(occurrence)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        claims, 1,
        "the original claim is what refused the republish"
    );

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

/// `FOR UPDATE SKIP LOCKED` is what keeps two workers from both publishing the
/// same occurrence: the loser skips the row entirely and must leave the cursor
/// alone, so the occurrence is published by the winner and not lost. Nothing
/// covered the loser's side.
#[sqlx::test(migrations = "./migrations")]
async fn test_cron_publishes_nothing_while_another_transaction_holds_the_schedule(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let key = "repro-contended";
    let worker = cron_worker(&db.queue, "* * * * * *", key);
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    // Let it publish at least once, so the schedule is live and ticking.
    wait_until(
        Duration::from_secs(10),
        Duration::from_millis(10),
        "the per-second cron never published",
        || async { cron_job_count(&pool, db.queue.name(), key).await >= 1 },
    )
    .await;

    let mut holder = pool.begin().await.unwrap();
    let held: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
        "SELECT next_run_at FROM pgqueue.cron_schedules
         WHERE queue = $1 AND dedupe_key = $2 FOR UPDATE",
    )
    .bind(db.queue.name())
    .bind(key)
    .fetch_optional(&mut *holder)
    .await
    .unwrap();
    assert!(held.is_some());
    let published = cron_job_count(&pool, db.queue.name(), key).await;

    // Two seconds of a per-second cron: every tick in this window reaches the
    // locked row, skips it, and rolls back.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        cron_job_count(&pool, db.queue.name(), key).await,
        published,
        "a scheduler that skipped the locked row must not publish"
    );
    assert_eq!(
        cron_next_run_at(&pool, db.queue.name(), key).await,
        held,
        "and it must leave the cursor for the holder, not advance past it"
    );

    holder.rollback().await.unwrap();
    wait_until(
        Duration::from_secs(10),
        Duration::from_millis(10),
        "scheduling did not resume once the schedule row was released",
        || async { cron_job_count(&pool, db.queue.name(), key).await > published },
    )
    .await;

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

/// `schedule_cron` refuses to invent a schedule row: reconciliation owns the
/// durable definition, and publishing against a row that is not there would
/// mean publishing against no definition at all. The refusal degrades scheduler
/// health so an operator sees it, and that whole arm was untested.
#[sqlx::test(migrations = "./migrations")]
async fn test_cron_scheduling_degrades_health_when_its_schedule_row_disappears(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let key = "repro-unreconciled";
    let worker = cron_worker(&db.queue, "0 0 3 * * *", key);
    let health = worker.health();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    let scheduler_failure = || {
        health
            .snapshot()
            .failures
            .into_iter()
            .find(|failure| failure.component == WorkerComponent::Scheduler)
    };
    wait_until(
        Duration::from_secs(15),
        Duration::from_millis(10),
        "the cron was never reconciled",
        || async {
            cron_next_run_at(&pool, db.queue.name(), key)
                .await
                .is_some()
        },
    )
    .await;
    // Only the scheduler: an unrelated component degrading under load says
    // nothing about the arm under test.
    assert!(scheduler_failure().is_none());

    sqlx::query("DELETE FROM pgqueue.cron_schedules WHERE queue = $1 AND dedupe_key = $2")
        .bind(db.queue.name())
        .bind(key)
        .execute(&pool)
        .await
        .unwrap();
    // The scheduling loop repairs a lost row on its next pass
    // (`test_cron_reschedules_after_its_durable_row_is_removed`), so the
    // refusal is only stable to observe while nothing can rewrite the row.
    // An exclusive table lock parks that pass — readers are untouched, so the
    // scheduler still reaches the refusal — and holds the worker in the state
    // under test instead of leaving a one-tick window to catch.
    let mut reconcile_blocked = pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE pgqueue.cron_schedules IN EXCLUSIVE MODE")
        .execute(&mut *reconcile_blocked)
        .await
        .unwrap();

    let failure = crate::wait_for_some(
        Duration::from_secs(15),
        Duration::from_millis(10),
        "a missing schedule row was never reported",
        || async { scheduler_failure() },
    )
    .await;
    assert!(
        failure.message.contains("was not reconciled"),
        "the operator must be told the schedule row is missing: {}",
        failure.message
    );
    assert_eq!(health.snapshot().status, WorkerHealthStatus::Degraded);
    reconcile_blocked.rollback().await.unwrap();

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

/// And it repairs itself. `reconcile_crons` runs once, at startup, so the only
/// thing that can rewrite a schedule row a running worker lost is the retry
/// queue the scheduling loop drains first — the error names its own remedy, and
/// routing it anywhere else left the cron silently dead, and the worker
/// degraded, for the lifetime of the process.
#[sqlx::test(migrations = "./migrations")]
async fn test_cron_reschedules_after_its_durable_row_is_removed(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let key = "repro-row-removed";
    let worker = cron_worker(&db.queue, "* * * * * *", key);
    let health = worker.health();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    wait_until(
        Duration::from_secs(15),
        Duration::from_millis(10),
        "the per-second cron never published",
        || async { cron_job_count(&pool, db.queue.name(), key).await >= 1 },
    )
    .await;

    // Stale-definition cleanup, a `TRUNCATE` during an incident, or a restore
    // predating the cron. The delete waits behind any publication already in
    // flight, so the count read after it is a baseline nothing is racing.
    sqlx::query("DELETE FROM pgqueue.cron_schedules WHERE queue = $1 AND dedupe_key = $2")
        .bind(db.queue.name())
        .bind(key)
        .execute(&pool)
        .await
        .unwrap();
    let published = cron_job_count(&pool, db.queue.name(), key).await;

    wait_until(
        Duration::from_secs(15),
        Duration::from_millis(10),
        "the lost schedule row was never reconciled again",
        || async {
            cron_next_run_at(&pool, db.queue.name(), key)
                .await
                .is_some()
        },
    )
    .await;
    wait_until(
        Duration::from_secs(15),
        Duration::from_millis(10),
        "the cron never published again after losing its schedule row",
        || async { cron_job_count(&pool, db.queue.name(), key).await > published },
    )
    .await;
    assert_eq!(
        health.snapshot().status,
        WorkerHealthStatus::Ready,
        "a cron that repaired itself must stop degrading the worker"
    );

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .expect("worker did not stop")
        .unwrap()
        .unwrap();
}

/// The enqueue advisory lock only binds writers that take it, and the holder
/// check and the job insert are separate statements in one READ COMMITTED
/// transaction. A `BEFORE INSERT` trigger commits its row inside the
/// scheduler's own insert, which is exactly that interleaving with no timing to
/// arrange: it is the ops script, backfill or application `INSERT` this library
/// cannot stop from writing `pgqueue.jobs` directly.
async fn install_dedupe_usurper(pool: &PgPool) {
    sqlx::raw_sql(
        "CREATE FUNCTION pgqueue.repro_dedupe_usurper() RETURNS trigger
         LANGUAGE plpgsql AS $$
         BEGIN
             INSERT INTO pgqueue.jobs (queue, name, dedupe_key, status)
             VALUES (NEW.queue, 'usurper', NEW.dedupe_key, 'queued')
             ON CONFLICT DO NOTHING;
             RETURN NEW;
         END $$;
         CREATE TRIGGER repro_dedupe_usurper
         BEFORE INSERT ON pgqueue.jobs
         FOR EACH ROW WHEN (NEW.kind = 'cron')
         EXECUTE FUNCTION pgqueue.repro_dedupe_usurper();",
    )
    .execute(pool)
    .await
    .expect("install the dedupe usurper trigger");
}

fn ticker_worker(db: &TestDb) -> Worker {
    Worker::builder(db.queue.clone())
        .register_cron(repro_ticker)
        .timers(WorkerTimers {
            schedule: Duration::from_millis(50),
            ..test_timers()
        })
        .build()
        .expect("build cron worker")
}

/// Scheduling runs in the worker's schedule loop, so treating this conflict as
/// impossible cost the entire worker: the panic surfaced through the background
/// join as `Error::Task` instead of degrading `WorkerComponent::Scheduler`.
#[sqlx::test(migrations = "./migrations")]
async fn test_cron_scheduling_reports_a_dedupe_holder_that_appeared_after_its_check(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    install_dedupe_usurper(&pool).await;

    let shutdown = CancellationToken::new();
    let worker = ticker_worker(&db);
    let health = worker.health();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    wait_until(
        Duration::from_secs(10),
        Duration::from_millis(20),
        "the scheduler never committed an occurrence whose dedupe key was stolen",
        || async {
            assert!(
                !run.is_finished(),
                "the worker stopped instead of skipping the stolen occurrence"
            );
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM pgqueue.jobs WHERE queue = $1 AND name = 'usurper'",
            )
            .bind(db.queue.name())
            .fetch_one(&pool)
            .await
            .unwrap()
                > 0
        },
    )
    .await;

    // Skipping a held key is an ordinary outcome, not a failure, and the cursor
    // moved past the occurrence rather than retrying it forever.
    assert_eq!(health.snapshot().status, WorkerHealthStatus::Ready);
    let due: bool = sqlx::query_scalar(
        "SELECT next_run_at > now() FROM pgqueue.cron_schedules WHERE queue = $1",
    )
    .bind(db.queue.name())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        due,
        "the cron cursor must advance past a skipped occurrence"
    );

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .expect("worker did not stop")
        .unwrap()
        .unwrap();
}

/// And when the row that blocked the insert is gone again before it can be
/// named, the scheduler degrades with a diagnosis instead of panicking or
/// reporting a stale-misfire skip that would repeat every tick.
#[sqlx::test(migrations = "./migrations")]
async fn test_cron_scheduling_degrades_when_the_stolen_dedupe_key_is_released_again(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    install_dedupe_usurper(&pool).await;
    // The scheduler's insert is the one that ends with an empty transition
    // table — `ON CONFLICT DO NOTHING` swallowed its only row — so this retires
    // the usurper exactly then: after the conflict, before the holder re-read.
    sqlx::raw_sql(
        "CREATE FUNCTION pgqueue.repro_dedupe_release() RETURNS trigger
         LANGUAGE plpgsql AS $$
         BEGIN
             IF NOT EXISTS (SELECT 1 FROM inserted) THEN
                 UPDATE pgqueue.jobs SET status = 'complete', completed_at = now()
                 WHERE name = 'usurper' AND status = 'queued';
             END IF;
             RETURN NULL;
         END $$;
         CREATE TRIGGER repro_dedupe_release
         AFTER INSERT ON pgqueue.jobs
         REFERENCING NEW TABLE AS inserted
         FOR EACH STATEMENT EXECUTE FUNCTION pgqueue.repro_dedupe_release();",
    )
    .execute(&pool)
    .await
    .unwrap();

    let shutdown = CancellationToken::new();
    let worker = ticker_worker(&db);
    let health = worker.health();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));

    let failure = crate::wait_for_some(
        Duration::from_secs(10),
        Duration::from_millis(20),
        "the scheduler never reported the lost dedupe key",
        || async {
            assert!(
                !run.is_finished(),
                "the worker stopped instead of degrading its scheduler"
            );
            health
                .snapshot()
                .failures
                .into_iter()
                .find(|failure| failure.component == WorkerComponent::Scheduler)
        },
    )
    .await;
    assert!(
        failure.message.contains("lost its dedupe key"),
        "{}",
        failure.message
    );

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .expect("worker did not stop")
        .unwrap()
        .unwrap();
}

// ---------------------------------------------------------------------------
// Worker configuration that PostgreSQL will refuse must be refused up front
// ---------------------------------------------------------------------------

/// The `SELECT` that `schedule_cron` opens its transaction with. Identified by
/// `now() AS now`, which only that statement selects — `reconcile_cron` reads
/// the same table with an otherwise near-identical column list.
const CRON_SCHEDULE_READ: &str = "%next_run_at, now() AS now%cron_schedules%";
/// The pooled pre-filter that replaced it on the not-due path.
const CRON_DUE_FILTER: &str = "%LEFT JOIN pgqueue.cron_schedules%";

/// `schedule_cron` opens a transaction, reads the schedule row and — for a cron
/// that is not due, which is nearly every cron on nearly every tick — rolls it
/// back again. Calling it unconditionally for every registered cron cost
/// `BEGIN`/`SELECT`/`ROLLBACK` per cron, per worker, per tick purely to learn
/// there was nothing to do: at the one-second default that is O(crons x
/// workers) transactions a second against an idle registry.
#[sqlx::test(migrations = "./migrations")]
async fn test_cron_scheduling_reads_no_schedule_row_while_nothing_is_due(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let Some(mut stats) = Stats::new(&db.database).await else {
        return;
    };
    let key = "repro-idle-cron";
    // Daily at 03:00, so it is due at most once in the window this test runs.
    let worker = cron_worker(&db.queue, "0 0 3 * * *", key);
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    wait_until(
        Duration::from_secs(15),
        Duration::from_millis(10),
        "the cron was never reconciled",
        || async {
            cron_next_run_at(&pool, db.queue.name(), key)
                .await
                .is_some()
        },
    )
    .await;

    let reads = stats.since_now(CRON_SCHEDULE_READ).await;
    let filters = stats.since_now(CRON_DUE_FILTER).await;
    // Several scheduling ticks, waited for rather than slept through: at a
    // fixed 500 ms this guard measured how promptly a starved runtime delivers
    // a 40 ms timer, and failed on a loaded machine before the assertion it
    // exists to protect had been evaluated at all.
    stats
        .wait_for_calls(&filters, 3, "the scheduling loop did not tick")
        .await;
    assert_eq!(
        stats.delta(&reads).await,
        0,
        "a cron that is not due must cost no transaction of its own"
    );

    // And the pre-filter is only a pre-filter: a cron that becomes due still
    // publishes its occurrence.
    set_cron_due(&pool, db.queue.name(), key, None).await;
    wait_until(
        Duration::from_secs(15),
        Duration::from_millis(10),
        "a due cron was never published",
        || async { cron_job_count(&pool, db.queue.name(), key).await == 1 },
    )
    .await;
    assert!(
        stats.delta(&reads).await > 0,
        "a due cron must still be scheduled through schedule_cron"
    );

    shutdown.cancel();
    run.await.unwrap().unwrap();
}

// ---------------------------------------------------------------------------
// Invalid input is invalid input, with or without a dedupe key
// ---------------------------------------------------------------------------
