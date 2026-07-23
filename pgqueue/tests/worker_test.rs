//! End-to-end worker tests: processing, retries, timeouts, panics, aborts,
//! burst mode, and graceful shutdown against real Postgres.

use sqlx::PgPool;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::{
    EnqueueOutcomeTestExt, QueueProtocolTestExt, TestDb, backdate_job_liveness, pool_with_max,
    wait_for_some, wait_for_worker_intake_closed, wait_until,
};
use pgqueue::{
    Error, JobContext, JobError, JobErrorKind, JobRequest, JobState, JobStatus, Queue, Worker,
    WorkerComponent, WorkerHealthStatus, WorkerTimers,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Shared, clonable log of what handlers saw.
type Log = Arc<Mutex<Vec<String>>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Payload {
    tag: String,
}

#[pgqueue::job(max_attempts = 1)]
async fn record(args: Payload, log: JobState<Log>, ctx: JobContext) -> anyhow::Result<String> {
    // Exercise the full JobContext surface.
    anyhow::ensure!(ctx.attempt() >= 1);
    anyhow::ensure!(ctx.worker_id() != Uuid::nil());
    anyhow::ensure!(ctx.queue().name() == "default");
    anyhow::ensure!(!ctx.cancellation().is_cancelled());
    anyhow::ensure!(ctx.job().name == "record");
    anyhow::ensure!(format!("{ctx:?}").contains("record"));
    log.0
        .lock()
        .map_err(|_| anyhow::anyhow!("record log lock poisoned"))?
        .push(args.tag.clone());
    ctx.touch().await?;
    Ok(format!("done:{}", args.tag))
}

#[pgqueue::job(max_attempts = 2)]
async fn always_fails(_: ()) -> anyhow::Result<()> {
    anyhow::bail!("boom")
}

#[pgqueue::job(max_attempts = 1, timeout_ms = 200)]
async fn sleeps_forever(_: ()) -> anyhow::Result<()> {
    std::future::pending().await
}

#[pgqueue::job(max_attempts = 1, timeout_ms = 30_000)]
async fn slow_but_abortable(_: ()) -> anyhow::Result<()> {
    std::future::pending().await
}

#[derive(Clone)]
struct ShutdownLeaseProbe {
    started: Arc<tokio::sync::Notify>,
}

#[pgqueue::job(max_attempts = 1, timeout_ms = 30_000)]
async fn holds_during_shutdown(_: (), probe: JobState<ShutdownLeaseProbe>) -> anyhow::Result<()> {
    probe.0.started.notify_one();
    std::future::pending().await
}

#[pgqueue::job(max_attempts = 1)]
async fn cancels_observer_token(_: (), ctx: JobContext) -> anyhow::Result<()> {
    let cancellation = ctx.cancellation();
    cancellation.cancel();
    anyhow::ensure!(cancellation.is_cancelled());
    Ok(())
}

#[pgqueue::job(max_attempts = 1)]
async fn panics(_: ()) -> anyhow::Result<()> {
    panic!("kaboom {}", 42); // String panic payload
}

#[pgqueue::job(max_attempts = 1)]
async fn panics_static(_: ()) -> anyhow::Result<()> {
    panic!("static kaboom"); // &'static str panic payload
}

#[pgqueue::job(max_attempts = 1)]
async fn panics_weird(_: ()) -> anyhow::Result<()> {
    std::panic::panic_any(42u32); // non-string panic payload
}

#[pgqueue::job(max_attempts = 5)]
async fn needs_missing_state(_: (), missing: JobState<Uuid>) -> anyhow::Result<()> {
    let _ = missing;
    Ok(())
}

#[pgqueue::job(max_attempts = 5)]
async fn decodes_payload(args: Payload) -> anyhow::Result<()> {
    let _ = args;
    Ok(())
}

#[pgqueue::job(max_attempts = 5)]
async fn returns_decode_error(_: ()) -> Result<(), JobError> {
    Err(JobError::new(
        JobErrorKind::Decode,
        "handler decode failure",
    ))
}

#[pgqueue::job]
async fn counts(_: (), counter: JobState<Arc<AtomicU32>>) -> anyhow::Result<u32> {
    Ok(counter.0.fetch_add(1, Ordering::SeqCst) + 1)
}

#[derive(Clone)]
struct LeakedContext {
    next_attempt: Arc<tokio::sync::Notify>,
    checked: Arc<tokio::sync::Notify>,
    result: tokio::sync::mpsc::UnboundedSender<bool>,
}

#[pgqueue::job(max_attempts = 2, timeout_ms = 2_000, heartbeat_ms = 1_000)]
async fn leaks_old_context(
    _: (),
    state: JobState<LeakedContext>,
    ctx: JobContext,
) -> anyhow::Result<()> {
    if ctx.attempt() == 1 {
        let next_attempt = state.0.next_attempt.clone();
        let checked = state.0.checked.clone();
        tokio::spawn(async move {
            next_attempt.notified().await;
            let _ = state.0.result.send(ctx.touch().await.is_err());
            checked.notify_one();
        });
        anyhow::bail!("retry with a leaked context");
    }
    state.0.next_attempt.notify_one();
    state.0.checked.notified().await;
    Ok(())
}

#[derive(Clone)]
struct AbortingTouch {
    trigger: Arc<tokio::sync::Notify>,
    result: tokio::sync::mpsc::UnboundedSender<bool>,
}

#[pgqueue::job(max_attempts = 1, timeout_ms = 30_000, heartbeat_ms = 1_000)]
async fn touches_while_aborting(
    _: (),
    state: JobState<AbortingTouch>,
    ctx: JobContext,
) -> anyhow::Result<()> {
    state.0.trigger.notified().await;
    let _ = state.0.result.send(ctx.touch().await.is_ok());
    std::future::pending().await
}

#[pgqueue::job(max_attempts = 2, timeout_ms = 30_000, heartbeat_ms = 20)]
async fn swept_once(_: (), attempts: JobState<Arc<AtomicU32>>) -> anyhow::Result<()> {
    if attempts.0.fetch_add(1, Ordering::SeqCst) == 0 {
        std::future::pending::<()>().await;
    }
    Ok(())
}

#[derive(Clone)]
struct AbortFailureRace {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[derive(Clone)]
struct AbortSuccessRace {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[pgqueue::job(max_attempts = 1, timeout_ms = 30_000)]
async fn succeeds_during_abort(_: (), state: JobState<AbortSuccessRace>) -> anyhow::Result<String> {
    state.0.started.notify_one();
    state.0.release.notified().await;
    Ok("handler finished".to_string())
}

#[pgqueue::job(max_attempts = 1, timeout_ms = 30_000, heartbeat_ms = 20)]
async fn succeeds_as_sweeper_marks(_: (), state: JobState<AbortSuccessRace>) -> anyhow::Result<()> {
    state.0.started.notify_one();
    state.0.release.notified().await;
    Ok(())
}

#[pgqueue::job(max_attempts = 1, timeout_ms = 30_000)]
async fn fails_during_abort(_: (), state: JobState<AbortFailureRace>) -> anyhow::Result<()> {
    state.0.started.notify_one();
    state.0.release.notified().await;
    anyhow::bail!("handler failed while abort was pending")
}

#[derive(Clone)]
struct SweepFailureRace {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[pgqueue::job(max_attempts = 2, timeout_ms = 30_000, heartbeat_ms = 20)]
async fn fails_as_sweeper_marks(
    _: (),
    state: JobState<SweepFailureRace>,
    ctx: JobContext,
) -> anyhow::Result<()> {
    if ctx.attempt() == 1 {
        state.0.started.notify_one();
        state.0.release.notified().await;
        anyhow::bail!("handler failed as sweep abort landed");
    }
    Ok(())
}

use crate::test_timers;

fn test_worker(queue: Queue) -> pgqueue::WorkerBuilder {
    Worker::builder(queue)
        .timers(test_timers())
        .poll_interval(Duration::from_millis(50))
        .shutdown_grace(Duration::from_secs(5))
}

/// Polls until the job reaches a terminal status (or the deadline passes).
async fn wait_terminal(queue: &Queue, id: Uuid, secs: u64) -> pgqueue::JobRow {
    wait_for_some(
        Duration::from_secs(secs),
        Duration::from_millis(25),
        &format!("job {id} never finished"),
        || async {
            queue
                .job(id)
                .await
                .unwrap()
                .filter(|row| row.status.is_terminal())
        },
    )
    .await
}

async fn wait_status(queue: &Queue, id: Uuid, status: JobStatus, secs: u64) -> pgqueue::JobRow {
    wait_for_some(
        Duration::from_secs(secs),
        Duration::from_millis(25),
        &format!("job {id} never reached {status}"),
        || async {
            queue
                .job(id)
                .await
                .unwrap()
                .filter(|row| row.status == status)
        },
    )
    .await
}

#[sqlx::test(migrations = "./migrations")]
async fn worker_health_tracks_start_ready_and_stopped(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let worker = test_worker(db.queue.clone())
        .register(counts)
        .state(Arc::new(AtomicU32::new(0)))
        .build()
        .unwrap();
    let health = worker.health();
    assert_eq!(health.snapshot().status, WorkerHealthStatus::Starting);
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    wait_until(
        Duration::from_secs(3),
        Duration::from_millis(10),
        "worker did not report ready health",
        || async { health.snapshot().status == WorkerHealthStatus::Ready },
    )
    .await;

    shutdown.cancel();
    run.await.unwrap().unwrap();
    assert_eq!(health.snapshot().status, WorkerHealthStatus::Stopped);
}

#[sqlx::test(migrations = "./migrations")]
async fn worker_processes_typed_jobs_end_to_end(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let log: Log = Arc::new(Mutex::new(Vec::new()));

    let mut handles = Vec::new();
    for i in 0..3 {
        let handle = db
            .queue
            .enqueue(record::job(Payload {
                tag: format!("job{i}"),
            }))
            .await
            .unwrap()
            .unwrap();
        handles.push(handle);
    }

    let worker = test_worker(db.queue.clone())
        .register(record)
        .state(log.clone())
        .concurrency(2)
        .build()
        .unwrap();
    assert_ne!(worker.id(), Uuid::nil());
    assert_eq!(worker.queue().name(), "default");
    assert!(format!("{worker:?}").contains("Worker"));

    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    for (i, handle) in handles.iter().enumerate() {
        let row = wait_terminal(&db.queue, handle.id(), 10).await;
        assert_eq!(row.status, JobStatus::Complete, "{:?}", row.error);
        assert_eq!(row.result, Some(json!(format!("done:job{i}"))));
        assert!(row.completed_at.is_some());
        assert!(row.worker_id.is_some());
    }
    let mut seen = log.lock().unwrap().clone();
    seen.sort();
    assert_eq!(seen, vec!["job0", "job1", "job2"]);

    token.cancel();
    run.await.unwrap().unwrap();
    assert_eq!(db.queue.stats().complete, 3);
}

#[sqlx::test(migrations = "./migrations")]
async fn job_context_cancellation_does_not_cancel_attempt(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let handle = db
        .queue
        .enqueue(cancels_observer_token::job(()))
        .await
        .unwrap()
        .unwrap();
    let worker = test_worker(db.queue.clone())
        .register(cancels_observer_token)
        .build()
        .unwrap();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    let row = wait_terminal(&db.queue, handle.id(), 10).await;
    token.cancel();
    run.await.unwrap().unwrap();

    assert_eq!(row.status, JobStatus::Complete);
    assert_eq!(row.attempts, 1);
    assert_eq!(db.queue.stats().complete, 1);
    assert_eq!(db.queue.stats().retried, 0);
}

#[sqlx::test(migrations = "./migrations")]
async fn one_connection_pool_still_processes_jobs_after_listener_start(pool: PgPool) {
    let pool = pool_with_max(&pool, 1).await;
    let db = TestDb::new(pool).await;
    let handle = db.queue.enqueue(counts::job(())).await.unwrap().unwrap();
    let counter = Arc::new(AtomicU32::new(0));
    let worker = test_worker(db.queue.clone())
        .register(counts)
        .state(counter.clone())
        .burst(true)
        .dequeue_timeout(Duration::from_millis(400))
        .build()
        .unwrap();

    tokio::time::timeout(
        Duration::from_secs(10),
        worker.run_until(CancellationToken::new()),
    )
    .await
    .expect("the LISTEN connection must not consume the only pooled connection")
    .unwrap();

    assert_eq!(counter.load(Ordering::SeqCst), 1);
    assert_eq!(handle.refresh().await.unwrap().status, JobStatus::Complete);
}

#[sqlx::test(migrations = "./migrations")]
async fn failing_job_retries_then_fails(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let handle = db
        .queue
        .enqueue(always_fails::job(()))
        .await
        .unwrap()
        .unwrap();

    let worker = test_worker(db.queue.clone())
        .register(always_fails)
        .build()
        .unwrap();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    let row = wait_terminal(&db.queue, handle.id(), 10).await;
    assert_eq!(row.status, JobStatus::Failed);
    assert_eq!(row.attempts, 2, "one retry (max_attempts = 2)");
    assert_eq!(row.error.as_deref(), Some("failed: boom"));

    token.cancel();
    run.await.unwrap().unwrap();
    assert_eq!(db.queue.stats().failed, 1);
    assert_eq!(db.queue.stats().retried, 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn worker_times_out_slow_job(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let handle = db
        .queue
        .enqueue(sleeps_forever::job(()))
        .await
        .unwrap()
        .unwrap();

    let worker = test_worker(db.queue.clone())
        .register(sleeps_forever)
        .build()
        .unwrap();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    let row = wait_terminal(&db.queue, handle.id(), 10).await;
    assert_eq!(row.status, JobStatus::Failed);
    assert!(
        row.error
            .as_deref()
            .unwrap_or_default()
            .starts_with("timeout:"),
        "{:?}",
        row.error
    );

    token.cancel();
    run.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn panicking_job_fails_without_killing_the_worker(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let boom = db.queue.enqueue(panics::job(())).await.unwrap().unwrap();
    let fine = db
        .queue
        .enqueue(record::job(Payload {
            tag: "survivor".into(),
        }))
        .await
        .unwrap()
        .unwrap();

    let boom_static = db
        .queue
        .enqueue(panics_static::job(()))
        .await
        .unwrap()
        .unwrap();
    let boom_weird = db
        .queue
        .enqueue(panics_weird::job(()))
        .await
        .unwrap()
        .unwrap();
    let log: Log = Arc::new(Mutex::new(Vec::new()));
    let worker = test_worker(db.queue.clone())
        .register(panics)
        .register(panics_static)
        .register(panics_weird)
        .register(record)
        .state(log)
        .build()
        .unwrap();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    let row = wait_terminal(&db.queue, boom.id(), 10).await;
    assert_eq!(row.status, JobStatus::Failed);
    assert_eq!(row.error.as_deref(), Some("panic: kaboom 42"));

    let row = wait_terminal(&db.queue, boom_static.id(), 10).await;
    assert_eq!(row.error.as_deref(), Some("panic: static kaboom"));

    let row = wait_terminal(&db.queue, boom_weird.id(), 10).await;
    assert_eq!(row.error.as_deref(), Some("panic: handler panicked"));

    // The worker survived the panic and keeps processing.
    let row = wait_terminal(&db.queue, fine.id(), 10).await;
    assert_eq!(row.status, JobStatus::Complete);

    token.cancel();
    run.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn abort_cancels_a_running_job(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let handle = db
        .queue
        .enqueue(slow_but_abortable::job(()))
        .await
        .unwrap()
        .unwrap();
    let worker = test_worker(db.queue.clone())
        .register(slow_but_abortable)
        .build()
        .unwrap();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    wait_status(&db.queue, handle.id(), JobStatus::Running, 10).await;
    assert!(handle.abort("operator said stop").await.unwrap());

    let row = wait_terminal(&db.queue, handle.id(), 10).await;
    assert_eq!(row.status, JobStatus::Aborted);
    assert_eq!(row.error.as_deref(), Some("operator said stop"));

    token.cancel();
    run.await.unwrap().unwrap();
    assert_eq!(db.queue.stats().aborted, 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn pending_abort_wins_over_a_final_attempt_failure(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let handle = db
        .queue
        .enqueue(fails_during_abort::job(()))
        .await
        .unwrap()
        .unwrap();
    let state = AbortFailureRace {
        started: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
    };
    let worker = test_worker(db.queue.clone())
        .register(fails_during_abort)
        .state(state.clone())
        .timers(WorkerTimers {
            abort: Duration::from_secs(5),
            ..test_timers()
        })
        .build()
        .unwrap();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    state.started.notified().await;
    // Let the abort loop consume its immediate first interval tick. Its next
    // poll is deliberately later than the handler failure below.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(handle.abort("operator abort won").await.unwrap());
    state.release.notify_one();

    let row = wait_terminal(&db.queue, handle.id(), 5).await;
    assert_eq!(row.status, JobStatus::Aborted);
    assert_eq!(row.error.as_deref(), Some("operator abort won"));

    token.cancel();
    run.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn pending_abort_wins_over_a_successful_handler(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let handle = db
        .queue
        .enqueue(succeeds_during_abort::job(()))
        .await
        .unwrap()
        .unwrap();
    let state = AbortSuccessRace {
        started: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
    };
    let worker = test_worker(db.queue.clone())
        .register(succeeds_during_abort)
        .state(state.clone())
        .timers(WorkerTimers {
            abort: Duration::from_secs(5),
            ..test_timers()
        })
        .build()
        .unwrap();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    state.started.notified().await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(handle.abort("operator abort won").await.unwrap());
    state.release.notify_one();

    let row = wait_terminal(&db.queue, handle.id(), 5).await;
    assert_eq!(row.status, JobStatus::Aborted);
    assert_eq!(row.error.as_deref(), Some("operator abort won"));
    assert!(row.result.is_none());

    token.cancel();
    run.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn sweeper_abort_racing_a_retryable_failure_still_retries(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let mut sweeper = db.queue.sweeper();
    assert!(sweeper.sweep().await.unwrap().leader);

    let handle = db
        .queue
        .enqueue(fails_as_sweeper_marks::job(()))
        .await
        .unwrap()
        .unwrap();
    let state = SweepFailureRace {
        started: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
    };
    let worker = test_worker(db.queue.clone())
        .register(fails_as_sweeper_marks)
        .state(state.clone())
        .timers(WorkerTimers {
            abort: Duration::from_secs(5),
            ..test_timers()
        })
        .build()
        .unwrap();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    state.started.notified().await;
    backdate_job_liveness(&db, handle.id()).await;
    assert_eq!(sweeper.sweep().await.unwrap().cancelling, vec![handle.id()]);
    state.release.notify_one();

    let row = wait_terminal(&db.queue, handle.id(), 5).await;
    assert_eq!(row.status, JobStatus::Complete);
    assert_eq!(row.attempts, 2);

    token.cancel();
    run.await.unwrap().unwrap();
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn successful_handler_finishes_through_a_sweeper_abort(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let mut sweeper = db.queue.sweeper();
    assert!(sweeper.sweep().await.unwrap().leader);
    let handle = db
        .queue
        .enqueue(succeeds_as_sweeper_marks::job(()))
        .await
        .unwrap()
        .unwrap();
    let state = AbortSuccessRace {
        started: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
    };
    let worker = test_worker(db.queue.clone())
        .register(succeeds_as_sweeper_marks)
        .state(state.clone())
        .timers(WorkerTimers {
            abort: Duration::from_secs(5),
            ..test_timers()
        })
        .build()
        .unwrap();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    state.started.notified().await;
    backdate_job_liveness(&db, handle.id()).await;
    assert_eq!(sweeper.sweep().await.unwrap().cancelling, vec![handle.id()]);
    state.release.notify_one();

    let row = wait_terminal(&db.queue, handle.id(), 5).await;
    assert_eq!(row.status, JobStatus::Complete);
    assert_eq!(row.attempts, 1);
    assert_eq!(db.queue.counts().await.unwrap().queued, 0);

    token.cancel();
    run.await.unwrap().unwrap();
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn final_handler_failure_finishes_through_a_sweeper_abort(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let mut sweeper = db.queue.sweeper();
    assert!(sweeper.sweep().await.unwrap().leader);
    let handle = db
        .queue
        .enqueue(fails_as_sweeper_marks::job(()).max_attempts(1))
        .await
        .unwrap()
        .unwrap();
    let state = SweepFailureRace {
        started: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
    };
    let worker = test_worker(db.queue.clone())
        .register(fails_as_sweeper_marks)
        .state(state.clone())
        .timers(WorkerTimers {
            abort: Duration::from_secs(5),
            ..test_timers()
        })
        .build()
        .unwrap();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    state.started.notified().await;
    backdate_job_liveness(&db, handle.id()).await;
    assert_eq!(sweeper.sweep().await.unwrap().cancelling, vec![handle.id()]);
    state.release.notify_one();

    let row = wait_terminal(&db.queue, handle.id(), 5).await;
    assert_eq!(row.status, JobStatus::Failed);
    assert_eq!(
        row.error.as_deref(),
        Some("failed: handler failed as sweep abort landed")
    );

    token.cancel();
    run.await.unwrap().unwrap();
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn undecodable_payload_fails_with_decode_error(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    // Enqueue raw JSON that does not match `Payload`.
    let id = db
        .queue
        .enqueue_raw(JobRequest::new("record", json!({"wrong": "shape"})))
        .await
        .unwrap()
        .unwrap();

    let log: Log = Arc::new(Mutex::new(Vec::new()));
    let worker = test_worker(db.queue.clone())
        .register(record)
        .state(log)
        .build()
        .unwrap();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    let row = wait_terminal(&db.queue, id, 10).await;
    assert_eq!(row.status, JobStatus::Failed);
    assert!(
        row.error
            .as_deref()
            .unwrap_or_default()
            .starts_with("decode:"),
        "{:?}",
        row.error
    );

    token.cancel();
    run.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn missing_state_fails_with_extract_error(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let handle = db
        .queue
        .enqueue(needs_missing_state::job(()))
        .await
        .unwrap()
        .unwrap();

    let worker = test_worker(db.queue.clone())
        .register(needs_missing_state)
        .build()
        .unwrap();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    let row = wait_terminal(&db.queue, handle.id(), 10).await;
    assert_eq!(row.status, JobStatus::Failed);
    // Extract failures are deterministic: no retry despite max_attempts = 5.
    assert_eq!(row.attempts, 1);
    let error = row.error.as_deref().unwrap_or_default();
    assert!(
        error.starts_with("extract:") && error.contains("Uuid"),
        "{error:?}"
    );

    token.cancel();
    run.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn undecodable_payload_fails_without_retry_when_attempts_remain(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(JobRequest::new(
            "decodes_payload",
            json!({"wrong": "shape"}),
        ))
        .await
        .unwrap()
        .unwrap();

    let worker = test_worker(db.queue.clone())
        .register(decodes_payload)
        .build()
        .unwrap();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    let row = wait_terminal(&db.queue, id, 10).await;
    assert_eq!(row.status, JobStatus::Failed);
    // Decode failures are deterministic: no retry despite max_attempts = 5.
    assert_eq!(row.attempts, 1);
    assert!(
        row.error
            .as_deref()
            .unwrap_or_default()
            .starts_with("decode:"),
        "{:?}",
        row.error
    );

    token.cancel();
    run.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn returned_job_error_preserves_its_kind_and_retry_policy(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let handle = db
        .queue
        .enqueue(returns_decode_error::job(()))
        .await
        .unwrap()
        .unwrap();

    let worker = test_worker(db.queue.clone())
        .register(returns_decode_error)
        .build()
        .unwrap();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    let row = wait_terminal(&db.queue, handle.id(), 10).await;
    assert_eq!(row.status, JobStatus::Failed);
    assert_eq!(row.attempts, 1, "decode errors must not retry");
    assert_eq!(row.error.as_deref(), Some("decode: handler decode failure"));

    token.cancel();
    run.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn workers_only_dequeue_registered_job_names(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let handle = db.queue.enqueue(counts::job(())).await.unwrap().unwrap();

    let worker = test_worker(db.queue.clone())
        .register(always_fails)
        .burst(true)
        .dequeue_timeout(Duration::from_millis(200))
        .build()
        .unwrap();
    worker.run_until(CancellationToken::new()).await.unwrap();

    let row = handle.refresh().await.unwrap();
    assert_eq!(row.status, JobStatus::Queued);
    assert_eq!(row.attempts, 0, "an incompatible worker must not claim it");

    let counter = Arc::new(AtomicU32::new(0));
    let worker = test_worker(db.queue.clone())
        .register(counts)
        .state(counter.clone())
        .burst(true)
        .dequeue_timeout(Duration::from_millis(200))
        .build()
        .unwrap();
    worker.run_until(CancellationToken::new()).await.unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 1);
    assert_eq!(handle.refresh().await.unwrap().status, JobStatus::Complete);
}

#[sqlx::test(migrations = "./migrations")]
async fn registered_name_filter_applies_through_grouped_batch_dequeues(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    db.queue
        .enqueue_raw(JobRequest::new("unhandled", json!(null)))
        .await
        .unwrap();
    let handles = vec![
        db.queue
            .enqueue(counts::job(()).group_key("serial"))
            .await
            .unwrap()
            .unwrap(),
        db.queue
            .enqueue(counts::job(()).group_key("serial"))
            .await
            .unwrap()
            .unwrap(),
        db.queue
            .enqueue(counts::job(()).group_key("parallel"))
            .await
            .unwrap()
            .unwrap(),
    ];

    let counter = Arc::new(AtomicU32::new(0));
    test_worker(db.queue.clone())
        .register(counts)
        .state(counter.clone())
        .concurrency(3)
        .burst(true)
        .dequeue_timeout(Duration::from_millis(200))
        .build()
        .unwrap()
        .run_until(CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(counter.load(Ordering::SeqCst), 3);
    for handle in handles {
        assert_eq!(handle.refresh().await.unwrap().status, JobStatus::Complete);
    }
    let unhandled: String =
        sqlx::query_scalar!("SELECT status FROM pgqueue.jobs WHERE name = 'unhandled'")
            .fetch_one(db.queue.pool())
            .await
            .unwrap();
    assert_eq!(unhandled, "queued");
}

#[sqlx::test(migrations = "./migrations")]
async fn registered_name_filter_preserves_group_ready_order(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let earlier = db
        .queue
        .enqueue(returns_decode_error::job(()).group_key("serial"))
        .await
        .unwrap()
        .unwrap();
    let later = db
        .queue
        .enqueue(counts::job(()).group_key("serial"))
        .await
        .unwrap()
        .unwrap();
    let counter = Arc::new(AtomicU32::new(0));

    tokio::time::timeout(
        Duration::from_secs(3),
        test_worker(db.queue.clone())
            .register(counts)
            .state(counter.clone())
            .burst(true)
            .dequeue_timeout(Duration::from_millis(200))
            .build()
            .unwrap()
            .run_until(CancellationToken::new()),
    )
    .await
    .expect("burst worker did not stop behind the unregistered group head")
    .unwrap();
    assert_eq!(earlier.refresh().await.unwrap().status, JobStatus::Queued);
    assert_eq!(later.refresh().await.unwrap().status, JobStatus::Queued);
    assert_eq!(counter.load(Ordering::SeqCst), 0);

    test_worker(db.queue.clone())
        .register(returns_decode_error)
        .burst(true)
        .dequeue_timeout(Duration::from_millis(200))
        .build()
        .unwrap()
        .run_until(CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(earlier.refresh().await.unwrap().status, JobStatus::Failed);

    test_worker(db.queue.clone())
        .register(counts)
        .state(counter.clone())
        .burst(true)
        .dequeue_timeout(Duration::from_millis(200))
        .build()
        .unwrap()
        .run_until(CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(later.refresh().await.unwrap().status, JobStatus::Complete);
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

const DEQUEUE_HANDOFF_GATE: i32 = 20_561;
const DEQUEUE_CONNECTION_GATE: i32 = 20_562;
const SHUTDOWN_REQUEUE_GATE: i32 = 20_563;

async fn install_dequeue_handoff_gate(pool: &PgPool) {
    sqlx::raw_sql(
        r#"
        CREATE FUNCTION pgqueue.wait_at_dequeue_handoff() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            PERFORM pg_advisory_xact_lock(20561, hashtext(current_database()));
            RETURN NEW;
        END
        $$;
        CREATE TRIGGER wait_at_dequeue_handoff
        BEFORE UPDATE ON pgqueue.jobs
        FOR EACH ROW
        WHEN (OLD.status = 'queued' AND NEW.status = 'running')
        EXECUTE FUNCTION pgqueue.wait_at_dequeue_handoff();
        "#,
    )
    .execute(pool)
    .await
    .expect("install dequeue handoff gate");
}

async fn install_shutdown_requeue_gate(pool: &PgPool) {
    sqlx::raw_sql(
        r#"
        CREATE FUNCTION pgqueue.wait_at_shutdown_requeue() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            PERFORM pg_advisory_xact_lock(20563, hashtext(current_database()));
            RETURN NEW;
        END
        $$;
        CREATE TRIGGER wait_at_shutdown_requeue
        BEFORE UPDATE ON pgqueue.jobs
        FOR EACH ROW
        WHEN (OLD.status = 'running' AND NEW.status = 'queued' AND NEW.error = 'cancelled')
        EXECUTE FUNCTION pgqueue.wait_at_shutdown_requeue();
        "#,
    )
    .execute(pool)
    .await
    .expect("install shutdown requeue gate");
}

#[sqlx::test(migrations = "./migrations")]
async fn graceful_shutdown_requeues_inflight_jobs(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let handle = db
        .queue
        .enqueue(slow_but_abortable::job(()).retry_delay(Duration::from_secs(60 * 60)))
        .await
        .unwrap()
        .unwrap();
    let scheduled_at = handle.refresh().await.unwrap().scheduled_at;

    let worker = test_worker(db.queue.clone())
        .register(slow_but_abortable)
        .shutdown_grace(Duration::from_millis(100))
        .build()
        .unwrap();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    wait_status(&db.queue, handle.id(), JobStatus::Running, 10).await;
    token.cancel();
    run.await.unwrap().unwrap();

    let row = handle.refresh().await.unwrap();
    assert_eq!(row.status, JobStatus::Queued, "requeued on shutdown");
    assert_eq!(row.error.as_deref(), Some("cancelled"));
    assert_eq!(row.attempts, 1, "execution history remains monotonic");
    assert_eq!(row.max_attempts, 2, "shutdown refunds the retry budget");
    assert_eq!(db.queue.stats().retried, 1);
    assert_eq!(
        row.scheduled_at, scheduled_at,
        "shutdown does not apply failure backoff"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn graceful_shutdown_waits_for_buffered_job_requeue(pool: PgPool) {
    let control = TestDb::new(pool_with_max(&pool, 6).await).await;
    let db = TestDb::new(pool_with_max(&pool, 5).await).await;
    install_dequeue_handoff_gate(control.queue.pool()).await;
    install_shutdown_requeue_gate(control.queue.pool()).await;

    let mut dequeue_gate = control.queue.pool().begin().await.unwrap();
    sqlx::query!(
        "SELECT pg_advisory_xact_lock($1, hashtext($2))",
        DEQUEUE_HANDOFF_GATE,
        control.database
    )
    .execute(&mut *dequeue_gate)
    .await
    .unwrap();
    let mut requeue_gate = control.queue.pool().begin().await.unwrap();
    sqlx::query!(
        "SELECT pg_advisory_xact_lock($1, hashtext($2))",
        SHUTDOWN_REQUEUE_GATE,
        control.database
    )
    .execute(&mut *requeue_gate)
    .await
    .unwrap();

    let counter = Arc::new(AtomicU32::new(0));
    let handle = control
        .queue
        .enqueue(counts::job(()))
        .await
        .unwrap()
        .unwrap();
    let worker = test_worker(db.queue.clone())
        .register(counts)
        .state(counter.clone())
        .concurrency(1)
        .timers(WorkerTimers {
            abort: Duration::from_secs(60),
            worker_info: Duration::from_millis(25),
            ..test_timers()
        })
        .shutdown_grace(Duration::from_secs(10))
        .build()
        .unwrap();
    let worker_id = worker.id();
    let token = CancellationToken::new();
    let mut run = tokio::spawn(worker.run_until(token.clone()));

    crate::wait_for_lock_waiter(
        &control,
        "%WITH candidates AS (%",
        "worker did not pause while dequeuing",
    )
    .await;
    let mut worker_gate = control.queue.pool().begin().await.unwrap();
    sqlx::query!(
        "UPDATE pgqueue.workers SET expires_at = now() - interval '1 second' WHERE id = $1",
        worker_id
    )
    .execute(&mut *worker_gate)
    .await
    .unwrap();

    token.cancel();
    crate::wait_for_lock_waiter(
        &control,
        "%UPDATE pgqueue.workers SET accepting = false%",
        "shutdown did not freeze worker intake",
    )
    .await;
    dequeue_gate.rollback().await.unwrap();
    crate::wait_for_lock_waiter(
        &control,
        "%WITH requeued AS (%",
        "fetcher did not begin draining its buffered job",
    )
    .await;
    assert_eq!(handle.refresh().await.unwrap().status, JobStatus::Running);

    worker_gate.rollback().await.unwrap();
    wait_for_worker_intake_closed(&control, worker_id).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(250), &mut run)
            .await
            .is_err(),
        "worker returned before its buffered job was requeued"
    );

    requeue_gate.rollback().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("worker did not finish after its buffered job was requeued")
        .unwrap()
        .unwrap();

    let row = handle.refresh().await.unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 0);
    assert_eq!(row.status, JobStatus::Queued);
    assert_eq!(row.error.as_deref(), Some("cancelled"));
    assert_eq!(db.queue.stats().retried, 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn graceful_shutdown_finalizes_abort_when_job_is_buffered(pool: PgPool) {
    let control = TestDb::new(pool_with_max(&pool, 5).await).await;
    let db = TestDb::new(pool_with_max(&pool, 2).await).await;
    let counter = Arc::new(AtomicU32::new(0));
    let probe = DatabaseLossProbe {
        started: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
    };
    let grace_job = control
        .queue
        .enqueue(quick_nap::job(()))
        .await
        .unwrap()
        .unwrap();

    let worker = test_worker(db.queue.clone())
        .register(counts)
        .register(quick_nap)
        .state(counter.clone())
        .state(probe.clone())
        .concurrency(3)
        .timers(WorkerTimers {
            abort: Duration::from_secs(60),
            worker_info: Duration::from_secs(60),
            ..test_timers()
        })
        .shutdown_grace(Duration::from_secs(10))
        .build()
        .unwrap();
    let worker_id = worker.id();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    tokio::time::timeout(Duration::from_secs(5), probe.started.notified())
        .await
        .expect("grace job did not start");
    install_dequeue_handoff_gate(control.queue.pool()).await;
    let mut dequeue_gate = control.queue.pool().begin().await.unwrap();
    sqlx::query!(
        "SELECT pg_advisory_xact_lock($1, hashtext($2))",
        DEQUEUE_HANDOFF_GATE,
        control.database
    )
    .execute(&mut *dequeue_gate)
    .await
    .unwrap();
    let mut connection_gate = control.queue.pool().begin().await.unwrap();
    sqlx::query!(
        "SELECT pg_advisory_xact_lock($1, hashtext($2))",
        DEQUEUE_CONNECTION_GATE,
        control.database
    )
    .execute(&mut *connection_gate)
    .await
    .unwrap();
    let handle = control
        .queue
        .enqueue(
            counts::job(())
                .timeout(Duration::from_secs(60 * 60))
                .heartbeat(Duration::from_secs(60 * 60)),
        )
        .await
        .unwrap()
        .unwrap();
    crate::wait_for_lock_waiter(
        &control,
        "%WITH candidates AS (%",
        "worker did not pause while dequeuing",
    )
    .await;

    let mut worker_gate = control.queue.pool().begin().await.unwrap();
    sqlx::query!(
        "UPDATE pgqueue.workers SET expires_at = now() - interval '1 second' WHERE id = $1",
        worker_id
    )
    .execute(&mut *worker_gate)
    .await
    .unwrap();
    token.cancel();
    crate::wait_for_lock_waiter(
        &control,
        "%UPDATE pgqueue.workers SET accepting = false%",
        "shutdown did not freeze worker intake",
    )
    .await;

    let worker_pool = db.queue.pool().clone();
    let database = db.database.clone();
    let mut connection_stealer = tokio::spawn(async move {
        sqlx::query!(
            "SELECT pg_advisory_xact_lock($1, hashtext($2))",
            DEQUEUE_CONNECTION_GATE,
            database
        )
        .execute(&worker_pool)
        .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut connection_stealer)
            .await
            .is_err(),
        "worker pool connections were not occupied by dequeue and shutdown"
    );

    dequeue_gate.rollback().await.unwrap();
    crate::wait_for_lock_waiter(
        &control,
        "%SELECT pg_advisory_xact_lock($1, hashtext($2))%",
        "worker connection was not held after committing its dequeue",
    )
    .await;
    assert_eq!(handle.refresh().await.unwrap().status, JobStatus::Running);

    assert!(handle.abort("buffered abort").await.unwrap());
    assert_eq!(handle.refresh().await.unwrap().status, JobStatus::Aborting);
    connection_gate.rollback().await.unwrap();
    connection_stealer.await.unwrap().unwrap();
    crate::wait_for_lock_waiter(
        &control,
        "%WITH requeued AS (%",
        "fetcher did not take responsibility for the buffered job",
    )
    .await;
    worker_gate.rollback().await.unwrap();

    let aborted = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let row = handle.refresh().await.unwrap();
            if row.status == JobStatus::Aborted {
                return row;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    probe.release.notify_one();
    let run_result = tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("worker did not finish graceful shutdown");

    let row = aborted.expect("buffered abort was not finalized");
    run_result.unwrap().unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 0);
    assert_eq!(row.status, JobStatus::Aborted);
    assert_eq!(row.error.as_deref(), Some("buffered abort"));
    assert!(row.completed_at.is_some());
    assert_eq!(
        grace_job.refresh().await.unwrap().status,
        JobStatus::Complete
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn run_until_requeues_inflight_jobs_when_aborted(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let handle = db
        .queue
        .enqueue(slow_but_abortable::job(()))
        .await
        .unwrap()
        .unwrap();
    let worker = test_worker(db.queue.clone())
        .register(slow_but_abortable)
        .shutdown_grace(Duration::from_millis(100))
        .build()
        .unwrap();
    let worker_id = worker.id();
    let run = tokio::spawn(worker.run_until(CancellationToken::new()));

    wait_status(&db.queue, handle.id(), JobStatus::Running, 10).await;
    run.abort();
    assert!(run.await.unwrap_err().is_cancelled());

    let row = wait_status(&db.queue, handle.id(), JobStatus::Queued, 10).await;
    assert_eq!(row.error.as_deref(), Some("cancelled"));
    assert_eq!(row.attempts, 1, "execution history remains monotonic");
    assert_eq!(row.max_attempts, 2, "shutdown refunds the retry budget");
    wait_for_some(
        Duration::from_secs(5),
        Duration::from_millis(25),
        "aborted run_until left a live worker lease",
        || async {
            db.queue
                .info()
                .await
                .unwrap()
                .workers
                .iter()
                .all(|worker| worker.id != worker_id)
                .then_some(())
        },
    )
    .await;
}

//noinspection SqlNoDataSourceInspection
#[sqlx::test(migrations = "./migrations")]
async fn worker_dequeue_does_not_wait_on_a_busy_queue_lock(pool: PgPool) {
    let blocker = TestDb::new(pool_with_max(&pool, 2).await).await.queue;
    let db = TestDb::new(pool_with_max(&pool, 1).await).await;
    let mut lock = blocker.pool().begin().await.unwrap();
    sqlx::query!(
        "SELECT pg_advisory_xact_lock($1, hashtext($2))",
        pgqueue::__private::dequeue_lock_key(&db.database),
        "default"
    )
    .execute(&mut *lock)
    .await
    .unwrap();

    let worker = test_worker(db.queue.clone())
        .register(counts)
        .shutdown_grace(Duration::from_millis(100))
        .build()
        .unwrap();
    let worker_id = worker.id();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    wait_for_some(
        Duration::from_secs(5),
        Duration::from_millis(10),
        "worker did not start",
        || async {
            blocker
                .info()
                .await
                .unwrap()
                .workers
                .iter()
                .any(|worker| worker.id == worker_id)
                .then_some(())
        },
    )
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    tokio::time::timeout(Duration::from_millis(250), db.queue.counts())
        .await
        .expect("a contended worker dequeue must not pin its pool connection")
        .unwrap();
    token.cancel();

    tokio::time::timeout(Duration::from_secs(1), run)
        .await
        .expect("dequeue contention must not postpone shutdown")
        .unwrap()
        .unwrap();
    lock.rollback().await.unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn burst_mode_drains_the_queue_and_exits(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    for _ in 0..3 {
        db.queue.enqueue(counts::job(())).await.unwrap().unwrap();
    }

    let worker = test_worker(db.queue.clone())
        .register(counts)
        .state(counter.clone())
        .burst(true)
        .dequeue_timeout(Duration::from_millis(400))
        .concurrency(2)
        .build()
        .unwrap();

    // No external cancellation: burst mode returns when drained.
    tokio::time::timeout(
        Duration::from_secs(15),
        worker.run_until(CancellationToken::new()),
    )
    .await
    .expect("burst worker should exit on its own")
    .unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 3);
}

//noinspection SqlNoDataSourceInspection
#[sqlx::test(migrations = "./migrations")]
async fn burst_waits_for_a_successful_fetch_after_dequeue_contention(pool: PgPool) {
    let blocker = TestDb::new(pool_with_max(&pool, 2).await).await.queue;
    let db = TestDb::new(pool_with_max(&pool, 1).await).await;
    let handle = db
        .queue
        .enqueue(counts::job(()).group_key("contended"))
        .await
        .unwrap()
        .unwrap();
    let mut lock = blocker.pool().begin().await.unwrap();
    sqlx::query!(
        "SELECT pg_advisory_xact_lock($1, hashtext($2))",
        pgqueue::__private::dequeue_lock_key(&db.database),
        "default"
    )
    .execute(&mut *lock)
    .await
    .unwrap();
    let counter = Arc::new(AtomicU32::new(0));
    let worker = test_worker(db.queue.clone())
        .register(counts)
        .state(counter.clone())
        .burst(true)
        .dequeue_timeout(Duration::from_millis(200))
        .build()
        .unwrap();
    let run = tokio::spawn(worker.run_until(CancellationToken::new()));

    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(
        !run.is_finished(),
        "burst must not report a drain after a contended dequeue"
    );
    assert_eq!(handle.refresh().await.unwrap().status, JobStatus::Queued);

    lock.rollback().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .expect("worker should recover after the lock is released")
        .unwrap()
        .unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 1);
    assert_eq!(handle.refresh().await.unwrap().status, JobStatus::Complete);
}

//noinspection SqlNoDataSourceInspection
#[sqlx::test(migrations = "./migrations")]
async fn burst_waits_for_ready_job_when_row_is_locked(pool: PgPool) {
    let control = TestDb::new(pool_with_max(&pool, 2).await).await.queue;
    let db = TestDb::new(pool_with_max(&pool, 1).await).await;
    let handle = db.queue.enqueue(counts::job(())).await.unwrap().unwrap();
    let mut lock = control.pool().begin().await.unwrap();
    sqlx::query!(
        "SELECT id FROM pgqueue.jobs WHERE id = $1 FOR UPDATE",
        handle.id()
    )
    .fetch_one(&mut *lock)
    .await
    .unwrap();

    let counter = Arc::new(AtomicU32::new(0));
    let worker = test_worker(db.queue.clone())
        .register(counts)
        .state(counter.clone())
        .burst(true)
        .dequeue_timeout(Duration::from_millis(200))
        .build()
        .unwrap();
    let run = tokio::spawn(worker.run_until(CancellationToken::new()));

    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(
        !run.is_finished(),
        "a locked ready row must not make a burst worker report a drain"
    );
    assert_eq!(handle.refresh().await.unwrap().status, JobStatus::Queued);

    lock.rollback().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .expect("worker should process the row after its lock is released")
        .unwrap()
        .unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 1);
    assert_eq!(handle.refresh().await.unwrap().status, JobStatus::Complete);
}

//noinspection SqlNoDataSourceInspection
#[sqlx::test(migrations = "./migrations")]
async fn worker_skips_locked_ready_row_and_processes_next(pool: PgPool) {
    let control = TestDb::new(pool_with_max(&pool, 2).await).await.queue;
    let db = TestDb::new(pool_with_max(&pool, 1).await).await;
    let locked = db.queue.enqueue(counts::job(())).await.unwrap().unwrap();
    let next = db.queue.enqueue(counts::job(())).await.unwrap().unwrap();
    let mut lock = control.pool().begin().await.unwrap();
    sqlx::query!(
        "SELECT id FROM pgqueue.jobs WHERE id = $1 FOR UPDATE",
        locked.id()
    )
    .fetch_one(&mut *lock)
    .await
    .unwrap();

    let counter = Arc::new(AtomicU32::new(0));
    let worker = test_worker(db.queue.clone())
        .register(counts)
        .state(counter.clone())
        .concurrency(1)
        .burst(true)
        .dequeue_timeout(Duration::from_millis(200))
        .build()
        .unwrap();
    let run = tokio::spawn(worker.run_until(CancellationToken::new()));

    wait_for_some(
        Duration::from_secs(2),
        Duration::from_millis(10),
        "worker did not skip the locked row",
        || {
            let counter = counter.clone();
            let next = next.clone();
            async move {
                (counter.load(Ordering::SeqCst) == 1
                    && next.refresh().await.ok()?.status == JobStatus::Complete)
                    .then_some(())
            }
        },
    )
    .await;
    assert_eq!(locked.refresh().await.unwrap().status, JobStatus::Queued);
    assert_eq!(next.refresh().await.unwrap().status, JobStatus::Complete);
    assert!(!run.is_finished(), "the locked row is still ready");

    lock.rollback().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .expect("worker should process the locked row after release")
        .unwrap()
        .unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 2);
    assert_eq!(locked.refresh().await.unwrap().status, JobStatus::Complete);
}

//noinspection SqlNoDataSourceInspection
#[sqlx::test(migrations = "./migrations")]
async fn burst_does_not_drain_when_worker_intake_is_closed(pool: PgPool) {
    let control = TestDb::new(pool_with_max(&pool, 2).await).await.queue;
    let db = TestDb::new(pool_with_max(&pool, 1).await).await;
    let handle = db.queue.enqueue(counts::job(())).await.unwrap().unwrap();
    let mut lock = control.pool().begin().await.unwrap();
    sqlx::query!(
        "SELECT pg_advisory_xact_lock($1, hashtext($2))",
        pgqueue::__private::dequeue_lock_key(&db.database),
        "default"
    )
    .execute(&mut *lock)
    .await
    .unwrap();
    let counter = Arc::new(AtomicU32::new(0));
    let worker = test_worker(db.queue.clone())
        .register(counts)
        .state(counter.clone())
        .timers(WorkerTimers {
            worker_info: Duration::from_secs(10),
            ..test_timers()
        })
        .burst(true)
        .dequeue_timeout(Duration::from_millis(200))
        .build()
        .unwrap();
    let worker_id = worker.id();
    let run = tokio::spawn(worker.run_until(CancellationToken::new()));

    wait_for_some(
        Duration::from_secs(5),
        Duration::from_millis(10),
        "worker did not start",
        || async {
            control
                .info()
                .await
                .unwrap()
                .workers
                .iter()
                .any(|worker| worker.id == worker_id)
                .then_some(())
        },
    )
    .await;
    sqlx::query!(
        "UPDATE pgqueue.workers SET accepting = false, expires_at = now() - interval '1 second' WHERE id = $1",
        worker_id
    )
    .execute(control.pool())
    .await
    .unwrap();
    lock.rollback().await.unwrap();

    tokio::time::sleep(Duration::from_millis(900)).await;
    assert!(
        !run.is_finished(),
        "an intake-gated empty result must not satisfy burst drain"
    );
    assert_eq!(handle.refresh().await.unwrap().status, JobStatus::Queued);

    control
        .write_worker_info(worker_id, json!({}), None, Duration::from_secs(30))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .expect("worker should continue after its intake lease is restored")
        .unwrap()
        .unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 1);
    assert_eq!(handle.refresh().await.unwrap().status, JobStatus::Complete);
}

#[sqlx::test(migrations = "./migrations")]
async fn max_burst_jobs_caps_processing_even_under_concurrency(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    for _ in 0..10 {
        db.queue.enqueue(counts::job(())).await.unwrap().unwrap();
    }

    // Ten processors race a cap of 2: the budget must hold exactly.
    let worker = test_worker(db.queue.clone())
        .register(counts)
        .state(counter.clone())
        .burst(true)
        .dequeue_timeout(Duration::from_millis(400))
        .max_burst_jobs(2)
        .concurrency(10)
        .build()
        .unwrap();

    tokio::time::timeout(
        Duration::from_secs(15),
        worker.run_until(CancellationToken::new()),
    )
    .await
    .expect("capped burst worker should exit")
    .unwrap();
    assert_eq!(counter.load(Ordering::SeqCst), 2, "cap must hold exactly");
    assert_eq!(
        db.queue.counts().await.unwrap().queued,
        8,
        "remaining jobs left untouched"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn stale_job_context_cannot_touch_a_newer_attempt(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<bool>();
    let handle = db
        .queue
        .enqueue(leaks_old_context::job(()))
        .await
        .unwrap()
        .unwrap();
    let worker = test_worker(db.queue.clone())
        .register(leaks_old_context)
        .state(LeakedContext {
            next_attempt: Arc::new(tokio::sync::Notify::new()),
            checked: Arc::new(tokio::sync::Notify::new()),
            result: tx,
        })
        .concurrency(1)
        .build()
        .unwrap();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    let stale_touch_failed = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("leaked context reported")
        .expect("channel remained open");
    assert!(stale_touch_failed, "attempt 1 must not heartbeat attempt 2");
    assert_eq!(
        wait_terminal(&db.queue, handle.id(), 10).await.status,
        JobStatus::Complete
    );

    token.cancel();
    run.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn current_job_context_can_touch_while_aborting(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let trigger = Arc::new(tokio::sync::Notify::new());
    let handle = db
        .queue
        .enqueue(touches_while_aborting::job(()))
        .await
        .unwrap()
        .unwrap();
    let worker = test_worker(db.queue.clone())
        .register(touches_while_aborting)
        .state(AbortingTouch {
            trigger: trigger.clone(),
            result: tx,
        })
        .timers(WorkerTimers {
            abort: Duration::from_secs(60),
            ..test_timers()
        })
        .build()
        .unwrap();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    wait_status(&db.queue, handle.id(), JobStatus::Running, 5).await;
    assert!(handle.abort("cleanup").await.unwrap());
    trigger.notify_one();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap(),
        "the current aborting attempt should retain heartbeat ownership"
    );

    token.cancel();
    run.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn live_worker_retries_a_sweeper_cancelled_attempt(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let attempts = Arc::new(AtomicU32::new(0));
    let handle = db
        .queue
        .enqueue(swept_once::job(()))
        .await
        .unwrap()
        .unwrap();
    // Hold sweep leadership outside the worker so phase 2 cannot race the live
    // worker's abort poll and hide a terminal-abort regression.
    let mut sweeper = db.queue.sweeper();
    assert!(sweeper.sweep().await.unwrap().leader);
    let worker = test_worker(db.queue.clone())
        .register(swept_once)
        .state(attempts.clone())
        .timers(WorkerTimers {
            abort: Duration::from_millis(20),
            sweep: Duration::from_secs(60),
            ..test_timers()
        })
        .concurrency(1)
        .build()
        .unwrap();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    wait_status(&db.queue, handle.id(), JobStatus::Running, 5).await;
    backdate_job_liveness(&db, handle.id()).await;
    let report = sweeper.sweep().await.unwrap();
    assert_eq!(report.cancelling, vec![handle.id()]);
    let row = wait_terminal(&db.queue, handle.id(), 10).await;
    assert_eq!(row.status, JobStatus::Complete);
    assert_eq!(row.attempts, 2, "the swept first attempt must be retried");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);

    token.cancel();
    run.await.unwrap().unwrap();
    sweeper.release().await;
}

#[sqlx::test(migrations = "./migrations")]
async fn worker_heartbeat_reports_only_jobs_processed_by_that_worker(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let counter = Arc::new(AtomicU32::new(0));
    let complete_worker = test_worker(db.queue.clone())
        .register(counts)
        .state(counter)
        .build()
        .unwrap();
    let failed_worker = test_worker(db.queue.clone())
        .register(always_fails)
        .build()
        .unwrap();
    let complete_worker_id = complete_worker.id();
    let failed_worker_id = failed_worker.id();

    let complete_job = db.queue.enqueue(counts::job(())).await.unwrap().unwrap();
    let failed_job = db
        .queue
        .enqueue(always_fails::job(()))
        .await
        .unwrap()
        .unwrap();

    let complete_token = CancellationToken::new();
    let failed_token = CancellationToken::new();
    let complete_run = tokio::spawn(complete_worker.run_until(complete_token.clone()));
    let failed_run = tokio::spawn(failed_worker.run_until(failed_token.clone()));

    wait_terminal(&db.queue, complete_job.id(), 5).await;
    wait_terminal(&db.queue, failed_job.id(), 5).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let info = db.queue.info().await.unwrap();
        let complete_stats = info
            .workers
            .iter()
            .find(|worker| worker.id == complete_worker_id)
            .map(|worker| &worker.stats);
        let failed_stats = info
            .workers
            .iter()
            .find(|worker| worker.id == failed_worker_id)
            .map(|worker| &worker.stats);
        if complete_stats.is_some_and(|stats| {
            stats["complete"] == 1
                && stats["failed"] == 0
                && stats["retried"] == 0
                && stats["aborted"] == 0
        }) && failed_stats.is_some_and(|stats| {
            stats["complete"] == 0
                && stats["failed"] == 1
                && stats["retried"] == 1
                && stats["aborted"] == 0
        }) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "worker heartbeats did not report isolated counters: {:?}",
            info.workers
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let aggregate = db.queue.stats();
    assert_eq!(aggregate.complete, 1);
    assert_eq!(aggregate.failed, 1);
    assert_eq!(aggregate.retried, 1);

    complete_token.cancel();
    failed_token.cancel();
    complete_run.await.unwrap().unwrap();
    failed_run.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn worker_lease_stays_live_during_processor_shutdown_grace(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let probe = ShutdownLeaseProbe {
        started: Arc::new(tokio::sync::Notify::new()),
    };
    db.queue
        .enqueue(holds_during_shutdown::job(()))
        .await
        .unwrap()
        .unwrap();
    let worker = test_worker(db.queue.clone())
        .register(holds_during_shutdown)
        .state(probe.clone())
        // Coverage instrumentation and the parallel Postgres suite can delay
        // the intake-close observation; leave enough grace to inspect the
        // still-live lease before the processor is cancelled.
        .shutdown_grace(Duration::from_secs(2))
        .build()
        .unwrap();
    let worker_id = worker.id();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));
    tokio::time::timeout(Duration::from_secs(5), probe.started.notified())
        .await
        .expect("handler did not claim the job");

    token.cancel();
    wait_for_worker_intake_closed(&db, worker_id).await;
    assert!(
        db.queue
            .info()
            .await
            .unwrap()
            .workers
            .iter()
            .any(|worker| worker.id == worker_id),
        "the lease expired while a processor still owned work"
    );
    run.await.unwrap().unwrap();
    assert!(
        !db.queue
            .info()
            .await
            .unwrap()
            .workers
            .iter()
            .any(|worker| worker.id == worker_id)
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn worker_builder_rejects_invalid_configuration(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;

    let err = Worker::builder(db.queue.clone()).build().unwrap_err();
    assert!(err.to_string().contains("no jobs registered"), "{err}");

    let err = Worker::builder(db.queue.clone())
        .register(always_fails)
        .burst(true)
        .build()
        .unwrap_err();
    assert!(err.to_string().contains("dequeue_timeout"), "{err}");

    let err = Worker::builder(db.queue.clone())
        .register(always_fails)
        .max_burst_jobs(5)
        .build()
        .unwrap_err();
    assert!(err.to_string().contains("max_burst_jobs requires"), "{err}");

    let err = Worker::builder(db.queue.clone())
        .register(always_fails)
        .register(always_fails)
        .build()
        .unwrap_err();
    assert!(err.to_string().contains("registered twice"), "{err}");

    let err = Worker::builder(db.queue.clone())
        .register(always_fails)
        .cron("0 * * * *", counts::job(()))
        .build()
        .unwrap_err();
    assert!(err.to_string().contains("not registered"), "{err}");

    let err = Worker::builder(db.queue.clone())
        .register(counts)
        .cron("not a cron", counts::job(()))
        .build()
        .unwrap_err();
    assert!(err.to_string().contains("invalid cron expression"), "{err}");

    let err = Worker::builder(db.queue.clone())
        .register(counts)
        .cron("0 * * * *", counts::job(()))
        .cron("30 * * * *", counts::job(()))
        .build()
        .unwrap_err();
    assert!(err.to_string().contains("cron unique key"), "{err}");

    let err = Worker::builder(db.queue.clone())
        .register(always_fails)
        .timers(WorkerTimers {
            abort: Duration::ZERO,
            ..WorkerTimers::default()
        })
        .build()
        .unwrap_err();
    assert!(err.to_string().contains("abort timer"), "{err}");

    let err = Worker::builder(db.queue.clone())
        .register(always_fails)
        .poll_interval(Duration::ZERO)
        .build()
        .unwrap_err();
    assert!(err.to_string().contains("poll interval"), "{err}");

    let err = Worker::builder(db.queue.clone())
        .register(always_fails)
        .burst(true)
        .dequeue_timeout(Duration::ZERO)
        .build()
        .unwrap_err();
    assert!(err.to_string().contains("dequeue timeout"), "{err}");

    if usize::BITS > i64::BITS {
        let err = Worker::builder(db.queue.clone())
            .register(always_fails)
            .concurrency(usize::MAX)
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("concurrency"), "{err}");
    }
}

#[derive(Clone)]
struct DatabaseLossProbe {
    started: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[pgqueue::job]
async fn quick_nap(_: (), state: JobState<DatabaseLossProbe>) -> anyhow::Result<()> {
    state.0.started.notify_one();
    state.0.release.notified().await;
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
async fn abort_health_recovers_when_no_attempts_remain(pool: PgPool) {
    let control = TestDb::new(pool.clone()).await;
    let worker_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect_with(
            pool.connect_options()
                .as_ref()
                .clone()
                .options([("lock_timeout", "100ms")]),
        )
        .await
        .unwrap();
    let worker_db = TestDb::new(worker_pool).await;
    let probe = DatabaseLossProbe {
        started: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
    };
    control
        .queue
        .enqueue(quick_nap::job(()))
        .await
        .unwrap()
        .unwrap();
    let worker = test_worker(worker_db.queue.clone())
        .register(quick_nap)
        .state(probe.clone())
        .timers(WorkerTimers {
            abort: Duration::from_secs(2),
            ..test_timers()
        })
        .concurrency(1)
        .build()
        .unwrap();
    let health = worker.health();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    tokio::time::timeout(Duration::from_secs(5), probe.started.notified())
        .await
        .expect("job did not start");

    let mut lock = control.queue.pool().begin().await.unwrap();
    sqlx::query!("LOCK TABLE pgqueue.jobs IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        "abort poll failure was not reported",
        || async {
            health
                .snapshot()
                .failures
                .iter()
                .any(|failure| failure.component == WorkerComponent::Abort)
        },
    )
    .await;

    lock.rollback().await.unwrap();
    probe.release.notify_one();
    wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        "abort health did not recover after the attempt finished",
        || async {
            !health
                .snapshot()
                .failures
                .iter()
                .any(|failure| failure.component == WorkerComponent::Abort)
        },
    )
    .await;

    shutdown.cancel();
    run.await.unwrap().unwrap();
}

#[pgqueue::job(max_attempts = 2)]
async fn fails_after_release(
    _: (),
    state: JobState<DatabaseLossProbe>,
    ctx: JobContext,
) -> anyhow::Result<()> {
    if ctx.attempt() == 1 {
        state.0.started.notify_one();
        state.0.release.notified().await;
    }
    anyhow::bail!("attempt failed")
}

async fn fail_first_running_transition(pool: &PgPool) {
    sqlx::raw_sql(
        r#"
        CREATE SEQUENCE pgqueue.finalization_failures;
        CREATE FUNCTION pgqueue.fail_first_finalization() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF nextval('pgqueue.finalization_failures') = 1 THEN
                RAISE EXCEPTION 'injected transient finalization failure';
            END IF;
            RETURN NEW;
        END
        $$;
        CREATE TRIGGER fail_first_finalization
        BEFORE UPDATE ON pgqueue.jobs
        FOR EACH ROW
        WHEN (OLD.status = 'running' AND NEW.status IS DISTINCT FROM OLD.status)
        EXECUTE FUNCTION pgqueue.fail_first_finalization();
        "#,
    )
    .execute(pool)
    .await
    .expect("install transient finalization failure");
}

#[sqlx::test(migrations = "./migrations")]
async fn worker_retries_a_transient_finish_failure(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let handle = db.queue.enqueue(quick_nap::job(())).await.unwrap().unwrap();
    let probe = DatabaseLossProbe {
        started: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
    };
    let worker = test_worker(db.queue.clone())
        .register(quick_nap)
        .state(probe.clone())
        .build()
        .unwrap();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    probe.started.notified().await;
    fail_first_running_transition(&pool).await;
    probe.release.notify_one();

    let row = wait_terminal(&db.queue, handle.id(), 10).await;
    assert_eq!(row.status, JobStatus::Complete);
    assert_eq!(row.attempts, 1);

    token.cancel();
    run.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn worker_retries_a_transient_retry_failure(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let handle = db
        .queue
        .enqueue(fails_after_release::job(()))
        .await
        .unwrap()
        .unwrap();
    let probe = DatabaseLossProbe {
        started: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
    };
    let worker = test_worker(db.queue.clone())
        .register(fails_after_release)
        .state(probe.clone())
        .build()
        .unwrap();
    let token = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(token.clone()));

    probe.started.notified().await;
    fail_first_running_transition(&pool).await;
    probe.release.notify_one();

    let row = wait_terminal(&db.queue, handle.id(), 10).await;
    assert_eq!(row.status, JobStatus::Failed);
    assert_eq!(row.attempts, 2);
    assert_eq!(row.error.as_deref(), Some("failed: attempt failed"));

    token.cancel();
    run.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn worker_survives_the_database_disappearing(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    db.queue.enqueue(quick_nap::job(())).await.unwrap().unwrap();
    let probe = DatabaseLossProbe {
        started: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Notify::new()),
    };

    let worker = test_worker(db.queue.clone())
        .register(quick_nap)
        .state(probe.clone())
        .timers(WorkerTimers {
            abort: Duration::from_millis(50),
            schedule: Duration::from_millis(100),
            sweep: Duration::from_millis(200),
            worker_info: Duration::from_millis(100),
        })
        .concurrency(2)
        .build()
        .unwrap();
    let token = CancellationToken::new();
    let mut run = tokio::spawn(worker.run_until(token.clone()));

    // Wait until the nap job is mid-flight, then nuke the schema: dequeues,
    // abort polls, sweeps, heartbeats, and the job finalization all start
    // failing. The worker must log and keep running, never crash.
    tokio::time::timeout(Duration::from_secs(5), probe.started.notified())
        .await
        .expect("job did not start");
    sqlx::query!("DROP SCHEMA pgqueue CASCADE")
        .execute(db.queue.pool())
        .await
        .unwrap();

    probe.release.notify_one();
    assert!(
        tokio::time::timeout(Duration::from_millis(500), &mut run)
            .await
            .is_err(),
        "worker must survive database loss"
    );

    token.cancel();
    run.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn cron_with_unserializable_payload_fails_at_build(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let err = Worker::builder(db.queue.clone())
        .register(always_fails)
        .cron(
            "* * * * *",
            bad_payload::job([((1u32, 2u32), 3u32)].into_iter().collect()),
        )
        .build()
        .unwrap_err();
    assert!(matches!(err, Error::Serde(_)), "{err}");
}

/// JSON object keys must be strings; a tuple-keyed map cannot serialize.
#[pgqueue::job]
async fn bad_payload(_args: std::collections::HashMap<(u32, u32), u32>) -> anyhow::Result<()> {
    Ok(())
}

#[derive(Clone, Serialize, Deserialize)]
struct StressGroupedPayload {
    id: usize,
    group: String,
}

#[derive(Clone, Default)]
struct StressGroupedState {
    active: Arc<Mutex<std::collections::HashSet<String>>>,
    completed: Arc<Mutex<std::collections::HashSet<usize>>>,
}

#[pgqueue::job(max_attempts = 1)]
async fn stress_grouped(
    args: StressGroupedPayload,
    state: JobState<StressGroupedState>,
) -> anyhow::Result<()> {
    {
        let mut active = state
            .0
            .active
            .lock()
            .map_err(|_| anyhow::anyhow!("active-group set poisoned"))?;
        anyhow::ensure!(active.insert(args.group.clone()), "group overlapped");
    }
    tokio::time::sleep(Duration::from_millis(1)).await;
    state
        .0
        .active
        .lock()
        .map_err(|_| anyhow::anyhow!("active-group set poisoned"))?
        .remove(&args.group);
    anyhow::ensure!(
        state
            .0
            .completed
            .lock()
            .map_err(|_| anyhow::anyhow!("completion set poisoned"))?
            .insert(args.id),
        "job completed twice"
    );
    Ok(())
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "stress test"]
async fn grouped_jobs_complete_once_without_group_overlap_under_stress(pool: PgPool) {
    const JOBS: usize = 2_000;
    let db = TestDb::new(pool).await;
    let state = StressGroupedState::default();
    for id in 0..JOBS {
        let group = format!("group-{}", id % 32);
        db.queue
            .enqueue(
                stress_grouped::job(StressGroupedPayload {
                    id,
                    group: group.clone(),
                })
                .group_key(group),
            )
            .await
            .unwrap()
            .unwrap();
    }

    let mut runs = Vec::new();
    for _ in 0..4 {
        let worker = test_worker(db.queue.clone())
            .register(stress_grouped)
            .state(state.clone())
            .burst(true)
            .dequeue_timeout(Duration::from_secs(1))
            .concurrency(8)
            .build()
            .unwrap();
        runs.push(tokio::spawn(worker.run_until(CancellationToken::new())));
    }
    for run in runs {
        tokio::time::timeout(Duration::from_secs(60), run)
            .await
            .expect("stress worker timed out")
            .unwrap()
            .unwrap();
    }
    assert_eq!(state.completed.lock().unwrap().len(), JOBS);
    assert!(state.active.lock().unwrap().is_empty());
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "stress test"]
async fn unique_enqueue_accepts_one_winner_under_stress(pool: PgPool) {
    let db = TestDb::new(pool).await;
    for round in 0..100 {
        let key = format!("stress-unique-{round}");
        let mut tasks = Vec::new();
        for _ in 0..32 {
            let queue = db.queue.clone();
            let key = key.clone();
            tasks.push(tokio::spawn(async move {
                queue.enqueue(counts::job(()).unique_key(key)).await
            }));
        }
        let mut winners = 0;
        for task in tasks {
            if task.await.unwrap().unwrap().is_some() {
                winners += 1;
            }
        }
        assert_eq!(winners, 1, "round {round}");
    }
}

#[sqlx::test(migrations = "./migrations")]
#[ignore = "stress test"]
async fn shutdown_abort_retry_sweep_and_dequeue_lock_interoperate_under_stress(pool: PgPool) {
    let db = TestDb::new(pool).await;
    for _ in 0..25 {
        let handle = db
            .queue
            .enqueue(counts::job(()).group_key("contended"))
            .await
            .unwrap()
            .unwrap();
        let mut lock = db.queue.pool().begin().await.unwrap();
        sqlx::query!(
            "SELECT pg_advisory_xact_lock($1, hashtext($2))",
            pgqueue::__private::dequeue_lock_key(&db.database),
            db.queue.name()
        )
        .execute(&mut *lock)
        .await
        .unwrap();

        let counter = Arc::new(AtomicU32::new(0));
        let worker = test_worker(db.queue.clone())
            .register(counts)
            .state(counter.clone())
            .build()
            .unwrap();
        let token = CancellationToken::new();
        let run = tokio::spawn(worker.run_until(token.clone()));
        tokio::time::sleep(Duration::from_millis(100)).await;
        token.cancel();
        tokio::time::timeout(Duration::from_secs(1), run)
            .await
            .expect("contended worker shutdown timed out")
            .unwrap()
            .unwrap();
        lock.rollback().await.unwrap();

        assert!(handle.abort("stress abort").await.unwrap());
        assert!(
            db.queue
                .retry_job(handle.id(), "stress retry")
                .await
                .unwrap()
        );
        let mut sweeper = db.queue.sweeper();
        assert!(sweeper.sweep().await.unwrap().leader);
        sweeper.release().await;

        test_worker(db.queue.clone())
            .register(counts)
            .state(counter.clone())
            .burst(true)
            .dequeue_timeout(Duration::from_millis(400))
            .build()
            .unwrap()
            .run_until(CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }
}
