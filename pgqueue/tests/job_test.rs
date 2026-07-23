//! Job API, typed macro, apply/map, and cron integration tests.

mod apply {
    //! Request/response tests: `Queue::apply`, `Queue::map`, `JobHandle::wait` —
    //! completion-NOTIFY driven with polling fallback.

    use sqlx::PgPool;
    use std::collections::HashSet;
    use std::time::Duration;

    use crate::{
        EnqueueOutcomeTestExt, QueueProtocolTestExt, TestDb, pool_with_max, wait_for_done_listener,
        wait_for_done_listeners,
    };
    use pgqueue::{
        Error, JobErrorKind, JobRetention, JobState, JobStatus, Queue, Worker, WorkerTimers,
    };
    use serde::{Deserialize, Serialize, Serializer};
    use sqlx::Connection;
    use tokio_util::sync::CancellationToken;

    #[pgqueue::job]
    async fn double(args: u32) -> anyhow::Result<u32> {
        Ok(args * 2)
    }

    #[pgqueue::job(max_attempts = 1)]
    async fn fails_if_odd(args: u32) -> anyhow::Result<u32> {
        anyhow::ensure!(args.is_multiple_of(2), "odd number {args}");
        Ok(args)
    }

    #[pgqueue::job(ttl_ms = 0)]
    async fn ephemeral(_: ()) -> anyhow::Result<u32> {
        Ok(7)
    }

    #[pgqueue::job(max_attempts = 1, timeout_ms = 30_000)]
    async fn very_slow(_: ()) -> anyhow::Result<()> {
        std::future::pending().await
    }

    #[pgqueue::job]
    async fn shared(_: (), tag: JobState<String>) -> anyhow::Result<String> {
        Ok(tag.0)
    }

    #[derive(Debug, Deserialize)]
    struct FalliblePayload {
        value: u32,
        fail: bool,
    }

    impl Serialize for FalliblePayload {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            if self.fail {
                return Err(serde::ser::Error::custom(
                    "intentional serialization failure",
                ));
            }
            serde_json::json!({ "value": self.value, "fail": self.fail }).serialize(serializer)
        }
    }

    #[pgqueue::job]
    async fn fallible_payload(args: FalliblePayload) -> anyhow::Result<u32> {
        Ok(args.value)
    }

    /// Starts a background worker for the given queue with all test handlers.
    fn spawn_worker(queue: Queue) -> (CancellationToken, tokio::task::JoinHandle<()>) {
        let worker = Worker::builder(queue)
            .register(double)
            .register(fails_if_odd)
            .register(ephemeral)
            .register(very_slow)
            .register(shared)
            .state("from-state".to_string())
            .timers(WorkerTimers {
                abort: Duration::from_millis(50),
                schedule: Duration::from_millis(200),
                sweep: Duration::from_secs(60),
                worker_info: Duration::from_secs(1),
            })
            .poll_interval(Duration::from_millis(50))
            .concurrency(4)
            .build()
            .unwrap();
        let token = CancellationToken::new();
        let stop = token.clone();
        let handle = tokio::spawn(async move {
            worker.run_until(stop).await.unwrap();
        });
        (token, handle)
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn apply_returns_the_typed_result(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let (token, run) = spawn_worker(db.queue.clone());

        let result: u32 = db
            .queue
            .apply(double::job(21), Some(Duration::from_secs(10)))
            .await
            .unwrap();
        assert_eq!(result, 42);

        token.cancel();
        run.await.unwrap();
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn apply_propagates_job_failures(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let (token, run) = spawn_worker(db.queue.clone());

        let err = db
            .queue
            .apply(fails_if_odd::job(3), Some(Duration::from_secs(10)))
            .await
            .unwrap_err();
        match err {
            Error::Job(job_error) => {
                assert_eq!(job_error.kind, JobErrorKind::Failed);
                assert_eq!(job_error.message, "odd number 3");
            }
            other => panic!("expected Error::Job, got {other}"),
        }

        token.cancel();
        run.await.unwrap();
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn apply_times_out_when_nothing_processes(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        // No worker running.
        let err = db
            .queue
            .apply(double::job(1), Some(Duration::from_millis(300)))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::WaitTimeout), "{err}");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn shared_pool_remains_available_when_multiple_listeners_start(pool: PgPool) {
        let query_pool = pool_with_max(&pool, 2).await;
        let db = TestDb::new(query_pool).await;
        let other = db.another_queue(|builder| builder).await;
        let first = db
            .queue
            .enqueue(double::job(1).delay(Duration::from_secs(60)))
            .await
            .unwrap()
            .unwrap();
        let second = other
            .enqueue(double::job(2).delay(Duration::from_secs(60)))
            .await
            .unwrap()
            .unwrap();
        let first_waiter = tokio::spawn(async move { first.wait_value(None).await });
        let second_waiter = tokio::spawn(async move { second.wait_value(None).await });

        wait_for_done_listeners(&pool, 2).await;
        tokio::time::timeout(Duration::from_secs(1), db.queue.counts())
            .await
            .expect("LISTEN connections must not exhaust the shared query pool")
            .unwrap();

        first_waiter.abort();
        second_waiter.abort();
        let _ = first_waiter.await;
        let _ = second_waiter.await;
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn apply_on_dedupe_hit_waits_on_the_existing_job(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;

        // A slow unique job is already live...
        let existing = db
            .queue
            .enqueue(very_slow::job(()).unique_key("singleton"))
            .await
            .unwrap()
            .unwrap();

        // ...so apply with the same key attaches to it rather than erroring.
        let queue = db.queue.clone();
        let waiter = tokio::spawn(async move {
            queue
                .apply(
                    very_slow::job(()).unique_key("singleton"),
                    Some(Duration::from_secs(10)),
                )
                .await
        });

        wait_for_done_listener(&db).await;
        assert!(existing.abort("cancelled by test").await.unwrap());

        let err = waiter.await.unwrap().unwrap_err();
        match err {
            Error::Job(job_error) => assert_eq!(job_error.kind, JobErrorKind::Aborted),
            other => panic!("expected Error::Job, got {other}"),
        }
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn apply_revives_a_terminal_unique_job_with_the_same_schedule(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let (token, run) = spawn_worker(db.queue.clone());

        let first = db
            .queue
            .enqueue(double::job(2).unique_key("reusable"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.wait(Some(Duration::from_secs(10))).await.unwrap(), 4);
        let scheduled_at = first.refresh().await.unwrap().scheduled_at;

        let second = db
            .queue
            .apply(
                double::job(3).unique_key("reusable").at(scheduled_at),
                Some(Duration::from_secs(10)),
            )
            .await
            .unwrap();
        assert_eq!(second, 6, "the terminal row must run again");

        token.cancel();
        run.await.unwrap();
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn repeated_unique_key_reuse_preserves_every_occurrence_result(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let worker_id = uuid::Uuid::now_v7();
        let mut occurrence_ids = HashSet::new();
        let mut handles = Vec::new();
        for value in 0..16_u32 {
            let handle = db
                .queue
                .enqueue(double::job(value).unique_key("hot-key"))
                .await
                .unwrap()
                .expect("the prior occurrence is terminal");
            assert!(
                occurrence_ids.insert(handle.id()),
                "key reuse must create a distinct occurrence"
            );
            let active = db.queue.dequeue(1, worker_id).await.unwrap().remove(0);
            assert_eq!(active.id, handle.id());
            assert!(
                db.queue
                    .finish(
                        &active,
                        JobStatus::Complete,
                        Some(serde_json::json!(value * 2)),
                        None,
                    )
                    .await
                    .unwrap()
            );
            handles.push((value, handle));
        }

        let mut waits = tokio::task::JoinSet::new();
        for (value, handle) in handles {
            waits.spawn(async move {
                (
                    value,
                    handle.wait(Some(Duration::from_secs(5))).await.unwrap(),
                )
            });
        }
        while let Some(result) = waits.join_next().await {
            let (value, output) = result.unwrap();
            assert_eq!(output, value * 2);
        }
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn apply_rejects_a_unique_key_owned_by_another_job_type(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        db.queue
            .enqueue(very_slow::job(()).unique_key("shared-key"))
            .await
            .unwrap()
            .unwrap();

        let error = db
            .queue
            .apply(
                double::job(1).unique_key("shared-key"),
                Some(Duration::from_secs(1)),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("belongs to job"), "{error}");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn wait_rejects_delete_immediately_jobs_without_a_durable_result(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let handle = db.queue.enqueue(ephemeral::job(())).await.unwrap().unwrap();
        let error = handle
            .wait_value(Some(Duration::from_secs(1)))
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Config(_)), "{error}");
        assert_eq!(handle.refresh().await.unwrap().status, JobStatus::Queued);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn apply_rejects_delete_immediately_before_enqueue(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let error = db
            .queue
            .apply(ephemeral::job(()), Some(Duration::from_secs(1)))
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Config(_)), "{error}");
        assert_eq!(db.queue.counts().await.unwrap().queued, 0);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn apply_rejects_a_deduplicated_delete_immediately_owner(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let owner = db
            .queue
            .enqueue(ephemeral::job(()).unique_key("ephemeral-owner"))
            .await
            .unwrap()
            .unwrap();
        let error = db
            .queue
            .apply(
                ephemeral::job(())
                    .unique_key("ephemeral-owner")
                    .retention(JobRetention::Forever),
                Some(Duration::from_secs(1)),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Config(_)), "{error}");
        assert_eq!(owner.refresh().await.unwrap().status, JobStatus::Queued);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn wait_on_a_missing_job_errors(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let handle = db.queue.enqueue(double::job(1)).await.unwrap().unwrap();
        // Delete the row out from under the handle.
        sqlx::query!("DELETE FROM pgqueue.jobs")
            .execute(db.queue.pool())
            .await
            .unwrap();
        let err = handle
            .wait_value(Some(Duration::from_secs(2)))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::JobNotFound(_)), "{err}");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn wait_reports_expired_result_when_completed_row_was_purged(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let handle = db.queue.enqueue(double::job(21)).await.unwrap().unwrap();
        let waiter = {
            let handle = handle.clone();
            tokio::spawn(async move { handle.wait(Some(Duration::from_secs(5))).await })
        };
        wait_for_done_listener(&db).await;

        // Delete the row and send its completion NOTIFY atomically,
        // reproducing retention purging a completed row before the waiter
        // could re-fetch its result.
        let channel = pgqueue::__private::done_channel(db.queue.name());
        let payload = format!(r#"{{"id":"{}","status":"complete"}}"#, handle.id());
        let mut tx = db.queue.pool().begin().await.unwrap();
        sqlx::query!("DELETE FROM pgqueue.jobs WHERE id = $1", handle.id())
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query!("SELECT pg_notify($1, $2)", &channel, payload)
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let err = waiter.await.unwrap().unwrap_err();
        assert!(
            matches!(err, Error::ResultExpired(id) if id == handle.id()),
            "{err}"
        );
    }

    //noinspection SqlNoDataSourceInspection
    #[sqlx::test(migrations = "./migrations")]
    async fn foreign_notifications_do_not_postpone_fallback_polling(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let handle = db
            .queue
            .enqueue(double::job(21).delay(Duration::from_secs(60)))
            .await
            .unwrap()
            .unwrap();
        let waiter = {
            let handle = handle.clone();
            tokio::spawn(async move { handle.wait(Some(Duration::from_millis(800))).await })
        };
        wait_for_done_listener(&db).await;

        // Complete the target without NOTIFY, reproducing a notification lost
        // during listener reconnect. The waiter must discover it on its deadline.
        sqlx::query!(
            "UPDATE pgqueue.jobs SET status = 'complete', result = '42'::jsonb, \
             completed_at = now() WHERE id = $1",
            handle.id()
        )
        .execute(db.queue.pool())
        .await
        .unwrap();

        let channel = pgqueue::__private::done_channel(db.queue.name());
        let pool = db.queue.pool().clone();
        let notifier = tokio::spawn(async move {
            let mut conn = pool.acquire().await.unwrap();
            for _ in 0..100 {
                let payload = format!(r#"{{"id":"{}","status":"complete"}}"#, uuid::Uuid::now_v7());
                sqlx::query!("SELECT pg_notify($1, $2)", &channel, payload)
                    .execute(&mut *conn)
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });

        assert_eq!(waiter.await.unwrap().unwrap(), 42);
        notifier.abort();
        let _ = notifier.await;
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn map_preserves_order_and_isolates_failures(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let (token, run) = spawn_worker(db.queue.clone());

        let results = db
            .queue
            .map(
                vec![
                    fails_if_odd::job(2),
                    fails_if_odd::job(3),
                    fails_if_odd::job(4),
                    fails_if_odd::job(5),
                ],
                Some(Duration::from_secs(15)),
            )
            .await
            .unwrap();

        assert_eq!(results.len(), 4);
        assert_eq!(results[0].as_ref().unwrap(), &2);
        assert_eq!(results[1].as_ref().unwrap_err().message, "odd number 3");
        assert_eq!(results[2].as_ref().unwrap(), &4);
        assert_eq!(results[3].as_ref().unwrap_err().message, "odd number 5");

        token.cancel();
        run.await.unwrap();
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn map_processes_batches_larger_than_the_waiter_bound(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let (token, run) = spawn_worker(db.queue.clone());
        let jobs = (0..130).map(double::job).collect();

        let results = db
            .queue
            .map(jobs, Some(Duration::from_secs(20)))
            .await
            .unwrap();
        assert_eq!(results.len(), 130);
        for (value, result) in results.into_iter().enumerate() {
            assert_eq!(result.unwrap(), value as u32 * 2);
        }

        token.cancel();
        run.await.unwrap();
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn map_times_out_as_a_whole(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        // No worker: nothing will finish.
        let err = db
            .queue
            .map(
                vec![double::job(1), double::job(2)],
                Some(Duration::from_millis(300)),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::WaitTimeout), "{err}");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn map_timeout_includes_blocked_enqueues(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let mut connection = db.queue.pool().acquire().await.unwrap();
        let mut lock = connection.begin().await.unwrap();
        sqlx::query!("LOCK TABLE pgqueue.jobs IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *lock)
            .await
            .unwrap();

        let started = tokio::time::Instant::now();
        let error = db
            .queue
            .map(
                vec![double::job(1), double::job(2)],
                Some(Duration::from_millis(100)),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, Error::WaitTimeout), "{error}");
        assert!(started.elapsed() < Duration::from_secs(1));
        lock.rollback().await.unwrap();
        assert_eq!(db.queue.counts().await.unwrap().queued, 0);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn map_rejects_unique_key_jobs(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let err = db
            .queue
            .map(vec![double::job(1), double::job(2).unique_key("k")], None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("unique_key"), "{err}");
        assert_eq!(
            db.queue.counts().await.unwrap().queued,
            0,
            "validation must happen before any part of the batch is enqueued"
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn map_rejects_delete_immediately_before_enqueue(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let error = db
            .queue
            .map(vec![ephemeral::job(())], Some(Duration::from_secs(1)))
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Config(_)), "{error}");
        assert_eq!(db.queue.counts().await.unwrap().queued, 0);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn map_rejects_serialization_failure_before_enqueue(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let error = db
            .queue
            .map(
                vec![
                    fallible_payload::job(FalliblePayload {
                        value: 1,
                        fail: false,
                    }),
                    fallible_payload::job(FalliblePayload {
                        value: 2,
                        fail: true,
                    }),
                    fallible_payload::job(FalliblePayload {
                        value: 3,
                        fail: false,
                    }),
                ],
                None,
            )
            .await
            .unwrap_err();

        assert!(
            error.to_string().contains("serialization failure"),
            "{error}"
        );
        assert_eq!(
            db.queue.counts().await.unwrap().queued,
            0,
            "every payload must serialize before any job is enqueued"
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn apply_resolves_results_from_state_backed_handlers(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let (token, run) = spawn_worker(db.queue.clone());

        let out: String = db
            .queue
            .apply(shared::job(()), Some(Duration::from_secs(10)))
            .await
            .unwrap();
        assert_eq!(out, "from-state");

        token.cancel();
        run.await.unwrap();
    }

    //noinspection SqlNoDataSourceInspection
    #[sqlx::test(migrations = "./migrations")]
    async fn malformed_done_notifications_are_tolerated(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let (token, run) = spawn_worker(db.queue.clone());

        // Blast garbage onto the done channel while a waiter is subscribed; the
        // The listener must log-and-continue, and the real completion still resolves.
        let handle = db
            .queue
            .enqueue(double::job(5).delay(Duration::from_millis(700)))
            .await
            .unwrap()
            .unwrap();
        let waiter = {
            let handle = handle.clone();
            tokio::spawn(async move { handle.wait(Some(Duration::from_secs(10))).await })
        };
        let done_channel = pgqueue::__private::done_channel(db.queue.name());
        let pool = db.queue.pool().clone();
        let malformed = tokio::spawn(async move {
            for _ in 0..100 {
                sqlx::query!("SELECT pg_notify($1, $2)", &done_channel, "not json at all")
                    .execute(&pool)
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });

        let result = waiter.await.unwrap().unwrap();
        assert_eq!(result, 10);
        malformed.abort();
        let _ = malformed.await;

        token.cancel();
        run.await.unwrap();
    }
}

mod typed {
    //! End-to-end tests of the `#[pgqueue::job]` macro output: typed enqueue,
    //! config propagation, and the generated helpers.

    use sqlx::PgPool;
    use std::time::Duration;

    use crate::{EnqueueOutcomeTestExt, TestDb};
    use pgqueue::{
        EnqueueOutcome, Error, JobConfig, JobErrorKind, JobRetention, JobRetryBackoff, JobState,
        JobStatus, JobType,
    };
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct SendEmail {
        to: String,
        body: String,
    }

    /// Sends an email (test fixture).
    #[pgqueue::job(
        max_attempts = 3,
        timeout_ms = 30_000,
        ttl_ms = 3_600_000,
        retry_delay_ms = 250,
        backoff_max_ms = 60_000
    )]
    async fn send_email(args: SendEmail) -> anyhow::Result<String> {
        Ok(format!("sent to {}", args.to))
    }

    #[pgqueue::job(name = "cleanup_v2", timeout_ms = 0, ttl_ms = 0, priority = -5)]
    async fn cleanup(_: ()) -> anyhow::Result<u64> {
        Ok(42)
    }

    #[pgqueue::job]
    async fn with_state(args: u32, state: JobState<String>) -> Result<String, std::io::Error> {
        Ok(format!("{}-{args}", state.0))
    }

    #[test]
    fn job_macro_generates_name_and_config() {
        assert_eq!(send_email::NAME, "send_email");
        let config = send_email::config();
        assert_eq!(config.max_attempts, 3);
        assert_eq!(config.timeout, Some(Duration::from_secs(30)));
        assert_eq!(
            config.retention,
            JobRetention::For(Duration::from_secs(3600))
        );
        assert_eq!(config.retry_delay, Duration::from_millis(250));
        assert_eq!(
            config.backoff,
            JobRetryBackoff::Exponential {
                max: Some(Duration::from_secs(60))
            }
        );
        assert_eq!(config.priority, 0);

        assert_eq!(
            cleanup::NAME,
            "cleanup_v2",
            "name attribute overrides the fn name"
        );
        let config = cleanup::config();
        assert_eq!(config.timeout, None);
        assert_eq!(config.retention, JobRetention::DeleteImmediately);
        assert_eq!(config.priority, -5);

        // No attributes: pure defaults.
        assert_eq!(with_state::config(), JobConfig::default());

        // Plain #[pgqueue::job] types carry no cron schedule.
        assert_eq!(send_email::SCHEDULE, None);
        assert_eq!(cleanup::SCHEDULE, None);

        // The generated struct is Copy/Clone/Debug.
        let job = send_email;
        #[allow(clippy::clone_on_copy)]
        let _ = job.clone();
        assert_eq!(format!("{job:?}"), "send_email");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn generated_call_invokes_the_original_function(_pool: PgPool) {
        let out = send_email::call(SendEmail {
            to: "a@b.c".into(),
            body: "hi".into(),
        })
        .await
        .unwrap();
        assert_eq!(out, "sent to a@b.c");
        assert_eq!(cleanup::call(()).await.unwrap(), 42);
    }

    #[test]
    fn erased_handler_carries_name_and_config() {
        let handler = send_email::erased();
        assert_eq!(handler.name(), "send_email");
        assert_eq!(handler.config().max_attempts, 3);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn typed_enqueue_round_trips_payload_and_config(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let handle = db
            .queue
            .enqueue(send_email::job(SendEmail {
                to: "a@b.c".into(),
                body: "hello".into(),
            }))
            .await
            .unwrap()
            .expect("enqueued");

        let row = handle.refresh().await.unwrap();
        assert_eq!(row.name, "send_email");
        assert_eq!(row.status, JobStatus::Queued);
        assert_eq!(row.max_attempts, 3);
        assert_eq!(row.timeout(), Some(Duration::from_secs(30)));
        assert_eq!(row.retry_delay_ms, 250);
        let payload: SendEmail = serde_json::from_value(row.payload).unwrap();
        assert_eq!(
            payload,
            SendEmail {
                to: "a@b.c".into(),
                body: "hello".into()
            }
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn typed_enqueue_in_commits_with_the_caller_transaction(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let mut transaction = db.queue.pool().begin().await.unwrap();
        let outcome = db
            .queue
            .enqueue_in(
                &mut transaction,
                send_email::job(SendEmail {
                    to: "tx@example.com".into(),
                    body: "hello".into(),
                }),
            )
            .await
            .unwrap();
        let handle = outcome.into_handle();
        assert!(matches!(handle.refresh().await, Err(Error::JobNotFound(_))));
        transaction.commit().await.unwrap();
        assert_eq!(handle.refresh().await.unwrap().name, "send_email");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn typed_enqueue_reports_dedupe_and_rejects_a_foreign_owner(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let existing = db
            .queue
            .enqueue(
                send_email::job(SendEmail {
                    to: "owner@example.com".into(),
                    body: "first".into(),
                })
                .unique_key("typed-owner"),
            )
            .await
            .unwrap()
            .unwrap();

        let mut transaction = db.queue.pool().begin().await.unwrap();
        let duplicate = db
            .queue
            .enqueue_in(
                &mut transaction,
                send_email::job(SendEmail {
                    to: "ignored@example.com".into(),
                    body: "ignored".into(),
                })
                .unique_key("typed-owner"),
            )
            .await
            .unwrap();
        assert!(matches!(
            duplicate,
            EnqueueOutcome::Deduplicated(ref handle) if handle.id() == existing.id()
        ));
        transaction.rollback().await.unwrap();

        let error = db
            .queue
            .enqueue(cleanup::job(()).unique_key("typed-owner"))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("belongs to job"), "{error}");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn aborting_delete_immediately_job_resolves_as_a_job_outcome(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let handle = db
            .queue
            .enqueue(cleanup::job(()))
            .await
            .unwrap()
            .expect("enqueued");

        assert!(handle.abort("not needed").await.unwrap());
        let error = handle
            .wait_value(Some(Duration::from_secs(1)))
            .await
            .unwrap_err();
        assert!(
            matches!(error, Error::Job(ref job) if job.kind == JobErrorKind::Aborted),
            "{error}"
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn typed_job_builder_overrides_attribute_config(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let handle = db
            .queue
            .enqueue(
                send_email::job(SendEmail {
                    to: "x".into(),
                    body: "y".into(),
                })
                .max_attempts(9)
                .timeout(Duration::from_secs(5))
                .heartbeat(Duration::from_secs(2))
                .retention(JobRetention::Forever)
                .retry_delay(Duration::from_millis(10))
                .backoff(JobRetryBackoff::None)
                .priority(4)
                .group_key("emails")
                .meta(serde_json::json!({"req": 1})),
            )
            .await
            .unwrap()
            .unwrap();

        let row = handle.refresh().await.unwrap();
        assert_eq!(row.max_attempts, 9);
        assert_eq!(row.timeout(), Some(Duration::from_secs(5)));
        assert_eq!(row.heartbeat(), Some(Duration::from_secs(2)));
        assert_eq!(row.retention(), JobRetention::Forever);
        assert_eq!(row.retry_delay_ms, 10);
        assert_eq!(row.backoff, JobRetryBackoff::None);
        assert_eq!(row.priority, 4);
        assert_eq!(row.group_key.as_deref(), Some("emails"));
        assert_eq!(row.meta, serde_json::json!({"req": 1}));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn typed_job_builder_applies_uniqueness_and_scheduling(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let first = db
            .queue
            .enqueue(
                cleanup::job(())
                    .unique_key("cron:cleanup")
                    .delay(Duration::from_secs(60)),
            )
            .await
            .unwrap();
        assert!(first.is_some());
        let row = first.unwrap().refresh().await.unwrap();
        assert_eq!(
            (row.scheduled_at - row.enqueued_at).num_microseconds(),
            Some(60_000_000),
            "relative delay and enqueue time must share the same database clock"
        );

        // Same unique key while live: dedupe.
        let second = db
            .queue
            .enqueue(cleanup::job(()).unique_key("cron:cleanup"))
            .await
            .unwrap();
        assert!(second.is_none());

        // `at` pins an absolute schedule.
        let when = chrono::Utc::now() + chrono::Duration::seconds(120);
        let handle = db
            .queue
            .enqueue(cleanup::job(()).at(when))
            .await
            .unwrap()
            .unwrap();
        let row = handle.refresh().await.unwrap();
        assert!((row.scheduled_at - when).num_milliseconds().abs() < 5);

        let error = db
            .queue
            .enqueue(cleanup::job(()).delay(Duration::MAX))
            .await
            .unwrap_err();
        use Error::Config;
        assert!(matches!(error, Config(_)), "{error}");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn job_handle_aborts_and_refreshes(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let handle = db.queue.enqueue(cleanup::job(())).await.unwrap().unwrap();
        assert_ne!(handle.id(), uuid::Uuid::nil());
        assert!(handle.abort("changed my mind").await.unwrap());
        assert_eq!(handle.refresh().await.unwrap().status, JobStatus::Aborted);
        assert!(format!("{handle:?}").contains("JobHandle"));
    }
}

mod macro_ui {
    //! Compile-pass and compile-fail tests for `#[pgqueue::job]` diagnostics.

    #[test]
    fn job_macro_ui_cases_compile_as_expected() {
        let t = trybuild::TestCases::new();
        t.pass("tests/ui/pass.rs");
        t.compile_fail("tests/ui/fail.rs");
    }
}
