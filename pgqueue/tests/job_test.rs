//! Job API, typed macro, enqueue-and-wait, and cron integration tests.

mod enqueue_and_wait {
    //! Request/response tests: `Queue::enqueue_and_wait` and `JobHandle::wait`,
    //! completion-NOTIFY driven with polling fallback.

    use sqlx::PgPool;
    use std::collections::HashSet;
    use std::time::Duration;

    use crate::{
        EnqueueResultTestExt, QueueProtocolTestExt, TestDb, pool_with_max, wait_for_done_listener,
        wait_for_done_listeners,
    };
    use pgqueue::{
        Error, JobErrorKind, JobRetention, JobState, JobStatus, Queue, Worker, WorkerTimers,
    };
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

    #[pgqueue::job(result_ttl_ms = 0)]
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

    /// Starts a background worker for the given queue with all test handlers.
    fn spawn_worker(queue: Queue) -> (CancellationToken, tokio::task::JoinHandle<()>) {
        let worker = Worker::builder(queue)
            .register_job(double)
            .register_job(fails_if_odd)
            .register_job(ephemeral)
            .register_job(very_slow)
            .register_job(shared)
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
    async fn test_enqueue_and_wait_returns_the_typed_result(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let (token, run) = spawn_worker(db.queue.clone());

        let result: u32 = db
            .queue
            .enqueue_and_wait(double::job(21), Some(Duration::from_secs(10)))
            .await
            .unwrap();
        assert_eq!(result, 42);

        token.cancel();
        run.await.unwrap();
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_enqueue_and_wait_propagates_job_failures(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let (token, run) = spawn_worker(db.queue.clone());

        let err = db
            .queue
            .enqueue_and_wait(fails_if_odd::job(3), Some(Duration::from_secs(10)))
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
    async fn test_enqueue_and_wait_times_out_when_nothing_processes(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        // No worker running.
        let err = db
            .queue
            .enqueue_and_wait(double::job(1), Some(Duration::from_millis(300)))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::WaitTimeout), "{err}");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_shared_pool_remains_available_when_multiple_listeners_start(pool: PgPool) {
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
    async fn test_enqueue_and_wait_on_dedupe_hit_waits_on_the_existing_job(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;

        // A slow deduplicated job is already live...
        let existing = db
            .queue
            .enqueue(very_slow::job(()).dedupe_key("singleton"))
            .await
            .unwrap()
            .unwrap();

        // ...so enqueue_and_wait with the same key attaches to it rather than erroring.
        let queue = db.queue.clone();
        let waiter = tokio::spawn(async move {
            queue
                .enqueue_and_wait(
                    very_slow::job(()).dedupe_key("singleton"),
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
    async fn test_enqueue_and_wait_revives_a_terminal_deduplicated_job_with_the_same_schedule(
        pool: PgPool,
    ) {
        let db = TestDb::new(pool.clone()).await;
        let (token, run) = spawn_worker(db.queue.clone());

        let first = db
            .queue
            .enqueue(double::job(2).dedupe_key("reusable"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.wait(Some(Duration::from_secs(10))).await.unwrap(), 4);
        let scheduled_at = first.fetch_job().await.unwrap().scheduled_at;

        let second = db
            .queue
            .enqueue_and_wait(
                double::job(3).dedupe_key("reusable").at(scheduled_at),
                Some(Duration::from_secs(10)),
            )
            .await
            .unwrap();
        assert_eq!(second, 6, "the terminal row must run again");

        token.cancel();
        run.await.unwrap();
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_repeated_dedupe_key_reuse_preserves_every_occurrence_result(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let worker_id = uuid::Uuid::now_v7();
        let mut occurrence_ids = HashSet::new();
        let mut handles = Vec::new();
        for value in 0..16_u32 {
            let handle = db
                .queue
                .enqueue(double::job(value).dedupe_key("hot-key"))
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
    async fn test_enqueue_and_wait_rejects_a_dedupe_key_owned_by_another_job_type(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        db.queue
            .enqueue(very_slow::job(()).dedupe_key("shared-key"))
            .await
            .unwrap()
            .unwrap();

        let error = db
            .queue
            .enqueue_and_wait(
                double::job(1).dedupe_key("shared-key"),
                Some(Duration::from_secs(1)),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("belongs to job"), "{error}");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_wait_rejects_delete_immediately_jobs_without_a_durable_result(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let handle = db.queue.enqueue(ephemeral::job(())).await.unwrap().unwrap();
        let error = handle
            .wait_value(Some(Duration::from_secs(1)))
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Config(_)), "{error}");
        assert_eq!(handle.fetch_job().await.unwrap().status, JobStatus::Queued);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_enqueue_and_wait_rejects_delete_immediately_before_enqueue(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let error = db
            .queue
            .enqueue_and_wait(ephemeral::job(()), Some(Duration::from_secs(1)))
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Config(_)), "{error}");
        assert_eq!(db.queue.counts().await.unwrap().queued, 0);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_enqueue_and_wait_rejects_a_deduplicated_delete_immediately_owner(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let owner = db
            .queue
            .enqueue(ephemeral::job(()).dedupe_key("ephemeral-owner"))
            .await
            .unwrap()
            .unwrap();
        let error = db
            .queue
            .enqueue_and_wait(
                ephemeral::job(())
                    .dedupe_key("ephemeral-owner")
                    .retention(JobRetention::Forever),
                Some(Duration::from_secs(1)),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Config(_)), "{error}");
        assert_eq!(owner.fetch_job().await.unwrap().status, JobStatus::Queued);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_wait_on_a_missing_job_errors(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let handle = db.queue.enqueue(double::job(1)).await.unwrap().unwrap();
        // Delete the row out from under the handle.
        sqlx::query("DELETE FROM pgqueue.jobs")
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
    async fn test_wait_reports_expired_result_when_completed_row_was_purged(pool: PgPool) {
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
        let channel = pgqueue::__test_support::done_channel(db.queue.name());
        let payload = format!(r#"{{"id":"{}","status":"complete"}}"#, handle.id());
        let mut tx = db.queue.pool().begin().await.unwrap();
        sqlx::query("DELETE FROM pgqueue.jobs WHERE id = $1")
            .bind(handle.id())
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(channel.as_str())
            .bind(payload)
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
    async fn test_foreign_notifications_do_not_postpone_fallback_polling(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let handle = db
            .queue
            .enqueue(double::job(21).delay(Duration::from_secs(60)))
            .await
            .unwrap()
            .unwrap();
        // The deadline is far longer than the fallback poll it is testing: a
        // waiter that lets foreign traffic postpone that poll never polls at
        // all, so it fails here however long the deadline is — while a tight
        // one only asks that the machine be fast, which under `cargo llvm-cov`
        // and a parallel suite it is not.
        let waiter = {
            let handle = handle.clone();
            tokio::spawn(async move { handle.wait(Some(Duration::from_secs(5))).await })
        };
        wait_for_done_listener(&db).await;

        // Complete the target without NOTIFY, reproducing a notification lost
        // during listener reconnect. The waiter must discover it on its deadline.
        sqlx::query(
            "UPDATE pgqueue.jobs SET status = 'complete', result = '42'::jsonb, \
             completed_at = now() WHERE id = $1",
        )
        .bind(handle.id())
        .execute(db.queue.pool())
        .await
        .unwrap();

        let channel = pgqueue::__test_support::done_channel(db.queue.name());
        let pool = db.queue.pool().clone();
        let notifier = tokio::spawn(async move {
            let mut conn = pool.acquire().await.unwrap();
            // Foreign traffic for the whole of the waiter's deadline, not just
            // its first second.
            for _ in 0..600 {
                let payload = format!(r#"{{"id":"{}","status":"complete"}}"#, uuid::Uuid::now_v7());
                sqlx::query("SELECT pg_notify($1, $2)")
                    .bind(channel.as_str())
                    .bind(payload)
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
    async fn test_enqueue_and_wait_resolves_results_from_state_backed_handlers(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let (token, run) = spawn_worker(db.queue.clone());

        let out: String = db
            .queue
            .enqueue_and_wait(shared::job(()), Some(Duration::from_secs(10)))
            .await
            .unwrap();
        assert_eq!(out, "from-state");

        token.cancel();
        run.await.unwrap();
    }

    //noinspection SqlNoDataSourceInspection
    #[sqlx::test(migrations = "./migrations")]
    async fn test_malformed_done_notifications_are_tolerated(pool: PgPool) {
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
        let done_channel = pgqueue::__test_support::done_channel(db.queue.name());
        let pool = db.queue.pool().clone();
        let malformed = tokio::spawn(async move {
            for _ in 0..100 {
                sqlx::query("SELECT pg_notify($1, $2)")
                    .bind(done_channel.as_str())
                    .bind("not json at all")
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

    use crate::{EnqueueResultTestExt, TestDb};
    use pgqueue::{
        EnqueueResult, Error, JobConfig, JobErrorKind, JobRetention, JobRetryBackoff, JobState,
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
        result_ttl_ms = 3_600_000,
        retry_delay_ms = 250,
        max_backoff_ms = 60_000
    )]
    async fn send_email(args: SendEmail) -> anyhow::Result<String> {
        Ok(format!("sent to {}", args.to))
    }

    #[pgqueue::job(
        name = "cleanup_v2",
        timeout_ms = 0,
        result_ttl_ms = 0,
        priority = -5
    )]
    async fn cleanup(_: ()) -> anyhow::Result<u64> {
        Ok(42)
    }

    #[pgqueue::job]
    async fn with_state(args: u32, state: JobState<String>) -> Result<String, std::io::Error> {
        Ok(format!("{}-{args}", state.0))
    }

    #[test]
    fn test_job_macro_generates_name_and_config() {
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

        // The generated struct is Copy/Clone/Debug.
        let job = send_email;
        #[allow(clippy::clone_on_copy)]
        let _ = job.clone();
        assert_eq!(format!("{job:?}"), "send_email");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_generated_call_invokes_the_original_function(_pool: PgPool) {
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
    fn test_erased_handler_carries_name_and_config() {
        let handler = send_email::erased();
        assert_eq!(handler.name(), "send_email");
        assert_eq!(handler.config().max_attempts, 3);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_typed_enqueue_round_trips_payload_and_config(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let result = db
            .queue
            .enqueue(send_email::job(SendEmail {
                to: "a@b.c".into(),
                body: "hello".into(),
            }))
            .await
            .unwrap();
        assert!(result.is_enqueued());
        let id = result.job_id();
        let handle = result.into_job_handle();
        assert_eq!(handle.id(), id);

        let row = handle.fetch_job().await.unwrap();
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
    async fn test_typed_enqueue_in_commits_with_the_caller_transaction(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let mut transaction = db.queue.pool().begin().await.unwrap();
        let result = db
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
        let handle = result.into_job_handle();
        assert!(matches!(
            handle.fetch_job().await,
            Err(Error::JobNotFound(_))
        ));
        transaction.commit().await.unwrap();
        assert_eq!(handle.fetch_job().await.unwrap().name, "send_email");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_typed_enqueue_in_anchors_delay_to_the_statement_in_an_aged_transaction(
        pool: PgPool,
    ) {
        let db = TestDb::new(pool.clone()).await;
        let mut transaction = db.queue.pool().begin().await.unwrap();
        sqlx::query("SELECT pg_sleep(0.3)")
            .execute(&mut *transaction)
            .await
            .unwrap();
        let enqueue_started =
            sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>("SELECT clock_timestamp()")
                .fetch_one(&mut *transaction)
                .await
                .unwrap();

        let handle = db
            .queue
            .enqueue_in(
                &mut transaction,
                cleanup::job(()).delay(Duration::from_secs(1)),
            )
            .await
            .unwrap()
            .unwrap();
        let (scheduled_at, enqueued_at) =
            sqlx::query_as::<_, (chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>(
                "SELECT scheduled_at, enqueued_at FROM pgqueue.jobs WHERE id = $1",
            )
            .bind(handle.id())
            .fetch_one(&mut *transaction)
            .await
            .unwrap();

        assert!(enqueued_at >= enqueue_started);
        assert!(
            scheduled_at >= enqueue_started + chrono::Duration::milliseconds(950),
            "delay was anchored before enqueue: {} < {}",
            scheduled_at,
            enqueue_started,
        );
        transaction.rollback().await.unwrap();
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_typed_enqueue_reports_dedupe_and_rejects_a_foreign_owner(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let existing = db
            .queue
            .enqueue(
                send_email::job(SendEmail {
                    to: "owner@example.com".into(),
                    body: "first".into(),
                })
                .dedupe_key("typed-owner"),
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
                .dedupe_key("typed-owner"),
            )
            .await
            .unwrap();
        assert_eq!(duplicate.job_id(), existing.id());
        assert!(matches!(
            duplicate,
            EnqueueResult::Deduplicated(ref handle) if handle.id() == existing.id()
        ));
        transaction.rollback().await.unwrap();

        let error = db
            .queue
            .enqueue(cleanup::job(()).dedupe_key("typed-owner"))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("belongs to job"), "{error}");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_aborting_delete_immediately_job_resolves_as_a_job_result(pool: PgPool) {
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
    async fn test_typed_job_builder_overrides_attribute_config(pool: PgPool) {
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
                .retention(JobRetention::Forever)
                .retry_delay(Duration::from_millis(10))
                .backoff(JobRetryBackoff::None)
                .priority(4)
                .meta(serde_json::json!({"req": 1})),
            )
            .await
            .unwrap()
            .unwrap();

        let row = handle.fetch_job().await.unwrap();
        assert_eq!(row.max_attempts, 9);
        assert_eq!(row.timeout(), Some(Duration::from_secs(5)));
        assert_eq!(row.retention(), JobRetention::Forever);
        assert_eq!(row.retry_delay_ms, 10);
        assert_eq!(row.backoff, JobRetryBackoff::None);
        assert_eq!(row.priority, 4);
        assert_eq!(row.meta, serde_json::json!({"req": 1}));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn test_typed_job_builder_applies_dedupe_and_scheduling(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let first = db
            .queue
            .enqueue(
                cleanup::job(())
                    .dedupe_key("cron:cleanup")
                    .delay(Duration::from_secs(60)),
            )
            .await
            .unwrap();
        assert!(first.is_some());
        let row = first.unwrap().fetch_job().await.unwrap();
        assert_eq!(
            (row.scheduled_at - row.enqueued_at).num_microseconds(),
            Some(60_000_000),
            "relative delay and enqueue time must share the same database clock"
        );

        // Same dedupe key while live: dedupe.
        let second = db
            .queue
            .enqueue(cleanup::job(()).dedupe_key("cron:cleanup"))
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
        let row = handle.fetch_job().await.unwrap();
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
    async fn test_job_handle_aborts_and_refreshes(pool: PgPool) {
        let db = TestDb::new(pool.clone()).await;
        let handle = db.queue.enqueue(cleanup::job(())).await.unwrap().unwrap();
        assert_ne!(handle.id(), uuid::Uuid::nil());
        assert!(handle.abort("changed my mind").await.unwrap());
        assert_eq!(handle.fetch_job().await.unwrap().status, JobStatus::Aborted);
        assert!(format!("{handle:?}").contains("JobHandle"));
    }
}

mod macros {
    //! Compile-pass and compile-fail tests for `#[pgqueue::job]` diagnostics.

    #[test]
    fn test_job_macro_cases_compile_as_expected() {
        let t = trybuild::TestCases::new();
        t.pass("tests/macros/pass.rs");
        t.pass("tests/macros/pass_deprecated.rs");
        t.pass("tests/macros/pass_hygiene.rs");
        t.pass("tests/macros/pass_lint_attrs.rs");
        t.pass("tests/macros/pass_macro_rules.rs");
        t.compile_fail("tests/macros/fail.rs");
        t.compile_fail("tests/macros/fail_deprecated.rs");
        t.compile_fail("tests/macros/fail_registration.rs");
    }
}

mod macro_telemetry {
    //! `#[tracing::instrument]` is the motivating example for leaving non-lint
    //! attributes on the hidden function, and it takes its span name from the
    //! identifier. Renaming that function to a private placeholder labelled
    //! every job's telemetry `__pgqueue_inner`, so the handler name was lost
    //! across all of it.

    use std::sync::{Arc, Mutex};

    use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
    use tracing_subscriber::registry::Registry;

    /// Records the name of every span opened while it is the default.
    #[derive(Clone, Default)]
    struct RecordedSpans(Arc<Mutex<Vec<String>>>);

    impl<S: tracing::Subscriber> Layer<S> for RecordedSpans {
        fn on_new_span(
            &self,
            attrs: &tracing::span::Attributes<'_>,
            _id: &tracing::span::Id,
            _ctx: Context<'_, S>,
        ) {
            if let Ok(mut names) = self.0.lock() {
                names.push(attrs.metadata().name().to_string());
            }
        }
    }

    #[pgqueue::job(name = "instrumented_handler")]
    #[tracing::instrument]
    async fn instrumented_handler(_: ()) -> anyhow::Result<()> {
        Ok(())
    }

    #[test]
    fn test_instrumented_job_reports_the_handler_name_as_its_span() {
        let recorded = RecordedSpans::default();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        // Thread-local, so it takes precedence over the suite's global
        // subscriber without disturbing tests running on other threads.
        tracing::subscriber::with_default(Registry::default().with(recorded.clone()), || {
            runtime
                .block_on(instrumented_handler::call(()))
                .expect("handler");
        });

        let names = recorded.0.lock().expect("recorded spans").clone();
        assert!(
            names.iter().any(|name| name == "instrumented_handler"),
            "job telemetry lost the handler name: {names:?}"
        );
        assert!(
            !names.iter().any(|name| name.contains("pgqueue_inner")),
            "the expansion's private placeholder leaked into telemetry: {names:?}"
        );
    }
}

mod wait_without_notifications {
    use std::time::Duration;

    use pgqueue::{Queue, Worker};
    use tokio_util::sync::CancellationToken;

    #[pgqueue::job(name = "wait_without_listener", max_attempts = 1)]
    async fn wait_without_listener(_: ()) -> anyhow::Result<u32> {
        Ok(42)
    }

    /// `wait` subscribes to completion notifications, but that needs a
    /// connection outside the query pool. Losing it must not fail a caller
    /// whose job runs and completes normally — the backing-off poll in
    /// `wait_inner` covers it.
    #[tokio::test]
    async fn test_enqueue_and_wait_falls_back_to_polling_when_the_listener_cannot_connect() {
        crate::init_tracing();
        let url = crate::fresh_database("wait_polling").await;
        let admin_queue = Queue::connect(&url).await.unwrap();
        let client_url = crate::limited_role_url(&url, -1).await;

        // A warm pool that can no longer open new connections: the caller can talk
        // to the database, but cannot start a LISTEN.
        let pool = sqlx::postgres::PgPoolOptions::new()
            .min_connections(2)
            .max_connections(2)
            .connect(&client_url)
            .await
            .unwrap();
        crate::revoke_connect(&url, &client_url).await;
        let client_queue = pgqueue::Queue::builder(&client_url)
            .pool(pool)
            .migration_mode(pgqueue::MigrationMode::Skip)
            .connect()
            .await
            .unwrap();

        let shutdown = CancellationToken::new();
        let worker = Worker::builder(admin_queue.clone())
            .register_job(wait_without_listener)
            .timers(crate::test_timers())
            .build()
            .unwrap();
        let run = tokio::spawn(worker.run_until(shutdown.clone()));

        let value = client_queue
            .enqueue_and_wait(
                wait_without_listener::job(()),
                Some(Duration::from_secs(30)),
            )
            .await
            .expect("wait must poll instead of surfacing the listener failure");
        assert_eq!(value, 42);

        shutdown.cancel();
        run.await.unwrap().unwrap();
    }
}
