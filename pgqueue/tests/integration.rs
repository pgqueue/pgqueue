//! Real-Postgres integration tests.
//!
//! SQLx creates a migrated database for every test, so suites can run in
//! parallel without schema-name plumbing or asynchronous cleanup.

use std::future::Future;
use std::time::Duration;

use pgqueue::{
    EnqueueOutcome, Error, JobConfig, JobRequest, JobRow, JobStatus, Queue, QueueBuilder,
    WorkerTimers,
};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

#[path = "cron_test.rs"]
mod cron_test;
#[path = "dashboard_test.rs"]
mod dashboard_test;
#[path = "job_test.rs"]
mod job_test;
#[path = "queue_test.rs"]
mod queue_test;
#[path = "worker_test.rs"]
mod worker_test;

pub struct TestDb {
    pub queue: Queue,
    pub pool: PgPool,
    pub database: String,
}

/// Keeps existing setup terse while preserving the former Option assertions:
/// "some" means inserted and "none" means deduplicated.
pub trait EnqueueOutcomeTestExt<H> {
    fn unwrap(self) -> H;
    fn expect(self, message: &str) -> H;
    fn is_some(&self) -> bool;
    fn is_none(&self) -> bool;
}

impl<H> EnqueueOutcomeTestExt<H> for EnqueueOutcome<H> {
    fn unwrap(self) -> H {
        match self {
            EnqueueOutcome::Enqueued(handle) => handle,
            EnqueueOutcome::Deduplicated(_) => panic!("expected a newly enqueued job"),
        }
    }

    fn expect(self, message: &str) -> H {
        match self {
            EnqueueOutcome::Enqueued(handle) => handle,
            EnqueueOutcome::Deduplicated(_) => panic!("{message}"),
        }
    }

    fn is_some(&self) -> bool {
        self.is_enqueued()
    }

    fn is_none(&self) -> bool {
        self.is_deduplicated()
    }
}

#[allow(async_fn_in_trait)]
pub trait QueueProtocolTestExt {
    async fn dequeue(&self, limit: i64, worker_id: Uuid) -> Result<Vec<JobRow>, Error>;
    async fn finish(
        &self,
        job: &JobRow,
        status: JobStatus,
        result: Option<Value>,
        error: Option<&str>,
    ) -> Result<bool, Error>;
    async fn retry(&self, job: &JobRow, error: &str) -> Result<bool, Error>;
    async fn touch(&self, id: Uuid) -> Result<(), Error>;
    async fn write_worker_info(
        &self,
        worker_id: Uuid,
        stats: Value,
        metadata: Option<Value>,
        ttl: Duration,
    ) -> Result<(), Error>;
}

impl QueueProtocolTestExt for Queue {
    async fn dequeue(&self, limit: i64, worker_id: Uuid) -> Result<Vec<JobRow>, Error> {
        pgqueue::__private::dequeue(self, limit, worker_id).await
    }

    async fn finish(
        &self,
        job: &JobRow,
        status: JobStatus,
        result: Option<Value>,
        error: Option<&str>,
    ) -> Result<bool, Error> {
        pgqueue::__private::finish(self, job, status, result, error).await
    }

    async fn retry(&self, job: &JobRow, error: &str) -> Result<bool, Error> {
        pgqueue::__private::retry(self, job, error).await
    }

    async fn touch(&self, id: Uuid) -> Result<(), Error> {
        pgqueue::__private::touch(self, id).await
    }

    async fn write_worker_info(
        &self,
        worker_id: Uuid,
        stats: Value,
        metadata: Option<Value>,
        ttl: Duration,
    ) -> Result<(), Error> {
        pgqueue::__private::write_worker_info(self, worker_id, stats, metadata, ttl).await
    }
}

pub fn init_tracing() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::level_filters::LevelFilter::TRACE)
            .with_test_writer()
            .try_init();
    });
}

impl TestDb {
    pub async fn new(pool: PgPool) -> Self {
        Self::with(pool, |builder| builder).await
    }

    pub async fn with(pool: PgPool, customize: impl FnOnce(QueueBuilder) -> QueueBuilder) -> Self {
        init_tracing();
        let database = sqlx::query_scalar!(r#"SELECT current_database() AS "database!""#)
            .fetch_one(&pool)
            .await
            .expect("read test database name");
        let queue = customize(
            Queue::builder("postgres://unused")
                .pool(pool.clone())
                .sweep_grace(Duration::ZERO),
        )
        .connect()
        .await
        .expect("test queue connect");
        Self {
            queue,
            pool,
            database,
        }
    }

    pub async fn another_queue(
        &self,
        customize: impl FnOnce(QueueBuilder) -> QueueBuilder,
    ) -> Queue {
        customize(
            Queue::builder("postgres://unused")
                .pool(self.pool.clone())
                .sweep_grace(Duration::ZERO),
        )
        .connect()
        .await
        .expect("second queue connect")
    }
}

pub fn new_job(name: &str, customize: impl FnOnce(&mut JobRequest)) -> JobRequest {
    let mut job = JobRequest::new(name, serde_json::json!({"n": 1}));
    customize(&mut job);
    job
}

pub fn with_config(name: &str, customize: impl FnOnce(&mut JobConfig)) -> JobRequest {
    new_job(name, |job| customize(&mut job.config))
}

pub async fn pool_with_max(pool: &PgPool, max_connections: u32) -> PgPool {
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(max_connections)
        .connect_with(pool.connect_options().as_ref().clone())
        .await
        .expect("connect test pool")
}

/// Fast loop intervals for worker tests; override single fields with struct
/// update syntax: `WorkerTimers { abort: ..., ..test_timers() }`.
pub fn test_timers() -> WorkerTimers {
    WorkerTimers {
        abort: Duration::from_millis(50),
        schedule: Duration::from_millis(100),
        sweep: Duration::from_secs(60),
        worker_info: Duration::from_millis(100),
    }
}

/// Expires a worker's lease so sweep and intake logic treat it as dead.
pub async fn expire_worker(db: &TestDb, worker_id: Uuid) {
    sqlx::query!(
        "UPDATE pgqueue.workers SET expires_at = now() - interval '1 second' WHERE id = $1",
        worker_id
    )
    .execute(db.queue.pool())
    .await
    .expect("expire worker lease");
}

pub async fn backdate_job_liveness(db: &TestDb, id: Uuid) {
    sqlx::query!(
        "UPDATE pgqueue.jobs SET started_at = now() - interval '1 second', touched_at = now() - interval '1 second' WHERE id = $1",
        id
    )
    .execute(db.queue.pool())
    .await
    .expect("backdate job liveness");
}

/// Polls until `poll` yields a value, panicking with `message` once the
/// deadline passes.
pub async fn wait_for_some<T, F, Fut>(
    timeout: Duration,
    interval: Duration,
    message: &str,
    mut poll: F,
) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(value) = poll().await {
            return value;
        }
        assert!(tokio::time::Instant::now() < deadline, "{message}");
        tokio::time::sleep(interval).await;
    }
}

pub async fn wait_until<F, Fut>(
    timeout: Duration,
    interval: Duration,
    message: &str,
    mut condition: F,
) where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    wait_for_some(timeout, interval, message, || {
        let check = condition();
        async move { check.await.then_some(()) }
    })
    .await;
}

/// Polls until some backend in the test database blocks on a lock while
/// running a query matching `pattern`.
pub async fn wait_for_lock_waiter(db: &TestDb, pattern: &str, message: &str) {
    wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        message,
        || async {
            sqlx::query_scalar!(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock' AND query LIKE $1)",
                pattern
            )
            .fetch_one(db.queue.pool())
            .await
            .expect("inspect lock waiters")
            .unwrap_or(false)
        },
    )
    .await;
}

pub async fn wait_for_dequeue_lock_waiter(queue: &Queue, waiting: bool) {
    wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        &format!("dequeue lock waiter did not become {waiting}"),
        || async {
            let found = sqlx::query_scalar!(
                "SELECT EXISTS (SELECT 1 FROM pg_locks locks JOIN pg_stat_activity activity USING (pid) WHERE locks.locktype = 'advisory' AND NOT locks.granted AND activity.datname = current_database())"
            )
            .fetch_one(queue.pool())
            .await
            .expect("inspect dequeue lock waiter")
            .unwrap_or(false);
            found == waiting
        },
    )
    .await;
}

pub async fn wait_for_worker_intake_closed(db: &TestDb, worker_id: Uuid) {
    wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        "worker intake did not close",
        || async {
            sqlx::query_scalar!(
                "SELECT accepting FROM pgqueue.workers WHERE id = $1",
                worker_id
            )
            .fetch_optional(db.queue.pool())
            .await
            .expect("inspect worker intake")
                == Some(false)
        },
    )
    .await;
}

pub async fn wait_for_done_listener(db: &TestDb) {
    wait_for_done_listeners(db.queue.pool(), 1).await;
}

pub async fn wait_for_done_listeners(pool: &PgPool, count: i64) {
    wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        &format!("{count} completion listeners did not subscribe"),
        || async {
            sqlx::query_scalar!(
                r#"SELECT count(*) AS "count!" FROM pg_stat_activity WHERE datname = current_database() AND query LIKE 'LISTEN %'"#
            )
            .fetch_one(pool)
            .await
            .expect("inspect completion listener")
                >= count
        },
    )
    .await;
}
