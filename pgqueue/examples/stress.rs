//! Multi-process end-to-end stress harness.
//!
//! In-process tests share one pool and one runtime, so they cannot exercise
//! what actually breaks in production: worker leases held by separate
//! connections, sweep-leadership contention between processes, and a worker
//! that dies without ever running its shutdown path. This binary is one role
//! per process so a shell script can drive all three.
//!
//! ```text
//! cargo run --release --example stress -- produce 5000
//! cargo run --release --example stress -- work            # several of these
//! cargo run --release --example stress -- verify 5000
//! ```
//!
//! `verify` compares absolute counts, so `produce` first deletes what an
//! earlier run left behind. What it can prove is that every row it produced
//! reached `complete` exactly once carrying the right payload; it cannot see a
//! handler *body* running twice, because a recovered attempt overwrites the
//! same row with the same value. Catching that would need a side-effect ledger
//! — a table with a unique insert per `n`.

use std::time::Duration;

use pgqueue::{JobRetention, Queue, Worker};
use tokio_util::sync::CancellationToken;

/// Sleeps briefly so a `SIGKILL` lands with attempts genuinely in flight, and
/// returns its input so `verify` can check a payload sum rather than only a
/// row count.
#[pgqueue::job(max_attempts = 10, timeout_ms = 60_000)]
async fn stress_job(n: i64) -> anyhow::Result<i64> {
    tokio::time::sleep(Duration::from_millis(15)).await;
    Ok(n)
}

/// A second name so the dequeue path has more than one handler to route.
#[pgqueue::job(max_attempts = 10, timeout_ms = 60_000)]
async fn stress_dedupe(n: i64) -> anyhow::Result<i64> {
    Ok(n)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init()
        .ok();

    let database_url = std::env::var("DATABASE_URL")?;
    let mut args = std::env::args().skip(1);
    let command = args.next().unwrap_or_default();
    let count: i64 = args.next().unwrap_or_default().parse().unwrap_or(0);

    let queue = Queue::connect(&database_url).await?;
    match command.as_str() {
        "produce" => produce(&queue, count).await,
        "work" => work(queue).await,
        "verify" => verify(&queue, count).await,
        other => anyhow::bail!("unknown command {other:?}; expected produce, work, or verify"),
    }
}

async fn produce(queue: &Queue, count: i64) -> anyhow::Result<()> {
    let stale = sqlx::query(
        "DELETE FROM pgqueue.jobs
         WHERE queue = $1 AND name IN ('stress_job', 'stress_dedupe')",
    )
    .bind(queue.name())
    .execute(queue.pool())
    .await?
    .rows_affected();

    for n in 0..count {
        // Retention is pinned because `verify` runs after the workers do: under
        // the default ten minutes, the sweeper inside any still-running `work`
        // process deletes finished rows and `verify` reports the loss as a lost
        // job. `produce` deletes them instead, on its own schedule.
        queue
            .enqueue(stress_job::job(n).retention(JobRetention::Forever))
            .await?;
    }

    // Every one of these collapses onto a single row, so `verify` can tell a
    // broken dedupe apart from a lost job. Capped because contention, not
    // volume, is what makes it interesting. One transaction, so every contender
    // after the first collides with a row this transaction has inserted but not
    // committed, and no worker can complete it and free the key mid-burst. That
    // is what makes `deduped == 1` an invariant rather than a bet on no worker
    // running during `produce`.
    let contenders = count.min(500);
    let mut transaction = queue.pool().begin().await?;
    for _ in 0..contenders {
        queue
            .enqueue_in(
                &mut transaction,
                stress_dedupe::job(1)
                    .dedupe_key("stress-singleton")
                    .retention(JobRetention::Forever),
            )
            .await?;
    }
    transaction.commit().await?;

    println!("produced {count} jobs + {contenders} deduplicated enqueues, cleared {stale} stale");
    Ok(())
}

async fn work(queue: Queue) -> anyhow::Result<()> {
    let worker = Worker::builder(queue)
        .register_job(stress_job)
        .register_job(stress_dedupe)
        .concurrency(8)
        .poll_interval(Duration::from_millis(50))
        .shutdown_grace(Duration::from_secs(10))
        .build()?;
    let token = CancellationToken::new();
    let stop = token.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        stop.cancel();
    });
    worker.run_until(token).await?;
    Ok(())
}

async fn verify(queue: &Queue, count: i64) -> anyhow::Result<()> {
    let name = queue.name();

    // One snapshot for all six reads. Read committed would let a worker finish
    // a job between the count and the sum, and a disagreement between two
    // snapshots is indistinguishable from the corruption these checks hunt for.
    let mut snapshot = queue.pool().begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *snapshot)
        .await?;

    let statuses = sqlx::query_as::<_, (String, i64)>(
        "SELECT status, count(*) FROM pgqueue.jobs WHERE queue = $1 AND name = 'stress_job'
         GROUP BY status ORDER BY status",
    )
    .bind(name)
    .fetch_all(&mut *snapshot)
    .await?;
    println!("stress_job by status: {statuses:?}");

    let complete = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM pgqueue.jobs
         WHERE queue = $1 AND name = 'stress_job' AND status = 'complete'",
    )
    .bind(name)
    .fetch_one(&mut *snapshot)
    .await?;

    // Proves every distinct payload ran, not merely that `count` rows finished.
    // `sum()` is NUMERIC in Postgres, so cast before decoding it as an i64.
    let sum = sqlx::query_scalar::<_, Option<i64>>(
        "SELECT sum((result#>>'{}')::bigint)::bigint FROM pgqueue.jobs
         WHERE queue = $1 AND name = 'stress_job' AND status = 'complete'",
    )
    .bind(name)
    .fetch_one(&mut *snapshot)
    .await?
    .unwrap_or(0);

    let retried = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM pgqueue.jobs
         WHERE queue = $1 AND name = 'stress_job' AND attempts > 1",
    )
    .bind(name)
    .fetch_one(&mut *snapshot)
    .await?;

    let deduped = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM pgqueue.jobs WHERE queue = $1 AND name = 'stress_dedupe'",
    )
    .bind(name)
    .fetch_one(&mut *snapshot)
    .await?;

    let live_leases = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM pgqueue.workers WHERE queue = $1 AND expires_at > now()",
    )
    .bind(name)
    .fetch_one(&mut *snapshot)
    .await?;
    snapshot.rollback().await?;

    let expected_sum = (0..count).sum::<i64>();
    println!(
        "complete={complete}/{count} sum={sum}/{expected_sum} recovered={retried} dedupe_rows={deduped} live_leases={live_leases}"
    );

    anyhow::ensure!(
        complete == count,
        "expected {count} complete, got {complete}"
    );
    anyhow::ensure!(
        sum == expected_sum,
        "payload sum {sum} != {expected_sum}: a job ran with the wrong payload or not at all"
    );
    anyhow::ensure!(
        deduped == 1,
        "dedupe key produced {deduped} rows, expected 1"
    );
    println!(
        "OK: every job row reached complete exactly once with the right payload, \
         {retried} recovered after a crash"
    );
    Ok(())
}
