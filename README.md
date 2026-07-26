# pgqueue

Async and cron jobs for Rust, backed by PostgreSQL 18+.

## Features

- Turn async functions into background jobs with `#[pgqueue::job]`.
- Schedule cron jobs with `#[pgqueue::cron]`.
- Uses Postgres only. No separate message broker.
- Retry, delay, prioritize, deduplicate, and wait for jobs.
- Inspect queues, workers, and jobs in the built-in dashboard.

## Background Jobs

Define a job and start a worker:

```rust
use pgqueue::{Queue, Worker};
use serde::{Deserialize, Serialize};

// Background job input (JSON-serializable).
#[derive(Serialize, Deserialize)]
pub struct Email {
    pub address: String,
}

// Background job output (JSON-serializable).
#[derive(Serialize, Deserialize)]
pub struct Receipt {
    pub address: String,
}

// Define a background job.
#[pgqueue::job(
    // Job name, at most 255 bytes (optional; default: function name).
    name = "deliver_email",
    // Total attempts including the initial run (optional; default: 1).
    max_attempts = 5,
    // Max duration of each attempt in milliseconds (optional; default: 10,000; 0 disables timeout).
    timeout_ms = 30_000,
    // Result retention in milliseconds (optional; default: 600,000; 0 deletes immediately).
    result_ttl_ms = 3_600_000,
    // Base retry delay in milliseconds (optional; default: 0).
    retry_delay_ms = 500,
    // Max exponential backoff in milliseconds (optional; default: disabled).
    max_backoff_ms = 60_000,
    // Dequeue priority; lower values run first (optional; default: 0).
    priority = -10,
)]
pub async fn send_email(args: Email) -> anyhow::Result<Receipt> {
    println!("emailing {}", args.address);
    Ok(Receipt { address: args.address })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let database_url = std::env::var("DATABASE_URL")?;
    let queue = Queue::connect(&database_url).await?;

    Worker::builder(queue)
        .register_job(send_email)
        .run()
        .await?;

    Ok(())
}
```

Elsewhere, enqueue the job:

```rust
use std::time::Duration;

use crate::{send_email, Email, Receipt};
use pgqueue::{EnqueueResult, JobHandle, Queue};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let database_url = std::env::var("DATABASE_URL")?;
    let queue = Queue::connect(&database_url).await?;

    // Enqueue without waiting for the job to finish.
    let job1 = send_email::job(Email { address: "user1@example.com".into() })
        .dedupe_key("welcome:user@example.com")
        .delay(Duration::from_secs(5));
    let result: EnqueueResult<JobHandle<send_email>> = queue.enqueue(job1).await?;
    println!("job id: {}", result.job_id());

    // Enqueue and wait for the job to finish.
    let job2 = send_email::job(Email { address: "user2@example.com".into() });
    let receipt: Receipt = queue
        .enqueue_and_wait(job2, Some(Duration::from_secs(30)))
        .await?;
    println!("receipt for: {}", receipt.address);

    Ok(())
}
```

## Cron Jobs

Define cron jobs to run on recurring schedule:

```rust
use pgqueue::{JobContext, Queue, Worker};

// Cron jobs have no payload. Parameters are context extractors.
#[pgqueue::cron(
    // Schedule in UTC (required).
    "0 * * * *",
    // Revision; increment after changes, highest wins across workers (optional; default: 0).
    revision = 1,
    // Job name, at most 250 bytes due to the cron dedupe key (optional; default: function name).
    name = "collect_hourly_metrics",
    // Total attempts including the initial run (optional; default: 1).
    max_attempts = 2,
    // Max duration of each attempt in milliseconds (optional; default: 10,000; 0 disables timeout).
    timeout_ms = 120_000,
    // Result retention in milliseconds (optional; default: 600,000; 0 deletes immediately).
    result_ttl_ms = 604_800_000,
    // Base retry delay in milliseconds (optional; default: 0).
    retry_delay_ms = 1_000,
    // Max exponential backoff in milliseconds (optional; default: disabled).
    max_backoff_ms = 60_000,
    // Dequeue priority; lower values run first (optional; default: 0).
    priority = 10,
)]
async fn collect_hourly_metrics(ctx: JobContext) -> anyhow::Result<()> {
    let queued = ctx.queue().counts().await?.queued;
    println!("{queued} job(s) queued");
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let database_url = std::env::var("DATABASE_URL")?;
    let queue = Queue::connect(&database_url).await?;

    Worker::builder(queue)
        .register_cron(collect_hourly_metrics)
        .run()
        .await?;

    Ok(())
}
```

For schedules loaded at runtime, define a regular `#[pgqueue::job]` and use `WorkerBuilder::schedule_cron`:

```rust
use pgqueue::{Queue, Worker};

#[pgqueue::job]
async fn cleanup(_: ()) -> anyhow::Result<()> {
    println!("cleaning up");
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let database_url = std::env::var("DATABASE_URL")?;
    let queue = Queue::connect(&database_url).await?;

    Worker::builder(queue)
        .schedule_cron("0 3 * * *", cleanup::job(()))
        .run()
        .await?;

    Ok(())
}
```

## Dashboard

Run the built-in web dashboard as a standalone server to inspect queues, workers, and jobs:

```rust
use pgqueue::{Dashboard, Queue};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let database_url = std::env::var("DATABASE_URL")?;
    let queue = Queue::connect(&database_url).await?;

    Dashboard::new([queue])
        .basic_auth("admin", std::env::var("PGQUEUE_DASHBOARD_PASSWORD")?)
        .serve_on("localhost", 8080)
        .run()
        .await?;

    Ok(())
}
```
