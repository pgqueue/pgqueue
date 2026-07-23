# pgqueue

Background and cron job processing for Rust, using PostgreSQL 18 as the backend.

`Queue::connect` creates and migrates the fixed `pgqueue` schema with SQLx. Queue
names isolate independent queues within that schema. `QueueBuilder::migration_mode`
can instead validate an externally migrated schema without DDL privileges, or
skip schema checks when deployment tooling owns them. Development queries are
compile-checked against the PostgreSQL 18 service in `compose.yaml`; the
checked-in `.sqlx` metadata supports offline and downstream builds.

## Enqueueing Jobs

```rust
use std::time::Duration;

use pgqueue::{Queue, Worker};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct SendEmail {
    to: String,
}

#[derive(Serialize, Deserialize)]
struct Receipt {
    delivered_to: String,
}

// Jobs are async functions with serializable inputs and outputs.
// Jobs may also take JobState<T> and JobContext extractors.
#[pgqueue::job(
    name = "deliver_email", // Job name; defaults to the function name.
    max_attempts = 5, // Includes the initial attempt.
    timeout_ms = 30_000, // Per-attempt timeout; 0 disables it.
    heartbeat_ms = 10_000, // Required JobContext::touch interval.
    ttl_ms = 3_600_000, // Result retention; 0 deletes immediately.
    retry_delay_ms = 500, // Base delay between attempts.
    backoff_max_ms = 60_000, // JobRetryBackoff cap; use bare `backoff` for no cap.
    priority = -10, // Lower values run first.
)]
async fn send_email(args: SendEmail) -> anyhow::Result<Receipt> {
    println!("emailing {}", args.to);
    Ok(Receipt {
        delivered_to: args.to,
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let queue = Queue::connect(&std::env::var("DATABASE_URL")?).await?;
    let email = SendEmail { to: "user@example.com".into() };
    let job = send_email::job(email)
        .unique_key("welcome:user@example.com")
        .delay(Duration::from_secs(5));

    // With another worker running, wait for the typed result instead:
    // let receipt = queue.apply(job, Some(Duration::from_secs(30))).await?;
    let outcome = queue.enqueue(job).await?;
    println!("job id: {}", outcome.handle().id());

    Worker::builder(queue)
        .register(send_email)
        .build()?
        .run()
        .await?;
    Ok(())
}
```

Enqueue returns `EnqueueOutcome::Enqueued` or `EnqueueOutcome::Deduplicated`.
Deduplication only covers a live row (`queued`, `running`, or `aborting`) with
the same queue and unique key; it is not exactly-once execution. Typed enqueue
rejects a key already owned by another job type. Use `enqueue_in` or
`enqueue_raw_in` to publish in a caller-owned SQLx transaction. Unique-key
advisory locks then remain held until that transaction ends, so applications
should use a consistent lock and enqueue order.

Delivery is at least once. A worker or database failure can run an attempt more
than once, so handlers should make external effects idempotent. Priority and
schedule determine selection order, while concurrent `SKIP LOCKED` dequeues may
overtake locked rows. A group key guarantees at most one live attempt per group
and preserves that group's ready-row order; it does not impose global FIFO.

## Defining Cron Jobs

```rust
use pgqueue::{JobContext, Queue, Worker};

// Cron handlers have no payload; every parameter is an extractor. Registering
// the job also registers its compile-time-validated schedule. Each occurrence
// is deduplicated across all workers.
#[pgqueue::cron(
    "0 * * * *", // Compile-time-validated schedule.
    revision = 1, // Increase when changing this durable definition.
    name = "collect_hourly_metrics", // Job name; defaults to the function name.
    max_attempts = 2, // Includes the initial attempt.
    timeout_ms = 120_000, // Per-attempt timeout; 0 disables it.
    heartbeat_ms = 30_000, // Required JobContext::touch interval.
    ttl_ms = 604_800_000, // Result retention; 0 deletes immediately.
    retry_delay_ms = 1_000, // Base delay between attempts.
    backoff_max_ms = 60_000, // JobRetryBackoff cap; use bare `backoff` for no cap.
    priority = 10, // Lower values run first.
)]
async fn hourly_metrics(ctx: JobContext) -> anyhow::Result<()> {
    let queued = ctx.queue().counts().await?.queued;
    println!("{queued} job(s) queued");
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let queue = Queue::connect(&std::env::var("DATABASE_URL")?).await?;
    Worker::builder(queue)
        .register(hourly_metrics)
        .build()?
        .run()
        .await?;
    Ok(())
}
```

Cron definitions live in `pgqueue.cron_schedules`; workers publish a job only
when its durable cursor becomes due, rather than keeping a speculative future
job row. Cron expressions are evaluated in UTC. Workers with the same revision
must provide the same canonical
definition. A higher revision takes authority, and lower-revision workers stay
running but report degraded health and stop scheduling that key. Runtime
schedules can select `CronMisfirePolicy::Skip` with a bounded grace or
`FireOnce`, which publishes only the most recent missed occurrence. Cursor
advance, occurrence claim, and job insertion commit atomically. A foreign live
job holding the cron key causes that occurrence to be claimed and skipped. A
template-only revision preserves a due cursor; changing the cron expression
starts the revised schedule at its next UTC occurrence.

## Operations

- `Worker::health` reports `Starting`, `Ready`, `Degraded`, and `Stopped`, with
  failures attributed to notification, dequeue, abort, scheduler, sweeper, or
  worker-heartbeat components. A worker-hosted dashboard returns HTTP 503 from
  `/health` while any component is degraded.
- Sweeps are leader-coordinated and bounded by `QueueBuilder::sweep_batch_size`.
  `SweeperReport::more_work` indicates that another pass may be useful.
- `Queue::jobs_page` uses a stable `JobCursor` and caps pages at 1,000 rows.
- Custom consumers should use `Queue::consumer`. Its `Attempt` values fence
  touch, retry, and finish operations to the dequeued attempt and worker ID.
  Call `Consumer::heartbeat` before dequeueing and refresh its lease while any
  attempt is live.

## Dashboard

Enable the `dashboard` feature to host the dashboard with a worker.

```rust
use pgqueue::{Dashboard, Queue, Worker};

#[pgqueue::job]
async fn cleanup(_: ()) {}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let queue = Queue::connect(&std::env::var("DATABASE_URL")?).await?;
    let dashboard = Dashboard::new([queue.clone()])
        .serve_on("127.0.0.1:8080".parse()?);

    Worker::builder(queue)
        .register(cleanup)
        .dashboard(dashboard)
        .build()?
        .run()
        .await?;
    Ok(())
}
```

## Best Practices

- Evolve payload structs additively. Payloads are stored as JSON and decoded
  with the currently deployed struct on every attempt: new fields need
  `#[serde(default)]` or `Option`, renamed fields need `#[serde(alias = "…")]`,
  and `#[serde(deny_unknown_fields)]` should be avoided. A payload that no
  longer decodes fails its job permanently, without retrying.
- Keep job names stable. Rows are routed to handlers by name, so renaming a
  job strands already-enqueued rows. When renaming, keep a handler registered
  under the old name until the queue drains.
- Give long jobs a deadline. A per-attempt `timeout_ms` — or `heartbeat_ms`
  plus periodic `JobContext::touch` — lets the sweeper recover jobs from
  crashed workers promptly. With neither, recovery waits on the dead worker's
  lease to expire. Low-level consumers that never create a worker lease have
  no such protection: their deadline-free attempts become sweepable after the
  queue's sweep grace.
- Choose retention deliberately. Results are kept for `ttl_ms` (default 10
  minutes). Anything you may want to inspect or retry from the dashboard must
  still be retained.
- Reserve `cron:` unique keys for schedules. A live one-off job holding a
  cron's dedupe key makes due occurrences skip while the key remains held.
- Shut down gracefully. Use `run` (SIGINT/SIGTERM) or `run_until` with a
  `CancellationToken`; in-flight jobs get the shutdown grace and are then
  requeued, not lost.
