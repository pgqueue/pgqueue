use pgqueue::{JobContext, JobState, JobType};

#[derive(Clone)]
struct Db;

#[derive(serde::Serialize, serde::Deserialize)]
struct Payload {
    value: u32,
}

#[pgqueue::job(max_attempts = 3, timeout_ms = 30_000, priority = -1)]
async fn work(args: Payload, db: JobState<Db>, ctx: JobContext) -> anyhow::Result<u32> {
    let (_, _) = (db, ctx);
    Ok(args.value)
}

#[pgqueue::cron("*/5 * * * *", max_attempts = 2, revision = 3)]
async fn cleanup(db: JobState<Db>) {
    let _ = db;
}

fn main() {
    assert_eq!(work::NAME, "work");
    assert_eq!(cleanup::SCHEDULE, Some("*/5 * * * *"));
    assert_eq!(cleanup::CRON_REVISION, 3);
    let _ = work::job(Payload { value: 1 });
    let _ = cleanup::job();
}
