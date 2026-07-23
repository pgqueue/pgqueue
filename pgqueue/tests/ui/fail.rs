#[pgqueue::job(timeout_ms = "30000")]
async fn bad_duration(_: ()) {}

#[pgqueue::job(max_attempts = 2147483647)]
async fn bad_attempts(_: ()) {}

#[pgqueue::job(max_attempt = 3)]
async fn unknown_attribute(_: ()) {}

#[pgqueue::job(revision = 1)]
async fn job_with_cron_revision(_: ()) {}

#[pgqueue::job]
async fn no_payload() {}

#[pgqueue::job]
fn not_async(_: ()) {}

#[pgqueue::job]
async unsafe fn unsafe_job(_: ()) {}

#[pgqueue::cron("* * * * *")]
async unsafe fn unsafe_cron() {}

#[pgqueue::job]
async fn generic<T: serde::de::DeserializeOwned>(args: T) {
    let _ = args;
}

#[pgqueue::cron("99 * * * *")]
async fn impossible() {}

#[pgqueue::cron(30)]
async fn not_a_string() {}

#[derive(Clone)]
struct NotAnExtractor;

#[pgqueue::job]
async fn bad_extractor(_: (), value: NotAnExtractor) {
    let _ = value;
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Payload;

#[pgqueue::cron("* * * * *")]
async fn cron_payload(value: Payload) {
    let _ = value;
}

fn main() {}
