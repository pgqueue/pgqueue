use pgqueue::{Dashboard, Queue};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init()
        .map_err(|error| anyhow::anyhow!("failed to initialize logging: {error}"))?;

    let database_url = std::env::var("DATABASE_URL")?;
    let queue = Queue::connect(&database_url).await?;
    let username = std::env::var("PGQUEUE_DASHBOARD_USERNAME")?;
    let password = std::env::var("PGQUEUE_DASHBOARD_PASSWORD")?;
    if username.is_empty() || password.is_empty() {
        anyhow::bail!("dashboard username and password must not be empty");
    }
    let secure_cookies = match std::env::var("PGQUEUE_DASHBOARD_SECURE_COOKIES") {
        Ok(value) => parse_bool(&value)?,
        Err(_) => true,
    };

    Dashboard::new([queue])
        .basic_auth(username, password)
        .secure_cookies(secure_cookies)
        .serve_on("0.0.0.0", 8080)
        .run()
        .await?;
    Ok(())
}

/// Accepts the usual spellings on both sides rather than treating everything
/// that is not exactly `false` as true.
fn parse_bool(value: &str) -> anyhow::Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => anyhow::bail!("expected a boolean (true/false), got {other:?}"),
    }
}
