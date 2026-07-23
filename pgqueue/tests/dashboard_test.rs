//! Dashboard router tests, driven with `tower::ServiceExt::oneshot` — no
//! listener needed.

#![cfg(feature = "dashboard")]

use sqlx::PgPool;
use std::time::Duration;

use crate::{
    EnqueueOutcomeTestExt, QueueProtocolTestExt, TestDb, new_job, wait_for_worker_intake_closed,
};
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use pgqueue::{
    CronMisfirePolicy, CronOptions, Dashboard, Error, JobRequest, JobState, JobStatus, Worker,
    WorkerHealthStatus, WorkerTimers,
};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;
use uuid::Uuid;

#[pgqueue::job]
async fn dashboard_probe(_: ()) {}

#[pgqueue::job]
async fn dashboard_slow(_: (), state: JobState<DashboardDrain>) {
    state.0.started.notify_one();
    state.0.release.notified().await;
}

#[derive(Clone)]
struct DashboardDrain {
    started: std::sync::Arc<tokio::sync::Notify>,
    release: std::sync::Arc<tokio::sync::Notify>,
}

async fn get_json(router: &axum::Router, path: &str) -> (StatusCode, Value) {
    request(router, "GET", path, None).await
}

async fn post_json(router: &axum::Router, path: &str) -> (StatusCode, Value) {
    request(router, "POST", path, None).await
}

async fn request(
    router: &axum::Router,
    method: &str,
    path: &str,
    auth: Option<&str>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(path);
    if let Some(auth) = auth {
        builder = builder.header(header::AUTHORIZATION, auth);
    }
    if method == "POST" {
        builder = builder.header("x-pgqueue-request", "dashboard");
    }
    let response = router
        .clone()
        .oneshot(builder.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .unwrap();
    let value = serde_json::from_slice(&bytes)
        .unwrap_or(Value::String(String::from_utf8_lossy(&bytes).to_string()));
    (status, value)
}

async fn login_cookie(router: &axum::Router) -> String {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/login")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from("username=admin&password=s3cret"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    response.headers()[header::SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string()
}

async fn http_get(address: std::net::SocketAddr, path: &str, auth: Option<&str>) -> String {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut stream = loop {
        match tokio::net::TcpStream::connect(address).await {
            Ok(stream) => break stream,
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(error) => panic!("dashboard did not listen at {address}: {error}"),
        }
    };
    let auth = auth
        .map(|value| format!("Authorization: {value}\r\n"))
        .unwrap_or_default();
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: {address}\r\n{auth}Connection: close\r\n\r\n")
                .as_bytes(),
        )
        .await
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    response
}

#[sqlx::test(migrations = "./migrations")]
async fn health_endpoint_reports_ok(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = Dashboard::new([db.queue.clone()]).router().unwrap();
    let (status, body) = get_json(&router, "/health").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, Value::String("OK".into()));
}

#[sqlx::test(migrations = "./migrations")]
async fn worker_hosted_health_reports_degraded_worker_components(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let key = "dashboard-health-revision";
    let higher = Worker::builder(db.queue.clone())
        .register(dashboard_probe)
        .cron_with_options(
            "0 0 1 1 *",
            dashboard_probe::job(()).unique_key(key),
            CronOptions {
                revision: 2,
                misfire: CronMisfirePolicy::default(),
            },
        )
        .timers(crate::test_timers())
        .build()
        .unwrap();
    let higher_shutdown = CancellationToken::new();
    let higher_run = tokio::spawn(higher.run_until(higher_shutdown.clone()));
    crate::wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        "authoritative cron revision was not stored",
        || async {
            sqlx::query_scalar!(
                "SELECT revision FROM pgqueue.cron_schedules WHERE queue = $1 AND unique_key = $2",
                db.queue.name(),
                key,
            )
            .fetch_optional(&pool)
            .await
            .unwrap()
                == Some(2)
        },
    )
    .await;
    higher_shutdown.cancel();
    higher_run.await.unwrap().unwrap();

    let dashboard = Dashboard::new([db.queue.clone()]).serve_on("127.0.0.1:0".parse().unwrap());
    let mut dashboard_handle = dashboard.handle();
    let lower = Worker::builder(db.queue.clone())
        .register(dashboard_probe)
        .cron_with_options(
            "0 0 1 1 *",
            dashboard_probe::job(()).unique_key(key),
            CronOptions {
                revision: 1,
                misfire: CronMisfirePolicy::default(),
            },
        )
        .timers(WorkerTimers {
            schedule: Duration::from_millis(50),
            ..crate::test_timers()
        })
        .dashboard(dashboard)
        .build()
        .unwrap();
    let health = lower.health();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(lower.run_until(shutdown.clone()));
    let address = tokio::time::timeout(Duration::from_secs(5), dashboard_handle.wait_until_ready())
        .await
        .unwrap()
        .unwrap();
    crate::wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        "lower revision worker did not degrade",
        || async { health.snapshot().status == WorkerHealthStatus::Degraded },
    )
    .await;

    let response = http_get(address, "/health", None).await;
    assert!(response.starts_with("HTTP/1.1 503"), "{response}");
    shutdown.cancel();
    run.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn queues_overview_lists_bounded_signals_and_workers_pages(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    db.queue.enqueue_raw(new_job("a", |_| {})).await.unwrap();
    let worker_id = Uuid::now_v7();
    db.queue
        .write_worker_info(
            worker_id,
            json!({"complete": 1}),
            None,
            Duration::from_secs(60),
        )
        .await
        .unwrap();

    let router = Dashboard::new([db.queue.clone()]).router().unwrap();
    let (status, body) = get_json(&router, "/api/queues").await;
    assert_eq!(status, StatusCode::OK);
    let queues = body["queues"].as_array().unwrap();
    assert_eq!(queues.len(), 1);
    assert_eq!(queues[0]["name"], "default");
    assert!(queues[0]["oldest_ready_at"].is_string());
    assert_eq!(queues[0]["execution"], "idle");
    assert_eq!(queues[0]["has_live_workers"], true);
    assert!(queues[0]["latest_failure_at"].is_null());

    let (status, body) = get_json(&router, "/api/queues/default/workers?limit=1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["workers"].as_array().unwrap().len(), 1);
    assert!(body["next_cursor"].is_null());

    let (status, body) =
        get_json(&router, &format!("/api/queues/default/workers/{worker_id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["worker"]["id"], worker_id.to_string());

    let missing = Uuid::now_v7();
    let (status, _) = get_json(&router, &format!("/api/queues/default/workers/{missing}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[sqlx::test(migrations = "./migrations")]
async fn worker_pages_accept_non_object_stats(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let null_stats_worker = Uuid::now_v7();
    db.queue
        .write_worker_info(
            null_stats_worker,
            Value::Null,
            None,
            Duration::from_secs(60),
        )
        .await
        .unwrap();
    let scalar_stats_worker = Uuid::now_v7();
    db.queue
        .write_worker_info(scalar_stats_worker, json!(7), None, Duration::from_secs(60))
        .await
        .unwrap();

    let router = Dashboard::new([db.queue.clone()]).router().unwrap();
    let (status, body) = get_json(&router, "/api/queues/default/workers").await;
    assert_eq!(status, StatusCode::OK);
    let workers = body["workers"].as_array().unwrap();
    assert!(workers.iter().any(|worker| worker["stats"].is_null()));
    assert!(workers.iter().any(|worker| worker["stats"] == 7));

    let (status, body) = get_json(
        &router,
        &format!("/api/queues/default/workers/{null_stats_worker}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["worker"]["stats"].is_null());

    let (status, body) = get_json(
        &router,
        &format!("/api/queues/default/workers/{scalar_stats_worker}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["worker"]["stats"], 7);

    let (status, body) = get_json(&router, "/static/app.js").await;
    assert_eq!(status, StatusCode::OK);
    let javascript = body.as_str().unwrap();
    assert!(javascript.contains("w.stats?.complete"));
    assert!(javascript.contains("worker.stats?.complete"));
}

#[sqlx::test(migrations = "./migrations")]
async fn worker_pages_use_cursors_without_exact_totals(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    for complete in 1..=3 {
        db.queue
            .write_worker_info(
                Uuid::now_v7(),
                json!({"complete": complete}),
                None,
                Duration::from_secs(60),
            )
            .await
            .unwrap();
    }
    let router = Dashboard::new([db.queue.clone()]).router().unwrap();

    let (status, first) = get_json(&router, "/api/queues/default/workers?limit=2").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["workers"].as_array().unwrap().len(), 2);
    assert!(first.get("total").is_none());
    let first_ids = first["workers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|worker| worker["id"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    let cursor = first["next_cursor"].as_object().unwrap();
    let cursor_time = cursor["started_at"].as_str().unwrap();
    let cursor_id = cursor["id"].as_str().unwrap();

    let (status, second) = get_json(
        &router,
        &format!(
            "/api/queues/default/workers?limit=2&cursor_started_at={cursor_time}&cursor_id={cursor_id}"
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(second["workers"].as_array().unwrap().len(), 1);
    assert!(second["next_cursor"].is_null());
    assert!(!first_ids.contains(&second["workers"][0]["id"].as_str().unwrap().to_owned()));

    let (status, _) = get_json(
        &router,
        "/api/queues/default/workers?cursor_id=00000000-0000-0000-0000-000000000000",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[sqlx::test(migrations = "./migrations")]
async fn queue_signals_report_ready_scheduled_execution_and_failure_states(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    db.queue
        .enqueue_raw(new_job("ready", |_| {}))
        .await
        .unwrap();
    db.queue
        .enqueue(dashboard_probe::job(()).delay(Duration::from_secs(3_600)))
        .await
        .unwrap()
        .unwrap();
    let mut running = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap();
    let running = running.remove(0);
    db.queue
        .enqueue_raw(new_job("failure", |_| {}))
        .await
        .unwrap();
    let mut failed = db.queue.dequeue(1, Uuid::now_v7()).await.unwrap();
    let failed = failed.remove(0);
    assert!(
        db.queue
            .finish(&failed, JobStatus::Failed, None, Some("test failure"))
            .await
            .unwrap()
    );

    let router = Dashboard::new([db.queue.clone()]).router().unwrap();
    let (status, body) = get_json(&router, "/api/queues").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["queues"][0]["oldest_ready_at"].is_null());
    assert!(body["queues"][0]["next_scheduled_at"].is_string());
    assert_eq!(body["queues"][0]["execution"], "running");
    assert!(body["queues"][0]["latest_failure_at"].is_string());

    assert!(
        db.queue
            .abort(running.id, "dashboard signal test")
            .await
            .unwrap()
    );
    let (_, body) = get_json(&router, "/api/queues").await;
    assert_eq!(body["queues"][0]["execution"], "aborting");

    let (status, _) = get_json(&router, "/api/queues/default").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[sqlx::test(migrations = "./migrations")]
async fn jobs_listing_filters_by_status_and_name(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    for name in ["alpha", "alpha", "beta"] {
        db.queue.enqueue_raw(new_job(name, |_| {})).await.unwrap();
    }
    db.queue.dequeue(1, Uuid::now_v7()).await.unwrap();

    let router = Dashboard::new([db.queue.clone()]).router().unwrap();

    let (_, body) = get_json(&router, "/api/queues/default/jobs").await;
    assert_eq!(body["jobs"].as_array().unwrap().len(), 3);

    let (_, body) = get_json(&router, "/api/queues/default/jobs?status=queued").await;
    assert_eq!(body["jobs"].as_array().unwrap().len(), 2);

    let (_, body) = get_json(&router, "/api/queues/default/jobs?status=queued,running").await;
    assert_eq!(body["jobs"].as_array().unwrap().len(), 3);

    let (_, body) = get_json(&router, "/api/queues/default/jobs?status=queued,queued").await;
    assert_eq!(body["jobs"].as_array().unwrap().len(), 2);

    let (_, body) = get_json(&router, "/api/queues/default/jobs?status=").await;
    assert_eq!(body["jobs"].as_array().unwrap().len(), 3);

    let (_, body) = get_json(&router, "/api/queues/default/jobs?name=beta").await;
    assert_eq!(body["jobs"].as_array().unwrap().len(), 1);

    let (_, body) = get_json(&router, "/api/queues/default/jobs?name=ALP").await;
    assert_eq!(
        body["jobs"].as_array().unwrap().len(),
        0,
        "job listing uses an exact handler name"
    );

    let (_, body) = get_json(&router, "/api/queues/default/jobs?limit=1").await;
    assert_eq!(body["jobs"].as_array().unwrap().len(), 1);
    let first_id = body["jobs"][0]["id"].as_str().unwrap();
    let cursor = body["next_cursor"].as_object().unwrap();
    let cursor_time = cursor["enqueued_at"].as_str().unwrap();
    let cursor_id = cursor["id"].as_str().unwrap();
    let (_, body) = get_json(
        &router,
        &format!(
            "/api/queues/default/jobs?limit=1&cursor_enqueued_at={cursor_time}&cursor_id={cursor_id}"
        ),
    )
    .await;
    assert_eq!(body["jobs"].as_array().unwrap().len(), 1);
    assert_ne!(body["jobs"][0]["id"], first_id);

    let (_, body) = get_json(&router, "/api/queues/default/job-names?kind=job&prefix=ALP").await;
    assert_eq!(body["names"], json!(["alpha"]));

    let (status, _) = get_json(
        &router,
        "/api/queues/default/jobs?cursor_id=00000000-0000-0000-0000-000000000000",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _) = get_json(&router, "/api/queues/default/jobs?status=bogus").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _) = get_json(&router, "/api/queues/default/jobs?status=queued,bogus").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _) = get_json(&router, "/api/queues/default/jobs?status=active").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _) = get_json(&router, "/api/queues/default/jobs?kind=bogus").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _) = get_json(
        &router,
        "/api/queues/default/job-names?kind=bogus&prefix=job",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _) = get_json(&router, "/api/queues/default/jobs?offset=1").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, _) = get_json(&router, "/api/queues/default/jobs?updated_within=60").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[sqlx::test(migrations = "./migrations")]
async fn dashboard_separates_jobs_and_crons(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    db.queue
        .enqueue(dashboard_probe::job(()))
        .await
        .unwrap()
        .unwrap();
    let shutdown = CancellationToken::new();
    let worker = Worker::builder(db.queue.clone())
        .register(dashboard_probe)
        .cron(
            "* * * * * *",
            dashboard_probe::job(()).unique_key("custom-dashboard-cron"),
        )
        .timers(crate::test_timers())
        .poll_interval(Duration::from_millis(20))
        .dequeue_timeout(Duration::from_millis(50))
        .build()
        .unwrap();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    let router = Dashboard::new([db.queue.clone()]).router().unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let cron = loop {
        let (_, body) = get_json(&router, "/api/queues/default/jobs?kind=cron").await;
        if let Some(cron) = body["jobs"].as_array().and_then(|jobs| jobs.first()) {
            break cron.clone();
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "cron row did not appear"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };

    assert_eq!(cron["kind"], "cron");
    assert_eq!(cron["cron_expr"], "* * * * * *");
    assert_eq!(cron["unique_key"], "custom-dashboard-cron");
    let (_, body) = get_json(&router, "/api/queues/default/jobs?kind=job").await;
    assert_eq!(body["jobs"].as_array().unwrap().len(), 1);
    assert_eq!(body["jobs"][0]["kind"], "job");
    assert!(body["jobs"][0]["cron_expr"].is_null());

    let id = cron["id"].as_str().unwrap();
    let (_, body) = get_json(&router, &format!("/api/queues/default/jobs/{id}")).await;
    assert_eq!(body["job"]["kind"], "cron");
    assert_eq!(body["job"]["cron_expr"], "* * * * * *");
    assert!(
        body["cron_description"]
            .as_str()
            .is_some_and(|description| !description.is_empty())
    );

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn job_detail_retry_and_abort_actions(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(new_job("j", |_| {}))
        .await
        .unwrap()
        .unwrap();
    let router = Dashboard::new([db.queue.clone()]).router().unwrap();

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/queues/default/jobs/{id}/abort"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN, "CSRF guard");

    let (status, body) = get_json(&router, &format!("/api/queues/default/jobs/{id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["job"]["id"], json!(id.to_string()));
    assert_eq!(body["job"]["status"], "queued");

    // Abort the queued job from the dashboard.
    let (status, body) = post_json(&router, &format!("/api/queues/default/jobs/{id}/abort")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["aborted"], true);
    let row = db.queue.job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Aborted);
    assert_eq!(row.error.as_deref(), Some("aborted from dashboard"));

    // Retry it as a fresh occurrence, preserving the terminal row.
    let (status, body) = post_json(&router, &format!("/api/queues/default/jobs/{id}/retry")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["retried"], true);
    let retry_id: Uuid = body["job_id"].as_str().unwrap().parse().unwrap();
    assert_ne!(retry_id, id);
    assert_eq!(
        db.queue.job(id).await.unwrap().unwrap().status,
        JobStatus::Aborted
    );
    let row = db.queue.job(retry_id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Queued);

    // Retrying a queued job is a no-op.
    let (_, body) = post_json(
        &router,
        &format!("/api/queues/default/jobs/{retry_id}/retry"),
    )
    .await;
    assert_eq!(body["retried"], false);

    // Missing job.
    let missing = Uuid::now_v7();
    let (status, _) = get_json(&router, &format!("/api/queues/default/jobs/{missing}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[sqlx::test(migrations = "./migrations")]
async fn retry_reruns_a_cron_occurrence_when_the_next_occurrence_is_live(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    // A failed cron occurrence...
    let failed = db
        .queue
        .enqueue_raw(new_job("tick", |_| {}))
        .await
        .unwrap()
        .unwrap();
    sqlx::query!(
        "UPDATE pgqueue.jobs SET kind = 'cron', cron_expr = '* * * * *', \
         unique_key = 'cron:tick', status = 'failed', completed_at = now(), \
         error = 'failed: boom' WHERE id = $1",
        failed
    )
    .execute(db.queue.pool())
    .await
    .unwrap();
    // ...while the schedule loop has already enqueued the next occurrence
    // under the same unique key.
    let next = db
        .queue
        .enqueue_raw(new_job("tick", |job| {
            job.unique_key = Some("cron:tick".into())
        }))
        .await
        .unwrap()
        .unwrap();
    sqlx::query!(
        "UPDATE pgqueue.jobs SET kind = 'cron', cron_expr = '* * * * *' WHERE id = $1",
        next
    )
    .execute(db.queue.pool())
    .await
    .unwrap();

    let router = Dashboard::new([db.queue.clone()]).router().unwrap();
    let (status, body) =
        post_json(&router, &format!("/api/queues/default/jobs/{failed}/retry")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["retried"], true, "{body}");
    let retry_id: Uuid = body["job_id"].as_str().unwrap().parse().unwrap();

    // The manual rerun is a keyless one-off beside the live next occurrence.
    let rerun = db.queue.job(retry_id).await.unwrap().unwrap();
    assert_eq!(rerun.status, JobStatus::Queued);
    assert_eq!(rerun.unique_key, None);
    assert_eq!(
        db.queue.job(next).await.unwrap().unwrap().status,
        JobStatus::Queued,
        "the scheduled occurrence is untouched"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn basic_auth_gates_every_route(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = Dashboard::new([db.queue.clone()])
        .basic_auth("admin", "s3cret")
        .router()
        .unwrap();

    let (status, _) = get_json(&router, "/api/queues").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = get_json(&router, "/").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // echo -n "admin:s3cret" | base64 => YWRtaW46czNjcmV0
    let (status, _) = request(
        &router,
        "GET",
        "/api/queues",
        Some("Basic YWRtaW46czNjcmV0"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = request(&router, "GET", "/", Some("Basic YWRtaW46czNjcmV0")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.as_str()
            .unwrap()
            .contains("name=\"pgqueue-user\" content=\"admin\"")
    );

    // RFC 7617: the auth-scheme token is case-insensitive, and more than one
    // space may separate it from the credentials.
    let (status, _) = request(
        &router,
        "GET",
        "/api/queues",
        Some("basic YWRtaW46czNjcmV0"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = request(
        &router,
        "GET",
        "/api/queues",
        Some("BASIC  YWRtaW46czNjcmV0"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = request(
        &router,
        "GET",
        "/api/queues",
        Some("Basic d3Jvbmc6Y3JlZHM="),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[sqlx::test(migrations = "./migrations")]
async fn browser_auth_supports_password_changes_and_logout(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = Dashboard::new([db.queue.clone()])
        .basic_auth("admin", "s3cret")
        .router()
        .unwrap();

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/")
                .header(header::ACCEPT, "text/html")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert_eq!(response.headers()[header::LOCATION], "/login");

    let (status, body) = get_json(&router, "/login").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.as_str().unwrap().contains("PGQUEUE dashboard"));
    assert!(body.as_str().unwrap().contains("value=\"admin\""));

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/login")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from("username=admin&password=s3cret"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert_eq!(response.headers()[header::LOCATION], "/");
    assert!(
        response.headers()[header::SET_COOKIE]
            .to_str()
            .unwrap()
            .contains("; Secure;")
    );
    let cookie = response.headers()[header::SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/queues")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/account/password")
                .header(header::COOKIE, &cookie)
                .header("x-pgqueue-request", "dashboard")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"current_password":"s3cret","new_password":"newsecret"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let (status, _) = request(
        &router,
        "GET",
        "/api/queues",
        Some("Basic YWRtaW46czNjcmV0"),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = request(
        &router,
        "GET",
        "/api/queues",
        Some("Basic YWRtaW46bmV3c2VjcmV0"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/account/logout")
                .header(header::COOKIE, &cookie)
                .header("x-pgqueue-request", "dashboard")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response.headers()[header::SET_COOKIE]
            .to_str()
            .unwrap()
            .contains("Max-Age=0")
    );
    assert!(
        response.headers()[header::SET_COOKIE]
            .to_str()
            .unwrap()
            .contains("; Secure;")
    );

    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/queues")
                .header(header::COOKIE, cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[sqlx::test(migrations = "./migrations")]
async fn browser_auth_can_opt_out_of_secure_cookies_for_direct_http(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = Dashboard::new([db.queue.clone()])
        .basic_auth("admin", "s3cret")
        .secure_cookies(false)
        .router()
        .unwrap();

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/login")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from("username=admin&password=s3cret"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    let cookie = response.headers()[header::SET_COOKIE].to_str().unwrap();
    assert!(!cookie.contains("; Secure;"));
    assert!(cookie.contains("; HttpOnly; SameSite=Strict;"));
}

#[sqlx::test(migrations = "./migrations")]
async fn browser_auth_namespaces_session_cookies_per_dashboard(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let first = Dashboard::new([db.queue.clone()])
        .basic_auth("admin", "s3cret")
        .router()
        .unwrap();
    let second = Dashboard::new([db.queue.clone()])
        .basic_auth("admin", "s3cret")
        .router()
        .unwrap();

    let first_cookie = login_cookie(&first).await;
    let second_cookie = login_cookie(&second).await;
    assert_ne!(
        first_cookie.split_once('=').map(|(name, _)| name),
        second_cookie.split_once('=').map(|(name, _)| name)
    );

    let browser_cookies = format!("{first_cookie}; {second_cookie}");
    for router in [&first, &second] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/queues")
                    .header(header::COOKIE, &browser_cookies)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn browser_auth_scopes_session_cookie_to_mount_path(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = Dashboard::new([db.queue.clone()])
        .mount_path("/admin")
        .basic_auth("admin", "s3cret")
        .router()
        .unwrap();

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/login")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from("username=admin&password=s3cret"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    let set_cookie = response.headers()[header::SET_COOKIE].to_str().unwrap();
    assert!(set_cookie.contains("; Path=/admin;"));
    let cookie = set_cookie.split(';').next().unwrap();

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/account/logout")
                .header(header::COOKIE, cookie)
                .header("x-pgqueue-request", "dashboard")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response.headers()[header::SET_COOKIE]
            .to_str()
            .unwrap()
            .contains("; Path=/admin;")
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn spa_shell_and_static_assets_are_served(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = Dashboard::new([db.queue.clone()])
        .mount_path("/admin")
        .router()
        .unwrap();

    for path in [
        "/",
        "/queues/default",
        &format!("/queues/default/workers/{}", Uuid::now_v7()),
        &format!("/queues/default/jobs/{}", Uuid::now_v7()),
    ] {
        let (status, body) = get_json(&router, path).await;
        assert_eq!(status, StatusCode::OK, "{path}");
        let html = body.as_str().unwrap();
        assert!(html.contains("<!doctype html>"), "{path}");
        // mount_path is injected for asset/API resolution.
        assert!(html.contains("/admin/static/app.js"), "{path}");
        assert!(html.contains("/admin/static/app.js?v="), "{path}");
        assert!(html.contains("/admin/static/app.css?v="), "{path}");
        assert!(!html.contains("{asset_version}"), "{path}");
        assert!(!html.contains("{username}"), "{path}");
        assert!(
            html.contains("name=\"pgqueue-root\" content=\"/admin\""),
            "{path}"
        );
        assert!(
            html.contains("name=\"pgqueue-user\" content=\"anonymous\""),
            "{path}"
        );
        assert!(
            html.contains("name=\"pgqueue-auth-enabled\" content=\"false\""),
            "{path}"
        );
    }

    let (status, body) = get_json(&router, "/static/app.js").await;
    assert_eq!(status, StatusCode::OK);
    let javascript = body.as_str().unwrap();
    for expected in [
        "pgqueue dashboard",
        "data-row-nav",
        "data-kind=\"cron\"",
        "job-name-suggestions",
        "next_cursor",
        "data-action=\"retry\"",
    ] {
        assert!(javascript.contains(expected), "missing {expected:?}");
    }

    let (status, body) = get_json(&router, "/static/app.css").await;
    assert_eq!(status, StatusCode::OK);
    let css = body.as_str().unwrap();
    assert!(css.contains(".breadcrumbs li:first-child > a"));
    assert!(css.contains("text-overflow: ellipsis"));
    assert!(css.contains("padding-inline: 0.3rem !important"));

    let (status, _) = get_json(&router, "/static/pico.min.css").await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = get_json(&router, "/static/nope.css").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let response = router
        .clone()
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert!(
        response
            .headers()
            .contains_key(header::CONTENT_SECURITY_POLICY)
    );
    assert_eq!(
        response.headers().get(header::CACHE_CONTROL).unwrap(),
        "no-store"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn mount_path_adds_leading_slash_when_relative(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = Dashboard::new([db.queue.clone()])
        .mount_path("admin")
        .router()
        .unwrap();

    let (status, body) = get_json(&router, "/queues/default").await;
    assert_eq!(status, StatusCode::OK);
    let html = body.as_str().unwrap();
    assert!(html.contains("name=\"pgqueue-root\" content=\"/admin\""));
    assert!(html.contains("src=\"/admin/static/app.js?v="));
}

#[sqlx::test(migrations = "./migrations")]
async fn nested_static_assets_keep_their_cache_policy(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = axum::Router::new().nest(
        "/admin",
        Dashboard::new([db.queue.clone()])
            .mount_path("/admin")
            .router()
            .unwrap(),
    );
    let response = router
        .oneshot(
            Request::builder()
                .uri("/admin/static/app.js")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CACHE_CONTROL).unwrap(),
        "max-age=3600"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn mount_path_is_escaped_in_the_shell(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = Dashboard::new([db.queue.clone()])
        .mount_path("/admin\"><script>alert(1)</script>")
        .router()
        .unwrap();
    let (_, body) = get_json(&router, "/").await;
    let html = body.as_str().unwrap();
    assert!(!html.contains("<script>alert(1)</script>"));
    assert!(html.contains("&quot;&gt;&lt;script&gt;"));
}

#[sqlx::test(migrations = "./migrations")]
async fn dashboard_surfaces_worker_and_job_data_for_multiple_queues(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let other = db.another_queue(|b| b.name("emails")).await;
    let other_id = other
        .enqueue_raw(JobRequest::new("send", json!({"to": "x"})))
        .await
        .unwrap()
        .unwrap();

    let router = Dashboard::new([db.queue.clone(), other.clone()])
        .router()
        .unwrap();
    let (_, body) = get_json(&router, "/api/queues").await;
    let queues = body["queues"].as_array().unwrap();
    assert_eq!(queues.len(), 2);
    assert_eq!(queues[0]["name"], "default");
    assert_eq!(queues[1]["name"], "emails");
    assert!(queues[1]["oldest_ready_at"].is_string());

    let (status, _) = get_json(&router, &format!("/api/queues/default/jobs/{other_id}")).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "queue path cannot cross-read ids"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn dashboard_deduplicates_repeated_queue_handles(pool: PgPool) {
    let first = TestDb::new(pool.clone()).await;
    let second = TestDb::new(pool.clone()).await;
    let router = Dashboard::new([first.queue.clone(), second.queue.clone()])
        .router()
        .unwrap();
    let (_, body) = get_json(&router, "/api/queues").await;
    assert_eq!(body["queues"].as_array().unwrap().len(), 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn broken_database_yields_500s(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = Dashboard::new([db.queue.clone()]).router().unwrap();
    // Nuke the schema out from under the dashboard.
    sqlx::query!("DROP SCHEMA pgqueue CASCADE")
        .execute(db.queue.pool())
        .await
        .unwrap();

    let (status, body) = get_json(&router, "/api/queues").await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(body["error"], "internal server error");

    let (status, _) = get_json(&router, "/health").await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}

#[sqlx::test(migrations = "./migrations")]
async fn worker_hosts_authenticated_dashboard_and_stops_it_on_shutdown(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let dashboard = Dashboard::new([db.queue.clone()])
        .basic_auth("admin", "s3cret")
        .serve_on("127.0.0.1:0".parse().unwrap());
    let mut dashboard_handle = dashboard.handle();
    let worker = Worker::builder(db.queue.clone())
        .register(dashboard_probe)
        .dashboard(dashboard)
        .build()
        .unwrap();
    let worker_id = worker.id();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    let address = tokio::time::timeout(Duration::from_secs(5), dashboard_handle.wait_until_ready())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(dashboard_handle.local_addr(), Some(address));

    let response = http_get(address, "/api/queues", None).await;
    assert!(response.starts_with("HTTP/1.1 401"), "{response}");

    let response = http_get(address, "/api/queues", Some("Basic YWRtaW46czNjcmV0")).await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.contains("\"name\":\"default\""), "{response}");
    let response = http_get(
        address,
        "/api/queues/default/workers",
        Some("Basic YWRtaW46czNjcmV0"),
    )
    .await;
    assert!(response.contains(&worker_id.to_string()), "{response}");

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(tokio::net::TcpStream::connect(address).await.is_err());
}

#[sqlx::test(migrations = "./migrations")]
async fn dashboard_remains_available_while_worker_drains(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let drain = DashboardDrain {
        started: std::sync::Arc::new(tokio::sync::Notify::new()),
        release: std::sync::Arc::new(tokio::sync::Notify::new()),
    };
    let job = db
        .queue
        .enqueue(dashboard_slow::job(()))
        .await
        .unwrap()
        .unwrap();
    let dashboard = Dashboard::new([db.queue.clone()]).serve_on("127.0.0.1:0".parse().unwrap());
    let mut dashboard_handle = dashboard.handle();
    let worker = Worker::builder(db.queue.clone())
        .register(dashboard_slow)
        .state(drain.clone())
        .dashboard(dashboard)
        .shutdown_grace(Duration::from_secs(2))
        .build()
        .unwrap();
    let worker_id = worker.id();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(worker.run_until(shutdown.clone()));
    let address = tokio::time::timeout(Duration::from_secs(5), dashboard_handle.wait_until_ready())
        .await
        .unwrap()
        .unwrap();

    tokio::time::timeout(Duration::from_secs(5), drain.started.notified())
        .await
        .expect("job did not start");
    assert_eq!(job.refresh().await.unwrap().status, JobStatus::Running);

    shutdown.cancel();
    wait_for_worker_intake_closed(&db, worker_id).await;
    let response = http_get(address, "/health", None).await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");

    drain.release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn dashboard_bind_failure_prevents_worker_startup(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = occupied.local_addr().unwrap();
    let dashboard = Dashboard::new([db.queue.clone()]).serve_on(address);
    let mut dashboard_handle = dashboard.handle();
    let worker = Worker::builder(db.queue.clone())
        .register(dashboard_probe)
        .dashboard(dashboard)
        .build()
        .unwrap();

    let error = worker
        .run_until(CancellationToken::new())
        .await
        .unwrap_err();
    match error {
        Error::Dashboard(error) => assert_eq!(error.kind(), std::io::ErrorKind::AddrInUse),
        other => panic!("expected dashboard bind error, got {other}"),
    }
    assert_eq!(dashboard_handle.wait_until_ready().await, None);
}

#[sqlx::test(migrations = "./migrations")]
async fn worker_hosted_dashboard_rejects_custom_mount_path(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let dashboard = Dashboard::new([db.queue.clone()])
        .mount_path("/admin")
        .serve_on("127.0.0.1:0".parse().unwrap());
    let result = Worker::builder(db.queue.clone())
        .register(dashboard_probe)
        .dashboard(dashboard)
        .build();

    match result {
        Err(Error::Config(message)) => assert!(message.contains("requires mount_path `/`")),
        Err(other) => panic!("expected configuration error, got {other}"),
        Ok(_) => panic!("custom mount path should be rejected"),
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn burst_completion_stops_worker_hosted_dashboard(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let dashboard = Dashboard::new([db.queue.clone()]).serve_on("127.0.0.1:0".parse().unwrap());
    let mut dashboard_handle = dashboard.handle();
    let worker = Worker::builder(db.queue.clone())
        .register(dashboard_probe)
        .dashboard(dashboard)
        .burst(true)
        .dequeue_timeout(Duration::from_secs(1))
        .build()
        .unwrap();
    let run = tokio::spawn(worker.run_until(CancellationToken::new()));
    let address = tokio::time::timeout(Duration::from_secs(5), dashboard_handle.wait_until_ready())
        .await
        .unwrap()
        .unwrap();

    let response = http_get(address, "/health", None).await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(tokio::net::TcpStream::connect(address).await.is_err());
}
