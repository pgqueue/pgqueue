//! Dashboard router tests, driven with `tower::ServiceExt::oneshot` — no
//! listener needed.

use sqlx::PgPool;
use std::time::Duration;

use crate::wait_until;
use crate::{
    EnqueueResultTestExt, QueueProtocolTestExt, TestDb, new_job, wait_for_worker_intake_closed,
};
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use pgqueue::{
    CronMisfirePolicy, CronOptions, Dashboard, Error, JobRequest, JobState, JobStatus, Worker,
    WorkerHealthStatus, WorkerTimers,
};
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
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
async fn test_health_endpoint_reports_ok(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = Dashboard::new([db.queue.clone()]).router().unwrap();
    let (status, body) = get_json(&router, "/health").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, Value::String("OK".into()));
}

#[sqlx::test(migrations = "./migrations")]
async fn test_worker_hosted_health_reports_degraded_worker_components(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let key = "dashboard-health-revision";
    // Publish revision 1, then bring up a worker that reuses that revision for a
    // different schedule. The database rejects the definition, which is a real
    // deploy mistake and degrades `Scheduler` health. (A *superseded* revision
    // is not a failure — that is the normal state of a not-yet-upgraded worker.)
    let authority = Worker::builder(db.queue.clone())
        .schedule_cron_with_options(
            "0 0 1 1 *",
            dashboard_probe::job(()).dedupe_key(key),
            CronOptions {
                revision: 1,
                misfire: CronMisfirePolicy::default(),
            },
        )
        .timers(crate::test_timers())
        .build()
        .unwrap();
    let authority_shutdown = CancellationToken::new();
    let authority_run = tokio::spawn(authority.run_until(authority_shutdown.clone()));
    crate::wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        "authoritative cron revision was not stored",
        || async {
            sqlx::query_scalar::<_, i64>(
                "SELECT revision FROM pgqueue.cron_schedules WHERE queue = $1 AND dedupe_key = $2",
            )
            .bind(db.queue.name())
            .bind(key)
            .fetch_optional(&pool)
            .await
            .unwrap()
                == Some(1)
        },
    )
    .await;
    authority_shutdown.cancel();
    authority_run.await.unwrap().unwrap();

    let dashboard = Dashboard::new([db.queue.clone()]).serve_on("127.0.0.1", 0);
    let mut dashboard_handle = dashboard.server_handle();
    let lower = Worker::builder(db.queue.clone())
        .schedule_cron_with_options(
            "0 0 2 1 *",
            dashboard_probe::job(()).dedupe_key(key),
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

    // A degraded component remains ready while its queue is reachable, and the
    // degradation is still visible in the body and in `Worker::health`.
    let response = http_get(address, "/health", None).await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.contains("DEGRADED"), "{response}");

    // Degradation must not bypass the database half of the readiness check.
    // Let the successful probe age out, then make the shared queue pool fail
    // immediately without depending on an external Postgres outage.
    db.queue.pool().close().await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    let response = http_get(address, "/health", None).await;
    assert!(response.starts_with("HTTP/1.1 500"), "{response}");
    assert!(response.contains("unhealthy"), "{response}");
    shutdown.cancel();
    run.await.unwrap().unwrap();
}

#[sqlx::test(migrations = "./migrations")]
async fn test_queues_overview_lists_bounded_signals_and_workers_pages(pool: PgPool) {
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
async fn test_worker_pages_accept_non_object_stats(pool: PgPool) {
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
}

#[sqlx::test(migrations = "./migrations")]
async fn test_worker_pages_use_cursors_without_exact_totals(pool: PgPool) {
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
async fn test_queue_signals_report_ready_scheduled_execution_and_failure_states(pool: PgPool) {
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
            .abort_job(running.id, "dashboard signal test")
            .await
            .unwrap()
    );
    let (_, body) = get_json(&router, "/api/queues").await;
    assert_eq!(body["queues"][0]["execution"], "aborting");

    let (status, _) = get_json(&router, "/api/queues/default").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_jobs_listing_filters_by_status_and_name(pool: PgPool) {
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
async fn test_jobs_list_omits_bodies_while_detail_includes_them(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let id = db
        .queue
        .enqueue_raw(new_job("body-shape", |_| {}))
        .await
        .unwrap()
        .unwrap();
    let router = Dashboard::new([db.queue.clone()]).router().unwrap();

    let (status, body) = get_json(&router, "/api/queues/default/jobs").await;
    assert_eq!(status, StatusCode::OK);
    let summary = body["jobs"][0].as_object().unwrap();
    assert_eq!(summary["id"], id.to_string());
    for field in ["payload", "result", "error", "meta"] {
        assert!(
            !summary.contains_key(field),
            "list summary unexpectedly included {field}"
        );
    }

    let (status, body) = get_json(&router, &format!("/api/queues/default/jobs/{id}")).await;
    assert_eq!(status, StatusCode::OK);
    let detail = body["job"].as_object().unwrap();
    for field in ["payload", "result", "error", "meta"] {
        assert!(
            detail.contains_key(field),
            "job detail omitted required field {field}"
        );
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_separates_jobs_and_crons(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    db.queue
        .enqueue(dashboard_probe::job(()))
        .await
        .unwrap()
        .unwrap();
    let shutdown = CancellationToken::new();
    let worker = Worker::builder(db.queue.clone())
        .schedule_cron(
            "* * * * * *",
            dashboard_probe::job(()).dedupe_key("custom-dashboard-cron"),
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
    assert_eq!(cron["dedupe_key"], "custom-dashboard-cron");
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
async fn test_job_detail_retry_and_abort_actions(pool: PgPool) {
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
    let row = db.queue.fetch_job(id).await.unwrap().unwrap();
    assert_eq!(row.status, JobStatus::Aborted);
    assert_eq!(row.error.as_deref(), Some("aborted from dashboard"));

    // Retry it as a fresh occurrence, preserving the terminal row.
    let (status, body) = post_json(&router, &format!("/api/queues/default/jobs/{id}/retry")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["retried"], true);
    let retry_id: Uuid = body["job_id"].as_str().unwrap().parse().unwrap();
    assert_ne!(retry_id, id);
    assert_eq!(
        db.queue.fetch_job(id).await.unwrap().unwrap().status,
        JobStatus::Aborted
    );
    let row = db.queue.fetch_job(retry_id).await.unwrap().unwrap();
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
async fn test_dashboard_mutations_are_scoped_to_the_route_queue(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let other = db.another_queue(|builder| builder.name("other")).await;
    let id = db
        .queue
        .enqueue_raw(new_job("owned", |_| {}))
        .await
        .unwrap()
        .unwrap();
    let router = Dashboard::new([db.queue.clone(), other]).router().unwrap();

    let (status, body) = post_json(&router, &format!("/api/queues/other/jobs/{id}/abort")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["aborted"], false);
    assert_eq!(
        db.queue.fetch_job(id).await.unwrap().unwrap().status,
        JobStatus::Queued
    );

    let (_, body) = post_json(&router, &format!("/api/queues/default/jobs/{id}/abort")).await;
    assert_eq!(body["aborted"], true);
    let (status, body) = post_json(&router, &format!("/api/queues/other/jobs/{id}/retry")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["retried"], false);
    assert_eq!(
        db.queue.fetch_job(id).await.unwrap().unwrap().status,
        JobStatus::Aborted
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_retry_reruns_a_cron_occurrence_when_the_next_occurrence_is_live(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    // A failed cron occurrence...
    let failed = db
        .queue
        .enqueue_raw(new_job("tick", |_| {}))
        .await
        .unwrap()
        .unwrap();
    sqlx::query(
        "UPDATE pgqueue.jobs SET kind = 'cron', cron_expr = '* * * * *', \
         dedupe_key = 'cron:tick', status = 'failed', completed_at = now(), \
         error = 'failed: boom' WHERE id = $1",
    )
    .bind(failed)
    .execute(db.queue.pool())
    .await
    .unwrap();
    // ...while the schedule loop has already enqueued the next occurrence
    // under the same dedupe key.
    let next = db
        .queue
        .enqueue_raw(new_job("tick", |job| {
            job.dedupe_key = Some("cron:tick".into())
        }))
        .await
        .unwrap()
        .unwrap();
    sqlx::query("UPDATE pgqueue.jobs SET kind = 'cron', cron_expr = '* * * * *' WHERE id = $1")
        .bind(next)
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
    let rerun = db.queue.fetch_job(retry_id).await.unwrap().unwrap();
    assert_eq!(rerun.status, JobStatus::Queued);
    assert_eq!(rerun.dedupe_key, None);
    assert_eq!(
        db.queue.fetch_job(next).await.unwrap().unwrap().status,
        JobStatus::Queued,
        "the scheduled occurrence is untouched"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_basic_auth_gates_every_route(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let job_id = db
        .queue
        .enqueue_raw(new_job("protected", |_| {}))
        .await
        .unwrap()
        .unwrap();
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

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/queues/default/jobs/{job_id}/abort"))
                .header("x-pgqueue-request", "dashboard")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        db.queue.fetch_job(job_id).await.unwrap().unwrap().status,
        JobStatus::Queued,
        "unauthenticated mutation reached the queue"
    );

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/queues/default/jobs/{job_id}/abort"))
                .header(header::AUTHORIZATION, "Basic YWRtaW46czNjcmV0")
                .header("x-pgqueue-request", "dashboard")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        db.queue.fetch_job(job_id).await.unwrap().unwrap().status,
        JobStatus::Aborted
    );
}

/// `constant_time_eq(b"", b"")` is true, so an empty username or password
/// matched the credential every client can send: `Authorization: Basic
/// YWRtaW46` (`admin:`) was answered `200 OK` on every protected route. Nothing
/// on the wire distinguished such an instance from a correctly protected one —
/// it still 401s without credentials and still renders the login page — while
/// exposing every job payload plus Retry and Abort.
/// `basic_auth(user, env::var("PGQUEUE_DASHBOARD_PASSWORD").unwrap_or_default())`
/// is one unset environment variable away from it.
#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_refuses_basic_auth_with_an_empty_username_or_password(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    for (user, password) in [("admin", ""), ("", "s3cret"), ("", "")] {
        match Dashboard::new([db.queue.clone()])
            .basic_auth(user, password)
            .router()
        {
            Err(Error::Config(message)) => assert!(
                message.contains("basic_auth"),
                "{user:?}/{password:?}: {message}"
            ),
            Err(error) => panic!("{user:?}/{password:?}: unexpected error: {error}"),
            Ok(_) => panic!("{user:?}/{password:?}: empty credentials built a router"),
        }
        // The served path is the one an operator actually deploys, and it must
        // refuse the same way rather than binding a port first. The token is
        // pre-cancelled so a regression that builds the router cannot park the
        // suite on a running server.
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let error = Dashboard::new([db.queue.clone()])
            .basic_auth(user, password)
            .serve_on("127.0.0.1", 0)
            .run_until(shutdown)
            .await
            .unwrap_err();
        assert!(
            matches!(&error, Error::Config(message) if message.contains("basic_auth")),
            "{user:?}/{password:?}: unexpected error: {error}"
        );
    }

    // Credentials that are actually set still build, and so does no auth at all.
    let router = Dashboard::new([db.queue.clone()])
        .basic_auth("admin", "s3cret")
        .router()
        .unwrap();
    let (status, _) = request(&router, "GET", "/api/queues", Some("Basic YWRtaW46")).await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "an empty password must never match a configured one"
    );
    let unprotected = Dashboard::new([db.queue.clone()]).router().unwrap();
    let (status, _) = get_json(&unprotected, "/api/queues").await;
    assert_eq!(status, StatusCode::OK);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_health_endpoint_bypasses_dashboard_auth(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = Dashboard::new([db.queue.clone()])
        .basic_auth("admin", "s3cret")
        .router()
        .unwrap();

    let (status, body) = get_json(&router, "/health").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, Value::String("OK".into()));

    let (status, _) = get_json(&router, "/api/queues").await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_browser_auth_supports_password_changes_and_logout(pool: PgPool) {
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
    assert!(!body.as_str().unwrap().contains("value=\"admin\""));

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
    let other_cookie = login_cookie(&router).await;

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
    // The caller keeps a session, but not the *same* token: a password change
    // is how an admin evicts a leaked cookie, so the one token that survives is
    // re-minted and re-issued.
    let rotated_cookie = response.headers()[header::SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();
    assert_ne!(rotated_cookie, cookie);

    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/queues")
                .header(header::COOKIE, &rotated_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    for stale in [&cookie, &other_cookie] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/queues")
                    .header(header::COOKIE, stale)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{stale}");
    }

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
                .header(header::COOKIE, &rotated_cookie)
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
                .header(header::COOKIE, rotated_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

/// `HeaderMap::get` yields the first `Cookie` field line only. RFC 9113 §8.2.3
/// lets an HTTP/2 client split `cookie` across several field lines, and neither
/// `hyper` nor `h2` rejoins them — so a dashboard nested into an application
/// that serves h2 saw a browser's session cookie only when it happened to land
/// in the first line, looping login → home → login forever while
/// `remove_session` and `rotate_password` silently did nothing.
#[sqlx::test(migrations = "./migrations")]
async fn test_browser_auth_reads_a_session_cookie_from_any_cookie_header(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = Dashboard::new([db.queue.clone()])
        .basic_auth("admin", "s3cret")
        .router()
        .unwrap();
    let cookie = login_cookie(&router).await;

    let split_cookies = |request: axum::http::request::Builder| {
        request
            .header(header::COOKIE, "theme=dark")
            .header(header::COOKIE, &cookie)
    };
    let response = router
        .clone()
        .oneshot(
            split_cookies(Request::builder().uri("/api/queues"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // The action endpoints resolve the same token, so they were no-ops too.
    let response = router
        .clone()
        .oneshot(
            split_cookies(
                Request::builder()
                    .method("POST")
                    .uri("/api/account/logout")
                    .header("x-pgqueue-request", "dashboard"),
            )
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = router
        .oneshot(
            Request::builder()
                .uri("/api/queues")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "the logout found and removed the session"
    );
}

/// A password change is how an admin evicts a session token that may have
/// leaked — under `secure_cookies(false)` on plain HTTP it crossed the network
/// in cleartext. Keeping the caller's session *by key* and only re-stamping its
/// credential revision left exactly that token valid for the rest of its 12h
/// TTL, and issued no `Set-Cookie`. A caller authenticated by HTTP Basic has no
/// session to re-issue.
#[sqlx::test(migrations = "./migrations")]
async fn test_password_change_rotates_the_callers_session_token(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = Dashboard::new([db.queue.clone()])
        .basic_auth("admin", "s3cret")
        .router()
        .unwrap();
    let cookie = login_cookie(&router).await;

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
    let rotated = response.headers()[header::SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();
    assert_ne!(rotated, cookie, "the surviving token is a fresh one");

    for (stale, expected) in [
        (&cookie, StatusCode::UNAUTHORIZED),
        (&rotated, StatusCode::OK),
    ] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/queues")
                    .header(header::COOKIE, stale)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), expected, "{stale}");
    }

    // Basic auth carries no session, so there is nothing to re-issue.
    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/account/password")
                .header(header::AUTHORIZATION, "Basic YWRtaW46bmV3c2VjcmV0")
                .header("x-pgqueue-request", "dashboard")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"current_password":"newsecret","new_password":"thirdsecret"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(!response.headers().contains_key(header::SET_COOKIE));
}

#[sqlx::test(migrations = "./migrations")]
async fn test_browser_auth_can_opt_out_of_secure_cookies_for_direct_http(pool: PgPool) {
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
async fn test_authentication_failure_waits_before_rejecting_supplied_credentials(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = Dashboard::new([db.queue.clone()])
        .basic_auth("admin", "s3cret")
        .router()
        .unwrap();

    let started = tokio::time::Instant::now();
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/login")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from("username=admin&password=wrong"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(started.elapsed() >= Duration::from_millis(90));

    let started = tokio::time::Instant::now();
    let (status, _) = request(
        &router,
        "GET",
        "/api/queues",
        Some("Basic d3Jvbmc6Y3JlZHM="),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert!(started.elapsed() >= Duration::from_millis(90));
}

#[sqlx::test(migrations = "./migrations")]
async fn test_valid_basic_credentials_are_accepted_when_failed_comparison_is_in_flight(
    pool: PgPool,
) {
    let db = TestDb::new(pool.clone()).await;
    let router = Dashboard::new([db.queue.clone()])
        .basic_auth("admin", "s3cret")
        .router()
        .unwrap();

    let invalid = router.clone().oneshot(
        Request::builder()
            .uri("/api/queues")
            .header(header::AUTHORIZATION, "Basic d3Jvbmc6Y3JlZHM=")
            .body(Body::empty())
            .unwrap(),
    );
    let valid = router.clone().oneshot(
        Request::builder()
            .uri("/api/queues")
            .header(header::AUTHORIZATION, "Basic YWRtaW46czNjcmV0")
            .body(Body::empty())
            .unwrap(),
    );
    let (invalid, valid) = tokio::join!(
        biased;
        invalid,
        valid
    );

    assert_eq!(invalid.unwrap().status(), StatusCode::UNAUTHORIZED);
    let valid = valid.unwrap();
    assert_eq!(valid.status(), StatusCode::OK);

    let (status, _) = request(
        &router,
        "GET",
        "/api/queues",
        Some("Basic YWRtaW46czNjcmV0"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_authentication_attempts_are_refused_when_the_attempt_budget_is_spent(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = Dashboard::new([db.queue.clone()])
        .basic_auth("admin", "s3cret")
        .router()
        .unwrap();

    let invalid_request = || {
        let router = router.clone();
        async move {
            router
                .oneshot(
                    Request::builder()
                        .uri("/api/queues")
                        .header(header::AUTHORIZATION, "Basic d3Jvbmc6Y3JlZHM=")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap()
        }
    };

    // Enough guesses at once to outrun the budget however large the burst is:
    // each spends its attempt before it waits, so the ones past the burst find
    // nothing left to spend and are refused without being compared at all.
    let mut guesses = tokio::task::JoinSet::new();
    for _ in 0..64 {
        guesses.spawn(invalid_request());
    }
    let mut compared = 0;
    let mut refused = 0;
    while let Some(response) = guesses.join_next().await {
        let response = response.unwrap();
        match response.status() {
            StatusCode::UNAUTHORIZED => compared += 1,
            StatusCode::TOO_MANY_REQUESTS => {
                assert_eq!(response.headers()[header::RETRY_AFTER], "1");
                refused += 1;
            }
            other => panic!("unexpected status {other}"),
        }
    }
    assert!(compared > 0, "the burst must reach the comparison at all");
    // The bound has to be on the comparisons: every request is answered either
    // way, so asserting that the refusals make up the remainder is arithmetic
    // rather than a claim about the throttle. `MAX_COMPARED` is the burst (16)
    // with room for the handful of refills a burst this short can earn — an
    // unthrottled account would compare all 64.
    const MAX_COMPARED: usize = 32;
    assert!(
        compared <= MAX_COMPARED,
        "guessing must be bounded by the budget, not by how fast the guesses arrive: \
         {compared} compared, {refused} refused"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn test_password_change_throttles_wrong_current_password_and_accepts_correct(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = Dashboard::new([db.queue.clone()])
        .basic_auth("admin", "s3cret")
        .router()
        .unwrap();
    let cookie = login_cookie(&router).await;

    let started = tokio::time::Instant::now();
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
                    r#"{"current_password":"wrong","new_password":"newsecret"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert!(started.elapsed() >= Duration::from_millis(90));

    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/account/password")
                .header(header::COOKIE, cookie)
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
}

#[sqlx::test(migrations = "./migrations")]
async fn test_browser_auth_namespaces_session_cookies_per_dashboard(pool: PgPool) {
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
async fn test_browser_auth_scopes_session_cookie_to_mount_path(pool: PgPool) {
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
async fn test_spa_shell_and_static_assets_are_served(pool: PgPool) {
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
        let (status, _) = get_json(&router, path).await;
        assert_eq!(status, StatusCode::OK, "{path}");
    }

    for asset in ["app.js", "app.css", "pico.min.css"] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/static/{asset}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{asset}");
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL).unwrap(),
            "max-age=3600",
            "{asset}"
        );
    }

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

/// `/static/` is merged outside the `require_auth` layer so the stylesheet and
/// script the login page needs stay reachable, which makes everything it serves
/// public on an otherwise authenticated dashboard. It serves an allowlist, not
/// the embedded directory: the HTML templates belong to the shell and login
/// routes, and a file added to `pgqueue/assets/` must not become an endpoint on
/// its own.
#[sqlx::test(migrations = "./migrations")]
async fn test_static_route_serves_only_the_public_asset_allowlist(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = Dashboard::new([db.queue.clone()])
        .basic_auth("admin", "s3cret")
        .router()
        .unwrap();

    // Authentication really is on, so these are anonymous requests.
    for guarded in ["/api/queues", "/api/account/password", "/"] {
        let (status, _) = get_json(&router, guarded).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{guarded}");
    }

    for public in ["/static/app.js", "/static/app.css", "/static/pico.min.css"] {
        let (status, _) = get_json(&router, public).await;
        assert_eq!(status, StatusCode::OK, "{public}");
    }

    for private in ["/static/index.html", "/static/login.html"] {
        let (status, _) = get_json(&router, private).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{private}");
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn test_nested_static_assets_keep_their_cache_policy(pool: PgPool) {
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

/// The href the shipped script renders for `breadcrumb("/", ...)`.
///
/// The home view is served at the mount path itself, never at
/// `mount_path + "/"`: axum collapses the nested `"/"` route to exactly the
/// mount path, so `/admin/` is a URL the router does not answer. The assertions
/// below fetch that href and require a served page, so a script that stopped
/// agreeing fails them on the response rather than on a mirrored rule.
fn home_breadcrumb_href(root: &str) -> String {
    if root.is_empty() {
        "/".to_string()
    } else {
        root.to_string()
    }
}

/// Under a non-root `mount_path` the home breadcrumb — rendered on every queue,
/// worker, job and error page — was built as `ROOT + "/"`, i.e. `/admin/`. Axum
/// collapses the nested `"/"` route to exactly `/admin` and matchit has no
/// trailing-slash tolerance, so that href names a URL the router does not
/// serve: a refresh, a bookmark, a share, or the Cmd/Ctrl-click deliberately
/// handed to the browser all 404. `DashboardAuthState::home_path` already
/// answers `/admin`, which is why the post-login redirect worked; the script
/// now agrees with it.
#[sqlx::test(migrations = "./migrations")]
async fn test_nested_home_breadcrumb_points_at_a_url_the_router_serves(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = axum::Router::new().nest(
        "/admin",
        Dashboard::new([db.queue.clone()])
            .mount_path("/admin")
            .router()
            .unwrap(),
    );

    // The root the shell hands the script is the input to that rule.
    let (status, body) = get_json(&router, "/admin/queues/default").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body.as_str()
            .unwrap_or_default()
            .contains("name=\"pgqueue-root\" content=\"/admin\""),
        "the shell must publish the mount path the script builds hrefs from"
    );

    let home = home_breadcrumb_href("/admin");
    assert_eq!(home, "/admin", "the breadcrumb must not append a slash");
    let (status, _) = get_json(&router, &home).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the home breadcrumb must name a URL the nested router answers: {home}"
    );

    // And the URL the old rule produced really is unroutable, which is why the
    // script cannot simply concatenate.
    let (status, _) = get_json(&router, "/admin/").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_mount_path_rejects_protocol_relative_and_cookie_unsafe_values(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    for path in [
        "//attacker.example",
        "/admin; SameSite=None",
        "/admin\"><script>",
        "/admin?redirect=elsewhere",
        "/admin\\login",
        "/admin/../login",
    ] {
        match Dashboard::new([db.queue.clone()]).mount_path(path).router() {
            Err(Error::Config(message)) => {
                assert!(message.contains("mount_path"), "{path}: {message}");
            }
            Err(error) => panic!("{path}: unexpected error: {error}"),
            Ok(_) => panic!("{path}: unsafe mount path was accepted"),
        }
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_surfaces_worker_and_job_data_for_multiple_queues(pool: PgPool) {
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
async fn test_dashboard_deduplicates_repeated_queue_handles(pool: PgPool) {
    let first = TestDb::new(pool.clone()).await;
    let second = TestDb::new(pool.clone()).await;
    let router = Dashboard::new([first.queue.clone(), second.queue.clone()])
        .router()
        .unwrap();
    let (_, body) = get_json(&router, "/api/queues").await;
    assert_eq!(body["queues"].as_array().unwrap().len(), 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_broken_database_yields_500s(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = Dashboard::new([db.queue.clone()]).router().unwrap();
    // Nuke the schema out from under the dashboard.
    sqlx::query("DROP SCHEMA pgqueue CASCADE")
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
async fn test_standalone_dashboard_runs_until_cancelled(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let dashboard = Dashboard::new([db.queue.clone()])
        .basic_auth("admin", "s3cret")
        .serve_on("localhost", 0);
    let mut dashboard_handle = dashboard.server_handle();
    let shutdown = CancellationToken::new();
    let run = tokio::spawn(dashboard.run_until(shutdown.clone()));
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

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(tokio::net::TcpStream::connect(address).await.is_err());
}

#[sqlx::test(migrations = "./migrations")]
async fn test_standalone_dashboard_reports_bind_failure(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = occupied.local_addr().unwrap();
    let dashboard =
        Dashboard::new([db.queue.clone()]).serve_on(address.ip().to_string(), address.port());
    let mut dashboard_handle = dashboard.server_handle();

    let error = dashboard
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
async fn test_worker_hosts_authenticated_dashboard_and_stops_it_on_shutdown(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let dashboard = Dashboard::new([db.queue.clone()])
        .basic_auth("admin", "s3cret")
        .serve_on("127.0.0.1", 0);
    let mut dashboard_handle = dashboard.server_handle();
    let worker = Worker::builder(db.queue.clone())
        .register_job(dashboard_probe)
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
async fn test_dashboard_remains_available_while_worker_drains(pool: PgPool) {
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
    let dashboard = Dashboard::new([db.queue.clone()]).serve_on("127.0.0.1", 0);
    let mut dashboard_handle = dashboard.server_handle();
    let worker = Worker::builder(db.queue.clone())
        .register_job(dashboard_slow)
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
    assert_eq!(job.fetch_job().await.unwrap().status, JobStatus::Running);

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
async fn test_dashboard_bind_failure_prevents_worker_startup(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = occupied.local_addr().unwrap();
    let dashboard =
        Dashboard::new([db.queue.clone()]).serve_on(address.ip().to_string(), address.port());
    let mut dashboard_handle = dashboard.server_handle();
    let worker = Worker::builder(db.queue.clone())
        .register_job(dashboard_probe)
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
async fn test_dashboard_bind_is_skipped_when_shutdown_is_pre_cancelled(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = occupied.local_addr().unwrap();
    let dashboard =
        Dashboard::new([db.queue.clone()]).serve_on(address.ip().to_string(), address.port());
    let mut dashboard_handle = dashboard.server_handle();
    let worker = Worker::builder(db.queue.clone())
        .register_job(dashboard_probe)
        .dashboard(dashboard)
        .build()
        .unwrap();
    let shutdown = CancellationToken::new();
    shutdown.cancel();

    tokio::time::timeout(Duration::from_secs(1), worker.run_until(shutdown))
        .await
        .expect("pre-cancelled dashboard worker should stop promptly")
        .expect("pre-cancelled dashboard worker should stop cleanly");

    assert_eq!(dashboard_handle.local_addr(), None);
    assert_eq!(dashboard_handle.wait_until_ready().await, None);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_worker_hosted_dashboard_rejects_custom_mount_path(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let dashboard = Dashboard::new([db.queue.clone()])
        .mount_path("/admin")
        .serve_on("127.0.0.1", 0);
    let result = Worker::builder(db.queue.clone())
        .register_job(dashboard_probe)
        .dashboard(dashboard)
        .build();

    match result {
        Err(Error::Config(message)) => assert!(message.contains("requires mount_path `/`")),
        Err(other) => panic!("expected configuration error, got {other}"),
        Ok(_) => panic!("custom mount path should be rejected"),
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn test_burst_completion_stops_worker_hosted_dashboard(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let dashboard = Dashboard::new([db.queue.clone()]).serve_on("127.0.0.1", 0);
    let mut dashboard_handle = dashboard.server_handle();
    let worker = Worker::builder(db.queue.clone())
        .register_job(dashboard_probe)
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

#[sqlx::test(migrations = "./migrations")]
async fn test_job_name_suggestions_find_names_older_than_the_recency_sample(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    // One older job with a distinctive name...
    sqlx::query(
        "INSERT INTO pgqueue.jobs (queue, name, status, kind, enqueued_at)
         VALUES ('default', 'nightly_report', 'complete', 'job', now() - interval '1 day')",
    )
    .execute(&pool)
    .await
    .unwrap();
    // ...buried under more rows than the suggestion sample keeps.
    sqlx::query(
        "INSERT INTO pgqueue.jobs (queue, name, status, kind, enqueued_at)
         SELECT 'default', 'send_email', 'complete', 'job', now() - (g * interval '1 second')
         FROM generate_series(1, 1200) AS g",
    )
    .execute(&pool)
    .await
    .unwrap();

    let router = Dashboard::new([db.queue.clone()]).router().unwrap();
    let (status, body) = get_json(
        &router,
        "/api/queues/default/job-names?kind=job&prefix=night",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["names"],
        json!(["nightly_report"]),
        "the prefix must filter inside the sample, not after it"
    );

    let (_, body) = get_json(
        &router,
        "/api/queues/default/job-names?kind=job&prefix=send",
    )
    .await;
    assert_eq!(body["names"], json!(["send_email"]));
}

/// The typeahead answers the question the listing beside it asks. Ignoring the
/// status filter offered names that exist only under some other status, and
/// choosing one rendered "No jobs found" — the one outcome a typeahead exists
/// to make impossible.
#[sqlx::test(migrations = "./migrations")]
async fn test_job_name_suggestions_are_filtered_by_the_selected_statuses(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    sqlx::query(
        "INSERT INTO pgqueue.jobs (queue, name, status, kind) VALUES
             ('default', 'report_done', 'complete', 'job'),
             ('default', 'report_failed', 'failed', 'job')",
    )
    .execute(&pool)
    .await
    .unwrap();
    let router = Dashboard::new([db.queue.clone()]).router().unwrap();

    let (status, body) = get_json(
        &router,
        "/api/queues/default/job-names?kind=job&prefix=report&status=failed",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["names"],
        json!(["report_failed"]),
        "a suggestion must name a job the filtered listing can show"
    );

    let (_, body) = get_json(
        &router,
        "/api/queues/default/job-names?kind=job&prefix=report&status=complete,failed",
    )
    .await;
    assert_eq!(body["names"], json!(["report_done", "report_failed"]));

    // No status filter still means every status, as the listing does.
    let (_, body) = get_json(
        &router,
        "/api/queues/default/job-names?kind=job&prefix=report",
    )
    .await;
    assert_eq!(body["names"], json!(["report_done", "report_failed"]));

    let (status, _) = get_json(
        &router,
        "/api/queues/default/job-names?kind=job&prefix=report&status=bogus",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// `/health` is deliberately unauthenticated, so its request rate must not turn
/// into query rate on the pool the worker dequeues and finalizes with.
#[sqlx::test(migrations = "./migrations")]
async fn test_health_probes_are_cached_so_request_rate_does_not_become_database_load(pool: PgPool) {
    let single = crate::pool_with_max(&pool, 1).await;
    let db = TestDb::new(single.clone()).await;
    let router = Dashboard::new([db.queue.clone()]).router().unwrap();

    let (status, _) = get_json(&router, "/health").await;
    assert_eq!(status, StatusCode::OK);

    // Hold the only connection, exactly as a worker dequeue would. Further
    // requests must be served from the cached probe rather than queueing for it.
    let held = single.acquire().await.unwrap();
    for _ in 0..25 {
        let (status, _) = get_json(&router, "/health").await;
        assert_eq!(
            status,
            StatusCode::OK,
            "a flood of /health requests must not each need a pooled connection"
        );
    }
    drop(held);
}

/// How many backends are parked on a lock while running the `/health` probe.
///
/// Matched against the shipped statement itself rather than a copy of its text,
/// so tuning the probe's plan cannot silently turn this into a matcher that
/// counts nothing.
async fn blocked_health_probes(pool: &PgPool) -> i64 {
    sqlx::query_scalar::<_, i64>(
        "SELECT count(*)::bigint FROM pg_stat_activity
         WHERE datname = current_database()
           AND wait_event_type = 'Lock'
           AND query = $1",
    )
    .bind(pgqueue::__test_support::health_probe_sql())
    .fetch_one(pool)
    .await
    .unwrap()
}

/// `/health` is merged outside `require_auth`, so an anonymous client sets its
/// rate. The result cache was *read* before the probes and *written* only after
/// they returned, and nothing marked a round in flight: while a probe was slow
/// — lock contention, `max_connections` pressure, exactly when it matters —
/// every concurrent request raced past the not-yet-written cache and took a
/// pooled connection of its own, draining the pool the worker dequeues and
/// finalizes with. The TTL bounds the steady-state rate only while probes are
/// fast, which is precisely when the bound is not needed.
#[sqlx::test(migrations = "./migrations")]
async fn test_concurrent_health_requests_run_one_probe_when_the_probe_is_slow(pool: PgPool) {
    const REQUESTS: usize = 8;
    let db = TestDb::new(crate::pool_with_max(&pool, REQUESTS as u32).await).await;
    let router = Dashboard::new([db.queue.clone()]).router().unwrap();

    // Park every probe: `ACCESS EXCLUSIVE` blocks even a plain SELECT until
    // this transaction ends, standing in for any slow probe.
    let mut lock = pool.begin().await.unwrap();
    sqlx::raw_sql("LOCK TABLE pgqueue.jobs IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();

    // A cold cache, so nothing can be served from a previous round.
    let mut requests = tokio::task::JoinSet::new();
    for _ in 0..REQUESTS {
        let router = router.clone();
        requests.spawn(async move {
            router
                .oneshot(
                    Request::builder()
                        .uri("/health")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap()
                .status()
        });
    }

    wait_until(
        Duration::from_secs(5),
        Duration::from_millis(10),
        "no /health probe ever reached the database",
        || async { blocked_health_probes(&pool).await >= 1 },
    )
    .await;
    // Long enough for every one of the other requests to have opened a probe of
    // its own, had it been allowed to.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        blocked_health_probes(&pool).await,
        1,
        "{REQUESTS} concurrent /health requests must share one probe rather than \
         taking a pooled connection each"
    );

    lock.rollback().await.unwrap();
    while let Some(status) = requests.join_next().await {
        assert_eq!(status.unwrap(), StatusCode::OK);
    }
}

// ---------------------------------------------------------------------------
// The unauthenticated probe and the 5s signal poll must stay O(1) under the
// generic plan sqlx's prepared statements settle into
// ---------------------------------------------------------------------------

/// Retained history for a queue *other* than the one these plans are taken for,
/// and all of it `running`. One queue holding every row is what makes the
/// generic plan's average-rows-per-queue estimate for `queue = $1` wrong for a
/// queue that has none, and 500 rows is enough for an early-exit sequential scan
/// to out-cost an index on that estimate.
async fn seed_history_for_another_queue(pool: &PgPool) {
    sqlx::query(
        "INSERT INTO pgqueue.jobs (queue, name, status)
         SELECT 'plan-decoy', 'seed', 'running' FROM generate_series(1, 500)",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::raw_sql("ANALYZE pgqueue.jobs")
        .execute(pool)
        .await
        .unwrap();
}

/// The plan PostgreSQL runs `sql` under once it is prepared, which sqlx always
/// does. `force_generic_plan` is the sixth-and-later execution of any prepared
/// statement, reached deterministically: the generic plan is the one that costs
/// `queue = $1` against the table-wide average instead of this queue's real
/// count, and it is where an early-exit sequential scan looks cheap. The
/// argument is a literal because a generic plan is by definition independent of
/// it.
async fn generic_plan_for(pool: &PgPool, sql: &str) -> String {
    let mut connection = pool.acquire().await.unwrap();
    sqlx::raw_sql("SET plan_cache_mode = force_generic_plan")
        .execute(&mut *connection)
        .await
        .unwrap();
    // The only interpolation is this crate's own statement text.
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "PREPARE plan_under_test(text) AS {sql}"
    )))
    .execute(&mut *connection)
    .await
    .unwrap();
    sqlx::query_scalar::<_, String>("EXPLAIN (COSTS OFF) EXECUTE plan_under_test('plan-quiet')")
        .fetch_all(&mut *connection)
        .await
        .unwrap()
        .join("\n")
}

/// `/health` is deliberately unauthenticated, and `HEALTH_PROBE_TTL` plus
/// `health_gate` bound how *often* the probe runs, not what one run costs. As
/// `EXISTS (SELECT 1 ... WHERE queue = $1 LIMIT 1)` it cost a full sequential
/// scan of `pgqueue.jobs` — `EXISTS` strips any `ORDER BY`, so the planner had
/// no ordering to satisfy and costed an early exit against average-rows-per-
/// queue. Linear in retained history, which `JobRetention::Forever` never
/// bounds, on the pool the worker dequeues with.
#[sqlx::test(migrations = "./migrations")]
async fn test_health_probe_uses_an_index_when_history_belongs_to_another_queue(pool: PgPool) {
    seed_history_for_another_queue(&pool).await;
    let plan = generic_plan_for(&pool, pgqueue::__test_support::health_probe_sql()).await;
    assert!(
        !plan.contains("Seq Scan on jobs"),
        "the unauthenticated health probe must not read the whole jobs table: {plan}"
    );
    assert!(
        plan.contains("Index Only Scan using jobs_page_idx on jobs"),
        "the probe answers from the queue's own page index: {plan}"
    );
}

/// Every open dashboard polls this per queue every 5s. The `execution` signal
/// evaluated its `running` arm first and always, and that arm carried the same
/// trap: with `running` rows common table-wide but absent from *this* queue, the
/// generic plan scanned every retained row, while its `aborting` sibling used
/// `jobs_active_idx` correctly.
#[sqlx::test(migrations = "./migrations")]
async fn test_queue_signals_use_an_index_when_running_jobs_belong_to_another_queue(pool: PgPool) {
    seed_history_for_another_queue(&pool).await;
    let plan = generic_plan_for(&pool, pgqueue::__test_support::dashboard_signals_sql()).await;
    assert!(
        !plan.contains("Seq Scan on jobs"),
        "no signal may read the whole jobs table: {plan}"
    );
    assert!(
        plan.contains("Index Only Scan Backward using jobs_active_idx on jobs"),
        "the execution signal answers from one backward walk of the active index: {plan}"
    );
}

// ---------------------------------------------------------------------------
// A rotated session cookie must inherit the expiry it preserved server-side
// ---------------------------------------------------------------------------

/// The `Max-Age` seconds of a `Set-Cookie` header.
fn cookie_max_age(response: &axum::response::Response) -> u64 {
    response.headers()[axum::http::header::SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .map(str::trim)
        .find_map(|attribute| attribute.strip_prefix("Max-Age="))
        .expect("session cookie carries a Max-Age")
        .parse()
        .expect("Max-Age is a number of seconds")
}

/// `rotate_password` deliberately carries the *old* expiry onto the re-minted
/// session, "so a rotation neither logs the admin out nor extends their
/// session". The cookie half did not hold up its end: it was built with a
/// hard-coded full `SESSION_TTL`, so an admin who logged in at 09:00 and
/// changed their password at 20:55 was handed a cookie the browser kept until
/// ~08:55 the next day while the session itself died at 21:00 — a dead
/// credential persisted on disk almost a whole TTL longer than intended.
#[sqlx::test(migrations = "./migrations")]
async fn test_rotated_session_cookie_expires_when_the_replaced_session_would_have(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = Dashboard::new([db.queue.clone()])
        .basic_auth("admin", "s3cret")
        .router()
        .unwrap();

    let login = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/login")
                .header(
                    axum::http::header::CONTENT_TYPE,
                    "application/x-www-form-urlencoded",
                )
                .body(Body::from("username=admin&password=s3cret"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(login.status(), StatusCode::SEE_OTHER);
    let issued_max_age = cookie_max_age(&login);
    let cookie = login.headers()[axum::http::header::SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let rotation = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/account/password")
                .header(axum::http::header::COOKIE, &cookie)
                .header("x-pgqueue-request", "dashboard")
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"current_password":"s3cret","new_password":"newsecret"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(rotation.status(), StatusCode::OK);
    let rotated_max_age = cookie_max_age(&rotation);

    assert!(
        rotated_max_age < issued_max_age,
        "a rotation must not restart the cookie's lifetime: issued {issued_max_age}, \
         rotated {rotated_max_age}"
    );
    // ...and it inherits the surviving expiry rather than inventing a new one,
    // so the admin is not logged out either.
    assert!(
        rotated_max_age + 60 >= issued_max_age,
        "the rotated cookie must inherit the replaced session's expiry: issued \
         {issued_max_age}, rotated {rotated_max_age}"
    );
}

// ---------------------------------------------------------------------------
// SQL branches that no integration test reached
// ---------------------------------------------------------------------------

async fn dashboard_login(router: &axum::Router, password: &str) -> axum::response::Response {
    dashboard_login_from(router, None, password).await
}

/// The peer address a served request would carry. `axum::serve` records it as a
/// `ConnectInfo` extension, so setting it here is what a real connection from
/// that address looks like to the router.
fn dashboard_peer(host: u8) -> axum::extract::ConnectInfo<std::net::SocketAddr> {
    axum::extract::ConnectInfo(std::net::SocketAddr::from((
        [10, 0, 0, host],
        40_000 + u16::from(host),
    )))
}

async fn dashboard_login_from(
    router: &axum::Router,
    peer: Option<axum::extract::ConnectInfo<std::net::SocketAddr>>,
    password: &str,
) -> axum::response::Response {
    let mut request = Request::builder().method("POST").uri("/login").header(
        axum::http::header::CONTENT_TYPE,
        "application/x-www-form-urlencoded",
    );
    if let Some(peer) = peer {
        request = request.extension(peer);
    }
    router
        .clone()
        .oneshot(
            request
                .body(Body::from(format!("username=admin&password={password}")))
                .unwrap(),
        )
        .await
        .unwrap()
}

/// The same form post with the `Sec-Fetch-Site` a browser attaches on its own.
/// `dashboard_login_from` is the header-less shape a curl or a script sends.
async fn dashboard_login_from_site(
    router: &axum::Router,
    peer: Option<axum::extract::ConnectInfo<std::net::SocketAddr>>,
    site: &str,
    password: &str,
) -> axum::response::Response {
    let mut request = Request::builder()
        .method("POST")
        .uri("/login")
        .header(
            axum::http::header::CONTENT_TYPE,
            "application/x-www-form-urlencoded",
        )
        .header("sec-fetch-site", site);
    if let Some(peer) = peer {
        request = request.extension(peer);
    }
    router
        .clone()
        .oneshot(
            request
                .body(Body::from(format!("username=admin&password={password}")))
                .unwrap(),
        )
        .await
        .unwrap()
}

/// `POST /login` is the one state-changing route that cannot require the action
/// header — it is a real `<form method="post">`, so nothing of ours runs before
/// the browser sends it — and its `application/x-www-form-urlencoded` body is a
/// CORS-simple content type, so any page the operator visits can post it with no
/// preflight. Each post spent a comparison from the *victim's* interactive
/// budget, keyed to the victim's own address, before anything was compared: a
/// page they merely visited locked them out of their own dashboard, however
/// privately it is bound. Sequential posts do not do it — the failure delay
/// matches the refill rate — so the flood below is concurrent, which is what an
/// attacking page actually does.
#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_login_refuses_a_cross_site_post_before_spending_the_budget(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = Dashboard::new([db.queue.clone()])
        .basic_auth("admin", "s3cret")
        .router()
        .unwrap();

    let mut flood = tokio::task::JoinSet::new();
    for attempt in 0..64 {
        let router = router.clone();
        flood.spawn(async move {
            dashboard_login_from_site(
                &router,
                Some(dashboard_peer(1)),
                "cross-site",
                &format!("wrong-{attempt}"),
            )
            .await
            .status()
        });
    }
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }

    // The operator's own address — the one the flood is charged to, if it is
    // charged to anything — while the flood is still in flight, because a
    // budget spent and refilled by the time they arrive is no lockout.
    let login = dashboard_login_from(&router, Some(dashboard_peer(1)), "s3cret").await;
    assert_eq!(
        login.status(),
        StatusCode::SEE_OTHER,
        "a cross-site flood must not spend the operator's own login budget"
    );
    assert!(
        login
            .headers()
            .get(axum::http::header::SET_COOKIE)
            .is_some()
    );

    let mut refused = 0;
    while let Some(status) = flood.join_next().await {
        refused += usize::from(status.unwrap() == StatusCode::FORBIDDEN);
    }
    assert_eq!(refused, 64, "every cross-site post must be refused");
}

/// The guard is only ever a statement about a browser's own origin, so it must
/// not turn away the operator's real form post or a client that sends no
/// `Sec-Fetch-Site` at all. `none` is a typed URL or a bookmark; a missing
/// header is curl, a password manager, or a script — none of which a page can
/// cause. `same-site` is refused: the form is served by the dashboard itself,
/// so a genuine submission is always `same-origin`.
#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_login_accepts_every_post_a_browser_calls_its_own(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = Dashboard::new([db.queue.clone()])
        .basic_auth("admin", "s3cret")
        .router()
        .unwrap();

    for site in ["same-origin", "none"] {
        let login = dashboard_login_from_site(&router, None, site, "s3cret").await;
        assert_eq!(
            login.status(),
            StatusCode::SEE_OTHER,
            "a {site} login must be accepted"
        );
    }
    assert_eq!(
        dashboard_login(&router, "s3cret").await.status(),
        StatusCode::SEE_OTHER,
        "a client that sends no Sec-Fetch-Site must be accepted"
    );
    let refused = dashboard_login_from_site(&router, None, "same-site", "s3cret").await;
    assert_eq!(refused.status(), StatusCode::FORBIDDEN);
    assert!(
        String::from_utf8(
            axum::body::to_bytes(refused.into_body(), usize::MAX)
                .await
                .unwrap()
                .to_vec()
        )
        .unwrap()
        .contains("Cross-site login posts are refused."),
        "the refusal must say so on the login form"
    );
}

async fn dashboard_basic_guess(
    router: &axum::Router,
    peer: Option<axum::extract::ConnectInfo<std::net::SocketAddr>>,
) -> StatusCode {
    let mut request = Request::builder()
        .uri("/api/queues")
        .header(axum::http::header::AUTHORIZATION, "Basic d3Jvbmc6Y3JlZHM=");
    if let Some(peer) = peer {
        request = request.extension(peer);
    }
    router
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap()
        .status()
}

/// The session cookie is found by name, and an empty value is not a session.
/// Testing emptiness on the *result* of the scan let the first same-name cookie
/// end it: a planted `name=` — the shape a cleared cookie leaves behind, and
/// one anybody can set over cleartext HTTP under `secure_cookies(false)` — hid
/// the real session sitting behind it. `remove_session` then silently no-opped
/// and `rotate_password` issued no replacement cookie: a persistent lockout.
#[sqlx::test(migrations = "./migrations")]
async fn test_session_cookie_is_read_past_an_empty_cookie_of_the_same_name(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = Dashboard::new([db.queue.clone()])
        .basic_auth("admin", "s3cret")
        .router()
        .unwrap();

    let login = dashboard_login(&router, "s3cret").await;
    assert_eq!(login.status(), StatusCode::SEE_OTHER);
    let cookie = login.headers()[axum::http::header::SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();
    let (name, _) = cookie.split_once('=').expect("a name=value cookie");
    let empty = format!("{name}=");

    // One header carrying the decoy first, then the same pair split across two
    // `Cookie` field lines the way an HTTP/2 client may send them.
    for cookies in [
        vec![format!("{empty}; {cookie}")],
        vec![empty.clone(), cookie.clone()],
        vec![cookie.clone(), empty.clone()],
    ] {
        let mut request = Request::builder().uri("/api/queues");
        for line in &cookies {
            request = request.header(axum::http::header::COOKIE, line);
        }
        let response = router
            .clone()
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "the session must be found behind {cookies:?}"
        );
    }
}

/// Posts a password change as the browser does, authenticated by `cookie`.
async fn dashboard_change_password(
    router: &axum::Router,
    cookie: &str,
    current: &str,
    new: &str,
) -> axum::response::Response {
    router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/account/password")
                .header(axum::http::header::COOKIE, cookie)
                .header("x-pgqueue-request", "dashboard")
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({"current_password": current, "new_password": new})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap()
}

/// The minimum is stated in characters and was measured in `String::len()`,
/// which is UTF-8 bytes. `éééé` is four characters and eight bytes, so it was
/// accepted end to end — `200 {"changed": true}` — as an eight-character
/// password, and a three-character CJK one would have been too.
#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_password_minimum_counts_characters_not_bytes(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = Dashboard::new([db.queue.clone()])
        .basic_auth("admin", "s3cret")
        .router()
        .unwrap();
    let cookie = dashboard_login(&router, "s3cret").await.headers()[axum::http::header::SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let response = dashboard_change_password(&router, &cookie, "s3cret", "éééé").await;
    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "four characters is under the minimum, whatever they weigh in bytes"
    );

    // Not a refusal of non-ASCII, and proof the account is untouched: the same
    // alphabet at the stated length is accepted, on the current password the
    // refused change would have replaced.
    let response = dashboard_change_password(&router, &cookie, "s3cret", "ééééééée").await;
    assert_eq!(response.status(), StatusCode::OK);
}

/// The delay a rejection carries bounds the latency of one guess, never the
/// rate of guesses: every concurrent failure past the single in-flight permit
/// was refused instantly, and a *correct* password skipped the gate entirely.
/// 303-versus-429 was therefore an unthrottled oracle — measured at ~4,800
/// guesses a second. The budget is now spent before anything is compared, so a
/// saturated account answers a correct password exactly as it answers a wrong
/// one.
#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_refuses_a_correct_password_while_guesses_have_the_budget_spent(
    pool: PgPool,
) {
    let db = TestDb::new(pool.clone()).await;
    let router = Dashboard::new([db.queue.clone()])
        .basic_auth("admin", "s3cret")
        .router()
        .unwrap();

    let mut guesses = tokio::task::JoinSet::new();
    for attempt in 0..64 {
        let router = router.clone();
        guesses.spawn(async move {
            dashboard_login(&router, &format!("wrong-{attempt}"))
                .await
                .status()
        });
    }
    // Every guess spends its attempt before its first await, so a handful of
    // scheduler turns — microseconds, far short of a refill — leaves the state
    // a burst of concurrent guesses leaves behind. No database work happens on
    // either path, so nothing here waits on the pool.
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }

    let correct = dashboard_login(&router, "s3cret").await;
    assert_eq!(
        correct.status(),
        StatusCode::TOO_MANY_REQUESTS,
        "a spent budget must refuse the correct password too, or the refusal is an oracle"
    );
    assert!(
        correct
            .headers()
            .get(axum::http::header::SET_COOKIE)
            .is_none()
    );

    let mut refused = 0;
    while let Some(status) = guesses.join_next().await {
        refused += usize::from(status.unwrap() == StatusCode::TOO_MANY_REQUESTS);
    }
    assert!(
        refused > 0,
        "concurrent guessing must run out of budget rather than being compared as fast as it \
         arrives"
    );

    // It is a throttle, not a lockout: the budget refills and the operator gets
    // back in.
    let recovered = crate::wait_for_some(
        Duration::from_secs(10),
        Duration::from_millis(50),
        "the account never recovered after the guessing stopped",
        || async {
            let response = dashboard_login(&router, "s3cret").await;
            (response.status() == StatusCode::SEE_OTHER).then_some(())
        },
    );
    recovered.await;
}

/// One budget for the whole process is spent by whoever asks most. An attacker
/// never gets a refund — nothing they send matches — so a flood held the only
/// budget at zero and the operator's *correct* password was refused without
/// ever being read: measured at 5 logins in 100 against a moderate flood, and 0
/// in 100 against a saturating one, with no reset short of restarting the
/// process. Charging each client its own budget makes a flood cost the flooder.
#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_login_survives_a_guessing_flood_from_another_client(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = Dashboard::new([db.queue.clone()])
        .basic_auth("admin", "s3cret")
        .router()
        .unwrap();

    // Same endpoint, same channel, different client: only the address tells the
    // attacker's guesses apart from the operator's login.
    let mut flood = tokio::task::JoinSet::new();
    for attempt in 0..64 {
        let router = router.clone();
        flood.spawn(async move {
            dashboard_login_from(
                &router,
                Some(dashboard_peer(1)),
                &format!("wrong-{attempt}"),
            )
            .await
            .status()
        });
    }
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }

    let login = dashboard_login_from(&router, Some(dashboard_peer(2)), "s3cret").await;
    assert_eq!(
        login.status(),
        StatusCode::SEE_OTHER,
        "an operator elsewhere must still be able to sign in during a flood"
    );
    assert!(
        login
            .headers()
            .get(axum::http::header::SET_COOKIE)
            .is_some()
    );

    let mut refused = 0;
    while let Some(status) = flood.join_next().await {
        refused += usize::from(status.unwrap() == StatusCode::TOO_MANY_REQUESTS);
    }
    assert!(
        refused > 0,
        "the flooding client must have spent its own budget"
    );
}

/// Behind a reverse proxy, or in a router nested without connection info, every
/// request looks like the same client — so the budget is split by channel too.
/// An `Authorization` header is anybody's to send and needs no session, no form
/// and no CSRF header, which makes the API the flood surface; the login form is
/// the only way in for an operator holding no session. Neither budget is any
/// larger than the single one was, so this costs nothing in guessing resistance.
#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_login_survives_a_basic_auth_flood_from_an_indistinguishable_client(
    pool: PgPool,
) {
    let db = TestDb::new(pool.clone()).await;
    let router = Dashboard::new([db.queue.clone()])
        .basic_auth("admin", "s3cret")
        .router()
        .unwrap();

    let mut flood = tokio::task::JoinSet::new();
    for _ in 0..64 {
        let router = router.clone();
        flood.spawn(async move { dashboard_basic_guess(&router, None).await });
    }
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }

    let login = dashboard_login(&router, "s3cret").await;
    assert_eq!(
        login.status(),
        StatusCode::SEE_OTHER,
        "API guessing must not spend the login form's budget"
    );

    let mut refused = 0;
    while let Some(status) = flood.join_next().await {
        refused += usize::from(status.unwrap() == StatusCode::TOO_MANY_REQUESTS);
    }
    assert!(refused > 0, "the flood must have spent the API budget");
}

/// Sends one request per connection and returns the whole response text.
async fn http_once(address: std::net::SocketAddr, request: String) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(address)
        .await
        .unwrap_or_else(|error| panic!("connect to {address}: {error}"));
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    response
}

async fn http_login(address: std::net::SocketAddr, password: &str) -> String {
    let body = format!("username=admin&password={password}");
    http_once(
        address,
        format!(
            "POST /login HTTP/1.1\r\nHost: {address}\r\n\
             Content-Type: application/x-www-form-urlencoded\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        ),
    )
    .await
}

/// A per-client budget is only per-client if the server records who the client
/// is, and nothing about a router served without connection info looks any
/// different — the throttle simply charges every request to one bucket again.
/// So this drives the real [`pgqueue::DashboardServer`] over sockets, from two
/// peer addresses at once: the IPv4-mapped and IPv6 loopbacks of one dual-stack
/// listener. Both sides post the login form, so the channel split cannot cover
/// for a missing address and the flood locks the operator out without it.
///
/// Skipped where a dual-stack listener will not accept IPv4, since one process
/// cannot then be reached from two addresses at all.
#[sqlx::test(migrations = "./migrations")]
async fn test_served_dashboard_charges_a_guess_to_the_client_that_made_it(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let Some(()) = dual_stack_loopback_available().await else {
        eprintln!("skipping: this host has no dual-stack loopback");
        return;
    };

    let dashboard = Dashboard::new([db.queue.clone()])
        .basic_auth("admin", "s3cret")
        .secure_cookies(false)
        .serve_on("::", 0);
    let mut handle = dashboard.server_handle();
    let shutdown = CancellationToken::new();
    let server = tokio::spawn(dashboard.run_until(shutdown.clone()));
    let bound = tokio::time::timeout(Duration::from_secs(5), handle.wait_until_ready())
        .await
        .unwrap()
        .unwrap();
    let attacker = std::net::SocketAddr::from(([127, 0, 0, 1], bound.port()));
    let operator = std::net::SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 1], bound.port()));

    let flooding = CancellationToken::new();
    let refused = Arc::new(AtomicUsize::new(0));
    let mut flood = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let stop = flooding.clone();
        let refused = refused.clone();
        flood.spawn(async move {
            while !stop.is_cancelled() {
                let response = tokio::select! {
                    biased;
                    _ = stop.cancelled() => break,
                    response = http_login(attacker, "wrong") => response,
                };
                if response.starts_with("HTTP/1.1 429") {
                    refused.fetch_add(1, Ordering::SeqCst);
                }
            }
        });
    }
    wait_until(
        Duration::from_secs(10),
        Duration::from_millis(20),
        "the flooding client never ran out of budget",
        || async { refused.load(Ordering::SeqCst) > 0 },
    )
    .await;

    // The flood is still running throughout: the operator's budget is untouched
    // by it, so every one of these succeeds rather than winning a share of a
    // budget the flood keeps at zero.
    for attempt in 0..10 {
        let response = http_login(operator, "s3cret").await;
        assert!(
            response.starts_with("HTTP/1.1 303"),
            "attempt {attempt} was locked out by another client's flood: {response}"
        );
    }

    flooding.cancel();
    flood.join_all().await;
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

/// `Some(())` when a listener bound to `::` accepts an IPv4 loopback connection,
/// which is how one process is reached from two peer addresses.
async fn dual_stack_loopback_available() -> Option<()> {
    let listener = tokio::net::TcpListener::bind("[::]:0").await.ok()?;
    let port = listener.local_addr().ok()?.port();
    let connect =
        tokio::net::TcpStream::connect(std::net::SocketAddr::from(([127, 0, 0, 1], port)));
    tokio::time::timeout(Duration::from_secs(2), connect)
        .await
        .ok()?
        .ok()?;
    listener.accept().await.ok()?;
    Some(())
}

/// Every other outcome of the login form re-renders the page. The throttled one
/// returned the JSON API's body, so a browser posting the form during a throttle
/// window was shown `{"error":"too many authentication attempts"}` as a bare
/// document with no way back to the form.
#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_login_form_renders_the_login_page_when_the_budget_is_spent(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let router = Dashboard::new([db.queue.clone()])
        .basic_auth("admin", "s3cret")
        .router()
        .unwrap();

    let mut guesses = tokio::task::JoinSet::new();
    for attempt in 0..64 {
        let router = router.clone();
        guesses.spawn(async move { dashboard_login(&router, &format!("wrong-{attempt}")).await });
    }
    let mut throttled = None;
    while let Some(response) = guesses.join_next().await {
        let response = response.unwrap();
        if response.status() == StatusCode::TOO_MANY_REQUESTS {
            throttled = Some(response);
        }
    }
    let throttled = throttled.expect("concurrent guessing must exhaust the budget");

    assert_eq!(
        throttled.headers()[axum::http::header::RETRY_AFTER],
        "1",
        "a throttled form post still says when to come back"
    );
    let content_type = throttled.headers()[axum::http::header::CONTENT_TYPE]
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        content_type.starts_with("text/html"),
        "a form post must be answered with a page, not {content_type}"
    );
}

// ---------------------------------------------------------------------------
// A cancelled sweep must not strand the sweep-leadership advisory lock
// ---------------------------------------------------------------------------

/// Posts the login form as a request that reached the dashboard through a
/// proxy: the socket peer is the proxy, and `forwarded` is the chain it appended
/// to.
async fn dashboard_login_forwarded(
    router: &axum::Router,
    peer: axum::extract::ConnectInfo<std::net::SocketAddr>,
    forwarded: &str,
    password: &str,
) -> axum::response::Response {
    let request = Request::builder()
        .method("POST")
        .uri("/login")
        .header(
            axum::http::header::CONTENT_TYPE,
            "application/x-www-form-urlencoded",
        )
        .header("x-forwarded-for", forwarded)
        .extension(peer);
    router
        .clone()
        .oneshot(
            request
                .body(Body::from(format!("username=admin&password={password}")))
                .unwrap(),
        )
        .await
        .unwrap()
}

/// Starts a burst of wrong logins that all arrive through `peer`, each carrying
/// the chain `chain(attempt)`, and returns once they have spent what they can.
///
/// The guesses are still in flight: a budget refills a token every
/// `AUTH_ATTEMPT_REFILL`, and joining them would wait out the rejection delay
/// they each sleep — so what the caller is measuring has to be measured now.
async fn dashboard_forwarded_flood(
    router: &axum::Router,
    peer: axum::extract::ConnectInfo<std::net::SocketAddr>,
    chain: impl Fn(usize) -> String,
) -> tokio::task::JoinSet<StatusCode> {
    let mut flood = tokio::task::JoinSet::new();
    for attempt in 0..64 {
        let router = router.clone();
        let forwarded = chain(attempt);
        flood.spawn(async move {
            dashboard_login_forwarded(&router, peer, &forwarded, &format!("wrong-{attempt}"))
                .await
                .status()
        });
    }
    // Every guess spends its attempt before its first await, so a few scheduler
    // turns leave the state a concurrent burst leaves behind.
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    flood
}

/// How many of a flood's guesses were refused without being compared.
async fn dashboard_flood_refusals(mut flood: tokio::task::JoinSet<StatusCode>) -> usize {
    let mut refused = 0;
    while let Some(status) = flood.join_next().await {
        refused += usize::from(status.unwrap() == StatusCode::TOO_MANY_REQUESTS);
    }
    refused
}

/// `POST /login` is itself the interactive channel and needs no credentials to
/// reach, so splitting the budget by channel does nothing for the login form
/// when every request lands in one client bucket. Behind a TLS-terminating
/// proxy — the deployment the docs recommend, since `DashboardServer` has no TLS
/// of its own — every socket peer *is* the proxy, so a flood of wrong passwords
/// from anywhere on the internet kept the operator's own login refused
/// indefinitely. Trusting the proxy's `X-Forwarded-For` restores per-client
/// keying, and charges the flood to the flooder.
#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_login_survives_a_proxied_flood_when_a_trusted_proxy_hop_is_configured(
    pool: PgPool,
) {
    let db = TestDb::new(pool.clone()).await;
    // One proxy in front, so every request arrives from its address.
    let proxy = dashboard_peer(1);
    let attacker = "203.0.113.5";
    let operator = "198.51.100.9";

    // The default ignores the header, which behind a proxy is one bucket for
    // the whole internet: the operator is locked out by somebody else's flood.
    let shared = Dashboard::new([db.queue.clone()])
        .basic_auth("admin", "s3cret")
        .router()
        .unwrap();
    let flood = dashboard_forwarded_flood(&shared, proxy, |_| attacker.to_string()).await;
    assert_eq!(
        dashboard_login_forwarded(&shared, proxy, operator, "s3cret")
            .await
            .status(),
        StatusCode::TOO_MANY_REQUESTS,
        "keying by peer alone cannot tell two clients behind one proxy apart"
    );
    assert!(
        dashboard_flood_refusals(flood).await > 0,
        "the flood must have spent a budget"
    );

    // Trusting exactly the proxies that are there makes the flood cost the
    // flooder and nobody else.
    let proxied = Dashboard::new([db.queue.clone()])
        .basic_auth("admin", "s3cret")
        .trusted_proxy_hops(1)
        .router()
        .unwrap();
    let flood = dashboard_forwarded_flood(&proxied, proxy, |_| attacker.to_string()).await;
    let login = dashboard_login_forwarded(&proxied, proxy, operator, "s3cret").await;
    assert_eq!(
        login.status(),
        StatusCode::SEE_OTHER,
        "an operator behind the same proxy must still be able to sign in during a flood"
    );
    assert!(
        login
            .headers()
            .get(axum::http::header::SET_COOKIE)
            .is_some()
    );
    assert!(
        dashboard_flood_refusals(flood).await > 0,
        "the flooding client must still spend its own budget"
    );
}

/// Trusting a proxy must not hand the header to whoever sends it. The chain is
/// read from the right, so the entry charged is one the trusted proxies wrote
/// and a client forging entries only pushes its own address further along; a
/// chain too short to have crossed them all is not trusted at all and falls back
/// to the socket peer. Either way a flood cannot mint a fresh budget per
/// request, which is what honouring the leftmost entry would have cost.
#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_trusted_proxy_hops_ignore_forwarded_entries_a_client_forged(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    let proxy = dashboard_peer(1);

    let one_hop = Dashboard::new([db.queue.clone()])
        .basic_auth("admin", "s3cret")
        .trusted_proxy_hops(1)
        .router()
        .unwrap();
    // Every guess claims a different origin, and the proxy appends the one real
    // address behind them all.
    let flood = dashboard_forwarded_flood(&one_hop, proxy, |attempt| {
        format!("192.0.2.{}, 203.0.113.5", attempt % 256)
    })
    .await;
    assert_eq!(
        dashboard_login_forwarded(&one_hop, proxy, "198.51.100.9", "s3cret")
            .await
            .status(),
        StatusCode::SEE_OTHER,
        "the flood must stay charged to the client the proxy observed"
    );
    assert!(
        dashboard_flood_refusals(flood).await > 0,
        "a forged prefix must not mint a budget per request"
    );

    // Two proxies configured, one entry supplied: the chain never crossed them,
    // so it names nobody and the peer pays.
    let two_hops = Dashboard::new([db.queue.clone()])
        .basic_auth("admin", "s3cret")
        .trusted_proxy_hops(2)
        .router()
        .unwrap();
    let flood = dashboard_forwarded_flood(&two_hops, proxy, |attempt| {
        format!("192.0.2.{}", attempt % 256)
    })
    .await;
    assert_eq!(
        dashboard_login_forwarded(&two_hops, dashboard_peer(2), "192.0.2.1", "s3cret")
            .await
            .status(),
        StatusCode::SEE_OTHER,
        "a different peer must keep its own budget"
    );
    assert!(
        dashboard_flood_refusals(flood).await > 0,
        "a chain shorter than the trusted hops must fall back to the peer, not be believed"
    );
}

// ---------------------------------------------------------------------------
// A dedupe collision the guarded read could not see is still a collision
// ---------------------------------------------------------------------------

/// `%00` percent-decodes into the `String` like any other byte, so a NUL sailed
/// past the length guards on `?name=` and `?prefix=`, reached PostgreSQL — which
/// cannot hold one in `text` (`22021`) — and came back as `Internal`: a 500 and
/// an `ERROR`-level log for a request whose own contract promises a 400, having
/// burned a pooled connection to find out. `?status=` already 400d on the same
/// input, and so does every other entry point that writes a name (`JobRequest`,
/// `JobError`). This is on a router that is unauthenticated unless `basic_auth`
/// is configured.
#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_name_filters_reject_a_nul_byte_as_a_bad_request(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    db.queue
        .enqueue_raw(new_job("alpha", |_| {}))
        .await
        .unwrap();
    let router = Dashboard::new([db.queue.clone()]).router().unwrap();

    async fn get(router: &axum::Router, path: &str) -> (StatusCode, String) {
        let response = router
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    for (path, message) in [
        (
            "/api/queues/default/jobs?name=%00",
            "job name must not contain NUL",
        ),
        (
            "/api/queues/default/jobs?name=alp%00ha",
            "job name must not contain NUL",
        ),
        (
            "/api/queues/default/job-names?prefix=%00",
            "job name prefix must not contain NUL",
        ),
        (
            "/api/queues/default/job-names?prefix=alp%00ha",
            "job name prefix must not contain NUL",
        ),
    ] {
        let (status, body) = get(&router, path).await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "{path} must be refused as malformed, not answered with {body}"
        );
        assert!(
            body.contains(message),
            "{path} must say why it was refused: {body}"
        );
    }

    // And the same filters without the NUL still answer normally, so the guard
    // rejects the byte rather than the endpoint.
    for path in [
        "/api/queues/default/jobs?name=alpha",
        "/api/queues/default/job-names?prefix=alp",
    ] {
        let (status, body) = get(&router, path).await;
        assert_eq!(status, StatusCode::OK, "{path}: {body}");
        assert!(body.contains("alpha"), "{path} must still match: {body}");
    }
}

// ---------------------------------------------------------------------------
// A page cursor PostgreSQL cannot hold is a malformed request, not a 500
// ---------------------------------------------------------------------------

/// `cursor_pair` checked only that both halves of the cursor were present.
/// `DateTime<Utc>` reaches ISO year -262144 while `timestamptz` stops at
/// `4714-11-24 00:00:00 BC`, so every timestamp in between deserialized,
/// reached the query and came back as `22008` -> `Internal`: a 500 and an
/// `ERROR`-level log for a request this type promises to 400, having burned a
/// pooled connection to find out — the same class of defect as the `%00` name
/// filter above. Both paged endpoints funnel through that one helper, so both
/// were exposed, on a router that is unauthenticated unless `basic_auth` is
/// configured.
#[sqlx::test(migrations = "./migrations")]
async fn test_dashboard_page_cursors_reject_a_timestamp_postgres_cannot_hold(pool: PgPool) {
    let db = TestDb::new(pool.clone()).await;
    db.queue
        .enqueue_raw(new_job("alpha", |_| {}))
        .await
        .unwrap();
    let router = Dashboard::new([db.queue.clone()]).router().unwrap();
    let cursor_id = Uuid::now_v7();

    async fn get(router: &axum::Router, path: &str) -> (StatusCode, String) {
        let response = router
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    for (endpoint, key) in [
        ("/api/queues/default/jobs", "cursor_enqueued_at"),
        ("/api/queues/default/workers", "cursor_started_at"),
    ] {
        // One second under PostgreSQL's floor, and the far end of what
        // `DateTime<Utc>` can hold at all.
        for timestamp in ["-004713-11-23T23:59:59Z", "-262143-01-01T00:00:00Z"] {
            let path = format!("{endpoint}?{key}={timestamp}&cursor_id={cursor_id}");
            let (status, body) = get(&router, &path).await;
            assert_eq!(
                status,
                StatusCode::BAD_REQUEST,
                "{path} must be refused as malformed, not answered with {body}"
            );
            assert!(
                body.contains("page cursor timestamp is out of range"),
                "{path} must say why it was refused: {body}"
            );
        }

        // PostgreSQL's floor exactly still pages, so the guard rejects the
        // value rather than the endpoint.
        let path = format!("{endpoint}?{key}=-004713-11-24T00:00:00Z&cursor_id={cursor_id}");
        let (status, body) = get(&router, &path).await;
        assert_eq!(status, StatusCode::OK, "{path}: {body}");
    }
}
