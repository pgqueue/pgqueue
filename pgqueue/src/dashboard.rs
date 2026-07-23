//! The embedded web dashboard: an axum router serving a JSON API and a
//! no-build-step static frontend for managing queues and jobs.
//!
//! Run it inside a worker process:
//!
//! ```ignore
//! let dashboard = Dashboard::new([queue.clone()])
//!     .basic_auth("admin", "secret")
//!     .secure_cookies(false) // only for direct HTTP on a trusted network
//!     .serve_on("0.0.0.0:8080".parse()?);
//! Worker::builder(queue)
//!     .register(job)
//!     .dashboard(dashboard)
//!     .build()?
//!     .run()
//!     .await?;
//! ```
//!
//! Or mount its router in an existing axum application:
//!
//! ```ignore
//! app.nest(
//!     "/admin",
//!     Dashboard::new([queue]).mount_path("/admin").router()?,
//! );
//! ```
//!
//! The router is unauthenticated by default. Use [`Dashboard::basic_auth`] or
//! application middleware, and serve credentials only over TLS, before
//! exposing it outside a trusted network.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use axum::extract::{Form, Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use include_dir::{Dir, include_dir};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::Error;
use crate::database::Database;
use crate::job::{JobRetryBackoff, JobRow, JobStatus};
use crate::queue::Queue;
use crate::worker::{WorkerHealth, WorkerHealthStatus, WorkerInfo};

pub(crate) struct DashboardState {
    queues: Vec<Queue>,
    worker_health: Option<WorkerHealth>,
}

/// Configures the dashboard router. See the module docs.
pub struct Dashboard {
    queues: Vec<Queue>,
    auth: Option<(String, String)>,
    mount_path: String,
    secure_cookies: bool,
}

/// A complete worker-hosted dashboard configuration, created with
/// [`Dashboard::serve_on`].
///
/// Pass this value to [`crate::WorkerBuilder::dashboard`] to run the HTTP
/// server in the worker process. Use [`Dashboard::router`] instead when an
/// application already owns an axum server.
pub struct DashboardServer {
    dashboard: Dashboard,
    bind: SocketAddr,
    ready: tokio::sync::watch::Sender<Option<SocketAddr>>,
}

/// Observes a worker-hosted dashboard as it starts.
///
/// Obtain a handle with [`DashboardServer::handle`] before passing the server
/// to [`crate::WorkerBuilder::dashboard`]. This is especially useful with port
/// `0`, where the operating system chooses the listening port.
#[derive(Clone)]
pub struct DashboardServerHandle {
    ready: tokio::sync::watch::Receiver<Option<SocketAddr>>,
}

impl Dashboard {
    /// A dashboard over the given queues (one row per queue on the overview).
    pub fn new(queues: impl IntoIterator<Item = Queue>) -> Self {
        Self {
            queues: queues.into_iter().collect(),
            auth: None,
            mount_path: "/".to_string(),
            secure_cookies: true,
        }
    }

    /// Protects the dashboard with a browser login and HTTP Basic
    /// authentication for API clients. Password changes made in the dashboard
    /// last for the lifetime of the running dashboard process.
    pub fn basic_auth(mut self, user: impl Into<String>, password: impl Into<String>) -> Self {
        self.auth = Some((user.into(), password.into()));
        self
    }

    /// Controls the `Secure` attribute on browser session cookies. Defaults
    /// to `true`; disable it only for direct plain-HTTP access on a trusted
    /// network. TLS-terminated deployments should keep the secure default.
    ///
    /// ```no_run
    /// # fn dashboard(queue: pgqueue::Queue) -> Result<axum::Router, pgqueue::Error> {
    /// let router = pgqueue::Dashboard::new([queue])
    ///     .basic_auth("admin", "secret")
    ///     .secure_cookies(false)
    ///     .router()?;
    /// # Ok(router)
    /// # }
    /// ```
    pub fn secure_cookies(mut self, secure: bool) -> Self {
        self.secure_cookies = secure;
        self
    }

    /// The path prefix the router will be nested under (default `/`), so the
    /// frontend can locate its assets and API. Worker-hosted dashboards must
    /// keep the default and are served at `/`. A relative path is normalized
    /// to start with `/`.
    pub fn mount_path(mut self, path: impl Into<String>) -> Self {
        let path = path.into();
        self.mount_path = if path.starts_with('/') {
            path
        } else {
            format!("/{path}")
        };
        self
    }

    /// Converts this dashboard into a worker-hosted server bound to `address`.
    ///
    /// Worker-hosted dashboards are served at `/`; use [`Dashboard::router`]
    /// to mount the dashboard under a custom path in an existing application.
    ///
    /// ```no_run
    /// # fn dashboard(queue: pgqueue::Queue) -> anyhow::Result<pgqueue::DashboardServer> {
    /// let server = pgqueue::Dashboard::new([queue])
    ///     .basic_auth("admin", "secret")
    ///     .serve_on("127.0.0.1:8080".parse()?);
    /// # Ok(server)
    /// # }
    /// ```
    pub fn serve_on(self, address: SocketAddr) -> DashboardServer {
        let (ready, _) = tokio::sync::watch::channel(None);
        DashboardServer {
            dashboard: self,
            bind: address,
            ready,
        }
    }

    /// Builds the axum router: serve it standalone or `.nest(...)` it into an
    /// existing application. Duplicate queue names are shown once because names
    /// are the dashboard's URL identifiers.
    ///
    /// ```no_run
    /// # fn build(queue: pgqueue::Queue) -> Result<axum::Router, pgqueue::Error> {
    /// let router = pgqueue::Dashboard::new([queue]).router()?;
    /// # Ok(router)
    /// # }
    /// ```
    pub fn router(self) -> Result<Router, Error> {
        self.router_with_health(None)
    }

    fn router_with_health(self, worker_health: Option<WorkerHealth>) -> Result<Router, Error> {
        let mut queues: Vec<Queue> = Vec::new();
        for queue in self.queues {
            if queues
                .iter()
                .any(|existing| existing.name() == queue.name())
            {
                continue;
            }
            queues.push(queue);
        }
        let state = Arc::new(DashboardState {
            queues,
            worker_health,
        });

        let root = self.mount_path.trim_end_matches('/').to_string();
        let auth_enabled = self.auth.is_some();
        let username = self
            .auth
            .as_ref()
            .map(|(username, _)| username.as_str())
            .unwrap_or("anonymous");
        let index = render_index(&root, username, auth_enabled);
        let shell = get(move || {
            let index = index.clone();
            async move { Html(index) }
        });

        let protected = Router::new()
            .route("/", shell.clone())
            .route("/queues/{queue}", shell.clone())
            .route("/queues/{queue}/workers/{id}", shell.clone())
            .route("/queues/{queue}/jobs/{id}", shell)
            .route("/health", get(health))
            .route("/api/queues", get(list_queues))
            .route("/api/queues/{queue}/workers", get(list_workers))
            .route("/api/queues/{queue}/workers/{id}", get(worker_detail))
            .route("/api/queues/{queue}/jobs", get(list_jobs))
            .route("/api/queues/{queue}/job-names", get(list_job_names))
            .route("/api/queues/{queue}/jobs/{id}", get(job_detail))
            .route("/api/queues/{queue}/jobs/{id}/retry", post(retry_job))
            .route("/api/queues/{queue}/jobs/{id}/abort", post(abort_job))
            .with_state(state);

        let router = if let Some((user, password)) = self.auth {
            let auth = DashboardAuthState::new(user, password, root, self.secure_cookies);
            let protected = protected.merge(account_router(auth.clone())).layer(
                axum::middleware::from_fn_with_state(auth.clone(), require_auth),
            );
            dashboard_asset_router()
                .merge(login_router(auth))
                .merge(protected)
        } else {
            dashboard_asset_router().merge(protected)
        };
        Ok(router.layer(axum::middleware::from_fn(security_headers)))
    }
}

impl DashboardServer {
    /// Returns a handle that reports the actual address once the dashboard
    /// server task is running.
    ///
    /// ```no_run
    /// # async fn run(queue: pgqueue::Queue) -> anyhow::Result<()> {
    /// # #[pgqueue::job]
    /// # async fn cleanup(_: ()) {}
    /// let dashboard = pgqueue::Dashboard::new([queue.clone()])
    ///     .serve_on("127.0.0.1:0".parse()?);
    /// let mut handle = dashboard.handle();
    /// tokio::spawn(
    ///     pgqueue::Worker::builder(queue)
    ///         .register(cleanup)
    ///         .dashboard(dashboard)
    ///         .build()?
    ///         .run(),
    /// );
    /// let address = handle.wait_until_ready().await;
    /// assert!(address.is_some());
    /// assert_eq!(handle.local_addr(), address);
    /// # Ok(())
    /// # }
    /// ```
    pub fn handle(&self) -> DashboardServerHandle {
        DashboardServerHandle {
            ready: self.ready.subscribe(),
        }
    }

    pub(crate) fn into_worker_dashboard(
        self,
        worker_health: WorkerHealth,
    ) -> Result<DashboardWorkerConfig, Error> {
        if !self.dashboard.mount_path.trim_end_matches('/').is_empty() {
            return Err(Error::Config(
                "worker-hosted dashboard requires mount_path `/`; use Dashboard::router for a custom path"
                    .into(),
            ));
        }
        Ok(DashboardWorkerConfig {
            bind: self.bind,
            router: self.dashboard.router_with_health(Some(worker_health))?,
            ready: self.ready,
        })
    }
}

impl DashboardServerHandle {
    /// The actual listening address, once the dashboard task is ready.
    pub fn local_addr(&self) -> Option<SocketAddr> {
        *self.ready.borrow()
    }

    /// Waits for the dashboard task to start and returns its actual listening
    /// address, or `None` if the worker exits before the dashboard is ready.
    pub async fn wait_until_ready(&mut self) -> Option<SocketAddr> {
        loop {
            let address = *self.ready.borrow_and_update();
            if address.is_some() {
                return address;
            }
            if self.ready.changed().await.is_err() {
                return None;
            }
        }
    }
}

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) struct DashboardWorkerConfig {
    bind: SocketAddr,
    router: axum::Router,
    ready: tokio::sync::watch::Sender<Option<SocketAddr>>,
}

pub(crate) struct DashboardBoundServer {
    bind: SocketAddr,
    listener: tokio::net::TcpListener,
    router: axum::Router,
    ready: tokio::sync::watch::Sender<Option<SocketAddr>>,
}

pub(crate) struct DashboardRuntime {
    bind: SocketAddr,
    shutdown: CancellationToken,
    task: Option<JoinHandle<std::io::Result<()>>>,
}

pub(crate) async fn bind_dashboard(
    dashboard: Option<&DashboardWorkerConfig>,
) -> Result<Option<DashboardBoundServer>, Error> {
    let Some(dashboard) = dashboard else {
        return Ok(None);
    };
    let listener = tokio::net::TcpListener::bind(dashboard.bind)
        .await
        .map_err(Error::Dashboard)?;
    let bind = listener.local_addr().map_err(Error::Dashboard)?;
    tracing::info!(dashboard.addr = %bind, configured.addr = %dashboard.bind, "dashboard bound");
    Ok(Some(DashboardBoundServer {
        bind,
        listener,
        router: dashboard.router.clone(),
        ready: dashboard.ready.clone(),
    }))
}

impl DashboardRuntime {
    pub(crate) fn start(bound: DashboardBoundServer) -> Self {
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let bind = bound.bind;
        let task = tokio::spawn(async move {
            bound.ready.send_replace(Some(bind));
            tracing::info!(dashboard.addr = %bind, "dashboard ready");
            axum::serve(bound.listener, bound.router)
                .with_graceful_shutdown(server_shutdown.cancelled_owned())
                .await
        });
        Self {
            bind,
            shutdown,
            task: Some(task),
        }
    }

    fn begin_shutdown(&self) {
        if !self.shutdown.is_cancelled() {
            tracing::info!(dashboard.addr = %self.bind, "dashboard shutting down");
            self.shutdown.cancel();
        }
    }

    async fn wait(&mut self) -> Result<(), Error> {
        let result = match self.task.as_mut() {
            Some(task) => task.await,
            None => return Ok(()),
        };
        self.task = None;
        dashboard_task_result(result)
    }

    async fn unexpected_exit(&mut self) -> Error {
        match self.wait().await {
            Ok(()) => Error::Dashboard(std::io::Error::other(
                "dashboard server stopped unexpectedly",
            )),
            Err(error) => error,
        }
    }

    pub(crate) async fn finish_shutdown(&mut self) -> Result<(), Error> {
        self.begin_shutdown();
        if self.task.is_none() {
            return Ok(());
        }

        let result = match self.task.as_mut() {
            Some(task) => tokio::time::timeout(SHUTDOWN_TIMEOUT, task).await,
            None => return Ok(()),
        };
        match result {
            Ok(result) => {
                self.task = None;
                dashboard_task_result(result)
            }
            Err(_) => {
                tracing::warn!(
                    dashboard.addr = %self.bind,
                    timeout = ?SHUTDOWN_TIMEOUT,
                    "dashboard graceful shutdown timed out; aborting server task"
                );
                if let Some(task) = self.task.take() {
                    task.abort();
                    let _ = task.await;
                }
                Ok(())
            }
        }
    }
}

pub(crate) async fn wait_for_dashboard_exit(dashboard: &mut Option<DashboardRuntime>) -> Error {
    match dashboard {
        Some(dashboard) => dashboard.unexpected_exit().await,
        None => std::future::pending().await,
    }
}

fn dashboard_task_result(
    result: Result<std::io::Result<()>, tokio::task::JoinError>,
) -> Result<(), Error> {
    match result {
        Ok(Ok(())) => Ok(()),
        // Axum 0.8 handles accept errors internally, but retain the typed
        // mapping in case a future server implementation returns one.
        Ok(Err(error)) => Err(Error::Dashboard(error)),
        Err(error) => Err(Error::Dashboard(std::io::Error::other(error))),
    }
}

// Dashboard API

const MAX_PAGE_SIZE: i64 = 100;
const JOB_NAME_SAMPLE_SIZE: i64 = 1_000;
const JOB_NAME_SUGGESTION_LIMIT: i64 = 20;
const ALL_STATUSES: [JobStatus; 6] = [
    JobStatus::Queued,
    JobStatus::Running,
    JobStatus::Complete,
    JobStatus::Failed,
    JobStatus::Aborting,
    JobStatus::Aborted,
];

/// API failure: infrastructure errors become 500s, malformed requests 400s,
/// lookups 404s, and rejected state-changing requests 403s.
pub(crate) enum DashboardApiError {
    BadRequest(&'static str),
    NotFound(&'static str),
    Forbidden(&'static str),
    Internal(Error),
}

impl From<Error> for DashboardApiError {
    fn from(error: Error) -> Self {
        match error {
            Error::JobNotFound(_) => DashboardApiError::NotFound("job not found"),
            other => DashboardApiError::Internal(other),
        }
    }
}

impl IntoResponse for DashboardApiError {
    fn into_response(self) -> Response {
        match self {
            DashboardApiError::BadRequest(what) => {
                (StatusCode::BAD_REQUEST, Json(json!({ "error": what }))).into_response()
            }
            DashboardApiError::NotFound(what) => {
                (StatusCode::NOT_FOUND, Json(json!({ "error": what }))).into_response()
            }
            DashboardApiError::Forbidden(what) => {
                (StatusCode::FORBIDDEN, Json(json!({ "error": what }))).into_response()
            }
            DashboardApiError::Internal(error) => {
                tracing::error!(%error, "dashboard api error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": "internal server error" })),
                )
                    .into_response()
            }
        }
    }
}

fn require_action_header(headers: &HeaderMap) -> Result<(), DashboardApiError> {
    if headers
        .get(ACTION_HEADER)
        .is_some_and(|value| value.as_bytes() == ACTION_HEADER_VALUE)
    {
        Ok(())
    } else {
        Err(DashboardApiError::Forbidden(
            "missing dashboard action header",
        ))
    }
}

fn queue_of(state: &DashboardState, name: &str) -> Result<Queue, DashboardApiError> {
    state
        .queues
        .iter()
        .find(|queue| queue.name() == name)
        .cloned()
        .ok_or(DashboardApiError::NotFound("queue not found"))
}

pub(crate) async fn health(State(state): State<Arc<DashboardState>>) -> Response {
    if state
        .worker_health
        .as_ref()
        .is_some_and(|health| health.snapshot().status != WorkerHealthStatus::Ready)
    {
        return (StatusCode::SERVICE_UNAVAILABLE, "unhealthy").into_response();
    }
    let mut probes = tokio::task::JoinSet::new();
    for queue in &state.queues {
        let queue = queue.clone();
        probes.spawn(async move { queue.database().dashboard_probe().await });
    }
    while let Some(result) = probes.join_next().await {
        if !matches!(result, Ok(Ok(()))) {
            return (StatusCode::INTERNAL_SERVER_ERROR, "unhealthy").into_response();
        }
    }
    (StatusCode::OK, "OK").into_response()
}

pub(crate) async fn list_queues(
    State(state): State<Arc<DashboardState>>,
) -> Result<Response, DashboardApiError> {
    let mut tasks = tokio::task::JoinSet::new();
    for (index, queue) in state.queues.iter().enumerate() {
        let queue = queue.clone();
        tasks.spawn(async move { (index, queue.database().dashboard_signals().await) });
    }
    let mut queues = Vec::new();
    queues.resize_with(tasks.len(), || None);
    while let Some(result) = tasks.join_next().await {
        let (index, queue) = result.map_err(Error::from)?;
        queues[index] = Some(queue?);
    }
    let queues: Vec<_> = queues.into_iter().flatten().collect();
    Ok(Json(json!({ "queues": queues })).into_response())
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DashboardJobsQuery {
    status: Option<String>,
    name: Option<String>,
    kind: Option<String>,
    limit: Option<i64>,
    cursor_enqueued_at: Option<DateTime<Utc>>,
    cursor_id: Option<Uuid>,
}

struct DashboardFilteredJobsQuery {
    statuses: Vec<JobStatus>,
    name: Option<String>,
    kind: String,
    limit: i64,
    cursor: Option<(DateTime<Utc>, Uuid)>,
}

impl DashboardJobsQuery {
    fn filter(self) -> Result<DashboardFilteredJobsQuery, DashboardApiError> {
        let mut statuses = Vec::new();
        if let Some(value) = self.status {
            for value in value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                let status = value
                    .parse::<JobStatus>()
                    .map_err(|_| DashboardApiError::BadRequest("unknown status"))?;
                if !statuses.contains(&status) {
                    statuses.push(status);
                }
            }
        }
        if statuses.is_empty() {
            statuses.extend(ALL_STATUSES);
        }
        let kind = job_kind(self.kind.as_deref())?.to_string();
        let cursor = cursor_pair(self.cursor_enqueued_at, self.cursor_id)?;
        let name = self.name.filter(|name| !name.is_empty());
        if name.as_ref().is_some_and(|name| name.len() > 255) {
            return Err(DashboardApiError::BadRequest("job name is too long"));
        }
        Ok(DashboardFilteredJobsQuery {
            statuses,
            name,
            kind,
            limit: page_limit(self.limit),
            cursor,
        })
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DashboardWorkersQuery {
    limit: Option<i64>,
    cursor_started_at: Option<DateTime<Utc>>,
    cursor_id: Option<Uuid>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DashboardJobNamesQuery {
    kind: Option<String>,
    prefix: Option<String>,
}

fn cursor_pair(
    timestamp: Option<DateTime<Utc>>,
    id: Option<Uuid>,
) -> Result<Option<(DateTime<Utc>, Uuid)>, DashboardApiError> {
    match (timestamp, id) {
        (None, None) => Ok(None),
        (Some(timestamp), Some(id)) => Ok(Some((timestamp, id))),
        _ => Err(DashboardApiError::BadRequest("incomplete page cursor")),
    }
}

fn page_limit(limit: Option<i64>) -> i64 {
    limit.unwrap_or(50).clamp(1, MAX_PAGE_SIZE)
}

fn job_kind(kind: Option<&str>) -> Result<&str, DashboardApiError> {
    match kind {
        None | Some("") => Ok("job"),
        Some(kind @ ("job" | "cron")) => Ok(kind),
        Some(_) => Err(DashboardApiError::BadRequest("unknown job kind")),
    }
}

/// Shared cursor-pagination epilogue: the page was fetched with `limit + 1`,
/// so trim the probe row and, when it existed, project the last visible item
/// into the response's `next_cursor`.
fn next_cursor<T>(
    items: &mut Vec<T>,
    limit: i64,
    cursor: impl Fn(&T) -> serde_json::Value,
) -> Option<serde_json::Value> {
    let Ok(limit) = usize::try_from(limit) else {
        return None;
    };
    if items.len() <= limit {
        return None;
    }
    items.pop();
    items.last().map(cursor)
}

pub(crate) async fn list_jobs(
    State(state): State<Arc<DashboardState>>,
    Path(name): Path<String>,
    Query(query): Query<DashboardJobsQuery>,
) -> Result<Response, DashboardApiError> {
    let queue = queue_of(&state, &name)?;
    let DashboardFilteredJobsQuery {
        statuses,
        name,
        kind,
        limit,
        cursor,
    } = query.filter()?;
    let mut jobs = queue
        .database()
        .dashboard_jobs_page(&statuses, &kind, name.as_deref(), cursor, limit + 1)
        .await?;
    let next_cursor = next_cursor(&mut jobs, limit, |job| {
        json!({
            "enqueued_at": job.job.enqueued_at,
            "id": job.job.id,
        })
    });
    Ok(Json(json!({
        "jobs": jobs,
        "next_cursor": next_cursor,
    }))
    .into_response())
}

pub(crate) async fn list_workers(
    State(state): State<Arc<DashboardState>>,
    Path(name): Path<String>,
    Query(query): Query<DashboardWorkersQuery>,
) -> Result<Response, DashboardApiError> {
    let queue = queue_of(&state, &name)?;
    let limit = page_limit(query.limit);
    let cursor = cursor_pair(query.cursor_started_at, query.cursor_id)?;
    let mut workers = queue
        .database()
        .dashboard_workers_page(cursor, limit + 1)
        .await?;
    let next_cursor = next_cursor(&mut workers, limit, |worker| {
        json!({
            "started_at": worker.started_at,
            "id": worker.id,
        })
    });
    Ok(Json(json!({
        "workers": workers,
        "next_cursor": next_cursor,
    }))
    .into_response())
}

pub(crate) async fn list_job_names(
    State(state): State<Arc<DashboardState>>,
    Path(name): Path<String>,
    Query(query): Query<DashboardJobNamesQuery>,
) -> Result<Response, DashboardApiError> {
    let queue = queue_of(&state, &name)?;
    let kind = job_kind(query.kind.as_deref())?;
    let prefix = query.prefix.unwrap_or_default();
    if prefix.len() > 255 {
        return Err(DashboardApiError::BadRequest("job name prefix is too long"));
    }
    let names = if prefix.is_empty() {
        Vec::new()
    } else {
        queue
            .database()
            .dashboard_job_names(
                &ALL_STATUSES,
                kind,
                &prefix,
                JOB_NAME_SAMPLE_SIZE,
                JOB_NAME_SUGGESTION_LIMIT,
            )
            .await?
    };
    Ok(Json(json!({ "names": names })).into_response())
}

pub(crate) async fn worker_detail(
    State(state): State<Arc<DashboardState>>,
    Path((name, id)): Path<(String, Uuid)>,
) -> Result<Response, DashboardApiError> {
    let queue = queue_of(&state, &name)?;
    let worker = queue
        .database()
        .dashboard_worker(id)
        .await?
        .ok_or(DashboardApiError::NotFound("worker not found"))?;
    Ok(Json(json!({ "worker": worker })).into_response())
}

pub(crate) async fn job_detail(
    State(state): State<Arc<DashboardState>>,
    Path((name, id)): Path<(String, Uuid)>,
) -> Result<Response, DashboardApiError> {
    let queue = queue_of(&state, &name)?;
    let job = queue
        .database()
        .dashboard_job(id)
        .await?
        .ok_or(DashboardApiError::NotFound("job not found"))?;
    let cron_description = job
        .cron_expr
        .as_deref()
        .and_then(|expression| crate::job::parse_cron(expression).ok())
        .map(|cron| cron.describe());
    Ok(Json(json!({
        "job": job,
        "cron_description": cron_description,
    }))
    .into_response())
}

pub(crate) async fn retry_job(
    State(state): State<Arc<DashboardState>>,
    Path((name, id)): Path<(String, Uuid)>,
    headers: HeaderMap,
) -> Result<Response, DashboardApiError> {
    require_action_header(&headers)?;
    let queue = queue_of(&state, &name)?;
    let job_id = queue
        .retry_job_occurrence(id, "retried from dashboard")
        .await?;
    Ok(Json(json!({ "retried": job_id.is_some(), "job_id": job_id })).into_response())
}

pub(crate) async fn abort_job(
    State(state): State<Arc<DashboardState>>,
    Path((name, id)): Path<(String, Uuid)>,
    headers: HeaderMap,
) -> Result<Response, DashboardApiError> {
    require_action_header(&headers)?;
    let queue = queue_of(&state, &name)?;
    let aborted = queue.abort(id, "aborted from dashboard").await?;
    Ok(Json(json!({ "aborted": aborted })).into_response())
}

#[cfg(test)]
mod dashboard_api_tests {
    use super::*;

    #[test]
    fn jobs_query_clamps_page_size() {
        let query = DashboardJobsQuery {
            status: None,
            name: None,
            kind: None,
            limit: Some(MAX_PAGE_SIZE + 1),
            cursor_enqueued_at: None,
            cursor_id: None,
        };
        let Ok(filter) = query.filter() else {
            panic!("valid jobs query should produce a filter");
        };
        assert_eq!(filter.limit, MAX_PAGE_SIZE);
        assert_eq!(filter.statuses, ALL_STATUSES);
        assert_eq!(filter.kind, "job");

        let query = DashboardJobsQuery {
            status: None,
            name: None,
            kind: None,
            limit: Some(0),
            cursor_enqueued_at: None,
            cursor_id: None,
        };
        let Ok(filter) = query.filter() else {
            panic!("valid jobs query should produce a filter");
        };
        assert_eq!(filter.limit, 1);
        assert_eq!(page_limit(Some(MAX_PAGE_SIZE + 1)), MAX_PAGE_SIZE);
        assert_eq!(page_limit(Some(0)), 1);
    }

    #[test]
    fn cursor_requires_timestamp_and_id() {
        let error = cursor_pair(Some(Utc::now()), None);
        assert!(matches!(error, Err(DashboardApiError::BadRequest(_))));
        let error = cursor_pair(None, Some(Uuid::now_v7()));
        assert!(matches!(error, Err(DashboardApiError::BadRequest(_))));
    }
}

// Embedded assets

static ASSETS: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/assets");

fn render_index(root: &str, username: &str, auth_enabled: bool) -> axum::body::Bytes {
    let asset_version = application_asset_version();
    let root = html_attr_escape(root);
    let username = html_attr_escape(username);
    axum::body::Bytes::from(render_template(
        ASSETS
            .get_file("index.html")
            .and_then(|file| file.contents_utf8())
            .unwrap_or_default(),
        &[
            ("root", root.as_str()),
            ("username", username.as_str()),
            ("auth_enabled", if auth_enabled { "true" } else { "false" }),
            ("asset_version", asset_version.as_str()),
        ],
    ))
}

/// Substitutes `{name}` placeholders in one pass, so a substituted value
/// that itself contains a placeholder literal (a username of
/// `"{asset_version}"`, say) is never substituted again.
fn render_template(template: &str, values: &[(&str, &str)]) -> String {
    let mut rendered = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(start) = rest.find('{') {
        rendered.push_str(&rest[..start]);
        rest = &rest[start..];
        let placeholder = values.iter().find(|(name, _)| {
            rest.as_bytes().get(name.len() + 1) == Some(&b'}') && rest[1..].starts_with(name)
        });
        match placeholder {
            Some((name, value)) => {
                rendered.push_str(value);
                rest = &rest[name.len() + 2..];
            }
            None => {
                rendered.push('{');
                rest = &rest[1..];
            }
        }
    }
    rendered.push_str(rest);
    rendered
}

fn application_asset_version() -> String {
    asset_fingerprint(
        ["app.css", "app.js"]
            .into_iter()
            .filter_map(|path| ASSETS.get_file(path))
            .flat_map(|file| file.contents().iter().copied()),
    )
}

fn render_login(root: &str, username: &str, error: &str) -> String {
    let root = html_attr_escape(root);
    let username = html_attr_escape(username);
    let error = html_attr_escape(error);
    render_template(
        ASSETS
            .get_file("login.html")
            .and_then(|file| file.contents_utf8())
            .unwrap_or_default(),
        &[
            ("root", root.as_str()),
            ("username", username.as_str()),
            ("error", error.as_str()),
        ],
    )
}

fn dashboard_asset_router() -> Router {
    Router::new().route("/static/{*path}", get(serve_asset))
}

async fn security_headers(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let sensitive = !request.uri().path().starts_with("/static/");
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(
            "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; \
             connect-src 'self'; img-src 'self' data:; frame-ancestors 'none'; base-uri 'none'",
        ),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    if sensitive {
        headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    }
    response
}

fn html_attr_escape(input: &str) -> String {
    let mut escaped = String::with_capacity(input.len());
    for c in input.chars() {
        match c {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#39;"),
            _ => escaped.push(c),
        }
    }
    escaped
}

fn asset_fingerprint(contents: impl IntoIterator<Item = u8>) -> String {
    format!("{:016x}", crate::database::stable_hash(contents))
}

async fn serve_asset(path: axum::extract::Path<String>) -> Response {
    let Some(file) = ASSETS.get_file(path.as_str()) else {
        return (StatusCode::NOT_FOUND, "not found").into_response();
    };
    let content_type = match path.rsplit('.').next() {
        Some("css") => "text/css; charset=utf-8",
        Some("js") => "application/javascript; charset=utf-8",
        Some("html") => "text/html; charset=utf-8",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        _ => "application/octet-stream",
    };
    (
        [
            (header::CONTENT_TYPE, content_type),
            (header::CACHE_CONTROL, "max-age=3600"),
        ],
        file.contents(),
    )
        .into_response()
}

#[cfg(test)]
mod dashboard_assets_tests {
    use super::*;

    #[test]
    fn asset_fingerprint_is_stable_and_content_sensitive() {
        assert_eq!(
            asset_fingerprint(*b"app"),
            asset_fingerprint(b"app".iter().copied())
        );
        assert_ne!(asset_fingerprint(*b"app"), asset_fingerprint(*b"changed"));
    }

    #[test]
    fn application_asset_version_covers_css_and_javascript() {
        let version = application_asset_version();
        assert_eq!(version.len(), 16);
        assert_ne!(version, asset_fingerprint([]));
    }

    #[test]
    fn render_template_substitutes_each_placeholder_once() {
        let rendered = render_template(
            r#"<meta root="{root}" user="{username}" other="{unknown}">"#,
            &[("root", "/pg"), ("username", "{root}")],
        );
        // A substituted value containing a placeholder literal stays
        // literal, and unknown placeholders survive untouched.
        assert_eq!(
            rendered,
            r#"<meta root="/pg" user="{root}" other="{unknown}">"#
        );
    }

    #[test]
    fn render_template_keeps_unterminated_braces() {
        assert_eq!(
            render_template("{root {root} {roots}", &[("root", "/pg")]),
            "{root /pg {roots}"
        );
    }

    #[test]
    fn render_index_keeps_placeholder_literals_in_username() {
        let index = render_index("/pg", "{asset_version}", true);
        let index = std::str::from_utf8(&index).unwrap_or_default();
        assert!(
            index.contains(r#"content="{asset_version}""#),
            "username must stay literal: {index}"
        );
    }

    #[test]
    fn render_login_keeps_error_placeholder_literals_in_values() {
        let login = render_login("/{error}", "{error}", "Invalid <credentials>");
        assert!(login.contains(r#"href="/{error}/static/pico.min.css""#));
        assert!(login.contains(r#"action="/{error}/login""#));
        assert!(login.contains(r#"value="{error}""#));
        assert!(login.contains("Invalid &lt;credentials&gt;"));
    }
}

// Authentication

const SESSION_COOKIE_PREFIX: &str = "pgqueue_session_";
const ACTION_HEADER: &str = "x-pgqueue-request";
const ACTION_HEADER_VALUE: &[u8] = b"dashboard";
const SESSION_TTL: Duration = Duration::from_secs(12 * 60 * 60);
const MAX_SESSIONS: usize = 64;

struct DashboardAuthState {
    username: String,
    password: RwLock<String>,
    sessions: Mutex<HashMap<String, Instant>>,
    root: String,
    session_cookie_name: String,
    secure_cookies: bool,
}

#[derive(Deserialize)]
struct DashboardLoginForm {
    username: String,
    password: String,
}

#[derive(Deserialize)]
struct DashboardPasswordChange {
    current_password: String,
    new_password: String,
}

impl DashboardAuthState {
    fn new(username: String, password: String, root: String, secure_cookies: bool) -> Arc<Self> {
        Arc::new(Self {
            username,
            password: RwLock::new(password),
            sessions: Mutex::new(HashMap::new()),
            root,
            session_cookie_name: format!("{SESSION_COOKIE_PREFIX}{}", Uuid::now_v7().simple()),
            secure_cookies,
        })
    }

    fn credentials_match(&self, username: &str, password: &str) -> bool {
        let Ok(expected_password) = self.password.read() else {
            tracing::error!("dashboard password lock poisoned");
            return false;
        };
        constant_time_eq(username.as_bytes(), self.username.as_bytes())
            && constant_time_eq(password.as_bytes(), expected_password.as_bytes())
    }

    fn basic_credentials_match(&self, headers: &HeaderMap) -> bool {
        let Ok(password) = self.password.read() else {
            tracing::error!("dashboard password lock poisoned");
            return false;
        };
        let expected = base64(format!("{}:{}", self.username, password.as_str()));
        headers.get(header::AUTHORIZATION).is_some_and(|value| {
            // RFC 7617: the auth-scheme token is case-insensitive and is
            // separated from the credentials by one or more spaces.
            let value = value.as_bytes();
            let Some((scheme, rest)) = value.split_at_checked(5) else {
                return false;
            };
            if !scheme.eq_ignore_ascii_case(b"basic") || !rest.starts_with(b" ") {
                return false;
            }
            let credentials = &rest[rest
                .iter()
                .position(|byte| *byte != b' ')
                .unwrap_or(rest.len())..];
            constant_time_eq(credentials, expected.as_bytes())
        })
    }

    fn create_session(&self) -> Option<String> {
        let bytes = rand::random::<[u8; 32]>();
        let token = bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let now = Instant::now();
        let Ok(mut sessions) = self.sessions.lock() else {
            tracing::error!("dashboard session lock poisoned");
            return None;
        };
        sessions.retain(|_, expires_at| *expires_at > now);
        if sessions.len() >= MAX_SESSIONS {
            let oldest = sessions
                .iter()
                .min_by_key(|(_, expires_at)| **expires_at)
                .map(|(token, _)| token.clone());
            if let Some(oldest) = oldest {
                sessions.remove(&oldest);
            }
        }
        sessions.insert(token.clone(), now + SESSION_TTL);
        Some(token)
    }

    fn session_is_valid(&self, headers: &HeaderMap) -> bool {
        let Some(token) = session_token(headers, &self.session_cookie_name) else {
            return false;
        };
        let now = Instant::now();
        let Ok(mut sessions) = self.sessions.lock() else {
            tracing::error!("dashboard session lock poisoned");
            return false;
        };
        sessions.retain(|_, expires_at| *expires_at > now);
        sessions.contains_key(token)
    }

    fn remove_session(&self, headers: &HeaderMap) {
        let Some(token) = session_token(headers, &self.session_cookie_name) else {
            return;
        };
        let Ok(mut sessions) = self.sessions.lock() else {
            tracing::error!("dashboard session lock poisoned");
            return;
        };
        sessions.remove(token);
    }

    fn invalidate_other_sessions(&self, headers: &HeaderMap) {
        let current = session_token(headers, &self.session_cookie_name).map(str::to_owned);
        let Ok(mut sessions) = self.sessions.lock() else {
            tracing::error!("dashboard session lock poisoned");
            return;
        };
        sessions.retain(|token, _| current.as_ref().is_some_and(|current| current == token));
    }

    fn login_html(&self, error: &str) -> String {
        render_login(&self.root, &self.username, error)
    }

    fn home_path(&self) -> String {
        if self.root.is_empty() {
            "/".to_string()
        } else {
            self.root.clone()
        }
    }

    fn login_path(&self) -> String {
        format!("{}/login", self.root)
    }

    fn cookie_path(&self) -> &str {
        if self.root.is_empty() {
            "/"
        } else {
            &self.root
        }
    }
}

fn account_router(auth: Arc<DashboardAuthState>) -> Router {
    Router::new()
        .route("/api/account/password", post(change_password))
        .route("/api/account/logout", post(logout))
        .with_state(auth)
}

fn login_router(auth: Arc<DashboardAuthState>) -> Router {
    Router::new()
        .route("/login", get(login_page).post(login))
        .with_state(auth)
}

async fn require_auth(
    State(auth): State<Arc<DashboardAuthState>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    if auth.session_is_valid(request.headers()) || auth.basic_credentials_match(request.headers()) {
        return next.run(request).await;
    }

    let wants_html = !request.uri().path().starts_with("/api/")
        && request
            .headers()
            .get(header::ACCEPT)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.contains("text/html"));
    if wants_html {
        return redirect_response(&auth.login_path(), None);
    }

    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, "Basic realm=\"pgqueue\"")],
        "unauthorized",
    )
        .into_response()
}

async fn login_page(State(auth): State<Arc<DashboardAuthState>>) -> Html<String> {
    Html(auth.login_html(""))
}

async fn login(
    State(auth): State<Arc<DashboardAuthState>>,
    Form(form): Form<DashboardLoginForm>,
) -> Response {
    if !auth.credentials_match(&form.username, &form.password) {
        return (
            StatusCode::UNAUTHORIZED,
            Html(auth.login_html("Invalid username or password.")),
        )
            .into_response();
    }
    let Some(token) = auth.create_session() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    redirect_response(
        &auth.home_path(),
        Some(&session_cookie(
            &auth.session_cookie_name,
            &token,
            auth.secure_cookies,
            auth.cookie_path(),
        )),
    )
}

async fn change_password(
    State(auth): State<Arc<DashboardAuthState>>,
    headers: HeaderMap,
    Json(change): Json<DashboardPasswordChange>,
) -> Result<Response, DashboardApiError> {
    require_action_header(&headers)?;
    if change.new_password.len() < 8 {
        return Err(DashboardApiError::BadRequest(
            "new password must be at least 8 characters",
        ));
    }
    if change.new_password.len() > 1_024 {
        return Err(DashboardApiError::BadRequest("new password is too long"));
    }
    if !auth.credentials_match(&auth.username, &change.current_password) {
        return Err(DashboardApiError::Forbidden(
            "current password is incorrect",
        ));
    }
    let Ok(mut password) = auth.password.write() else {
        return Err(DashboardApiError::Internal(Error::Dashboard(
            std::io::Error::other("dashboard password lock poisoned"),
        )));
    };
    *password = change.new_password;
    drop(password);
    auth.invalidate_other_sessions(&headers);
    Ok(Json(json!({ "changed": true })).into_response())
}

async fn logout(
    State(auth): State<Arc<DashboardAuthState>>,
    headers: HeaderMap,
) -> Result<Response, DashboardApiError> {
    require_action_header(&headers)?;
    auth.remove_session(&headers);
    let mut response = Json(json!({ "logged_out": true })).into_response();
    let clear_cookie = session_cookie_attributes(
        &format!("{}=", auth.session_cookie_name),
        auth.secure_cookies,
        auth.cookie_path(),
        Some(0),
    );
    let Ok(clear_cookie) = HeaderValue::from_str(&clear_cookie) else {
        tracing::error!("invalid dashboard session cookie name");
        return Ok(StatusCode::INTERNAL_SERVER_ERROR.into_response());
    };
    response
        .headers_mut()
        .insert(header::SET_COOKIE, clear_cookie);
    Ok(response)
}

fn session_token<'a>(headers: &'a HeaderMap, cookie_name: &str) -> Option<&'a str> {
    headers
        .get(header::COOKIE)?
        .to_str()
        .ok()?
        .split(';')
        .map(str::trim)
        .filter_map(|cookie| cookie.split_once('='))
        .find_map(|(name, token)| (name == cookie_name).then_some(token))
        .filter(|token| !token.is_empty())
}

fn session_cookie(cookie_name: &str, token: &str, secure: bool, path: &str) -> String {
    session_cookie_attributes(
        &format!("{cookie_name}={token}"),
        secure,
        path,
        Some(SESSION_TTL.as_secs()),
    )
}

fn session_cookie_attributes(
    value: &str,
    secure: bool,
    path: &str,
    max_age: Option<u64>,
) -> String {
    let secure = if secure { "; Secure" } else { "" };
    let max_age = max_age
        .map(|seconds| format!("; Max-Age={seconds}"))
        .unwrap_or_default();
    format!("{value}; Path={path}{secure}; HttpOnly; SameSite=Strict{max_age}")
}

fn redirect_response(location: &str, cookie: Option<&str>) -> Response {
    let Ok(location) = HeaderValue::from_str(location) else {
        tracing::error!(location, "invalid dashboard redirect path");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let mut response = StatusCode::SEE_OTHER.into_response();
    response.headers_mut().insert(header::LOCATION, location);
    if let Some(cookie) = cookie {
        let Ok(cookie) = HeaderValue::from_str(cookie) else {
            tracing::error!("invalid dashboard session cookie");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        };
        response.headers_mut().insert(header::SET_COOKIE, cookie);
    }
    response
}

/// Constant-time byte comparison (length mismatch short-circuits, which only
/// leaks the credential length).
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Standard base64 (with padding), kept local to avoid a dependency for one
/// HTTP Basic header.
fn base64(input: impl AsRef<[u8]>) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let bytes = input.as_ref();
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(ALPHABET[(n >> (18 - 6 * i)) as usize & 0x3f] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

#[cfg(test)]
mod dashboard_auth_tests {
    use super::*;

    #[test]
    fn base64_matches_reference_vectors() {
        assert_eq!(base64(""), "");
        assert_eq!(base64("f"), "Zg==");
        assert_eq!(base64("fo"), "Zm8=");
        assert_eq!(base64("foo"), "Zm9v");
        assert_eq!(base64("foob"), "Zm9vYg==");
        assert_eq!(base64("fooba"), "Zm9vYmE=");
        assert_eq!(base64("foobar"), "Zm9vYmFy");
        assert_eq!(base64("admin:s3cret"), "YWRtaW46czNjcmV0");
    }

    #[test]
    fn constant_time_eq_accepts_only_equal_bytes() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn session_cookie_security_is_configurable() {
        let secure = session_cookie("cookie", "token", true, "/");
        assert!(secure.contains("; Secure;"));
        let plain_http = session_cookie("cookie", "token", false, "/");
        assert!(!plain_http.contains("; Secure;"));
        assert!(plain_http.contains("; HttpOnly; SameSite=Strict;"));
    }

    #[test]
    fn session_cookie_uses_configured_path() {
        let cookie = session_cookie("cookie", "token", true, "/admin");
        assert!(cookie.contains("; Path=/admin;"));
    }

    #[test]
    fn dashboard_auth_states_use_distinct_session_cookie_names() {
        let first = DashboardAuthState::new("admin".into(), "secret".into(), String::new(), true);
        let second = DashboardAuthState::new("admin".into(), "secret".into(), String::new(), true);

        assert!(first.session_cookie_name.starts_with(SESSION_COOKIE_PREFIX));
        assert!(
            second
                .session_cookie_name
                .starts_with(SESSION_COOKIE_PREFIX)
        );
        assert_ne!(first.session_cookie_name, second.session_cookie_name);
    }
}

// Dashboard persistence

/// Dashboard representation with persisted job and cron metadata.
#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct DashboardJobRow {
    /// The common execution fields.
    #[serde(flatten)]
    pub job: JobRow,
    /// Either `job` or `cron`.
    pub kind: String,
    /// Source schedule for cron rows.
    pub cron_expr: Option<String>,
    /// Most recent enqueue, touch, or completion time.
    pub updated_at: DateTime<Utc>,
}

/// One row of the dashboard job queries: `JobRow` plus the dashboard-only
/// columns, kept flat because the SQLx macros map flat records only. Both
/// dashboard job queries share this struct so the column list and the
/// `JobRow` assembly exist exactly once.
struct DashboardJobRecord {
    id: Uuid,
    unique_key: Option<String>,
    queue: String,
    name: String,
    payload: Value,
    status: JobStatus,
    priority: i16,
    group_key: Option<String>,
    attempts: i32,
    max_attempts: i32,
    timeout_ms: Option<i64>,
    heartbeat_ms: Option<i64>,
    retry_delay_ms: i64,
    backoff: JobRetryBackoff,
    ttl_ms: Option<i64>,
    scheduled_at: DateTime<Utc>,
    enqueued_at: DateTime<Utc>,
    started_at: Option<DateTime<Utc>>,
    touched_at: Option<DateTime<Utc>>,
    completed_at: Option<DateTime<Utc>>,
    expires_at: Option<DateTime<Utc>>,
    result: Option<Value>,
    error: Option<String>,
    meta: Value,
    worker_id: Option<Uuid>,
    kind: String,
    cron_expr: Option<String>,
    updated_at: DateTime<Utc>,
}

impl From<DashboardJobRecord> for DashboardJobRow {
    fn from(row: DashboardJobRecord) -> Self {
        Self {
            job: JobRow {
                id: row.id,
                unique_key: row.unique_key,
                queue: row.queue,
                name: row.name,
                payload: row.payload,
                status: row.status,
                priority: row.priority,
                group_key: row.group_key,
                attempts: row.attempts,
                max_attempts: row.max_attempts,
                timeout_ms: row.timeout_ms,
                heartbeat_ms: row.heartbeat_ms,
                retry_delay_ms: row.retry_delay_ms,
                backoff: row.backoff,
                ttl_ms: row.ttl_ms,
                scheduled_at: row.scheduled_at,
                enqueued_at: row.enqueued_at,
                started_at: row.started_at,
                touched_at: row.touched_at,
                completed_at: row.completed_at,
                expires_at: row.expires_at,
                result: row.result,
                error: row.error,
                meta: row.meta,
                worker_id: row.worker_id,
            },
            kind: row.kind,
            cron_expr: row.cron_expr,
            updated_at: row.updated_at,
        }
    }
}

/// Bounded operational signals used instead of exact retained-job counts.
#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct DashboardQueueSignals {
    /// Queue name.
    pub name: String,
    /// Oldest job ready to dequeue now.
    pub oldest_ready_at: Option<DateTime<Utc>>,
    /// Next future-scheduled job.
    pub next_scheduled_at: Option<DateTime<Utc>>,
    /// `running`, `aborting`, or `idle`.
    pub execution: String,
    /// Whether at least one unexpired worker exists.
    pub has_live_workers: bool,
    /// Most recent retained failure.
    pub latest_failure_at: Option<DateTime<Utc>>,
}

impl Database {
    pub(crate) async fn dashboard_jobs_page(
        &self,
        statuses: &[JobStatus],
        kind: &str,
        name: Option<&str>,
        cursor: Option<(DateTime<Utc>, Uuid)>,
        limit: i64,
    ) -> Result<Vec<DashboardJobRow>, Error> {
        if statuses.is_empty() {
            return Err(Error::Config(
                "dashboard jobs page requires at least one status".into(),
            ));
        }
        if limit <= 0 {
            return Err(Error::Config(
                "dashboard jobs page limit must be greater than zero".into(),
            ));
        }
        let statuses = statuses
            .iter()
            .map(|status| status.as_str().to_owned())
            .collect::<Vec<_>>();
        let (cursor_time, cursor_id) = cursor.unzip();
        let rows = sqlx::query_as!(
            DashboardJobRecord,
            r#"
            WITH keys AS (
                SELECT candidate.enqueued_at, candidate.id
                FROM unnest($2::text[]) AS requested(status)
                CROSS JOIN LATERAL (
                    SELECT enqueued_at, id
                    FROM pgqueue.jobs
                    WHERE queue = $1
                      AND kind = $3
                      AND status = requested.status
                      AND ($4::text IS NULL OR name = $4)
                      AND ($5::timestamptz IS NULL OR (enqueued_at, id) < ($5, $6))
                    ORDER BY enqueued_at DESC, id DESC
                    LIMIT $7
                ) candidate
                ORDER BY candidate.enqueued_at DESC, candidate.id DESC
                LIMIT $7
            )
            SELECT
                jobs.id,
                jobs.unique_key,
                jobs.queue,
                jobs.name,
                jobs.payload,
                jobs.status AS "status: JobStatus",
                jobs.priority,
                jobs.group_key,
                jobs.attempts,
                jobs.max_attempts,
                jobs.timeout_ms,
                jobs.heartbeat_ms,
                jobs.retry_delay_ms,
                jobs.backoff AS "backoff: JobRetryBackoff",
                jobs.ttl_ms,
                jobs.scheduled_at,
                jobs.enqueued_at,
                jobs.started_at,
                jobs.touched_at,
                jobs.completed_at,
                jobs.expires_at,
                jobs.result,
                jobs.error,
                jobs.meta,
                jobs.worker_id,
                jobs.kind,
                jobs.cron_expr,
                GREATEST(
                    jobs.enqueued_at,
                    COALESCE(jobs.touched_at, jobs.enqueued_at),
                    COALESCE(jobs.completed_at, jobs.enqueued_at)
                ) AS "updated_at!"
            FROM keys
            JOIN pgqueue.jobs AS jobs ON jobs.id = keys.id
            ORDER BY keys.enqueued_at DESC, keys.id DESC
            "#,
            self.name(),
            &statuses,
            kind,
            name,
            cursor_time,
            cursor_id,
            limit,
        )
        .fetch_all(self.pool())
        .await?;

        Ok(rows.into_iter().map(DashboardJobRow::from).collect())
    }

    pub(crate) async fn dashboard_job_names(
        &self,
        statuses: &[JobStatus],
        kind: &str,
        prefix: &str,
        sample: i64,
        limit: i64,
    ) -> Result<Vec<String>, Error> {
        let statuses = statuses
            .iter()
            .map(|status| status.as_str().to_owned())
            .collect::<Vec<_>>();
        Ok(sqlx::query_scalar!(
            r#"
            WITH keys AS (
                SELECT candidate.enqueued_at, candidate.id
                FROM unnest($2::text[]) AS requested(status)
                CROSS JOIN LATERAL (
                    SELECT enqueued_at, id
                    FROM pgqueue.jobs
                    WHERE queue = $1
                      AND kind = $3
                      AND status = requested.status
                    ORDER BY enqueued_at DESC, id DESC
                    LIMIT $4
                ) candidate
                ORDER BY candidate.enqueued_at DESC, candidate.id DESC
                LIMIT $4
            )
            SELECT jobs.name AS "name!"
            FROM keys
            JOIN pgqueue.jobs AS jobs ON jobs.id = keys.id
            WHERE starts_with(lower(jobs.name), lower($5))
            GROUP BY jobs.name
            ORDER BY lower(jobs.name), jobs.name
            LIMIT $6
            "#,
            self.name(),
            &statuses,
            kind,
            sample,
            prefix,
            limit,
        )
        .fetch_all(self.pool())
        .await?)
    }

    pub(crate) async fn dashboard_job(&self, id: Uuid) -> Result<Option<DashboardJobRow>, Error> {
        let row = sqlx::query_as!(
            DashboardJobRecord,
            r#"
            SELECT
                id,
                unique_key,
                queue,
                name,
                payload,
                status AS "status: JobStatus",
                priority,
                group_key,
                attempts,
                max_attempts,
                timeout_ms,
                heartbeat_ms,
                retry_delay_ms,
                backoff AS "backoff: JobRetryBackoff",
                ttl_ms,
                scheduled_at,
                enqueued_at,
                started_at,
                touched_at,
                completed_at,
                expires_at,
                result,
                error,
                meta,
                worker_id,
                kind,
                cron_expr,
                GREATEST(
                    enqueued_at,
                    COALESCE(touched_at, enqueued_at),
                    COALESCE(completed_at, enqueued_at)
                ) AS "updated_at!"
            FROM pgqueue.jobs
            WHERE id = $1 AND queue = $2
            "#,
            id,
            self.name(),
        )
        .fetch_optional(self.pool())
        .await?;

        Ok(row.map(DashboardJobRow::from))
    }

    pub(crate) async fn dashboard_signals(&self) -> Result<DashboardQueueSignals, Error> {
        Ok(sqlx::query_as!(
            DashboardQueueSignals,
            r#"
            SELECT
                $1::text AS "name!",
                (
                    SELECT scheduled_at
                    FROM pgqueue.jobs
                    WHERE queue = $1 AND status = 'queued' AND scheduled_at <= now()
                    ORDER BY scheduled_at, id
                    LIMIT 1
                ) AS oldest_ready_at,
                (
                    SELECT scheduled_at
                    FROM pgqueue.jobs
                    WHERE queue = $1 AND status = 'queued' AND scheduled_at > now()
                    ORDER BY scheduled_at, id
                    LIMIT 1
                ) AS next_scheduled_at,
                CASE
                    WHEN EXISTS (
                        SELECT 1 FROM pgqueue.jobs
                        WHERE queue = $1 AND status = 'running'
                        LIMIT 1
                    ) THEN 'running'
                    WHEN EXISTS (
                        SELECT 1 FROM pgqueue.jobs
                        WHERE queue = $1 AND status = 'aborting'
                        LIMIT 1
                    ) THEN 'aborting'
                    ELSE 'idle'
                END AS "execution!",
                EXISTS (
                    SELECT 1 FROM pgqueue.workers
                    WHERE queue = $1 AND expires_at > now()
                    LIMIT 1
                ) AS "has_live_workers!",
                (
                    SELECT completed_at
                    FROM pgqueue.jobs
                    WHERE queue = $1 AND status = 'failed'
                    ORDER BY completed_at DESC, id DESC
                    LIMIT 1
                ) AS latest_failure_at
            "#,
            self.name(),
        )
        .fetch_one(self.pool())
        .await?)
    }

    pub(crate) async fn dashboard_probe(&self) -> Result<(), Error> {
        let _ = sqlx::query_scalar!(
            "SELECT EXISTS (SELECT 1 FROM pgqueue.jobs WHERE queue = $1 LIMIT 1)",
            self.name(),
        )
        .fetch_one(self.pool())
        .await?;
        Ok(())
    }

    pub(crate) async fn dashboard_workers_page(
        &self,
        cursor: Option<(DateTime<Utc>, Uuid)>,
        limit: i64,
    ) -> Result<Vec<WorkerInfo>, Error> {
        if limit <= 0 {
            return Err(Error::Config(
                "dashboard workers page limit must be greater than zero".into(),
            ));
        }
        let (cursor_time, cursor_id) = cursor.unzip();
        Ok(sqlx::query_as!(
            WorkerInfo,
            r#"
            SELECT id, queue, stats, metadata, started_at, heartbeat_at, expires_at
            FROM pgqueue.workers
            WHERE queue = $1
              AND expires_at > now()
              AND ($2::timestamptz IS NULL OR (started_at, id) > ($2, $3))
            ORDER BY started_at, id
            LIMIT $4
            "#,
            self.name(),
            cursor_time,
            cursor_id,
            limit,
        )
        .fetch_all(self.pool())
        .await?)
    }

    pub(crate) async fn dashboard_worker(&self, id: Uuid) -> Result<Option<WorkerInfo>, Error> {
        Ok(sqlx::query_as!(
            WorkerInfo,
            r#"
            SELECT id, queue, stats, metadata, started_at, heartbeat_at, expires_at
            FROM pgqueue.workers
            WHERE id = $1 AND queue = $2 AND expires_at > now()
            "#,
            id,
            self.name(),
        )
        .fetch_optional(self.pool())
        .await?)
    }
}
