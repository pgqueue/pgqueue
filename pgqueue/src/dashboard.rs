//! The embedded web dashboard: an axum router serving a JSON API and a
//! no-build-step static frontend for managing queues and jobs.
//!
//! Run it as a standalone server:
//!
//! ```ignore
//! Dashboard::new([queue])
//!     .basic_auth("admin", "secret")
//!     .secure_cookies(false) // only for direct HTTP on a trusted network
//!     .serve_on("0.0.0.0", 8080)
//!     .run()
//!     .await?;
//! ```
//!
//! Or host it inside a worker process:
//!
//! ```ignore
//! let dashboard = Dashboard::new([queue.clone()])
//!     .basic_auth("admin", "secret")
//!     .secure_cookies(false) // only for direct HTTP on a trusted network
//!     .serve_on("0.0.0.0", 8080);
//! Worker::builder(queue)
//!     .register_job(job)
//!     .dashboard(dashboard)
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
//!
//! Every state-changing route — the job retry and abort actions, the password
//! change and the logout — requires the request header
//! `X-Pgqueue-Request: dashboard`. It is the CSRF guard: a cross-site form post
//! cannot set a request header, so the credentials a browser attaches on its
//! own cannot reach an action. A `POST` without it is answered `403 Forbidden`,
//! so a script driving the API has to send it as well.
//!
//! `POST /login` is the one state-changing route that cannot require it — it is
//! a real HTML form, so nothing of ours runs before the browser sends it. It is
//! guarded on `Sec-Fetch-Site` instead: a post the browser reports as coming
//! from anywhere but the dashboard itself is answered `403 Forbidden` before it
//! can spend any of the account's rate-limit budget. Clients that send no
//! `Sec-Fetch-Site` at all are unaffected.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, LazyLock, Mutex, RwLock};
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
use crate::job::{JobRetryBackoff, JobRow, JobStatus, MIN_TIMESTAMPTZ_SECONDS};
use crate::queue::Queue;
use crate::worker::{WorkerHealth, WorkerHealthStatus, WorkerInfo};

pub(crate) struct DashboardState {
    queues: Vec<Queue>,
    worker_health: Option<WorkerHealth>,
    /// Last `/health` probe result and when it was taken. The route is
    /// deliberately unauthenticated, so without this a request flood would run
    /// one query per queue per request on the very pool the worker dequeues
    /// and finalizes with.
    health_probe: std::sync::Mutex<Option<(std::time::Instant, bool)>>,
    /// One permit, so at most one probe round is ever in flight. The TTL alone
    /// bounds the rate only while probes are *fast*, which is exactly when it
    /// does not matter: with the probe query slow (lock contention,
    /// `max_connections` pressure), an anonymous flood raced past the
    /// not-yet-written cache and took one pooled connection per request —
    /// draining the pool the worker dequeues and finalizes with.
    health_gate: tokio::sync::Semaphore,
}

/// How long a `/health` probe result is reused. Short enough that an
/// orchestrator still sees a real outage promptly, long enough that request
/// rate cannot translate into database load.
const HEALTH_PROBE_TTL: Duration = Duration::from_millis(500);

/// Configures the dashboard router. See the module docs.
pub struct Dashboard {
    queues: Vec<Queue>,
    auth: Option<(String, String)>,
    mount_path: String,
    secure_cookies: bool,
    trusted_proxy_hops: usize,
}

/// A complete dashboard server configuration, created with
/// [`Dashboard::serve_on`].
///
/// Run it as a standalone server with [`DashboardServer::run`], or pass it to
/// [`crate::WorkerBuilder::dashboard`] to host it in a worker process. Use
/// [`Dashboard::router`] instead when an application already owns an axum
/// server.
pub struct DashboardServer {
    dashboard: Dashboard,
    host: String,
    port: u16,
    ready: tokio::sync::watch::Sender<Option<SocketAddr>>,
}

/// Observes a dashboard server as it starts.
///
/// Obtain a handle with [`DashboardServer::server_handle`] before running or
/// passing the server to [`crate::WorkerBuilder::dashboard`]. This is
/// especially useful with port `0`, where the operating system chooses the
/// listening port.
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
            trusted_proxy_hops: 0,
        }
    }

    /// Protects the dashboard with a browser login and HTTP Basic
    /// authentication for API clients. Password changes made in the dashboard
    /// last for the lifetime of the running dashboard process. The `/health`
    /// endpoint remains unauthenticated for orchestrator probes.
    ///
    /// Both values must be non-empty: an empty one compares equal to the empty
    /// credential every client can send, which is a dashboard that looks
    /// protected and admits anyone. [`Dashboard::router`] and
    /// [`Dashboard::serve_on`] refuse it with [`Error::Config`] rather than
    /// serving it.
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

    /// How many trusted reverse proxies sit in front of this dashboard, each
    /// appending the address it saw to `X-Forwarded-For`. Defaults to `0`, which
    /// ignores the header entirely and charges authentication attempts to the
    /// socket peer.
    ///
    /// Behind a proxy the socket peer is the proxy, so every request in the
    /// world shares one throttle bucket and a flood of wrong passwords from
    /// anywhere keeps the operator's own login refused (see
    /// [`Dashboard::basic_auth`]). Setting this to the number of proxies restores
    /// per-client keying: the client is the `hops`-th address from the *right*
    /// of the chain, which is the last one your own proxies appended, so
    /// anything a client puts in the header itself is pushed out of reach.
    ///
    /// Set it only when the dashboard cannot be reached except through those
    /// proxies. A client that can connect directly supplies the whole chain, and
    /// so can pick a fresh bucket per request and evade the throttle.
    ///
    /// ```no_run
    /// # fn dashboard(queue: pgqueue::Queue) -> Result<axum::Router, pgqueue::Error> {
    /// let router = pgqueue::Dashboard::new([queue])
    ///     .basic_auth("admin", "secret")
    ///     // One TLS-terminating proxy, which no client can bypass.
    ///     .trusted_proxy_hops(1)
    ///     .router()?;
    /// # Ok(router)
    /// # }
    /// ```
    pub fn trusted_proxy_hops(mut self, hops: usize) -> Self {
        self.trusted_proxy_hops = hops;
        self
    }

    /// The path prefix the router will be nested under (default `/`), so the
    /// frontend can locate its assets and API. [`DashboardServer`] instances
    /// must keep the default and are served at `/`. A relative path is
    /// normalized to start with `/`. Path segments may contain ASCII letters,
    /// digits, `-`, `_`, `.`, and `~`.
    pub fn mount_path(mut self, path: impl Into<String>) -> Self {
        let path = path.into();
        self.mount_path = if path.starts_with('/') {
            path
        } else {
            format!("/{path}")
        };
        self
    }

    /// Converts this dashboard into a server bound to `host` and `port`.
    ///
    /// `host` may be a hostname such as `"localhost"` or an IP address.
    /// Hostnames are resolved asynchronously when the server starts.
    ///
    /// Dashboard servers are served at `/`; use [`Dashboard::router`] to mount
    /// the dashboard under a custom path in an existing application.
    ///
    /// ```no_run
    /// # async fn run(queue: pgqueue::Queue) -> anyhow::Result<()> {
    /// pgqueue::Dashboard::new([queue])
    ///     .basic_auth("admin", "secret")
    ///     .serve_on("localhost", 8080)
    ///     .run()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn serve_on(self, host: impl Into<String>, port: u16) -> DashboardServer {
        let (ready, _) = tokio::sync::watch::channel(None);
        DashboardServer {
            dashboard: self,
            host: host.into(),
            port,
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
        validate_mount_path(&self.mount_path)?;
        // `constant_time_eq(b"", b"")` is true, so an empty username or password
        // matches the credential any client can send. The instance still 401s
        // without credentials and still renders a login page, so nothing
        // distinguishes it from a correctly protected one — and it exposes every
        // job payload plus Retry and Abort. `basic_auth(user,
        // env::var("...").unwrap_or_default())` reaches it from one missing
        // environment variable, so refuse to build the router at all.
        if self
            .auth
            .as_ref()
            .is_some_and(|(user, password)| user.is_empty() || password.is_empty())
        {
            return Err(Error::Config(
                "dashboard basic_auth requires a non-empty username and password".into(),
            ));
        }
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
            health_probe: std::sync::Mutex::new(None),
            health_gate: tokio::sync::Semaphore::new(1),
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

        // Health probes must remain usable by an orchestrator even when the
        // interactive dashboard is protected by browser/basic authentication.
        let health_route = Router::new()
            .route("/health", get(health))
            .with_state(state.clone());
        let protected = Router::new()
            .route("/", shell.clone())
            .route("/queues/{queue}", shell.clone())
            .route("/queues/{queue}/workers/{id}", shell.clone())
            .route("/queues/{queue}/jobs/{id}", shell)
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
            let auth = DashboardAuthState::new(
                user,
                password,
                root,
                self.secure_cookies,
                self.trusted_proxy_hops,
            );
            let protected = protected.merge(account_router(auth.clone())).layer(
                axum::middleware::from_fn_with_state(auth.clone(), require_auth),
            );
            dashboard_asset_router()
                .merge(health_route)
                .merge(login_router(auth))
                .merge(protected)
        } else {
            dashboard_asset_router()
                .merge(health_route)
                .merge(protected)
        };
        Ok(router.layer(axum::middleware::from_fn(security_headers)))
    }
}

impl DashboardServer {
    /// Runs the dashboard until `SIGINT` or `SIGTERM`, then shuts down
    /// gracefully.
    ///
    /// Use [`DashboardServer::run_until`] when another component owns the
    /// shutdown signal.
    pub async fn run(self) -> Result<(), Error> {
        let token = CancellationToken::new();
        let run = self.run_until(token.clone());
        tokio::pin!(run);
        tokio::select! {
            result = &mut run => result,
            _ = crate::worker::wait_for_shutdown_signal() => {
                token.cancel();
                run.await
            }
        }
    }

    /// Runs the dashboard until `shutdown` is cancelled.
    ///
    /// Dropping this future requests the same bounded graceful shutdown in a
    /// background task, making this the embeddable, test-friendly entry point.
    pub async fn run_until(self, shutdown: CancellationToken) -> Result<(), Error> {
        let dropped = CancellationToken::new();
        let drop_guard = dropped.clone().drop_guard();
        let result = tokio::spawn(self.run_until_inner(shutdown, dropped)).await?;
        drop_guard.disarm();
        result
    }

    async fn run_until_inner(
        self,
        shutdown: CancellationToken,
        dropped: CancellationToken,
    ) -> Result<(), Error> {
        let config = self.into_server_config(None)?;
        let bound = tokio::select! {
            biased;
            _ = shutdown.cancelled() => return Ok(()),
            _ = dropped.cancelled() => return Ok(()),
            bound = bind_dashboard_server(&config) => bound?,
        };
        let mut runtime = DashboardRuntime::start(bound);
        let error = tokio::select! {
            _ = crate::worker::wait_for_shutdown_or_drop(&shutdown, &dropped) => None,
            error = runtime.unexpected_exit() => Some(error),
        };
        match error {
            Some(error) => Err(error),
            None => runtime.finish_shutdown().await,
        }
    }

    /// Returns a handle that reports the actual address once the dashboard
    /// server task is running.
    ///
    /// ```no_run
    /// # async fn run(queue: pgqueue::Queue) -> anyhow::Result<()> {
    /// let dashboard = pgqueue::Dashboard::new([queue])
    ///     .serve_on("localhost", 0);
    /// let mut handle = dashboard.server_handle();
    /// let shutdown = tokio_util::sync::CancellationToken::new();
    /// let task = tokio::spawn(dashboard.run_until(shutdown.clone()));
    /// let address = handle.wait_until_ready().await;
    /// assert!(address.is_some());
    /// assert_eq!(handle.local_addr(), address);
    /// shutdown.cancel();
    /// task.await??;
    /// # Ok(())
    /// # }
    /// ```
    pub fn server_handle(&self) -> DashboardServerHandle {
        DashboardServerHandle {
            ready: self.ready.subscribe(),
        }
    }

    pub(crate) fn into_server_config(
        self,
        worker_health: Option<WorkerHealth>,
    ) -> Result<DashboardServerConfig, Error> {
        if !self.dashboard.mount_path.trim_end_matches('/').is_empty() {
            return Err(Error::Config(
                "DashboardServer requires mount_path `/`; use Dashboard::router for a custom path"
                    .into(),
            ));
        }
        Ok(DashboardServerConfig {
            host: self.host,
            port: self.port,
            router: self.dashboard.router_with_health(worker_health)?,
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
    /// address, or `None` if the server exits before the dashboard is ready.
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

pub(crate) struct DashboardServerConfig {
    host: String,
    port: u16,
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
    dashboard: Option<&DashboardServerConfig>,
) -> Result<Option<DashboardBoundServer>, Error> {
    let Some(dashboard) = dashboard else {
        return Ok(None);
    };
    Ok(Some(bind_dashboard_server(dashboard).await?))
}

async fn bind_dashboard_server(
    dashboard: &DashboardServerConfig,
) -> Result<DashboardBoundServer, Error> {
    let listener = tokio::net::TcpListener::bind((dashboard.host.as_str(), dashboard.port))
        .await
        .map_err(Error::Dashboard)?;
    let bind = listener.local_addr().map_err(Error::Dashboard)?;
    tracing::info!(
        dashboard.addr = %bind,
        configured.host = dashboard.host,
        configured.port = dashboard.port,
        "dashboard bound"
    );
    Ok(DashboardBoundServer {
        bind,
        listener,
        router: dashboard.router.clone(),
        ready: dashboard.ready.clone(),
    })
}

impl DashboardRuntime {
    pub(crate) fn start(bound: DashboardBoundServer) -> Self {
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let bind = bound.bind;
        let task = tokio::spawn(async move {
            bound.ready.send_replace(Some(bind));
            tracing::info!(dashboard.addr = %bind, "dashboard ready");
            // With connection info, so the authentication throttle can charge a
            // failed credential comparison to the client that made it rather
            // than to everyone at once.
            axum::serve(
                bound.listener,
                bound
                    .router
                    .into_make_service_with_connect_info::<SocketAddr>(),
            )
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
/// lookups 404s, rejected state-changing requests 403s, and throttled requests
/// 429s.
pub(crate) enum DashboardApiError {
    BadRequest(&'static str),
    NotFound(&'static str),
    Forbidden(&'static str),
    TooManyRequests(&'static str),
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
            DashboardApiError::TooManyRequests(what) => (
                StatusCode::TOO_MANY_REQUESTS,
                [(header::RETRY_AFTER, "1")],
                Json(json!({ "error": what })),
            )
                .into_response(),
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

/// Whether the browser says this post came from somewhere other than the
/// dashboard itself.
///
/// [`require_action_header`] cannot guard the login form: it is a real
/// `<form method="post">`, so nothing of ours runs before the browser sends it
/// and no request header can be attached. Its
/// `application/x-www-form-urlencoded` body is a CORS-simple content type, so
/// any page the operator visits can post it with no preflight — and every post
/// spends one comparison from the *victim's* interactive budget before
/// anything is compared, keyed to the victim's own address. Enough concurrent
/// posts and the operator's correct password is answered `429` on their own
/// dashboard, however privately it is bound.
///
/// `Sec-Fetch-Site` is set by the browser and forbidden to scripts, so the
/// attacking page cannot forge it. Its absence is allowed: a curl, a password
/// manager or a script driving the form is not a browser, and is not a
/// cross-site request either — refusing those would break every non-browser
/// client to no benefit, since anything that can omit the header can equally
/// send `same-origin`.
///
/// `same-site` is *not* accepted. The login form is served by the dashboard
/// itself, so a genuine submission is always `same-origin`; anything else is a
/// sibling origin posting credentials at us, which is the vector.
fn is_cross_site_post(headers: &HeaderMap) -> bool {
    headers
        .get(SITE_HEADER)
        .is_some_and(|site| !matches!(site.as_bytes(), b"same-origin" | b"none"))
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
    // A degraded component is deliberately survivable, but readiness still
    // requires database access: degradation must not hide a worker that can no
    // longer reach its queue.
    let degraded = if let Some(status) = state
        .worker_health
        .as_ref()
        .map(|health| health.snapshot().status)
    {
        match status {
            WorkerHealthStatus::Starting | WorkerHealthStatus::Stopped => {
                return (StatusCode::SERVICE_UNAVAILABLE, "unhealthy").into_response();
            }
            WorkerHealthStatus::Degraded => true,
            WorkerHealthStatus::Ready => false,
        }
    } else {
        false
    };
    if let Some(healthy) = cached_health_probe(&state) {
        return health_response(healthy, degraded);
    }
    // Single-flight. Everything that loses the race waits here rather than
    // opening a probe of its own, so a cold cache plus a slow probe costs one
    // pooled connection per queue in total instead of one per request. The
    // semaphore is never closed, so this always resolves to a permit; keeping
    // the whole `Result` alive is what holds it for the round.
    let _round = state.health_gate.acquire().await;
    // The winner of this round may already have answered while we queued.
    if let Some(healthy) = cached_health_probe(&state) {
        return health_response(healthy, degraded);
    }
    let mut probes = tokio::task::JoinSet::new();
    for queue in &state.queues {
        let queue = queue.clone();
        probes.spawn(async move { queue.database().dashboard_probe().await });
    }
    let mut healthy = true;
    while let Some(result) = probes.join_next().await {
        healthy &= matches!(result, Ok(Ok(())));
    }
    if let Ok(mut cache) = state.health_probe.lock() {
        *cache = Some((std::time::Instant::now(), healthy));
    }
    health_response(healthy, degraded)
}

fn cached_health_probe(state: &DashboardState) -> Option<bool> {
    let cache = state.health_probe.lock().ok()?;
    let (taken_at, healthy) = (*cache)?;
    (taken_at.elapsed() < HEALTH_PROBE_TTL).then_some(healthy)
}

fn health_response(healthy: bool, degraded: bool) -> Response {
    if healthy {
        if degraded {
            (StatusCode::OK, "DEGRADED").into_response()
        } else {
            (StatusCode::OK, "OK").into_response()
        }
    } else {
        (StatusCode::INTERNAL_SERVER_ERROR, "unhealthy").into_response()
    }
}

pub(crate) async fn list_queues(
    State(state): State<Arc<DashboardState>>,
) -> Result<Response, DashboardApiError> {
    // Spawned up front so the queries overlap, then awaited in order: the
    // response follows the configured queue order without an index-tagged
    // result set to reassemble. Aborted on drop, so an early return does not
    // leave queries running for a response nobody will read.
    let tasks: Vec<_> = state
        .queues
        .iter()
        .cloned()
        .map(|queue| {
            tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
                queue.database().dashboard_signals().await
            }))
        })
        .collect();
    let mut queues = Vec::with_capacity(tasks.len());
    for task in tasks {
        queues.push(task.await.map_err(Error::from)??);
    }
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

/// The statuses a `status=a,b` filter names, or all of them when it names none.
/// Shared by the job listing and the name typeahead so a suggestion cannot
/// offer a name that the listing beside it then filters away.
fn filter_statuses(status: Option<&str>) -> Result<Vec<JobStatus>, DashboardApiError> {
    let mut statuses = Vec::new();
    if let Some(value) = status {
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
    Ok(statuses)
}

impl DashboardJobsQuery {
    fn filter(self) -> Result<DashboardFilteredJobsQuery, DashboardApiError> {
        let statuses = filter_statuses(self.status.as_deref())?;
        let kind = job_kind(self.kind.as_deref())?.to_string();
        let cursor = cursor_pair(self.cursor_enqueued_at, self.cursor_id)?;
        let name = self.name.filter(|name| !name.is_empty());
        if name.as_ref().is_some_and(|name| name.len() > 255) {
            return Err(DashboardApiError::BadRequest("job name is too long"));
        }
        // `%00` decodes into the `String` like any other byte, and PostgreSQL
        // `text` cannot hold it (`22021`). Left to reach the query it came back
        // as an `Internal`: a 500 and an error-level log for a request this
        // type promises to 400, having burned a pooled connection to find out.
        if name.as_ref().is_some_and(|name| name.contains('\0')) {
            return Err(DashboardApiError::BadRequest(
                "job name must not contain NUL",
            ));
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
    status: Option<String>,
}

fn cursor_pair(
    timestamp: Option<DateTime<Utc>>,
    id: Option<Uuid>,
) -> Result<Option<(DateTime<Utc>, Uuid)>, DashboardApiError> {
    match (timestamp, id) {
        (None, None) => Ok(None),
        (Some(timestamp), Some(id)) => {
            // `DateTime<Utc>` reaches ISO year -262144, so every cursor between
            // there and PostgreSQL's floor deserialized, reached the query and
            // came back as `22008` -> `Internal`: a 500 and an error-level log
            // for a request this type promises to 400, having burned a pooled
            // connection to find out. Same class as the `%00` name filter above,
            // and checked here because both paged endpoints funnel through it.
            if timestamp.timestamp() < MIN_TIMESTAMPTZ_SECONDS {
                return Err(DashboardApiError::BadRequest(
                    "page cursor timestamp is out of range",
                ));
            }
            Ok(Some((timestamp, id)))
        }
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
            "enqueued_at": job.enqueued_at,
            "id": job.id,
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
    // The suggestions have to answer the same question the listing beside them
    // does. Ignoring the status filter offered names that exist only under some
    // other status, and choosing one rendered "No jobs found".
    let statuses = filter_statuses(query.status.as_deref())?;
    let prefix = query.prefix.unwrap_or_default();
    if prefix.len() > 255 {
        return Err(DashboardApiError::BadRequest("job name prefix is too long"));
    }
    // Same as the `name` filter: a NUL is a malformed request, not a 500.
    if prefix.contains('\0') {
        return Err(DashboardApiError::BadRequest(
            "job name prefix must not contain NUL",
        ));
    }
    let names = if prefix.is_empty() {
        Vec::new()
    } else {
        queue
            .database()
            .dashboard_job_names(
                &statuses,
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
    let aborted = queue.abort_job(id, "aborted from dashboard").await?;
    Ok(Json(json!({ "aborted": aborted })).into_response())
}

#[cfg(test)]
mod dashboard_api_tests {
    use super::*;

    #[test]
    fn test_jobs_query_clamps_page_size() {
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
    fn test_cursor_requires_timestamp_and_id() {
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
            ("asset_version", asset_version),
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

/// The fingerprint every `?v=` in the templates carries.
///
/// It covers `PUBLIC_ASSETS` rather than a list of its own: those are exactly
/// the files `/static/` serves with `max-age=3600`, so a file missing from the
/// fingerprint is one browsers keep serving stale for an hour after an upgrade
/// — the failure the versioning exists to prevent, and the one the vendored
/// `pico.min.css` had while it was linked without a `?v=` at all.
///
/// Hashed once rather than per request: the assets are embedded at compile
/// time, so this cannot change while the process runs, and the shell and the
/// login page — the one page an unauthenticated flood reaches — folded every
/// byte of all of them again on every single render.
static ASSET_VERSION: LazyLock<String> = LazyLock::new(|| {
    asset_fingerprint(
        PUBLIC_ASSETS
            .iter()
            .filter_map(|(path, _)| ASSETS.get_file(path))
            .flat_map(|file| file.contents().iter().copied()),
    )
});

fn application_asset_version() -> &'static str {
    &ASSET_VERSION
}

fn render_login(root: &str, error: &str) -> String {
    let asset_version = application_asset_version();
    let root = html_attr_escape(root);
    let error = html_attr_escape(error);
    render_template(
        ASSETS
            .get_file("login.html")
            .and_then(|file| file.contents_utf8())
            .unwrap_or_default(),
        &[
            ("root", root.as_str()),
            ("error", error.as_str()),
            ("asset_version", asset_version),
        ],
    )
}

fn validate_mount_path(path: &str) -> Result<(), Error> {
    let valid_characters = path.bytes().all(|byte| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'-' | b'_' | b'.' | b'~')
    });
    let without_trailing_slashes = path.trim_end_matches('/');
    let valid_segments = without_trailing_slashes.is_empty()
        || without_trailing_slashes
            .strip_prefix('/')
            .is_some_and(|rest| {
                rest.split('/')
                    .all(|segment| !segment.is_empty() && !matches!(segment, "." | ".."))
            });
    if !path.starts_with('/') || path.starts_with("//") || !valid_characters || !valid_segments {
        return Err(Error::Config(
            "dashboard mount_path must be a same-origin absolute path with safe ASCII segments"
                .into(),
        ));
    }
    Ok(())
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

/// The only files `/static/` serves, with the content type each is served as.
///
/// `ASSETS` embeds the whole `assets/` directory, including the HTML templates
/// — which are meant to be reached only through the shell and login routes,
/// rendered and (when configured) behind `require_auth`. `/static/` is mounted
/// outside that layer, so serving the directory wholesale made every file in it
/// a public endpoint of an otherwise authenticated dashboard, and would keep
/// doing so for every file added later. This list makes that exposure a
/// deliberate choice instead.
const PUBLIC_ASSETS: &[(&str, &str)] = &[
    ("app.css", "text/css; charset=utf-8"),
    ("app.js", "application/javascript; charset=utf-8"),
    ("pico.min.css", "text/css; charset=utf-8"),
];

async fn serve_asset(path: axum::extract::Path<String>) -> Response {
    // `and_then` folds the "allowlisted but not embedded" case into the same
    // 404 as any other unknown path rather than leaving an arm nothing can
    // reach.
    let asset = PUBLIC_ASSETS
        .iter()
        .find(|(name, _)| *name == path.as_str())
        .and_then(|(name, content_type)| Some((ASSETS.get_file(name)?, *content_type)));
    let Some((file, content_type)) = asset else {
        return (StatusCode::NOT_FOUND, "not found").into_response();
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
    fn test_asset_fingerprint_is_stable_and_content_sensitive() {
        assert_eq!(
            asset_fingerprint(*b"app"),
            asset_fingerprint(b"app".iter().copied())
        );
        assert_ne!(asset_fingerprint(*b"app"), asset_fingerprint(*b"changed"));
    }

    #[test]
    fn test_render_template_substitutes_each_placeholder_once() {
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
    fn test_render_template_keeps_unterminated_braces() {
        assert_eq!(
            render_template("{root {root} {roots}", &[("root", "/pg")]),
            "{root /pg {roots}"
        );
    }
}

// Authentication

const SESSION_COOKIE_PREFIX: &str = "pgqueue_session_";
const ACTION_HEADER: &str = "x-pgqueue-request";
const ACTION_HEADER_VALUE: &[u8] = b"dashboard";
/// The browser's own statement of where a request came from; see
/// [`is_cross_site_post`].
const SITE_HEADER: &str = "sec-fetch-site";
const SESSION_TTL: Duration = Duration::from_secs(12 * 60 * 60);
const MAX_SESSIONS: usize = 64;
const AUTH_FAILURE_DELAY: Duration = Duration::from_millis(100);
/// How often one credential comparison is handed back to the account, and how
/// many an idle account may spend at once. Together they cap guessing at ten a
/// second sustained — the delay above only ever made *one* guess slow.
const AUTH_ATTEMPT_REFILL: Duration = Duration::from_millis(100);
const AUTH_ATTEMPT_BURST: u32 = 16;
/// How many client/channel budgets are tracked at once. Bounds what a client
/// hopping addresses can make this process allocate; see
/// [`make_room_for_a_bucket`] for what gives way when it is reached.
const MAX_AUTH_CLIENTS: usize = 1_024;
const AUTH_SATURATED_MESSAGE: &str = "too many authentication attempts";

enum CredentialCheck {
    Accepted(Uuid),
    Rejected,
    Saturated,
}

struct DashboardCredentials {
    password: String,
    revision: Uuid,
}

struct DashboardSession {
    expires_at: Instant,
    credential_revision: Uuid,
}

enum SessionCreation {
    Created(String),
    StaleCredentials,
    Unavailable,
}

enum PasswordRotation {
    /// The password changed. `session` is a freshly minted token for the caller
    /// — paired with the surviving expiry it inherited — when the request was
    /// authenticated by a session cookie, and `None` when it came in over HTTP
    /// Basic and so has no session to re-issue.
    Changed {
        session: Option<(String, Instant)>,
    },
    StaleCredentials,
    Unavailable,
}

struct DashboardAuthState {
    username: String,
    credentials: RwLock<DashboardCredentials>,
    sessions: Mutex<HashMap<String, DashboardSession>>,
    throttle: AuthThrottle,
    root: String,
    session_cookie_name: String,
    secure_cookies: bool,
    trusted_proxy_hops: usize,
}

/// Which client a credential comparison is charged to.
///
/// The socket peer, and `X-Forwarded-For` only as far back as
/// [`Dashboard::trusted_proxy_hops`] says the deployment's own proxies reach:
/// the header is otherwise attacker-controlled, so honouring it would let one
/// client mint a fresh budget per request and erase the throttle entirely.
///
/// [`DashboardServer`] serves its router with connection info, so its requests
/// always carry a peer. A [`Dashboard::router`] nested in another application
/// carries one only if that application supplies it (axum's
/// `into_make_service_with_connect_info`), and behind a reverse proxy every peer
/// is the proxy. Requests with no distinguishable peer share [`AuthClient::Any`].
///
/// Splitting the budget by [`AuthChannel`] does *not* rescue that bucket for the
/// login form: `POST /login` is itself [`AuthChannel::Interactive`] and needs no
/// credentials to reach, so `Any`-bucket traffic spends the interactive budget
/// directly. The split only keeps an HTTP Basic flood out of the form. Where
/// every request shares one bucket — behind a proxy, or nested without connect
/// info — a sustained flood of wrong passwords therefore keeps the form refused
/// for everyone, which is what `trusted_proxy_hops` exists to prevent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum AuthClient {
    Peer(std::net::IpAddr),
    Any,
}

/// How the credentials arrived, and so which budget they draw on.
///
/// The two are deliberately separate. Anyone can put an `Authorization` header
/// on an API request, so that is the flood surface; the login form is the only
/// way an operator who holds no session can get in. Sharing one budget meant an
/// unauthenticated flood on the API locked the operator out of the form, which
/// is a denial of service rather than a throttle. Each budget is still the same
/// size, so neither channel is easier to guess at than before.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum AuthChannel {
    /// HTTP Basic credentials on a protected route.
    Basic,
    /// The login form and the password-change endpoint.
    Interactive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct AuthThrottleKey {
    client: AuthClient,
    channel: AuthChannel,
}

/// A token bucket per client and channel over the credential comparisons this
/// dashboard performs.
///
/// [`AUTH_FAILURE_DELAY`] bounds the latency of a single rejection, not the
/// rate of rejections: concurrent guesses were compared as fast as the network
/// could deliver them, because the comparison happened before anything
/// throttled it. Spending budget *first* bounds the rate instead, and — because
/// an exhausted budget refuses the attempt without comparing anything — a
/// correct password is then refused exactly like a wrong one, leaving no
/// 303-versus-429 oracle to guess against.
///
/// The budget is spent before the first `await`, so a client that cancels its
/// request mid-check has already paid for the attempt.
///
/// A comparison that *matches* hands its token straight back. A legitimate
/// client polling the JSON API over HTTP Basic would otherwise throttle itself
/// out of its own dashboard, and an attacker holding the password has nothing
/// left to guess.
///
/// The buckets are keyed because one shared budget is spent by whoever asks
/// most: an unauthenticated flood — which never gets a refund, having nothing
/// that matches — held the only budget at zero and refused the operator's
/// correct password for as long as it ran, with no reset short of a restart.
struct AuthThrottle {
    buckets: Mutex<HashMap<AuthThrottleKey, AuthThrottleTokens>>,
}

struct AuthThrottleTokens {
    available: u32,
    refilled_at: tokio::time::Instant,
}

impl AuthThrottleTokens {
    fn full(now: tokio::time::Instant) -> Self {
        Self {
            available: AUTH_ATTEMPT_BURST,
            refilled_at: now,
        }
    }

    /// The bucket a key arriving into a full map starts with: what the bucket
    /// it displaced had left, if it displaced one that was still spent.
    ///
    /// Never below one comparison, so an operator arriving while an attacker
    /// holds every bucket at zero is still heard — and a correct password hands
    /// that comparison straight back and leaves with a session cookie, which is
    /// not throttled at all.
    fn arriving(now: tokio::time::Instant, displaced: Option<u32>) -> Self {
        match displaced {
            Some(available) => Self {
                available: available.max(1),
                refilled_at: now,
            },
            None => Self::full(now),
        }
    }

    fn refill(&mut self, now: tokio::time::Instant) {
        let elapsed = now.saturating_duration_since(self.refilled_at).as_nanos()
            / AUTH_ATTEMPT_REFILL.as_nanos();
        let headroom = u128::from(AUTH_ATTEMPT_BURST - self.available);
        if elapsed >= headroom {
            // Time past a full bucket is not banked — a client nobody heard from
            // all day is still worth exactly one burst — and resetting the mark
            // keeps the interval arithmetic small.
            self.available = AUTH_ATTEMPT_BURST;
            self.refilled_at = now;
        } else if elapsed > 0 {
            // Still filling, so carry the sub-interval remainder instead of
            // rounding it away on every attempt.
            let refill = elapsed as u32;
            self.available += refill;
            self.refilled_at += AUTH_ATTEMPT_REFILL * refill;
        }
    }
}

impl AuthThrottle {
    fn new() -> Self {
        Self {
            buckets: Mutex::new(HashMap::new()),
        }
    }

    /// Spends one comparison from `key`'s budget, reporting `false` once that
    /// budget is exhausted.
    fn spend(&self, key: AuthThrottleKey) -> bool {
        let Ok(mut buckets) = self.buckets.lock() else {
            // Refuse rather than admit an unmetered attempt: the lock is only
            // ever held across integer arithmetic and map bookkeeping, so
            // poisoning it takes a panic that cannot come from here.
            tracing::error!("dashboard authentication throttle lock poisoned");
            return false;
        };
        let now = tokio::time::Instant::now();
        let displaced = if buckets.contains_key(&key) {
            None
        } else {
            make_room_for_a_bucket(&mut buckets, now)
        };
        let tokens = buckets
            .entry(key)
            .or_insert_with(|| AuthThrottleTokens::arriving(now, displaced));
        tokens.refill(now);
        if tokens.available == 0 {
            return false;
        }
        tokens.available -= 1;
        true
    }

    /// Returns a token spent by a comparison that turned out to match.
    ///
    /// A bucket evicted in between is gone and its refund is dropped:
    /// [`make_room_for_a_bucket`] first drops the buckets that have refilled to
    /// full, which had their token back already, and only then evicts the
    /// fullest *still-spent* one — whose refund is genuinely lost. Losing one
    /// only ever errs toward throttling, and the client's next arrival is
    /// granted a comparison regardless.
    fn refund(&self, key: AuthThrottleKey) {
        let Ok(mut buckets) = self.buckets.lock() else {
            tracing::error!("dashboard authentication throttle lock poisoned");
            return;
        };
        if let Some(tokens) = buckets.get_mut(&key) {
            tokens.available = tokens.available.saturating_add(1).min(AUTH_ATTEMPT_BURST);
        }
    }
}

/// Bounds the map without letting one client's pressure deny another a bucket,
/// reporting what the client that gave way still had left.
///
/// A bucket that has refilled to full is indistinguishable from an absent one,
/// so those go first, and displacing one takes nothing from anybody. When none
/// has refilled, the fullest still-spent bucket gives way and its remaining
/// budget is what the arriving key starts from ([`AuthThrottleTokens::arriving`])
/// rather than a fresh burst — otherwise evicting a *saturated* bucket handed
/// the client being throttled its burst straight back, which is exactly what an
/// attacker filling the map was buying.
///
/// This bounds what cycling identities is worth; it does not make it worthless.
/// An arriving key is always granted one comparison, because refusing it is the
/// operator lockout this keying exists to avoid, so a flood that can mint
/// unlimited identities still buys one guess per identity instead of a burst of
/// [`AUTH_ATTEMPT_BURST`]. Minting them is what [`AuthClient::peer`] makes
/// expensive.
fn make_room_for_a_bucket(
    buckets: &mut HashMap<AuthThrottleKey, AuthThrottleTokens>,
    now: tokio::time::Instant,
) -> Option<u32> {
    if buckets.len() < MAX_AUTH_CLIENTS {
        return None;
    }
    buckets.retain(|_, tokens| {
        tokens.refill(now);
        tokens.available < AUTH_ATTEMPT_BURST
    });
    let mut displaced = None;
    while buckets.len() >= MAX_AUTH_CLIENTS
        && let Some((fullest, available)) = buckets
            .iter()
            .max_by_key(|(_, tokens)| tokens.available)
            .map(|(key, tokens)| (*key, tokens.available))
    {
        buckets.remove(&fullest);
        // Each pass takes the fullest of what is left, so the last one taken is
        // the smallest budget displaced.
        displaced = Some(available);
    }
    displaced
}

impl AuthClient {
    fn of(extensions: &axum::http::Extensions) -> Self {
        extensions
            .get::<axum::extract::ConnectInfo<SocketAddr>>()
            .map(|axum::extract::ConnectInfo(peer)| Self::peer(peer.ip()))
            .unwrap_or(Self::Any)
    }

    /// The client an address names, with IPv6 folded to its /64.
    ///
    /// A single IPv6 subscriber is normally handed a whole /64, so an
    /// individual address is not an identity anyone had to pay for: taking a
    /// fresh one per request costs nothing, and 2^64 of them is enough to cycle
    /// every bucket out of the map for as long as the flood lasts. The /64 is
    /// the smallest unit an attacker cannot mint more of. IPv4 addresses are
    /// scarce and routed one at a time, so they stay whole — folding them to a
    /// /24 would make unrelated customers of one ISP share a budget, which is
    /// the lockout this keying exists to avoid.
    fn peer(address: std::net::IpAddr) -> Self {
        // An IPv4 client arriving on a dual-stack socket is mapped into IPv6
        // (`::ffff:a.b.c.d`); folding *that* to a /64 would put every such
        // client in one bucket.
        match address.to_canonical() {
            std::net::IpAddr::V6(address) => {
                let mut octets = address.octets();
                octets[8..].fill(0);
                Self::Peer(std::net::Ipv6Addr::from(octets).into())
            }
            canonical => Self::Peer(canonical),
        }
    }
}

/// The client `trusted_proxy_hops` hops back along `X-Forwarded-For`, or `None`
/// where the header cannot be trusted to name one.
///
/// Counting from the *right* is what makes this unspoofable: each trusted proxy
/// appends the address it saw, so whatever a client writes into the header
/// itself is pushed one place further left per hop it travels, and the entry
/// this picks is always one of our own proxies' observations. A chain shorter
/// than the configured hops did not come through them all, and an entry that
/// names no address (`unknown`, an obfuscated identifier) names no client, so
/// both fall back to the socket peer rather than to what the header claims.
fn forwarded_client(headers: &HeaderMap, trusted_proxy_hops: usize) -> Option<AuthClient> {
    if trusted_proxy_hops == 0 {
        return None;
    }
    let chain: Vec<&str> = headers
        .get_all("x-forwarded-for")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .collect();
    let entry = chain
        .len()
        .checked_sub(trusted_proxy_hops)
        .and_then(|index| chain.get(index))?;
    forwarded_address(entry).map(AuthClient::peer)
}

/// The address in one `X-Forwarded-For` entry, which proxies write bare, with a
/// port, or bracketed when it is IPv6.
fn forwarded_address(entry: &str) -> Option<std::net::IpAddr> {
    if let Ok(address) = entry.parse::<std::net::IpAddr>() {
        return Some(address);
    }
    if let Ok(address) = entry.parse::<SocketAddr>() {
        return Some(address.ip());
    }
    entry
        .strip_prefix('[')?
        .strip_suffix(']')?
        .parse::<std::net::IpAddr>()
        .ok()
}

impl axum::extract::FromRequestParts<Arc<DashboardAuthState>> for AuthClient {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &Arc<DashboardAuthState>,
    ) -> Result<Self, Self::Rejection> {
        Ok(state.client_of(&parts.headers, &parts.extensions))
    }
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
    fn new(
        username: String,
        password: String,
        root: String,
        secure_cookies: bool,
        trusted_proxy_hops: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            username,
            credentials: RwLock::new(DashboardCredentials {
                password,
                revision: Uuid::now_v7(),
            }),
            sessions: Mutex::new(HashMap::new()),
            throttle: AuthThrottle::new(),
            root,
            session_cookie_name: format!("{SESSION_COOKIE_PREFIX}{}", Uuid::now_v7().simple()),
            secure_cookies,
            trusted_proxy_hops,
        })
    }

    /// Which client this request's credential comparison is charged to.
    fn client_of(&self, headers: &HeaderMap, extensions: &axum::http::Extensions) -> AuthClient {
        forwarded_client(headers, self.trusted_proxy_hops)
            .unwrap_or_else(|| AuthClient::of(extensions))
    }

    fn credentials_match(&self, username: &str, password: &str) -> Option<Uuid> {
        let Ok(credentials) = self.credentials.read() else {
            tracing::error!("dashboard password lock poisoned");
            return None;
        };
        let username_matches = constant_time_eq(username.as_bytes(), self.username.as_bytes());
        let password_matches =
            constant_time_eq(password.as_bytes(), credentials.password.as_bytes());
        (username_matches & password_matches).then_some(credentials.revision)
    }

    fn basic_credentials_match(&self, supplied: &[u8]) -> Option<Uuid> {
        let Ok(credentials) = self.credentials.read() else {
            tracing::error!("dashboard password lock poisoned");
            return None;
        };
        let expected = base64(format!("{}:{}", self.username, credentials.password));
        constant_time_eq(supplied, expected.as_bytes()).then_some(credentials.revision)
    }

    async fn check_credentials(
        &self,
        client: AuthClient,
        username: &str,
        password: &str,
    ) -> CredentialCheck {
        self.check(AuthChannel::Interactive, client, || {
            self.credentials_match(username, password)
        })
        .await
    }

    async fn check_basic_credentials(
        &self,
        client: AuthClient,
        headers: &HeaderMap,
    ) -> CredentialCheck {
        // A header carrying no Basic credential is refused without spending
        // budget: there is nothing to compare, so letting it through would let
        // any anonymous client hold the throttle saturated for everyone else.
        let Some(supplied) = basic_credentials(headers) else {
            return CredentialCheck::Rejected;
        };
        self.check(AuthChannel::Basic, client, || {
            self.basic_credentials_match(supplied)
        })
        .await
    }

    /// Spends one attempt from this client's budget for this channel and, if
    /// there was one to spend, compares the supplied credentials.
    async fn check(
        &self,
        channel: AuthChannel,
        client: AuthClient,
        compare: impl FnOnce() -> Option<Uuid>,
    ) -> CredentialCheck {
        let key = AuthThrottleKey { client, channel };
        if !self.throttle.spend(key) {
            return CredentialCheck::Saturated;
        }
        if let Some(revision) = compare() {
            self.throttle.refund(key);
            return CredentialCheck::Accepted(revision);
        }
        // The delay makes one guess expensive; the budget above is what makes a
        // million of them expensive.
        tokio::time::sleep(AUTH_FAILURE_DELAY).await;
        CredentialCheck::Rejected
    }

    fn create_session(&self, credential_revision: Uuid) -> SessionCreation {
        let token = random_session_token();
        let now = Instant::now();
        let Ok(credentials) = self.credentials.read() else {
            tracing::error!("dashboard password lock poisoned");
            return SessionCreation::Unavailable;
        };
        if credentials.revision != credential_revision {
            return SessionCreation::StaleCredentials;
        }
        let Ok(mut sessions) = self.sessions.lock() else {
            tracing::error!("dashboard session lock poisoned");
            return SessionCreation::Unavailable;
        };
        sessions.retain(|_, session| {
            session.expires_at > now && session.credential_revision == credentials.revision
        });
        if sessions.len() >= MAX_SESSIONS {
            let oldest = sessions
                .iter()
                .min_by_key(|(_, session)| session.expires_at)
                .map(|(token, _)| token.clone());
            if let Some(oldest) = oldest {
                sessions.remove(&oldest);
            }
        }
        sessions.insert(
            token.clone(),
            DashboardSession {
                expires_at: now + SESSION_TTL,
                credential_revision,
            },
        );
        SessionCreation::Created(token)
    }

    fn session_is_valid(&self, headers: &HeaderMap) -> bool {
        let Some(token) = session_token(headers, &self.session_cookie_name) else {
            return false;
        };
        let now = Instant::now();
        let Ok(credentials) = self.credentials.read() else {
            tracing::error!("dashboard password lock poisoned");
            return false;
        };
        let Ok(mut sessions) = self.sessions.lock() else {
            tracing::error!("dashboard session lock poisoned");
            return false;
        };
        sessions.retain(|_, session| {
            session.expires_at > now && session.credential_revision == credentials.revision
        });
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

    fn rotate_password(
        &self,
        expected_revision: Uuid,
        new_password: String,
        headers: &HeaderMap,
    ) -> PasswordRotation {
        let current = session_token(headers, &self.session_cookie_name).map(str::to_owned);
        let Ok(mut credentials) = self.credentials.write() else {
            tracing::error!("dashboard password lock poisoned");
            return PasswordRotation::Unavailable;
        };
        if credentials.revision != expected_revision {
            return PasswordRotation::StaleCredentials;
        }
        let Ok(mut sessions) = self.sessions.lock() else {
            tracing::error!("dashboard session lock poisoned");
            return PasswordRotation::Unavailable;
        };
        let revision = Uuid::now_v7();
        credentials.password = new_password;
        credentials.revision = revision;
        let now = Instant::now();
        // Every token minted under the old password dies, the caller's
        // included: an admin who changes the password because a token may have
        // leaked would otherwise leave the one token worth rotating — the one
        // that crossed the network, in cleartext under `secure_cookies(false)`
        // — valid for the rest of its TTL. The caller gets a fresh token in
        // exchange, expiring when the old one would have, so a rotation neither
        // logs the admin out nor extends their session.
        let expires_at = current
            .as_ref()
            .and_then(|token| sessions.get(token))
            .map(|session| session.expires_at)
            .filter(|expires_at| *expires_at > now);
        sessions.clear();
        let session = expires_at.map(|expires_at| {
            let token = random_session_token();
            sessions.insert(
                token.clone(),
                DashboardSession {
                    expires_at,
                    credential_revision: revision,
                },
            );
            (token, expires_at)
        });
        PasswordRotation::Changed { session }
    }

    fn login_html(&self, error: &str) -> String {
        render_login(&self.root, error)
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

/// The credentials a request supplies over HTTP Basic, if it supplies any.
fn basic_credentials(headers: &HeaderMap) -> Option<&[u8]> {
    // RFC 7617: the auth-scheme token is case-insensitive and is separated from
    // the credentials by one or more spaces.
    let value = headers.get(header::AUTHORIZATION)?.as_bytes();
    let (scheme, rest) = value.split_at_checked(5)?;
    if !scheme.eq_ignore_ascii_case(b"basic") || !rest.starts_with(b" ") {
        return None;
    }
    Some(
        &rest[rest
            .iter()
            .position(|byte| *byte != b' ')
            .unwrap_or(rest.len())..],
    )
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
    let supplied_authorization = request.headers().contains_key(header::AUTHORIZATION);
    if auth.session_is_valid(request.headers()) {
        return next.run(request).await;
    }
    if supplied_authorization {
        let client = auth.client_of(request.headers(), request.extensions());
        match auth
            .check_basic_credentials(client, request.headers())
            .await
        {
            CredentialCheck::Accepted(_) => return next.run(request).await,
            CredentialCheck::Rejected => {}
            CredentialCheck::Saturated => {
                return DashboardApiError::TooManyRequests(AUTH_SATURATED_MESSAGE).into_response();
            }
        }
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
    client: AuthClient,
    headers: HeaderMap,
    Form(form): Form<DashboardLoginForm>,
) -> Response {
    if is_cross_site_post(&headers) {
        return (
            StatusCode::FORBIDDEN,
            Html(auth.login_html("Cross-site login posts are refused.")),
        )
            .into_response();
    }
    let credential_revision = match auth
        .check_credentials(client, &form.username, &form.password)
        .await
    {
        CredentialCheck::Accepted(revision) => revision,
        CredentialCheck::Rejected => {
            return (
                StatusCode::UNAUTHORIZED,
                Html(auth.login_html("Invalid username or password.")),
            )
                .into_response();
        }
        CredentialCheck::Saturated => {
            // This arm answers a browser posting the login form, so it renders
            // the page like every other outcome of that form. The API's JSON
            // body arrived as a bare document with no way back to the form.
            return (
                StatusCode::TOO_MANY_REQUESTS,
                [(header::RETRY_AFTER, "1")],
                Html(auth.login_html("Too many attempts. Try again shortly.")),
            )
                .into_response();
        }
    };
    let token = match auth.create_session(credential_revision) {
        SessionCreation::Created(token) => token,
        SessionCreation::StaleCredentials => {
            return (
                StatusCode::UNAUTHORIZED,
                Html(auth.login_html("Invalid username or password.")),
            )
                .into_response();
        }
        SessionCreation::Unavailable => {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
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
    client: AuthClient,
    headers: HeaderMap,
    Json(change): Json<DashboardPasswordChange>,
) -> Result<Response, DashboardApiError> {
    require_action_header(&headers)?;
    // Characters, as the message says: `len()` counts UTF-8 bytes, so a
    // four-character Latin-1 password — or a three-character CJK one — passed a
    // rule stated in characters. The maximum below stays a byte bound: it is a
    // size guard on what the process stores, not a policy.
    if change.new_password.chars().count() < 8 {
        return Err(DashboardApiError::BadRequest(
            "new password must be at least 8 characters",
        ));
    }
    if change.new_password.len() > 1_024 {
        return Err(DashboardApiError::BadRequest("new password is too long"));
    }
    let credential_revision = match auth
        .check_credentials(client, &auth.username, &change.current_password)
        .await
    {
        CredentialCheck::Accepted(revision) => revision,
        CredentialCheck::Rejected => {
            return Err(DashboardApiError::Forbidden(
                "current password is incorrect",
            ));
        }
        CredentialCheck::Saturated => {
            return Err(DashboardApiError::TooManyRequests(AUTH_SATURATED_MESSAGE));
        }
    };
    match auth.rotate_password(credential_revision, change.new_password, &headers) {
        PasswordRotation::Changed { session } => {
            let mut response = Json(json!({ "changed": true })).into_response();
            if let Some((token, expires_at)) = session {
                // The re-minted session inherits the old one's server-side
                // expiry, so the cookie has to inherit it too. Issuing the full
                // `SESSION_TTL` here left the browser holding a credential the
                // server had already forgotten — up to a whole TTL longer than
                // the rotation intends.
                let cookie = session_cookie_attributes(
                    &format!("{}={token}", auth.session_cookie_name),
                    auth.secure_cookies,
                    auth.cookie_path(),
                    Some(
                        expires_at
                            .saturating_duration_since(Instant::now())
                            .as_secs(),
                    ),
                );
                let Ok(cookie) = HeaderValue::from_str(&cookie) else {
                    tracing::error!("invalid dashboard session cookie");
                    return Ok(StatusCode::INTERNAL_SERVER_ERROR.into_response());
                };
                response.headers_mut().insert(header::SET_COOKIE, cookie);
            }
            Ok(response)
        }
        PasswordRotation::StaleCredentials => Err(DashboardApiError::Forbidden(
            "current password is incorrect",
        )),
        PasswordRotation::Unavailable => Err(DashboardApiError::Internal(Error::Dashboard(
            std::io::Error::other("dashboard authentication state unavailable"),
        ))),
    }
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

/// The session token this request carries, if any.
///
/// Every `Cookie` field line is searched, not just the first: RFC 9113 §8.2.3
/// lets an HTTP/2 client split `cookie` into several field lines, and neither
/// `hyper` nor `h2` rejoins them. The standalone [`DashboardServer`] is
/// HTTP/1.1 only, but [`Dashboard::router`] is documented for nesting into an
/// application that does serve h2 — and there a browser that split its cookies
/// looped login → home → login forever, with `remove_session` and
/// `rotate_password` silently doing nothing.
fn session_token<'a>(headers: &'a HeaderMap, cookie_name: &str) -> Option<&'a str> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|line| line.to_str().ok())
        .flat_map(|line| line.split(';'))
        .map(str::trim)
        .filter_map(|cookie| cookie.split_once('='))
        // Emptiness is part of what makes a cookie *the* session cookie, not a
        // test applied to the first one that happened to match the name: an
        // empty duplicate — the shape a cleared cookie leaves behind, and the
        // shape anyone able to plant one can arrange — used to end the scan and
        // hide the real session sitting further along the same header.
        .find_map(|(name, token)| (name == cookie_name && !token.is_empty()).then_some(token))
}

fn random_session_token() -> String {
    rand::random::<[u8; 32]>()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
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
    fn test_base64_matches_reference_vectors() {
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
    fn test_constant_time_eq_accepts_only_equal_bytes() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn test_session_cookie_security_is_configurable() {
        let secure = session_cookie("cookie", "token", true, "/");
        assert!(secure.contains("; Secure;"));
        let plain_http = session_cookie("cookie", "token", false, "/");
        assert!(!plain_http.contains("; Secure;"));
        assert!(plain_http.contains("; HttpOnly; SameSite=Strict;"));
    }

    #[test]
    fn test_session_cookie_uses_configured_path() {
        let cookie = session_cookie("cookie", "token", true, "/admin");
        assert!(cookie.contains("; Path=/admin;"));
    }

    #[test]
    fn test_dashboard_auth_states_use_distinct_session_cookie_names() {
        let first = test_auth_state();
        let second = test_auth_state();

        assert!(first.session_cookie_name.starts_with(SESSION_COOKIE_PREFIX));
        assert!(
            second
                .session_cookie_name
                .starts_with(SESSION_COOKIE_PREFIX)
        );
        assert_ne!(first.session_cookie_name, second.session_cookie_name);
    }

    #[test]
    fn test_create_session_rejects_validated_credentials_when_password_rotates_before_mint() {
        let auth = test_auth_state();
        let validated_revision = auth.credentials_match("admin", "secret").unwrap();

        // No cookie on the request, so there is no session to re-issue.
        assert!(matches!(
            auth.rotate_password(validated_revision, "new-secret".into(), &HeaderMap::new()),
            PasswordRotation::Changed { session: None }
        ));
        assert!(matches!(
            auth.create_session(validated_revision),
            SessionCreation::StaleCredentials
        ));
    }

    /// The auth state these tests share: secure cookies, mounted at the root,
    /// and trusting no proxy — which is [`Dashboard`]'s default.
    fn test_auth_state() -> Arc<DashboardAuthState> {
        DashboardAuthState::new("admin".into(), "secret".into(), String::new(), true, 0)
    }

    fn test_client(last: u8) -> AuthClient {
        AuthClient::Peer(std::net::IpAddr::from([10, 0, 0, last]))
    }

    fn interactive(client: AuthClient) -> AuthThrottleKey {
        AuthThrottleKey {
            client,
            channel: AuthChannel::Interactive,
        }
    }

    /// Real time, not paused: every assertion below runs in microseconds, while
    /// handing an attempt back takes [`AUTH_ATTEMPT_REFILL`] — and a paused
    /// clock auto-advances past exactly that while a cancelled task settles.
    #[tokio::test]
    async fn test_cancelled_credential_check_still_spends_its_attempt_budget() {
        let auth = test_auth_state();
        let client = test_client(1);
        let attempt_auth = auth.clone();
        let attempt = tokio::spawn(async move {
            attempt_auth
                .check_credentials(client, "admin", "incorrect")
                .await
        });
        tokio::task::yield_now().await;

        assert!(!attempt.is_finished());
        attempt.abort();
        match attempt.await {
            Err(error) => assert!(error.is_cancelled()),
            Ok(_) => panic!("failed credential check completed before cancellation"),
        }
        // The budget is spent before the first await, so cancelling the request
        // that spent it buys nothing: only time hands attempts back.
        for _ in 1..AUTH_ATTEMPT_BURST {
            assert!(auth.throttle.spend(interactive(client)));
        }
        assert!(
            !auth.throttle.spend(interactive(client)),
            "the cancelled attempt kept the budget it spent"
        );
    }

    #[tokio::test]
    async fn test_credential_check_refuses_a_correct_password_when_the_budget_is_spent() {
        let auth = test_auth_state();
        let client = test_client(1);
        // A correct password hands its own attempt straight back, so guessing
        // is what draws the budget down.
        for _ in 0..AUTH_ATTEMPT_BURST {
            assert!(matches!(
                auth.check_credentials(client, "admin", "secret").await,
                CredentialCheck::Accepted(_)
            ));
        }
        for _ in 0..AUTH_ATTEMPT_BURST {
            assert!(auth.throttle.spend(interactive(client)));
        }

        // No comparison happens at all now, so the correct password is refused
        // exactly like a wrong one: the reply says nothing about the guess.
        assert!(matches!(
            auth.check_credentials(client, "admin", "secret").await,
            CredentialCheck::Saturated
        ));
        assert!(matches!(
            auth.check_credentials(client, "admin", "incorrect").await,
            CredentialCheck::Saturated
        ));
    }

    /// The client whose guessing spent a budget is the only one refused: a
    /// shared budget made one flooding client a lockout for everybody, and the
    /// operator's correct password was refused without ever being read.
    #[tokio::test]
    async fn test_credential_check_spends_only_the_budget_of_the_client_that_guessed() {
        let auth = test_auth_state();
        let attacker = test_client(1);
        let operator = test_client(2);

        // Drawn down the way a flood of wrong guesses draws it down, without
        // paying [`AUTH_FAILURE_DELAY`] for each one — which would hand the
        // bucket a refill per guess and never saturate it.
        for _ in 0..AUTH_ATTEMPT_BURST {
            assert!(auth.throttle.spend(interactive(attacker)));
        }
        assert!(matches!(
            auth.check_credentials(attacker, "admin", "secret").await,
            CredentialCheck::Saturated
        ));

        assert!(matches!(
            auth.check_credentials(operator, "admin", "secret").await,
            CredentialCheck::Accepted(_)
        ));
        // And an anonymous flood with no address at all — everything behind one
        // proxy, or a router nested without connection info — cannot spend the
        // budget of a client that has one either.
        for _ in 0..AUTH_ATTEMPT_BURST {
            assert!(auth.throttle.spend(interactive(AuthClient::Any)));
        }
        assert!(matches!(
            auth.check_credentials(AuthClient::Any, "admin", "secret")
                .await,
            CredentialCheck::Saturated
        ));
        assert!(matches!(
            auth.check_credentials(operator, "admin", "secret").await,
            CredentialCheck::Accepted(_)
        ));
    }

    /// Basic-auth traffic is anybody's to send, so it must not be able to spend
    /// the budget the login form needs — the operator's only way in when they
    /// hold no session.
    #[tokio::test]
    async fn test_basic_auth_guessing_leaves_the_login_form_budget_alone() {
        let auth = test_auth_state();
        let client = AuthClient::Any;
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Basic d3Jvbmc6Y3JlZHM="),
        );

        for _ in 0..AUTH_ATTEMPT_BURST {
            assert!(auth.throttle.spend(AuthThrottleKey {
                client,
                channel: AuthChannel::Basic
            }));
        }
        assert!(matches!(
            auth.check_basic_credentials(client, &headers).await,
            CredentialCheck::Saturated
        ));
        assert!(matches!(
            auth.check_credentials(client, "admin", "secret").await,
            CredentialCheck::Accepted(_)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn test_auth_throttle_refills_one_attempt_per_interval() {
        let throttle = AuthThrottle::new();
        let key = interactive(test_client(1));
        for _ in 0..AUTH_ATTEMPT_BURST {
            assert!(throttle.spend(key));
        }
        assert!(!throttle.spend(key));

        tokio::time::advance(AUTH_ATTEMPT_REFILL - Duration::from_millis(1)).await;
        assert!(!throttle.spend(key), "a partial interval refills nothing");
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(throttle.spend(key), "one interval refills one attempt");
        assert!(!throttle.spend(key));

        // An idle client accumulates at most a full burst, however long it
        // waited, and a refund never pushes it past that ceiling either.
        tokio::time::advance(AUTH_ATTEMPT_REFILL * (AUTH_ATTEMPT_BURST + 10)).await;
        throttle.refund(key);
        for _ in 0..AUTH_ATTEMPT_BURST {
            assert!(throttle.spend(key));
        }
        assert!(!throttle.spend(key));
    }

    /// Tracking a bucket per client cannot become a way to make this process
    /// allocate without bound, and the eviction it does instead must not hand a
    /// throttled client its burst back.
    #[tokio::test(start_paused = true)]
    async fn test_auth_throttle_bounds_its_buckets_without_reviving_a_saturated_one() {
        let throttle = AuthThrottle::new();
        let saturated = interactive(AuthClient::Any);
        for _ in 0..AUTH_ATTEMPT_BURST {
            assert!(throttle.spend(saturated));
        }

        for index in 0..MAX_AUTH_CLIENTS as u32 * 2 {
            let key = interactive(AuthClient::Peer(std::net::IpAddr::from(
                index.to_be_bytes(),
            )));
            // Two attempts each, so no bucket is full and eviction has to choose
            // among partly spent ones.
            assert!(throttle.spend(key));
            assert!(throttle.spend(key));
        }

        let tracked = throttle.buckets.lock().unwrap().len();
        assert!(tracked <= MAX_AUTH_CLIENTS, "{tracked} buckets tracked");
        assert!(
            !throttle.spend(saturated),
            "eviction must not refund the client being throttled"
        );
    }

    /// Cycling identities must not be a way to buy budget back. Once every
    /// bucket is spent, a key that displaces one starts from what that client
    /// had left instead of a fresh burst — while still being granted the single
    /// comparison that keeps an operator arriving into a full map from being
    /// locked out.
    #[tokio::test(start_paused = true)]
    async fn test_auth_throttle_grants_no_burst_to_a_client_that_displaced_a_spent_bucket() {
        let throttle = AuthThrottle::new();
        // Every bucket saturated: the state a flood cycling identities creates,
        // where eviction has nothing but spent buckets to choose from.
        for index in 0..MAX_AUTH_CLIENTS as u32 * 2 {
            let key = interactive(AuthClient::Peer(std::net::IpAddr::from(
                index.to_be_bytes(),
            )));
            while throttle.spend(key) {}
        }
        let tracked = throttle.buckets.lock().unwrap().len();
        assert!(tracked <= MAX_AUTH_CLIENTS, "{tracked} buckets tracked");

        let arriving = interactive(test_client(1));
        assert!(
            throttle.spend(arriving),
            "an arriving key must still be heard once"
        );
        assert!(
            !throttle.spend(arriving),
            "displacing a spent bucket must not mint a burst"
        );
        // Time, and only time, hands comparisons back.
        tokio::time::advance(AUTH_ATTEMPT_REFILL).await;
        assert!(throttle.spend(arriving));
        assert!(!throttle.spend(arriving));
    }

    /// An IPv6 client is charged to its /64: the whole prefix is normally one
    /// subscriber's, so addresses within it are free to mint and worthless as
    /// identities. IPv4 stays per-address, including the mapped form a
    /// dual-stack listener reports.
    #[test]
    fn test_auth_client_charges_an_ipv6_client_to_its_prefix() {
        let peer = |address: &str| AuthClient::peer(address.parse().unwrap());
        let folded = AuthClient::Peer("2001:db8:1:2::".parse().unwrap());

        assert_eq!(peer("2001:db8:1:2::1"), folded);
        assert_eq!(peer("2001:db8:1:2:ffff:ffff:ffff:ffff"), folded);
        assert_ne!(
            peer("2001:db8:1:3::1"),
            folded,
            "a different /64 is a different client"
        );
        assert_eq!(
            peer("203.0.113.5"),
            AuthClient::Peer("203.0.113.5".parse().unwrap())
        );
        assert_eq!(
            peer("::ffff:203.0.113.5"),
            AuthClient::Peer("203.0.113.5".parse().unwrap()),
            "a mapped IPv4 client keeps its own budget"
        );
        assert_ne!(peer("::ffff:203.0.113.6"), peer("::ffff:203.0.113.5"));
    }

    #[tokio::test]
    async fn test_credential_check_spends_no_budget_when_no_basic_credential_is_supplied() {
        let auth = test_auth_state();
        let client = test_client(1);
        let mut headers = HeaderMap::new();
        headers.insert(header::AUTHORIZATION, HeaderValue::from_static("Bearer x"));

        for _ in 0..AUTH_ATTEMPT_BURST * 4 {
            assert!(matches!(
                auth.check_basic_credentials(client, &headers).await,
                CredentialCheck::Rejected
            ));
        }
        // A header with nothing to compare cannot be used to hold a budget at
        // zero for the operator who does have credentials.
        for _ in 0..AUTH_ATTEMPT_BURST {
            assert!(auth.throttle.spend(AuthThrottleKey {
                client,
                channel: AuthChannel::Basic
            }));
        }
    }

    /// `X-Forwarded-For` is read from the right, so the entry picked is one our
    /// own proxies wrote and everything a client forges is out of reach. A chain
    /// too short to have passed through them all, an entry naming no address,
    /// and the default hop count of zero all fall back to the socket peer.
    #[test]
    fn test_forwarded_client_reads_the_chain_from_the_trusted_end() {
        let chain = |hops: usize, entries: &[&str]| {
            let mut headers = HeaderMap::new();
            for entry in entries {
                headers.append("x-forwarded-for", HeaderValue::from_str(entry).unwrap());
            }
            forwarded_client(&headers, hops)
        };
        let peer = |address: &str| Some(AuthClient::Peer(address.parse().unwrap()));

        // One proxy: the client is the last entry, whatever it claims to the
        // left of it.
        assert_eq!(chain(1, &["203.0.113.5"]), peer("203.0.113.5"));
        assert_eq!(chain(1, &["1.2.3.4, 203.0.113.5"]), peer("203.0.113.5"));
        assert_eq!(chain(1, &["1.2.3.4", "203.0.113.5"]), peer("203.0.113.5"));
        // Two proxies: one more entry back, and the forged prefix stays out.
        assert_eq!(
            chain(2, &["9.9.9.9, 203.0.113.5, 10.0.0.2"]),
            peer("203.0.113.5")
        );
        // Ports and brackets are how proxies write an address, not a client.
        // A forwarded IPv6 client is charged to its /64, like a socket peer.
        assert_eq!(chain(1, &["203.0.113.5:41234"]), peer("203.0.113.5"));
        assert_eq!(chain(1, &["[2001:db8::1]:41234"]), peer("2001:db8::"));
        assert_eq!(chain(1, &["[2001:db8::1]"]), peer("2001:db8::"));
        assert_eq!(chain(1, &["2001:db8::1"]), peer("2001:db8::"));

        // Nothing trustworthy: too short a chain, an unusable entry, no header,
        // and the default that ignores the header entirely.
        assert_eq!(chain(2, &["203.0.113.5"]), None);
        assert_eq!(chain(1, &["unknown"]), None);
        assert_eq!(chain(1, &["_obfuscated"]), None);
        assert_eq!(chain(1, &[" , "]), None);
        assert_eq!(chain(1, &[]), None);
        assert_eq!(chain(0, &["203.0.113.5"]), None);
    }

    /// The extractor and `require_auth` must agree, and both must honour the
    /// configured hop count rather than the socket peer alone.
    #[test]
    fn test_auth_state_charges_a_forwarded_client_only_when_a_proxy_is_trusted() {
        let mut extensions = axum::http::Extensions::new();
        let peer: SocketAddr = "10.0.0.7:54321".parse().unwrap();
        extensions.insert(axum::extract::ConnectInfo(peer));
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-forwarded-for",
            HeaderValue::from_static("1.2.3.4, 203.0.113.5"),
        );

        assert_eq!(
            test_auth_state().client_of(&headers, &extensions),
            AuthClient::Peer(peer.ip()),
            "the header is attacker-controlled until a proxy is trusted"
        );
        let proxied =
            DashboardAuthState::new("admin".into(), "secret".into(), String::new(), true, 1);
        assert_eq!(
            proxied.client_of(&headers, &extensions),
            AuthClient::Peer("203.0.113.5".parse().unwrap())
        );
        assert_eq!(
            proxied.client_of(&HeaderMap::new(), &extensions),
            AuthClient::Peer(peer.ip()),
            "a request that carried no chain is still charged to its peer"
        );
    }

    #[test]
    fn test_auth_client_is_the_socket_peer_when_the_server_records_one() {
        let mut extensions = axum::http::Extensions::new();
        assert_eq!(AuthClient::of(&extensions), AuthClient::Any);

        let peer: SocketAddr = "10.0.0.7:54321".parse().unwrap();
        extensions.insert(axum::extract::ConnectInfo(peer));
        assert_eq!(
            AuthClient::of(&extensions),
            AuthClient::Peer(peer.ip()),
            "the port changes per connection, so only the address identifies a client"
        );
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
    /// Most recent enqueue, lifecycle update, or completion time.
    pub updated_at: DateTime<Utc>,
}

/// Dashboard list representation without the potentially large job bodies.
#[derive(Debug, Clone, serde::Serialize, sqlx::FromRow)]
pub(crate) struct DashboardJobSummaryRow {
    pub id: Uuid,
    pub dedupe_key: Option<String>,
    pub queue: String,
    pub name: String,
    pub status: JobStatus,
    pub priority: i16,
    pub attempts: i32,
    pub max_attempts: i32,
    pub timeout_ms: Option<i64>,
    pub retry_delay_ms: i64,
    pub backoff: JobRetryBackoff,
    pub result_ttl_ms: Option<i64>,
    pub scheduled_at: DateTime<Utc>,
    pub enqueued_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub touched_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
    pub worker_id: Option<Uuid>,
    pub kind: String,
    pub cron_expr: Option<String>,
    pub updated_at: DateTime<Utc>,
}

/// Flat SQLx record used to assemble the full dashboard job detail.
#[derive(sqlx::FromRow)]
struct DashboardJobRecord {
    id: Uuid,
    dedupe_key: Option<String>,
    queue: String,
    name: String,
    payload: Value,
    status: JobStatus,
    priority: i16,
    attempts: i32,
    max_attempts: i32,
    timeout_ms: Option<i64>,
    retry_delay_ms: i64,
    backoff: JobRetryBackoff,
    result_ttl_ms: Option<i64>,
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
                dedupe_key: row.dedupe_key,
                queue: row.queue,
                name: row.name,
                payload: row.payload,
                status: row.status,
                priority: row.priority,
                attempts: row.attempts,
                max_attempts: row.max_attempts,
                timeout_ms: row.timeout_ms,
                retry_delay_ms: row.retry_delay_ms,
                backoff: row.backoff,
                result_ttl_ms: row.result_ttl_ms,
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
#[derive(Debug, Clone, serde::Serialize, sqlx::FromRow)]
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

/// The queue-signal poll, which every open dashboard runs every 5s per queue.
///
/// The `execution` arm is a single `max()` over `jobs_active_idx` rather than
/// the pair of `EXISTS` probes it reads like. sqlx prepares every statement, so
/// PostgreSQL switches to a *generic* plan whose estimate for
/// `queue = $1 AND status = 'running'` is the table-wide frequency of `running`
/// spread over the average queue: when `running` rows are common elsewhere but
/// absent here, an early-exit sequential scan looks cheap and reads every
/// retained row of the table. `max()` over the partial index is answered by a
/// backward index-only scan with `LIMIT 1`, which is O(1) either way; `'running'
/// > 'aborting'` orders the two correctly, and `max()` of no rows is NULL, so
/// the `ELSE 'idle'` fallback is unchanged.
pub(crate) const DASHBOARD_SIGNALS_SQL: &str = r#"
            SELECT
                $1::text AS name,
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
                (
                    SELECT CASE max(status)
                        WHEN 'running' THEN 'running'
                        WHEN 'aborting' THEN 'aborting'
                        ELSE 'idle'
                    END
                    FROM pgqueue.jobs
                    WHERE queue = $1 AND status IN ('running', 'aborting')
                ) AS execution,
                EXISTS (
                    SELECT 1 FROM pgqueue.workers
                    WHERE queue = $1 AND expires_at > now()
                    LIMIT 1
                ) AS has_live_workers,
                (
                    SELECT completed_at
                    FROM pgqueue.jobs
                    WHERE queue = $1 AND status = 'failed'
                    ORDER BY completed_at DESC, id DESC
                    LIMIT 1
                ) AS latest_failure_at
            "#;

/// The `/health` liveness probe, one per configured queue.
///
/// The `ORDER BY` is load-bearing and has to sit *outside* the existence test,
/// which strips it. `EXISTS (SELECT 1 ... WHERE queue = $1 LIMIT 1)` gives the
/// planner no ordering to satisfy, so under the generic plan sqlx's prepared
/// statement settles into, an early-exit sequential scan costed against
/// average-rows-per-queue wins — and for a queue whose rows are not near the
/// front of the heap it then reads the whole table. `/health` is deliberately
/// unauthenticated: `HEALTH_PROBE_TTL` and `health_gate` bound how often this
/// runs, not what one run costs, and the cost is otherwise linear in retained
/// history, which [`JobRetention::Forever`](crate::JobRetention::Forever) never
/// bounds. Sorted, it is an index-only scan of `jobs_page_idx`.
pub(crate) const HEALTH_PROBE_SQL: &str = r#"
            SELECT count(*) > 0 FROM (
                SELECT 1 FROM pgqueue.jobs
                WHERE queue = $1
                ORDER BY enqueued_at DESC, id DESC
                LIMIT 1
            ) AS probe
            "#;

impl Database {
    pub(crate) async fn dashboard_jobs_page(
        &self,
        statuses: &[JobStatus],
        kind: &str,
        name: Option<&str>,
        cursor: Option<(DateTime<Utc>, Uuid)>,
        limit: i64,
    ) -> Result<Vec<DashboardJobSummaryRow>, Error> {
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
        Ok(sqlx::query_as::<_, DashboardJobSummaryRow>(
            r#"
            WITH keys AS (
                SELECT enqueued_at, id
                FROM pgqueue.job_page_keys(
                    $1, $3, $2::text[], $4::text, NULL::text, $5::timestamptz, $6::uuid, $7
                )
                ORDER BY enqueued_at DESC, id DESC
                LIMIT $7
            )
            SELECT
                jobs.id,
                jobs.dedupe_key,
                jobs.queue,
                jobs.name,
                jobs.status,
                jobs.priority,
                jobs.attempts,
                jobs.max_attempts,
                jobs.timeout_ms,
                jobs.retry_delay_ms,
                jobs.backoff,
                jobs.result_ttl_ms,
                jobs.scheduled_at,
                jobs.enqueued_at,
                jobs.started_at,
                jobs.touched_at,
                jobs.completed_at,
                jobs.expires_at,
                jobs.worker_id,
                jobs.kind,
                jobs.cron_expr,
                GREATEST(
                    jobs.enqueued_at,
                    COALESCE(jobs.touched_at, jobs.enqueued_at),
                    COALESCE(jobs.completed_at, jobs.enqueued_at)
                ) AS updated_at
            FROM keys
            JOIN pgqueue.jobs AS jobs ON jobs.id = keys.id
            ORDER BY keys.enqueued_at DESC, keys.id DESC
            "#,
        )
        .bind(self.name())
        .bind(&statuses)
        .bind(kind)
        .bind(name)
        .bind(cursor_time)
        .bind(cursor_id)
        .bind(limit)
        .fetch_all(self.pool())
        .await?)
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
        Ok(sqlx::query_scalar::<_, String>(
            r#"
            -- The prefix goes to `job_page_keys` as a filter, so it applies
            -- *inside* each status's lateral. Sampling first and filtering
            -- after would only ever suggest names present in the newest `$4`
            -- rows, hiding everything older behind busier jobs.
            SELECT name
            FROM pgqueue.job_page_keys(
                $1, $3, $2::text[], NULL::text, $5::text, NULL::timestamptz, NULL::uuid, $4
            )
            GROUP BY name
            ORDER BY lower(name), name
            LIMIT $6
            "#,
        )
        .bind(self.name())
        .bind(&statuses)
        .bind(kind)
        .bind(sample)
        .bind(prefix)
        .bind(limit)
        .fetch_all(self.pool())
        .await?)
    }

    pub(crate) async fn dashboard_job(&self, id: Uuid) -> Result<Option<DashboardJobRow>, Error> {
        let row = sqlx::query_as::<_, DashboardJobRecord>(
            r#"
            SELECT
                id,
                dedupe_key,
                queue,
                name,
                payload,
                status,
                priority,
                attempts,
                max_attempts,
                timeout_ms,
                retry_delay_ms,
                backoff,
                result_ttl_ms,
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
                ) AS updated_at
            FROM pgqueue.jobs
            WHERE id = $1 AND queue = $2
            "#,
        )
        .bind(id)
        .bind(self.name())
        .fetch_optional(self.pool())
        .await?;

        Ok(row.map(DashboardJobRow::from))
    }

    pub(crate) async fn dashboard_signals(&self) -> Result<DashboardQueueSignals, Error> {
        Ok(
            sqlx::query_as::<_, DashboardQueueSignals>(DASHBOARD_SIGNALS_SQL)
                .bind(self.name())
                .fetch_one(self.pool())
                .await?,
        )
    }

    pub(crate) async fn dashboard_probe(&self) -> Result<(), Error> {
        let _ = sqlx::query_scalar::<_, bool>(HEALTH_PROBE_SQL)
            .bind(self.name())
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
        Ok(sqlx::query_as::<_, WorkerInfo>(
            r#"
            SELECT id, queue, stats, metadata, started_at, heartbeat_at, expires_at
            FROM pgqueue.workers
            WHERE queue = $1
              AND expires_at > now()
              AND ($2::timestamptz IS NULL OR (started_at, id) > ($2, $3))
            ORDER BY started_at, id
            LIMIT $4
            "#,
        )
        .bind(self.name())
        .bind(cursor_time)
        .bind(cursor_id)
        .bind(limit)
        .fetch_all(self.pool())
        .await?)
    }

    pub(crate) async fn dashboard_worker(&self, id: Uuid) -> Result<Option<WorkerInfo>, Error> {
        Ok(sqlx::query_as::<_, WorkerInfo>(
            r#"
            SELECT id, queue, stats, metadata, started_at, heartbeat_at, expires_at
            FROM pgqueue.workers
            WHERE id = $1 AND queue = $2 AND expires_at > now()
            "#,
        )
        .bind(id)
        .bind(self.name())
        .fetch_optional(self.pool())
        .await?)
    }
}
