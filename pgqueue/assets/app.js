/* pgqueue dashboard — no-build-step vanilla JS SPA.
 * Routes:  /            queues overview
 *          /queues/:q          queue detail (workers, jobs, crons)
 *          /queues/:q/workers/:id worker detail
 *          /queues/:q/jobs/:id job detail (payload/result, retry/abort)
 * Data refreshes every 5s while visible; navigation uses pushState.
 */
(() => {
  "use strict";

  const ROOT = document.querySelector('meta[name="pgqueue-root"]')?.content || "";
  const DASHBOARD_USER = document.querySelector('meta[name="pgqueue-user"]')?.content || "anonymous";
  const AUTH_ENABLED = document.querySelector('meta[name="pgqueue-auth-enabled"]')?.content === "true";
  const DASHBOARD_TIME_ZONE = Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC";
  const app = document.getElementById("app");
  const REFRESH_MS = 5000;

  // Per-table view state surviving background refreshes and detail navigation.
  const queuesView = { name: "", offset: 0, limit: 25 };
  const cursorView = () => ({
    cursor: null,
    history: [],
    start: 1,
    nextCursor: null,
    pageCount: 0,
    limit: 25,
  });
  const workersView = { queue: null, ...cursorView() };
  const createEntryView = () => ({
    queue: null,
    ...cursorView(),
    statuses: new Set(),
    name: "",
    query: "",
    suggestions: [],
    suggestionIndex: -1,
    suggestionsOpen: false,
  });
  const entries = {
    job: { key: "jobs", view: createEntryView() },
    cron: { key: "crons", view: createEntryView() },
  };
  const activeEntry = () => entries[entryKind];
  const scrollPositions = new Map();
  let entryKind = "job";
  let queueSearchTimer;
  let suggestionTimer;

  const esc = (value) =>
    String(value).replace(/[&<>"']/g, (c) => ({
      "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
    })[c]);

  const requestJson = async (path, options) => {
    const response = await fetch(`${ROOT}/api${path}`, options);
    if (response.status === 401) {
      // The session expired (or was invalidated by a password change):
      // return to the login page instead of rendering an error forever.
      window.location.assign(`${ROOT}/login`);
      return new Promise(() => {});
    }
    if (!response.ok) {
      const body = await response.json().catch(() => ({}));
      throw new Error(body.error || `${response.status} ${response.statusText}`);
    }
    return response.json();
  };

  const api = (path) => requestJson(path);

  const post = (path, payload) => {
    const headers = { "X-Pgqueue-Request": "dashboard" };
    if (payload !== undefined) headers["Content-Type"] = "application/json";
    return requestJson(path, {
      method: "POST",
      headers,
      body: payload === undefined ? undefined : JSON.stringify(payload),
    });
  };

  const compact = (n) => {
    if (n == null) return "–";
    if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
    if (n >= 10_000) return `${(n / 1_000).toFixed(1)}K`;
    return String(n);
  };

  const duration = (ms) => {
    if (ms == null) return "–";
    const s = Math.floor(ms / 1000);
    if (s < 60) return `${s}s`;
    if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`;
    return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
  };

  const dateFormat = new Intl.DateTimeFormat(undefined, {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    timeZone: DASHBOARD_TIME_ZONE,
  });

  /** @type {Intl.DateTimeFormatOptions} */
  const timeFormatOptions = {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23",
    timeZone: DASHBOARD_TIME_ZONE,
  };
  const timeFormat = new Intl.DateTimeFormat(undefined, timeFormatOptions);

  const whenText = (iso) => {
    if (!iso) return "–";
    const date = new Date(iso);
    const parts = Object.fromEntries(
      dateFormat
        .formatToParts(date)
        .filter((part) => part.type !== "literal")
        .map((part) => [part.type, part.value]),
    );
    const timeParts = Object.fromEntries(
      timeFormat
        .formatToParts(date)
        .filter((part) => part.type !== "literal")
        .map((part) => [part.type, part.value]),
    );
    return `${parts.year}-${parts.month}-${parts.day} ${timeParts.hour}:${timeParts.minute}:${timeParts.second}`;
  };

  const when = (iso) => iso
    ? `<time class="timestamp" datetime="${esc(new Date(iso).toISOString())}">${esc(whenText(iso))}</time>`
    : "–";

  const statusBadge = (status) =>
    `<span class="status ${esc(status)}"><span class="dot"></span>${esc(status)}</span>`;

  const jsonBlock = (value) =>
    `<pre class="blob">${value == null ? "–" : esc(JSON.stringify(value, null, 2))}</pre>`;

  const detailRow = (label, value) =>
    `<tr><th scope="row">${label}</th><td>${value}</td></tr>`;

  const link = (href, text) => `<a href="${esc(ROOT + href)}" data-nav>${text}</a>`;

  const breadcrumb = (href, label) => {
    const safeLabel = esc(label);
    return href
      ? `<li><a href="${esc(ROOT + href)}" data-nav title="${safeLabel}">${safeLabel}</a></li>`
      : `<li><span aria-current="page" title="${safeLabel}">${safeLabel}</span></li>`;
  };

  const rowNavAttrs = (href) =>
    `class="clickable-row" data-row-nav="${esc(ROOT + href)}" tabindex="0"`;

  const pageOf = (items, view) => {
    const lastOffset = items.length
      ? Math.floor((items.length - 1) / view.limit) * view.limit
      : 0;
    view.offset = Math.min(view.offset, lastOffset);
    return items.slice(view.offset, view.offset + view.limit);
  };

  const resetCursor = (view) => {
    view.cursor = null;
    view.history = [];
    view.start = 1;
    view.nextCursor = null;
    view.pageCount = 0;
  };

  const appendCursor = (params, cursor, timestampKey) => {
    if (!cursor) return;
    params.set(timestampKey, cursor.timestamp);
    params.set("cursor_id", cursor.id);
  };

  const pagerMarkup = (name, first, last, hasPrevious, hasNext) => {
    return `<div class="pager" data-pager-container="${esc(name)}">
      <span class="pager-summary">Showing ${first}-${last}</span>
      <div class="pager-controls">
        <button type="button" class="outline" data-pager="${esc(name)}" data-page="-1"
                ${hasPrevious ? "" : "disabled"}>Previous</button>
        <button type="button" class="outline" data-pager="${esc(name)}" data-page="1"
                ${hasNext ? "" : "disabled"}>Next</button>
      </div>
    </div>`;
  };

  const accountControls = () => AUTH_ENABLED
    ? `<span class="account-message" role="status" aria-live="polite"></span>
      <details class="account-menu">
        <summary title="Account actions">${esc(DASHBOARD_USER)}</summary>
        <ul>
          <li><button type="button" data-account-action="password">Change password</button></li>
          <li><button type="button" data-account-action="logout">Log out</button></li>
        </ul>
      </details>`
    : `<span title="Signed-in user">${esc(DASHBOARD_USER)}</span>`;

  const passwordDialog = () => AUTH_ENABLED
    ? `<dialog class="account-dialog" id="password-dialog">
        <article>
          <form id="password-form">
            <header><h2>Change password</h2></header>
            <label for="current-password">Current password</label>
            <input id="current-password" name="current_password" type="password"
                   autocomplete="current-password" required>
            <label for="new-password">New password</label>
            <input id="new-password" name="new_password" type="password"
                   autocomplete="new-password" minlength="8" required>
            <label for="confirm-password">Confirm new password</label>
            <input id="confirm-password" name="confirm_password" type="password"
                   autocomplete="new-password" minlength="8" required>
            <p class="form-error" role="alert"></p>
            <footer>
              <button type="button" class="secondary" data-account-action="cancel-password">Cancel</button>
              <button type="submit">Change password</button>
            </footer>
          </form>
        </article>
      </dialog>`
    : "";

  const layout = (crumbs, body, pageClass = "") => `
    <header class="app-header">
      <nav class="breadcrumbs" aria-label="Breadcrumb">
        <ol>${crumbs.join("")}</ol>
      </nav>
      <div class="nav-context" aria-label="Dashboard context">
        ${accountControls()}
      </div>
    </header>
    ${passwordDialog()}
    <div class="page-content ${esc(pageClass)}">${body}</div>`;

  /* ------------------------------- views -------------------------------- */

  const signalBadge = (value, tone = "") =>
    `<span class="signal${tone ? ` ${esc(tone)}` : ""}">${esc(value)}</span>`;

  const executionTone = (execution) => execution === "running"
    ? "running"
    : execution === "aborting" ? "aborted" : "";

  const homeView = async () => {
    const { queues } = await api("/queues");
    const queueNeedle = queuesView.name.trim().toLowerCase();
    const filteredQueues = queueNeedle
      ? queues.filter((queue) => queue.name.toLowerCase().includes(queueNeedle))
      : queues;
    const visibleQueues = pageOf(filteredQueues, queuesView);
    const rows = visibleQueues
      .map(
        (q) => `<tr ${rowNavAttrs(`/queues/${encodeURIComponent(q.name)}`)}>
          <td>${link(`/queues/${encodeURIComponent(q.name)}`, esc(q.name))}</td>
          <td>${q.oldest_ready_at ? when(q.oldest_ready_at) : signalBadge("Idle")}</td>
          <td class="queue-state">${signalBadge(q.execution, executionTone(q.execution))}</td>
          <td>${q.next_scheduled_at ? when(q.next_scheduled_at) : "–"}</td>
          <td class="queue-state">${signalBadge(q.has_live_workers ? "Online" : "None", q.has_live_workers ? "complete" : "failed")}</td>
          <td>${q.latest_failure_at ? when(q.latest_failure_at) : "–"}</td>
        </tr>`,
      )
      .join("");
    return layout(
      [breadcrumb(null, "PGQUEUE")],
      `<section class="table-section queues-section">
        <h2>Queues</h2>
        <form class="table-search search-filter" aria-label="Search queues">
          <div class="search-field">
            <input type="search" id="queue-name-filter" placeholder="Search queue names"
                   aria-label="Search queues by name" autocomplete="off" spellcheck="false"
                   value="${esc(queuesView.name)}">
          </div>
        </form>
        <div class="table-frame">
          <div class="table-scroll" data-scroll-key="queues"><table class="data-table">
            <thead><tr><th>Name</th><th>Ready since</th><th>Execution</th>
            <th>Next scheduled</th><th>Workers</th><th>Latest failure</th></tr></thead>
            <tbody>${rows || '<tr><td colspan="6">No matching queues.</td></tr>'}</tbody>
          </table></div>
          ${pagerMarkup(
            "queues",
            visibleQueues.length ? queuesView.offset + 1 : 0,
            queuesView.offset + visibleQueues.length,
            queuesView.offset > 0,
            queuesView.offset + visibleQueues.length < filteredQueues.length,
          )}
        </div>
      </section>`,
      "home-page",
    );
  };

  const STATUSES = ["queued", "running", "complete", "failed", "aborting", "aborted"];

  const queueView = async (name) => {
    const { view: entryView, key: entryKey } = activeEntry();
    if (entryView.queue !== name) {
      resetEntryView(entryView);
      entryView.queue = name;
    }
    const params = new URLSearchParams();
    if (entryView.statuses.size) params.set("status", [...entryView.statuses].join(","));
    if (entryView.name) params.set("name", entryView.name);
    params.set("kind", entryKind);
    params.set("limit", entryView.limit);
    appendCursor(params, entryView.cursor, "cursor_enqueued_at");

    if (workersView.queue !== name) {
      workersView.queue = name;
      resetCursor(workersView);
    }
    const workerParams = new URLSearchParams({ limit: String(workersView.limit) });
    appendCursor(workerParams, workersView.cursor, "cursor_started_at");

    const [workerPage, jobPage] = await Promise.all([
      api(`/queues/${encodeURIComponent(name)}/workers?${workerParams}`),
      api(`/queues/${encodeURIComponent(name)}/jobs?${params}`),
    ]);
    const { workers } = workerPage;
    const { jobs } = jobPage;
    workersView.nextCursor = workerPage.next_cursor
      ? { timestamp: workerPage.next_cursor.started_at, id: workerPage.next_cursor.id }
      : null;
    workersView.pageCount = workers.length;
    entryView.nextCursor = jobPage.next_cursor
      ? { timestamp: jobPage.next_cursor.enqueued_at, id: jobPage.next_cursor.id }
      : null;
    entryView.pageCount = jobs.length;

    const workerRows = workers
      .map(
        (w) => `<tr ${rowNavAttrs(`/queues/${encodeURIComponent(name)}/workers/${w.id}`)}>
          <td class="mono job-id">${link(
            `/queues/${encodeURIComponent(name)}/workers/${w.id}`,
            esc(w.id),
          )}</td>
          <td class="num">${esc(compact(w.stats?.complete))}</td>
          <td class="num">${esc(compact(w.stats?.retried))}</td>
          <td class="num">${esc(compact(w.stats?.failed))}</td>
          <td class="num">${esc(compact(w.stats?.aborted))}</td>
          <td class="num">${esc(duration(w.stats?.uptime_ms))}</td>
        </tr>`,
      )
      .join("");

    const tabs = STATUSES.map((status) => {
      const selected = entryView.statuses.has(status);
      return `<button type="button" data-status="${status}" aria-pressed="${selected}"><span class="status ${esc(status)}"><span class="dot"></span>${esc(status)}</span></button>`;
    }).join("");

    const suggestions = entryView.suggestionsOpen && entryView.suggestions.length
      ? `<ul class="name-suggestions" id="job-name-suggestions" role="listbox">${entryView.suggestions.map((suggestion, index) =>
        `<li><button type="button" role="option" data-job-name="${esc(suggestion)}" aria-selected="${index === entryView.suggestionIndex}">${esc(suggestion)}</button></li>`,
      ).join("")}</ul>`
      : "";

    const rows = jobs
      .map(
        (j) => `<tr ${rowNavAttrs(`/queues/${encodeURIComponent(name)}/jobs/${j.id}`)}>
          <td class="mono job-id">${link(
            `/queues/${encodeURIComponent(name)}/jobs/${j.id}`,
            esc(j.id),
          )}</td>
          <td class="job-name"><span title="${esc(j.name)}">${esc(j.name)}</span></td>
          ${entryKind === "cron" ? `<td class="mono cron-expression">${esc(j.cron_expr ?? "–")}</td>` : ""}
          <td><button type="button" class="status status-chip ${esc(j.status)}" data-status="${esc(j.status)}" title="Filter by ${esc(j.status)}"><span class="dot"></span>${esc(j.status)}</button></td>
          <td class="num">${j.attempts}/${j.max_attempts}</td>
          <td>${when(j.scheduled_at)}</td>
          <td>${when(j.completed_at)}</td>
        </tr>`,
      )
      .join("");

    return layout(
      [breadcrumb("/", "PGQUEUE"), breadcrumb(null, `Queue ${name}`)],
      `<section class="table-section workers-section">
        <h2>Workers</h2>
        <div class="table-frame">
          <div class="table-scroll" data-scroll-key="workers"><table class="data-table">
            <thead><tr><th>Worker</th><th class="num">Complete</th><th class="num">Retried</th>
            <th class="num">Failed</th><th class="num">Aborted</th><th class="num">Uptime</th></tr></thead>
            <tbody>${workerRows || '<tr class="empty-row"><td colspan="6">No results found.</td></tr>'}</tbody>
          </table></div>
          ${pagerMarkup(
            "workers",
            workers.length ? workersView.start : 0,
            workers.length ? workersView.start + workers.length - 1 : 0,
            workersView.history.length > 0,
            Boolean(workersView.nextCursor),
          )}
        </div>
      </section>
      <section class="jobs-section">
        <h2>Jobs</h2>
        <div class="job-toolbar">
          <div class="segmented kind-tabs" role="tablist" aria-label="Queue entries">
            <button type="button" role="tab" data-kind="job" aria-selected="${entryKind === "job"}">One-Off</button>
            <button type="button" role="tab" data-kind="cron" aria-selected="${entryKind === "cron"}">Cron</button>
          </div>
          <div class="filter-group">
            <div class="segmented" role="group" aria-label="Filter jobs by status">${tabs}</div>
          </div>
          <form class="search-filter job-name-search" aria-label="Search by job name">
            <div class="search-field">
              <input type="search" id="${entryKey.slice(0, -1)}-name-filter" placeholder="Search by job name"
                     aria-label="Search by job name" autocomplete="off" spellcheck="false"
                     aria-controls="job-name-suggestions" aria-expanded="${entryView.suggestionsOpen && entryView.suggestions.length > 0}"
                     value="${esc(entryView.query)}">
            </div>
            ${suggestions}
          </form>
        </div>
        <div class="table-frame">
          <div class="table-scroll" data-scroll-key="${entryKey}"><table class="data-table">
            <thead><tr><th>ID</th><th>Name</th>${entryKind === "cron" ? "<th>Schedule</th>" : ""}<th>Status</th><th class="num">Attempts</th>
            <th>${entryKind === "cron" ? "Next run" : "Scheduled"}</th><th>Completed</th></tr></thead>
            <tbody>${rows || `<tr><td colspan="${entryKind === "cron" ? 7 : 6}">No jobs found</td></tr>`}</tbody>
          </table></div>
          ${pagerMarkup(
            entryKey,
            jobs.length ? entryView.start : 0,
            jobs.length ? entryView.start + jobs.length - 1 : 0,
            entryView.history.length > 0,
            Boolean(entryView.nextCursor),
          )}
        </div>
      </section>`,
      "queue-page",
    );
  };

  const workerView = async (name, id) => {
    const { worker } = await api(`/queues/${encodeURIComponent(name)}/workers/${id}`);

    return layout(
      [
        breadcrumb("/", "PGQUEUE"),
        breadcrumb(`/queues/${encodeURIComponent(name)}`, `Queue ${name}`),
        breadcrumb(null, `Worker ${worker.id}`),
      ],
      `<div class="detail-heading"><h2>Worker ${esc(worker.id)}</h2></div>
      <div class="table-scroll" data-scroll-key="worker-details"><table class="data-table">
        ${detailRow("ID", `<span class="mono">${esc(worker.id)}</span>`)}
        ${detailRow("Queue", esc(worker.queue))}
        ${detailRow("Complete", esc(compact(worker.stats?.complete)))}
        ${detailRow("Retried", esc(compact(worker.stats?.retried)))}
        ${detailRow("Failed", esc(compact(worker.stats?.failed)))}
        ${detailRow("Aborted", esc(compact(worker.stats?.aborted)))}
        ${detailRow("Uptime", esc(duration(worker.stats?.uptime_ms)))}
        ${detailRow("Started", when(worker.started_at))}
        ${detailRow("Last heartbeat", when(worker.heartbeat_at))}
        ${detailRow("Expires", when(worker.expires_at))}
        ${detailRow("Metadata", jsonBlock(worker.metadata))}
      </table></div>`,
      "detail-page",
    );
  };

  const jobView = async (name, id) => {
    const { job, cron_description: cronDescription } = await api(`/queues/${encodeURIComponent(name)}/jobs/${id}`);
    const isCron = job.kind === "cron";
    const detailLabel = isCron ? "Cron" : "Job";
    const terminal = ["complete", "failed", "aborted"].includes(job.status);
    const abortable = ["queued", "running"].includes(job.status);

    return layout(
      [
        breadcrumb("/", "PGQUEUE"),
        breadcrumb(`/queues/${encodeURIComponent(name)}`, `Queue ${name}`),
        breadcrumb(null, `${detailLabel} ${job.id}`),
      ],
      `<div class="detail-heading">
        <h2>${detailLabel} ${esc(job.id)}</h2>
        <div class="actions">
          <button type="button" data-action="retry" ${terminal ? "" : "disabled"}>Retry</button>
          <button type="button" class="secondary" data-action="abort" ${abortable ? "" : "disabled"}>Abort</button>
        </div>
      </div>
      <div class="table-scroll" data-scroll-key="job-details"><table class="data-table">
        ${detailRow("ID", `<span class="mono">${esc(job.id)}</span>`)}
        ${detailRow("Name", esc(job.name))}
        ${isCron ? detailRow("Schedule", `<div class="cron-schedule-detail"><span class="mono cron-expression">${esc(job.cron_expr ?? "–")}</span><span class="cron-description">${esc(cronDescription ?? "Schedule description unavailable.")}</span></div>`) : ""}
        ${detailRow("Status", statusBadge(job.status))}
        ${detailRow("Attempts", `${job.attempts}/${job.max_attempts}`)}
        ${detailRow("Priority", job.priority)}
        ${detailRow("Unique key", esc(job.unique_key ?? "–"))}
        ${detailRow("Group", esc(job.group_key ?? "–"))}
        ${detailRow("Enqueued", when(job.enqueued_at))}
        ${detailRow("Scheduled", when(job.scheduled_at))}
        ${detailRow("Started", when(job.started_at))}
        ${detailRow("Completed", when(job.completed_at))}
        ${detailRow("Last updated", when(job.updated_at))}
        ${detailRow("Worker", `<span class="mono">${esc(job.worker_id ?? "–")}</span>`)}
        ${detailRow("Error", job.error ? `<span class="error-banner">${esc(job.error)}</span>` : "–")}
        ${detailRow("Payload", jsonBlock(job.payload))}
        ${detailRow("Result", jsonBlock(job.result))}
        ${detailRow("Metadata", jsonBlock(job.meta))}
      </table></div>`,
      "detail-page",
    );
  };

  const errorView = (error) =>
    layout(
      [breadcrumb("/", "PGQUEUE"), breadcrumb(null, "Error")],
      `<article class="error-banner">${esc(error.message || error)}</article>`,
    );

  /* ----------------------------- router --------------------------------- */

  const route = () => {
    let path = location.pathname;
    if (ROOT && path.startsWith(ROOT)) path = path.slice(ROOT.length) || "/";
    const workerMatch = path.match(/^\/queues\/([^/]+)\/workers\/([^/]+)$/);
    if (workerMatch) {
      const queue = decodeURIComponent(workerMatch[1]);
      const id = workerMatch[2];
      return { render: () => workerView(queue, id), queue, id };
    }
    const jobMatch = path.match(/^\/queues\/([^/]+)\/jobs\/([^/]+)$/);
    if (jobMatch) {
      const queue = decodeURIComponent(jobMatch[1]);
      const id = jobMatch[2];
      return { render: () => jobView(queue, id), queue, id };
    }
    const queueMatch = path.match(/^\/queues\/([^/]+)$/);
    if (queueMatch) {
      const queue = decodeURIComponent(queueMatch[1]);
      return { render: () => queueView(queue), queue, id: null };
    }
    return { render: homeView, queue: null, id: null };
  };

  let rendering = false;
  let renderRequested = false;
  let lastMarkup = null;
  const render = async () => {
    // Renders are deferred, not dropped, while the password dialog is open:
    // the URL may change underneath it (popstate), so the view re-syncs as
    // soon as the dialog closes.
    if (app.querySelector(".account-dialog[open]")) {
      renderRequested = true;
      return;
    }
    if (rendering) {
      renderRequested = true;
      return;
    }
    rendering = true;
    try {
      const nextMarkup = await route().render();
      if (app.querySelector(".account-dialog[open]")) {
        renderRequested = true;
        return;
      }
      if (nextMarkup !== lastMarkup) {
        // Capture live UI state after the request: the user may have typed,
        // moved focus, or scrolled while data was in flight.
        const active = document.activeElement;
        const focusId = active?.id;
        const selectionStart = active?.selectionStart;
        const selectionEnd = active?.selectionEnd;
        app.querySelectorAll("[data-scroll-key]").forEach((element) => {
          scrollPositions.set(element.dataset.scrollKey, {
            left: element.scrollLeft,
            top: element.scrollTop,
          });
        });
        app.innerHTML = nextMarkup;
        lastMarkup = nextMarkup;
        app.querySelectorAll("[data-scroll-key]").forEach((element) => {
          const position = scrollPositions.get(element.dataset.scrollKey);
          if (position?.left) element.scrollLeft = position.left;
          if (position?.top) element.scrollTop = position.top;
        });
        const nextActive = focusId ? document.getElementById(focusId) : null;
        if (nextActive) {
          nextActive.focus({ preventScroll: true });
          if (selectionStart != null && selectionEnd != null) {
            nextActive.setSelectionRange(selectionStart, selectionEnd);
          }
        }
      }
    } catch (error) {
      app.innerHTML = errorView(error);
      lastMarkup = null;
    } finally {
      rendering = false;
      if (renderRequested) {
        renderRequested = false;
        void render();
      }
    }
  };

  const resetEntryView = (view) => {
    view.statuses.clear();
    view.name = "";
    view.query = "";
    view.suggestions = [];
    view.suggestionIndex = -1;
    view.suggestionsOpen = false;
    resetCursor(view);
  };

  // Per-table view state deliberately survives navigation (see the header
  // comment): filters, search, and cursors reset only when the queue changes,
  // which queueView handles itself.
  const navigate = (url) => {
    history.pushState(null, "", url);
    void render();
  };

  const resetTableScroll = (name) => {
    scrollPositions.set(name, { left: 0, top: 0 });
    const table = app.querySelector(`[data-scroll-key="${name}"]`);
    if (table) {
      table.scrollLeft = 0;
      table.scrollTop = 0;
    }
  };

  const chooseJobName = (view, key, name) => {
    view.name = name.trim();
    view.query = view.name;
    view.suggestions = [];
    view.suggestionIndex = -1;
    view.suggestionsOpen = false;
    resetCursor(view);
    resetTableScroll(key);
    void render();
  };

  const paintSuggestionSelection = (view) => {
    app.querySelectorAll(".name-suggestions [role=option]").forEach((option, index) => {
      option.setAttribute("aria-selected", String(index === view.suggestionIndex));
    });
  };

  const loadJobNameSuggestions = async (view, kind) => {
    const prefix = view.query.trim();
    const requestId = (view.suggestionRequest ?? 0) + 1;
    view.suggestionRequest = requestId;
    if (!prefix) {
      view.suggestions = [];
      view.suggestionsOpen = false;
      void render();
      return;
    }
    const { queue } = route();
    if (!queue) return;
    const params = new URLSearchParams({ kind, prefix });
    try {
      const { names } = await api(
        `/queues/${encodeURIComponent(queue)}/job-names?${params}`,
      );
      if (view.suggestionRequest !== requestId || view.query.trim() !== prefix) return;
      view.suggestions = names;
      view.suggestionIndex = -1;
      view.suggestionsOpen = true;
      void render();
    } catch {
      if (view.suggestionRequest !== requestId) return;
      view.suggestions = [];
      view.suggestionsOpen = false;
      void render();
    }
  };

  app.addEventListener("click", async (event) => {
    const accountAction = event.target.closest("button[data-account-action]");
    if (accountAction) {
      event.preventDefault();
      const dialog = app.querySelector("#password-dialog");
      if (accountAction.dataset.accountAction === "password") {
        app.querySelector(".account-menu")?.removeAttribute("open");
        dialog?.querySelector("form")?.reset();
        const error = dialog?.querySelector(".form-error");
        if (error) error.textContent = "";
        dialog?.showModal();
      } else if (accountAction.dataset.accountAction === "cancel-password") {
        dialog?.close();
      } else if (accountAction.dataset.accountAction === "logout") {
        accountAction.disabled = true;
        try {
          await post("/account/logout");
          window.location.assign(`${ROOT}/login`);
        } catch (error) {
          accountAction.disabled = false;
          const status = app.querySelector(".account-message");
          if (status) {
            status.classList.add("error");
            status.textContent = error.message;
          }
        }
      }
      return;
    }
    const nav = event.target.closest("a[data-nav]");
    if (nav) {
      event.preventDefault();
      navigate(nav.getAttribute("href"));
      return;
    }
    const nameOption = event.target.closest("button[data-job-name]");
    if (nameOption) {
      event.preventDefault();
      const { view, key } = activeEntry();
      chooseJobName(view, key, nameOption.dataset.jobName);
      return;
    }
    const tab = event.target.closest("button[data-status]");
    if (tab) {
      event.preventDefault();
      const { view: entryView, key: entryKey } = activeEntry();
      const { status } = tab.dataset;
      if (entryView.statuses.has(status)) {
        entryView.statuses.delete(status);
      } else {
        entryView.statuses.add(status);
      }
      resetCursor(entryView);
      resetTableScroll(entryKey);
      void render();
      return;
    }
    const kindTab = event.target.closest("button[data-kind]");
    if (kindTab) {
      event.preventDefault();
      entryKind = kindTab.dataset.kind;
      void render();
      return;
    }
    const rowNav = event.target.closest("tr[data-row-nav]");
    if (rowNav) {
      navigate(rowNav.dataset.rowNav);
      return;
    }
    const pagerButton = event.target.closest("button[data-page]");
    if (pagerButton && !pagerButton.disabled) {
      event.preventDefault();
      const name = pagerButton.dataset.pager;
      const direction = Number(pagerButton.dataset.page);
      if (name === "queues") {
        queuesView.offset = Math.max(0, queuesView.offset + direction * queuesView.limit);
      } else {
        const view = { workers: workersView, jobs: entries.job.view, crons: entries.cron.view }[name];
        if (!view) return;
        if (direction > 0 && view.nextCursor) {
          view.history.push({ cursor: view.cursor, start: view.start });
          view.cursor = view.nextCursor;
          view.start += view.pageCount;
        } else if (direction < 0 && view.history.length) {
          const previous = view.history.pop();
          view.cursor = previous.cursor;
          view.start = previous.start;
        }
      }
      resetTableScroll(pagerButton.dataset.pager);
      void render();
      return;
    }
    const action = event.target.closest("button[data-action]");
    if (action && !action.disabled) {
      event.preventDefault();
      const { queue, id } = route();
      if (!queue || !id) return;
      try {
        const result = await post(
          `/queues/${encodeURIComponent(queue)}/jobs/${id}/${action.dataset.action}`,
        );
        if (action.dataset.action === "retry" && result.job_id) {
          navigate(`${ROOT}/queues/${encodeURIComponent(queue)}/jobs/${result.job_id}`);
        } else {
          void render();
        }
      } catch (error) {
        app.innerHTML = errorView(error);
        lastMarkup = null;
      }
    }
  });

  app.addEventListener("submit", async (event) => {
    if (event.target.matches("#password-form")) {
      event.preventDefault();
      const form = event.target;
      const error = form.querySelector(".form-error");
      const currentPassword = form.elements.current_password.value;
      const newPassword = form.elements.new_password.value;
      const confirmPassword = form.querySelector('[name="confirm_password"]');
      if (newPassword !== confirmPassword.value) {
        error.textContent = "New passwords do not match.";
        return;
      }
      const submit = form.querySelector('button[type="submit"]');
      submit.disabled = true;
      error.textContent = "";
      try {
        await post("/account/password", {
          current_password: currentPassword,
          new_password: newPassword,
        });
        form.reset();
        app.querySelector("#password-dialog")?.close();
        const status = app.querySelector(".account-message");
        if (status) {
          status.classList.remove("error");
          status.textContent = "Password changed";
        }
      } catch (submitError) {
        error.textContent = submitError.message;
      } finally {
        submit.disabled = false;
      }
      return;
    }
    if (!event.target.matches(".search-filter")) return;
    event.preventDefault();
    if (!event.target.matches(".job-name-search")) return;
    const { view, key } = activeEntry();
    const selected = view.suggestions[view.suggestionIndex];
    chooseJobName(view, key, selected ?? view.query);
  });

  app.addEventListener("keydown", (event) => {
    if (event.target.matches("#job-name-filter, #cron-name-filter")) {
      const view = entries[event.target.id === "cron-name-filter" ? "cron" : "job"].view;
      if (["ArrowDown", "ArrowUp"].includes(event.key) && view.suggestions.length) {
        event.preventDefault();
        const delta = event.key === "ArrowDown" ? 1 : -1;
        view.suggestionIndex = Math.max(
          -1,
          Math.min(view.suggestions.length - 1, view.suggestionIndex + delta),
        );
        view.suggestionsOpen = true;
        paintSuggestionSelection(view);
        return;
      }
      if (event.key === "Escape" && view.suggestionsOpen) {
        event.preventDefault();
        view.suggestionsOpen = false;
        app.querySelector(".name-suggestions")?.remove();
        event.target.setAttribute("aria-expanded", "false");
        return;
      }
    }
    const rowNav = event.target.closest("tr[data-row-nav]");
    if (rowNav && event.target === rowNav && ["Enter", " "].includes(event.key)) {
      event.preventDefault();
      navigate(rowNav.dataset.rowNav);
    }
  });

  app.addEventListener("input", (event) => {
    if (event.target.id === "queue-name-filter") {
      queuesView.name = event.target.value;
      queuesView.offset = 0;
      resetTableScroll("queues");
      clearTimeout(queueSearchTimer);
      queueSearchTimer = setTimeout(render, 250);
    }
    const kind = { "job-name-filter": "job", "cron-name-filter": "cron" }[event.target.id];
    if (kind) {
      const { view, key } = entries[kind];
      view.query = event.target.value;
      if (view.query !== view.name) view.name = "";
      resetCursor(view);
      resetTableScroll(key);
      clearTimeout(suggestionTimer);
      suggestionTimer = setTimeout(() => loadJobNameSuggestions(view, kind), 250);
    }
  });

  document.addEventListener("click", (event) => {
    const accountMenu = app.querySelector(".account-menu[open]");
    if (accountMenu && !event.target.closest(".account-menu")) {
      accountMenu.removeAttribute("open");
    }
    if (event.target.closest(".job-name-search")) return;
    for (const { view } of Object.values(entries)) view.suggestionsOpen = false;
    app.querySelector(".name-suggestions")?.remove();
  });

  // `close` does not bubble, so catch it in the capture phase to flush any
  // render deferred while the password dialog was open.
  app.addEventListener(
    "close",
    (event) => {
      if (event.target.matches?.(".account-dialog") && renderRequested) {
        renderRequested = false;
        void render();
      }
    },
    true,
  );

  window.addEventListener("popstate", () => {
    void render();
  });
  setInterval(() => {
    if (document.visibilityState === "visible") void render();
  }, REFRESH_MS);
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") void render();
  });
  void render();
})();
