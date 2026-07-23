# AGENTS.md

## Overview

See [README.md](README.md).

## Coding Rules

- Do not use `unwrap()` or `expect()` outside tests.
- Use shared helpers from pgqueue/tests/integration.rs or pgqueue::__private instead of reimplementing them in tests.
- Use SQLx's compile-time `query!`, `query_as!`, and `query_scalar!` macros for application queries.
- Keep query SQL literal and use bind parameters for every value. Never interpolate SQL.
- Manage the fixed `pgqueue` schema through SQLx migrations in `pgqueue/migrations`.
- Validate queries against the migrated Docker Compose PostgreSQL 18 database and keep `pgqueue/.sqlx` current for offline and downstream builds.
- Generate database-managed timestamps with SQL `now()` instead of using client time.
- Use `JobError` when only a job attempt fails. Use `Error` when the queue or worker fails.
- Document user-facing APIs with Rustdoc comments (///) and relevant doctests.
- Every SQL query must have a matching integration test against real Postgres.
- Aim for 100% test coverage. Explicitly mark genuinely unreachable arms.
- Name tests `<subject>_<behavior>[_when_<condition>]` in snake case, so the tested subject and expected behavior are clear from the test list (for example, `apply_times_out_when_nothing_processes`).
- After making changes, run `prek run --all-files`.
