# AGENTS.md

See [README.md](README.md).

## Design Invariants

- Job input and output must be JSON serializable.
- Delivery is at least once.

## Development

- Do not use `unwrap()` or `expect()` outside tests.
- Use `thiserror` for library errors and `anyhow` for application errors.
- Use runtime `sqlx` query functions, not the compile-time macro variants.
- Every SQL query must have a matching integration test against the Postgres instance in Docker Compose.
- Start dependencies with `docker compose up -d --wait` before developing or testing.
- Prefix database commands with `DATABASE_URL="${DATABASE_URL:-postgres://pgqueue:pgqueue@localhost:5439/pgqueue}"`.
- Run `prek run --all-files --stage manual` after making changes.

## Git

- Do not commit, push, or open pull requests unless explicitly asked.
- Do not add AI attribution to commits or pull requests.
- Use single-line, imperative commit subjects without a trailing period.

## Apply Occam's Razor

- Prefer the simplest design and implementation that fully satisfies the given requirements.
- Do not add abstractions, layers, extensions, or dependencies for hypothetical future needs. Introduce them only when
  concrete requirements or repeated patterns justify their cost.
- Before adding code, consider whether the goal can be met by deleting, consolidating, or reusing existing code.
- When multiple approaches are correct, choose the one with fewer concepts, moving parts, and maintenance obligations,
  unless evidence shows that a more complex approach is necessary.
- Treat patterns and principles as tools, not goals. Do not apply SOLID, design patterns, or architectural boundaries in
  ways that make a small solution more complicated than the problem requires.
