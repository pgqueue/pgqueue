---
name: loop-review
description: Review and fix uncommitted changes when `$loop-review` is invoked.
---

Review and fix the uncommitted changes using the following procedure.

1. If there are no staged, unstaged, or untracked changes, say so and stop.
2. Start three `issue-finder` subagents concurrently. Before starting each finder, run `scripts/agent start`, which
   returns an eight-character lowercase hexadecimal `<agent-id>`, and include the returned `<agent-id>` in the
   subagent's prompt. If a finder halts because of a platform failure, run `scripts/agent stop <agent-id>` and replace
   it once. If its replacement fails, a subagent otherwise behaves unexpectedly or halts before finishing, or a
   `start`, `stop`, or `apply` command fails, run `scripts/agent clear`, stop the review, and report the problem.
3. As each finder returns, track its findings and run `scripts/agent stop <agent-id>`. Consolidate overlapping findings
   across all cycles, keep the first ID, and combine new evidence. Do not treat a previously fixed or rejected finding
   as new unless new evidence changes it. If the finders return no new findings, go to step 5.
4. Run `scripts/agent start`, then start an `issue-fixer` subagent. Include the returned `<agent-id>` and the new
   findings in the subagent's prompt. After the fixer returns, run `scripts/agent apply <agent-id>`, tell the user
   which findings it fixed or rejected, and go to step 5 if it fixed nothing. Otherwise, repeat from step 2.
5. Run `scripts/agent clear`; if it fails, stop and report the failure. Run `prek run --all-files --stage manual`. Fix
   failures and rerun it until it passes; if a failure cannot be fixed in the checkout, stop and report it. Then report
   the status.
