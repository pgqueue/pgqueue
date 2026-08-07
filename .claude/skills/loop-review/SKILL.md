---
name: loop-review
description: Review and fix uncommitted changes using subagents running in a loop.
disable-model-invocation: true
disallowed-tools: AskUserQuestion
---

Start a reviewer subagent (Opus 5, max effort) with fresh, unbiased context to rate the uncommitted changes on a
10-point scale and identify issues. Then start a separate fixer subagent (Opus 5, xhigh effort), also with fresh,
unbiased context, to address only critical-, high-, or medium-severity issues. Repeat with fresh subagents until the
reviewer finds no such issues and assigns a rating of at least 8.5/10.
