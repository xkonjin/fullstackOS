---
name: budget-check
description: "Estimate tool-call budget for a pipeline plan. Warns if plan exceeds single-session budget and auto-splits into session chunks. Use before /pipeline or /swarm."
user-invocable: true
---

# /budget-check - Pipeline Budget Estimator

Activate when user says `/budget-check`, `check budget`, `will this fit`, or `estimate session cost`.

## Syntax

```
/budget-check                           # Analyze current .pipeline-state.json
/budget-check <plan-file.md>            # Analyze a markdown plan
/budget-check "audit fix test PR merge"  # Quick inline estimate
```

## How It Works

Run `~/.claude/scripts/budget_tracker.py` against the input:

```bash
# From a plan file
python3 ~/.claude/scripts/budget_tracker.py docs/plans/my-plan.md

# Quick inline estimate
python3 ~/.claude/scripts/budget_tracker.py --estimate "audit fix test PR merge"

# Custom budget (default: 80 tool calls)
python3 ~/.claude/scripts/budget_tracker.py --budget 100 docs/plans/my-plan.md
```

## Phase Cost Reference

| Phase Type     | Est. Cost | Examples                           |
| -------------- | --------- | ---------------------------------- |
| Read-only      | 5-15      | audit, discover, explore, research |
| Implementation | 18-30     | fix, implement, refactor, build    |
| Testing        | 4-10      | test, verify, lint, typecheck      |
| Git ops        | 3-8       | commit, PR, merge, push            |
| Orchestration  | 12-25     | swarm, parallel, deploy, review    |

## Decision Logic

After running the estimator:

### Fits in 1 session

Proceed with `/pipeline` or direct execution. Note the utilization %.

### Needs 2+ sessions

Present the session split to the user:

```
This plan needs ~{N} sessions:
  Session 1: Phases 1-3 (audit, fix, test) - 50/68 calls
  Session 2: Phases 4-5 (PR, merge) - 14/68 calls

Options:
1. Execute session 1 now, /pipeline-resume later
2. Reduce scope (which phases to cut?)
3. Proceed anyway (risk hitting limits)
```

### Over budget with --split flag

Auto-create separate task files for each session chunk.

## Critical Rules

1. **15% closing reserve is non-negotiable** - always reserve budget for test + PR + commit at the end
2. **Run BEFORE /pipeline or /swarm** - not during
3. **Estimates are conservative** - real cost may be lower, but planning for overhead is safer
4. **If utilization >85%**, warn that the session may hit limits
