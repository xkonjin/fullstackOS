---
description: "Decompose complex objectives into parallel sub-tasks with cost-optimized agent assignment and auto-execution"
user-invocable: true
---

# /fractal-planner - LLM Task Decomposition & Dispatch

Activate when user says `/fractal-planner`, `/fractal`, `/decompose`, `break this down`, `plan and execute`, `decompose this`, or when a task is clearly multi-step and benefits from parallel sub-agent execution.

## Syntax

```
/fractal-planner <objective>
```

- `objective`: what to accomplish - a high-level coding task, feature, or multi-step operation

## Architecture

```
/fractal-planner <objective>
    │
    Phase 1: PLAN ──────── LLM decomposes objective → flat task list (JSON)
    Phase 2: CLASSIFY ──── task-classifier scores each leaf (trivial/standard/complex/deep)
    Phase 3: ASSIGN ────── Map complexity → cheapest capable CLI/model
    Phase 4: EXECUTE ───── Convert to SwarmTaskSpec[], dispatch via SwarmCoordinator
    Phase 5: REPORT ────── Stats: task count, complexity breakdown, cost savings
```

### Agent Assignment Tiers

| Complexity | Candidates (random per dispatch)                            | Cost Range |
| ---------- | ----------------------------------------------------------- | ---------- |
| trivial    | glm-4.7-flash, minimax-m2.1-fast, gemini-2.5-flash-lite     | FREE–0.01  |
| standard   | gemini-2.5-flash, kimi-code, glm-4.7, minimax-m2.5-fast     | 0.03–0.13  |
| complex    | gpt-5.1-codex-mini, gemini-3.1-pro-low, glm-5, claude-haiku | 0.16–0.53  |
| deep       | claude-sonnet-4-6, gpt-5.3-codex, gemini-3.1-pro-high       | 0.93–1.07  |

---

## Phase 1: PLAN

Call the orchestrator fractal plan endpoint:

```bash
curl -s -X POST http://localhost:8318/v1/fleet/fractal/plan \
  -H "Authorization: Bearer your-proxy-key" \
  -H "Content-Type: application/json" \
  -d '{
    "objective": "<USER_OBJECTIVE>",
    "auto_execute": true,
    "cwd": "<REPO_ROOT>"
  }' | jq .
```

If `auto_execute: true`, the plan is immediately dispatched as a swarm. The response includes:

- `plan_id`: unique plan identifier
- `swarm_id`: swarm execution identifier (if auto_execute)
- `stats`: total tasks, by complexity, by CLI, estimated cost reduction %
- `leaf_tasks[]`: each with id, description, complexity, assigned CLI/model

## Phase 2: MONITOR

If auto_execute was used, monitor the swarm:

```bash
curl -s http://localhost:8318/v1/fleet/swarm/<swarm_id> \
  -H "Authorization: Bearer your-proxy-key" | jq .
```

## Phase 3: REPORT

Present results as a table:

| Task | Complexity | Agent | Model | Status |
| ---- | ---------- | ----- | ----- | ------ |

Include the estimated cost reduction percentage vs. running everything on claude-sonnet.

---

## Auto-Trigger

The fractal planner is also triggered **automatically** during fleet dispatch when:

1. `fractal_fleet.auto_decompose` is enabled in orchestrator config (default: true)
2. Task complexity meets threshold (`auto_decompose_min_complexity`, default: "complex")
3. Request does not have `metadata.no_decompose: true`

When auto-triggered, the normal single dispatch is replaced with a fractal plan + swarm execution. The response includes `auto_decomposed: true`.

## Plan-Only Mode

To plan without executing (for review):

```bash
curl -s -X POST http://localhost:8318/v1/fleet/fractal/plan \
  -H "Authorization: Bearer your-proxy-key" \
  -H "Content-Type: application/json" \
  -d '{
    "objective": "<OBJECTIVE>",
    "auto_execute": false
  }' | jq .
```

Then execute manually:

```bash
curl -s -X POST http://localhost:8318/v1/fleet/fractal/execute/<plan_id> \
  -H "Authorization: Bearer your-proxy-key" | jq .
```
