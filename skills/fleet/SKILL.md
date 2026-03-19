---
description: "Run AI Fleet on a task. Just say /fleet <what you want done>. Smart model selection, auto-routes to Codex, reports results. Zero config needed."
user-invocable: true
---

# /fleet - Run AI Fleet

The user said `/fleet` followed by what they want done. Execute it immediately. No questions.

## Rules

1. **Never ask clarifying questions** - just run it
2. **Never use `--via claude`** - you're inside Claude already (nested session = crash)
3. **Always pass `--autonomous --approve`** - skip gates for one-shot tasks
4. **Pick the right tier** - don't blast premium on a typo fix
5. **Always run codex-preflight** before dispatching - validate model name, auth, connectivity
6. **Always generate a fleet report** at the end (see fleet-review skill for format)
7. **Report results concisely** - what changed, what model was used, any errors
8. **Auto-route to experiment-loop when needed** before one-shot execution

## Smart Model Selection

Pick the tier based on the objective:

| Signal                                              | Tier      | Model                                   | Timeout |
| --------------------------------------------------- | --------- | --------------------------------------- | ------- |
| ≤3 words, rename/typo/format/lint                   | `simple`  | Codex mini (via `--tier simple`)        | 120s    |
| Standard dev: implement/fix/debug/test/refactor     | `coding`  | Codex o3 (via `--tier coding`)          | 600s    |
| Architecture/security/migration/multi-file redesign | `premium` | Codex o3 premium (via `--tier premium`) | 900s    |

**Default to `coding` if unclear.** Only use `simple` for genuinely trivial tasks. Only use `premium` for tasks that mention architecture, security, migration, or require understanding 5+ files.

## Execution

### Step 1: Get the repo and objective

- **Repo**: Use the current working directory. If user specified a path, use that.
- **Objective**: Everything after `/fleet` is the objective.

### Step 1.5: Auto experiment-loop trigger

If objective matches any of:

- flaky/intermittent/regression/debug-hard cases
- explicit variant search (`best implementation`, `compare approaches`, `iterate until pass`)
- long high-complexity objective

then call `POST /v1/fleet/experiment-loops` first and poll `GET /v1/fleet/experiment-loops/:id` to terminal.

If experiment-loop launch fails, continue with normal `/fleet` execution and report the fallback reason.

### Step 2: Preflight (MANDATORY - prevents wasted launches)

Run codex-preflight checks:

```bash
# 1. CLIProxyAPI health
curl -sf http://127.0.0.1:8317/ || echo "FAIL: CLIProxyAPI down"

# 2. Validate model name (CRITICAL: codex-5.3 WRONG, gpt-5.3-codex CORRECT)
curl -s http://127.0.0.1:8317/v1/models | python3 -c "import json,sys; print('\n'.join(m['id'] for m in json.load(sys.stdin).get('data',[])))" | grep -q "<MODEL>" || echo "FAIL: model not found"

# 3. Auth token check
python3 -c "import json; d=json.load(open('$HOME/.codex/auth.json')); assert d.get('api_key'), 'NULL KEY'" 2>&1

# 4. Gateway health
curl -sf http://127.0.0.1:4105/v1/health || echo "WARN: Gateway down"
```

If gateway (:4105) is down:

```bash
launchctl kickstart -k gui/$(id -u)/com.ai.fleet.gateway 2>/dev/null; sleep 2
```

**If preflight fails, fix the issue first. Do NOT dispatch with broken auth/model.**

### Step 3: Run it

```bash
ai-coordinator exec \
  --repo <REPO_DIR> \
  --objective "<OBJECTIVE>" \
  --via codex \
  --tier <SELECTED_TIER> \
  --autonomous \
  --approve \
  --timeout <SELECTED_TIMEOUT>
```

### Step 4: Read results

```bash
# Find latest run
ls -t ~/.ai-fleet/coordinator/runs/run_*.log 2>/dev/null | head -1
```

Read the log file, then check for code changes:

```bash
git diff --stat
git status
```

### Step 5: Fleet Report (MANDATORY)

Generate a comprehensive fleet report with:

1. **Executive summary** (1 line)
2. **Deliverables table** (repo, commits, files, lines changed)
3. **Timeline** (start → end, wall time)
4. **Token spend** (agent, model, estimated tokens, estimated cost)
5. **What was found & fixed** (categorized by type: bug, security, perf, etc.)
6. **Lessons learned** (what worked, what didn't)
7. **Improvements for next time** (what to fix in the fleet pipeline)
8. **Cost efficiency** (total cost, comparison to manual effort)

Then suggest: "commit?", "run tests?", "review changes?", "create PR?"

## Error Recovery

| Error                      | Fix                                                            |
| -------------------------- | -------------------------------------------------------------- |
| `ai-coordinator` not found | `export PATH="$HOME/.local/bin:$PATH"` then retry              |
| Gateway down               | `launchctl kickstart -k gui/$(id -u)/com.ai.fleet.gateway`     |
| Rate limited / 429         | Coordinator auto-rotates profiles - just wait for it           |
| Codex auth expired         | `ai-fleet profiles check` to re-enable after reset             |
| Timeout                    | Bump tier to premium (longer timeout + better model) and retry |
