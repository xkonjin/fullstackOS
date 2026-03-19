---
description: "Unified autonomous multi-repo engineering: Codex + Claude agents, RALPH self-correction, CI monitor, sequential merge, live testing"
user-invocable: true
---

# /swarm - Autonomous Multi-Repo Engineering

Activate when user says `/swarm`, `/lfg-multi`, `autonomous swarm`, `multi-repo swarm`, `parallel repos`, or `repo sweep`.

## Syntax

```
/swarm <repos|all> [objective]
```

- `repos`: space-separated repo names (e.g., `project-a project-b project-c`)
- `all`: discover all repos with open PRs or uncommitted work
- `objective` (optional): what to accomplish (default: review, fix, merge, test)

## Architecture

```
/swarm <repos|all> [objective]
    │
    Phase 0: DISCOVERY ─── Map repos, PRs, CI, branches → status table
    Phase 1: ASSESS ────── Parallel Explore(haiku) per repo → routing decisions
    Phase 2: IMPLEMENT ─── Codex (3+ files) or Claude(sonnet) per repo
    Phase 3: VERIFY ────── Type/lint/test + fan-out review (security + code + perf)
    Phase 4: RALPH ─────── Self-correcting loop (max 3 iter, escalate on 3+)
    Phase 5: CI MONITOR ── Poll checks, fix failures, re-poll
    Phase 6: MERGE ─────── Sequential only, resolve conflicts
    Phase 7: LIVE TEST ─── Per-repo smoke tests
    Phase 8: REPORT ────── Summary table + learnings
```

---

## Phase 0: DISCOVERY

Map user's repo selections to concrete state.

### Repo Registry

```
~/Dev/webapp                     # Web Application
~/Dev/api-server                 # API Server
~/Dev/mobile-app                 # Mobile App
~/Dev/shared-libs                # Shared Libraries
~/Dev/fullstackOS                # Infrastructure
~/Dev/data-pipeline              # Data Pipeline
```

### Discovery Steps

For each selected repo (or all if `all`):

```bash
# Run these as SEPARATE Bash calls per repo (never batch - sibling cascade)
cd <repo_dir>
git status --short
git branch --show-current
gh pr list --state open --json number,headRefName,title,url --limit 5
```

If repo has open PR:

```bash
gh pr checks <pr_number> --json name,state,conclusion 2>/dev/null || echo "NO_CHECKS"
gh pr view <pr_number> --json mergeable,mergeStateStatus
```

### Output: Status Table

Print this table after discovery. Update it after each subsequent phase.

```
| Repo | Dir | Branch | PR | CI | Mergeable | Route | Status |
|------|-----|--------|----|----|-----------|-------|--------|
```

Possible values:

- CI: `GREEN`, `RED`, `PENDING`, `NO_CI`, `INFRA`
- Mergeable: `YES`, `CONFLICT`, `BLOCKED`, `N/A`
- Route: `codex`, `claude-sonnet`, `claude-haiku`, `skip`
- Status: `PENDING`, `ASSESS`, `IMPLEMENTING`, `VERIFYING`, `CI_WAIT`, `MERGING`, `TESTING`, `DONE`, `BLOCKED`, `ERROR`

---

## Phase 1: ASSESS

Launch ONE Explore agent per repo in parallel to determine what work is needed and route to the right tool.

```
Task(
  subagent_type="Explore",
  model="haiku",
  description="Assess <repo_name>",
  prompt="""
  Working directory: <repo_dir>
  Branch: <branch>
  PR: #<number> - <title>
  Objective: <objective or "review, fix, and prepare for merge">

  Assess this repo:
  1. List files changed in the PR (git diff --name-only origin/main..HEAD)
  2. Count files changed
  3. Check if tests exist and what commands run them (package.json scripts, Makefile, CI config)
  4. Check for type errors (tsc --noEmit --pretty 2>&1 | tail -20)
  5. Check for lint errors (run lint command from package.json or CI)
  6. Look for obvious bugs or security issues in the diff
  7. Check CI workflow files for known issues (pnpm version duplication, missing bun setup)

  Return a JSON assessment:
  {
    "repo": "<name>",
    "files_changed": <count>,
    "file_list": ["path1", "path2"],
    "has_tests": true/false,
    "test_command": "bun test" or null,
    "lint_command": "eslint src/" or null,
    "type_check_command": "tsc --noEmit" or null,
    "type_errors": <count>,
    "lint_errors": <count>,
    "test_failures": <count>,
    "ci_issues": ["description"],
    "code_issues": ["description"],
    "recommended_route": "codex|claude-sonnet|claude-haiku|skip",
    "route_reason": "why this route"
  }
  """
)
```

### Routing Decision Matrix

After assessments return, route each repo:

| Condition                                   | Route             | Why                        |
| ------------------------------------------- | ----------------- | -------------------------- |
| 3+ files changed, pure implementation fixes | **Codex**         | Parallel file writes, fast |
| 1-2 files, needs judgment/architecture      | **Claude sonnet** | Better reasoning           |
| Only CI config issues                       | **Claude haiku**  | Simple pattern fixes       |
| Tests pass, lint clean, no issues           | **Skip**          | Nothing to do              |
| Long-running (estimated 20+ min)            | **Codex tmux**    | Background execution       |

Codex-first default: unless explicitly overridden, swarm should prefer Codex for implementation/fix/review execution stages and keep Claude focused on routing/judgment steps.

---

## Phase 2: IMPLEMENT

Execute fixes per repo using the routed agent. Launch ALL repo agents in parallel (each repo is isolated).

### Route A: Codex Agent

For repos routed to Codex (3+ files, pure implementation):

```bash
# IMPORTANT: Clear CLAUDECODE env var to prevent nested session guard
env -u CLAUDECODE codex exec \
  --full-auto \
  --skip-git-repo-check \
  --ephemeral \
  -m gpt-5.3-codex \
  "Working directory: <repo_dir>

Fix the following issues in this repo:
<issues from assessment>

Files to modify: <file_list>

Rules:
- Fix real issues only. Don't refactor working code.
- Each change should be minimal and targeted.
- Run tests after changes if test command available: <test_command>
- Commit with descriptive messages.
- Push to branch: <branch>"
```

For long-running Codex tasks (20+ min estimated):

```bash
env -u CLAUDECODE codex-agent start \
  "<prompt>" \
  --map -r xhigh
```

### Route B: Claude Implementer

For repos routed to Claude (1-2 files, judgment needed):

```
Task(
  subagent_type="implementer",
  model="sonnet",
  description="Fix <repo_name>",
  prompt="""
  You are an autonomous engineering agent for <repo_name>.

  Working directory: <repo_dir>
  Branch: <branch>
  PR: #<pr_number>

  ## Issues to Fix
  <issues from assessment>

  ## Files to Modify
  <file_list>

  ## Instructions
  1. Read each file that needs changes
  2. Make minimal, targeted fixes
  3. Run tests: <test_command>
  4. Run lint: <lint_command>
  5. Run type check: <type_check_command>
  6. Commit fixes with descriptive messages (imperative mood)
  7. Push: git push origin <branch>

  ## Rules
  - Fix real issues only. Don't refactor working code.
  - Each commit should be atomic and well-described.
  - If you can't fix something, document it in your output.
  - NEVER force push. NEVER amend published commits.

  Return JSON:
  {
    "repo": "<name>",
    "status": "clean|fixed|issues_remaining",
    "commits": ["msg1", "msg2"],
    "fixes_applied": ["description"],
    "remaining_issues": ["description"],
    "test_results": "X passed, Y failed"
  }
  """
)
```

### Route C: Claude Haiku (CI config only)

```
Task(
  subagent_type="implementer",
  model="haiku",
  description="CI fix <repo_name>",
  prompt="""
  Fix CI configuration issues in <repo_dir>:
  <ci_issues from assessment>

  Common fixes:
  - "Multiple versions of pnpm specified" → Remove version: from CI, keep packageManager in package.json
  - "bun: not found" → Add oven-sh/setup-bun@v2 step before build
  - Missing Node version → Add actions/setup-node@v4 with node-version from package.json

  Commit and push the fix.
  """
)
```

---

## Phase 3: VERIFY

After implementation agents complete, run verification for EACH repo. Launch all verifications in parallel.

### Per-Repo Verification Agent

```
Task(
  subagent_type="reviewer",
  model="sonnet",
  description="Verify <repo_name>",
  prompt="""
  Working directory: <repo_dir>
  Branch: <branch>

  Run ALL of these checks and report results:

  1. Type check: <type_check_command or "skip">
  2. Lint: <lint_command or "skip">
  3. Tests: <test_command or "skip">
  4. Review git diff for bugs, security issues, dead code
  5. Check for secrets/credentials in diff
  6. Verify imports and no circular dependencies

  Return JSON:
  {
    "repo": "<name>",
    "type_check": {"pass": bool, "errors": [...]},
    "lint": {"pass": bool, "errors": [...]},
    "tests": {"pass": bool, "total": N, "passed": N, "failed": N, "failures": [...]},
    "review": {"critical": N, "warnings": N, "issues": [...]},
    "security": {"vulnerabilities": N, "issues": [...]},
    "overall_pass": bool
  }
  """
)
```

---

## Phase 4: RALPH Loop

If ANY repo's verification failed, enter the self-correcting loop.

```
RALPH = Research → Analyze → Learn → Plan → Hypothesize

For each repo with failures (max 3 iterations):

  Iteration N:
    MODEL = sonnet (iter 1-2) or opus (iter 3+)  ← escalation from Agent Gateway pattern

    R - RESEARCH the failures
        Task(subagent_type="Explore", model="haiku"):
        "In <repo_dir>, why did these fail? <error list>"

    A - ANALYZE root causes
        Task(subagent_type="debugger", model=MODEL):
        "Root cause analysis for: <errors> in files: <modified files>"

    L - LEARN from the pattern
        Append learning to /tmp/swarm-learnings.txt

    P - PLAN the fix
        Minimal patch - fewest changes to resolve all errors

    H - HYPOTHESIZE and implement
        "If I make these changes, errors X/Y/Z should resolve because..."
        Route to Codex or Claude based on fix scope
        → Return to Phase 3: Verify

EXIT CONDITIONS:
  - All verifications pass → continue to Phase 5
  - 3 iterations exhausted → report remaining issues, continue with passing repos
  - Same error repeats 3x → mark repo as BLOCKED, continue with others
```

### Error Tracking

Maintain per-repo error tracking across iterations:

```json
{
  "<repo>": {
    "iteration": 2,
    "resolved": ["TS2304: Cannot find name 'foo'"],
    "remaining": ["TS2345: Argument type mismatch"],
    "recurring": { "TS2345": 2 },
    "learnings": ["Always import types from ./types.ts not ./types"]
  }
}
```

### CRITICAL: Fresh Agent Per Iteration

**NEVER reuse agents across RALPH iterations.** Each iteration spawns a fresh agent with only:

- The error list (not full logs)
- Previous iteration's compressed summary (max 2000 chars)
- The learning from L step

This prevents context drift (AI Fleet pattern).

---

## Phase 5: CI MONITOR

After fixes are pushed, monitor CI for each repo with open PRs.

```
CI Monitor Loop (per repo, max 3 fix cycles):

  1. POLL: gh pr checks <pr_number> --json name,state,conclusion
     Parse: pass/fail/pending for each check

  2. If ALL pass → mark repo GREEN, continue to Phase 6

  3. If PENDING → wait 60s, re-poll (max 10 polls = 10 min)

  4. If FAIL → classify:
     a. Get logs: gh run view <run_id> --log-failed | tail -60
     b. Classify:
        - INFRA (billing exhausted, runner timeout, rate limit) → flag INFRA, skip
        - CI_CONFIG (pnpm version, missing bun, wrong node version) → fix CI file
        - CODE (type error, test failure, lint error) → fix code
     c. Fix and push
     d. Wait for new CI run, re-poll

  5. After 3 fix cycles → report remaining, mark BLOCKED
```

### Known CI Failure Patterns

| Pattern                               | Classification | Fix                                                                   |
| ------------------------------------- | -------------- | --------------------------------------------------------------------- |
| "Multiple versions of pnpm specified" | CI_CONFIG      | Remove `version:` from CI YAML, keep `packageManager` in package.json |
| "bun: not found"                      | CI_CONFIG      | Add `oven-sh/setup-bun@v2` step before build step                     |
| Runner timeout (0 steps, 3s)          | INFRA          | GitHub Actions billing exhausted - skip                               |
| "Cannot find module"                  | CODE           | Fix import path or add missing dependency                             |
| "Type error" / "TS\d+"                | CODE           | Fix type error in source                                              |
| "Lint error" / "eslint"               | CODE           | Fix lint violation                                                    |
| Download/install timeout              | INFRA          | Transient - re-trigger with empty commit, max 2 retries               |

### Re-trigger CI

```bash
# Empty commit to re-trigger CI (use sparingly, max 2x per repo)
cd <repo_dir>
git commit --allow-empty -m "ci: re-trigger checks" && git push origin <branch>
```

---

## Phase 6: MERGE

Merge repos **ONE AT A TIME** - never parallel (conflict cascade risk).

### Merge Sequence

```
For each GREEN repo (sorted by fewest conflicts first):

  1. Check mergeable state:
     gh pr view <pr_number> --json mergeable,mergeStateStatus

  2. If MERGEABLE:
     gh pr merge <pr_number> --squash
     # Do NOT use --delete-branch (worktree issues)

  3. If CONFLICTING:
     a. cd <repo_dir>
     b. git fetch origin main
     c. git merge origin/main
     d. Resolve conflicts (prefer PR changes for feature work, prefer main for config)
     e. git commit -m "merge: resolve conflicts with main"
     f. git push origin <branch>
     g. Wait for CI (back to Phase 5 for this repo)
     h. Then merge: gh pr merge <pr_number> --squash

  4. If BLOCKED (required checks failing):
     Skip, report to user

  5. Verify: gh pr view <pr_number> --json state → must be MERGED

  6. Wait 5s before next merge (let GitHub settle)
```

### Conflict Resolution Agent

For non-trivial conflicts, spawn a dedicated agent:

```bash
env -u CLAUDECODE codex exec \
  --full-auto \
  --skip-git-repo-check \
  --ephemeral \
  -m gpt-5.3-codex \
  "Resolve merge conflicts in <repo_dir>.
  The branch <branch> has conflicts with main.
  Resolve all conflicts preserving the PR's intended changes.
  Commit the resolution and push."
```

### Worktree Safety

Before ANY git operation:

```bash
git worktree list  # Check for existing worktrees
```

- If target branch is already checked out in a worktree → work from that worktree, NOT main repo
- Never create a worktree for a branch that's already checked out (detached HEAD)
- Don't delete branches that have worktrees

---

## Phase 7: LIVE TEST

Per-repo smoke testing after merge.

### Test Matrix

| Project Type                   | Test Method                                       |
| ------------------------------ | ------------------------------------------------- |
| Web app (Next.js, React)       | Check deployed URL responds 200, verify key pages |
| API (FastAPI, Express, Hono)   | Hit health endpoint, test key routes with curl    |
| Bot (Discord, Slack, Telegram) | Check bot status, verify slash commands respond   |
| CLI tool                       | Run --help, verify key commands execute           |
| Library/SDK                    | Import and call key functions                     |
| Railway service                | `railway status`, check logs for recent errors    |

### Live Test Agent

```
Task(
  subagent_type="researcher",
  model="haiku",
  description="Live test <repo_name>",
  prompt="""
  Verify <repo_name> is working after merge.

  Project type: <type>
  Deploy URL: <url or "local">

  1. Verify the service is running and responding
  2. Test the core user journey
  3. Check for errors in responses
  4. Verify no regressions from recent changes

  Return JSON:
  {
    "repo": "<name>",
    "live_test": "pass|fail|skip",
    "evidence": "what you tested and saw",
    "issues": ["any problems found"]
  }
  """
)
```

### Deploy URLs (Known)

```
webapp:              Check web app responses
api-server:           Railway services (api, ingest, etc.)
support-ai:           Check web app deployment
project-g:                Check web app deployment
Project D:             Check deployed app
dashboard:            Check deployed page
```

---

## Phase 8: REPORT

### Progress Table (final update)

```
| Repo | Assess | Implement | Verify | CI | Merge | Live | Status |
|------|--------|-----------|--------|----|-------|------|--------|
```

### Final Summary Template

```
## Swarm Summary

Completed: X/Y repos merged and verified
Blocked: N (reason)
Skipped: N (reason)

### Fixes Applied
- <repo>: <what was fixed>

### RALPH Iterations
- <repo>: N iterations, resolved <issues>, escalated to <model>

### CI Fixes
- <repo>: <what CI issues were fixed>

### Remaining Issues
- <repo>: <what's still broken and why>

### Learnings
- <pattern discovered>
```

### Persist Learnings

After completion, append significant learnings to `/tmp/swarm-learnings.txt` and report them to the user for potential `/reflect` capture.

---

## Model Routing Summary

| Task                          | Model         | Rationale                  |
| ----------------------------- | ------------- | -------------------------- |
| Repo assessment               | haiku         | Fast file search, cheap    |
| Code review + fix (1-2 files) | sonnet        | Good judgment              |
| Bulk file implementation (3+) | Codex gpt-5.3 | Parallel writes            |
| CI log analysis               | haiku         | Pattern matching           |
| Merge conflict resolution     | Codex gpt-5.3 | File manipulation          |
| RALPH iteration 1-2           | sonnet        | Standard debugging         |
| RALPH iteration 3+            | opus          | Escalate for hard problems |
| Live smoke test               | haiku         | Simple verification        |
| Fan-out review                | sonnet        | Security + code quality    |
| Codebase exploration          | haiku         | Fast, cheap                |

### Fallback Chains (from AI Fleet)

```
claude-opus-4-6    → claude-opus-4-5 → claude-sonnet-4-5
claude-sonnet-4-5  → claude-haiku-4-5
gpt-5.3-codex     → o3-mini → gpt-5.3
```

---

## Critical Rules

1. **One agent per repo** - never mix repos in one agent (context isolation)
2. **Never parallel merge** - merge one at a time, check conflicts between each
3. **Never batch independent ops in one Bash call** - sibling error cascade kills all siblings on one failure
4. **Fresh agent per RALPH iteration** - prevents context drift (AI Fleet pattern)
5. **Check `git worktree list` before git operations** - avoid detached HEAD
6. **Don't retry infra failures** - flag and skip (billing, runner timeout)
7. **Compress cycle summaries** - pass error list, not full logs, to next iteration (max 2000 chars)
8. **Report progress after each phase** - update the status table, don't go silent
9. **Max 3 RALPH iterations per repo** - then report and let user decide
10. **No external messages** - no Slack, Telegram, email posts (security rule)
11. **Clear CLAUDECODE env var** for Codex spawns - `env -u CLAUDECODE codex exec ...`
12. **Sequential merge, parallel everything else** - merge is the only sequential phase
13. **Per-repo error isolation** - one repo failing does NOT block others
14. **Exit on 3x same error** - don't infinite loop on unfixable issues

## Quick Reference

```
/swarm all                          # All repos with open PRs
/swarm webapp api-server              # Specific repos
/swarm all "fix CI and merge"       # With objective
/swarm api-server "implement phase 1 query router"  # Targeted work
```
