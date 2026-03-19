---
description: "End-to-end autonomous engineering pipeline: requirements discovery through deployed, verified, documented solution. Chains askquestionsspec, plan, implement, review, adversarial testing, ship, verify, and learn into a single command."
user-invocable: true
---

# /pipeline - End-to-End Autonomous Engineering

Activate when user says `/pipeline`, `end to end`, `full pipeline`, `do everything`, or `build this from scratch`.

## Syntax

```
/pipeline <objective>
/pipeline <repo> <objective>
/pipeline all <objective>          # multi-repo mode
```

## Architecture

```
/pipeline <objective>
    |
    Phase 1: DISCOVER ───── Deep requirements (/askquestionsspec)
    |                       Output: spec.md
    Phase 2: PLAN ─────────  /workflows:plan + /deepen-plan
    |                       Output: plan.md
    |                       *** GATE: User approves plan ***
    Phase 3: IMPLEMENT ────  Route: /swarm or /workflows:work
    |                       Use /experiment-loop when the task benefits from candidate search and verifier-driven iteration
    |                       Output: code on feature branch
    Phase 4: REVIEW ─────── /workflows:review (10+ agents, static)
    |                       Output: P1/P2/P3 findings
    Phase 5: FIX ──────────  RALPH loop on review findings
    |                       Output: P1s resolved
    Phase 6: TEST ─────────  Build harness, feed live input, adversarial QA
    |                       RALPH loop until all scenarios pass
    |                       Output: test report + all edge cases green
    Phase 7: VERIFY ─────── /deploy-check → GO/NO-GO
    |                       Output: final evidence
    Phase 8: SHIP ─────────  /pr-flow → CI monitor → merge
    |                       Output: merged PR
    Phase 9: DEPLOY ─────── /sentry-setup + live smoke test
    |                       Output: running in production
    Phase 10: LEARN ──────   /workflows:compound + /reflect + /session-recap
                            Output: documented, learnings captured
```

---

## Phase 1: DISCOVER

Run deep requirements discovery. Skip if user provides a complete spec or plan file.

### Skip Conditions

If the user provides:

- A plan file path (`.md` with plan structure) → skip to Phase 2
- An existing issue/PR number → skip to Phase 3
- "just do it" / "skip discovery" → skip to Phase 2 with minimal spec

### Execution

Run `/askquestionsspec <objective>`:

1. Ask 3-4 high-priority questions from Phases 1-2 (root problem, users)
2. Based on answers, drill into Phases 3-6 (functional, technical, operational, scope)
3. Continue until exhausted OR user signals readiness
4. Summarize as structured spec

### Output: Spec Document

Save to `docs/specs/YYYY-MM-DD-<name>-spec.md`:

```markdown
# Spec: <objective>

## Problem

<what and why>

## Users

<who uses this>

## Requirements

### Must Have

- ...

### Should Have

- ...

### Won't Have (this iteration)

- ...

## Technical Constraints

- ...

## Success Criteria

- ...
```

### Handoff

Pass spec file path to Phase 2.

---

## Phase 2: PLAN

Create detailed implementation plan and enhance it with research.

### Step 2a: Create Plan

Run `/workflows:plan <spec_path or objective>`:

- Reads the spec from Phase 1
- Conducts local research (parallel Explore agents across codebase)
- Structures implementation tasks with dependencies
- Identifies files to create/modify
- Estimates scope (MINIMAL / MORE / A LOT)

Output: `docs/plans/YYYY-MM-DD-<type>-<name>-plan.md`

### Step 2b: Deepen Plan

Run `/deepen-plan <plan_path>`:

- Discovers ALL available skills and matches relevant ones
- Scans `docs/solutions/` for prior learnings
- Launches per-section research agents
- Runs review agents (security, performance, architecture, data integrity)
- Enhances plan sections with findings

### Step 2c: GATE - User Approval

**This is the ONLY human gate in the pipeline.**

Present the plan summary and ask:

```
Plan ready for: <objective>
- Tasks: N implementation steps
- Files: N files to create/modify
- Scope: MINIMAL / MORE / A LOT
- Route: single-repo / multi-repo
- System type: API / Bot / CLI / Web app / Library

Approve plan and begin autonomous implementation?
[Yes, execute] [Modify plan first] [Cancel]
```

**If user says "modify"** → let them edit, then re-present.
**If user says "cancel"** → stop pipeline, save plan for later.
**If user says "yes"** → proceed to Phase 3. Everything after this is autonomous.

---

## Phase 3: IMPLEMENT

Route to the right execution engine based on scope.

### Routing Decision

| Condition                                 | Route                                           | Why                      |
| ----------------------------------------- | ----------------------------------------------- | ------------------------ |
| Multi-repo (`/pipeline all ...`)          | `/swarm all <objective>`                        | Parallel repo processing |
| Named repos (`/pipeline repo1 repo2 ...`) | `/swarm repo1 repo2 <objective>`                | Targeted multi-repo      |
| Single repo, 3+ files to change           | `/workflows:work <plan_path>` with Codex assist | Parallel file writes     |
| Single repo, 1-2 files                    | `/workflows:work <plan_path>`                   | Direct implementation    |
| Existing PR needs fixes                   | `/swarm <repo> "fix and merge"`                 | Fix-and-ship mode        |

### Route A: Codex Subagent (3+ files, pure implementation)

For tasks that modify 3+ files with clear instructions (no ambiguity, no architecture decisions):

```bash
# IMPORTANT: Clear CLAUDECODE env var to prevent nested session guard
env -u CLAUDECODE codex exec \
  --full-auto \
  --skip-git-repo-check \
  --ephemeral \
  -m gpt-5.3-codex \
  "Working directory: <repo_dir>
Branch: <branch>

Implement the following from the plan:
<plan tasks with file list>

Files to create/modify:
<file_list from plan>

Rules:
- Implement exactly what the plan specifies. Don't add extras.
- Each change should be minimal and targeted.
- Run tests after changes if available: <test_command>
- Commit with descriptive messages (imperative mood).
- Push to branch: <branch>"
```

For long-running Codex tasks (20+ min estimated, large refactors):

```bash
env -u CLAUDECODE codex-agent start \
  "<prompt>" \
  --map -r xhigh
```

### Route B: Claude Implementer (1-2 files, needs judgment)

For tasks requiring architectural decisions or careful reasoning:

```
Task(
  subagent_type="implementer",
  model="sonnet",
  description="Implement <feature>",
  prompt="""
  You are implementing a feature for <repo_name>.

  Working directory: <repo_dir>
  Branch: <branch>
  Plan: <plan_path>

  ## Tasks
  <tasks from plan>

  ## Files to Modify
  <file_list>

  ## Instructions
  1. Read each file that needs changes
  2. Implement changes per plan
  3. Run tests: <test_command>
  4. Run lint: <lint_command>
  5. Run type check: <type_check_command>
  6. Commit fixes with descriptive messages (imperative mood)
  7. Push: git push origin <branch>

  ## Rules
  - Follow the plan exactly. Don't over-engineer.
  - Each commit should be atomic and well-described.
  - If you can't implement something, document why in your output.
  - NEVER force push. NEVER amend published commits.
  """
)
```

### Route C: /workflows:work (default single-repo)

For standard implementation with Claude orchestrating directly:

1. Read plan file
2. Create feature branch: `<type>/<short-name>` (e.g., `feat/query-router`)
3. Execute tasks from plan in order
4. Incremental commits per logical unit
5. Run tests after each change
6. Update plan checkboxes `[ ]` → `[x]`

### Route D: /swarm (multi-repo)

Enters the 8-phase swarm pipeline (which has its own Codex routing):

- Phase 0-1: Discover and assess repos
- Phase 2: Route to Codex or Claude per repo
- Phase 3-4: Verify + RALPH self-correction
- Phase 5-6: CI monitor + sequential merge
- Phase 7: Live test

### Codex vs Claude Decision Matrix

| Signal        | Route to Codex                      | Route to Claude                       |
| ------------- | ----------------------------------- | ------------------------------------- |
| Files changed | 3+ files                            | 1-2 files                             |
| Task type     | Pure implementation, clear spec     | Needs judgment, architecture          |
| Ambiguity     | Low - "add X to files Y,Z"          | High - "design the right approach"    |
| Duration      | Long-running (20+ min)              | Quick iterations                      |
| Error type    | Mechanical (imports, types, wiring) | Logical (wrong algorithm, bad design) |

### Handoff

Pass branch name + list of changes to Phase 4.

---

## Phase 4: REVIEW

Run comprehensive code review on the implementation.

### Execution

Run `/workflows:review <branch or PR>`:

Fan-out parallel review agents:

- **Security sentinel** - OWASP, secrets, injection
- **Performance oracle** - N+1 queries, memory leaks, bottlenecks
- **Architecture strategist** - patterns, coupling, boundaries
- **Pattern recognition** - anti-patterns, code smells
- **Data integrity guardian** - migrations, constraints, race conditions
- **Code simplicity** - over-engineering, dead code, unnecessary abstractions
- **Git history analyzer** - commit quality, file history
- Stack-specific reviewers (Python/TypeScript/Rails)

### Output

Todo files in `todos/` categorized by severity:

- **P1 (Critical)**: Must fix before merge - bugs, security, data loss
- **P2 (Important)**: Should fix - performance, maintainability
- **P3 (Nice-to-have)**: Minor style, optional improvements

### Handoff

If P1 count > 0 → proceed to Phase 5 (fix loop).
If P1 count = 0 → skip to Phase 6 (test).

---

## Phase 5: FIX (RALPH Loop)

Self-correcting loop to resolve review findings. Only runs if Phase 4 found P1/P2 issues.

```
RALPH = Research → Analyze → Learn → Plan → Hypothesize

For each iteration (max 3):

  MODEL = sonnet (iter 1-2) or opus (iter 3+)

  R - RESEARCH: Read the todo files, understand each finding
  A - ANALYZE: Root cause each P1, then P2s
  L - LEARN: Note patterns for /reflect later
  P - PLAN: Minimal patch to resolve all P1s
  H - HYPOTHESIZE: "If I change X, issues Y/Z resolve because..."
      → Route fix to Codex or Claude (see below)
      → Run tests
      → Re-run targeted review on changed files

EXIT CONDITIONS:
  - All P1s resolved → Phase 6
  - 3 iterations exhausted → report remaining, ask user
  - Same error 3x → mark blocked, ask user
```

### Fix Routing (Codex vs Claude)

Route each RALPH fix based on scope:

**Codex - for mechanical multi-file fixes:**

```bash
env -u CLAUDECODE codex exec \
  --full-auto \
  --skip-git-repo-check \
  --ephemeral \
  -m gpt-5.3-codex \
  "Working directory: <repo_dir>
Branch: <branch>

Fix these review findings:
<P1 findings list>

Files to modify: <file_list>
Test command: <test_command>

Rules:
- Fix only the listed issues. Don't refactor.
- Run tests after each fix.
- Commit each fix separately with descriptive messages.
- Push to branch: <branch>"
```

**Claude - for judgment-heavy fixes (security, architecture, logic):**

```
Task(
  subagent_type="implementer",
  model=MODEL,  # sonnet iter 1-2, opus iter 3+
  description="RALPH fix iter N",
  prompt="""
  Fix these review findings in <repo_dir>:
  <P1 findings with file:line references>

  Previous iteration summary: <2000 char compressed summary>

  Read the code, understand the root cause, implement minimal fix.
  Run tests: <test_command>
  Commit and push.
  """
)
```

### Critical Rules

- **Fresh agent per iteration** - no context drift
- **P1s first, then P2s** - never fix P3s in the loop
- **Compressed handoff** - pass error list + 2000-char summary, not full logs
- **Atomic commits** - one commit per fix, descriptive message
- **Codex for 3+ file mechanical fixes, Claude for judgment** - same matrix as Phase 3

---

## Phase 6: TEST - Adversarial QA Engine

**The core innovation.** Build a test harness, feed live input into the system, find every edge case, log everything, RALPH loop until clean. No external side effects.

### Step 6a: Classify System Type

Determine what kind of system was built and how to interact with it:

| System Type                      | Detection                                        | Interaction Method                                     |
| -------------------------------- | ------------------------------------------------ | ------------------------------------------------------ |
| **REST API**                     | FastAPI, Express, Hono, Flask routes             | `curl` / `httpie` requests to localhost                |
| **GraphQL API**                  | GraphQL schema, resolvers                        | `curl` with query payloads                             |
| **CLI tool**                     | argparse, commander, clap, `bin` in package.json | Direct command execution with args                     |
| **Bot** (Slack/Telegram/Discord) | Bot handlers, command parsers                    | Call handler functions directly, mock webhook payloads |
| **Web app**                      | Next.js, React pages, templates                  | Playwright browser automation (localhost)              |
| **Library/SDK**                  | Exported functions, no server                    | Import and call functions in test script               |
| **Worker/Queue**                 | Celery, BullMQ, cron jobs                        | Enqueue test jobs, check results                       |
| **Database migration**           | Alembic, Prisma, Drizzle                         | Run migration, verify schema, test rollback            |

### Step 6b: Build Test Harness

Generate a test harness script tailored to the system type. Save to `tests/harness/` or `test_harness/`.

**Harness Architecture:**

```
Task(
  subagent_type="implementer",
  model="sonnet",
  description="Build test harness",
  prompt="""
  You are building an adversarial test harness for <system_type>.

  Working directory: <repo_dir>
  Branch: <branch>
  System entry point: <entry_point>
  Spec: <spec_summary>
  Plan: <plan_summary>
  Files changed: <file_list>

  ## What to Generate

  Create a test harness script that:

  1. STARTS the system (or imports the module) in a sandboxed way
     - For APIs: start server on a random port, wait for ready
     - For CLIs: just invoke the binary
     - For bots: import handler functions directly (NO real bot connections)
     - For web apps: start dev server, use Playwright
     - For libraries: import directly

  2. DEFINES test scenarios across these categories:

     ### Happy Path
     - The primary use case from the spec works end-to-end
     - Each documented feature/endpoint/command works as described

     ### Edge Cases (Product Thinking)
     - Empty input / null / undefined / missing fields
     - Maximum length strings (1 char, 100K chars)
     - Unicode, emoji, RTL text, zero-width characters
     - Numeric boundaries (0, -1, MAX_INT, NaN, Infinity)
     - Empty collections (no items, no results)
     - Single item collections
     - Duplicate submissions (idempotency)

     ### Error States
     - Malformed input (wrong types, missing required fields)
     - Invalid authentication / missing auth
     - Resource not found (404 paths, deleted entities)
     - Conflict states (already exists, stale data)
     - Upstream dependency failure (mock timeouts, 500s)

     ### Concurrency & Timing
     - Rapid sequential requests (same input)
     - Parallel identical requests (race conditions)
     - Request during shutdown / startup
     - Slow client (partial request, timeout)

     ### Security Boundaries
     - SQL injection payloads in every string field
     - XSS payloads in every string field
     - Path traversal (../../etc/passwd)
     - Oversized payloads (exceed Content-Length)
     - Unexpected HTTP methods
     - Missing CORS headers (if applicable)

     ### State Transitions
     - Out-of-order operations
     - Repeat operations (create, create again)
     - Operations on deleted/archived entities
     - Operations with stale references

  3. RUNS each scenario and CAPTURES:
     - Request sent (method, path, body)
     - Response received (status, body, headers)
     - Expected vs actual behavior
     - Any stderr, exceptions, or log output
     - Response time (flag if >1s for APIs)

  4. OUTPUTS a structured test report as JSON:
     {
       "system_type": "<type>",
       "total_scenarios": N,
       "passed": N,
       "failed": N,
       "errors": N,
       "scenarios": [
         {
           "category": "edge_case",
           "name": "empty string input",
           "input": {...},
           "expected": "400 with validation error",
           "actual": "500 Internal Server Error",
           "status": "FAIL",
           "error_log": "TypeError: Cannot read property...",
           "severity": "P1"
         }
       ]
     }

  5. CLEANS UP after itself
     - Stop any servers it started
     - Remove test data it created
     - Restore original state

  ## Language & Framework

  Match the project's language:
  - Python project → pytest harness with httpx/requests
  - TypeScript/JS → vitest/jest harness with fetch/supertest
  - Go → go test harness with net/http
  - Multi-language → shell script wrapper with curl

  ## Critical Rules

  - NEVER send real messages (Slack, Telegram, email, SMS)
  - NEVER hit external APIs (mock them)
  - NEVER modify production data
  - NEVER listen on ports that conflict with running services
  - ALL network calls go to localhost only
  - Use random ports to avoid conflicts
  - Set a timeout on every test (30s max per scenario)
  - The harness must be runnable with a single command

  Return the harness file path and the run command.
  """
)
```

**Alternative: Codex for large test harnesses (10+ test files, boilerplate-heavy):**

```bash
env -u CLAUDECODE codex exec \
  --full-auto \
  --skip-git-repo-check \
  --ephemeral \
  -m gpt-5.3-codex \
  "Working directory: <repo_dir>
Branch: <branch>

Build an adversarial test harness for this <system_type> system.
Entry point: <entry_point>
Spec summary: <spec_summary>
Files changed: <file_list>

Create test files in tests/harness/ covering:
1. Happy path (all features from spec)
2. Edge cases (null, empty, unicode, boundaries, duplicates)
3. Error states (malformed input, missing auth, not found, conflicts)
4. Security (SQL injection, XSS, path traversal in every string field)
5. Concurrency (rapid sequential, parallel identical requests)

Output: JSON test report to stdout with pass/fail per scenario.
Language: match project (<language>)
Framework: <test_framework>

CRITICAL: All requests to localhost only. Mock all external services.
Never send real messages. Random ports to avoid conflicts.
Single command to run: <proposed_run_command>"
```

Use Codex when the harness itself is 200+ lines or spans multiple test files. Use Claude when the harness needs careful reasoning about system behavior.

### Step 6c: Run Harness

Execute the generated test harness:

```bash
# Start system in background if needed (API/web)
<start_command> &
SERVER_PID=$!
sleep 3  # wait for startup

# Run harness
<harness_run_command> 2>&1 | tee /tmp/pipeline-*/test-report.json

# Capture exit code
HARNESS_EXIT=$?

# Stop system
kill $SERVER_PID 2>/dev/null
```

### Step 6d: Analyze Results

Parse the test report. Classify failures:

| Severity | Criteria                                                     | Action       |
| -------- | ------------------------------------------------------------ | ------------ |
| **P1**   | Crash, 500 error, data corruption, security bypass           | Must fix     |
| **P2**   | Wrong status code, missing validation, poor error message    | Should fix   |
| **P3**   | Slow response (>1s), inconsistent formatting, missing header | Nice to have |

### Step 6e: TEST RALPH Loop

If any P1 or P2 failures exist, enter the self-correcting loop:

```
TEST-RALPH Loop (max 5 iterations - more than review since tests are concrete):

  Iteration N:
    MODEL = sonnet (iter 1-2), opus (iter 3-4), opus (iter 5 = final)

    R - RESEARCH the failure
        Read the failing test scenario
        Read the code path that handles this input
        Read error logs / stack traces

    A - ANALYZE root cause
        "The handler at <file>:<line> doesn't check for <condition>"
        "Missing validation for <field> before <operation>"
        "Race condition between <op1> and <op2>"

    L - LEARN the pattern
        Append to /tmp/pipeline-*/learnings.txt:
        "<category>: <what went wrong> → <what the fix pattern is>"

    P - PLAN the fix
        Minimal code change to handle the edge case
        Add/update test for the specific scenario

    H - HYPOTHESIZE and implement
        "If I add validation at <file>:<line>, the empty-input
        scenario should return 400 instead of crashing"
        → Route fix to Codex or Claude (see below)
        → Re-run ONLY the failing scenarios (not full harness)
        → If fixed, run full harness to check for regressions

  EXIT CONDITIONS:
    - All P1s and P2s pass → Phase 7
    - 5 iterations exhausted → report remaining, continue with what passes
    - Regression detected (previously passing test now fails) →
        revert last change, try different approach
    - Same failure 3x with different fixes → mark as KNOWN ISSUE, continue
```

### TEST Fix Routing (Codex vs Claude)

**Codex - for adding validation/guards across multiple handlers:**

```bash
env -u CLAUDECODE codex exec \
  --full-auto \
  --skip-git-repo-check \
  --ephemeral \
  -m gpt-5.3-codex \
  "Working directory: <repo_dir>
Branch: <branch>

These test scenarios failed:
<failing scenarios with input/expected/actual>

Files that handle these inputs: <file_list>

For each failure, add the missing validation/error handling so the
scenario returns the expected result instead of crashing.

Rules:
- Fix the root cause, not just the symptom
- Each fix gets its own commit
- Run the test harness after all fixes: <harness_run_command>
- Push to branch: <branch>"
```

**Claude - for logic bugs, race conditions, security fixes:**

```
Task(
  subagent_type="debugger",
  model=MODEL,  # sonnet iter 1-2, opus iter 3+
  description="TEST-RALPH fix iter N",
  prompt="""
  These adversarial test scenarios failed in <repo_dir>:
  <failing scenarios with full error logs>

  Previous iteration summary: <compressed summary>

  Root cause analysis required. Read the code paths, understand why
  each scenario fails, implement minimal fixes.

  Run harness to verify: <harness_run_command>
  Commit and push.
  """
)
```

### Step 6f: Augment Existing Tests

After the harness passes, take the most valuable test scenarios and add them as permanent tests:

```
Task(
  subagent_type="implementer",
  model="sonnet",
  description="Add permanent tests",
  prompt="""
  The adversarial test harness found and fixed these issues:
  <fixed_scenarios>

  Convert the most valuable scenarios into permanent test cases
  in the project's existing test framework:
  - <test_framework> at <test_directory>

  Only add tests for:
  1. Scenarios that caught real bugs (were P1/P2 failures)
  2. Edge cases not already covered by existing tests
  3. Security boundary tests

  Do NOT add tests for:
  - Happy paths already tested
  - P3 formatting/style issues
  - Scenarios that are harness-specific (server lifecycle)

  Commit the new tests with message:
  "test: add edge case tests from adversarial QA"
  """
)
```

### Step 6g: Test Report

Print the final test summary:

```
## Phase 6: TEST Results

| Category | Scenarios | Passed | Failed | Fixed |
|----------|-----------|--------|--------|-------|
| Happy Path | 5 | 5 | 0 | 0 |
| Edge Cases | 12 | 10 | 2 | 2 |
| Error States | 8 | 6 | 2 | 2 |
| Concurrency | 4 | 4 | 0 | 0 |
| Security | 6 | 5 | 1 | 1 |
| State Transitions | 4 | 4 | 0 | 0 |
| **Total** | **39** | **34** | **5** | **5** |

RALPH iterations: 3
Permanent tests added: 4
Known issues: 0
```

### Interaction Method Reference (No External Side Effects)

These are the ONLY allowed interaction methods per system type:

**REST/GraphQL API:**

```bash
# Start on random port
PORT=$((RANDOM + 10000))
<start_cmd> --port $PORT &
# All requests to localhost:$PORT only
curl -s http://localhost:$PORT/endpoint -d '{"test": "data"}'
```

**Bot (Slack/Telegram/Discord):**

```python
# Import handler directly - NO real bot connection
from app.handlers import handle_message, handle_command

# Simulate incoming webhook payload
mock_event = {"type": "message", "text": "/help", "user": "test_user"}
result = await handle_message(mock_event)
# Assert on result, never send to real channel
```

**CLI Tool:**

```bash
# Run with test args, capture stdout+stderr
OUTPUT=$(<cli_command> --input "test data" 2>&1)
EXIT_CODE=$?
# Assert on output and exit code
```

**Web App (Playwright):**

```typescript
// Headless browser, localhost only
const browser = await chromium.launch({ headless: true });
const page = await browser.newPage();
await page.goto(`http://localhost:${PORT}`);
// Fill forms, click buttons, assert on page content
// Screenshot on failure for evidence
await page.screenshot({ path: "/tmp/pipeline-*/failure-screenshot.png" });
```

**Library/SDK:**

```python
# Direct function calls
from mylib import process_data
result = process_data(None)  # edge case: null input
assert result.error == "Input required"
```

**Worker/Queue:**

```python
# Enqueue test job directly (no real queue connection needed if possible)
from app.workers import process_job
result = process_job({"type": "test", "data": None})
# Or with real queue: enqueue and poll for result
```

---

## Phase 7: VERIFY

Final pre-ship verification. Lightweight - the heavy lifting was Phase 6.

### Step 7a: Deploy Check

Run `/deploy-check`:

| Check        | Method                                                             |
| ------------ | ------------------------------------------------------------------ |
| Git clean    | `git status` - no uncommitted changes                              |
| Tests        | Run full test suite (including new tests from Phase 6) - all green |
| Types        | `tsc --noEmit` or equivalent - no errors                           |
| Lint         | Run linter - clean                                                 |
| Dependencies | Check for vulnerabilities                                          |
| Config       | Validate config files parse                                        |

**Verdict: GO or NO-GO**

If NO-GO → return to Phase 6 TEST-RALPH with the blockers as new scenarios.
If GO → proceed to Phase 8.

---

## Phase 8: SHIP

Create PR, monitor CI, merge.

### Step 8a: Create PR

Run `/pr-flow`:

1. `git diff <base>...HEAD` - understand all changes
2. Check for merge conflicts with `git merge-tree`
3. If conflicts → resolve (prefer PR changes for features, main for config)
4. Create PR:

   ```
   gh pr create --title "<concise title>" --body "$(cat <<'EOF'
   ## Summary
   <bullets from plan>

   ## Changes
   <files modified with descriptions>

   ## Test Results
   - Adversarial QA: X/Y scenarios passed
   - Edge cases found and fixed: N
   - Permanent tests added: N
   - RALPH iterations: N

   ## Test plan
   - [x] Unit tests pass
   - [x] Type check clean
   - [x] Lint clean
   - [x] Adversarial QA harness pass
   - [ ] Manual verification: <specific checks>

   Generated with [Claude Code](https://claude.com/claude-code) /pipeline
   EOF
   )"
   ```

### Step 8b: CI Monitor

Poll CI checks (max 10 polls, 60s apart):

```bash
gh pr checks <pr_number> --json name,state,conclusion
```

- ALL pass → merge
- PENDING → wait and re-poll
- FAIL → classify:
  - INFRA (billing, timeout) → flag and skip
  - CI_CONFIG (setup issues) → fix CI file, push, re-poll
  - CODE (test/type/lint failure) → fix code, push, re-poll (max 3 fix cycles)

### Step 8c: Merge

```bash
gh pr merge <pr_number> --squash
```

Verify: `gh pr view <pr_number> --json state` → must be MERGED.

---

## Phase 9: DEPLOY

Set up observability and verify production.

### Step 9a: Sentry Setup (Conditional)

**Only if this is a NEW service or project without existing Sentry.**

Run `/sentry-setup`:

1. Detect stack
2. Create Sentry project
3. Install SDK
4. Configure alerts
5. Send test event

### Step 9b: Live Smoke Test

Verify the deployed changes work in production:

| Project Type    | Test Method                                       |
| --------------- | ------------------------------------------------- |
| Web app         | Check deployed URL responds 200, verify key pages |
| API             | Hit health endpoint, test key routes with curl    |
| Bot             | Check bot status, verify commands respond         |
| CLI tool        | Run --help, verify key commands execute           |
| Railway service | `railway status`, check logs for errors           |

### Step 9c: Production Evidence

Capture and report:

- Screenshot or curl output showing feature works
- No new errors in Sentry
- Response times within expected range

---

## Phase 10: LEARN

Document everything for future sessions.

### Step 10a: Document Solution

Run `/workflows:compound`:

- Parallel subagents: context analyzer, solution extractor, prevention strategist
- Categorize solution (bug fix, feature, performance, security, etc.)
- Write to `docs/solutions/<category>/<filename>.md` with YAML frontmatter
- **Include test harness findings** - edge cases discovered become documented patterns

### Step 10b: Capture Learnings

Run `/reflect`:

- Review queued learnings from this session
- Include learnings from TEST-RALPH iterations
- Update CLAUDE.md with new patterns
- User reviews and approves each learning

### Step 10c: Session Recap

Run `/session-recap`:

- List files modified
- Key decisions made
- What was accomplished
- Pending items
- Persist to `~/.claude/context-reminders.md`

---

## Pipeline State Management

Track pipeline progress in BOTH locations for resilience:

1. **Persistent (repo root):** `.pipeline-state.json` - survives session boundaries, committed to git
2. **Ephemeral (detailed):** `/tmp/pipeline-<timestamp>/state.json` - full logs, test artifacts

### Persistent Checkpoint Protocol

At the START of each phase:

```bash
# Write state to repo root
jq '.phases[PHASE_INDEX].status = "in_progress" | .updated = "NOW"' .pipeline-state.json > .tmp && mv .tmp .pipeline-state.json
```

At the END of each phase:

```bash
# Update state + commit
jq '.phases[PHASE_INDEX].status = "done" | .phases[PHASE_INDEX].commit = "SHA" | .updated = "NOW"' .pipeline-state.json > .tmp && mv .tmp .pipeline-state.json
git add .pipeline-state.json && git commit -m "checkpoint: phase N complete - [summary]"
```

On INTERRUPTION or session end:

```bash
# Save remaining items for /pipeline-resume
jq '.remaining_items = ["item1", "item2"]' .pipeline-state.json > .tmp && mv .tmp .pipeline-state.json
git add .pipeline-state.json && git commit -m "checkpoint: interrupted at phase N - [remaining count] items left"
```

### Ephemeral state (full detail):

```json
{
  "objective": "<what we're building>",
  "repo": "<repo path>",
  "started": "<ISO timestamp>",
  "current_phase": 6,
  "system_type": "api",
  "spec_path": "docs/specs/...",
  "plan_path": "docs/plans/...",
  "branch": "feat/...",
  "pr_number": null,
  "phases": {
    "1_discover": { "status": "done", "output": "docs/specs/..." },
    "2_plan": { "status": "done", "output": "docs/plans/..." },
    "3_implement": { "status": "done", "route": "workflows:work" },
    "4_review": { "status": "done", "p1": 2, "p2": 5, "p3": 3 },
    "5_fix": { "status": "done", "ralph_iters": 2 },
    "6_test": {
      "status": "in_progress",
      "harness_path": "tests/harness/test_adversarial.py",
      "total_scenarios": 39,
      "passed": 34,
      "failed": 5,
      "fixed": 3,
      "ralph_iters": 2,
      "known_issues": []
    },
    "7_verify": { "status": "pending" },
    "8_ship": { "status": "pending" },
    "9_deploy": { "status": "pending" },
    "10_learn": { "status": "pending" }
  },
  "test_report": "/tmp/pipeline-*/test-report.json",
  "learnings": []
}
```

Update state after each phase. If the pipeline is interrupted, it can resume from the last completed phase.

---

## Model Routing

| Phase | Task                                       | Model                 | Rationale                       |
| ----- | ------------------------------------------ | --------------------- | ------------------------------- |
| 1     | Requirements questions                     | opus                  | Needs deep reasoning            |
| 2a    | Plan creation                              | opus                  | Architecture decisions          |
| 2b    | Plan deepening (research)                  | haiku                 | Fast parallel search            |
| 2b    | Plan deepening (review)                    | sonnet                | Quality analysis                |
| 3     | Implementation (1-2 files)                 | Claude sonnet         | Good judgment                   |
| 3     | Implementation (3+ files)                  | **Codex gpt-5.3**     | Parallel writes                 |
| 3     | Long-running impl (20+ min)                | **Codex agent xhigh** | Background execution            |
| 4     | Review agents                              | sonnet                | Quality + speed balance         |
| 5     | Fix RALPH (mechanical, 3+ files)           | **Codex gpt-5.3**     | Bulk fixes                      |
| 5     | Fix RALPH (judgment, 1-2 files)            | Claude sonnet/opus    | Root cause reasoning            |
| 6     | Build test harness (small)                 | Claude sonnet         | Needs judgment on scenarios     |
| 6     | Build test harness (large, 200+ lines)     | **Codex gpt-5.3**     | Boilerplate generation          |
| 6     | Test RALPH (validation guards, multi-file) | **Codex gpt-5.3**     | Mechanical fixes                |
| 6     | Test RALPH (logic bugs, security)          | Claude sonnet/opus    | Deep reasoning                  |
| 6     | Augment permanent tests                    | Claude sonnet         | Integration with existing suite |
| 7     | Deploy check                               | haiku                 | Simple command running          |
| 8     | CI fix classification                      | haiku                 | Pattern matching                |
| 9     | Live test                                  | haiku                 | Simple verification             |
| 10    | Solution documentation                     | sonnet                | Good writing                    |

### Codex Invocation Rules

All Codex subagent calls MUST follow these patterns:

```bash
# Standard Codex exec (synchronous, <20 min tasks)
env -u CLAUDECODE codex exec \
  --full-auto \
  --skip-git-repo-check \
  --ephemeral \
  -m gpt-5.3-codex \
  "<prompt>"

# Long-running Codex agent (async, 20+ min tasks)
env -u CLAUDECODE codex-agent start \
  "<prompt>" \
  --map -r xhigh

# Check Codex agent status
env -u CLAUDECODE codex-agent status
```

**Critical Codex rules:**

- **Always `env -u CLAUDECODE`** - prevents nested session guard error
- **Always `--skip-git-repo-check`** - repos may have uncommitted changes during pipeline
- **Always `--ephemeral`** - don't persist Codex sessions (pipeline manages state)
- **Always `-m gpt-5.3-codex`** - use the Codex-optimized model
- **Never run 2 Codex agents in the same repo simultaneously** - git conflicts
- **Verify Codex output** - after Codex completes, Claude verifies the changes (git diff, run tests)

### Fallback Chains

```
Codex:   gpt-5.3-codex → o3-mini → gpt-5.3
Claude:  claude-opus-4-6 → claude-sonnet-4-5 → claude-haiku-4-5
```

If Codex fails or is unavailable, fall back to Claude implementer agent with equivalent prompt.

---

## Critical Rules

1. **ONE human gate** - plan approval in Phase 2c. Everything else is autonomous.
2. **Never skip TEST** - Phase 6 must run the system with live input. Existing unit tests are not enough.
3. **No external side effects in TEST** - localhost only, mock external services, no real messages.
4. **Test harness must be single-command runnable** - no manual setup.
5. **RALPH max 3 for review fixes, max 5 for test fixes** - test fixes get more iterations because failures are concrete and reproducible.
6. **Fresh agent per RALPH iteration** - prevents context drift.
7. **Sequential phases** - never run Phase N+1 before Phase N completes (except skip conditions).
8. **Compressed handoffs** - each phase passes a summary, not full logs, to the next phase.
9. **State file** - update `/tmp/pipeline-*/state.json` after every phase for resumability.
10. **No external messages** - no Slack, Telegram, email posts (security rule).
11. **Single repo = /workflows:work, multi-repo = /swarm** - never mix.
12. **P1s block shipping** - Phase 8 cannot start with unresolved P1 findings from review OR test.
13. **Progress reporting** - print a status table after each phase completes.
14. **Atomic commits** - one logical change per commit, imperative mood messages.
15. **Revert on regression** - if a fix in TEST-RALPH breaks a previously passing scenario, revert immediately.
16. **Augment don't replace** - new permanent tests supplement existing tests, never remove existing passing tests.

---

## Progress Table

Print and update after each phase:

```
## Pipeline: <objective>
| # | Phase | Status | Output |
|---|-------|--------|--------|
| 1 | Discover | DONE | docs/specs/... |
| 2 | Plan | DONE | docs/plans/... |
| 3 | Implement | DONE | feat/query-router |
| 4 | Review | DONE | 2 P1, 5 P2, 3 P3 |
| 5 | Fix | DONE | 2 P1 fixed (2 RALPH iters) |
| 6 | Test | IN PROGRESS | 34/39 pass, fixing 5... |
| 7 | Verify | PENDING | |
| 8 | Ship | PENDING | |
| 9 | Deploy | PENDING | |
| 10 | Learn | PENDING | |
```

---

## Quick Reference

```
/pipeline "add user authentication"                    # Full end-to-end, single repo
/pipeline api-server "implement query router"     # Named repo
/pipeline all "fix CI across all repos"                # Multi-repo swarm mode
/pipeline --from-plan docs/plans/my-plan.md            # Skip discovery, start from plan
/pipeline --from-branch feat/my-feature                # Skip to review phase
```
