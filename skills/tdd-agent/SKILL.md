---
description: "Test-driven development agent. Enforces red-green-refactor loop for every code change: write failing test first, implement until green, run full suite before commit. Auto-detects test framework. Supports parallel mode."
user-invocable: true
---

# /tdd-agent - Test-Driven Development Agent

Activate when user says `/tdd-agent`, `tdd`, `test first`, or `write tests first`.

## Syntax

```
/tdd-agent <task description>
/tdd-agent --parallel <task1> | <task2> | <task3>
```

## Step 0: Detect Test Framework

Auto-detect from project files:

| Signal                                 | Framework  | Test Command                        | Watch Command  |
| -------------------------------------- | ---------- | ----------------------------------- | -------------- |
| `pyproject.toml` with `[tool.pytest]`  | pytest     | `python -m pytest -x -q --tb=short` | `ptw -- -x -q` |
| `pytest.ini`                           | pytest     | `python -m pytest -x -q --tb=short` |                |
| `package.json` + `bun test` in scripts | bun:test   | `bun test --bail`                   |                |
| `package.json` + `vitest` in deps      | vitest     | `npx vitest run`                    | `npx vitest`   |
| `package.json` + `jest` in deps        | jest       | `npx jest --bail`                   |                |
| `Cargo.toml`                           | cargo test | `cargo test`                        |                |
| `go.mod`                               | go test    | `go test ./...`                     |                |

If no test framework detected, inform user and suggest setting one up.

## Step 1: UNDERSTAND

Read the relevant code and existing tests:

```
1. Read the file(s) that need changes
2. Read existing test files for those modules
3. Identify the test directory structure and naming convention
4. Note any test utilities, fixtures, or factories already in use
```

Output: Brief summary of code to change + existing test coverage.

## Step 2: RED - Write Failing Test

Write a minimal test that captures the EXPECTED behavior after the change:

```python
# Python example
def test_sanitize_user_input_strips_html():
    """The bug: user input with HTML tags causes XSS. Fix: strip HTML."""
    result = sanitize_user_input("<script>alert('xss')</script>Hello")
    assert result == "Hello"
    assert "<script>" not in result
```

```typescript
// TypeScript example
test("sanitize user input strips HTML", () => {
  const result = sanitizeUserInput("<script>alert('xss')</script>Hello");
  expect(result).toBe("Hello");
  expect(result).not.toContain("<script>");
});
```

### Verify RED

Run the test to confirm it FAILS:

```bash
<test_command> -k "test_name"  # Python
<test_command> --testNamePattern "test name"  # JS/TS
```

If it passes unexpectedly → the behavior already exists. Report and skip to next item.
If it errors (import error, etc.) → fix the test setup, not the implementation.

## Step 3: GREEN - Implement Minimum Fix

Write the minimum code change to make the failing test pass:

- Change ONLY what's needed to pass the test
- Don't refactor, don't add features, don't clean up
- Run the specific test after implementation:

```bash
<test_command> -k "test_name"
```

### Retry Logic (max 3 attempts)

If the test still fails after implementation:

```
Attempt 1: Re-read error, adjust implementation
Attempt 2: Re-read both test and code, try different approach
Attempt 3: If still failing, report the issue:
  "❌ Could not make test pass after 3 attempts.
   Test: test_sanitize_user_input_strips_html
   Error: [last error message]
   Attempted approaches:
   1. [approach 1]
   2. [approach 2]
   3. [approach 3]
   Escalating for manual review."
```

## Step 4: SUITE - Run Full Test Suite

After the specific test passes, run the FULL test suite:

```bash
<full_test_command>
```

### Handle Regressions

If any previously-passing test now fails:

1. Read the failing test to understand what it expects
2. Determine if the implementation needs adjustment (not the old test)
3. Fix the implementation to satisfy BOTH the new test and old test
4. Re-run full suite
5. Max 2 regression fix cycles - if still failing, report

## Step 5: COMMIT

Only after ALL tests pass:

```bash
git add <test_file> <implementation_file>
git commit -m "<type>: <description>

- Added test: <test_name>
- Implementation: <what changed>
- Tests: all passing (<count> total)"
```

## Parallel Mode (--parallel)

When `--parallel` flag is used, split tasks into independent work items and spawn Task sub-agents:

```
/tdd-agent --parallel "fix auth validation" | "add rate limiting" | "sanitize input"
```

Each sub-agent follows the same RED-GREEN-REFACTOR loop independently.

### Parallel Execution Rules

1. Only parallelize truly independent tasks (no shared file modifications)
2. Each agent works on its own branch or own files
3. Each agent reports back: tests written, attempts needed, final status
4. Main agent synthesizes results and handles any conflicts
5. Final full test suite run after all agents complete

### Sub-Agent Template

```
Task(
  subagent_type="implementer",
  model="sonnet",
  description="TDD: <task>",
  prompt="""
  You are a TDD agent. Follow this exact loop:

  1. READ: Understand the code at <file_path>
  2. RED: Write a failing test in <test_dir>/<test_file>
     Run: <test_command> -k "<test_name>"
     Verify it FAILS.
  3. GREEN: Implement the minimum fix in <file_path>
     Run: <test_command> -k "<test_name>"
     If fails, retry up to 3 times with different approaches.
  4. SUITE: Run full suite: <full_test_command>
     If regressions, fix implementation (not old tests). Max 2 cycles.
  5. COMMIT: Only after all green.
     git add <files> && git commit -m "<message>"

  Task: <task_description>
  File: <file_path>
  Test framework: <framework>
  Test command: <test_command>
  Full suite: <full_test_command>

  Report back:
  - Test written: <test name>
  - Attempts needed: <N>
  - Status: PASS / FAIL / ESCALATED
  - Regressions found: <count>
  """
)
```

## Summary Report

After all items complete, print:

```
## TDD Agent Results

| Task | Test Written | Attempts | Status | Regressions |
|------|-------------|----------|--------|-------------|
| Fix auth validation | test_auth_rejects_expired_token | 1 | ✅ PASS | 0 |
| Add rate limiting | test_rate_limit_blocks_excess | 2 | ✅ PASS | 1 (fixed) |
| Sanitize input | test_strips_html_tags | 3 | ❌ ESCALATED | 0 |

Full suite: 142/142 passing
New tests added: 2
Commits: 2
```

## Critical Rules

1. **NEVER write implementation before the test** - this defeats the purpose
2. **NEVER modify existing tests to make them pass** - fix the implementation instead
3. **NEVER skip the full suite run** - regressions are the #1 reason TDD exists
4. **NEVER commit with failing tests** - the commit gate is non-negotiable
5. **Test names should describe the behavior, not the implementation**
6. **One logical change per RED-GREEN-REFACTOR cycle** - keep cycles small
7. **If the test already passes, the behavior exists** - move on
