# Discipline Rules

## Scope Lock

- When asked to save a file, write a plan, or do a simple task - do EXACTLY that
- Do NOT explore the codebase, begin implementation, or expand scope unless explicitly asked
- If the task has one obvious interpretation, execute it immediately without reconnaissance
- When given a numbered plan, execute steps in order - do NOT skip ahead or add steps

## Verification Before Completion

- After implementing changes, verify they work end-to-end before claiming completion
- Run the relevant command, test, or pipeline and show real output
- NEVER say "should work", "appears to be working", or "installed successfully" without proof
- If you cannot verify (no test command, no way to run), say so explicitly

## Environment Targeting

- Default to PRODUCTION environment unless explicitly told to use local/dev
- When .env files exist with "production" or "prod" in the name, prefer those
- NEVER silently fall back to local databases when production is intended
- If you cannot connect to the target environment, STOP and report - do not switch environments

## Hook-Blocked Files

- When a git hook or pre-commit hook blocks a write to .env or protected files, immediately tell the user the exact manual command to run
- Do NOT repeatedly try workarounds - surface the command once and move on
