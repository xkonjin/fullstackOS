# Workflow Rules

## Claude Code Session Integration

- Use /reflect after significant learning sessions to sync to CLAUDE.md
- Use /session-recap at end of session to persist context for next session
- context-reminders.md is auto-injected at session start via SessionStart hook
- Check /view-queue periodically for pending reflections

## Config Authority

- Central authority for API keys: CLIProxyAPI config.yaml (port 8317)
- Central authority for model routing: LiteLLM config.yaml (port 4000)
- When fixing auth issues, update the proxy, not individual clients

## PR Workflow

- Before creating PRs, check for merge conflicts with the target branch
- If conflicts exist, report them and ask the user whether to resolve or close
- Do NOT silently skip conflicting PRs or create PRs that will fail to merge
- After PR creation, verify CI status before declaring success

## Theorist Workflow

- Persistent architecture/decision/runbook outputs must be stored in `docs/theorist/notes/` using schema v1
- Validate theorist notes before finalizing:
  - `python3 scripts/theorist/validate.py --root docs/theorist`
- Machine updates must preserve human-editable prose and append to `## Change Log`
- If a note is replaced, mark old note as `superseded` and link in `links.supersedes`
- Fleet pipelines should emit theorist notes at least for `plan` and `cleanup` stages when enabled
- Do not write secrets into theorist notes
