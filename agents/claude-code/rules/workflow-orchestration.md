# Workflow Orchestration Rules

## Plan Mode

- Enter plan mode for ANY task with 3+ steps or architectural decisions
- If implementation goes sideways, STOP and re-plan - don't push through a broken approach
- Write specs upfront: WHAT / WHY / WHERE / HOW / VERIFY
- Plan mode applies to verification steps too, not just building

## Subagent Strategy

- Use subagents to keep main context window clean - offload research and parallel analysis
- One focused task per subagent; don't combine unrelated work
- For complex problems, throw more compute at it via parallel subagents
- Never duplicate work a subagent is already doing

## Self-Improvement Loop

- After ANY correction from the user: run `/reflect` to capture the pattern
- Corrections update `~/.claude/rules/` or project CLAUDE.md (not a separate lessons file)
- Review `/view-queue` at session start for pending corrections
- Use `/workflows:compound` after solving non-trivial problems to index solutions via claude-mem

## Elegance Check (non-trivial changes only)

- Before presenting a non-trivial fix: "is there a more elegant way?"
- If a fix feels hacky, pause and implement the clean solution
- Skip for simple/obvious fixes - don't over-engineer
- Reconciles with scope-lock: elegance applies WITHIN the requested scope, not beyond it
