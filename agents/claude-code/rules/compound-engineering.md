# Compound Engineering

## Available Workflow Commands

| Command               | Phase     | Use When                                            |
| --------------------- | --------- | --------------------------------------------------- |
| /askquestionsspec     | Discovery | Starting any new project - ask deep questions first |
| /workflows:brainstorm | Explore   | Unclear requirements                                |
| /workflows:plan       | Plan      | Before implementing any non-trivial feature         |
| /workflows:work       | Execute   | Implementing from a plan                            |
| /workflows:review     | Review    | Before merging PRs                                  |
| /workflows:compound   | Document  | After solving a non-trivial problem                 |

## Review Agents by Stack

| Stack      | Primary Agents                                                                              |
| ---------- | ------------------------------------------------------------------------------------------- |
| Python     | kieran-python-reviewer, security-sentinel, performance-oracle, code-simplicity-reviewer     |
| TypeScript | kieran-typescript-reviewer, security-sentinel, performance-oracle, code-simplicity-reviewer |
| Universal  | architecture-strategist, pattern-recognition-specialist, data-integrity-guardian            |

## Memory Integration

- Solutions documented via /workflows:compound are saved to docs/solutions/
- claude-mem indexes these observations for cross-session retrieval
- claude-reflect captures corrections from compound workflow sessions
- Use MCP search tools to find prior solutions before re-solving problems
