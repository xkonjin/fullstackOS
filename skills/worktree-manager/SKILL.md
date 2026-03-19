---
description: "Create, bootstrap (venv/node_modules), and cleanup git worktrees for fleet isolation. Use when dispatching fleet agents to avoid touching the user's working directory."
user-invocable: true
---

# /worktree - Git Worktree Manager for Fleet Isolation

Manages git worktrees for fleet agent isolation. Every fleet dispatch should run in a worktree, not the user's working directory.

## Commands

### Create: `/worktree create <repo> [branch-prefix]`

```bash
REPO="<repo_path>"
PREFIX="${branch_prefix:-fleet}"
BRANCH="$PREFIX/$(date +%Y%m%d-%H%M%S)"
WORKTREE="$REPO/.worktrees/$PREFIX-$(date +%s)"

cd "$REPO"
git fetch origin
git worktree add "$WORKTREE" -b "$BRANCH" origin/main
echo "WORKTREE=$WORKTREE"
echo "BRANCH=$BRANCH"
```

### Bootstrap: Auto-detect and install dependencies

After creating the worktree, detect the project type and bootstrap:

```bash
cd "$WORKTREE"

# Python
if [ -f "pyproject.toml" ] || [ -f "requirements.txt" ]; then
  python3 -m venv .venv
  if [ -f "pyproject.toml" ]; then
    .venv/bin/pip install -e ".[dev]" 2>/dev/null || .venv/bin/pip install -e .
  elif [ -f "requirements.txt" ]; then
    .venv/bin/pip install -r requirements.txt
  fi
fi

# TypeScript/JavaScript (Bun preferred)
if [ -f "package.json" ]; then
  if command -v bun >/dev/null; then
    bun install
  elif [ -f "pnpm-lock.yaml" ]; then
    pnpm install
  else
    npm install
  fi
fi

# Rust
if [ -f "Cargo.toml" ]; then
  cargo build 2>/dev/null
fi
```

### Cleanup: `/worktree cleanup <repo>`

Remove all fleet worktrees and their branches:

```bash
REPO="<repo_path>"
cd "$REPO"

# List worktrees
git worktree list

# Remove fleet worktrees
for wt in $(git worktree list --porcelain | grep "worktree.*\.worktrees/fleet" | awk '{print $2}'); do
  echo "Removing worktree: $wt"
  git worktree remove "$wt" --force 2>/dev/null
done

# Prune stale worktree references
git worktree prune

# Remove merged fleet branches
for branch in $(git branch --list "fleet/*" --merged); do
  echo "Removing branch: $branch"
  git branch -d "$branch" 2>/dev/null
done
```

### Status: `/worktree status <repo>`

```bash
REPO="<repo_path>"
cd "$REPO"
git worktree list
echo "---"
du -sh .worktrees/*/ 2>/dev/null || echo "No worktrees"
```

## Auto-Cleanup Rule

After a fleet PR is merged, automatically clean up:

1. Remove the worktree directory
2. Delete the local branch
3. Prune stale references

## Naming Convention

| Purpose           | Branch Pattern               | Worktree Path                       |
| ----------------- | ---------------------------- | ----------------------------------- |
| Fleet single task | `fleet/<task>-YYYYMMDD`      | `.worktrees/fleet-<timestamp>`      |
| Fleetmax session  | `fleetmax/YYYYMMDD-HHMMSS`   | `.worktrees/fleetmax-<timestamp>`   |
| Fleet review      | `fleet/full-review-YYYYMMDD` | `<repo>-fleet-review` (sibling dir) |
| Pipeline          | `pipeline/<id>`              | `.worktrees/pipeline-<id>`          |
