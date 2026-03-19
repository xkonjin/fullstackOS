# fullstackOS — AI development environment
# Usage: make setup | make doctor | make status | make start | make stop

SHELL := /bin/bash
FULLSTACKOS_DIR := $(shell cd "$(dir $(lastword $(MAKEFILE_LIST)))" && pwd)
HOME_DIR := $(HOME)
LOCAL_BIN := $(HOME_DIR)/.local/bin
LAUNCH_AGENTS := $(HOME_DIR)/Library/LaunchAgents
BUN := $(shell command -v bun 2>/dev/null || echo $(HOME_DIR)/.bun/bin/bun)
BUN_DIR := $(dir $(BUN:%/=%))

.PHONY: setup doctor status start stop services agents fleet skills modules orchestrator claudemax-doctor clean clean-orphans help theorist-check theorist-sync power-server test test-count

theorist-sync: ## Generate repo notes and sync theorist into Obsidian vault
	@python3 $(FULLSTACKOS_DIR)/scripts/theorist/generate_repo_notes.py
	@bash $(FULLSTACKOS_DIR)/scripts/theorist/sync_obsidian.sh "~/Documents/Notes"
	@python3 $(FULLSTACKOS_DIR)/scripts/theorist/validate.py --root $(FULLSTACKOS_DIR)/docs/theorist

theorist-check: ## Validate theorist notes schema/sections
	@python3 $(FULLSTACKOS_DIR)/scripts/theorist/validate.py --root $(FULLSTACKOS_DIR)/docs/theorist

# ─── Main targets ──────────────────────────────────────────────────────

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup: prereqs services agents fleet skills modules orchestrator scripts ## Full setup — everything from scratch
	@echo ""
	@echo "✅ fullstackOS installed. Run 'make doctor' to verify."

prereqs: ## Check prerequisites (git, python3, bun, jq, curl)
	@echo "=== Checking prerequisites ==="
	@command -v git >/dev/null || (echo "❌ git not found" && exit 1)
	@command -v python3 >/dev/null || (echo "❌ python3 not found" && exit 1)
	@command -v jq >/dev/null || (echo "❌ jq not found" && exit 1)
	@command -v curl >/dev/null || (echo "❌ curl not found" && exit 1)
	@command -v bun >/dev/null || (echo "⚠️  bun not found — orchestrator won't build" && true)
	@mkdir -p $(LOCAL_BIN) $(LAUNCH_AGENTS)
	@echo "✅ Prerequisites OK"

# ─── Services ──────────────────────────────────────────────────────────

services: svc-cliproxyapi svc-gateway svc-token-sync ## Install all background services

svc-cliproxyapi: ## Install CLIProxyAPI (Homebrew)
	@echo "=== CLIProxyAPI (port 8317) ==="
	@bash $(FULLSTACKOS_DIR)/services/cliproxyapi/setup.sh

svc-gateway: ## Install Fleet Gateway (port 4105)
	@echo "=== Fleet Gateway (port 4105) ==="
	@cp $(FULLSTACKOS_DIR)/services/fleet-gateway/ai-fleet-gateway $(LOCAL_BIN)/ai-fleet-gateway 2>/dev/null || true
	@chmod +x $(LOCAL_BIN)/ai-fleet-gateway 2>/dev/null || true
	@sed -e "s|__REPO_ROOT__|$(FULLSTACKOS_DIR)|g" -e "s|__HOME__|$(HOME_DIR)|g" \
		$(FULLSTACKOS_DIR)/launchd/com.ai.fleet.gateway.plist > $(LAUNCH_AGENTS)/com.ai.fleet.gateway.plist
	@echo "✅ Gateway configured"

svc-token-sync: ## Install token sync service (every 30min)
	@echo "=== Token Sync (every 30min) ==="
	@sed -e "s|__REPO_ROOT__|$(FULLSTACKOS_DIR)|g" -e "s|__HOME__|$(HOME_DIR)|g" \
		$(FULLSTACKOS_DIR)/launchd/com.ai.token-sync.plist > $(LAUNCH_AGENTS)/com.ai.token-sync.plist
	@echo "✅ Token sync configured"

# ─── Orchestrator (orchestratorv2 — port 8318) ────────────────────────

orchestrator: ## Build and install ClaudeMax orchestrator
	@echo "=== ClaudeMax Orchestrator (port 8318) ==="
	@if command -v bun >/dev/null; then \
		cd $(FULLSTACKOS_DIR)/services/orchestrator && bun install --frozen-lockfile 2>/dev/null || bun install; \
		mkdir -p $(HOME_DIR)/.claudemax/logs $(HOME_DIR)/.claudemax/fleet-jobs; \
		sed -e "s|__REPO_ROOT__|$(FULLSTACKOS_DIR)|g" -e "s|__HOME__|$(HOME_DIR)|g" \
			-e "s|__BUN__|$(BUN)|g" \
			-e "s|__BUN_DIR__|$(BUN_DIR)|g" \
			$(FULLSTACKOS_DIR)/launchd/com.claudemax.orchestrator.plist > $(LAUNCH_AGENTS)/com.claudemax.orchestrator.plist; \
		echo "✅ Orchestrator installed"; \
	else \
		echo "⚠️  bun not found — skipping orchestrator build"; \
	fi

# ─── Agents ────────────────────────────────────────────────────────────

agents: ## Deploy agent configs (Claude, Codex, Droid, OpenCode)
	@echo "=== Agent configs ==="
	@bash $(FULLSTACKOS_DIR)/agents/claude-code/setup.sh 2>/dev/null || \
		(mkdir -p $(HOME_DIR)/.claude && rsync -a $(FULLSTACKOS_DIR)/agents/claude-code/ $(HOME_DIR)/.claude/ --exclude=setup.sh)
	@mkdir -p $(HOME_DIR)/.codex && cp -n $(FULLSTACKOS_DIR)/agents/codex/config.toml $(HOME_DIR)/.codex/config.toml 2>/dev/null || true
	@mkdir -p $(HOME_DIR)/.config/droid && rsync -a $(FULLSTACKOS_DIR)/agents/droid/ $(HOME_DIR)/.config/droid/
	@mkdir -p $(HOME_DIR)/.config/opencode && rsync -a $(FULLSTACKOS_DIR)/agents/opencode/ $(HOME_DIR)/.config/opencode/
	@echo "✅ Agent configs deployed"

# ─── Fleet ─────────────────────────────────────────────────────────────

fleet: ## Install Fleet CLI + Coordinator
	@echo "=== Fleet CLI + Coordinator ==="
	@ln -sf $(FULLSTACKOS_DIR)/fleet/bin/ai-fleet $(LOCAL_BIN)/ai-fleet
	@ln -sf $(FULLSTACKOS_DIR)/coordinator/ai_coordinator.py $(LOCAL_BIN)/ai-coordinator
	@chmod +x $(FULLSTACKOS_DIR)/fleet/bin/ai-fleet $(FULLSTACKOS_DIR)/coordinator/ai_coordinator.py
	@mkdir -p $(HOME_DIR)/.ai-fleet/coordinator
	@cp -n $(FULLSTACKOS_DIR)/config/coordinator.json.template $(HOME_DIR)/.ai-fleet/coordinator.json 2>/dev/null || true
	@cp -n $(FULLSTACKOS_DIR)/config/gateway.json.template $(HOME_DIR)/.ai-fleet/gateway.json 2>/dev/null || true
	@echo "✅ Fleet CLI + Coordinator installed"

# ─── Skills ────────────────────────────────────────────────────────────

skills: ## Symlink all 40 skills to ~/.claude/skills/
	@echo "=== Skills ==="
	@mkdir -p $(HOME_DIR)/.claude/skills
	@for skill in $(FULLSTACKOS_DIR)/skills/*/; do \
		name=$$(basename "$$skill"); \
		if [ "$$name" != "lib" ] && [ "$$name" != "data" ] && [ "$$name" != "references" ]; then \
			ln -sfn "$$skill" "$(HOME_DIR)/.claude/skills/$$name"; \
		fi; \
	done
	@echo "✅ $$(ls -d $(FULLSTACKOS_DIR)/skills/*/ 2>/dev/null | wc -l | tr -d ' ') skills linked"

# ─── Modules ───────────────────────────────────────────────────────────

modules: ## Install absorbed modules (claude-mem, reflect, auth-manager, sentinel, etc.)
	@echo "=== Modules ==="
	@# claude-mem
	@if [ -d "$(FULLSTACKOS_DIR)/modules/claude-mem" ]; then \
		echo "  → claude-mem plugin"; \
	fi
	@# claude-reflect
	@if [ -d "$(FULLSTACKOS_DIR)/modules/claude-reflect" ]; then \
		echo "  → claude-reflect plugin"; \
	fi
	@# auth-manager
	@if [ -d "$(FULLSTACKOS_DIR)/modules/auth-manager" ]; then \
		ln -sf $(FULLSTACKOS_DIR)/modules/auth-manager/bin/auth-manage $(LOCAL_BIN)/ai-auth-manage 2>/dev/null || true; \
		echo "  → auth-manager"; \
	fi
	@# codex-orchestrator
	@if [ -d "$(FULLSTACKOS_DIR)/modules/codex-orchestrator" ]; then \
		echo "  → codex-orchestrator"; \
	fi
	@# sentinel
	@if [ -d "$(FULLSTACKOS_DIR)/modules/sentinel" ]; then \
		echo "  → sentinel watchdog"; \
		pip3 install --quiet -r $(FULLSTACKOS_DIR)/modules/sentinel/requirements.txt 2>/dev/null || true; \
		if [ -f "$(FULLSTACKOS_DIR)/launchd/com.sentinel.watchdog.plist" ]; then \
			mkdir -p $(HOME_DIR)/.sentinel/logs; \
			sed -e "s|__REPO_ROOT__|$(FULLSTACKOS_DIR)|g" -e "s|__HOME__|$(HOME_DIR)|g" \
				$(FULLSTACKOS_DIR)/launchd/com.sentinel.watchdog.plist > $(LAUNCH_AGENTS)/com.sentinel.watchdog.plist; \
		fi; \
	fi
	@echo "✅ Modules installed"

# ─── Scripts ───────────────────────────────────────────────────────────

scripts: ## Install consolidated CLI scripts to ~/.local/bin/
	@echo "=== Scripts ==="
	@for script in $(FULLSTACKOS_DIR)/scripts/*; do \
		name=$$(basename "$$script"); \
		ln -sf "$$script" "$(LOCAL_BIN)/$$name"; \
		chmod +x "$$script"; \
	done
	@echo "✅ $$(ls $(FULLSTACKOS_DIR)/scripts/ | wc -l | tr -d ' ') scripts linked"

# ─── Start / Stop ─────────────────────────────────────────────────────

start: ## Start all background services
	@echo "=== Starting services ==="
	@launchctl load $(LAUNCH_AGENTS)/com.ai.fleet.gateway.plist 2>/dev/null || true
	@launchctl load $(LAUNCH_AGENTS)/com.claudemax.orchestrator.plist 2>/dev/null || true
	@launchctl load $(LAUNCH_AGENTS)/com.ai.token-sync.plist 2>/dev/null || true
	@echo "✅ Services started (CLIProxyAPI managed by Homebrew)"

stop: ## Stop all background services
	@echo "=== Stopping services ==="
	@launchctl unload $(LAUNCH_AGENTS)/com.ai.fleet.gateway.plist 2>/dev/null || true
	@launchctl unload $(LAUNCH_AGENTS)/com.claudemax.orchestrator.plist 2>/dev/null || true
	@launchctl unload $(LAUNCH_AGENTS)/com.ai.token-sync.plist 2>/dev/null || true
	@echo "✅ Services stopped"

restart: stop start ## Restart all services

# ─── Doctor ────────────────────────────────────────────────────────────

doctor: ## Full health check
	@./install.sh --doctor

claudemax-doctor: ## Check claudemax proxy-mode readiness
	@$(LOCAL_BIN)/claude-smart doctor

test: ## Run all tests
	@python3 -m pytest tests/ -x --tb=short -q

test-count: ## Show current test count
	@python3 -m pytest tests/ --collect-only -q 2>/dev/null | tail -1

power-server: ## Configure Mac for always-on server mode (prevents sleep)
	@echo "=== Configuring server power mode ==="
	@sudo pmset -a sleep 0
	@sudo pmset -a disksleep 0
	@sudo pmset -a displaysleep 15
	@sudo pmset -a womp 1
	@sudo pmset -a autorestart 1
	@echo "✅ Server power mode configured (sleep disabled, auto-restart on power failure)"

# ─── Status ────────────────────────────────────────────────────────────

status: ## Quick status of all services
	@echo "=== Service Status ==="
	@printf "%-25s %s\n" "Service" "Status"
	@printf "%-25s %s\n" "─────────────────────────" "──────────"
	@printf "%-25s " "CLIProxyAPI :8317" && (curl -sf http://127.0.0.1:8317/ >/dev/null 2>&1 && echo "✅ UP" || echo "❌ DOWN")
	@printf "%-25s " "Orchestrator :8318" && (curl -sf http://127.0.0.1:8318/health >/dev/null 2>&1 && echo "✅ UP" || echo "❌ DOWN")
	@printf "%-25s " "Gateway :4105" && (curl -sf http://127.0.0.1:4105/v1/health >/dev/null 2>&1 && echo "✅ UP" || echo "❌ DOWN")
	@printf "%-25s " "Task API :4106" && (curl -sf http://127.0.0.1:4106/v1/health >/dev/null 2>&1 && echo "✅ UP" || echo "❌ DOWN")
	@printf "%-25s " "Token Sync" && (launchctl list | grep -q com.ai.token-sync && echo "✅ LOADED" || echo "❌ NOT LOADED")

# ─── Clean ─────────────────────────────────────────────────────────────

clean: ## Remove deployed symlinks (does NOT touch configs)
	@echo "=== Cleaning symlinks ==="
	@rm -f $(LOCAL_BIN)/ai-fleet $(LOCAL_BIN)/ai-coordinator $(LOCAL_BIN)/ai-fleet-gateway $(LOCAL_BIN)/sync-tokens
	@echo "✅ Cleaned"

clean-orphans: ## Unload and remove orphaned LaunchAgents no longer managed by fullstackOS
	@echo "=== Cleaning orphan LaunchAgents ==="
	@for label in \
		com.ai.cliproxyapi.monitor \
		com.cliproxyapi.healthcheck \
		com.cliproxyapi.tokenrefresh \
		com.ai.cortex.connector \
		com.anthropic.claude.env \
		com.user.claude-reflect-daily \
		com.poe1.autopilot.guard \
		com.poe1.autopilot.supervisor \
		com.ai.service.guardian; do \
		plist="$(LAUNCH_AGENTS)/$$label.plist"; \
		if [ -f "$$plist" ]; then \
			launchctl bootout "gui/$$(id -u)/$$label" 2>/dev/null || true; \
			rm -f "$$plist"; \
			echo "  removed $$label"; \
		fi; \
	done
	@echo "✅ Orphan LaunchAgents cleaned"
