"""Kimi-powered reasoning engine for nanoclaw.

Only invoked on anomalies — not for routine checks.
Uses kimi-k2.5 for fast triage, kimi-k2 for deep diagnosis.
Returns structured JSON actions.
"""

import json
import logging
import urllib.request
import urllib.error

log = logging.getLogger("nanoclaw")

TRIAGE_MODEL = "kimi-k2.5"
DIAGNOSIS_MODEL = "kimi-k2"
TRIAGE_MAX_TOKENS = 800
DIAGNOSIS_MAX_TOKENS = 2000

SYSTEM_PROMPT = """You are nanoclaw, an infrastructure self-healing daemon for the claudemax system.
You diagnose failures in a multi-provider AI proxy stack:
- CLIProxyAPI (port 8317): Go proxy routing to 75+ models across 9 providers
- Orchestrator (port 8318): Bun/TypeScript account health manager, 22 accounts
- Fleet Gateway (port 4105): Python fleet dispatch coordinator
- Providers: Claude, Codex, Gemini, Kimi, Antigravity, GLM, MiniMax, OpenRouter

Your job: analyze the failure context and return a JSON action.
You must respond with ONLY valid JSON, no markdown, no explanation outside the JSON.

Available actions:
- {"action": "restart", "target": "<service_name>", "explanation": "..."}
- {"action": "refresh_token", "provider": "<provider_name>", "explanation": "..."}
- {"action": "alert_only", "explanation": "..."}
- {"action": "no_action", "explanation": "..."}

Rules:
- Never suggest config changes (Level 4 is excluded)
- Never suggest actions outside restart/refresh/alert
- If unsure, choose alert_only
- Be concise in explanations (under 100 words)
"""


class KimiReasoner:
    def __init__(self, proxy_url: str, dry_run: bool = False):
        self.proxy_url = proxy_url
        self.dry_run = dry_run

    def diagnose(self, issue_type: str, context: dict) :
        """Ask Kimi to diagnose an issue and recommend an action.

        Returns dict with: action, explanation, target/provider, tokens_used
        """
        if self.dry_run:
            log.info(f"[DRY RUN] Would ask Kimi about: {issue_type}")
            return {"action": "alert_only", "explanation": "dry run", "tokens_used": 0}

        # Choose model based on complexity
        is_complex = (
            context.get("still_failing") or
            context.get("restart_attempted") or
            issue_type in ("multi_failure", "cascade", "unknown")
        )
        model = DIAGNOSIS_MODEL if is_complex else TRIAGE_MODEL
        max_tokens = DIAGNOSIS_MAX_TOKENS if is_complex else TRIAGE_MAX_TOKENS

        user_msg = f"Issue type: {issue_type}\nContext: {json.dumps(context, indent=2, default=str)}"

        try:
            data = json.dumps({
                "model": model,
                "max_tokens": max_tokens,
                "response_format": {"type": "json_object"},
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_msg},
                ],
            }).encode()

            req = urllib.request.Request(
                f"{self.proxy_url}/v1/chat/completions",
                data=data,
                headers={
                    "Authorization": "Bearer your-proxy-key",
                    "Content-Type": "application/json",
                },
                method="POST",
            )

            with urllib.request.urlopen(req, timeout=60) as resp:
                result = json.loads(resp.read())

            choice = result.get("choices", [{}])[0]
            content = choice.get("message", {}).get("content", "")
            tokens_used = result.get("usage", {}).get("total_tokens", 0)

            if choice.get("finish_reason") != "stop":
                log.warning(f"Kimi response truncated (finish_reason={choice.get('finish_reason')})")

            try:
                action = json.loads(content)
                action["tokens_used"] = tokens_used
                return action
            except json.JSONDecodeError:
                log.error(f"Kimi returned non-JSON: {content[:200]}")
                return {"action": "alert_only", "explanation": f"Kimi parse error: {content[:100]}", "tokens_used": tokens_used}

        except urllib.error.HTTPError as e:
            log.error(f"Kimi HTTP error: {e.code} {e.read().decode()[:200]}")
            return None
        except Exception as e:
            log.error(f"Kimi request failed: {e}")
            return None
