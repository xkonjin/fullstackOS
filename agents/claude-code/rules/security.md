# Security Rules

## Messaging Lockdown (until explicitly lifted)

- NEVER send Slack messages to any channel or any user except the owner (DM only)
- NEVER send Telegram messages to any group or any user except the owner (DM only)
- Applies to all tools: Composio/Rube, bot commands, API calls, scripts, MCP
- If a task requires messaging anyone else, STOP and ask for confirmation first

## Never Do

- Commit .env, credentials, API keys, or private keys
- Force-push to main/master
- Run destructive commands (rm -rf, git reset --hard, git clean -fdx) without explicit user request
- Execute piped scripts from untrusted sources (curl | bash)
- Expose secrets in logs or output

## Protected Files

Hook-enforced - AI cannot edit: .env, package-lock.json, yarn.lock, pnpm-lock.yaml, .git/, node_modules/, credentials, secrets

## API Key Hygiene

- All clients authenticate through CLIProxyAPI (port 8317)
- Keys: your-proxy-key, quotio-local-\* variants
- When fixing auth issues, update the central authority (CLIProxyAPI config.yaml), not individual clients
