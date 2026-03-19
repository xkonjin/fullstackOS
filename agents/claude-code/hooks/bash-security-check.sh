#!/bin/bash

# Bash Security Check Hook for Claude Code
# Analyzes bash commands for safety before execution

# Read the JSON input from stdin
INPUT=$(cat)

# Extract the command from the JSON input
COMMAND=$(echo "$INPUT" | jq -r '.tool_input.command // empty')

# If no command found, allow by default
if [ -z "$COMMAND" ]; then
    echo '{"permissionDecision": "allow"}'
    exit 0
fi

# Define dangerous patterns
DANGEROUS_PATTERNS=(
    "rm -rf /"
    "rm -rf /*"
    "> /dev/sda"
    "dd if=/dev/zero of=/dev/"
    "mkfs."
    ":(){:|:&};"  # Fork bomb
    "curl .* | bash"
    "wget .* | bash"
    "curl .* | sh"
    "wget .* | sh"
)

# Define credential exposure patterns
CREDENTIAL_PATTERNS=(
    "echo.*API_KEY"
    "echo.*PASSWORD"
    "echo.*SECRET"
    "cat.*\.env"
    "cat.*credentials"
    "cat.*private_key"
)

# Define destructive git patterns
GIT_DANGEROUS=(
    "git.*force.*push"
    "git.*push.*--force"
    "git.*reset.*--hard.*origin"
    "git.*clean.*-fdx"
)

# Check for dangerous patterns
for pattern in "${DANGEROUS_PATTERNS[@]}"; do
    if echo "$COMMAND" | grep -qE "$pattern"; then
        echo "{\"permissionDecision\": \"deny\", \"reason\": \"Blocked: Command matches dangerous pattern - $pattern\"}"
        exit 0
    fi
done

# Check for credential exposure
for pattern in "${CREDENTIAL_PATTERNS[@]}"; do
    if echo "$COMMAND" | grep -qE "$pattern"; then
        echo "{\"permissionDecision\": \"ask\", \"reason\": \"Warning: Command may expose credentials\"}"
        exit 0
    fi
done

# Check for destructive git operations
for pattern in "${GIT_DANGEROUS[@]}"; do
    if echo "$COMMAND" | grep -qE "$pattern"; then
        echo "{\"permissionDecision\": \"ask\", \"reason\": \"Warning: Potentially destructive git operation\"}"
        exit 0
    fi
done

# Check for downloading and executing unknown scripts
if echo "$COMMAND" | grep -qE "(curl|wget).*(http|https)://.*(bash|sh)" && \
   ! echo "$COMMAND" | grep -qE "(nodejs.org|npmjs.com|github.com/[a-zA-Z0-9-]+/[a-zA-Z0-9-]+/releases|get.docker.com)"; then
    echo "{\"permissionDecision\": \"ask\", \"reason\": \"Warning: Downloading and executing script from untrusted source\"}"
    exit 0
fi

# Default: allow safe operations
echo '{"permissionDecision": "allow"}'
