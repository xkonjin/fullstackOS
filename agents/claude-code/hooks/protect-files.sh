#!/bin/bash
# Protect sensitive files from AI edits
INPUT=$(cat)
FILE_PATH=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty')

PROTECTED_PATTERNS=(".env" "package-lock.json" "yarn.lock" "pnpm-lock.yaml" ".git/" "node_modules/" ".DS_Store" "credentials" "secrets")

for pattern in "${PROTECTED_PATTERNS[@]}"; do
  if [[ "$FILE_PATH" == *"$pattern"* ]]; then
    echo "Blocked: $FILE_PATH matches protected pattern '$pattern'. These files should not be edited by AI." >&2
    exit 2
  fi
done

exit 0
