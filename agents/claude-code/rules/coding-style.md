# Coding Style Rules

## Output Style

- Kolmogorov complexity: minimum description to fully reconstruct the idea
- Tables over prose for structured information
- Architecture diagrams with port numbers for system descriptions
- No AI slop - no filler words, no verbose explanations, no unnecessary hedging

## Code Conventions

- Prefer editing existing files over creating new ones
- No unnecessary comments, docstrings, or type annotations on unchanged code
- Three similar lines of code is better than a premature abstraction
- Only validate at system boundaries (user input, external APIs)
- Don't design for hypothetical future requirements

## Language Defaults

- Primary languages: Python, TypeScript, YAML, Shell
- When writing new code, match the language of the existing module
- TypeScript projects: always use Bun runtime

## Observability

- Every project going live MUST have Sentry error tracking
- Sentry org: `your-sentry-org` (sentry.io)
- Next.js: `@sentry/nextjs` with `sentry.client.config.ts`, `sentry.server.config.ts`, `instrumentation.ts`, `withSentryConfig` wrapper
- Bun/Node: `@sentry/bun` with init at top of entry file
- Python/FastAPI: `sentry-sdk[fastapi]` with `init_sentry()` called before app creation
- DSN goes in env vars (`SENTRY_DSN`, `NEXT_PUBLIC_SENTRY_DSN` for Next.js), never hardcoded
- Create the Sentry project via API: `POST /api/0/teams/your-sentry-org/your-sentry-org/projects/`
- Set up "Alert on new issues" rule for every project
- Production services also get "Error spike (>10/hr)" alert

## Commit Messages

- Imperative mood, concise subject line
- Focus on "why" not "what"
- Reference issue numbers when applicable
