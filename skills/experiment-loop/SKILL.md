---
name: experiment-loop
description: Run fan-out implementation experiments after askquestionsspec or planning. Generates candidate solutions, verifies them with explicit hard gates and soft checks, narrows to the best variants, and iterates until one satisfies the verifier contract or the loop hits its limits. Use when users ask to try multiple implementations, keep iterating until tests pass, prove an implementation, or search for the best verified outcome.
---

# /experiment-loop

Use this skill after requirements are known. Prefer it when a task has multiple plausible implementations, risky regressions, or a strong verifier contract.

## Trigger phrases

- `/experiment-loop <objective>`
- `/experiment-loop spec <path>`
- `/experiment-loop plan <path>`
- "run variants until tests pass"
- "keep iterating until this works"
- "try multiple implementations and verify them"
- "prove this implementation"
- "fan out and test candidates"

## Required inputs

At least one of:

- objective
- `spec_path`
- `plan_path`

And a verifier contract with at least one hard gate.

## Workflow

1. Read or derive the input objective/spec/plan.
2. Build a manifest using `scripts/build_manifest.ts` if one does not already exist.
3. Confirm the verifier contract. If it is incomplete, ask only for the missing contract pieces.
4. Call `POST /v1/fleet/experiment-loops` with the manifest-derived request.
5. Poll `GET /v1/fleet/experiment-loops/:id` until terminal.
6. Surface the winner, evidence, and remaining risks.

Useful request overrides:

- `round_timeout_ms`
- `stagnation_limit`
- `dispatch_policy`

## Output contract

Each run should leave artifacts under:

- `.artifacts/experiment-loops/<loop-id>/manifest.json`
- `.artifacts/experiment-loops/<loop-id>/candidates/*/verifier-summary.json`
- `.artifacts/experiment-loops/<loop-id>/candidates/*/review-summary.md`

Terminal failure reasons to report verbatim:

- `loop_timeout`
- `max_rounds_reached`
- `stagnation_limit_reached`
- `swarm_failed_before_verifier`
- `no_candidates_remaining`

## References

- Verifier design: `references/verifier-contract.md`
- Loop behavior: `references/loop-strategy.md`
- Prompt shaping: `references/prompt-recipes.md`

## Scripts

- `scripts/build_manifest.ts` - normalize objective/spec/plan + verifier into `manifest.json`
- `scripts/run_verifier.ts` - execute hard/soft checks and write `verifier-summary.json`
- `scripts/score_round.ts` - score a round from verifier outputs
- `scripts/derive_next_round.ts` - generate repair/optimize children from scored candidates
