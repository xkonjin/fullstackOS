import type { ErrorClass } from "./types.ts";

export interface BackoffConfig {
  baseMs: number;
  maxMs: number;
  jitter: number; // 0-1, fraction of randomness
}

export const INTERACTIVE_BACKOFF: BackoffConfig = {
  baseMs: 200,
  maxMs: 3_000,
  jitter: 0.3,
};

export const FLEET_BACKOFF: BackoffConfig = {
  baseMs: 500,
  maxMs: 30_000,
  jitter: 0.5,
};

// Extra backoff multipliers by error type (applied to base before exponential)
const ERROR_MULTIPLIER: Partial<Record<ErrorClass, number>> = {
  connection_error: 2.5, // CLIProxyAPI struggling — back off harder
  rate_limit: 2.0,
  server_error: 1.5,
  timeout: 1.0,
};

export function getBackoffMs(
  attempt: number,
  config: BackoffConfig,
  errorClass?: ErrorClass,
): number {
  const multiplier = errorClass ? (ERROR_MULTIPLIER[errorClass] ?? 1) : 1;
  const base = config.baseMs * multiplier;
  const exponential = Math.min(base * 2 ** attempt, config.maxMs);
  const jitterRange = exponential * config.jitter;
  const jitterOffset = (Math.random() - 0.5) * 2 * jitterRange;
  return Math.max(0, Math.round(exponential + jitterOffset));
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
