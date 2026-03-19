import { Database } from "bun:sqlite";
import { dirname } from "path";
import { mkdirSync, existsSync } from "fs";
import type { UsageRecord } from "../types.ts";

const SCORING_MAX_RECENT_ROWS = 20_000;

export class UsageDB {
  private db: Database;
  private insertStmt: ReturnType<Database["prepare"]>;
  private cleanupInterval: ReturnType<typeof setInterval> | null = null;

  private escapeLike(input: string): string {
    return input
      .replace(/\\/g, "\\\\")
      .replace(/%/g, "\\%")
      .replace(/_/g, "\\_");
  }

  constructor(dbPath: string) {
    const dir = dirname(dbPath);
    if (!existsSync(dir)) mkdirSync(dir, { recursive: true });

    this.db = new Database(dbPath, { create: true });
    this.db.exec("PRAGMA journal_mode = WAL");
    this.db.exec("PRAGMA busy_timeout = 5000");

    this.db.exec(`
      CREATE TABLE IF NOT EXISTS requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER NOT NULL,
        provider TEXT NOT NULL,
        model TEXT NOT NULL,
        account_id TEXT NOT NULL,
        input_tokens INTEGER NOT NULL DEFAULT 0,
        output_tokens INTEGER NOT NULL DEFAULT 0,
        latency_ms INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'success',
        error TEXT
      )
    `);

    this.db.exec(`
      CREATE INDEX IF NOT EXISTS idx_requests_timestamp ON requests(timestamp);
      CREATE INDEX IF NOT EXISTS idx_requests_provider ON requests(provider);
      CREATE INDEX IF NOT EXISTS idx_requests_account ON requests(account_id);
      CREATE INDEX IF NOT EXISTS idx_requests_provider_time_status_latency
        ON requests(provider, timestamp, status, latency_ms);
    `);

    this.insertStmt = this.db.prepare(`
      INSERT INTO requests (timestamp, provider, model, account_id, input_tokens, output_tokens, latency_ms, status, error)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    // Auto-cleanup every hour
    this.cleanupInterval = setInterval(() => this.cleanup(), 3600_000);
  }

  record(usage: UsageRecord): void {
    this.insertStmt.run(
      usage.timestamp,
      usage.provider,
      usage.model,
      usage.account_id,
      usage.input_tokens,
      usage.output_tokens,
      usage.latency_ms,
      usage.status,
      usage.error,
    );
  }

  getUsageByProvider(since?: number): {
    provider: string;
    requests: number;
    input_tokens: number;
    output_tokens: number;
    errors: number;
  }[] {
    const cutoff = since ?? Date.now() - 86400_000; // default: last 24h
    return this.db
      .prepare(
        `SELECT provider,
                COUNT(*) as requests,
                SUM(input_tokens) as input_tokens,
                SUM(output_tokens) as output_tokens,
                SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as errors
         FROM requests WHERE timestamp > ?
         GROUP BY provider ORDER BY requests DESC`,
      )
      .all(cutoff) as {
      provider: string;
      requests: number;
      input_tokens: number;
      output_tokens: number;
      errors: number;
    }[];
  }

  getUsageByAccount(since?: number): {
    account_id: string;
    provider: string;
    requests: number;
    input_tokens: number;
    output_tokens: number;
    errors: number;
  }[] {
    const cutoff = since ?? Date.now() - 86400_000;
    return this.db
      .prepare(
        `SELECT account_id, provider,
                COUNT(*) as requests,
                SUM(input_tokens) as input_tokens,
                SUM(output_tokens) as output_tokens,
                SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as errors
         FROM requests WHERE timestamp > ?
         GROUP BY account_id, provider ORDER BY requests DESC`,
      )
      .all(cutoff) as {
      account_id: string;
      provider: string;
      requests: number;
      input_tokens: number;
      output_tokens: number;
      errors: number;
    }[];
  }

  getErrorRates(
    since?: number,
  ): { provider: string; total: number; errors: number; rate: number }[] {
    const cutoff = since ?? Date.now() - 3600_000; // default: last hour
    return this.db
      .prepare(
        `SELECT provider,
                COUNT(*) as total,
                SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as errors,
                ROUND(CAST(SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) * 100, 1) as rate
         FROM requests WHERE timestamp > ?
         GROUP BY provider ORDER BY rate DESC`,
      )
      .all(cutoff) as {
      provider: string;
      total: number;
      errors: number;
      rate: number;
    }[];
  }

  getUsageByAccountEmail(
    provider: string,
    email: string,
    since: number,
  ): {
    requests: number;
    input_tokens: number;
    output_tokens: number;
    errors: number;
  } | null {
    const escapedEmail = this.escapeLike(email);
    const row = this.db
      .prepare(
        `SELECT COUNT(*) as requests,
                COALESCE(SUM(input_tokens), 0) as input_tokens,
                COALESCE(SUM(output_tokens), 0) as output_tokens,
                SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as errors
         FROM requests
         WHERE provider = ? AND account_id LIKE ? ESCAPE '\\' AND timestamp > ?`,
      )
      .get(provider, `%${escapedEmail}%`, since) as {
      requests: number;
      input_tokens: number;
      output_tokens: number;
      errors: number;
    } | null;
    return row;
  }

  getTokensByHourForAccount(
    provider: string,
    email: string,
    since: number,
  ): Record<string, number> {
    const escapedEmail = this.escapeLike(email);
    const rows = this.db
      .prepare(
        `SELECT CAST(((timestamp - ?) / 3600000) AS INTEGER) as hour,
                SUM(input_tokens + output_tokens) as tokens
         FROM requests
         WHERE provider = ? AND account_id LIKE ? ESCAPE '\\' AND timestamp > ?
         GROUP BY hour ORDER BY hour`,
      )
      .all(since, provider, `%${escapedEmail}%`, since) as {
      hour: number;
      tokens: number;
    }[];
    const result: Record<string, number> = {};
    for (const row of rows) {
      result[String(row.hour)] = row.tokens;
    }
    return result;
  }

  getTokensByHour(provider: string, since: number): Record<string, number> {
    const rows = this.db
      .prepare(
        `SELECT CAST(((timestamp - ?) / 3600000) AS INTEGER) as hour,
                SUM(input_tokens + output_tokens) as tokens
         FROM requests WHERE provider = ? AND timestamp > ?
         GROUP BY hour ORDER BY hour`,
      )
      .all(since, provider, since) as { hour: number; tokens: number }[];
    const result: Record<string, number> = {};
    for (const row of rows) {
      result[String(row.hour)] = row.tokens;
    }
    return result;
  }

  getTodayTokensByAccount(): Map<string, number> {
    const todayStart = new Date();
    todayStart.setHours(0, 0, 0, 0);
    const rows = this.db
      .prepare(
        `SELECT account_id, SUM(input_tokens + output_tokens) as total_tokens
         FROM requests WHERE timestamp > ?
         GROUP BY account_id`,
      )
      .all(todayStart.getTime()) as { account_id: string; total_tokens: number }[];
    const result = new Map<string, number>();
    for (const row of rows) {
      result.set(row.account_id, row.total_tokens);
    }
    return result;
  }

  getRecentRequests(limit = 50): UsageRecord[] {
    return this.db
      .prepare(`SELECT * FROM requests ORDER BY timestamp DESC LIMIT ?`)
      .all(limit) as UsageRecord[];
  }

  /**
   * Compute per-provider performance scores over a time window.
   * Returns success rate, average latency, p95 latency, total tokens, and request count.
   * Used by LearningRouter to adaptively reorder tier priorities.
   */
  getProviderScores(since?: number): {
    provider: string;
    success_rate: number;
    avg_latency_ms: number;
    p95_latency_ms: number;
    total_tokens: number;
    request_count: number;
    error_count: number;
  }[] {
    const cutoff = since ?? Date.now() - 24 * 3600_000;
    const rows = this.db
      .prepare(
        `WITH recent AS (
           SELECT provider, status, latency_ms, input_tokens, output_tokens
           FROM requests
           WHERE timestamp > ?
           ORDER BY timestamp DESC
           LIMIT ?
         ),
         base AS (
           SELECT provider,
                  COUNT(*) AS total,
                  SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS successes,
                  SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS errors,
                  AVG(CASE WHEN status = 'success' THEN latency_ms ELSE NULL END) AS avg_lat,
                  SUM(input_tokens + output_tokens) AS total_tokens
           FROM recent
           GROUP BY provider
         ),
         lat_ranked AS (
           SELECT provider,
                  latency_ms,
                  ROW_NUMBER() OVER (PARTITION BY provider ORDER BY latency_ms ASC) AS rn,
                  COUNT(*) OVER (PARTITION BY provider) AS cnt
           FROM recent
           WHERE status = 'success' AND latency_ms > 0
         ),
         p95 AS (
           SELECT provider, MIN(latency_ms) AS p95_latency_ms
           FROM lat_ranked
           WHERE rn >= max(1, CAST(cnt * 0.95 AS INTEGER))
           GROUP BY provider
         )
         SELECT b.provider AS provider,
                CASE
                  WHEN b.total > 0 THEN CAST(b.successes AS FLOAT) / b.total
                  ELSE 0
                END AS success_rate,
                COALESCE(b.avg_lat, 0) AS avg_latency_ms,
                COALESCE(p.p95_latency_ms, 0) AS p95_latency_ms,
                COALESCE(b.total_tokens, 0) AS total_tokens,
                b.total AS request_count,
                COALESCE(b.errors, 0) AS error_count
         FROM base b
         LEFT JOIN p95 p ON p.provider = b.provider
         ORDER BY b.total DESC`,
      )
      .all(cutoff, SCORING_MAX_RECENT_ROWS) as {
      provider: string;
      success_rate: number;
      avg_latency_ms: number;
      p95_latency_ms: number;
      total_tokens: number;
      request_count: number;
      error_count: number;
    }[];

    return rows;
  }

  /**
   * Get per-provider daily token usage for budget-aware routing.
   * Returns a map of provider → total tokens used today.
   */
  getTodayTokensByProvider(): Map<string, number> {
    const todayStart = new Date();
    todayStart.setHours(0, 0, 0, 0);
    const rows = this.db
      .prepare(
        `SELECT provider, SUM(input_tokens + output_tokens) as total_tokens
         FROM requests WHERE timestamp > ?
         GROUP BY provider`,
      )
      .all(todayStart.getTime()) as {
      provider: string;
      total_tokens: number;
    }[];
    const result = new Map<string, number>();
    for (const row of rows) {
      result.set(row.provider, row.total_tokens);
    }
    return result;
  }

  private cleanup(): void {
    const result = this.db
      .prepare(`DELETE FROM requests WHERE timestamp < ?`)
      .run(Date.now() - 30 * 86400_000);
    if ((result as any).changes > 0) {
      console.log(
        `[usage-db] Cleaned up ${(result as any).changes} records older than 30 days`,
      );
    }
  }

  close(): void {
    if (this.cleanupInterval) clearInterval(this.cleanupInterval);
    this.db.close();
  }
}
