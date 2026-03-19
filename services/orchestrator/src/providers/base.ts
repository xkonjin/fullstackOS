import type {
  ErrorClass,
  MessagesRequest,
  MessagesResponse,
  ProviderAccount,
} from "../types.ts";

export interface RateLimitHeaders {
  remaining: number | null;
  limit: number | null;
  resetSeconds: number | null;
}

export interface ProviderResponse {
  success: boolean;
  response?: Response; // Raw HTTP response (for streaming passthrough)
  body?: MessagesResponse; // Parsed body (for non-streaming)
  error?: string;
  errorClass?: ErrorClass;
  rateLimitHeaders?: RateLimitHeaders; // Proactive quota info from response headers
}

export abstract class BaseProvider {
  abstract name: string;

  abstract sendRequest(
    account: ProviderAccount,
    request: MessagesRequest,
    signal?: AbortSignal,
  ): Promise<ProviderResponse>;

  /**
   * Read error response body, handling gzip-encoded responses that CLIProxyAPI
   * sometimes returns as raw bytes instead of decompressed text.
   * Detects gzip magic bytes (0x1f 0x8b) and decompresses, falling back to
   * lossy UTF-8 decode with binary bytes stripped.
   */
  async readErrorBody(resp: Response): Promise<string> {
    try {
      const buf = new Uint8Array(await resp.arrayBuffer());
      // Detect gzip magic bytes
      if (buf.length >= 2 && buf[0] === 0x1f && buf[1] === 0x8b) {
        try {
          const ds = new DecompressionStream("gzip");
          const writer = ds.writable.getWriter();
          const reader = ds.readable.getReader();
          writer.write(buf);
          writer.close();
          const MAX_DECOMPRESS_SIZE = 50 * 1024 * 1024; // 50MB
          let totalSize = 0;
          const chunks: Uint8Array[] = [];
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            totalSize += value.length;
            if (totalSize > MAX_DECOMPRESS_SIZE) {
              throw new Error(
                `Decompressed response exceeds ${MAX_DECOMPRESS_SIZE} bytes`,
              );
            }
            chunks.push(value);
          }
          const totalLen = chunks.reduce((s, c) => s + c.length, 0);
          const merged = new Uint8Array(totalLen);
          let offset = 0;
          for (const c of chunks) {
            merged.set(c, offset);
            offset += c.length;
          }
          return new TextDecoder().decode(merged);
        } catch {
          // Gzip decompression failed — fall through to text decode
        }
      }
      // Normal text decode, replacing invalid bytes
      const text = new TextDecoder("utf-8", { fatal: false }).decode(buf);
      // Strip non-printable control chars (except newline/tab) that indicate binary corruption
      return text.replace(/[\x00-\x08\x0b\x0c\x0e-\x1f]/g, "");
    } catch {
      // Last resort: use the built-in text()
      return resp.text();
    }
  }

  extractRateLimitHeaders(resp: Response): RateLimitHeaders | undefined {
    const remaining = resp.headers.get("x-ratelimit-remaining-requests");
    const limit = resp.headers.get("x-ratelimit-limit-requests");
    const resetStr = resp.headers.get("x-ratelimit-reset-requests");
    if (remaining === null && limit === null) return undefined;
    let resetSeconds: number | null = null;
    if (resetStr) {
      const h = resetStr.match(/(\d+)h/)?.[1];
      const m = resetStr.match(/(\d+)m/)?.[1];
      const s = resetStr.match(/([\d.]+)s/)?.[1];
      resetSeconds =
        Number(h ?? 0) * 3600 + Number(m ?? 0) * 60 + Math.ceil(Number(s ?? 0));
    }
    return {
      remaining: remaining !== null ? Number(remaining) : null,
      limit: limit !== null ? Number(limit) : null,
      resetSeconds,
    };
  }

  classifyError(status: number, body?: string): ErrorClass {
    // Binary garbage detection FIRST — CLIProxyAPI sometimes returns gzip bytes
    // wrapped in JSON error responses. This can happen with ANY status code
    // (429, 400, 500, etc.) and is always a transport/proxy issue, not a real
    // API error. Must run before status-specific checks to avoid misclassifying
    // garbled 429s as rate_limit or garbled 400s as invalid_request.
    if (body) {
      const sample = body.slice(0, 500);
      if (/[\x00-\x08\x0e-\x1f]/.test(sample)) {
        console.warn(
          `[error-classify] Binary garbage (control chars) in ${status} response — treating as connection_error (proxy/transport issue)`,
        );
        return "connection_error";
      }
      // Actual U+FFFD replacement characters (binary decoded as UTF-8)
      const replacementCount = (sample.match(/\ufffd/g) || []).length;
      if (replacementCount >= 3) {
        console.warn(
          `[error-classify] Binary garbage (${replacementCount} U+FFFD) in ${status} response — treating as connection_error (proxy/transport issue)`,
        );
        return "connection_error";
      }
      // JSON-escaped binary garbage — CLIProxyAPI's Go json.Marshal wraps gzip
      // bytes as JSON escape sequences: \ufffd (literal 6-char text) instead of
      // actual U+FFFD (3-byte EF BF BD). Also detect \u001f (gzip magic byte 1).
      const jsonEscapedCount = (sample.match(/\\ufffd/g) || []).length;
      if (
        jsonEscapedCount >= 3 ||
        (/\\u001f/.test(sample) && jsonEscapedCount >= 1)
      ) {
        console.warn(
          `[error-classify] Binary garbage (${jsonEscapedCount} JSON-escaped \\ufffd + gzip magic) in ${status} response — treating as connection_error (proxy/transport issue)`,
        );
        return "connection_error";
      }
    }
    if (status === 429) return "rate_limit";
    if (status === 401 || status === 403) return "auth_error";
    // Deactivated workspace is permanent — exclude account, don't retry
    if (status === 402 && body?.includes("deactivated_workspace"))
      return "auth_error";
    if (status === 402) return "rate_limit"; // quota exhausted → rotate account
    if (status === 404) return "model_unavailable";
    // Context length exceeded — detect before generic invalid_request so we can fallback.
    // Status-agnostic: CLIProxyAPI may wrap upstream 400 as 500, and some providers
    // return context errors with different status codes.
    if (
      body &&
      (body.includes("prompt is too long") ||
        body.includes("context_length_exceeded") ||
        body.includes("maximum context length") ||
        body.includes("context window limit") ||
        body.includes("reached its context window") ||
        body.includes("too many tokens") ||
        body.includes("token limit exceeded") ||
        body.includes("exceeds the model's maximum"))
    )
      return "context_length";
    if (status === 400 && body?.includes("invalid_request_error"))
      return "invalid_request"; // request format issue — don't retry with other providers
    // CLIProxyAPI returns 500 with "auth_unavailable" when it has no usable token.
    // Classify as auth_unavailable so the orchestrator parks the account temporarily
    // (5min) instead of retrying it on every request and wasting retry budget.
    if (body?.includes("auth_unavailable")) return "auth_unavailable";
    // Genuine upstream auth rejection — token is permanently invalid
    if (body?.includes("invalid_token")) return "auth_error";
    // CLIProxyAPI wraps upstream network failures as 500 — these are systemic,
    // not per-account issues. Classify as connection_error so circuit breaker skips them.
    if (
      status >= 500 &&
      body &&
      (body.includes("no such host") ||
        body.includes("no route to host") ||
        body.includes("TLS handshake timeout") ||
        body.includes("unexpected EOF") ||
        body.includes("dial tcp") ||
        body.includes("connection refused") ||
        body.includes("connect: network is unreachable"))
    )
      return "connection_error";
    if (status >= 500) return "server_error";
    if (body?.includes("rate_limit") || body?.includes("rate limit"))
      return "rate_limit";
    if (body?.includes("quota") || body?.includes("RESOURCE_EXHAUSTED"))
      return "rate_limit";
    return "unknown";
  }

  isRetryable(errorClass: ErrorClass): boolean {
    return (
      errorClass === "rate_limit" ||
      errorClass === "server_error" ||
      errorClass === "timeout" ||
      errorClass === "connection_error"
    );
  }
}
