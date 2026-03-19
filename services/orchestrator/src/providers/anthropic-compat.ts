import type { MessagesRequest, MessagesResponse, ProviderAccount } from "../types.ts";
import { BaseProvider, type ProviderResponse } from "./base.ts";

/**
 * Provider for third-party APIs that expose Anthropic-compatible /v1/messages endpoints.
 * e.g., Z.AI GLM (https://api.z.ai/api/anthropic), MiniMax (https://api.minimax.io/anthropic)
 * Uses x-api-key auth directly — no CLIProxyAPI needed.
 */
export class AnthropicCompatProvider extends BaseProvider {
  name: string;
  private baseUrl: string;

  constructor(name: string, baseUrl: string) {
    super();
    this.name = name;
    this.baseUrl = baseUrl;
  }

  async sendRequest(
    account: ProviderAccount,
    request: MessagesRequest,
    signal?: AbortSignal,
  ): Promise<ProviderResponse> {
    const url = `${this.baseUrl}/v1/messages`;
    const isStreaming = request.stream === true;

    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      "anthropic-version": "2023-06-01",
      "Accept-Encoding": "identity",
      "x-api-key": account.accessToken,
    };

    try {
      const resp = await fetch(url, {
        method: "POST",
        headers,
        body: JSON.stringify(request),
        signal,
      });

      if (!resp.ok) {
        const body = await this.readErrorBody(resp);
        const errorClass = this.classifyError(resp.status, body);
        return {
          success: false,
          error: `${resp.status}: ${body.slice(0, 500)}`,
          errorClass,
        };
      }

      const rateLimitHeaders = this.extractRateLimitHeaders(resp);

      if (isStreaming) {
        return { success: true, response: resp, rateLimitHeaders };
      }

      const body = (await resp.json()) as MessagesResponse;
      return { success: true, body, rateLimitHeaders };
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      if (msg.includes("abort") || msg.includes("AbortError")) {
        return { success: false, error: "Request aborted", errorClass: "timeout" };
      }
      if (
        msg.includes("ECONNREFUSED") ||
        msg.includes("ECONNRESET") ||
        msg.includes("fetch failed")
      ) {
        return { success: false, error: msg, errorClass: "connection_error" };
      }
      return { success: false, error: msg, errorClass: "server_error" };
    }
  }
}
