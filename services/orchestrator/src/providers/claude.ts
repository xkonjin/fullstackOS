import type {
  MessagesRequest,
  MessagesResponse,
  ProviderAccount,
} from "../types.ts";
import {
  BaseProvider,
  type ProviderResponse,
} from "./base.ts";

const CLIPROXYAPI_BASE = "http://localhost:8317";
const ANTHROPIC_API = "https://api.anthropic.com";

export class ClaudeProvider extends BaseProvider {
  name = "claude";
  private useProxy: boolean;

  constructor(useProxy = true) {
    super();
    this.useProxy = useProxy;
  }

  async sendRequest(
    account: ProviderAccount,
    request: MessagesRequest,
    signal?: AbortSignal,
  ): Promise<ProviderResponse> {
    const baseUrl = this.useProxy ? CLIPROXYAPI_BASE : ANTHROPIC_API;
    const url = `${baseUrl}/v1/messages`;
    const isStreaming = request.stream === true;

    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      "anthropic-version": "2023-06-01",
      "Accept-Encoding": "identity",
    };

    if (this.useProxy) {
      // CLIProxyAPI accepts your-proxy-key key and routes using internal account selection
      // Pass the account email as a hint for account selection
      headers["Authorization"] = `Bearer your-proxy-key`;
      headers["x-account-email"] = account.email ?? "";
      headers["x-account-id"] = account.id;
      // Prevent keep-alive race: Go's net/http closes idle conns before Bun detects it → ECONNRESET
      headers["Connection"] = "close";
    } else {
      // Direct Anthropic API with OAuth token
      headers["Authorization"] = `Bearer ${account.accessToken}`;
    }

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
        return {
          success: false,
          error: "Request aborted",
          errorClass: "timeout",
        };
      }
      if (
        msg.includes("ECONNREFUSED") ||
        msg.includes("ECONNRESET") ||
        msg.includes("socket") ||
        msg.includes("Unable to connect") ||
        msg.includes("fetch failed")
      ) {
        return { success: false, error: msg, errorClass: "connection_error" };
      }
      return { success: false, error: msg, errorClass: "server_error" };
    }
  }
}
