import type { MessagesRequest, ProviderAccount } from "../types.ts";
import { BaseProvider, type ProviderResponse } from "./base.ts";
import { OpenAICompatProvider } from "./openai-compat.ts";

// Gemini supports OpenAI-compatible API, so we delegate to OpenAICompatProvider.
// This wrapper exists for future Gemini-specific logic (native Gemini API, etc.)

export class GeminiProvider extends BaseProvider {
  name = "gemini";
  private inner: OpenAICompatProvider;

  constructor(useProxy = true, baseUrl?: string) {
    super();
    this.inner = new OpenAICompatProvider(
      "gemini",
      baseUrl ?? "https://generativelanguage.googleapis.com/v1beta/openai",
      useProxy,
    );
  }

  async sendRequest(
    account: ProviderAccount,
    request: MessagesRequest,
    signal?: AbortSignal,
  ): Promise<ProviderResponse> {
    return this.inner.sendRequest(account, request, signal);
  }
}
