import type {
  ContentBlock,
  MessagesRequest,
  MessagesResponse,
  OpenAIChatMessage,
  OpenAIChatRequest,
  OpenAIToolCall,
  Provider,
  ProviderAccount,
} from "../types.ts";
import { BaseProvider, type ProviderResponse } from "./base.ts";

const CLIPROXYAPI_BASE = "http://localhost:8317";

// Legacy MODEL_MAP removed — all model translation handled by task-router.ts translateModel()
// Models arrive here already translated (e.g. "gpt-5.3-codex" not "claude-opus-4-6")

/**
 * Normalize a tool name against known tools from the request.
 * Non-Claude models (Codex, GLM, Kimi, etc.) sometimes hallucinate tool names
 * with hyphens instead of underscores (e.g. "mcp__posthog__projects-get"
 * instead of "mcp__posthog__projects_get"). This causes Amp/Anthropic to reject
 * the response with "Tool reference not found in available tools".
 */
function normalizeToolName(name: string, knownTools: Set<string>): string {
  if (knownTools.size === 0 || knownTools.has(name)) return name;

  // Try hyphen→underscore (most common hallucination)
  const withUnderscores = name.replace(/-/g, "_");
  if (knownTools.has(withUnderscores)) return withUnderscores;

  // Try underscore→hyphen (reverse)
  const withHyphens = name.replace(/_/g, "-");
  if (knownTools.has(withHyphens)) return withHyphens;

  // Normalize both sides: lowercase + hyphens→underscores
  const normalized = name.toLowerCase().replace(/-/g, "_");
  for (const known of knownTools) {
    if (known.toLowerCase().replace(/-/g, "_") === normalized) return known;
  }

  return name;
}

function normalizeJsonSchema(
  schema: unknown,
  isRoot = true,
): Record<string, unknown> {
  const visit = (node: unknown, root = false): Record<string, unknown> => {
    if (!node || typeof node !== "object" || Array.isArray(node)) {
      // Only coerce non-objects at the root level; leaf nodes (e.g. {type:"string"})
      // should be returned as-is to avoid corrupting valid property schemas.
      return root
        ? { type: "object", properties: {} }
        : ((node as Record<string, unknown>) ?? {});
    }

    const out = { ...(node as Record<string, unknown>) };

    if (out.type === "object") {
      const props = out.properties;
      if (!props || typeof props !== "object" || Array.isArray(props)) {
        out.properties = {};
      }
      if (out.required !== undefined && !Array.isArray(out.required)) {
        delete out.required;
      }
    }

    const properties = out.properties;
    if (
      properties &&
      typeof properties === "object" &&
      !Array.isArray(properties)
    ) {
      for (const [key, value] of Object.entries(
        properties as Record<string, unknown>,
      )) {
        (properties as Record<string, unknown>)[key] = visit(value, false);
      }
    }

    const items = out.items;
    if (items !== undefined) {
      if (Array.isArray(items)) {
        out.items = items.map((item) => visit(item, false));
      } else {
        out.items = visit(items, false);
      }
    }

    for (const key of ["allOf", "anyOf", "oneOf"] as const) {
      const value = out[key];
      if (Array.isArray(value)) {
        out[key] = value.map((entry) => visit(entry, false));
      }
    }

    const defs = out.$defs;
    if (defs && typeof defs === "object" && !Array.isArray(defs)) {
      for (const [key, value] of Object.entries(
        defs as Record<string, unknown>,
      )) {
        (defs as Record<string, unknown>)[key] = visit(value, false);
      }
    }

    return out;
  };

  const normalized = visit(schema, isRoot);
  if (normalized.type !== "object") normalized.type = "object";
  if (
    !normalized.properties ||
    typeof normalized.properties !== "object" ||
    Array.isArray(normalized.properties)
  ) {
    normalized.properties = {};
  }
  if (
    normalized.required !== undefined &&
    !Array.isArray(normalized.required)
  ) {
    delete normalized.required;
  }
  return normalized;
}

/**
 * Strip embedded <think>...</think> blocks from model output text.
 * Some providers (MiniMax) embed reasoning in the content field instead
 * of using the OpenAI reasoning_content field. These tags leak into
 * Claude Code's output as visible text, confusing the agent.
 */
function stripEmbeddedThinking(text: string): string {
  // Remove <think>...</think> blocks (may span multiple lines)
  let stripped = text.replace(/<think>[\s\S]*?<\/think>\s*/g, "");
  // Strip [DONE] if a provider mistakenly returns it as content
  stripped = stripped.replace(/\[DONE\]/g, "");
  return stripped.trim();
}

export class OpenAICompatProvider extends BaseProvider {
  name: string;
  private provider: Provider;
  private baseUrl: string | null;
  private useProxy: boolean;
  private defaultHeaders: Record<string, string>;

  constructor(
    provider: Provider,
    baseUrl?: string,
    useProxy = true,
    defaultHeaders: Record<string, string> = {},
  ) {
    super();
    this.name = provider;
    this.provider = provider;
    this.baseUrl = baseUrl ?? null;
    this.useProxy = useProxy;
    this.defaultHeaders = { ...defaultHeaders };
  }

  private translateToOpenAI(request: MessagesRequest): OpenAIChatRequest {
    const messages: OpenAIChatMessage[] = [];

    // System message
    if (request.system) {
      const systemText =
        typeof request.system === "string"
          ? request.system
          : request.system
              .filter((b) => b.type === "text")
              .map((b) => (b as { type: "text"; text: string }).text)
              .join("\n");
      messages.push({ role: "system", content: systemText });
    }

    // Conversation messages — translate tool_use/tool_result blocks
    for (const msg of request.messages) {
      if (typeof msg.content === "string") {
        messages.push({ role: msg.role, content: msg.content });
        continue;
      }

      // Array content — extract text, tool_use, tool_result blocks
      const textParts: string[] = [];
      const toolUseParts: OpenAIToolCall[] = [];
      const toolResultParts: { tool_call_id: string; content: string }[] = [];

      let hadThinkingBlocks = false;
      for (const block of msg.content) {
        const b = block as Record<string, unknown>;
        if (block.type === "thinking") {
          hadThinkingBlocks = true;
        } else if (block.type === "text") {
          textParts.push((b.text as string) ?? "");
        } else if (block.type === "tool_use") {
          toolUseParts.push({
            id: b.id as string,
            type: "function",
            function: {
              name: b.name as string,
              arguments: JSON.stringify(b.input ?? {}),
            },
          });
        } else if (block.type === "tool_result") {
          let resultText: string;
          if (typeof b.content === "string") {
            resultText = b.content;
          } else if (Array.isArray(b.content)) {
            // Handle mixed content blocks (text, image, etc.) from MCP tools
            const parts: string[] = [];
            for (const c of b.content as {
              type: string;
              text?: string;
              source?: unknown;
            }[]) {
              if (c.type === "text" && c.text) {
                parts.push(c.text);
              } else if (c.type === "image") {
                parts.push("[image content]");
              } else if (c.type === "document") {
                parts.push("[document content]");
              }
            }
            resultText = parts.join("\n") || JSON.stringify(b.content);
          } else {
            resultText = JSON.stringify(b.content ?? "");
          }
          // Prefix error results so the model knows the tool call failed
          if (b.is_error === true) {
            resultText = `[Tool Error] ${resultText}`;
          }
          toolResultParts.push({
            tool_call_id: b.tool_use_id as string,
            content: resultText,
          });
        }
        // Skip thinking blocks silently
      }

      if (msg.role === "assistant") {
        const assistantMsg: OpenAIChatMessage = {
          role: "assistant",
          content: textParts.length > 0 ? textParts.join("\n") : null,
        };
        if (toolUseParts.length > 0) {
          assistantMsg.tool_calls = toolUseParts;
        }
        // Kimi requires reasoning_content on ALL assistant messages with tool_calls,
        // even after thinking is stripped from the request. The conversation history
        // implies thinking was used (the original messages had thinking blocks before
        // stripThinkingFromHistory() removed them). Add reasoning_content unconditionally
        // on assistant tool call messages — providers that don't need it will ignore it.
        if (toolUseParts.length > 0) {
          assistantMsg.reasoning_content = "(thinking)";
        }
        messages.push(assistantMsg);
      } else {
        // User message — tool_results become separate tool messages
        for (const result of toolResultParts) {
          messages.push({
            role: "tool",
            content: result.content,
            tool_call_id: result.tool_call_id,
          });
        }
        if (textParts.length > 0) {
          messages.push({ role: "user", content: textParts.join("\n") });
        }
      }
    }

    const openaiReq: OpenAIChatRequest = {
      model: request.model, // already translated by task-router
      messages,
      max_tokens: request.max_tokens,
      temperature: request.temperature,
      top_p: request.top_p,
      stream: request.stream,
      stop: request.stop_sequences,
    };

    // Forward Anthropic tools → OpenAI function calling
    const tools = (request as Record<string, unknown>).tools;
    if (tools && Array.isArray(tools)) {
      openaiReq.tools = tools.map(
        (tool: {
          name: string;
          description?: string;
          input_schema?: Record<string, unknown>;
        }) => {
          const schema = normalizeJsonSchema(tool.input_schema);
          // Gemini rejects function names > 64 chars
          let toolName = tool.name;
          if (this.provider === "gemini" && toolName.length > 64) {
            toolName = toolName.slice(0, 64);
          }

          return {
            type: "function" as const,
            function: {
              name: toolName,
              description: tool.description,
              parameters: schema,
            },
          };
        },
      );
    }

    // Forward Anthropic tool_choice → OpenAI tool_choice
    const toolChoice = (request as Record<string, unknown>).tool_choice;
    if (toolChoice && typeof toolChoice === "object") {
      const tc = toolChoice as { type: string; name?: string };
      if (tc.type === "auto") {
        openaiReq.tool_choice = "auto";
      } else if (tc.type === "any") {
        openaiReq.tool_choice = "required";
      } else if (tc.type === "tool" && tc.name) {
        let choiceName = tc.name;
        if (this.provider === "gemini" && choiceName.length > 64) {
          choiceName = choiceName.slice(0, 64);
        }
        openaiReq.tool_choice = {
          type: "function",
          function: { name: choiceName },
        };
      }
    }

    // Forward thinking mode as reasoning_effort for Codex/OpenAI-compat providers
    if (request.thinking && typeof request.thinking === "object") {
      const thinking = request.thinking as Record<string, unknown>;
      const budget = thinking.budget_tokens as number | undefined;
      if (budget && budget >= 8000) {
        (openaiReq as Record<string, unknown>).reasoning_effort = "high";
      } else if (budget && budget >= 2048) {
        (openaiReq as Record<string, unknown>).reasoning_effort = "medium";
      } else {
        (openaiReq as Record<string, unknown>).reasoning_effort = "low";
      }
    }

    // Apply provider-specific optimizations
    this.applyProviderOptimizations(openaiReq, request);

    return openaiReq;
  }

  /**
   * Apply provider-specific optimizations to the OpenAI request.
   * Each provider has different optimal settings for temperature, tool calling,
   * and reasoning that maximize output quality.
   */
  private applyProviderOptimizations(
    req: OpenAIChatRequest,
    original: MessagesRequest,
  ): void {
    const ext = req as Record<string, unknown>;
    const model = req.model.toLowerCase();
    const hasTools = !!req.tools && req.tools.length > 0;
    const hasThinking = !!original.thinking;

    switch (this.provider) {
      case "codex": {
        // Codex: parallel_tool_calls for faster multi-tool responses
        if (hasTools) {
          ext.parallel_tool_calls = true;
        }
        // Codex 5.3 benefits from "xhigh" reasoning for premium tasks
        if (hasThinking && model.includes("5.3")) {
          const budget = (original.thinking as Record<string, unknown>)
            ?.budget_tokens as number | undefined;
          if (budget && budget >= 16000) {
            ext.reasoning_effort = "xhigh";
          }
        }
        break;
      }

      case "gemini": {
        // Gemini reasoning models require temperature=1.0 to avoid looping
        if (req.temperature === undefined || req.temperature === null) {
          req.temperature = 1.0;
        }
        // Parallel tool calls supported
        if (hasTools) {
          ext.parallel_tool_calls = true;
        }
        break;
      }

      // GLM uses AnthropicCompatProvider, not OpenAICompatProvider — no case needed here

      case "minimax": {
        // MiniMax recommended: temperature=1.0, top_p=0.95, top_k=40
        if (req.temperature === undefined || req.temperature === null) {
          req.temperature = 1.0;
        }
        if (req.top_p === undefined || req.top_p === null) {
          req.top_p = 0.95;
        }
        if (ext.top_k === undefined) {
          ext.top_k = 40;
        }
        break;
      }

      // kimi, openrouter: no special optimizations needed
    }
  }

  private translateFromOpenAI(
    openaiResp: {
      id: string;
      choices: {
        message: {
          content: string | null;
          tool_calls?: {
            id: string;
            type: string;
            function: { name: string; arguments: string };
          }[];
        };
        finish_reason: string;
      }[];
      usage: { prompt_tokens: number; completion_tokens: number };
      model: string;
    },
    requestModel: string,
    knownTools: Set<string> = new Set(),
    truncationMap: Map<string, string> = new Map(),
  ): MessagesResponse {
    const choice = openaiResp.choices[0];
    const content: ContentBlock[] = [];

    // Add text content if present
    if (choice?.message?.content) {
      // Strip embedded <think>...</think> tags from providers (e.g. MiniMax)
      // that embed reasoning in the content field instead of reasoning_content.
      // Without this, Claude Code sees raw thinking tags in the response text.
      const cleaned = stripEmbeddedThinking(choice.message.content);
      if (cleaned.length > 0) {
        content.push({ type: "text", text: cleaned });
      }
    }

    // Translate tool_calls → Anthropic tool_use blocks
    if (choice?.message?.tool_calls) {
      for (const tc of choice.message.tool_calls) {
        let parsedInput: unknown = {};
        try {
          parsedInput = JSON.parse(tc.function.arguments || "{}");
        } catch {
          parsedInput = { _raw: tc.function.arguments };
          console.log(
            `[openai-compat] ⚠️ Malformed tool arguments for '${tc.function.name}': not valid JSON (${tc.function.arguments?.length ?? 0} chars)`,
          );
        }
        // Resolve truncated Gemini names, then normalize for hallucinated names
        let resolvedName =
          truncationMap.get(tc.function.name) ?? tc.function.name;
        resolvedName = normalizeToolName(resolvedName, knownTools);
        if (resolvedName !== tc.function.name) {
          console.log(
            `[openai-compat] Normalized tool name: "${tc.function.name}" → "${resolvedName}"`,
          );
        }
        content.push({
          type: "tool_use",
          id: tc.id,
          name: resolvedName,
          input: parsedInput,
        });
      }
    }

    // Ensure at least one content block (text must be non-empty for Anthropic)
    if (content.length === 0) {
      content.push({ type: "text", text: " " });
    }

    // Map finish_reason → stop_reason
    // Some models (Codex, GLM, Kimi) return finish_reason "stop" even when tool_calls
    // are present. Detect tool_use blocks in content and force stop_reason accordingly.
    const hasToolUseBlocks = content.some((b) => b.type === "tool_use");
    let stopReason: string | null = null;
    if (choice?.finish_reason === "tool_calls" || hasToolUseBlocks) {
      stopReason = "tool_use";
    } else if (choice?.finish_reason === "length") {
      stopReason = "max_tokens";
    } else if (choice?.finish_reason === "stop") {
      stopReason = "end_turn";
    } else {
      stopReason = choice?.finish_reason ?? null;
    }

    return {
      id: openaiResp.id,
      type: "message",
      role: "assistant",
      content,
      model: requestModel,
      stop_reason: stopReason,
      stop_sequence: null,
      usage: {
        input_tokens: openaiResp.usage?.prompt_tokens ?? 0,
        output_tokens: openaiResp.usage?.completion_tokens ?? 0,
      },
    };
  }

  async sendRequest(
    account: ProviderAccount,
    request: MessagesRequest,
    signal?: AbortSignal,
  ): Promise<ProviderResponse> {
    const openaiRequest = this.translateToOpenAI(request);
    const isStreaming = request.stream === true;

    // Extract known tool names for response normalization
    const knownTools = new Set<string>();
    const tools = (request as Record<string, unknown>).tools;
    if (tools && Array.isArray(tools)) {
      for (const t of tools as { name: string }[]) {
        if (t.name) knownTools.add(t.name);
      }
    }

    // Build reverse map for Gemini truncated tool names (truncated → original)
    const truncationMap = new Map<string, string>();
    if (this.provider === "gemini" && tools && Array.isArray(tools)) {
      for (const t of tools as { name: string }[]) {
        if (t.name && t.name.length > 64) {
          truncationMap.set(t.name.slice(0, 64), t.name);
        }
      }
    }

    let url: string;
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      "Accept-Encoding": "identity",
    };

    // Provider-specific headers
    if (this.provider === "kimi") {
      headers["User-Agent"] = "claude-code/2.1.69";
    }

    Object.assign(headers, this.defaultHeaders);

    if (this.useProxy) {
      // Route through CLIProxyAPI's OpenAI-compat endpoint
      url = `${CLIPROXYAPI_BASE}/v1/chat/completions`;
      headers["Authorization"] = `Bearer your-proxy-key`;
      headers["x-account-id"] = account.id;
      headers["x-provider"] = this.provider;
      // Prevent keep-alive race: Go's net/http closes idle conns before Bun detects it → ECONNRESET
      headers["Connection"] = "close";
    } else {
      // Direct to provider API
      const base = this.baseUrl ?? CLIPROXYAPI_BASE;
      url = `${base}/chat/completions`;
      headers["Authorization"] = `Bearer ${account.accessToken}`;
    }

    try {
      const resp = await fetch(url, {
        method: "POST",
        headers,
        body: JSON.stringify(openaiRequest),
        signal,
      });

      if (!resp.ok) {
        const body = await this.readErrorBody(resp);
        return {
          success: false,
          error: `${resp.status}: ${body.slice(0, 500)}`,
          errorClass: this.classifyError(resp.status, body),
        };
      }

      const rateLimitHeaders = this.extractRateLimitHeaders(resp);

      if (isStreaming) {
        // Translate SSE stream from OpenAI format to Anthropic format
        const hasThinking = !!request.thinking;
        const translatedStream = this.translateStream(
          resp,
          request.model,
          hasThinking,
          knownTools,
          truncationMap,
          this.name,
        );
        return { success: true, response: translatedStream, rateLimitHeaders };
      }

      const openaiBody = (await resp.json()) as Parameters<
        typeof this.translateFromOpenAI
      >[0];
      const anthropicBody = this.translateFromOpenAI(
        openaiBody,
        request.model,
        knownTools,
        truncationMap,
      );
      return { success: true, body: anthropicBody, rateLimitHeaders };
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

  private translateStream(
    resp: Response,
    requestModel: string,
    emitThinking: boolean = false,
    knownTools: Set<string> = new Set(),
    truncationMap: Map<string, string> = new Map(),
    providerName: string = "",
  ): Response {
    if (!resp.body) return resp;

    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    const encoder = new TextEncoder();
    let buffer = "";
    const messageId = `msg_${Date.now()}`;
    let inputTokens = 0;
    let outputTokens = 0;
    let firstChunk = true;
    let inThinking = false;
    let thinkingStarted = false;
    let inTextBlock = false;
    let contentBlockIndex = 0;
    // Tool call streaming state
    let lastToolCallIdx = -1;
    let sawToolCalls = false; // sticky flag — once true, stop_reason will be tool_use
    const toolCallAnthropicIdx = new Map<number, number>(); // OpenAI index → Anthropic block index
    const toolCallIds = new Map<number, string>(); // OpenAI index → resolved tool call ID
    // Embedded <think> tag suppression — only for providers that put reasoning in content
    // (e.g. MiniMax). Other providers don't use <think> tags, so the holdback
    // causes visible character-level line splitting in Claude Code's output.
    const needsThinkFilter = providerName.startsWith("minimax");
    let inEmbeddedThink = false;
    let thinkBuffer = ""; // accumulates text that may contain partial <think> or </think> tags

    const emit = (event: string, data: unknown) => {
      controller_ref?.enqueue(
        encoder.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`),
      );
    };
    let controller_ref: ReadableStreamDefaultController<Uint8Array> | null =
      null;

    const stream = new ReadableStream({
      async pull(controller) {
        controller_ref = controller;
        try {
          const { done, value } = await reader.read();
          if (done) {
            // Ensure message_start was emitted even if stream had no normal chunks
            if (firstChunk) {
              firstChunk = false;
              emit("message_start", {
                type: "message_start",
                message: {
                  id: messageId,
                  type: "message",
                  role: "assistant",
                  content: [],
                  model: requestModel,
                  stop_reason: null,
                  stop_sequence: null,
                  usage: { input_tokens: 0, output_tokens: 0 },
                },
              });
            }
            // Flush any remaining thinkBuffer content (MiniMax holdback)
            // Must happen before finish_reason processing to maintain SSE event ordering
            if (thinkBuffer && !inEmbeddedThink) {
              if (!inTextBlock) {
                emit("content_block_start", {
                  type: "content_block_start",
                  index: contentBlockIndex,
                  content_block: { type: "text", text: "" },
                });
                inTextBlock = true;
              }
              emit("content_block_delta", {
                type: "content_block_delta",
                index: contentBlockIndex,
                delta: { type: "text_delta", text: thinkBuffer },
              });
              thinkBuffer = "";
            }
            // Process any remaining data in buffer before closing
            if (buffer.trim()) {
              const remaining = buffer;
              buffer = "";
              if (remaining.startsWith("data:")) {
                const data = remaining
                  .slice(remaining.startsWith("data: ") ? 6 : 5)
                  .trim();
                if (data !== "[DONE]") {
                  try {
                    const lines = [remaining];
                    for (const line of lines) {
                      if (!line.startsWith("data:")) continue;
                      const d = line
                        .slice(line.startsWith("data: ") ? 6 : 5)
                        .trim();
                      const chunk = JSON.parse(d);
                      // Process finish_reason if present
                      if (chunk.choices?.[0]?.finish_reason) {
                        const stopReason =
                          chunk.choices[0].finish_reason === "tool_calls" ||
                          sawToolCalls
                            ? "tool_use"
                            : chunk.choices[0].finish_reason === "length"
                              ? "max_tokens"
                              : "end_turn";
                        if (inTextBlock) {
                          emit("content_block_stop", {
                            type: "content_block_stop",
                            index: contentBlockIndex,
                          });
                          inTextBlock = false;
                        }
                        emit("message_delta", {
                          type: "message_delta",
                          delta: {
                            stop_reason: stopReason,
                            stop_sequence: null,
                          },
                          usage: {
                            output_tokens:
                              chunk.usage?.completion_tokens ?? outputTokens,
                          },
                        });
                      }
                    }
                  } catch {
                    // Skip unparseable final buffer
                  }
                }
              }
            }
            // Close any open blocks before message_stop
            if (inThinking) {
              emit("content_block_stop", {
                type: "content_block_stop",
                index: contentBlockIndex,
              });
              inThinking = false;
            }
            if (inTextBlock) {
              emit("content_block_stop", {
                type: "content_block_stop",
                index: contentBlockIndex,
              });
              inTextBlock = false;
            }
            emit("message_stop", { type: "message_stop" });
            controller.close();
            return;
          }

          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split("\n");
          buffer = lines.pop() ?? "";

          for (const line of lines) {
            // Accept both "data: {...}" (standard) and "data:{...}" (kimi)
            if (!line.startsWith("data:")) continue;
            const data = line.slice(line.startsWith("data: ") ? 6 : 5).trim();
            if (data === "[DONE]") {
              emit("message_stop", { type: "message_stop" });
              controller.close();
              return;
            }

            try {
              const chunk = JSON.parse(data);
              const delta = chunk.choices?.[0]?.delta;

              if (firstChunk) {
                firstChunk = false;
                emit("message_start", {
                  type: "message_start",
                  message: {
                    id: messageId,
                    type: "message",
                    role: "assistant",
                    content: [],
                    model: requestModel,
                    stop_reason: null,
                    stop_sequence: null,
                    usage: { input_tokens: 0, output_tokens: 0 },
                  },
                });

                const hasReasoning = !!delta?.reasoning_content && emitThinking;
                const hasToolCalls = !!delta?.tool_calls;
                const hasFinish = !!chunk.choices?.[0]?.finish_reason;
                if (!hasReasoning && !hasToolCalls && !hasFinish) {
                  emit("content_block_start", {
                    type: "content_block_start",
                    index: contentBlockIndex,
                    content_block: { type: "text", text: "" },
                  });
                  inTextBlock = true;
                }
              }

              const reasoningText = delta?.reasoning_content;
              // Filter out [DONE] if a provider mistakenly returns it as content
              let contentText = delta?.content;
              if (contentText && contentText.includes("[DONE]")) {
                contentText = contentText.replace(/\[DONE\]/g, "");
              }
              const toolCalls = delta?.tool_calls as
                | {
                    index: number;
                    id?: string;
                    type?: string;
                    function?: { name?: string; arguments?: string };
                  }[]
                | undefined;

              // --- Reasoning → thinking blocks ---
              if (reasoningText && !emitThinking) {
                emit("ping", { type: "ping" });
                // Completely strip reasoning from this chunk — do not let it
                // bleed into content processing below.
                continue;
              }
              if (reasoningText && emitThinking) {
                if (!thinkingStarted) {
                  thinkingStarted = true;
                  inThinking = true;
                  emit("content_block_start", {
                    type: "content_block_start",
                    index: contentBlockIndex,
                    content_block: { type: "thinking", thinking: "" },
                  });
                }
                emit("content_block_delta", {
                  type: "content_block_delta",
                  index: contentBlockIndex,
                  delta: { type: "thinking_delta", thinking: reasoningText },
                });
              }

              // --- Content text → text blocks ---
              // Filter embedded <think>...</think> tags from providers like MiniMax
              // that put reasoning in the content field instead of reasoning_content.
              // Only apply the holdback for providers that actually use <think> tags —
              // for others, pass text through immediately to prevent split-line artifacts.
              if (contentText) {
                let cleanText: string;

                if (!needsThinkFilter) {
                  // Fast path: no <think> tag processing needed — emit text directly
                  cleanText = contentText;
                } else {
                  // Slow path: MiniMax-style <think> tag filtering with holdback
                  thinkBuffer += contentText;
                  cleanText = "";
                  let remainder = "";
                  let pos = 0;
                  const buf = thinkBuffer;
                  while (pos < buf.length) {
                    if (inEmbeddedThink) {
                      const closeIdx = buf.indexOf("</think>", pos);
                      if (closeIdx === -1) {
                        remainder = buf.slice(-8);
                        break;
                      }
                      inEmbeddedThink = false;
                      pos = closeIdx + "</think>".length;
                      while (
                        pos < buf.length &&
                        (buf[pos] === "\n" ||
                          buf[pos] === "\r" ||
                          buf[pos] === " ")
                      ) {
                        pos++;
                      }
                    } else {
                      const openIdx = buf.indexOf("<think>", pos);
                      if (openIdx === -1) {
                        const safeEnd = buf.length - 7;
                        if (safeEnd > pos) {
                          cleanText += buf.slice(pos, safeEnd);
                          remainder = buf.slice(safeEnd);
                        } else {
                          remainder = buf.slice(pos);
                        }
                        break;
                      }
                      if (openIdx > pos) {
                        cleanText += buf.slice(pos, openIdx);
                      }
                      inEmbeddedThink = true;
                      pos = openIdx + "<think>".length;
                    }
                  }
                  if (pos >= buf.length && !inEmbeddedThink) remainder = "";
                  thinkBuffer = remainder;
                }

                // Emit the clean text (if any)
                if (cleanText) {
                  if (inThinking) {
                    inThinking = false;
                    emit("content_block_stop", {
                      type: "content_block_stop",
                      index: contentBlockIndex,
                    });
                    contentBlockIndex++;
                    emit("content_block_start", {
                      type: "content_block_start",
                      index: contentBlockIndex,
                      content_block: { type: "text", text: "" },
                    });
                    inTextBlock = true;
                  }
                  if (!inTextBlock) {
                    emit("content_block_start", {
                      type: "content_block_start",
                      index: contentBlockIndex,
                      content_block: { type: "text", text: "" },
                    });
                    inTextBlock = true;
                  }
                  emit("content_block_delta", {
                    type: "content_block_delta",
                    index: contentBlockIndex,
                    delta: { type: "text_delta", text: cleanText },
                  });
                }
              }

              // --- Tool calls → tool_use blocks ---
              if (toolCalls && Array.isArray(toolCalls)) {
                for (const tc of toolCalls) {
                  const tcIdx = tc.index ?? 0;

                  // Resolve or synthesize a stable tool call ID
                  const tcId =
                    tc.id ??
                    toolCallIds.get(tcIdx) ??
                    `toolu_synth_${Date.now()}_${tcIdx}`;
                  if (tc.id) toolCallIds.set(tcIdx, tc.id);
                  else if (!toolCallIds.has(tcIdx))
                    toolCallIds.set(tcIdx, tcId);

                  if (tc.function?.name) {
                    sawToolCalls = true;
                    // New tool call starting — close any open block
                    if (inTextBlock) {
                      emit("content_block_stop", {
                        type: "content_block_stop",
                        index: contentBlockIndex,
                      });
                      inTextBlock = false;
                      contentBlockIndex++;
                    }
                    if (inThinking) {
                      emit("content_block_stop", {
                        type: "content_block_stop",
                        index: contentBlockIndex,
                      });
                      inThinking = false;
                      contentBlockIndex++;
                    }
                    if (lastToolCallIdx >= 0 && lastToolCallIdx !== tcIdx) {
                      // Close previous tool call block
                      const prevIdx = toolCallAnthropicIdx.get(lastToolCallIdx);
                      if (prevIdx !== undefined) {
                        emit("content_block_stop", {
                          type: "content_block_stop",
                          index: prevIdx,
                        });
                        contentBlockIndex++;
                      }
                    }

                    toolCallAnthropicIdx.set(tcIdx, contentBlockIndex);
                    lastToolCallIdx = tcIdx;

                    // Resolve truncated Gemini names, then normalize for hallucinated names
                    let resolvedName =
                      truncationMap.get(tc.function.name) ?? tc.function.name;
                    resolvedName = normalizeToolName(resolvedName, knownTools);
                    if (resolvedName !== tc.function.name) {
                      console.log(
                        `[openai-compat] Stream: normalized tool name: "${tc.function.name}" → "${resolvedName}"`,
                      );
                    }

                    emit("content_block_start", {
                      type: "content_block_start",
                      index: contentBlockIndex,
                      content_block: {
                        type: "tool_use",
                        id: toolCallIds.get(tcIdx) ?? tcId,
                        name: resolvedName,
                        input: {},
                      },
                    });
                  }

                  // Stream argument fragments as input_json_delta
                  if (tc.function?.arguments) {
                    const anthropicIdx =
                      toolCallAnthropicIdx.get(tcIdx) ?? contentBlockIndex;
                    emit("content_block_delta", {
                      type: "content_block_delta",
                      index: anthropicIdx,
                      delta: {
                        type: "input_json_delta",
                        partial_json: tc.function.arguments,
                      },
                    });
                  }
                }
              }

              // --- Usage tracking ---
              if (chunk.usage) {
                inputTokens = chunk.usage.prompt_tokens ?? inputTokens;
                outputTokens = chunk.usage.completion_tokens ?? outputTokens;
              }

              // --- Finish ---
              if (chunk.choices?.[0]?.finish_reason) {
                const finishReason = chunk.choices[0].finish_reason;

                // If still inside an embedded <think> block at stream end (e.g. max_tokens
                // hit during reasoning), discard the reasoning fragment — don't leak raw XML.
                if (thinkBuffer && inEmbeddedThink) {
                  thinkBuffer = "";
                  inEmbeddedThink = false;
                }
                // Flush any remaining thinkBuffer content that isn't inside a <think> block
                if (thinkBuffer && !inEmbeddedThink) {
                  // Strip any partial tag fragments but preserve real whitespace
                  const flushed = thinkBuffer.replace(
                    /<\/?t(?:h(?:i(?:n(?:k)?)?)?)?$/i,
                    "",
                  );
                  if (flushed) {
                    if (!inTextBlock && !inThinking) {
                      emit("content_block_start", {
                        type: "content_block_start",
                        index: contentBlockIndex,
                        content_block: { type: "text", text: "" },
                      });
                      inTextBlock = true;
                    }
                    if (inThinking) {
                      inThinking = false;
                      emit("content_block_stop", {
                        type: "content_block_stop",
                        index: contentBlockIndex,
                      });
                      contentBlockIndex++;
                      emit("content_block_start", {
                        type: "content_block_start",
                        index: contentBlockIndex,
                        content_block: { type: "text", text: "" },
                      });
                      inTextBlock = true;
                    }
                    emit("content_block_delta", {
                      type: "content_block_delta",
                      index: contentBlockIndex,
                      delta: { type: "text_delta", text: flushed },
                    });
                  }
                  thinkBuffer = "";
                }

                // Close thinking block if still open
                if (inThinking) {
                  inThinking = false;
                  emit("content_block_stop", {
                    type: "content_block_stop",
                    index: contentBlockIndex,
                  });
                  contentBlockIndex++;
                  // Thinking-only responses are valid — don't emit empty text block
                }

                // Close last tool call block if open
                if (lastToolCallIdx >= 0) {
                  const lastIdx = toolCallAnthropicIdx.get(lastToolCallIdx);
                  if (lastIdx !== undefined) {
                    emit("content_block_stop", {
                      type: "content_block_stop",
                      index: lastIdx,
                    });
                  }
                  lastToolCallIdx = -1;
                } else if (inTextBlock) {
                  // Close text block
                  emit("content_block_stop", {
                    type: "content_block_stop",
                    index: contentBlockIndex,
                  });
                  inTextBlock = false;
                } else if (!thinkingStarted) {
                  // No blocks were opened — emit text block with space (must be non-empty)
                  emit("content_block_start", {
                    type: "content_block_start",
                    index: contentBlockIndex,
                    content_block: { type: "text", text: "" },
                  });
                  emit("content_block_delta", {
                    type: "content_block_delta",
                    index: contentBlockIndex,
                    delta: { type: "text_delta", text: " " },
                  });
                  emit("content_block_stop", {
                    type: "content_block_stop",
                    index: contentBlockIndex,
                  });
                }

                // Force tool_use if we saw ANY tool calls in the stream,
                // even if finish_reason is "stop" (Codex/GLM/Kimi behavior).
                // Uses sticky sawToolCalls flag — lastToolCallIdx may already
                // be reset to -1 by the close-block logic above.
                const stopReason =
                  finishReason === "tool_calls" || sawToolCalls
                    ? "tool_use"
                    : finishReason === "length"
                      ? "max_tokens"
                      : "end_turn";
                emit("message_delta", {
                  type: "message_delta",
                  delta: { stop_reason: stopReason, stop_sequence: null },
                  usage: { output_tokens: outputTokens },
                });
              }
            } catch {
              // Skip unparseable chunks
            }
          }
        } catch (e) {
          controller.error(e);
        }
      },
      cancel() {
        reader.cancel();
      },
    });

    return new Response(stream, {
      headers: {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        Connection: "keep-alive",
      },
    });
  }
}
