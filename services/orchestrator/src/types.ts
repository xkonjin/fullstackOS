export type Provider =
  | "claude"
  | "antigravity"
  | "codex"
  | "gemini"
  | "kimi"
  | "kimi-api"
  | "glm"
  | "minimax"
  | "openrouter";
export type Tier = "premium" | "standard" | "fast" | "budget" | "last_resort";
export type TokenHealth =
  | "healthy"
  | "degraded"
  | "expired"
  | "dead"
  | "quota_exhausted";
export type AuthType = "oauth" | "api_key" | "hybrid";
export type RoutingStrategy = "task_type" | "round_robin" | "cost_optimized";
export type RoutingLane =
  | "interactive_premium"
  | "design_premium"
  | "review_premium"
  | "strategy_premium"
  | "interactive_fast"
  | "worker_standard"
  | "worker_budget"
  | "last_resort";
export type RoutingTaskKind =
  | "code_deep"
  | "research_synthesis"
  | "review"
  | "design_visual"
  | "design_system"
  | "product_ux"
  | "product_strategy"
  | "small_transform"
  | "general";
export type FleetRole =
  | "planner"
  | "architect"
  | "design_lead"
  | "implementer"
  | "researcher"
  | "extractor"
  | "validator"
  | "reviewer"
  | "ui_reviewer"
  | "synthesizer"
  | "product_strategist";
export type RiskLevel = "low" | "medium" | "high";
export type ErrorClass =
  | "rate_limit"
  | "auth_error"
  | "auth_unavailable"
  | "server_error"
  | "model_unavailable"
  | "invalid_request"
  | "context_length"
  | "timeout"
  | "connection_error"
  | "unknown";

// --- Token file formats (as stored in ~/.cli-proxy-api/) ---

export interface ClaudeToken {
  type: "claude";
  access_token: string;
  refresh_token: string;
  email: string;
  expired: string; // ISO date
  last_refresh: string;
  id_token: string;
  disabled: boolean;
}

export interface AntigravityToken {
  type: "antigravity";
  access_token: string;
  refresh_token: string;
  email: string;
  expired: string;
  expires_in: number;
  project_id: string;
  timestamp: number;
  disabled: boolean;
}

export interface CodexToken {
  type: "codex";
  access_token: string;
  refresh_token: string;
  id_token: string;
  email: string;
  account_id: string;
  expired: string;
  last_refresh: string;
  disabled: boolean;
}

export interface GeminiToken {
  type: "gemini";
  email: string;
  project_id: string;
  disabled: boolean;
  checked: boolean;
  auto: boolean;
  token: {
    access_token: string;
    refresh_token: string;
    client_id: string;
    client_secret: string;
    expires_in: number;
    expiry: string;
    scopes: string[];
    token_type: string;
    token_uri: string;
    universe_domain: string;
  };
}

export interface KimiToken {
  type: "kimi";
  access_token: string;
  refresh_token: string;
  device_id: string;
  expired: string;
  last_refresh: string;
  scope: string;
  token_type: string;
  disabled: boolean;
}

export type TokenFile =
  | ClaudeToken
  | AntigravityToken
  | CodexToken
  | GeminiToken
  | KimiToken;

// --- Provider account (runtime state) ---

export interface ProviderAccount {
  id: string; // unique key: "{provider}-{filename}"
  provider: Provider;
  tier: Tier;
  filename: string;
  email?: string;
  accessToken: string;
  health: TokenHealth;
  expiresAt: Date | null;
  lastUsed: Date | null;
  lastRefresh: Date | null;
  errorCount: number;
  lastError: string | null;
  disabled: boolean;
  // Circuit breaker state
  circuitFailures: { timestamp: number }[];
  circuitOpenUntil: number | null;
  // Rate limit tracking
  rateLimitedUntil: number | null; // epoch ms when rate limit resets
  rateLimitHits: number; // count of 429s in current window
  lastRateLimitAt: number | null; // epoch ms of last 429
  // Proactive quota tracking (from response headers)
  quotaRemaining: number | null; // remaining requests in current window
  quotaLimit: number | null; // total requests allowed in window
  quotaResetAt: number | null; // epoch ms when quota resets
}

// --- Config ---

export interface ProviderConfig {
  type: AuthType;
  token_dir?: string;
  pattern?: string | string[];
  api_key?: string;
  base_url?: string;
  default_headers?: Record<string, string>;
  tier: Tier;
}

export interface BudgetLimit {
  provider: string;
  email?: string;
  daily_tokens: number;
  alert_at?: number; // fraction, default 0.8
  stop_at?: number; // fraction, default 0.95
}

export interface BootstrapConfig {
  interactive_premium_candidates?: string[];
  design_premium_candidates?: string[];
  worker_budget_candidates?: string[];
  sticky_cache?: {
    enabled?: boolean;
    max_age_minutes?: number;
    same_lane_only?: boolean;
    same_band_only?: boolean;
  };
}

export interface RoutingLanePolicy {
  quality_floor?: Tier;
  provider_band?: Provider[];
  task_heads?: Partial<Record<RoutingTaskKind, string[]>>;
  allow_learning_reorder?: "within_band" | "disabled";
}

export interface Config {
  port: number;
  api_keys: string[];
  excluded_emails: string[];
  excluded_provider_emails?: Partial<Record<Provider, string[]>>;
  providers: Record<string, ProviderConfig>;
  routing: {
    strategy: RoutingStrategy;
    tiers: Record<Tier, string[]>;
  };
  health: {
    check_interval: number;
    snapshot_interval_ms?: number;
    event_loop_lag_warn_ms?: number;
    event_loop_lag_critical_ms?: number;
    event_loop_suspend_threshold_ms?: number;
    circuit_breaker: {
      threshold: number;
      cooldown: number;
    };
    token_refresh: {
      interval: number;
      lead_time: number;
      max_parallel?: number;
      retry_cooldown_ms?: number;
      non_refreshable_cooldown_ms?: number;
    };
    token_rescan?: {
      interval_ms?: number;
    };
  };
  database: {
    path: string;
  };
  model_aliases?: Record<string, string>;
  bootstrap?: BootstrapConfig;
  budget?: {
    enabled: boolean;
    limits: BudgetLimit[];
    check_interval?: number; // seconds, default 60
  };
  autonomy?: {
    profile?: "safe" | "balanced" | "aggressive";
    gate_on_degrade?: boolean;
    max_parallel_by_tier?: Partial<Record<Tier, number>>;
  };
  slo?: {
    enabled?: boolean;
    max_degrade_rate?: number;
    max_provider_error_rate?: number;
  };
  learning?: {
    enabled?: boolean;
    interval_ms?: number;
    start_delay_ms?: number;
    max_cycle_ms?: number;
    min_requests_for_reorder?: number;
    guardrails?: {
      forbid_cross_band_promotion?: boolean;
      forbid_budget_provider_heading_premium?: boolean;
      forbid_non_gemini_heading_design_lane?: boolean;
      premium_head_quality_floor?: number;
      design_head_allowlist?: Provider[];
    };
  };
  routing_policy?: {
    lanes?: Partial<Record<RoutingLane, RoutingLanePolicy>>;
  };
  skills_policy?: {
    mode?: "deterministic" | "fallback_fuzzy";
    enable_root_skill_discovery?: boolean;
    enable_subtask_skill_discovery?: boolean;
    inject_by_role?: boolean;
    max_inline_skill_snippets?: number;
    max_selected_skills?: number;
    manifest_path?: string;
    resource_pack_root?: string;
    role_bundle_manifest?: string;
    roots?: string[];
  };
  budget_policy?: {
    interactive_premium_degrade?: "warn" | "allow";
    design_premium_degrade?: "warn" | "allow";
    worker_budget_aggressive?: boolean;
  };
  fleet_policy?: {
    roles?: Partial<
      Record<
        FleetRole,
        {
          lane?: RoutingLane | "mixed_by_complexity";
          preferred_models?: string[];
        }
      >
    >;
  };
  fractal_fleet?: {
    enabled?: boolean;
    auto_decompose?: boolean;
    auto_decompose_min_complexity?: "complex" | "deep";
  };
}

// --- Anthropic Messages API ---

export interface ContentBlock {
  type: "text" | "image" | "tool_use" | "tool_result" | "thinking";
  [key: string]: unknown;
}

export interface Message {
  role: "user" | "assistant";
  content: string | ContentBlock[];
}

export interface MessagesRequest {
  model: string;
  messages: Message[];
  max_tokens?: number;
  temperature?: number;
  top_p?: number;
  top_k?: number;
  stream?: boolean;
  system?: string | ContentBlock[];
  stop_sequences?: string[];
  metadata?: Record<string, unknown>;
  // Extended thinking
  thinking?: { type: string; budget_tokens?: number };
  [key: string]: unknown;
}

export interface MessagesResponse {
  id: string;
  type: "message";
  role: "assistant";
  content: ContentBlock[];
  model: string;
  stop_reason: string | null;
  stop_sequence: string | null;
  usage: {
    input_tokens: number;
    output_tokens: number;
    cache_creation_input_tokens?: number;
    cache_read_input_tokens?: number;
  };
}

// --- OpenAI Chat Completions (for translation) ---

export interface OpenAIToolCall {
  id: string;
  type: "function";
  function: {
    name: string;
    arguments: string;
  };
}

export interface OpenAITool {
  type: "function";
  function: {
    name: string;
    description?: string;
    parameters?: Record<string, unknown>;
  };
}

export interface OpenAIChatMessage {
  role: "system" | "user" | "assistant" | "tool";
  content: string | null;
  tool_calls?: OpenAIToolCall[];
  tool_call_id?: string;
  reasoning_content?: string;
}

export interface OpenAIChatRequest {
  model: string;
  messages: OpenAIChatMessage[];
  max_tokens?: number;
  temperature?: number;
  top_p?: number;
  stream?: boolean;
  stop?: string[];
  tools?: OpenAITool[];
  tool_choice?: string | { type: "function"; function: { name: string } };
  [key: string]: unknown;
}

export interface OpenAIChatResponse {
  id: string;
  object: string;
  created: number;
  model: string;
  choices: {
    index: number;
    message: {
      role: string;
      content: string | null;
      tool_calls?: OpenAIToolCall[];
    };
    finish_reason: string;
  }[];
  usage: {
    prompt_tokens: number;
    completion_tokens: number;
    total_tokens: number;
  };
}

// --- Usage tracking ---

export interface UsageRecord {
  timestamp: number;
  provider: string;
  model: string;
  account_id: string;
  input_tokens: number;
  output_tokens: number;
  latency_ms: number;
  status: "success" | "error";
  error: string | null;
}

// --- Events ---

export type TokenEvent =
  | { type: "token_healthy"; account: ProviderAccount }
  | { type: "token_degraded"; account: ProviderAccount; reason: string }
  | { type: "token_expired"; account: ProviderAccount }
  | { type: "token_dead"; account: ProviderAccount; reason: string }
  | { type: "token_refreshed"; account: ProviderAccount }
  | { type: "circuit_open"; account: ProviderAccount }
  | { type: "circuit_close"; account: ProviderAccount }
  | { type: "rate_limit_clear"; account: ProviderAccount }
  | { type: "budget_unpark"; account: ProviderAccount };
