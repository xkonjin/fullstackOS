import { parse as parseYaml, stringify as stringifyYaml } from "yaml";
import { homedir } from "os";
import { join } from "path";
import { watch } from "fs";
import type { Config } from "./types.ts";

const CONFIG_DIR = join(homedir(), ".claudemax");
const CONFIG_PATH = join(CONFIG_DIR, "config.yaml");

const DEFAULT_CONFIG: Config = {
  port: 8318,
  api_keys: ["your-proxy-key"],
  excluded_emails: [],
  model_aliases: {
    gemini_3_1_pro: "gemini-3.1-pro-high",
    gpt_5_4_codex: "gpt-5.4",
  },
  providers: {
    claude: {
      type: "oauth",
      token_dir: "~/.cli-proxy-api",
      pattern: "claude-*.json",
      tier: "premium",
    },
    antigravity: {
      type: "oauth",
      token_dir: "~/.cli-proxy-api",
      pattern: "antigravity-*.json",
      tier: "premium",
    },
    codex: {
      type: "oauth",
      token_dir: "~/.cli-proxy-api",
      pattern: "codex-*.json",
      tier: "standard",
    },
    gemini: {
      type: "oauth",
      token_dir: "~/.cli-proxy-api",
      pattern: ["*gen-lang-client-*.json", "gemini-*.json"],
      tier: "standard",
    },
    kimi: {
      type: "hybrid",
      token_dir: "~/.cli-proxy-api",
      pattern: "kimi-*.json",
      base_url: "https://api.kimi.com/coding/v1",
      tier: "standard",
    },
    glm: {
      type: "api_key",
      api_key: "${GLM_API_KEY}",
      base_url: "https://api.z.ai/api/paas/v4",
      tier: "standard",
    },
    minimax: {
      type: "api_key",
      api_key: "${MINIMAX_API_KEY}",
      base_url: "https://api.minimax.io/v1",
      tier: "budget",
    },
  },
  routing: {
    strategy: "task_type",
    tiers: {
      premium: ["codex", "claude", "gemini", "antigravity", "glm"],
      standard: ["codex", "gemini", "claude", "antigravity", "glm", "kimi"],
      fast: ["gemini", "claude", "codex", "glm", "minimax", "kimi"],
      budget: ["kimi", "glm", "minimax"],
      last_resort: ["glm", "minimax"],
    },
  },
  bootstrap: {
    interactive_premium_candidates: [
      "gpt-5.4",
      "claude-opus-4-6",
      "claude-sonnet-4-6",
      "gemini-2.5-pro",
    ],
    design_premium_candidates: [
      "gemini-3.1-pro-high",
      "claude-opus-4-6",
      "gemini-2.5-pro",
    ],
    worker_budget_candidates: [
      "kimi-k2.5",
      "glm-4.7",
      "MiniMax-M2.5-highspeed",
    ],
    sticky_cache: {
      enabled: true,
      max_age_minutes: 180,
      same_lane_only: true,
      same_band_only: true,
    },
  },
  health: {
    check_interval: 120,
    snapshot_interval_ms: 1000,
    event_loop_lag_warn_ms: 250,
    event_loop_lag_critical_ms: 1000,
    event_loop_suspend_threshold_ms: 60000,
    circuit_breaker: { threshold: 5, cooldown: 120 },
    token_refresh: {
      interval: 900,
      lead_time: 21600,
      max_parallel: 2,
      retry_cooldown_ms: 120000,
      non_refreshable_cooldown_ms: 900000,
    },
    token_rescan: {
      interval_ms: 600000,
    },
  },
  database: { path: "~/.claudemax/usage.db" },
  autonomy: {
    profile: "balanced",
    gate_on_degrade: true,
    max_parallel_by_tier: {
      premium: 4,
      standard: 6,
      fast: 8,
      budget: 8,
      last_resort: 2,
    },
  },
  slo: {
    enabled: true,
    max_degrade_rate: 0.35,
    max_provider_error_rate: 0.25,
  },
  learning: {
    enabled: true,
    start_delay_ms: 30000,
    interval_ms: 60 * 60 * 1000,
    max_cycle_ms: 7500,
    min_requests_for_reorder: 25,
    guardrails: {
      forbid_cross_band_promotion: true,
      forbid_budget_provider_heading_premium: true,
      forbid_non_gemini_heading_design_lane: false,
      premium_head_quality_floor: 0.8,
      design_head_allowlist: ["gemini", "claude", "codex"],
    },
  },
  routing_policy: {
    lanes: {
      interactive_premium: {
        quality_floor: "premium",
        provider_band: ["codex", "claude", "gemini", "antigravity"],
        allow_learning_reorder: "within_band",
        task_heads: {
          code_deep: ["gpt-5.4", "claude-opus-4-6", "claude-sonnet-4-6"],
          research_synthesis: ["claude-opus-4-6", "gpt-5.4", "gemini-2.5-pro"],
          review: ["claude-opus-4-6", "gpt-5.4"],
        },
      },
      design_premium: {
        quality_floor: "premium",
        provider_band: ["gemini", "claude", "codex"],
        allow_learning_reorder: "within_band",
        task_heads: {
          design_visual: [
            "gemini-3.1-pro-high",
            "claude-opus-4-6",
            "gemini-2.5-pro",
          ],
          design_system: ["gemini-3.1-pro-high", "gpt-5.4", "claude-opus-4-6"],
          product_ux: ["gemini-3.1-pro-high", "claude-opus-4-6", "gpt-5.4"],
        },
      },
      review_premium: {
        quality_floor: "premium",
        provider_band: ["claude", "codex", "gemini", "antigravity"],
        allow_learning_reorder: "within_band",
        task_heads: {
          review: ["claude-opus-4-6", "gpt-5.4"],
        },
      },
      worker_budget: {
        quality_floor: "budget",
        provider_band: ["kimi", "glm", "minimax", "gemini"],
        task_heads: {
          small_transform: ["kimi-k2.5", "glm-4.7", "MiniMax-M2.5-highspeed"],
        },
      },
    },
  },
  skills_policy: {
    mode: "deterministic",
    enable_root_skill_discovery: true,
    enable_subtask_skill_discovery: true,
    inject_by_role: true,
    max_inline_skill_snippets: 2,
    max_selected_skills: 4,
    manifest_path: "config/skill-tree.yaml",
    resource_pack_root: "docs/resource-packs",
    role_bundle_manifest: "config/skill-tree.yaml",
    roots: ["~/.agents/skills", "~/.codex/skills", "~/.claude/skills"],
  },
  budget_policy: {
    interactive_premium_degrade: "warn",
    design_premium_degrade: "warn",
    worker_budget_aggressive: true,
  },
  fleet_policy: {
    roles: {
      planner: {
        lane: "interactive_premium",
        preferred_models: ["gpt-5.4", "claude-opus-4-6"],
      },
      architect: {
        lane: "interactive_premium",
        preferred_models: ["gpt-5.4", "claude-opus-4-6"],
      },
      design_lead: {
        lane: "design_premium",
        preferred_models: ["gemini-3.1-pro-high", "claude-opus-4-6"],
      },
      ui_reviewer: {
        lane: "design_premium",
        preferred_models: ["gemini-3.1-pro-high", "claude-opus-4-6"],
      },
      synthesizer: {
        lane: "interactive_premium",
        preferred_models: ["claude-opus-4-6", "gpt-5.4"],
      },
      researcher: {
        lane: "worker_budget",
        preferred_models: ["gemini-2.5-flash", "kimi-k2.5"],
      },
      extractor: {
        lane: "worker_budget",
        preferred_models: ["kimi-k2.5", "glm-4.7"],
      },
    },
  },
};

function interpolateEnvVars(value: string): string {
  return value.replace(/\$\{(\w+)\}/g, (_, name) => {
    return process.env[name] ?? "";
  });
}

function expandTilde(p: string): string {
  if (p.startsWith("~/")) return join(homedir(), p.slice(2));
  return p;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function deepMerge(target: any, source: any): any {
  if (!source || typeof source !== "object" || Array.isArray(source))
    return source;
  if (!target || typeof target !== "object" || Array.isArray(target))
    return source;
  const result = { ...target };
  for (const key of Object.keys(source)) {
    const sourceVal = source[key];
    const targetVal = target[key];
    if (
      sourceVal &&
      typeof sourceVal === "object" &&
      !Array.isArray(sourceVal) &&
      targetVal &&
      typeof targetVal === "object" &&
      !Array.isArray(targetVal)
    ) {
      result[key] = deepMerge(targetVal, sourceVal);
    } else if (sourceVal !== undefined) {
      result[key] = sourceVal;
    }
  }
  return result;
}

function resolveConfigPaths(config: Config): Config {
  config.database.path = expandTilde(config.database.path);

  for (const [, prov] of Object.entries(config.providers)) {
    if (prov.token_dir) prov.token_dir = expandTilde(prov.token_dir);
    if (prov.api_key) prov.api_key = interpolateEnvVars(prov.api_key);
    if (prov.base_url) prov.base_url = interpolateEnvVars(prov.base_url);
    if (prov.default_headers) {
      for (const [key, value] of Object.entries(prov.default_headers)) {
        prov.default_headers[key] = interpolateEnvVars(value);
      }
    }
  }
  if (config.skills_policy?.roots) {
    config.skills_policy.roots = config.skills_policy.roots.map((root) =>
      interpolateEnvVars(expandTilde(root)),
    );
  }
  if (config.skills_policy?.manifest_path) {
    config.skills_policy.manifest_path = interpolateEnvVars(
      expandTilde(config.skills_policy.manifest_path),
    );
  }
  if (config.skills_policy?.resource_pack_root) {
    config.skills_policy.resource_pack_root = interpolateEnvVars(
      expandTilde(config.skills_policy.resource_pack_root),
    );
  }
  if (config.skills_policy?.role_bundle_manifest) {
    config.skills_policy.role_bundle_manifest = interpolateEnvVars(
      expandTilde(config.skills_policy.role_bundle_manifest),
    );
  }
  return config;
}

export async function loadConfig(): Promise<Config> {
  const configDir = CONFIG_DIR;
  const configPath = CONFIG_PATH;

  // Ensure config dir exists
  await Bun.write(join(configDir, ".keep"), "");

  let config: Config;
  const file = Bun.file(configPath);
  if (await file.exists()) {
    const raw = await file.text();
    const parsed = parseYaml(raw) as Config;
    // Merge with a deep clone of defaults to prevent shared mutation
    config = deepMerge(structuredClone(DEFAULT_CONFIG), parsed);
  } else {
    config = structuredClone(DEFAULT_CONFIG);
    // Write default config
    await Bun.write(configPath, stringifyYaml(DEFAULT_CONFIG));
    console.log(`[config] Created default config at ${configPath}`);
  }

  return resolveConfigPaths(config);
}

let configWatcher: ReturnType<typeof watch> | null = null;
let currentConfig: Config | null = null;

export function watchConfig(onChange: (config: Config) => void): void {
  // Debounce: atomic writes can fire multiple events in quick succession
  let reloadTimer: ReturnType<typeof setTimeout> | null = null;

  const doReload = async () => {
    try {
      const newConfig = await loadConfig();
      currentConfig = newConfig;
      onChange(newConfig);
      console.log("[config] Hot-reloaded config");
    } catch (e) {
      console.error("[config] Failed to reload:", e);
    }
  };

  const scheduleReload = () => {
    if (reloadTimer) clearTimeout(reloadTimer);
    reloadTimer = setTimeout(() => {
      reloadTimer = null;
      doReload();
    }, 100); // 100ms debounce
  };

  const startWatcher = () => {
    try {
      if (configWatcher) configWatcher.close();
      configWatcher = watch(CONFIG_PATH, (eventType) => {
        if (eventType === "change") {
          scheduleReload();
        } else if (eventType === "rename") {
          // Atomic writes (tmp + rename) emit "rename" on macOS.
          // The old inode is gone — recreate the watcher after a short delay
          // to let the new file settle.
          scheduleReload();
          setTimeout(() => startWatcher(), 200);
        }
      });
    } catch (e: unknown) {
      // File may briefly not exist during atomic writes — retry
      const code = (e as { code?: string })?.code;
      if (code === "ENOENT") {
        console.warn(
          "[config] Config file temporarily absent, retrying watcher in 250ms",
        );
        setTimeout(startWatcher, 250);
      } else {
        console.error("[config] Failed to start config watcher:", e);
      }
    }
  };

  startWatcher();
}

export function getConfig(): Config | null {
  return currentConfig;
}

export function setConfig(config: Config): void {
  currentConfig = config;
}
