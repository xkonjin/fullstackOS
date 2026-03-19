import type {
  FleetRole,
  RiskLevel,
  RoutingLane,
  RoutingTaskKind,
} from "../types.ts";

export type AgentCLI = "codex" | "kimi" | "claude" | "gemini";
export type FleetSandbox =
  | "workspace-write"
  | "read-only"
  | "danger-full-access";
export type FleetReasoningEffort = "low" | "medium" | "high" | "xhigh";

export type JobStatus =
  | "pending"
  | "planning"
  | "awaiting_approval"
  | "running"
  | "completed"
  | "failed"
  | "killed";

export interface FleetJob {
  id: string;
  cli: AgentCLI;
  prompt: string;
  model: string;
  cwd: string;
  status: JobStatus;
  tmuxSession: string;
  logFile: string;
  createdAt: string;
  startedAt: string | null;
  completedAt: string | null;
  error: string | null;
  // CLI-specific options
  options: {
    oneShot?: boolean;
    thinking?: boolean;
    sandbox?: FleetSandbox;
    reasoningEffort?: FleetReasoningEffort;
    /** Git worktree directory if job was dispatched with isolation */
    worktreeDir?: string;
    /** Plan-gate: job runs plan phase first, waits for approval, then executes */
    planGate?: boolean;
    /** Path to the plan file generated during planning phase */
    planFile?: string;
    /** Who approved the plan (user ID or "auto") */
    approvedBy?: string;
    /** Timestamp when plan was approved */
    approvedAt?: string;
    /** Verification command to run after changes (self-verify) */
    verifyCommand?: string;
    /** Original dispatch metadata (for audit/re-run) */
    metadata?: Record<string, unknown>;
  };
}

export interface FleetDispatchRequest {
  cli: AgentCLI;
  prompt: string;
  model?: string;
  cwd?: string;
  one_shot?: boolean;
  thinking?: boolean;
  sandbox?: FleetSandbox;
  reasoning_effort?: FleetReasoningEffort;
  /** Routing hint for orchestrator — when true, prefers codex-family models/providers */
  codex_preferred?: boolean;
  /** Task kind hint — "review" routes to Codex review quota (separate from general usage) */
  task_kind?: "review" | "coding" | "planning" | "research" | string;
  /** Routing metadata passed through to MessagesRequest.metadata */
  metadata?: Record<string, unknown>;
  /** Role hint used by fractal swarm routing */
  role?: FleetRole | string;
}

export interface FleetStatusResponse {
  id: string;
  cli: AgentCLI;
  status: JobStatus;
  model: string;
  cwd: string;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
  error: string | null;
  tmux_session: string;
  attach_command: string;
}

// Session control types (Agent Gateway/Claude Code visibility)

export type SessionSource = "tmux" | "warp" | "unknown";

export interface ClaudeSession {
  /** Host-prefixed ID (e.g., "mbp:abc123" or "macmini:def456") */
  id: string;
  /** Short ID for display */
  short_id: string;
  /** Host where session runs: "local" or remote hostname */
  host: string;
  /** Host label for display (e.g., "mbp", "macmini") */
  host_label: string;
  /** Raw tmux session name if available */
  tmux_session: string | null;
  /** Session source: tmux (direct), warp (warp bridge), unknown */
  source: SessionSource;
  /** Current working directory if detectable */
  cwd: string | null;
  /** Process command line if detectable */
  command: string | null;
  /** Last activity timestamp (epoch ms) if available */
  last_activity: number | null;
  /** Pane PID if available */
  pane_pid: number | null;
  /** TTY for process-backed Warp sessions (e.g. /dev/ttys007) */
  tty: string | null;
  /** Warp metadata if available */
  warp_metadata?: {
    terminal?: string;
    workspace?: string;
    tab?: string;
  };
}

export interface SessionListOptions {
  /** Filter by host: "local", "macmini", or "all" */
  host?: string;
  /** Include Warp bridge metadata */
  include_warp?: boolean;
  /** Filter by source type */
  source?: "tmux" | "warp" | "unknown" | "all";
}

export interface SessionSendRequest {
  /** Message/prompt to send to the session */
  message: string;
  /** Add Enter key after message (default: true) */
  enter?: boolean;
}

export interface SessionActionResponse {
  ok: boolean;
  id: string;
  action: "send" | "kill" | "restart";
  error?: string;
}

export interface RemoteHostConfig {
  label: string;
  host: string;
  user: string;
  /** Optional SSH port (default 22) */
  port?: number;
}

// --- Swarm coordination ---

export type SwarmStatus = "pending" | "running" | "completed" | "failed";

export type SwarmTaskStatus =
  | "pending"
  | "ready"
  | "running"
  | "completed"
  | "failed"
  | "blocked"
  | "timed_out";

export interface SwarmTaskSpec {
  id: string;
  /** Strategy: "single" (default) runs one job, "replicate" fans out to N model variants */
  strategy?: "single" | "replicate";
  /** Model variants for replicate strategy — each variant becomes a separate job */
  variants?: Array<{ model: string; cli?: AgentCLI; label?: string }>;
  /** How to pick the winner among replicas: "first" (fastest), "best" (compare outputs) */
  pick?: "first" | "best";
  depends_on?: string[];
  dispatch: FleetDispatchRequest;
  timeout_ms?: number;
}

export interface FleetSwarmRequest {
  tasks: SwarmTaskSpec[];
  max_concurrency?: number;
  swarm_timeout_ms?: number;
  fail_fast?: boolean;
  metadata?: Record<string, unknown>;
}

export interface SwarmTaskState {
  id: string;
  depends_on: string[];
  status: SwarmTaskStatus;
  job_id: string | null;
  error: string | null;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
  timeout_ms: number | null;
  /** For replicate strategy: IDs of replica sub-tasks */
  replica_ids?: string[];
  /** For replicate strategy: which replica won */
  winner_replica_id?: string;
}

export interface SwarmState {
  id: string;
  status: SwarmStatus;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
  error: string | null;
  max_concurrency: number;
  swarm_timeout_ms: number | null;
  fail_fast: boolean;
  tasks: Record<string, SwarmTaskState>;
}

// --- Experiment loop orchestration ---

export type VerifierParser = "exit_code" | "json" | "junit" | "regex" | "text";

export interface VerifierPassIf {
  exit_code?: number;
  json_path?: string;
  equals?: string | number | boolean;
  gte?: number;
  lte?: number;
  regex?: string;
}

export interface VerifierStep {
  id: string;
  label: string;
  run: string;
  cwd?: string;
  timeout_ms?: number;
  required: boolean;
  parser: VerifierParser;
  pass_if?: VerifierPassIf;
  weight?: number;
}

export interface VerifierContract {
  hard_gates: VerifierStep[];
  soft_checks?: VerifierStep[];
  minimum_soft_score?: number;
  artifacts?: Array<
    "junit" | "coverage" | "benchmark" | "screenshot" | "diff" | "log"
  >;
}

export interface ExperimentDispatchPolicy {
  implement_cli?: Extract<AgentCLI, "codex" | "claude">;
  verify_cli?: Extract<AgentCLI, "codex" | "claude">;
  review_cli?: Extract<AgentCLI, "codex" | "claude">;
}

export interface ExperimentLoopRequest {
  objective?: string;
  spec_path?: string;
  plan_path?: string;
  cwd?: string;
  strategy?: "fanout-narrow";
  initial_variants?: number;
  survivors_per_round?: number;
  children_per_survivor?: number;
  max_rounds?: number;
  max_concurrency?: number;
  loop_timeout_ms?: number;
  round_timeout_ms?: number;
  stagnation_limit?: number;
  fail_fast?: boolean;
  verifier: VerifierContract;
  dispatch_policy?: ExperimentDispatchPolicy;
  metadata?: Record<string, unknown>;
}

export type ExperimentLoopStatus =
  | "pending"
  | "running"
  | "completed"
  | "failed"
  | "cancelled";

export interface CandidateState {
  candidate_id: string;
  round: number;
  parent_candidate_id?: string;
  strategy_label: string;
  prompt: string;
  swarm_id?: string;
  hard_gate_passed: boolean;
  soft_score: number | null;
  status: "pending" | "running" | "passed" | "failed";
  artifacts_dir: string;
  workspace_dir?: string;
  verifier_summary_path?: string;
  review_summary_path?: string;
  verifier_summary?: Record<string, unknown>;
}

export interface ExperimentRoundState {
  round: number;
  status: "pending" | "running" | "completed" | "failed";
  candidate_ids: string[];
  winner_candidate_id?: string;
  swarm_id?: string;
}

export interface ExperimentLoopState {
  id: string;
  status: ExperimentLoopStatus;
  objective_summary: string;
  cwd: string;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
  verifier: VerifierContract;
  strategy?: "fanout-narrow";
  artifact_root?: string;
  manifest_path?: string;
  max_rounds?: number;
  max_concurrency?: number;
  rounds: ExperimentRoundState[];
  candidates: Record<string, CandidateState>;
  winner_candidate_id?: string;
  error?: string | null;
}

// --- Fractal Fleet Planner ---

export interface FractalTask {
  /** Hierarchical ID: "1", "1.1", "1.2.1" */
  id: string;
  /** Depth in the tree (0 = root) */
  depth: number;
  /** Task description / objective */
  description: string;
  /** Whether this is a leaf (executable) or needs further decomposition */
  kind: "atomic" | "composite";
  /** Task status through the lifecycle */
  status: "pending" | "decomposing" | "ready" | "running" | "done" | "failed";
  /** Ancestor descriptions from root → parent for context propagation */
  lineage: string[];
  /** Child tasks (empty for atomic) */
  children: FractalTask[];
  /** Assigned complexity from classifier */
  complexity?: import("./task-classifier.ts").TaskComplexity;
  /** Role selected for this node */
  role?: FleetRole;
  /** Routing lane selected for this node */
  lane?: RoutingLane;
  /** Task kind selected for this node */
  task_kind?: RoutingTaskKind;
  /** Minimum acceptable risk posture */
  risk_level?: RiskLevel;
  /** Provider/tier floor for failover decisions */
  quality_floor?: string;
  /** Whether final synthesis is required before surfacing output */
  synthesis_required?: boolean;
  /** Whether review is required before completing the task */
  review_required?: boolean;
  /** Fallback band name for degraded execution */
  fallback_band?: string;
  /** Optional design artifact classification */
  design_artifact_type?: string;
  /** Skill queries derived for the task */
  required_skill_queries?: string[];
  /** Selected skill descriptors */
  selected_skills?: Array<{
    name: string;
    path: string;
    score: number;
    summary?: string;
  }>;
  /** Expert role chosen by the deterministic skill bundle */
  expert_role?: string;
  /** Deterministic skill pack identifier */
  skill_pack_id?: string;
  /** Resource pack identifier */
  resource_pack_id?: string;
  /** Resolved resource pack path */
  resource_pack_path?: string;
  /** MCP profile name */
  mcp_profile?: string;
  /** Route explanation */
  route_reason?: string;
  /** Verification profile */
  verification_profile?: string;
  /** Assigned CLI for execution */
  assigned_cli?: AgentCLI;
  /** Assigned model for execution */
  assigned_model?: string;
  /** Result summary after execution */
  result?: string;
  /** Swarm job ID if dispatched */
  swarm_job_id?: string;
}

export interface FractalPlanRequest {
  /** High-level objective to decompose */
  objective: string;
  /** Working directory for all tasks */
  cwd?: string;
  /** Max decomposition depth (default: 3) */
  max_depth?: number;
  /** CLI to use for the planning step (default: claude) */
  planner_cli?: AgentCLI;
  /** Model for the planning step (default: claude-sonnet-4-6) */
  planner_model?: string;
  /** Max concurrency for the execution swarm (default: 4) */
  max_concurrency?: number;
  /** Whether to auto-execute after planning (default: false — plan only) */
  auto_execute?: boolean;
  /** Fail fast — stop on first task failure */
  fail_fast?: boolean;
  /** Metadata passed through to dispatch */
  metadata?: Record<string, unknown>;
}

export interface FractalPlanResponse {
  /** Plan ID for tracking */
  plan_id: string;
  /** The decomposed task tree */
  tree: FractalTask;
  /** Leaf tasks ready for execution */
  leaf_tasks: {
    id: string;
    description: string;
    complexity: string;
    role?: FleetRole;
    lane?: RoutingLane;
    task_kind?: RoutingTaskKind;
    risk_level?: RiskLevel;
    quality_floor?: string;
    synthesis_required?: boolean;
    review_required?: boolean;
    fallback_band?: string;
    design_artifact_type?: string;
    required_skill_queries?: string[];
    selected_skills?: Array<{
      name: string;
      path: string;
      score: number;
      summary?: string;
    }>;
    expert_role?: string;
    skill_pack_id?: string;
    resource_pack_id?: string;
    resource_pack_path?: string;
    mcp_profile?: string;
    route_reason?: string;
    verification_profile?: string;
    assigned_cli: AgentCLI;
    assigned_model: string;
    depends_on: string[];
  }[];
  /** Summary statistics */
  stats: {
    total_tasks: number;
    leaf_tasks: number;
    by_complexity: Record<string, number>;
    by_cli: Record<string, number>;
    estimated_cost_reduction_pct: number;
  };
  /** If auto_execute, the swarm ID */
  swarm_id?: string;
  /** Status */
  status: "planned" | "executing" | "completed" | "failed";
}
