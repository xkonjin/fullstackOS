import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { homedir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import {
  cpSync,
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { fileURLToPath } from "node:url";
import type {
  CandidateState,
  ExperimentDispatchPolicy,
  ExperimentLoopRequest,
  ExperimentLoopState,
  ExperimentRoundState,
  SwarmState,
  SwarmStatus,
  VerifierContract,
  VerifierPassIf,
  VerifierStep,
} from "./types.ts";

const DEFAULT_INITIAL_VARIANTS = 4;
const DEFAULT_SURVIVORS_PER_ROUND = 2;
const DEFAULT_CHILDREN_PER_SURVIVOR = 2;
const DEFAULT_MAX_ROUNDS = 3;
const DEFAULT_MAX_CONCURRENCY = 4;
const DEFAULT_LOOP_TIMEOUT_MS = 2 * 60 * 60 * 1000;
const DEFAULT_MINIMUM_SOFT_SCORE = 0.75;
const DEFAULT_STAGNATION_LIMIT = 2;
const DEFAULT_ROUND_TIMEOUT_MS = 30 * 60 * 1000;
const DEFAULT_TICK_INTERVAL_MS = 2_000;
const LOOPS_DIR = join(homedir(), ".claudemax", "experiment-loops");
const REPO_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "../../../../");
const CANONICAL_VERIFIER_SCRIPT = join(
  REPO_ROOT,
  "skills",
  "experiment-loop",
  "scripts",
  "run_verifier.ts",
);

type ExperimentLoopSummaryStatus = ExperimentLoopState["status"];

type SwarmCoordinatorLike = {
  create(request: import("./types.ts").FleetSwarmRequest): Promise<SwarmState>;
  getSwarm(id: string): SwarmState | null;
  listSwarms(status?: SwarmStatus): SwarmState[];
  killSwarm(id: string): boolean;
};

interface CandidateDraft {
  candidate_id: string;
  round: number;
  parent_candidate_id?: string;
  strategy_label: string;
  prompt_suffix: string;
}

interface ResolvedExperimentConfig {
  strategy: "fanout-narrow";
  initial_variants: number;
  survivors_per_round: number;
  children_per_survivor: number;
  max_rounds: number;
  max_concurrency: number;
  loop_timeout_ms: number;
  fail_fast: boolean;
  stagnation_limit: number;
  round_timeout_ms: number;
  dispatch_policy: Required<ExperimentDispatchPolicy>;
}

interface StoredExperimentLoopState extends ExperimentLoopState {
  config: ResolvedExperimentConfig;
  current_round: number;
  stagnation_count: number;
  best_soft_score: number | null;
  verifier_contract_hash: string;
  request_metadata?: Record<string, unknown>;
}

interface VerifierEvaluation {
  hard_gate_passed: boolean;
  soft_score: number;
  summary: Record<string, unknown>;
}

export interface ExperimentLoopControllerOptions {
  stateDir?: string;
  tickIntervalMs?: number;
  workspaceStrategy?: "auto" | "copy";
  artifactRoot?: string;
}

export class ExperimentLoopController {
  private swarmCoordinator: SwarmCoordinatorLike;
  private loops = new Map<string, StoredExperimentLoopState>();
  private stateDir: string;
  private tickIntervalMs: number;
  private workspaceStrategy: "auto" | "copy";
  private artifactRootOverride?: string;
  private tickTimer: ReturnType<typeof setInterval> | null = null;
  private tickInProgress = false;

  constructor(
    swarmCoordinator: SwarmCoordinatorLike,
    options: ExperimentLoopControllerOptions = {},
  ) {
    this.swarmCoordinator = swarmCoordinator;
    this.stateDir = options.stateDir ?? LOOPS_DIR;
    this.tickIntervalMs = options.tickIntervalMs ?? DEFAULT_TICK_INTERVAL_MS;
    this.workspaceStrategy = options.workspaceStrategy ?? "auto";
    this.artifactRootOverride = options.artifactRoot;
    mkdirSync(this.stateDir, { recursive: true });
    this.loadLoops();
  }

  start(): void {
    if (this.tickTimer) return;
    this.tickTimer = setInterval(() => {
      void this.tickOnce();
    }, this.tickIntervalMs);
    console.log(
      `[experiment-loop] Controller started (tick=${this.tickIntervalMs}ms, loaded=${this.loops.size})`,
    );
  }

  stop(): void {
    if (this.tickTimer) {
      clearInterval(this.tickTimer);
      this.tickTimer = null;
    }
  }

  async tickOnce(): Promise<void> {
    if (this.tickInProgress) return;
    this.tickInProgress = true;
    try {
      for (const loop of this.loops.values()) {
        if (loop.status !== "running") continue;
        try {
          await this.tickLoop(loop);
        } catch (error) {
          loop.status = "failed";
          loop.error = error instanceof Error ? error.message : String(error);
          loop.completed_at = new Date().toISOString();
          this.persist(loop);
        }
      }
    } finally {
      this.tickInProgress = false;
    }
  }

  async create(request: ExperimentLoopRequest): Promise<ExperimentLoopState> {
    this.validateRequest(request);

    const config = this.resolveConfig(request);
    const id = crypto.randomUUID().replace(/-/g, "").slice(0, 12);
    const cwd = resolve(request.cwd ?? process.cwd());
    const createdAt = new Date().toISOString();
    const objectiveSummary = this.resolveObjectiveSummary(request);
    const artifactRoot = this.resolveArtifactRoot(cwd, id);
    const manifestPath = join(artifactRoot, "manifest.json");
    const verifier = this.normalizeVerifier(request.verifier);
    const verifierContractHash = createHash("sha256")
      .update(JSON.stringify(verifier))
      .digest("hex")
      .slice(0, 12);

    mkdirSync(artifactRoot, { recursive: true });
    mkdirSync(join(artifactRoot, "candidates"), { recursive: true });
    mkdirSync(join(artifactRoot, "workspaces"), { recursive: true });

    const state: StoredExperimentLoopState = {
      id,
      status: "running",
      objective_summary: objectiveSummary,
      cwd,
      created_at: createdAt,
      started_at: createdAt,
      completed_at: null,
      verifier,
      strategy: config.strategy,
      artifact_root: artifactRoot,
      manifest_path: manifestPath,
      max_rounds: config.max_rounds,
      max_concurrency: config.max_concurrency,
      rounds: [],
      candidates: {},
      winner_candidate_id: undefined,
      error: null,
      config,
      current_round: 0,
      stagnation_count: 0,
      best_soft_score: null,
      verifier_contract_hash: verifierContractHash,
      request_metadata: request.metadata,
    };

    const initialDrafts = this.buildInitialCandidates(config.initial_variants);
    const manifest = {
      id,
      created_at: createdAt,
      objective_summary: objectiveSummary,
      input: {
        objective: request.objective,
        spec_path: request.spec_path,
        plan_path: request.plan_path,
      },
      cwd,
      artifact_root: artifactRoot,
      verifier,
      config,
      candidates: initialDrafts,
    };
    writeFileSync(manifestPath, JSON.stringify(manifest, null, 2));

    const round = await this.launchRound(state, 0, initialDrafts);
    state.rounds.push(round);

    this.loops.set(id, state);
    this.persist(state);
    return this.publicState(state);
  }

  getLoop(id: string): ExperimentLoopState | null {
    const existing = this.loops.get(id);
    if (existing) return this.publicState(existing);
    const loaded = this.loadLoop(id);
    return loaded ? this.publicState(loaded) : null;
  }

  listLoops(status?: ExperimentLoopSummaryStatus): ExperimentLoopState[] {
    const seen = new Set<string>();
    const all: StoredExperimentLoopState[] = [];

    for (const loop of this.loops.values()) {
      seen.add(loop.id);
      all.push(loop);
    }

    for (const file of this.safeStateFiles()) {
      const id = file.replace(/\.json$/, "");
      if (seen.has(id)) continue;
      const loaded = this.loadLoop(id);
      if (loaded) all.push(loaded);
    }

    return all
      .filter((loop) => (status ? loop.status === status : true))
      .sort((left, right) => right.created_at.localeCompare(left.created_at))
      .map((loop) => this.publicState(loop));
  }

  cancelLoop(id: string): boolean {
    const loop = this.loops.get(id) ?? this.loadLoop(id);
    if (!loop || loop.status !== "running") return false;

    const currentRound = loop.rounds[loop.current_round];
    if (currentRound?.swarm_id) {
      this.swarmCoordinator.killSwarm(currentRound.swarm_id);
      currentRound.status = "failed";
    }
    if (currentRound) {
      this.markRoundCandidatesAsFailed(loop, currentRound);
    }

    loop.status = "cancelled";
    loop.error = "cancelled";
    loop.completed_at = new Date().toISOString();
    this.persist(loop);
    this.loops.set(id, loop);
    return true;
  }

  private async tickLoop(loop: StoredExperimentLoopState): Promise<void> {
    if (loop.started_at) {
      const elapsed = Date.now() - new Date(loop.started_at).getTime();
      if (elapsed > loop.config.loop_timeout_ms) {
        this.failLoop(loop, "loop_timeout");
        return;
      }
    }

    const round = loop.rounds[loop.current_round];
    if (!round?.swarm_id) return;

    const swarm = this.swarmCoordinator.getSwarm(round.swarm_id);
    if (!swarm) return;
    if (swarm.status === "running" || swarm.status === "pending") return;

    const candidates = round.candidate_ids
      .map((id) => loop.candidates[id])
      .filter((candidate): candidate is CandidateState => Boolean(candidate));

    if (swarm.status === "failed") {
      const hasVerifierOutput = candidates.some(
        (candidate) =>
          Boolean(candidate.verifier_summary_path) &&
          existsSync(candidate.verifier_summary_path!),
      );
      if (!hasVerifierOutput) {
        this.failLoop(loop, "swarm_failed_before_verifier");
        return;
      }
    }

    const scored = candidates.map((candidate) => {
      const evaluation = this.readVerifierEvaluation(candidate);
      candidate.hard_gate_passed = evaluation.hard_gate_passed;
      candidate.soft_score = evaluation.soft_score;
      candidate.verifier_summary = evaluation.summary;
      candidate.status = evaluation.hard_gate_passed ? "passed" : "failed";
      return candidate;
    });

    round.status = scored.some((candidate) => candidate.hard_gate_passed)
      ? "completed"
      : "failed";

    const winner = scored
      .filter((candidate) => candidate.hard_gate_passed)
      .filter(
        (candidate) =>
          (candidate.soft_score ?? 0) >=
          (loop.verifier.minimum_soft_score ?? DEFAULT_MINIMUM_SOFT_SCORE),
      )
      .sort((left, right) => (right.soft_score ?? 0) - (left.soft_score ?? 0))[0];

    if (winner) {
      round.winner_candidate_id = winner.candidate_id;
      loop.winner_candidate_id = winner.candidate_id;
      loop.status = "completed";
      loop.completed_at = new Date().toISOString();
      loop.best_soft_score = winner.soft_score;
      this.persist(loop);
      return;
    }

    const topScore = scored.reduce(
      (best, candidate) => Math.max(best, candidate.soft_score ?? 0),
      0,
    );

    if (loop.best_soft_score !== null && topScore <= loop.best_soft_score + 1e-9) {
      loop.stagnation_count += 1;
    } else {
      loop.best_soft_score = topScore;
      loop.stagnation_count = 0;
    }

    if (loop.stagnation_count >= loop.config.stagnation_limit) {
      this.failLoop(loop, "stagnation_limit_reached");
      return;
    }

    if (loop.current_round + 1 >= loop.config.max_rounds) {
      this.failLoop(loop, "max_rounds_reached");
      return;
    }

    const nextDrafts = this.deriveNextRoundDrafts(loop, round.round, scored);
    if (nextDrafts.length === 0) {
      this.failLoop(loop, "no_candidates_remaining");
      return;
    }

    const nextRoundNumber = round.round + 1;
    const nextRound = await this.launchRound(loop, nextRoundNumber, nextDrafts);
    loop.current_round = nextRoundNumber;
    loop.rounds.push(nextRound);
    this.persist(loop);
  }

  private async launchRound(
    loop: StoredExperimentLoopState,
    roundNumber: number,
    drafts: CandidateDraft[],
  ): Promise<ExperimentRoundState> {
    const tasks: import("./types.ts").SwarmTaskSpec[] = [];
    const candidateIds: string[] = [];

    for (const draft of drafts) {
      const candidate = this.initializeCandidate(loop, draft);
      loop.candidates[candidate.candidate_id] = candidate;
      candidateIds.push(candidate.candidate_id);

      const implementTaskId = `${candidate.candidate_id}:implement`;
      const verifyTaskId = `${candidate.candidate_id}:verify`;
      const reviewTaskId = `${candidate.candidate_id}:review`;
      const metadataBase = {
        experiment_loop_id: loop.id,
        experiment_round: roundNumber,
        candidate_id: candidate.candidate_id,
        parent_candidate_id: candidate.parent_candidate_id,
        candidate_strategy: candidate.strategy_label,
        verifier_contract_hash: loop.verifier_contract_hash,
      };

      tasks.push({
        id: implementTaskId,
        timeout_ms: loop.config.round_timeout_ms,
        dispatch: {
          cli: loop.config.dispatch_policy.implement_cli,
          cwd: candidate.workspace_dir,
          prompt: this.buildImplementPrompt(loop, candidate),
          one_shot: true,
          metadata: {
            ...metadataBase,
            task_role: "implement",
          },
        },
      });
      tasks.push({
        id: verifyTaskId,
        depends_on: [implementTaskId],
        timeout_ms: loop.config.round_timeout_ms,
        dispatch: {
          cli: loop.config.dispatch_policy.verify_cli,
          cwd: candidate.workspace_dir,
          prompt: this.buildVerifyPrompt(loop, candidate),
          one_shot: true,
          metadata: {
            ...metadataBase,
            task_role: "verify",
          },
        },
      });
      tasks.push({
        id: reviewTaskId,
        depends_on: [verifyTaskId],
        timeout_ms: loop.config.round_timeout_ms,
        dispatch: {
          cli: loop.config.dispatch_policy.review_cli,
          cwd: candidate.workspace_dir,
          prompt: this.buildReviewPrompt(loop, candidate),
          one_shot: true,
          metadata: {
            ...metadataBase,
            task_role: "review",
          },
        },
      });
    }

    const swarm = await this.swarmCoordinator.create({
      tasks,
      max_concurrency: loop.config.max_concurrency,
      swarm_timeout_ms: loop.config.round_timeout_ms,
      fail_fast: loop.config.fail_fast,
      metadata: {
        experiment_loop_id: loop.id,
        experiment_round: roundNumber,
      },
    });

    for (const candidateId of candidateIds) {
      loop.candidates[candidateId]!.swarm_id = swarm.id;
      loop.candidates[candidateId]!.status = "running";
    }

    return {
      round: roundNumber,
      status: "running",
      candidate_ids: candidateIds,
      swarm_id: swarm.id,
    };
  }

  private initializeCandidate(
    loop: StoredExperimentLoopState,
    draft: CandidateDraft,
  ): CandidateState {
    const candidateRoot = join(loop.artifact_root!, "candidates", draft.candidate_id);
    const workspaceDir = this.prepareWorkspace(loop.cwd, loop.id, draft.candidate_id);
    mkdirSync(candidateRoot, { recursive: true });

    const prompt = this.composeCandidatePrompt(loop, draft, workspaceDir, candidateRoot);
    const candidate: CandidateState = {
      candidate_id: draft.candidate_id,
      round: draft.round,
      parent_candidate_id: draft.parent_candidate_id,
      strategy_label: draft.strategy_label,
      prompt,
      hard_gate_passed: false,
      soft_score: null,
      status: "pending",
      artifacts_dir: candidateRoot,
      workspace_dir: workspaceDir,
      verifier_summary_path: join(candidateRoot, "verifier-summary.json"),
      review_summary_path: join(candidateRoot, "review-summary.md"),
      verifier_summary: undefined,
    };

    writeFileSync(join(candidateRoot, "candidate.json"), JSON.stringify(candidate, null, 2));
    return candidate;
  }

  private composeCandidatePrompt(
    loop: StoredExperimentLoopState,
    draft: CandidateDraft,
    workspaceDir: string,
    candidateRoot: string,
  ): string {
    return [
      `Objective: ${loop.objective_summary}`,
      `Round: ${draft.round}`,
      `Strategy: ${draft.strategy_label}`,
      draft.parent_candidate_id
        ? `Parent candidate: ${draft.parent_candidate_id}`
        : "Parent candidate: none",
      `Workspace: ${workspaceDir}`,
      `Artifacts: ${candidateRoot}`,
      `Manifest: ${loop.manifest_path}`,
      draft.prompt_suffix,
    ].join("\n");
  }

  private buildImplementPrompt(loop: StoredExperimentLoopState, candidate: CandidateState): string {
    return [
      "You are an implementation candidate in an experiment loop.",
      candidate.prompt,
      "Read the manifest and any referenced spec/plan first.",
      "Work only inside the provided workspace.",
      "Aim to satisfy the verifier contract with the smallest coherent change set.",
      `When finished, write a concise implementation note to ${join(candidate.artifacts_dir, "implement-summary.md")}.`,
      "Do not claim success without actually running the relevant local checks you need before handoff to verifier.",
    ].join("\n\n");
  }

  private buildVerifyPrompt(loop: StoredExperimentLoopState, candidate: CandidateState): string {
    const scriptPath = existsSync(CANONICAL_VERIFIER_SCRIPT)
      ? CANONICAL_VERIFIER_SCRIPT
      : join(candidate.workspace_dir ?? loop.cwd, "skills", "experiment-loop", "scripts", "run_verifier.ts");
    return [
      "You are the verifier for an experiment-loop candidate.",
      `Run this exact command from the workspace: bun run ${this.shellQuote(scriptPath)} --manifest ${this.shellQuote(loop.manifest_path!)} --candidate ${this.shellQuote(candidate.candidate_id)}`,
      "If the command fails, investigate only enough to capture the failure in the verifier output; do not implement new fixes in this step.",
      `Ensure ${candidate.verifier_summary_path} exists before you finish.`,
    ].join("\n\n");
  }

  private buildReviewPrompt(loop: StoredExperimentLoopState, candidate: CandidateState): string {
    return [
      "You are the reviewer for an experiment-loop candidate.",
      `Read ${candidate.verifier_summary_path} and ${join(candidate.artifacts_dir, "implement-summary.md")} if present.`,
      `Write a short markdown review to ${candidate.review_summary_path}.`,
      "Include: notable risks, why this candidate should or should not continue, and one improvement direction.",
      "Do not modify code in this review step.",
    ].join("\n\n");
  }

  private deriveNextRoundDrafts(
    loop: StoredExperimentLoopState,
    completedRound: number,
    candidates: CandidateState[],
  ): CandidateDraft[] {
    const passing = candidates
      .filter((candidate) => candidate.hard_gate_passed)
      .sort((left, right) => (right.soft_score ?? 0) - (left.soft_score ?? 0));
    const source = (passing.length > 0 ? passing : candidates)
      .slice()
      .sort((left, right) => (right.soft_score ?? 0) - (left.soft_score ?? 0))
      .slice(0, loop.config.survivors_per_round);

    const nextRound = completedRound + 1;
    const drafts: CandidateDraft[] = [];
    for (const candidate of source) {
      drafts.push({
        candidate_id: `r${nextRound}-${candidate.candidate_id}-repair`,
        round: nextRound,
        parent_candidate_id: candidate.candidate_id,
        strategy_label: `${candidate.strategy_label} · repair`,
        prompt_suffix:
          "Create a repair-focused variant. Prioritize fixing failed verifier gates and preserving the candidate's strongest parts.",
      });
      drafts.push({
        candidate_id: `r${nextRound}-${candidate.candidate_id}-opt`,
        round: nextRound,
        parent_candidate_id: candidate.candidate_id,
        strategy_label: `${candidate.strategy_label} · optimize`,
        prompt_suffix:
          "Create an optimization-focused variant. Keep hard-gate behavior intact while improving quality, maintainability, or soft-score signals.",
      });
    }

    return drafts.slice(0, loop.config.survivors_per_round * loop.config.children_per_survivor);
  }

  private buildInitialCandidates(count: number): CandidateDraft[] {
    const templates: Array<Pick<CandidateDraft, "strategy_label" | "prompt_suffix">> = [
      {
        strategy_label: "baseline conservative",
        prompt_suffix: "Prefer the clearest, lowest-risk implementation that matches existing patterns.",
      },
      {
        strategy_label: "minimal-change fast path",
        prompt_suffix: "Minimize touched files and keep changes small, but still satisfy the verifier contract.",
      },
      {
        strategy_label: "robustness first",
        prompt_suffix: "Bias toward explicit error handling, guardrails, and maintainable structure.",
      },
      {
        strategy_label: "alternate approach",
        prompt_suffix: "Use a materially different implementation approach if one exists, while staying compatible with current architecture.",
      },
    ];

    const safeCount = Math.max(1, count);
    const drafts: CandidateDraft[] = [];
    for (let index = 0; index < safeCount; index += 1) {
      const template = templates[index % templates.length]!;
      drafts.push({
        candidate_id: `r0-c${index + 1}`,
        round: 0,
        strategy_label:
          index < templates.length
            ? template.strategy_label
            : `${template.strategy_label} #${Math.floor(index / templates.length) + 1}`,
        prompt_suffix: template.prompt_suffix,
      });
    }
    return drafts;
  }

  private resolveConfig(request: ExperimentLoopRequest): ResolvedExperimentConfig {
    const initialVariants = request.initial_variants ?? DEFAULT_INITIAL_VARIANTS;
    const survivorsPerRound =
      request.survivors_per_round ?? DEFAULT_SURVIVORS_PER_ROUND;
    const childrenPerSurvivor =
      request.children_per_survivor ?? DEFAULT_CHILDREN_PER_SURVIVOR;
    const maxRounds = request.max_rounds ?? DEFAULT_MAX_ROUNDS;
    const maxConcurrency = request.max_concurrency ?? DEFAULT_MAX_CONCURRENCY;
    const loopTimeoutMs = request.loop_timeout_ms ?? DEFAULT_LOOP_TIMEOUT_MS;
    const roundTimeoutMs = request.round_timeout_ms ?? DEFAULT_ROUND_TIMEOUT_MS;
    const stagnationLimit = request.stagnation_limit ?? DEFAULT_STAGNATION_LIMIT;
    if (initialVariants < 1) {
      throw new Error("initial_variants must be >= 1");
    }
    if (survivorsPerRound < 1) {
      throw new Error("survivors_per_round must be >= 1");
    }
    if (childrenPerSurvivor < 1) {
      throw new Error("children_per_survivor must be >= 1");
    }
    if (maxRounds < 1) {
      throw new Error("max_rounds must be >= 1");
    }
    if (maxConcurrency < 1) {
      throw new Error("max_concurrency must be >= 1");
    }
    if (loopTimeoutMs < 1000) {
      throw new Error("loop_timeout_ms must be >= 1000");
    }
    if (roundTimeoutMs < 1000) {
      throw new Error("round_timeout_ms must be >= 1000");
    }
    if (stagnationLimit < 1) {
      throw new Error("stagnation_limit must be >= 1");
    }

    return {
      strategy: request.strategy ?? "fanout-narrow",
      initial_variants: initialVariants,
      survivors_per_round: survivorsPerRound,
      children_per_survivor: childrenPerSurvivor,
      max_rounds: maxRounds,
      max_concurrency: maxConcurrency,
      loop_timeout_ms: loopTimeoutMs,
      fail_fast: request.fail_fast ?? false,
      stagnation_limit: stagnationLimit,
      round_timeout_ms: roundTimeoutMs,
      dispatch_policy: {
        implement_cli: request.dispatch_policy?.implement_cli ?? "codex",
        verify_cli: request.dispatch_policy?.verify_cli ?? "claude",
        review_cli: request.dispatch_policy?.review_cli ?? "claude",
      },
    };
  }

  private failLoop(loop: StoredExperimentLoopState, reason: string): void {
    const currentRound = loop.rounds[loop.current_round];
    loop.error = reason;
    if (currentRound?.swarm_id) {
      this.swarmCoordinator.killSwarm(currentRound.swarm_id);
    }
    if (currentRound) {
      currentRound.status = "failed";
      this.markRoundCandidatesAsFailed(loop, currentRound);
    }
    loop.status = "failed";
    loop.completed_at = new Date().toISOString();
    this.persist(loop);
  }

  private markRoundCandidatesAsFailed(
    loop: StoredExperimentLoopState,
    round: ExperimentRoundState,
  ): void {
    for (const candidateId of round.candidate_ids) {
      const candidate = loop.candidates[candidateId];
      if (!candidate) continue;
      if (candidate.status === "passed" || candidate.status === "failed") {
        continue;
      }
      candidate.status = "failed";
      if (!candidate.verifier_summary) {
        candidate.verifier_summary = {
          error: loop.error ?? "loop_failed",
          candidate_id: candidate.candidate_id,
        };
      }
    }
  }

  private normalizeVerifier(verifier: VerifierContract): VerifierContract {
    return {
      hard_gates: verifier.hard_gates.map((step) => this.normalizeStep(step, true)),
      soft_checks: (verifier.soft_checks ?? []).map((step) => this.normalizeStep(step, false)),
      minimum_soft_score:
        verifier.minimum_soft_score ?? DEFAULT_MINIMUM_SOFT_SCORE,
      artifacts: verifier.artifacts ?? ["log"],
    };
  }

  private normalizeStep(step: VerifierStep, required: boolean): VerifierStep {
    return {
      ...step,
      required,
      timeout_ms: step.timeout_ms ?? DEFAULT_ROUND_TIMEOUT_MS,
      pass_if: step.pass_if ?? { exit_code: 0 },
    };
  }

  private validateRequest(request: ExperimentLoopRequest): void {
    if (!request.objective && !request.spec_path && !request.plan_path) {
      throw new Error("Experiment loop requires objective, spec_path, or plan_path");
    }
    if (!request.verifier?.hard_gates || request.verifier.hard_gates.length === 0) {
      throw new Error("Experiment loop requires verifier.hard_gates");
    }
    if (request.spec_path && !existsSync(resolve(request.spec_path))) {
      throw new Error(`Spec path not found: ${request.spec_path}`);
    }
    if (request.plan_path && !existsSync(resolve(request.plan_path))) {
      throw new Error(`Plan path not found: ${request.plan_path}`);
    }
  }

  private resolveObjectiveSummary(request: ExperimentLoopRequest): string {
    if (request.objective) return request.objective.trim().slice(0, 240);

    const sourcePath = request.plan_path ?? request.spec_path;
    if (!sourcePath) return "experiment-loop objective";
    try {
      const text = readFileSync(resolve(sourcePath), "utf-8");
      const line = text
        .split(/\r?\n/)
        .map((entry) => entry.trim())
        .find(Boolean);
      return (line ?? basename(sourcePath)).replace(/^#+\s*/, "").slice(0, 240);
    } catch {
      return basename(sourcePath);
    }
  }

  private resolveArtifactRoot(cwd: string, loopId: string): string {
    if (this.artifactRootOverride) {
      return join(resolve(this.artifactRootOverride), loopId);
    }
    return join(cwd, ".artifacts", "experiment-loops", loopId);
  }

  private prepareWorkspace(baseCwd: string, loopId: string, candidateId: string): string {
    const workspaceRoot = join(this.resolveArtifactRoot(baseCwd, loopId), "workspaces");
    const workspaceDir = join(workspaceRoot, candidateId);
    rmSync(workspaceDir, { recursive: true, force: true });
    mkdirSync(workspaceRoot, { recursive: true });

    const sourceDir = this.tryGitRoot(baseCwd) ?? baseCwd;

    if (this.workspaceStrategy === "auto") {
      const gitRoot = this.tryGitRoot(baseCwd);
      if (gitRoot) {
        try {
          execFileSync("git", ["worktree", "add", "--detach", workspaceDir, "HEAD"], {
            cwd: gitRoot,
            stdio: "ignore",
          });
          return workspaceDir;
        } catch {
          rmSync(workspaceDir, { recursive: true, force: true });
        }
      }
    }

    cpSync(sourceDir, workspaceDir, {
      recursive: true,
      force: true,
      filter: (src) => {
        const name = basename(src);
        return !["node_modules", ".git", ".artifacts"].includes(name);
      },
    });
    return workspaceDir;
  }

  private tryGitRoot(cwd: string): string | null {
    try {
      return execFileSync("git", ["rev-parse", "--show-toplevel"], {
        cwd,
        encoding: "utf-8",
        stdio: ["ignore", "pipe", "ignore"],
      }).trim();
    } catch {
      return null;
    }
  }

  private readVerifierEvaluation(candidate: CandidateState): VerifierEvaluation {
    if (!candidate.verifier_summary_path || !existsSync(candidate.verifier_summary_path)) {
      return {
        hard_gate_passed: false,
        soft_score: 0,
        summary: {
          error: "missing_verifier_summary",
          candidate_id: candidate.candidate_id,
        },
      };
    }

    try {
      const parsed = JSON.parse(
        readFileSync(candidate.verifier_summary_path, "utf-8"),
      ) as Record<string, unknown>;
      return {
        hard_gate_passed: Boolean(parsed.hard_gate_passed),
        soft_score:
          typeof parsed.soft_score === "number" ? parsed.soft_score : 0,
        summary: parsed,
      };
    } catch (error) {
      return {
        hard_gate_passed: false,
        soft_score: 0,
        summary: {
          error: error instanceof Error ? error.message : String(error),
          candidate_id: candidate.candidate_id,
        },
      };
    }
  }

  private persist(loop: StoredExperimentLoopState): void {
    writeFileSync(
      join(this.stateDir, `${loop.id}.json`),
      JSON.stringify(loop, null, 2),
    );
  }

  private loadLoop(id: string): StoredExperimentLoopState | null {
    try {
      const file = join(this.stateDir, `${id}.json`);
      const parsed = JSON.parse(readFileSync(file, "utf-8")) as StoredExperimentLoopState;
      this.loops.set(id, parsed);
      return parsed;
    } catch {
      return null;
    }
  }

  private loadLoops(): void {
    for (const file of this.safeStateFiles()) {
      try {
        const parsed = JSON.parse(
          readFileSync(join(this.stateDir, file), "utf-8"),
        ) as StoredExperimentLoopState;
        this.loops.set(parsed.id, parsed);
      } catch {
        // ignore corrupt files
      }
    }
  }

  private safeStateFiles(): string[] {
    try {
      return readdirSync(this.stateDir).filter((file) => file.endsWith(".json"));
    } catch {
      return [];
    }
  }

  private publicState(loop: StoredExperimentLoopState): ExperimentLoopState {
    return {
      id: loop.id,
      status: loop.status,
      objective_summary: loop.objective_summary,
      cwd: loop.cwd,
      created_at: loop.created_at,
      started_at: loop.started_at,
      completed_at: loop.completed_at,
      verifier: loop.verifier,
      strategy: loop.strategy,
      artifact_root: loop.artifact_root,
      manifest_path: loop.manifest_path,
      max_rounds: loop.max_rounds,
      max_concurrency: loop.max_concurrency,
      rounds: loop.rounds,
      candidates: loop.candidates,
      winner_candidate_id: loop.winner_candidate_id,
      error: loop.error,
    };
  }

  private shellQuote(value: string): string {
    return `'${value.replace(/'/g, `'"'"'`)}'`;
  }
}

export function evaluateVerifierStep(
  parser: VerifierStep["parser"],
  output: string,
  passIf: VerifierPassIf | undefined,
  exitCode: number,
): boolean {
  const condition = passIf ?? { exit_code: 0 };
  switch (parser) {
    case "exit_code":
      return exitCode === (condition.exit_code ?? 0);
    case "regex":
      return condition.regex ? new RegExp(condition.regex, "m").test(output) : exitCode === 0;
    case "text": {
      const trimmed = output.trim();
      if (condition.equals !== undefined) return trimmed === String(condition.equals);
      const numeric = Number(trimmed);
      if (Number.isFinite(numeric)) {
        if (condition.gte !== undefined && numeric < condition.gte) return false;
        if (condition.lte !== undefined && numeric > condition.lte) return false;
        return true;
      }
      return exitCode === 0;
    }
    case "json": {
      try {
        const parsed = JSON.parse(output);
        const actual = condition.json_path ? readJsonPath(parsed, condition.json_path) : parsed;
        if (condition.equals !== undefined) return actual === condition.equals;
        if (typeof actual === "number") {
          if (condition.gte !== undefined && actual < condition.gte) return false;
          if (condition.lte !== undefined && actual > condition.lte) return false;
          return true;
        }
        return Boolean(actual);
      } catch {
        return false;
      }
    }
    case "junit": {
      const content = output.trim().endsWith(".xml") && existsSync(output.trim())
        ? readFileSync(output.trim(), "utf-8")
        : output;
      const failures = /failures="(\d+)"/i.exec(content)?.[1];
      const errors = /errors="(\d+)"/i.exec(content)?.[1];
      return Number(failures ?? 0) === 0 && Number(errors ?? 0) === 0;
    }
    default:
      return exitCode === 0;
  }
}

function readJsonPath(value: unknown, jsonPath: string): unknown {
  return jsonPath.split(".").reduce<unknown>((current, key) => {
    if (!current || typeof current !== "object") return undefined;
    return (current as Record<string, unknown>)[key];
  }, value);
}
