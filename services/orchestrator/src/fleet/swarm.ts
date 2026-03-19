import { homedir } from "os";
import { join } from "path";
import { mkdirSync, writeFileSync, readFileSync, readdirSync } from "fs";
import type { FleetDispatcher } from "./dispatcher.ts";
import type {
  FleetSwarmRequest,
  SwarmState,
  SwarmTaskState,
  SwarmTaskStatus,
  SwarmStatus,
} from "./types.ts";

const SWARMS_DIR = join(homedir(), ".claudemax", "fleet-swarms");
const DEFAULT_MAX_CONCURRENCY = 4;
const DEFAULT_SWARM_TIMEOUT_MS = 2 * 60 * 60 * 1000; // 2 hours
const DEFAULT_TASK_TIMEOUT_MS = 60 * 60 * 1000; // 1 hour
const TICK_INTERVAL_MS = 2000;

export class SwarmCoordinator {
  private dispatcher: FleetDispatcher;
  private swarms: Map<string, SwarmState> = new Map();
  private tickTimer: ReturnType<typeof setInterval> | null = null;
  private tickInProgress = false;

  constructor(dispatcher: FleetDispatcher) {
    this.dispatcher = dispatcher;
    mkdirSync(SWARMS_DIR, { recursive: true });
    this.loadActiveSwarms();
  }

  start(): void {
    if (this.tickTimer) return;
    this.tickTimer = setInterval(() => this.tick(), TICK_INTERVAL_MS);
    console.log(
      `[swarm] Coordinator started (tick=${TICK_INTERVAL_MS}ms, loaded=${this.swarms.size} active swarms)`,
    );
  }

  stop(): void {
    if (this.tickTimer) {
      clearInterval(this.tickTimer);
      this.tickTimer = null;
    }
  }

  /**
   * Create and start a new swarm from a task graph.
   * Validates the DAG, persists state, and dispatches root tasks.
   */
  async create(request: FleetSwarmRequest): Promise<SwarmState> {
    // Validate DAG
    const validationError = this.validateDAG(request);
    if (validationError) {
      throw new Error(validationError);
    }

    const id = crypto.randomUUID().replace(/-/g, "").slice(0, 12);
    const now = new Date().toISOString();

    // Expand replicate tasks into N replica sub-tasks
    const expandedSpecs = [...request.tasks];
    const replicaParents = new Map<string, string[]>();
    for (const spec of request.tasks) {
      if (
        spec.strategy === "replicate" &&
        spec.variants &&
        spec.variants.length > 0
      ) {
        const replicaIds: string[] = [];
        for (let i = 0; i < spec.variants.length; i++) {
          const variant = spec.variants[i]!;
          const replicaId = `${spec.id}__r${i}`;
          replicaIds.push(replicaId);
          expandedSpecs.push({
            id: replicaId,
            depends_on: spec.depends_on,
            dispatch: {
              ...spec.dispatch,
              cli: variant.cli ?? spec.dispatch.cli,
              model: variant.model,
              metadata: {
                ...spec.dispatch.metadata,
                replica_of: spec.id,
                replica_label: variant.label ?? variant.model,
                isolate: true, // each replica gets its own worktree
              },
            },
            timeout_ms: spec.timeout_ms,
          });
        }
        replicaParents.set(spec.id, replicaIds);
      }
    }

    const tasks: Record<string, SwarmTaskState> = {};
    for (const spec of expandedSpecs) {
      // Skip the parent replicate task (only its sub-tasks run)
      if (replicaParents.has(spec.id)) continue;
      const depsResolved = !spec.depends_on || spec.depends_on.length === 0;
      tasks[spec.id] = {
        id: spec.id,
        depends_on: spec.depends_on ?? [],
        status: depsResolved ? "ready" : "pending",
        job_id: null,
        error: null,
        created_at: now,
        started_at: null,
        completed_at: null,
        timeout_ms: spec.timeout_ms ?? null,
      };
    }

    // Add parent task entries that track their replicas
    for (const [parentId, replicaIds] of replicaParents) {
      tasks[parentId] = {
        id: parentId,
        depends_on:
          request.tasks.find((t) => t.id === parentId)?.depends_on ?? [],
        status: "pending",
        job_id: null,
        error: null,
        created_at: now,
        started_at: null,
        completed_at: null,
        timeout_ms: null,
        replica_ids: replicaIds,
      };
    }

    const swarm: SwarmState = {
      id,
      status: "running",
      created_at: now,
      started_at: now,
      completed_at: null,
      error: null,
      max_concurrency: request.max_concurrency ?? DEFAULT_MAX_CONCURRENCY,
      swarm_timeout_ms: request.swarm_timeout_ms ?? DEFAULT_SWARM_TIMEOUT_MS,
      fail_fast: request.fail_fast ?? false,
      tasks,
    };

    this.swarms.set(id, swarm);
    this.persist(swarm);

    // Store dispatch specs for ready tasks (needed for dispatching)
    const specMap = new Map(request.tasks.map((t) => [t.id, t]));
    (swarm as SwarmStateInternal)._specs = specMap;

    // Dispatch root tasks immediately
    await this.dispatchReadyTasks(swarm);

    console.log(
      `[swarm] Created swarm ${id} with ${request.tasks.length} tasks (max_concurrency=${swarm.max_concurrency})`,
    );
    return swarm;
  }

  getSwarm(id: string): SwarmState | null {
    return this.swarms.get(id) ?? this.loadSwarm(id);
  }

  listSwarms(status?: SwarmStatus): SwarmState[] {
    const all = [...this.swarms.values()];
    if (status) return all.filter((s) => s.status === status);
    return all;
  }

  /**
   * Kill all running tasks in a swarm and mark it failed.
   */
  killSwarm(id: string): boolean {
    const swarm = this.swarms.get(id);
    if (!swarm || swarm.status !== "running") return false;

    for (const task of Object.values(swarm.tasks)) {
      if (task.status === "running" && task.job_id) {
        this.dispatcher.killJob(task.job_id);
        task.status = "failed";
        task.error = "swarm_killed";
        task.completed_at = new Date().toISOString();
      }
      if (task.status === "pending" || task.status === "ready") {
        task.status = "blocked";
        task.error = "swarm_killed";
      }
    }

    swarm.status = "failed";
    swarm.error = "killed";
    swarm.completed_at = new Date().toISOString();
    this.persist(swarm);
    return true;
  }

  // --- Internal ---

  private validateDAG(request: FleetSwarmRequest): string | null {
    if (!request.tasks || request.tasks.length === 0) {
      return "Swarm must have at least one task";
    }
    if (request.tasks.length > 50) {
      return "Swarm exceeds maximum of 50 tasks";
    }

    const ids = new Set<string>();
    for (const task of request.tasks) {
      if (!task.id) return "Task missing required field: id";
      if (!task.dispatch)
        return `Task ${task.id} missing required field: dispatch`;
      if (!task.dispatch.cli || !task.dispatch.prompt) {
        return `Task ${task.id} dispatch missing required fields: cli, prompt`;
      }
      if (ids.has(task.id)) return `Duplicate task id: ${task.id}`;
      ids.add(task.id);
    }

    // Validate dependency references
    for (const task of request.tasks) {
      if (!task.depends_on) continue;
      for (const dep of task.depends_on) {
        if (!ids.has(dep)) {
          return `Task ${task.id} depends on unknown task: ${dep}`;
        }
        if (dep === task.id) {
          return `Task ${task.id} depends on itself`;
        }
      }
    }

    // Cycle detection via Kahn's algorithm
    const inDegree = new Map<string, number>();
    const adj = new Map<string, string[]>();
    for (const task of request.tasks) {
      inDegree.set(task.id, task.depends_on?.length ?? 0);
      adj.set(task.id, []);
    }
    for (const task of request.tasks) {
      if (!task.depends_on) continue;
      for (const dep of task.depends_on) {
        adj.get(dep)!.push(task.id);
      }
    }

    const queue: string[] = [];
    for (const [id, deg] of inDegree) {
      if (deg === 0) queue.push(id);
    }

    let processed = 0;
    while (queue.length > 0) {
      const node = queue.shift()!;
      processed++;
      for (const child of adj.get(node) ?? []) {
        const newDeg = (inDegree.get(child) ?? 1) - 1;
        inDegree.set(child, newDeg);
        if (newDeg === 0) queue.push(child);
      }
    }

    if (processed !== request.tasks.length) {
      return "Task graph contains a cycle";
    }

    return null;
  }

  /**
   * Main scheduler tick — runs every TICK_INTERVAL_MS.
   * Refreshes job statuses, resolves transitions, dispatches ready tasks.
   */
  private async tick(): Promise<void> {
    if (this.tickInProgress) return;
    this.tickInProgress = true;
    try {
      for (const swarm of this.swarms.values()) {
        if (swarm.status !== "running") continue;

        try {
          await this.tickSwarm(swarm);
        } catch (e) {
          console.error(
            `[swarm] Tick error for swarm ${swarm.id}:`,
            e instanceof Error ? e.message : String(e),
          );
        }
      }

      // Remove terminal swarms from in-memory map to prevent unbounded growth
      for (const [id, swarm] of this.swarms) {
        if (swarm.status === "completed" || swarm.status === "failed") {
          this.swarms.delete(id);
        }
      }
    } finally {
      this.tickInProgress = false;
    }
  }

  private async tickSwarm(swarm: SwarmState): Promise<void> {
    const now = Date.now();
    let changed = false;

    // Check swarm-level timeout
    if (swarm.swarm_timeout_ms && swarm.started_at) {
      const elapsed = now - new Date(swarm.started_at).getTime();
      if (elapsed > swarm.swarm_timeout_ms) {
        console.log(
          `[swarm] ⏱️ Swarm ${swarm.id} timed out after ${Math.round(elapsed / 1000)}s`,
        );
        for (const task of Object.values(swarm.tasks)) {
          if (task.status === "running" && task.job_id) {
            this.dispatcher.killJob(task.job_id);
            task.status = "timed_out";
            task.error = "swarm_timeout";
            task.completed_at = new Date().toISOString();
          }
          if (task.status === "pending" || task.status === "ready") {
            task.status = "blocked";
            task.error = "swarm_timeout";
          }
        }
        swarm.status = "failed";
        swarm.error = "swarm_timeout";
        swarm.completed_at = new Date().toISOString();
        this.persist(swarm);
        return;
      }
    }

    // Refresh running tasks
    for (const task of Object.values(swarm.tasks)) {
      if (task.status !== "running") continue;
      // Recovery: task persisted as running but job_id never written (crash during dispatch)
      if (!task.job_id) {
        task.status = "failed";
        task.error =
          "dispatch_incomplete (missing job_id — likely crash during dispatch)";
        task.completed_at = new Date().toISOString();
        changed = true;
        continue;
      }

      const job = this.dispatcher.refreshJobStatus(task.job_id);
      if (!job) {
        task.status = "failed";
        task.error = "job_not_found";
        task.completed_at = new Date().toISOString();
        changed = true;
        continue;
      }

      if (job.status === "completed") {
        task.status = "completed";
        task.completed_at = new Date().toISOString();
        changed = true;
        console.log(`[swarm] ✅ Task ${task.id} completed (swarm=${swarm.id})`);
        continue; // already terminal — skip timeout check
      } else if (job.status === "failed" || job.status === "killed") {
        task.status = "failed";
        task.error = job.error ?? job.status;
        task.completed_at = new Date().toISOString();
        changed = true;
        console.log(
          `[swarm] ❌ Task ${task.id} failed: ${task.error} (swarm=${swarm.id})`,
        );
        continue; // already terminal — skip timeout check
      }

      // Per-task timeout (only reached if task is still running)
      const taskTimeout = task.timeout_ms ?? DEFAULT_TASK_TIMEOUT_MS;
      if (task.started_at) {
        const taskElapsed = now - new Date(task.started_at).getTime();
        if (taskElapsed > taskTimeout) {
          this.dispatcher.killJob(task.job_id);
          task.status = "timed_out";
          task.error = `task_timeout (${Math.round(taskElapsed / 1000)}s)`;
          task.completed_at = new Date().toISOString();
          changed = true;
          console.log(
            `[swarm] ⏱️ Task ${task.id} timed out (swarm=${swarm.id})`,
          );
        }
      }
    }

    // Resolve dependency transitions
    for (const task of Object.values(swarm.tasks)) {
      if (task.status !== "pending") continue;

      const allDepsDone = task.depends_on.every(
        (dep) => swarm.tasks[dep]?.status === "completed",
      );
      const anyDepFailed = task.depends_on.some((dep) => {
        const s = swarm.tasks[dep]?.status;
        return s === "failed" || s === "blocked" || s === "timed_out";
      });

      if (anyDepFailed) {
        task.status = "blocked";
        task.error = "dependency_failed";
        changed = true;
      } else if (allDepsDone) {
        task.status = "ready";
        changed = true;
      }
    }

    // Fail-fast: if any task failed and fail_fast is true, block all pending/ready
    if (swarm.fail_fast) {
      const anyFailed = Object.values(swarm.tasks).some(
        (t) => t.status === "failed" || t.status === "timed_out",
      );
      if (anyFailed) {
        for (const task of Object.values(swarm.tasks)) {
          if (task.status === "pending" || task.status === "ready") {
            task.status = "blocked";
            task.error = "fail_fast";
            changed = true;
          }
        }
      }
    }

    // Resolve replica parents: when all replicas finish, mark parent complete
    for (const task of Object.values(swarm.tasks)) {
      if (
        !task.replica_ids ||
        task.status === "completed" ||
        task.status === "failed"
      )
        continue;
      const replicas = task.replica_ids
        .map((rid) => swarm.tasks[rid])
        .filter(Boolean);
      const allDone = replicas.every(
        (r) =>
          r!.status === "completed" ||
          r!.status === "failed" ||
          r!.status === "timed_out",
      );
      if (!allDone) continue;

      // Pick winner: "first" = first completed, "best" = TODO (needs output scoring)
      const completed = replicas.filter((r) => r!.status === "completed");
      if (completed.length > 0) {
        task.status = "completed";
        task.winner_replica_id = completed[0]!.id;
        task.completed_at = new Date().toISOString();
        console.log(
          `[swarm] 🏆 Replica parent ${task.id} completed — winner: ${task.winner_replica_id} (${completed.length}/${replicas.length} succeeded)`,
        );
      } else {
        task.status = "failed";
        task.error = `all ${replicas.length} replicas failed`;
        task.completed_at = new Date().toISOString();
      }
      changed = true;
    }

    // Dispatch ready tasks
    const dispatched = await this.dispatchReadyTasks(swarm);
    if (dispatched > 0) changed = true;

    // Check if swarm is complete
    const statuses = Object.values(swarm.tasks).map((t) => t.status);
    const allTerminal = statuses.every(
      (s) =>
        s === "completed" ||
        s === "failed" ||
        s === "blocked" ||
        s === "timed_out",
    );

    if (allTerminal) {
      const allCompleted = statuses.every((s) => s === "completed");
      swarm.status = allCompleted ? "completed" : "failed";
      swarm.completed_at = new Date().toISOString();
      if (!allCompleted) {
        const failedTasks = Object.values(swarm.tasks)
          .filter((t) => t.status === "failed" || t.status === "timed_out")
          .map((t) => t.id);
        swarm.error = `tasks_failed: ${failedTasks.join(", ")}`;
      }
      changed = true;
      console.log(
        `[swarm] 🏁 Swarm ${swarm.id} ${swarm.status} (${statuses.filter((s) => s === "completed").length}/${statuses.length} succeeded)`,
      );
    }

    if (changed) this.persist(swarm);
  }

  private async dispatchReadyTasks(swarm: SwarmState): Promise<number> {
    const runningCount = Object.values(swarm.tasks).filter(
      (t) => t.status === "running",
    ).length;
    const available = swarm.max_concurrency - runningCount;
    if (available <= 0) return 0;

    // Also check global running jobs across all swarms
    const globalRunning = this.countGlobalRunning();
    const globalSlots = DEFAULT_MAX_CONCURRENCY * 2 - globalRunning; // allow 2x default across all swarms
    const slots = Math.min(available, Math.max(0, globalSlots));
    if (slots <= 0) return 0;

    const readyTasks = Object.values(swarm.tasks)
      .filter((t) => t.status === "ready")
      .slice(0, slots);

    let dispatched = 0;
    for (const task of readyTasks) {
      // Retrieve the dispatch spec
      const specs = (swarm as SwarmStateInternal)._specs;
      const spec = specs?.get(task.id);
      if (!spec) {
        // Recover from restart: re-read from disk isn't possible since we
        // don't persist dispatch specs. Mark as failed.
        task.status = "failed";
        task.error = "dispatch_spec_lost (coordinator restarted?)";
        task.completed_at = new Date().toISOString();
        continue;
      }

      try {
        // Mark as running BEFORE dispatch to prevent double-dispatch
        task.status = "running";
        task.started_at = new Date().toISOString();
        this.persist(swarm);

        const job = await this.dispatcher.dispatch(spec.dispatch);
        task.job_id = job.id;
        dispatched++;
        console.log(
          `[swarm] 🚀 Dispatched task ${task.id} → job ${job.id} (swarm=${swarm.id})`,
        );
      } catch (e) {
        task.status = "failed";
        task.error = `dispatch_error: ${e instanceof Error ? e.message : String(e)}`;
        task.completed_at = new Date().toISOString();
        console.error(
          `[swarm] Failed to dispatch task ${task.id}: ${task.error}`,
        );
      }
    }

    if (dispatched > 0) this.persist(swarm);
    return dispatched;
  }

  private countGlobalRunning(): number {
    let count = 0;
    for (const swarm of this.swarms.values()) {
      if (swarm.status !== "running") continue;
      for (const task of Object.values(swarm.tasks)) {
        if (task.status === "running") count++;
      }
    }
    return count;
  }

  // --- Persistence ---

  private persist(swarm: SwarmState): void {
    // Don't persist internal fields
    const { _specs, ...clean } = swarm as SwarmStateInternal;
    writeFileSync(
      join(SWARMS_DIR, `${swarm.id}.json`),
      JSON.stringify(clean, null, 2),
    );
  }

  private loadSwarm(id: string): SwarmState | null {
    try {
      const data = readFileSync(join(SWARMS_DIR, `${id}.json`), "utf-8");
      const swarm = JSON.parse(data) as SwarmState;
      if (swarm.status === "running" || swarm.status === "pending") {
        this.swarms.set(id, swarm);
      }
      return swarm;
    } catch {
      return null;
    }
  }

  private loadActiveSwarms(): void {
    try {
      const files = readdirSync(SWARMS_DIR).filter((f) => f.endsWith(".json"));
      for (const f of files) {
        try {
          const data = readFileSync(join(SWARMS_DIR, f), "utf-8");
          const swarm = JSON.parse(data) as SwarmState;
          if (swarm.status === "running" || swarm.status === "pending") {
            this.swarms.set(swarm.id, swarm);
            console.log(`[swarm] Recovered active swarm ${swarm.id}`);
          }
        } catch {
          /* skip corrupt files */
        }
      }
    } catch {
      /* directory might not exist yet */
    }
  }
}

// Internal type for attaching dispatch specs to swarm state in-memory
interface SwarmStateInternal extends SwarmState {
  _specs?: Map<string, import("./types.ts").SwarmTaskSpec>;
}
