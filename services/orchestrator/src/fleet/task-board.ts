/**
 * Shared Task Board — File-based task claiming with lock semantics.
 * Prevents duplicate work when multiple fleet agents run in parallel.
 *
 * Tasks are stored as JSON in ~/.claudemax/fleet-tasks/{board-id}/
 * Each task file has a lock field that agents atomically claim.
 */
import { join } from "path";
import { homedir } from "os";
import {
  mkdirSync,
  writeFileSync,
  readFileSync,
  readdirSync,
  existsSync,
} from "fs";

const BOARDS_DIR = join(homedir(), ".claudemax", "fleet-tasks");

export interface BoardTask {
  id: string;
  description: string;
  status: "open" | "claimed" | "completed" | "failed";
  claimed_by: string | null;
  claimed_at: string | null;
  completed_at: string | null;
  result: string | null;
  error: string | null;
}

export interface TaskBoard {
  id: string;
  created_at: string;
  tasks: BoardTask[];
}

export class TaskBoardManager {
  constructor() {
    mkdirSync(BOARDS_DIR, { recursive: true });
  }

  /**
   * Create a new task board with a set of tasks.
   */
  createBoard(tasks: Array<{ id: string; description: string }>): TaskBoard {
    const id = crypto.randomUUID().replace(/-/g, "").slice(0, 12);
    const board: TaskBoard = {
      id,
      created_at: new Date().toISOString(),
      tasks: tasks.map((t) => ({
        id: t.id,
        description: t.description,
        status: "open",
        claimed_by: null,
        claimed_at: null,
        completed_at: null,
        result: null,
        error: null,
      })),
    };
    this.persist(board);
    console.log(`[task-board] Created board ${id} with ${tasks.length} tasks`);
    return board;
  }

  /**
   * Claim the next open task for a given agent.
   * Returns null if no tasks are available.
   * Uses atomic read-modify-write to prevent races.
   */
  claimNext(
    boardId: string,
    agentId: string,
  ): BoardTask | "board_not_found" | null {
    const board = this.load(boardId);
    if (!board) return "board_not_found";

    const task = board.tasks.find((t) => t.status === "open");
    if (!task) return null;

    task.status = "claimed";
    task.claimed_by = agentId;
    task.claimed_at = new Date().toISOString();
    this.persist(board);

    console.log(
      `[task-board] Agent ${agentId} claimed task ${task.id} on board ${boardId}`,
    );
    return task;
  }

  /**
   * Mark a claimed task as completed with a result.
   */
  complete(boardId: string, taskId: string, result: string): BoardTask | null {
    const board = this.load(boardId);
    if (!board) return null;

    const task = board.tasks.find((t) => t.id === taskId);
    if (!task || task.status !== "claimed") return null;

    task.status = "completed";
    task.completed_at = new Date().toISOString();
    task.result = result;
    this.persist(board);
    return task;
  }

  /**
   * Mark a claimed task as failed.
   */
  fail(boardId: string, taskId: string, error: string): BoardTask | null {
    const board = this.load(boardId);
    if (!board) return null;

    const task = board.tasks.find((t) => t.id === taskId);
    if (!task || task.status !== "claimed") return null;

    task.status = "failed";
    task.completed_at = new Date().toISOString();
    task.error = error;
    this.persist(board);
    return task;
  }

  /**
   * Get the current state of a board.
   */
  getBoard(boardId: string): TaskBoard | null {
    return this.load(boardId);
  }

  /**
   * List all boards.
   */
  listBoards(): TaskBoard[] {
    try {
      return readdirSync(BOARDS_DIR)
        .filter((f) => f.endsWith(".json"))
        .map((f) => {
          try {
            return JSON.parse(
              readFileSync(join(BOARDS_DIR, f), "utf-8"),
            ) as TaskBoard;
          } catch {
            return null;
          }
        })
        .filter(Boolean) as TaskBoard[];
    } catch {
      return [];
    }
  }

  private persist(board: TaskBoard): void {
    const path = join(BOARDS_DIR, `${board.id}.json`);
    writeFileSync(path, JSON.stringify(board, null, 2));
  }

  private load(boardId: string): TaskBoard | null {
    const path = join(BOARDS_DIR, `${boardId}.json`);
    if (!existsSync(path)) return null;
    try {
      return JSON.parse(readFileSync(path, "utf-8")) as TaskBoard;
    } catch {
      return null;
    }
  }
}
