export class QueueFullError extends Error {
  constructor(active: number, pending: number, limit: number) {
    super(`Queue full: ${active} active, ${pending} pending (limit ${limit})`);
    this.name = "QueueFullError";
  }
}

export class QueueTimeoutError extends Error {
  constructor(waitedMs: number) {
    super(`Queue timeout after ${waitedMs}ms`);
    this.name = "QueueTimeoutError";
  }
}

export class Semaphore {
  private _active = 0;
  private _queue: { resolve: () => void; reject: (e: Error) => void }[] = [];

  constructor(
    private readonly maxConcurrency: number,
    private readonly maxPending: number = Infinity,
  ) {}

  get active(): number {
    return this._active;
  }

  get pending(): number {
    return this._queue.length;
  }

  async acquire(timeoutMs?: number): Promise<void> {
    if (this._active < this.maxConcurrency) {
      this._active++;
      return;
    }

    if (this._queue.length >= this.maxPending) {
      throw new QueueFullError(
        this._active,
        this._queue.length,
        this.maxPending,
      );
    }

    return new Promise<void>((resolve, reject) => {
      let settled = false;
      let timer: ReturnType<typeof setTimeout> | undefined;

      const entry = {
        resolve: () => {
          if (settled) return;
          settled = true;
          if (timer) clearTimeout(timer);
          this._active++;
          resolve();
        },
        reject: (e: Error) => {
          if (settled) return;
          settled = true;
          if (timer) clearTimeout(timer);
          reject(e);
        },
      };
      this._queue.push(entry);

      if (timeoutMs && timeoutMs > 0) {
        timer = setTimeout(() => {
          if (settled) return;
          const idx = this._queue.indexOf(entry);
          if (idx !== -1) {
            this._queue.splice(idx, 1);
            settled = true;
            reject(new QueueTimeoutError(timeoutMs));
          }
        }, timeoutMs);
      }
    });
  }

  release(): void {
    if (this._active <= 0) {
      console.error("[semaphore] release() called without matching acquire()");
      return;
    }
    this._active--;
    const next = this._queue.shift();
    if (next) next.resolve();
  }

  /** Drain all pending waiters with an error (used during shutdown/reset) */
  drainPending(reason: string): void {
    const err = new Error(reason);
    while (this._queue.length > 0) {
      const entry = this._queue.shift()!;
      entry.reject(err);
    }
  }
}
