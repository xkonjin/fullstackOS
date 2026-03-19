export interface SandboxHarnessJobRequest {
  image: string;
  entrypoint?: string[];
  command?: string;
  env?: Record<string, string>;
  timeoutSeconds?: number;
  metadata?: Record<string, unknown>;
  priority?: "high" | "normal" | "low";
  policyProfile?: "strict" | "standard" | "agent-gateway_controlled";
  policy?: {
    maxRuntimeSeconds?: number;
    maxMemoryMb?: number;
    maxCpuCores?: number;
    allowNetwork?: boolean;
    allowedHosts?: string[];
    writableRoots?: string[];
  };
}

export interface SandboxHarnessJob {
  id: string;
  sandboxId: string;
  status: "running" | "completed" | "failed" | "killed";
  createdAt: string;
  updatedAt: string;
}

export class SandboxHarnessClient {
  constructor(
    private readonly baseUrl = process.env.SANDBOX_HARNESS_URL ??
      "http://127.0.0.1:18450",
    private readonly token = process.env.SANDBOX_HARNESS_TOKEN,
  ) {}

  private headers(): Record<string, string> {
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
    };
    if (this.token) headers.Authorization = `Bearer ${this.token}`;
    return headers;
  }

  async createJob(req: SandboxHarnessJobRequest): Promise<SandboxHarnessJob> {
    const response = await fetch(`${this.baseUrl}/v1/jobs`, {
      method: "POST",
      headers: this.headers(),
      body: JSON.stringify(req),
      signal: AbortSignal.timeout(20_000),
    });
    if (!response.ok) {
      const message = await response.text();
      throw new Error(
        `sandbox harness create failed: ${response.status} ${message}`,
      );
    }
    return (await response.json()) as SandboxHarnessJob;
  }

  async getJob(jobId: string): Promise<SandboxHarnessJob> {
    const response = await fetch(`${this.baseUrl}/v1/jobs/${jobId}`, {
      method: "GET",
      headers: this.headers(),
      signal: AbortSignal.timeout(10_000),
    });
    if (!response.ok) {
      const message = await response.text();
      throw new Error(
        `sandbox harness get failed: ${response.status} ${message}`,
      );
    }
    return (await response.json()) as SandboxHarnessJob;
  }

  async execJob(jobId: string, command: string): Promise<SandboxHarnessJob> {
    const response = await fetch(`${this.baseUrl}/v1/jobs/${jobId}/exec`, {
      method: "POST",
      headers: this.headers(),
      body: JSON.stringify({ command }),
      signal: AbortSignal.timeout(30_000),
    });
    if (!response.ok) {
      const message = await response.text();
      throw new Error(
        `sandbox harness exec failed: ${response.status} ${message}`,
      );
    }
    return (await response.json()) as SandboxHarnessJob;
  }

  async killJob(jobId: string): Promise<SandboxHarnessJob> {
    const response = await fetch(`${this.baseUrl}/v1/jobs/${jobId}`, {
      method: "DELETE",
      headers: this.headers(),
      signal: AbortSignal.timeout(10_000),
    });
    if (!response.ok) {
      const message = await response.text();
      throw new Error(
        `sandbox harness kill failed: ${response.status} ${message}`,
      );
    }
    return (await response.json()) as SandboxHarnessJob;
  }
}
