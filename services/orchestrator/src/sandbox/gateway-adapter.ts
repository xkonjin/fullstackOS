import { SandboxHarnessClient } from "./harness-client.ts";

export interface Agent GatewaySandboxIntent {
  image: string;
  command?: string;
  env?: Record<string, string>;
  priority?: "high" | "normal" | "low";
  policyProfile?: "strict" | "standard" | "agent-gateway_controlled";
  allowedHosts?: string[];
  writableRoots?: string[];
  timeoutSeconds?: number;
  metadata?: Record<string, unknown>;
}

export class Agent GatewaySandboxAdapter {
  constructor(
    private readonly client = new SandboxHarnessClient(),
    private readonly allowDangerousProfiles = process.env
      .GATEWAY_SANDBOX_ALLOW_DANGEROUS === "1",
  ) {}

  async create(intent: Agent GatewaySandboxIntent) {
    if (
      !this.allowDangerousProfiles &&
      intent.policyProfile === "agent-gateway_controlled"
    ) {
      throw new Error(
        "agent-gateway_controlled profile requires GATEWAY_SANDBOX_ALLOW_DANGEROUS=1",
      );
    }

    if (
      intent.writableRoots?.some(
        (root) => root.includes("..") || root.startsWith("/"),
      )
    ) {
      throw new Error(
        "writableRoots cannot include path traversal segments or absolute paths",
      );
    }

    if (intent.allowedHosts?.includes("*")) {
      throw new Error("allowedHosts cannot include wildcard");
    }

    return this.client.createJob({
      image: intent.image,
      command: intent.command,
      env: intent.env,
      priority: intent.priority,
      timeoutSeconds: intent.timeoutSeconds,
      policyProfile: intent.policyProfile ?? "standard",
      policy: {
        allowedHosts: intent.allowedHosts,
        writableRoots: intent.writableRoots,
      },
      metadata: {
        source: "agent-gateway",
        ...(intent.metadata ?? {}),
      },
    });
  }

  async exec(jobId: string, command: string) {
    return this.client.execJob(jobId, command);
  }

  async status(jobId: string) {
    return this.client.getJob(jobId);
  }

  async kill(jobId: string) {
    return this.client.killJob(jobId);
  }
}
