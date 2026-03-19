import {
  existsSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  writeFileSync,
} from "fs";
import { join, resolve } from "path";
import { parse as parseYaml } from "yaml";

interface SkillMetadata {
  id: string;
  [key: string]: unknown;
}

interface McpServerConfig {
  type: "url" | "command";
  url?: string;
  command?: string;
  args?: string[];
  env?: Record<string, string>;
  headers?: Record<string, string>;
  bearer_token_env_var?: string;
  http_headers?: Record<string, string>;
  startup_timeout_sec?: number;
  tool_timeout_sec?: number;
  profiles: string[];
}

interface McpRegistry {
  servers: Record<string, McpServerConfig>;
  profiles: Record<string, { servers: string[] }>;
}

interface ResourcePackIndex {
  id: string;
  [key: string]: unknown;
}

function repoRoot(): string {
  return resolve(import.meta.dir, "../../../..");
}

function ensureDir(path: string) {
  mkdirSync(path, { recursive: true });
}

function formatTomlInlineTable(values?: Record<string, string>): string | null {
  if (!values || Object.keys(values).length === 0) return null;
  const entries = Object.entries(values).map(
    ([key, value]) => `"${key}" = "${value}"`,
  );
  return `{ ${entries.join(", ")} }`;
}

function main() {
  const root = repoRoot();
  const generatedDir = join(root, "generated");
  ensureDir(generatedDir);

  const registry = parseYaml(
    readFileSync(join(root, "config", "mcp-registry.yaml"), "utf8"),
  ) as McpRegistry;

  const skillDirs = readdirSync(join(root, "skills"), { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => entry.name);
  const skillIndex = skillDirs
    .map((dir) => {
      const metaPath = join(root, "skills", dir, "skill.json");
      if (!existsSync(metaPath)) return null;
      const meta = JSON.parse(readFileSync(metaPath, "utf8")) as SkillMetadata;
      return {
        ...meta,
        path: join(root, "skills", dir),
      };
    })
    .filter(Boolean);

  const resourcePackRoot = join(root, "docs", "resource-packs");
  const resourcePackIndex = readdirSync(resourcePackRoot, {
    withFileTypes: true,
  })
    .filter((entry) => entry.isDirectory())
    .map((entry) => join(resourcePackRoot, entry.name, "index.json"))
    .filter((indexPath) => existsSync(indexPath))
    .map(
      (indexPath) =>
        JSON.parse(readFileSync(indexPath, "utf8")) as ResourcePackIndex,
    );

  const claudeMcp = {
    mcpServers: Object.fromEntries(
      Object.entries(registry.servers).map(([name, server]) => {
        if (server.type === "url") {
          const config: Record<string, unknown> = {
            type: "http",
            url: server.url,
          };
          if (server.headers) config.headers = server.headers;
          return [name, config];
        }
        const config: Record<string, unknown> = {
          command: server.command,
          args: server.args ?? [],
        };
        if (server.env) config.env = server.env;
        return [name, config];
      }),
    ),
  };

  const codexTomlLines = [
    "# Generated from config/mcp-registry.yaml",
    "[features]",
    "rmcp_client = true",
    "multi_agent = true",
    "",
  ];
  for (const [name, server] of Object.entries(registry.servers)) {
    codexTomlLines.push(`[mcp_servers.${name}]`);
    if (server.type === "url") {
      codexTomlLines.push(`url = \"${server.url}\"`);
      if (server.bearer_token_env_var) {
        codexTomlLines.push(
          `bearer_token_env_var = \"${server.bearer_token_env_var}\"`,
        );
      }
      const headers = formatTomlInlineTable(
        server.http_headers ?? server.headers,
      );
      if (headers) codexTomlLines.push(`http_headers = ${headers}`);
      if (typeof server.startup_timeout_sec === "number") {
        codexTomlLines.push(
          `startup_timeout_sec = ${server.startup_timeout_sec}`,
        );
      }
      if (typeof server.tool_timeout_sec === "number") {
        codexTomlLines.push(`tool_timeout_sec = ${server.tool_timeout_sec}`);
      }
    } else {
      codexTomlLines.push(`command = \"${server.command}\"`);
      codexTomlLines.push(
        `args = [${(server.args ?? []).map((arg) => `\"${arg}\"`).join(", ")}]`,
      );
    }
    const env = formatTomlInlineTable(server.env);
    if (env) codexTomlLines.push(`env = ${env}`);
    codexTomlLines.push("");
  }

  writeFileSync(
    join(generatedDir, "skill-index.json"),
    `${JSON.stringify(skillIndex, null, 2)}\n`,
  );
  writeFileSync(
    join(generatedDir, "mcp-profiles.json"),
    `${JSON.stringify(registry, null, 2)}\n`,
  );
  writeFileSync(
    join(generatedDir, "resource-pack-index.json"),
    `${JSON.stringify(resourcePackIndex, null, 2)}\n`,
  );
  writeFileSync(
    join(generatedDir, "claude.mcp.json"),
    `${JSON.stringify(claudeMcp, null, 2)}\n`,
  );
  writeFileSync(
    join(generatedDir, "codex.mcp.toml"),
    `${codexTomlLines.join("\n")}\n`,
  );

  // Generate per-profile MCP configs for dynamic agent dispatch
  for (const [profileName, profile] of Object.entries(registry.profiles)) {
    const profileServers = profile.servers;

    // Claude JSON format
    const profileClaudeMcp = {
      mcpServers: Object.fromEntries(
        profileServers
          .filter((name) => name in registry.servers)
          .map((name) => {
            const server = registry.servers[name];
            if (server.type === "url") {
              const config: Record<string, unknown> = {
                type: "http",
                url: server.url,
              };
              if (server.headers) config.headers = server.headers;
              return [name, config];
            }
            const config: Record<string, unknown> = {
              command: server.command,
              args: server.args ?? [],
            };
            if (server.env) config.env = server.env;
            return [name, config];
          }),
      ),
    };
    writeFileSync(
      join(generatedDir, `mcp-${profileName}.json`),
      `${JSON.stringify(profileClaudeMcp, null, 2)}\n`,
    );

    // Codex TOML format
    const profileTomlLines = [
      `# Generated profile: ${profileName}`,
      "[features]",
      "rmcp_client = true",
      "",
    ];
    for (const srvName of profileServers) {
      const server = registry.servers[srvName];
      if (!server) continue;
      profileTomlLines.push(`[mcp_servers.${srvName}]`);
      if (server.type === "url") {
        profileTomlLines.push(`url = \"${server.url}\"`);
        if (server.bearer_token_env_var) {
          profileTomlLines.push(
            `bearer_token_env_var = \"${server.bearer_token_env_var}\"`,
          );
        }
        const headers = formatTomlInlineTable(
          server.http_headers ?? server.headers,
        );
        if (headers) profileTomlLines.push(`http_headers = ${headers}`);
      } else {
        profileTomlLines.push(`command = \"${server.command}\"`);
        profileTomlLines.push(
          `args = [${(server.args ?? []).map((arg) => `\"${arg}\"`).join(", ")}]`,
        );
      }
      const env = formatTomlInlineTable(server.env);
      if (env) profileTomlLines.push(`env = ${env}`);
      profileTomlLines.push("");
    }
    writeFileSync(
      join(generatedDir, `mcp-${profileName}.toml`),
      `${profileTomlLines.join("\n")}\n`,
    );
  }

  console.log(`[stack] Generated files in ${generatedDir}`);
}

main();
