import { chmodSync, mkdirSync, readFileSync, writeFileSync } from "fs";
import { join, resolve } from "path";
import { parse as parseYaml } from "yaml";

interface ToolSpec {
  id: string;
  repo?: string;
  ref?: string;
  package?: string;
  version?: string;
  install: {
    type: "git" | "npm-global";
    target_dir?: string;
    bin?: string;
  };
}

interface ToolingLock {
  vendor_root: string;
  tools: ToolSpec[];
}

function repoRoot(): string {
  return resolve(import.meta.dir, "../../../..");
}

function expandHome(path: string): string {
  return path.replace(/^~(?=\/)/, process.env.HOME ?? "~");
}

function run(cmd: string[], dryRun: boolean) {
  console.log(`[vendor] ${cmd.join(" ")}`);
  if (dryRun) return;
  const proc = Bun.spawnSync(cmd, { stdout: "inherit", stderr: "inherit" });
  if (proc.exitCode !== 0) {
    throw new Error(`Command failed: ${cmd.join(" ")}`);
  }
}

function writeWrapper(binRoot: string, tool: ToolSpec, target?: string, dryRun = false) {
  const wrapperPath = join(binRoot, `vendor-${tool.id}`);
  const scriptLines = [
    "#!/usr/bin/env bash",
    "set -euo pipefail",
  ];

  if (target) {
    scriptLines.push(`cd \"${target}\"`);
    scriptLines.push('if [ "$#" -eq 0 ]; then');
    scriptLines.push('  pwd');
    scriptLines.push("  exit 0");
    scriptLines.push("fi");
    scriptLines.push('exec "$@"');
  } else if (tool.install.bin) {
    scriptLines.push(`exec \"${tool.install.bin}\" \"$@\"`);
  } else {
    scriptLines.push('echo "No executable configured for this vendor tool" >&2');
    scriptLines.push("exit 1");
  }

  console.log(`[vendor] wrapper ${wrapperPath}`);
  if (dryRun) return;
  writeFileSync(wrapperPath, `${scriptLines.join("\n")}\n`);
  chmodSync(wrapperPath, 0o755);
}

function syncGitTool(tool: ToolSpec, vendorRoot: string, binRoot: string, dryRun: boolean) {
  if (!tool.repo || !tool.install.target_dir) return;
  const target = join(vendorRoot, tool.install.target_dir);
  const ref = tool.ref ?? "main";
  run(
    [
      "bash",
      "-lc",
      [
        `[ -d \"${target}/.git\" ]`,
        `&& git -C \"${target}\" fetch --depth 1 origin \"${ref}\"`,
        `&& git -C \"${target}\" reset --hard FETCH_HEAD`,
        `|| git clone --depth 1 --branch \"${ref}\" https://github.com/${tool.repo}.git \"${target}\"`,
      ].join(" "),
    ],
    dryRun,
  );
  writeWrapper(binRoot, tool, target, dryRun);
}

function syncNpmTool(tool: ToolSpec, binRoot: string, dryRun: boolean) {
  if (!tool.package) return;
  run(["npm", "install", "-g", `${tool.package}@${tool.version ?? "latest"}`], dryRun);
  writeWrapper(binRoot, tool, undefined, dryRun);
}

function main() {
  const dryRun = process.argv.includes("--dry-run");
  const root = repoRoot();
  const lock = parseYaml(readFileSync(join(root, "config", "tooling.lock.yaml"), "utf8")) as ToolingLock;
  const vendorRoot = expandHome(lock.vendor_root);
  const binRoot = expandHome("~/.local/bin");
  mkdirSync(vendorRoot, { recursive: true });
  mkdirSync(binRoot, { recursive: true });

  for (const tool of lock.tools) {
    if (tool.install.type === "git") {
      syncGitTool(tool, vendorRoot, binRoot, dryRun);
      continue;
    }
    if (tool.install.type === "npm-global") {
      syncNpmTool(tool, binRoot, dryRun);
    }
  }
}

main();
