from __future__ import annotations

from collections import Counter
from pathlib import Path
import ast

IGNORED_DIRS = {
    ".git",
    ".hg",
    ".svn",
    ".idea",
    ".vscode",
    "node_modules",
    "dist",
    "build",
    "coverage",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "__pycache__",
    ".next",
    ".turbo",
    ".venv",
    "venv",
}

LANGUAGE_BY_SUFFIX = {
    ".py": "python",
    ".ts": "typescript",
    ".tsx": "typescript",
    ".js": "javascript",
    ".jsx": "javascript",
    ".mjs": "javascript",
    ".cjs": "javascript",
    ".json": "json",
    ".yaml": "yaml",
    ".yml": "yaml",
    ".sh": "shell",
    ".bash": "shell",
    ".zsh": "shell",
    ".md": "markdown",
    ".go": "go",
    ".rs": "rust",
    ".toml": "toml",
    ".css": "css",
    ".scss": "scss",
    ".html": "html",
}

ENTRYPOINT_BASENAMES = {
    "main.py",
    "main.ts",
    "main.js",
    "index.ts",
    "index.js",
    "server.ts",
    "server.js",
    "app.py",
    "app.ts",
    "cli.py",
}


def build_repo_census(repo_root: str) -> dict:
    root = Path(repo_root).resolve()
    files: list[dict] = []
    tests: list[str] = []
    entrypoints: list[str] = []
    modules: dict[str, dict] = {}
    symbols: list[dict] = []
    language_counts: Counter[str] = Counter()

    if not root.exists() or not root.is_dir():
        return {
            "repo_root": str(root),
            "total_files": 0,
            "languages": {},
            "modules": [],
            "tests": [],
            "entrypoints": [],
            "symbols": [],
            "files": [],
        }

    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(root).as_posix()
        if _is_ignored(rel):
            continue

        suffix = path.suffix.lower()
        language = LANGUAGE_BY_SUFFIX.get(suffix, "other")
        language_counts[language] += 1

        module = rel.split("/", 1)[0] if "/" in rel else "."
        info = {
            "path": rel,
            "language": language,
            "module": module,
            "size_bytes": path.stat().st_size,
            "is_test": _is_test_file(rel),
            "is_entrypoint": _is_entrypoint(rel),
        }
        files.append(info)

        mod = modules.setdefault(
            module,
            {
                "module": module,
                "file_count": 0,
                "languages": Counter(),
                "tests": 0,
                "entrypoints": 0,
                "files": [],
            },
        )
        mod["file_count"] += 1
        mod["languages"][language] += 1
        mod["files"].append(rel)

        if info["is_test"]:
            tests.append(rel)
            mod["tests"] += 1
        if info["is_entrypoint"]:
            entrypoints.append(rel)
            mod["entrypoints"] += 1

        file_symbols = _extract_symbols(path, rel, language)
        symbols.extend(file_symbols)

    normalized_modules = []
    for module, info in sorted(modules.items()):
        normalized_modules.append(
            {
                "module": module,
                "file_count": info["file_count"],
                "languages": dict(info["languages"]),
                "tests": info["tests"],
                "entrypoints": info["entrypoints"],
                "files": sorted(info["files"]),
            }
        )

    return {
        "repo_root": str(root),
        "total_files": len(files),
        "languages": dict(language_counts),
        "modules": normalized_modules,
        "tests": sorted(tests),
        "entrypoints": sorted(entrypoints),
        "symbols": symbols,
        "files": files,
    }


def _is_ignored(rel: str) -> bool:
    parts = rel.split("/")
    return any(part in IGNORED_DIRS for part in parts)


def _is_test_file(rel: str) -> bool:
    name = rel.rsplit("/", 1)[-1].lower()
    parts = [p.lower() for p in rel.split("/")]
    return (
        "tests" in parts
        or name.startswith("test_")
        or name.endswith("_test.py")
        or name.endswith(".test.ts")
        or name.endswith(".test.js")
        or name.endswith(".spec.ts")
        or name.endswith(".spec.js")
    )


def _is_entrypoint(rel: str) -> bool:
    name = rel.rsplit("/", 1)[-1].lower()
    return name in ENTRYPOINT_BASENAMES or rel.startswith("scripts/")


def _extract_symbols(path: Path, rel: str, language: str) -> list[dict]:
    if language != "python":
        return []
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except Exception:
        return []

    symbols = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            symbols.append(
                {
                    "path": rel,
                    "symbol": node.name,
                    "kind": "class" if isinstance(node, ast.ClassDef) else "function",
                    "line": getattr(node, "lineno", 0),
                }
            )
    return sorted(symbols, key=lambda item: (item["path"], item["line"], item["symbol"]))
