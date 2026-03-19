from __future__ import annotations

from collections import Counter, defaultdict
from pathlib import Path
import ast


def build_repo_topology(repo_root: str, census: dict) -> dict:
    root = Path(repo_root).resolve()
    files = census.get("files", []) if isinstance(census, dict) else []
    module_names = {m.get("module") for m in census.get("modules", []) if isinstance(m, dict)}

    imports_by_file: dict[str, list[str]] = {}
    module_edges: Counter[tuple[str, str]] = Counter()
    file_edges: list[dict] = []
    frontier_modules: set[str] = set()

    for info in files:
        if not isinstance(info, dict):
            continue
        rel = str(info.get("path", "")).strip()
        if not rel:
            continue
        language = str(info.get("language", ""))
        module = str(info.get("module", "."))
        imports = _extract_imports(root / rel, language)
        imports_by_file[rel] = imports

        for target in imports:
            target_module = _normalize_target_module(target, module_names)
            if not target_module:
                continue
            file_edges.append({"source": rel, "target_module": target_module})
            if target_module != module:
                module_edges[(module, target_module)] += 1
                frontier_modules.add(module)
                frontier_modules.add(target_module)

    inbound = defaultdict(int)
    outbound = defaultdict(int)
    for (source, target), weight in module_edges.items():
        outbound[source] += weight
        inbound[target] += weight

    modules = []
    for entry in census.get("modules", []):
        if not isinstance(entry, dict):
            continue
        module = str(entry.get("module", "."))
        modules.append(
            {
                "module": module,
                "file_count": int(entry.get("file_count", 0)),
                "imports_out": outbound.get(module, 0),
                "imports_in": inbound.get(module, 0),
                "frontier": module in frontier_modules,
            }
        )

    return {
        "repo_root": str(root),
        "module_graph": [
            {"source": source, "target": target, "weight": weight}
            for (source, target), weight in sorted(module_edges.items())
        ],
        "file_imports": imports_by_file,
        "modules": modules,
        "frontiers": sorted(frontier_modules),
    }


def _extract_imports(path: Path, language: str) -> list[str]:
    if language == "python":
        return _extract_python_imports(path)
    if language == "typescript" or language == "javascript":
        return _extract_js_imports(path)
    return []


def _extract_python_imports(path: Path) -> list[str]:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except Exception:
        return []

    imports: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imports.append(node.module)
    return sorted(set(imports))


def _extract_js_imports(path: Path) -> list[str]:
    try:
        text = path.read_text(encoding="utf-8")
    except Exception:
        return []

    imports: set[str] = set()
    for line in text.splitlines():
        stripped = line.strip()
        if " from " in stripped and (stripped.startswith("import ") or stripped.startswith("export ")):
            target = stripped.split(" from ", 1)[1].strip().strip(";'")
            target = target.strip('"')
            imports.add(target)
        elif stripped.startswith("import("):
            target = stripped.split("import(", 1)[1].split(")", 1)[0].strip().strip("'\"")
            if target:
                imports.add(target)
        elif stripped.startswith("require(") or " require(" in stripped:
            segment = stripped.split("require(", 1)[1].split(")", 1)[0].strip().strip("'\"")
            if segment:
                imports.add(segment)
    return sorted(imports)


def _normalize_target_module(target: str, module_names: set[str]) -> str | None:
    cleaned = target.strip()
    if not cleaned:
        return None
    if cleaned.startswith("./") or cleaned.startswith("../"):
        return None
    if cleaned.startswith("@/"):
        cleaned = cleaned[2:]
    cleaned = cleaned.lstrip("/")

    candidates = [cleaned]
    if "." in cleaned and not cleaned.startswith("@"):
        candidates.extend(part for part in cleaned.split(".") if part)

    for candidate in candidates:
        top = candidate.split("/", 1)[0]
        if top in module_names:
            return top
    return None
