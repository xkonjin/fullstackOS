from __future__ import annotations

import re
from collections import defaultdict


def build_coverage_map(
    objective: str,
    census: dict,
    topology: dict,
) -> dict:
    """Analyze census + topology against an objective to identify coverage gaps.

    Returns a map of modules/files ranked by relevance to the objective,
    with centrality scores from the topology graph.
    """
    modules = census.get("modules", [])
    topo_modules = {m["module"]: m for m in topology.get("modules", [])}
    module_graph = topology.get("module_graph", [])
    symbols = census.get("symbols", [])
    files = census.get("files", [])

    # Extract keywords from the objective
    keywords = _extract_keywords(objective)

    # Score each module by keyword relevance + centrality
    scored_modules: list[dict] = []
    for mod in modules:
        name = mod.get("module", ".")
        topo = topo_modules.get(name, {})

        keyword_score = _keyword_match_score(name, mod.get("files", []), keywords)
        centrality = topo.get("imports_in", 0) + topo.get("imports_out", 0)
        is_frontier = topo.get("frontier", False)
        has_tests = mod.get("tests", 0) > 0
        has_entrypoints = mod.get("entrypoints", 0) > 0

        relevance = (
            keyword_score * 10
            + centrality * 2
            + (5 if is_frontier else 0)
            + (3 if has_entrypoints else 0)
        )

        scored_modules.append({
            "module": name,
            "relevance_score": relevance,
            "keyword_hits": keyword_score,
            "centrality": centrality,
            "frontier": is_frontier,
            "file_count": mod.get("file_count", 0),
            "test_count": mod.get("tests", 0),
            "has_tests": has_tests,
            "languages": mod.get("languages", {}),
        })

    scored_modules.sort(key=lambda m: m["relevance_score"], reverse=True)

    # Identify files matching objective keywords
    relevant_files = _score_files(files, symbols, keywords)

    # Identify coverage gaps: high-relevance modules without tests
    gaps = [
        m for m in scored_modules
        if m["relevance_score"] > 0 and not m["has_tests"]
    ]

    # Identify high-complexity areas: frontier modules with high centrality
    complex_areas = [
        m for m in scored_modules
        if m["frontier"] and m["centrality"] >= 4
    ]

    # Build dependency chains for relevant modules
    dep_chains = _build_dependency_chains(
        [m["module"] for m in scored_modules if m["relevance_score"] > 0],
        module_graph,
    )

    return {
        "objective": objective,
        "keywords": keywords,
        "modules": scored_modules,
        "relevant_files": relevant_files[:100],  # cap output size
        "coverage_gaps": gaps,
        "complex_areas": complex_areas,
        "dependency_chains": dep_chains,
        "summary": {
            "total_modules": len(modules),
            "relevant_modules": sum(1 for m in scored_modules if m["relevance_score"] > 0),
            "modules_with_tests": sum(1 for m in scored_modules if m["has_tests"]),
            "coverage_gap_count": len(gaps),
            "complex_area_count": len(complex_areas),
        },
    }


def _extract_keywords(objective: str) -> list[str]:
    """Extract meaningful keywords from an objective string."""
    stop_words = {
        "the", "a", "an", "is", "are", "was", "were", "be", "been", "being",
        "have", "has", "had", "do", "does", "did", "will", "would", "could",
        "should", "may", "might", "shall", "can", "need", "must", "ought",
        "and", "or", "but", "if", "then", "else", "when", "while", "for",
        "to", "from", "with", "in", "on", "at", "by", "of", "about", "into",
        "through", "during", "before", "after", "above", "below", "between",
        "this", "that", "these", "those", "it", "its", "all", "each", "every",
        "both", "few", "more", "most", "other", "some", "such", "no", "not",
        "only", "same", "so", "than", "too", "very", "just", "also", "now",
        "add", "create", "make", "build", "implement", "fix", "update",
        "change", "modify", "remove", "delete", "new", "use", "using",
    }
    words = re.findall(r"[a-zA-Z_][a-zA-Z0-9_]*", objective.lower())
    return sorted(set(w for w in words if w not in stop_words and len(w) > 2))


def _keyword_match_score(module_name: str, files: list[str], keywords: list[str]) -> int:
    """Score how well a module matches objective keywords."""
    score = 0
    name_lower = module_name.lower()
    files_text = " ".join(f.lower() for f in files)

    for kw in keywords:
        if kw in name_lower:
            score += 3  # strong match on module name
        elif kw in files_text:
            score += 1  # weaker match on file paths
    return score


def _score_files(
    files: list[dict],
    symbols: list[dict],
    keywords: list[str],
) -> list[dict]:
    """Score individual files by keyword relevance."""
    symbol_map: dict[str, list[str]] = defaultdict(list)
    for sym in symbols:
        symbol_map[sym.get("path", "")].append(sym.get("symbol", "").lower())

    scored: list[dict] = []
    for f in files:
        path = f.get("path", "")
        path_lower = path.lower()
        syms = symbol_map.get(path, [])

        score = 0
        for kw in keywords:
            if kw in path_lower:
                score += 2
            if any(kw in s for s in syms):
                score += 3

        if score > 0:
            scored.append({
                "path": path,
                "score": score,
                "language": f.get("language", ""),
                "module": f.get("module", "."),
                "is_test": f.get("is_test", False),
            })

    scored.sort(key=lambda x: x["score"], reverse=True)
    return scored


def _build_dependency_chains(
    relevant_modules: list[str],
    module_graph: list[dict],
) -> dict[str, list[str]]:
    """For each relevant module, find its direct dependencies."""
    chains: dict[str, list[str]] = {}
    for mod in relevant_modules:
        deps = sorted(set(
            e["target"] for e in module_graph
            if e.get("source") == mod
        ))
        if deps:
            chains[mod] = deps
    return chains
