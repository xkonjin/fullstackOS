from __future__ import annotations

import logging

log = logging.getLogger(__name__)


def build_autoresearch_queries(
    objective: str,
    coverage_map: dict,
) -> dict:
    """Generate bounded research queries for high-complexity or ambiguous areas.

    Takes coverage map output and produces a set of research questions
    that should be answered before fractal planning can proceed.
    Zero LLM cost — this is pure heuristic query generation.
    """
    complex_areas = coverage_map.get("complex_areas", [])
    gaps = coverage_map.get("coverage_gaps", [])
    dep_chains = coverage_map.get("dependency_chains", {})
    relevant_files = coverage_map.get("relevant_files", [])

    queries: list[dict] = []

    # Query 1: Architecture questions for complex frontier modules
    for area in complex_areas[:5]:  # cap at 5 to bound cost
        module = area.get("module", "")
        centrality = area.get("centrality", 0)
        queries.append({
            "query_id": f"arch-{module}",
            "category": "architecture",
            "question": (
                f"Module '{module}' has centrality {centrality} and is a frontier module. "
                f"What is its public API surface, key abstractions, and how do other modules depend on it? "
                f"What are the risks of modifying it?"
            ),
            "target_module": module,
            "priority": min(centrality, 10),
            "estimated_tokens": 2000,
        })

    # Query 2: Gap analysis for untested relevant modules
    for gap in gaps[:5]:
        module = gap.get("module", "")
        queries.append({
            "query_id": f"gap-{module}",
            "category": "test_gap",
            "question": (
                f"Module '{module}' is relevant to the objective but has no tests. "
                f"What are its critical code paths and what test strategy would cover them?"
            ),
            "target_module": module,
            "priority": gap.get("relevance_score", 0),
            "estimated_tokens": 1500,
        })

    # Query 3: Dependency risk for modules with many deps
    for module, deps in sorted(dep_chains.items(), key=lambda x: len(x[1]), reverse=True)[:3]:
        if len(deps) >= 3:
            queries.append({
                "query_id": f"dep-{module}",
                "category": "dependency_risk",
                "question": (
                    f"Module '{module}' depends on {len(deps)} other modules: {', '.join(deps[:8])}. "
                    f"What are the interface contracts between them? "
                    f"Which dependencies are stable vs. likely to change?"
                ),
                "target_module": module,
                "priority": len(deps),
                "estimated_tokens": 2000,
            })

    # Query 4: Implementation approach for top relevant files
    top_files = [f for f in relevant_files if not f.get("is_test") and f.get("score", 0) >= 4]
    if top_files:
        file_list = ", ".join(f["path"] for f in top_files[:10])
        queries.append({
            "query_id": "impl-approach",
            "category": "implementation",
            "question": (
                f"Given objective: '{objective}', the most relevant files are: {file_list}. "
                f"What is the recommended implementation approach? "
                f"Which files should be modified first, and what are the key constraints?"
            ),
            "target_module": None,
            "priority": 8,
            "estimated_tokens": 3000,
        })

    # Sort by priority descending
    queries.sort(key=lambda q: q.get("priority", 0), reverse=True)

    total_estimated_tokens = sum(q.get("estimated_tokens", 0) for q in queries)

    return {
        "objective": objective,
        "queries": queries,
        "summary": {
            "total_queries": len(queries),
            "categories": _count_categories(queries),
            "estimated_total_tokens": total_estimated_tokens,
            "estimated_cost_usd": total_estimated_tokens * 0.000003,  # ~$3/MTok input
        },
    }


def synthesize_research_results(
    autoresearch: dict,
    results: list[dict],
) -> dict:
    """Merge research query results into a synthesis for downstream planning.

    Each result dict should have:
      - query_id: str matching an autoresearch query
      - answer: str (the LLM response)
      - ok: bool
    """
    queries = {q["query_id"]: q for q in autoresearch.get("queries", [])}
    findings: list[dict] = []
    failed: list[str] = []

    for r in results:
        qid = r.get("query_id", "")
        query = queries.get(qid, {})
        if r.get("ok"):
            findings.append({
                "query_id": qid,
                "category": query.get("category", "unknown"),
                "target_module": query.get("target_module"),
                "question": query.get("question", ""),
                "answer": r.get("answer", ""),
            })
        else:
            failed.append(qid)

    return {
        "findings": findings,
        "failed_queries": failed,
        "summary": {
            "answered": len(findings),
            "failed": len(failed),
            "total": len(queries),
        },
    }


def _count_categories(queries: list[dict]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for q in queries:
        cat = q.get("category", "unknown")
        counts[cat] = counts.get(cat, 0) + 1
    return counts
