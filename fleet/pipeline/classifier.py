"""Task classifier for role-based pipeline routing.

Classifies objectives into task types with complexity scoring,
domain detection, and research-need assessment.

Primary: deterministic regex/keyword rules (fast, no LLM needed).
Fallback: LLM classification for ambiguous cases (via Haiku/Flash).
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

log = logging.getLogger("aifleet.classifier")


@dataclass
class TaskClassification:
    """Output of the task classifier."""

    task_type: str  # ui, backend, fullstack, api, refactor, bugfix, etc.
    complexity: int  # 1-5
    domains: list[str] = field(default_factory=list)  # react, css, python, sql, etc.
    needs_research: bool = False
    reference_urls: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "task_type": self.task_type,
            "complexity": self.complexity,
            "domains": self.domains,
            "needs_research": self.needs_research,
            "reference_urls": self.reference_urls,
        }


# ---------------------------------------------------------------------------
# Keyword → task_type mapping (order matters — first match wins)
# ---------------------------------------------------------------------------

_TYPE_PATTERNS: list[tuple[str, list[str]]] = [
    # UI/Frontend
    (
        "ui",
        [
            r"\b(dashboard|component|page|layout|form|modal|sidebar|navbar|widget|tooltip)\b",
            r"\b(responsive|mobile[- ]first|dark[- ]mode|theme|animation|transition)\b",
            r"\b(figma|design[- ]system|style[- ]guide|mockup|wireframe)\b",
        ],
    ),
    (
        "frontend",
        [
            r"\b(react|vue|svelte|nextjs|next\.js|remix|astro|tailwind|css|scss|html)\b",
            r"\b(frontend|front[- ]end|client[- ]side|browser|dom|jsx|tsx)\b",
        ],
    ),
    # Backend
    (
        "api",
        [
            r"\b(api|endpoint|rest|graphql|grpc|webhook|route|middleware|controller)\b",
            r"\b(fastapi|express|flask|django|hono|trpc)\b",
        ],
    ),
    (
        "backend",
        [
            r"\b(database|migration|schema|model|orm|query|sql|postgres|redis|supabase)\b",
            r"\b(backend|back[- ]end|server[- ]side|microservice|queue|worker|cron)\b",
            r"\b(python|typescript|node|bun|deno)\b",
        ],
    ),
    # Operations
    (
        "deploy",
        [
            r"\b(deploy|ship|release|ci/?cd|pipeline|docker|kubernetes|railway|vercel)\b",
            r"\b(monitoring|alerting|sentry|health[- ]check|rollback|infrastructure)\b",
        ],
    ),
    (
        "security",
        [
            r"\b(security|auth|authentication|authorization|oauth|jwt|rbac|encryption)\b",
            r"\b(xss|sql[- ]injection|csrf|owasp|penetration|vulnerability|audit)\b",
        ],
    ),
    (
        "migration",
        [
            r"\b(migration|migrate|upgrade|schema[- ]change|data[- ]transfer|backward[- ]compat)\b",
        ],
    ),
    # Code quality
    (
        "refactor",
        [
            r"\b(refactor|restructure|reorganize|consolidate|simplify|deduplicate|clean[- ]up)\b",
            r"\b(optimize|performance|bottleneck|n\+1|memory[- ]leak|latency)\b",
        ],
    ),
    (
        "bugfix",
        [
            r"\b(fix|bug|broken|error|crash|exception|traceback|regression|issue)\b",
            r"\b(debug|diagnose|troubleshoot|investigate|root[- ]cause)\b",
        ],
    ),
    (
        "review",
        [
            r"\b(review|audit|inspect|check|evaluate|assess|critique)\b",
        ],
    ),
    # Content
    (
        "writing",
        [
            r"\b(write|document|readme|changelog|spec|rfc|proposal|copy|blog)\b",
            r"\b(documentation|docs|technical[- ]writing|content)\b",
        ],
    ),
    (
        "research",
        [
            r"\b(research|explore|investigate|compare|evaluate|benchmark|spike)\b",
            r"\b(poc|proof[- ]of[- ]concept|prototype|experiment)\b",
        ],
    ),
]

# Domain detection patterns
_DOMAIN_PATTERNS: dict[str, list[str]] = {
    "react": [r"\breact\b", r"\bjsx\b", r"\btsx\b", r"\bnext\.?js\b", r"\bremix\b"],
    "vue": [r"\bvue\b", r"\bnuxt\b"],
    "css": [r"\bcss\b", r"\bscss\b", r"\btailwind\b", r"\bstyled\b"],
    "animation": [
        r"\banimation\b",
        r"\btransition\b",
        r"\bframer\b",
        r"\bgsap\b",
        r"\blottie\b",
    ],
    "python": [
        r"\bpython\b",
        r"\bfastapi\b",
        r"\bdjango\b",
        r"\bflask\b",
        r"\bpytest\b",
    ],
    "typescript": [r"\btypescript\b", r"\b\.ts\b", r"\bbun\b", r"\bdeno\b"],
    "sql": [
        r"\bsql\b",
        r"\bpostgres\w*\b",
        r"\bsupabase\b",
        r"\bprisma\b",
        r"\bdrizzle\b",
    ],
    "redis": [r"\bredis\b", r"\bcache\b", r"\bupstash\b"],
    "docker": [r"\bdocker\b", r"\bcontainer\b", r"\bkubernetes\b", r"\bk8s\b"],
    "auth": [r"\bauth\b", r"\boauth\b", r"\bjwt\b", r"\bsession\b"],
    "testing": [r"\btest\b", r"\bpytest\b", r"\bjest\b", r"\bvitest\b", r"\btdd\b"],
    "charts": [
        r"\bchart\b",
        r"\bgraph\b",
        r"\bvisualization\b",
        r"\bd3\b",
        r"\brecharts\b",
    ],
}

# Complexity signals
_COMPLEXITY_SIGNALS: list[tuple[str, int]] = [
    # High complexity (+2)
    (r"\b(architecture|system[- ]design|migration|multi[- ]service)\b", 2),
    (r"\b(security[- ]audit|penetration|full[- ]stack)\b", 2),
    (r"\b(distributed|microservice|event[- ]driven|cqrs)\b", 2),
    # Medium complexity (+1)
    (r"\b(refactor|optimize|redesign|overhaul)\b", 1),
    (r"\b(api|middleware|authentication|authorization)\b", 1),
    (r"\b(responsive|accessible|i18n|l10n)\b", 1),
    (r"\btest(s|ing|suite)\b", 1),
    # Low complexity (-1)
    (r"\b(typo|rename|bump|format|lint)\b", -1),
    (r"\b(simple|quick|small|minor|tiny)\b", -1),
]

# URL extraction
_URL_PATTERN = re.compile(r"https?://[^\s\)\"'>]+")

# Precompiled regexes for performance
_COMPILED_TYPE_PATTERNS: list[tuple[str, list[re.Pattern[str]]]] = [
    (task_type, [re.compile(p) for p in patterns])
    for task_type, patterns in _TYPE_PATTERNS
]
_COMPILED_DOMAIN_PATTERNS: dict[str, list[re.Pattern[str]]] = {
    domain: [re.compile(p) for p in patterns]
    for domain, patterns in _DOMAIN_PATTERNS.items()
}
_COMPILED_COMPLEXITY_SIGNALS: list[tuple[re.Pattern[str], int]] = [
    (re.compile(pattern), delta) for pattern, delta in _COMPLEXITY_SIGNALS
]
_COMPILED_RESEARCH_SIGNALS: list[re.Pattern[str]] = [
    re.compile(r"\b(like|similar to|inspired by|reference|example)\b"),
    re.compile(r"\b(best practice|pattern|approach|compare)\b"),
    re.compile(r"\b(how to|what's the best|recommend)\b"),
]
_FILE_REF_PATTERN = re.compile(r"[\w/]+\.\w{1,4}")


def classify_task(objective: str) -> TaskClassification:
    """Classify a task objective into type, complexity, domains, and research needs.

    Uses deterministic pattern matching — no LLM call needed for most inputs.
    """
    if not objective:
        return TaskClassification(task_type="backend", complexity=2)

    obj_lower = objective.lower()

    # --- Task type ---
    task_type = _detect_task_type(obj_lower)

    # --- Domains ---
    domains = _detect_domains(obj_lower)

    # --- Complexity ---
    complexity = _score_complexity(obj_lower, objective, domains)

    # --- Research needed ---
    needs_research = _needs_research(task_type, complexity, obj_lower)

    # --- Reference URLs ---
    reference_urls = _URL_PATTERN.findall(objective)

    classification = TaskClassification(
        task_type=task_type,
        complexity=complexity,
        domains=domains,
        needs_research=needs_research,
        reference_urls=reference_urls,
    )

    log.info(
        "Classified: type=%s complexity=%d domains=%s research=%s",
        task_type,
        complexity,
        domains,
        needs_research,
    )
    return classification


def _detect_task_type(obj_lower: str) -> str:
    """Match objective against type patterns. Highest score wins.

    Action verbs (refactor, fix, deploy, review, write, research) get a 3x boost
    because they indicate explicit user intent regardless of nouns present.
    """
    # Action-verb types get a priority boost when they match
    _ACTION_VERB_TYPES = {
        "refactor",
        "bugfix",
        "deploy",
        "review",
        "writing",
        "research",
    }

    type_scores: dict[str, int] = {}

    for task_type, patterns in _COMPILED_TYPE_PATTERNS:
        score = 0
        for pattern in patterns:
            matches = pattern.findall(obj_lower)
            score += len(matches)
        if score > 0:
            # Boost action verbs — "refactor X" means refactor, not whatever X is
            if task_type in _ACTION_VERB_TYPES:
                score *= 3
            type_scores[task_type] = score

    if not type_scores:
        return "backend"  # safe default

    # If both frontend and backend score high → fullstack
    front_score = type_scores.get("ui", 0) + type_scores.get("frontend", 0)
    back_score = type_scores.get("backend", 0) + type_scores.get("api", 0)
    if front_score >= 2 and back_score >= 2:
        return "fullstack"

    # Return highest-scoring type
    return max(type_scores, key=type_scores.get)


def _detect_domains(obj_lower: str) -> list[str]:
    """Detect technology domains mentioned in objective."""
    domains = []
    for domain, patterns in _COMPILED_DOMAIN_PATTERNS.items():
        for pattern in patterns:
            if pattern.search(obj_lower):
                domains.append(domain)
                break
    return domains


def _score_complexity(obj_lower: str, objective: str, domains: list[str]) -> int:
    """Score task complexity 1-5."""
    score = 2  # baseline

    # Signal-based adjustments
    for pattern, delta in _COMPILED_COMPLEXITY_SIGNALS:
        if pattern.search(obj_lower):
            score += delta

    # Length-based adjustment
    if len(objective) > 2000:
        score += 1
    elif len(objective) > 500:
        score += 0  # no change
    elif len(objective) < 100:
        score -= 1

    # Domain count adjustment (more domains = more complex)
    if len(domains) >= 4:
        score += 1
    elif len(domains) >= 2:
        score += 0  # no change

    # File reference count
    file_refs = len(_FILE_REF_PATTERN.findall(objective))
    if file_refs >= 5:
        score += 1

    return max(1, min(5, score))


def _needs_research(task_type: str, complexity: int, obj_lower: str) -> bool:
    """Determine if task needs a research phase."""
    # Always research for these types
    if task_type in ("ui", "frontend", "design", "research", "writing", "fullstack"):
        return True

    # Research for complex tasks
    if complexity >= 4:
        return True

    # Explicit research signals
    for pattern in _COMPILED_RESEARCH_SIGNALS:
        if pattern.search(obj_lower):
            return True

    return False
