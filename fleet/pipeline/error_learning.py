"""Error pattern learning — Loop 24

Learns from past errors to prevent repeats and suggest fixes.
"""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Any

log = logging.getLogger("aifleet.learning")


@dataclass
class ErrorPattern:
    """A learned error pattern."""

    pattern_hash: str
    normalized_error: str
    context: str
    frequency: int
    first_seen: int
    last_seen: int
    fixes_attempted: List[Dict[str, Any]]
    successful_fix: Optional[str]
    prevention_suggestion: str


class ErrorPatternLearner:
    """Learns from errors to prevent repeats."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self._init_tables()
        self._pattern_cache: Dict[str, ErrorPattern] = {}

    def _init_tables(self) -> None:
        """Initialize learning tables."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS error_patterns (
                pattern_hash TEXT PRIMARY KEY,
                normalized_error TEXT NOT NULL,
                context TEXT,
                frequency INTEGER DEFAULT 1,
                first_seen INTEGER NOT NULL,
                last_seen INTEGER NOT NULL,
                fixes_attempted_json TEXT,
                successful_fix TEXT,
                prevention_suggestion TEXT
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS error_instances (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pattern_hash TEXT NOT NULL,
                pipeline_id TEXT,
                stage_name TEXT,
                raw_error TEXT,
                timestamp INTEGER NOT NULL,
                FOREIGN KEY (pattern_hash) REFERENCES error_patterns(pattern_hash)
            )
        """)

        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_error_pattern_hash 
            ON error_instances(pattern_hash)
        """)
        self.conn.commit()

    def _normalize_error(self, error: str) -> str:
        """Normalize error for pattern matching."""
        # Remove variable parts (file paths, line numbers, etc.)
        normalized = error.lower()

        # Replace file paths
        import re

        normalized = re.sub(r"/[\w/]+/", "<PATH>/", normalized)

        # Replace line numbers
        normalized = re.sub(r":\d+:", ":<LINE>:", normalized)

        # Replace specific values
        normalized = re.sub(r"'[^']+'", "'<VALUE>'", normalized)
        normalized = re.sub(r'"[^"]+"', '"<VALUE>"', normalized)

        # Remove timestamps and UUIDs
        normalized = re.sub(
            r"\d{4}-\d{2}-\d{2}[\sT]\d{2}:\d{2}:\d{2}", "<TIMESTAMP>", normalized
        )
        normalized = re.sub(
            r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
            "<UUID>",
            normalized,
        )

        return normalized[:500]

    def learn_from_error(
        self, error: str, pipeline_id: str, stage_name: str, context: str = ""
    ) -> ErrorPattern:
        """Learn from an error occurrence."""
        normalized = self._normalize_error(error)
        pattern_hash = hashlib.sha256(normalized.encode()).hexdigest()[:32]
        now = int(time.time())

        # Check if pattern exists
        existing = self.conn.execute(
            "SELECT * FROM error_patterns WHERE pattern_hash = ?", (pattern_hash,)
        ).fetchone()

        if existing:
            # Update existing pattern
            self.conn.execute(
                """
                UPDATE error_patterns
                SET frequency = frequency + 1, last_seen = ?
                WHERE pattern_hash = ?
            """,
                (now, pattern_hash),
            )
        else:
            # Create new pattern
            prevention = self._generate_prevention_suggestion(normalized, stage_name)
            self.conn.execute(
                """
                INSERT INTO error_patterns
                (pattern_hash, normalized_error, context, frequency, 
                 first_seen, last_seen, prevention_suggestion)
                VALUES (?, ?, ?, 1, ?, ?, ?)
            """,
                (pattern_hash, normalized, context, now, now, prevention),
            )

        # Record instance
        self.conn.execute(
            """
            INSERT INTO error_instances
            (pattern_hash, pipeline_id, stage_name, raw_error, timestamp)
            VALUES (?, ?, ?, ?, ?)
        """,
            (pattern_hash, pipeline_id, stage_name, error[:1000], now),
        )

        self.conn.commit()

        return self._get_pattern(pattern_hash)

    def _generate_prevention_suggestion(
        self, normalized_error: str, stage_name: str
    ) -> str:
        """Generate prevention suggestion based on error."""
        suggestions = {
            "timeout": "Increase timeout or optimize operation",
            "rate limit": "Implement exponential backoff",
            "memory": "Optimize memory usage or increase resources",
            "connection": "Check network connectivity and retry",
            "permission": "Verify file permissions and access rights",
            "not found": "Ensure all dependencies are installed",
            "syntax": "Validate code before execution",
            "import": "Check Python environment and dependencies",
        }

        for key, suggestion in suggestions.items():
            if key in normalized_error.lower():
                return suggestion

        return f"Review {stage_name} stage implementation"

    def _get_pattern(self, pattern_hash: str) -> ErrorPattern:
        """Get pattern by hash."""
        row = self.conn.execute(
            "SELECT * FROM error_patterns WHERE pattern_hash = ?", (pattern_hash,)
        ).fetchone()

        if not row:
            raise ValueError(f"Pattern not found: {pattern_hash}")

        fixes = json.loads(row[6]) if row[6] else []

        return ErrorPattern(
            pattern_hash=row[0],
            normalized_error=row[1],
            context=row[2] or "",
            frequency=row[3],
            first_seen=row[4],
            last_seen=row[5],
            fixes_attempted=fixes,
            successful_fix=row[7],
            prevention_suggestion=row[8] or "",
        )

    def check_for_known_pattern(self, error: str) -> Optional[ErrorPattern]:
        """Check if error matches a known pattern."""
        normalized = self._normalize_error(error)
        pattern_hash = hashlib.sha256(normalized.encode()).hexdigest()[:32]

        try:
            return self._get_pattern(pattern_hash)
        except ValueError:
            return None

    def record_fix_attempt(
        self, pattern_hash: str, fix_description: str, success: bool
    ) -> None:
        """Record a fix attempt for a pattern."""
        pattern = self._get_pattern(pattern_hash)

        fix_record = {
            "description": fix_description,
            "success": success,
            "timestamp": int(time.time()),
        }

        pattern.fixes_attempted.append(fix_record)

        if success and not pattern.successful_fix:
            self.conn.execute(
                """
                UPDATE error_patterns
                SET successful_fix = ?, fixes_attempted_json = ?
                WHERE pattern_hash = ?
            """,
                (fix_description, json.dumps(pattern.fixes_attempted), pattern_hash),
            )
        else:
            self.conn.execute(
                """
                UPDATE error_patterns
                SET fixes_attempted_json = ?
                WHERE pattern_hash = ?
            """,
                (json.dumps(pattern.fixes_attempted), pattern_hash),
            )

        self.conn.commit()

    def get_suggested_fix(self, error: str) -> Optional[str]:
        """Get suggested fix for an error."""
        pattern = self.check_for_known_pattern(error)
        if pattern and pattern.successful_fix:
            return pattern.successful_fix

        # Check for similar patterns
        similar = self._find_similar_patterns(error)
        for p in similar:
            if p.successful_fix:
                return p.successful_fix

        return None

    def _find_similar_patterns(self, error: str, limit: int = 5) -> List[ErrorPattern]:
        """Find similar error patterns."""
        # Simple similarity: shared keywords
        error_words = set(error.lower().split())

        rows = self.conn.execute(
            "SELECT pattern_hash, normalized_error FROM error_patterns"
        ).fetchall()

        scored_patterns = []
        for pattern_hash, normalized in rows:
            pattern_words = set(normalized.split())
            shared = len(error_words & pattern_words)
            score = shared / max(len(error_words), len(pattern_words))
            if score > 0.5:
                scored_patterns.append((score, pattern_hash))

        scored_patterns.sort(reverse=True)

        return [self._get_pattern(h) for _, h in scored_patterns[:limit]]

    def get_learning_stats(self) -> Dict[str, Any]:
        """Get learning statistics."""
        total_patterns = self.conn.execute(
            "SELECT COUNT(*) FROM error_patterns"
        ).fetchone()[0]

        total_instances = self.conn.execute(
            "SELECT COUNT(*) FROM error_instances"
        ).fetchone()[0]

        solved_patterns = self.conn.execute(
            "SELECT COUNT(*) FROM error_patterns WHERE successful_fix IS NOT NULL"
        ).fetchone()[0]

        top_patterns = self.conn.execute("""
            SELECT pattern_hash, frequency, prevention_suggestion
            FROM error_patterns
            ORDER BY frequency DESC
            LIMIT 10
        """).fetchall()

        return {
            "total_patterns": total_patterns,
            "total_instances": total_instances,
            "solved_patterns": solved_patterns,
            "solve_rate": solved_patterns / total_patterns if total_patterns > 0 else 0,
            "top_patterns": [
                {"hash": r[0][:8], "frequency": r[1], "suggestion": r[2]}
                for r in top_patterns
            ],
        }

    def get_prevention_checklist(self, stage_name: str) -> List[str]:
        """Get prevention checklist for a stage."""
        rows = self.conn.execute(
            """
            SELECT prevention_suggestion, COUNT(*) as count
            FROM error_patterns
            WHERE context = ? OR context = ''
            GROUP BY prevention_suggestion
            ORDER BY count DESC
            LIMIT 5
        """,
            (stage_name,),
        ).fetchall()

        return [r[0] for r in rows]


def get_learner(conn: sqlite3.Connection) -> ErrorPatternLearner:
    """Get pattern learner instance."""
    return ErrorPatternLearner(conn)


def learn_error(
    conn: sqlite3.Connection, error: str, pipeline_id: str, stage_name: str
) -> ErrorPattern:
    """Learn from an error."""
    learner = get_learner(conn)
    return learner.learn_from_error(error, pipeline_id, stage_name, stage_name)
