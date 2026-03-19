"""Automatic error classification — Loop 21

Classifies errors by recoverability for intelligent retry decisions.
"""

from __future__ import annotations

import hashlib
import logging
import re
import sqlite3
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

log = logging.getLogger("aifleet.errors")


class ErrorCategory(Enum):
    """Categories of errors by recoverability."""

    TRANSIENT = "transient"  # Can retry immediately
    RETRYABLE = "retryable"  # Can retry with backoff
    AUTH = "auth"  # Authentication issue
    CONFIG = "config"  # Configuration error
    RESOURCE = "resource"  # Resource exhaustion
    PERMANENT = "permanent"  # Won't succeed on retry
    UNKNOWN = "unknown"  # Unknown error type


class ErrorSeverity(Enum):
    """Error severity levels."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class ClassifiedError:
    """A classified error with metadata."""

    raw_error: str
    category: ErrorCategory
    severity: ErrorSeverity
    recoverable: bool
    retry_strategy: Optional[str]
    suggested_action: str
    confidence: float


class ErrorClassifier:
    """Classifies errors for intelligent handling."""

    # Error patterns for classification
    PATTERNS: Dict[ErrorCategory, List[str]] = {
        ErrorCategory.TRANSIENT: [
            r"timeout",
            r"connection reset",
            r"temporarily unavailable",
            r"try again",
            r"eagain",
            r"resource temporarily",
            r"network is unreachable",
        ],
        ErrorCategory.RETRYABLE: [
            r"rate.?limit",
            r"429",
            r"too many requests",
            r"quota exceeded",
            r"throttl",
            r"overloaded",
            r"capacity",
            r"server error",
            r"5\d{2}",
            r"503",
            r"502",
        ],
        ErrorCategory.AUTH: [
            r"authentication",
            r"unauthorized",
            r"401",
            r"403",
            r"forbidden",
            r"invalid.*token",
            r"credentials",
            r"api key",
            r"not authenticated",
        ],
        ErrorCategory.CONFIG: [
            r"configuration",
            r"config",
            r"missing.*variable",
            r"env",
            r"not found.*config",
            r"invalid.*setting",
            r"unknown.*option",
        ],
        ErrorCategory.RESOURCE: [
            r"out of memory",
            r"oom",
            r"disk full",
            r"no space",
            r"resource exhausted",
            r"quota",
            r"limit exceeded",
        ],
        ErrorCategory.PERMANENT: [
            r"not found",
            r"404",
            r"invalid.*request",
            r"bad request",
            r"400",
            r"syntax error",
            r"compilation failed",
            r"test failed",
            r"assertion",
        ],
    }

    # Severity mapping
    SEVERITY_MAP: Dict[ErrorCategory, ErrorSeverity] = {
        ErrorCategory.TRANSIENT: ErrorSeverity.LOW,
        ErrorCategory.RETRYABLE: ErrorSeverity.MEDIUM,
        ErrorCategory.AUTH: ErrorSeverity.HIGH,
        ErrorCategory.CONFIG: ErrorSeverity.HIGH,
        ErrorCategory.RESOURCE: ErrorSeverity.CRITICAL,
        ErrorCategory.PERMANENT: ErrorSeverity.HIGH,
        ErrorCategory.UNKNOWN: ErrorSeverity.MEDIUM,
    }

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self._init_table()

    def _init_table(self) -> None:
        """Initialize error classification table."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS error_classifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                error_hash TEXT UNIQUE NOT NULL,
                raw_error TEXT NOT NULL,
                category TEXT NOT NULL,
                severity TEXT NOT NULL,
                recoverable INTEGER NOT NULL,
                retry_strategy TEXT,
                confidence REAL NOT NULL,
                first_seen INTEGER NOT NULL,
                last_seen INTEGER NOT NULL,
                occurrence_count INTEGER DEFAULT 1
            )
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_error_hash
            ON error_classifications(error_hash)
        """)
        self.conn.commit()

    def classify(self, error: str) -> ClassifiedError:
        """Classify an error string."""
        error = error.encode("utf-8", errors="replace").decode("utf-8")
        error_lower = error.lower()

        # Check each category
        best_category = ErrorCategory.UNKNOWN
        best_confidence = 0.0
        matched_patterns = []

        for category, patterns in self.PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, error_lower):
                    # Calculate confidence based on pattern specificity
                    confidence = min(0.95, 0.5 + len(pattern) * 0.02)
                    if confidence > best_confidence:
                        best_confidence = confidence
                        best_category = category
                        matched_patterns.append(pattern)

        # Determine recoverability and retry strategy
        recoverable = best_category in (
            ErrorCategory.TRANSIENT,
            ErrorCategory.RETRYABLE,
        )

        retry_strategy = self._get_retry_strategy(best_category)
        suggested_action = self._get_suggested_action(best_category, error)

        result = ClassifiedError(
            raw_error=error,
            category=best_category,
            severity=self.SEVERITY_MAP[best_category],
            recoverable=recoverable,
            retry_strategy=retry_strategy,
            suggested_action=suggested_action,
            confidence=best_confidence,
        )

        # Store classification
        self._store_classification(result)

        return result

    def _get_retry_strategy(self, category: ErrorCategory) -> Optional[str]:
        """Get retry strategy for error category."""
        strategies = {
            ErrorCategory.TRANSIENT: "immediate_retry",
            ErrorCategory.RETRYABLE: "exponential_backoff",
            ErrorCategory.AUTH: "refresh_auth",
            ErrorCategory.CONFIG: "fix_config",
            ErrorCategory.RESOURCE: "scale_resources",
            ErrorCategory.PERMANENT: None,
            ErrorCategory.UNKNOWN: "exponential_backoff",
        }
        return strategies.get(category)

    def _get_suggested_action(self, category: ErrorCategory, error: str) -> str:
        """Get suggested action for error category."""
        actions = {
            ErrorCategory.TRANSIENT: "Retry immediately",
            ErrorCategory.RETRYABLE: "Retry with exponential backoff",
            ErrorCategory.AUTH: "Refresh authentication credentials",
            ErrorCategory.CONFIG: "Check and fix configuration",
            ErrorCategory.RESOURCE: "Scale up resources or wait",
            ErrorCategory.PERMANENT: "Fix underlying issue before retry",
            ErrorCategory.UNKNOWN: "Investigate error and retry cautiously",
        }
        return actions.get(category, "Investigate error")

    def _store_classification(self, classified: ClassifiedError) -> None:
        """Store error classification in database."""
        error_hash = hashlib.sha256(
            classified.raw_error.encode("utf-8", errors="replace")
        ).hexdigest()[:32]
        now = int(time.time())

        # Check if exists
        row = self.conn.execute(
            "SELECT id, occurrence_count FROM error_classifications WHERE error_hash = ?",
            (error_hash,),
        ).fetchone()

        if row:
            self.conn.execute(
                """
                UPDATE error_classifications
                SET last_seen = ?, occurrence_count = ?
                WHERE id = ?
            """,
                (now, row[1] + 1, row[0]),
            )
        else:
            self.conn.execute(
                """
                INSERT INTO error_classifications
                (error_hash, raw_error, category, severity, recoverable,
                 retry_strategy, confidence, first_seen, last_seen)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    error_hash,
                    classified.raw_error[:500],
                    classified.category.value,
                    classified.severity.value,
                    int(classified.recoverable),
                    classified.retry_strategy,
                    classified.confidence,
                    now,
                    now,
                ),
            )

        self.conn.commit()

    def should_retry(self, error: str, attempt: int, max_attempts: int = 3) -> bool:
        """Determine if error should be retried."""
        classified = self.classify(error)

        if not classified.recoverable:
            return False

        if attempt >= max_attempts:
            return False

        # Check error history
        error_hash = hashlib.sha256(
            error.encode("utf-8", errors="replace")
        ).hexdigest()[:32]
        row = self.conn.execute(
            "SELECT occurrence_count FROM error_classifications WHERE error_hash = ?",
            (error_hash,),
        ).fetchone()

        if row and row[0] > 10:
            # Too many occurrences, may be permanent
            log.warning("Error occurred %d times, may need manual intervention", row[0])

        return True

    def get_error_stats(self) -> Dict[str, Any]:
        """Get error classification statistics."""
        rows = self.conn.execute("""
            SELECT category, COUNT(*) as count,
                   AVG(confidence) as avg_confidence
            FROM error_classifications
            GROUP BY category
        """).fetchall()

        return {
            "by_category": {
                r[0]: {"count": r[1], "avg_confidence": r[2]} for r in rows
            },
            "total_classified": sum(r[1] for r in rows),
        }


def classify_error(conn: sqlite3.Connection, error: str) -> ClassifiedError:
    """Classify an error."""
    classifier = ErrorClassifier(conn)
    return classifier.classify(error)


def should_retry_error(
    conn: sqlite3.Connection, error: str, attempt: int, max_attempts: int = 3
) -> bool:
    """Check if error should be retried."""
    classifier = ErrorClassifier(conn)
    return classifier.should_retry(error, attempt, max_attempts)
