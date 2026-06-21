"""
Mandarin Learning Engine
Version: 01  |  Phase: 01 - Core Data Architecture
"""

from __future__ import annotations

import hashlib
import json
import sqlite3

# patch_manager is a same-repo sibling module — import at module level is intentional.
from patch_manager import PatchManager
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Version constants
# ---------------------------------------------------------------------------

SCHEMA_VERSION = "01.00"
CONTENT_VERSION = "01.00"
VALIDATOR_VERSION = "01.00"
RANK_VERSION = "01.00"
PATCH_VERSION = "01.00"

# ---------------------------------------------------------------------------
# Database paths
# ---------------------------------------------------------------------------

DATA_DIR = Path("data")

GENERATION_CACHE_DB = DATA_DIR / "generation_cache.sqlite"
COMMON_CONTENT_DB = DATA_DIR / "common_content.sqlite"


def learner_db_path(learner_id: str) -> Path:
    return DATA_DIR / f"learner_{learner_id}.sqlite"


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _checksum(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _get_connection(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


# ---------------------------------------------------------------------------
# Schema initialisation
# ---------------------------------------------------------------------------

_GENERATION_CACHE_DDL = """
CREATE TABLE IF NOT EXISTS schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sources (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    url         TEXT,
    source_type TEXT NOT NULL,       -- 'corpus' | 'frequency' | 'dictionary'
    import_date TEXT NOT NULL,
    checksum    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS revisions (
    id            TEXT PRIMARY KEY,
    source_id     TEXT NOT NULL REFERENCES sources(id),
    revision_date TEXT NOT NULL,
    content_hash  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ai_generations (
    id         TEXT PRIMARY KEY,
    prompt     TEXT NOT NULL,
    response   TEXT NOT NULL,
    model      TEXT NOT NULL,
    timestamp  TEXT NOT NULL,
    validated  INTEGER NOT NULL DEFAULT 0   -- 0=no, 1=yes
);

CREATE TABLE IF NOT EXISTS validation_history (
    id                TEXT PRIMARY KEY,
    generation_id     TEXT NOT NULL REFERENCES ai_generations(id),
    validator_version TEXT NOT NULL,
    result            TEXT NOT NULL,   -- 'pass' | 'fail' | 'pending'
    notes             TEXT,
    timestamp         TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS derived_artifacts (
    id            TEXT PRIMARY KEY,
    source_id     TEXT NOT NULL REFERENCES sources(id),
    artifact_type TEXT NOT NULL,
    content       TEXT NOT NULL,
    created_at    TEXT NOT NULL
);
"""

_COMMON_CONTENT_DDL = """
CREATE TABLE IF NOT EXISTS schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS lexemes (
    id             TEXT PRIMARY KEY,
    simplified     TEXT NOT NULL,
    traditional    TEXT,
    pinyin         TEXT NOT NULL,
    english        TEXT NOT NULL,
    frequency_rank INTEGER,
    hsk_level      INTEGER,
    source         TEXT NOT NULL,
    created_at     TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_lexemes_simplified ON lexemes(simplified);

CREATE TABLE IF NOT EXISTS rankings (
    id           TEXT PRIMARY KEY,
    lexeme_id    TEXT NOT NULL REFERENCES lexemes(id),
    rank_type    TEXT NOT NULL,   -- 'frequency' | 'hsk' | 'composite'
    rank_value   REAL NOT NULL,
    rank_version TEXT NOT NULL,
    created_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sentence_candidates (
    id                TEXT PRIMARY KEY,
    content           TEXT NOT NULL,
    pinyin            TEXT,
    english           TEXT,
    grammar_tags      TEXT,   -- JSON list
    naturalness_score REAL,
    ambiguity_score   REAL,
    created_at        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS approved_content (
    id             TEXT PRIMARY KEY,
    sentence_id    TEXT NOT NULL REFERENCES sentence_candidates(id),
    approved_by    TEXT NOT NULL,
    approved_at    TEXT NOT NULL,
    content_version TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS analysis (
    id                TEXT PRIMARY KEY,
    sentence_id       TEXT NOT NULL REFERENCES sentence_candidates(id),
    analysis_type     TEXT NOT NULL,
    result            TEXT NOT NULL,   -- JSON
    validator_version TEXT NOT NULL,
    created_at        TEXT NOT NULL
);
"""

_LEARNER_DDL = """
CREATE TABLE IF NOT EXISTS schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS knowledge_estimates (
    id               TEXT PRIMARY KEY,
    lexeme_id        TEXT NOT NULL UNIQUE,
    probability_known REAL NOT NULL DEFAULT 0.0,
    exposure_count   INTEGER NOT NULL DEFAULT 0,
    last_updated     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS review_schedule (
    id          TEXT PRIMARY KEY,
    lexeme_id   TEXT NOT NULL UNIQUE,
    next_review TEXT NOT NULL,
    interval    REAL NOT NULL DEFAULT 1.0,   -- days
    ease_factor REAL NOT NULL DEFAULT 2.5
);

CREATE TABLE IF NOT EXISTS review_history (
    id            TEXT PRIMARY KEY,
    lexeme_id     TEXT NOT NULL,
    review_date   TEXT NOT NULL,
    result        TEXT NOT NULL,   -- 'pass' | 'fail'
    response_time REAL             -- seconds
);

CREATE TABLE IF NOT EXISTS progress (
    id            TEXT PRIMARY KEY,
    date          TEXT NOT NULL,
    new_words     INTEGER NOT NULL DEFAULT 0,
    reviewed_words INTEGER NOT NULL DEFAULT 0,
    ci_score      REAL
);
"""


def _write_meta(conn: sqlite3.Connection) -> None:
    meta = {
        "schema_version":    SCHEMA_VERSION,
        "content_version":   CONTENT_VERSION,
        "validator_version": VALIDATOR_VERSION,
        "rank_version":      RANK_VERSION,
        "patch_version":     PATCH_VERSION,
    }
    conn.executemany(
        "INSERT OR IGNORE INTO schema_meta(key, value) VALUES (?, ?)",
        meta.items(),
    )
    conn.commit()


def init_generation_cache(db_path: Path = GENERATION_CACHE_DB) -> sqlite3.Connection:
    conn = _get_connection(db_path)
    conn.executescript(_GENERATION_CACHE_DDL)
    _write_meta(conn)
    return conn


def init_common_content(db_path: Path = COMMON_CONTENT_DB) -> sqlite3.Connection:
    conn = _get_connection(db_path)
    conn.executescript(_COMMON_CONTENT_DDL)
    _write_meta(conn)
    return conn


def init_learner_db(learner_id: str) -> sqlite3.Connection:
    conn = _get_connection(learner_db_path(learner_id))
    conn.executescript(_LEARNER_DDL)
    _write_meta(conn)
    return conn


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class Source:
    name: str
    source_type: str          # 'corpus' | 'frequency' | 'dictionary'
    content: str              # raw text used to compute checksum
    url: Optional[str] = None
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    import_date: str = field(default_factory=_now_utc)
    checksum: str = field(init=False)

    def __post_init__(self):
        self.checksum = _checksum(self.content)


@dataclass
class Lexeme:
    simplified: str
    pinyin: str
    english: str
    source: str
    traditional: Optional[str] = None
    frequency_rank: Optional[int] = None
    hsk_level: Optional[int] = None
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: str = field(default_factory=_now_utc)


@dataclass
class SentenceCandidate:
    content: str
    pinyin: Optional[str] = None
    english: Optional[str] = None
    grammar_tags: list[str] = field(default_factory=list)
    naturalness_score: Optional[float] = None
    ambiguity_score: Optional[float] = None
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: str = field(default_factory=_now_utc)


@dataclass
class KnowledgeEstimate:
    lexeme_id: str
    probability_known: float = 0.0
    exposure_count: int = 0
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    last_updated: str = field(default_factory=_now_utc)


# ---------------------------------------------------------------------------
# CorpusCache
# ---------------------------------------------------------------------------

class CorpusCache:
    """
    Manages source tracking, revision tracking, derived artifacts,
    and AI generation history in generation_cache.sqlite.
    """

    def __init__(self, db_path: Path = GENERATION_CACHE_DB):
        self.conn = init_generation_cache(db_path)

    # --- Sources ---

    def add_source(self, source: Source) -> str:
        with self.conn:
            self.conn.execute(
                """
                INSERT OR IGNORE INTO sources
                    (id, name, url, source_type, import_date, checksum)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (source.id, source.name, source.url, source.source_type,
                 source.import_date, source.checksum),
            )
        return source.id

    def get_source(self, source_id: str) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM sources WHERE id = ?", (source_id,)
        ).fetchone()

    def list_sources(self) -> list[sqlite3.Row]:
        return self.conn.execute("SELECT * FROM sources ORDER BY import_date").fetchall()

    # --- Revisions ---

    def add_revision(self, source_id: str, content_hash: str) -> str:
        revision_id = str(uuid.uuid4())
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO revisions (id, source_id, revision_date, content_hash)
                VALUES (?, ?, ?, ?)
                """,
                (revision_id, source_id, _now_utc(), content_hash),
            )
        return revision_id

    def get_latest_revision(self, source_id: str) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            """
            SELECT * FROM revisions
            WHERE source_id = ?
            ORDER BY revision_date DESC LIMIT 1
            """,
            (source_id,),
        ).fetchone()

    # --- AI Generations ---

    def record_generation(self, prompt: str, response: str, model: str) -> str:
        gen_id = str(uuid.uuid4())
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO ai_generations (id, prompt, response, model, timestamp, validated)
                VALUES (?, ?, ?, ?, ?, 0)
                """,
                (gen_id, prompt, response, model, _now_utc()),
            )
        return gen_id

    def record_validation(
        self,
        generation_id: str,
        result: str,
        notes: Optional[str] = None,
    ) -> str:
        val_id = str(uuid.uuid4())
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO validation_history
                    (id, generation_id, validator_version, result, notes, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (val_id, generation_id, VALIDATOR_VERSION, result, notes, _now_utc()),
            )
            if result == "pass":
                self.conn.execute(
                    "UPDATE ai_generations SET validated = 1 WHERE id = ?",
                    (generation_id,),
                )
        return val_id

    # --- Derived artifacts ---

    def store_artifact(
        self, source_id: str, artifact_type: str, content: str
    ) -> str:
        artifact_id = str(uuid.uuid4())
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO derived_artifacts
                    (id, source_id, artifact_type, content, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (artifact_id, source_id, artifact_type, content, _now_utc()),
            )
        return artifact_id

    def get_artifacts(self, source_id: str, artifact_type: Optional[str] = None) -> list[sqlite3.Row]:
        if artifact_type:
            return self.conn.execute(
                "SELECT * FROM derived_artifacts WHERE source_id = ? AND artifact_type = ?",
                (source_id, artifact_type),
            ).fetchall()
        return self.conn.execute(
            "SELECT * FROM derived_artifacts WHERE source_id = ?", (source_id,)
        ).fetchall()


# ---------------------------------------------------------------------------
# CorpusIngestor
# ---------------------------------------------------------------------------

class CorpusIngestor:
    """
    Handles corpus imports, frequency imports, and raw sentence extraction.
    Writes raw sources into CorpusCache; does not write to common_content.
    """

    def __init__(self, corpus_cache: CorpusCache):
        self.cache = corpus_cache

    def ingest_raw_text(
        self, name: str, raw_text: str, url: Optional[str] = None
    ) -> str:
        source = Source(name=name, source_type="corpus", content=raw_text, url=url)
        source_id = self.cache.add_source(source)
        self.cache.add_revision(source_id, source.checksum)
        return source_id

    def ingest_frequency_list(
        self,
        name: str,
        entries: list[dict],   # [{"simplified": ..., "rank": ..., ...}]
        url: Optional[str] = None,
    ) -> str:
        raw_text = json.dumps(entries, ensure_ascii=False)
        source = Source(name=name, source_type="frequency", content=raw_text, url=url)
        source_id = self.cache.add_source(source)
        self.cache.add_revision(source_id, source.checksum)
        self.cache.store_artifact(source_id, "frequency_json", raw_text)
        return source_id

    def extract_sentences(self, source_id: str, sentences: list[str]) -> str:
        content = json.dumps(sentences, ensure_ascii=False)
        return self.cache.store_artifact(source_id, "extracted_sentences", content)


# ---------------------------------------------------------------------------
# LexemeRepository
# ---------------------------------------------------------------------------

class LexemeRepository:
    """
    Manages lexeme storage, ranking calculations, and token resolution
    in common_content.sqlite.
    """

    def __init__(self, db_path: Path = COMMON_CONTENT_DB):
        self.conn = init_common_content(db_path)

    # --- Storage ---

    def add_lexeme(self, lexeme: Lexeme) -> str:
        with self.conn:
            self.conn.execute(
                """
                INSERT OR IGNORE INTO lexemes
                    (id, simplified, traditional, pinyin, english,
                     frequency_rank, hsk_level, source, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    lexeme.id, lexeme.simplified, lexeme.traditional,
                    lexeme.pinyin, lexeme.english, lexeme.frequency_rank,
                    lexeme.hsk_level, lexeme.source, lexeme.created_at,
                ),
            )
        return lexeme.id

    def get_lexeme_by_simplified(self, simplified: str) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM lexemes WHERE simplified = ?", (simplified,)
        ).fetchone()

    def get_lexeme(self, lexeme_id: str) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM lexemes WHERE id = ?", (lexeme_id,)
        ).fetchone()

    def list_lexemes(
        self,
        limit: int = 100,
        offset: int = 0,
        order_by: str = "frequency_rank",
    ) -> list[sqlite3.Row]:
        return self.conn.execute(
            f"SELECT * FROM lexemes ORDER BY {order_by} LIMIT ? OFFSET ?",
            (limit, offset),
        ).fetchall()

    # --- Rankings ---

    def set_ranking(
        self, lexeme_id: str, rank_type: str, rank_value: float
    ) -> str:
        ranking_id = str(uuid.uuid4())
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO rankings
                    (id, lexeme_id, rank_type, rank_value, rank_version, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (ranking_id, lexeme_id, rank_type, rank_value, RANK_VERSION, _now_utc()),
            )
        return ranking_id

    def get_rankings(self, lexeme_id: str) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM rankings WHERE lexeme_id = ? ORDER BY created_at DESC",
            (lexeme_id,),
        ).fetchall()

    # --- Token resolution ---

    def resolve_tokens(self, tokens: list[str]) -> dict[str, Optional[sqlite3.Row]]:
        """
        Given a list of tokens (simplified characters), returns a mapping
        of token -> lexeme row (or None if not found).
        """
        return {t: self.get_lexeme_by_simplified(t) for t in tokens}


# ---------------------------------------------------------------------------
# SentenceAnalyzer
# ---------------------------------------------------------------------------

class SentenceAnalyzer:
    """
    Provides grammar analysis, naturalness scoring, ambiguity scoring,
    and inference-clue extraction for sentence candidates.
    Writes candidates and analysis results to common_content.sqlite.
    """

    def __init__(self, db_path: Path = COMMON_CONTENT_DB):
        self.conn = init_common_content(db_path)

    def add_candidate(self, candidate: SentenceCandidate) -> str:
        with self.conn:
            self.conn.execute(
                """
                INSERT OR IGNORE INTO sentence_candidates
                    (id, content, pinyin, english, grammar_tags,
                     naturalness_score, ambiguity_score, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    candidate.id,
                    candidate.content,
                    candidate.pinyin,
                    candidate.english,
                    json.dumps(candidate.grammar_tags, ensure_ascii=False),
                    candidate.naturalness_score,
                    candidate.ambiguity_score,
                    candidate.created_at,
                ),
            )
        return candidate.id

    def record_analysis(
        self,
        sentence_id: str,
        analysis_type: str,
        result: dict,
    ) -> str:
        analysis_id = str(uuid.uuid4())
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO analysis
                    (id, sentence_id, analysis_type, result, validator_version, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    analysis_id,
                    sentence_id,
                    analysis_type,
                    json.dumps(result, ensure_ascii=False),
                    VALIDATOR_VERSION,
                    _now_utc(),
                ),
            )
        return analysis_id

    def update_scores(
        self,
        sentence_id: str,
        naturalness_score: Optional[float] = None,
        ambiguity_score: Optional[float] = None,
    ) -> None:
        if naturalness_score is not None:
            with self.conn:
                self.conn.execute(
                    "UPDATE sentence_candidates SET naturalness_score = ? WHERE id = ?",
                    (naturalness_score, sentence_id),
                )
        if ambiguity_score is not None:
            with self.conn:
                self.conn.execute(
                    "UPDATE sentence_candidates SET ambiguity_score = ? WHERE id = ?",
                    (ambiguity_score, sentence_id),
                )

    def get_candidate(self, sentence_id: str) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM sentence_candidates WHERE id = ?", (sentence_id,)
        ).fetchone()

    def list_candidates(
        self,
        limit: int = 100,
        offset: int = 0,
        min_naturalness: Optional[float] = None,
    ) -> list[sqlite3.Row]:
        if min_naturalness is not None:
            return self.conn.execute(
                """
                SELECT * FROM sentence_candidates
                WHERE naturalness_score >= ?
                ORDER BY naturalness_score DESC LIMIT ? OFFSET ?
                """,
                (min_naturalness, limit, offset),
            ).fetchall()
        return self.conn.execute(
            "SELECT * FROM sentence_candidates ORDER BY created_at LIMIT ? OFFSET ?",
            (limit, offset),
        ).fetchall()

    def approve(self, sentence_id: str, approved_by: str) -> str:
        approval_id = str(uuid.uuid4())
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO approved_content
                    (id, sentence_id, approved_by, approved_at, content_version)
                VALUES (?, ?, ?, ?, ?)
                """,
                (approval_id, sentence_id, approved_by, _now_utc(), CONTENT_VERSION),
            )
        return approval_id


# ---------------------------------------------------------------------------
# LearnerModel
# ---------------------------------------------------------------------------

class LearnerModel:
    """
    Manages knowledge estimates, exposure history, and review scheduling
    for a single learner in learner_<id>.sqlite.
    """

    def __init__(self, learner_id: str):
        self.learner_id = learner_id
        self.conn = init_learner_db(learner_id)

    # --- Knowledge estimates ---

    def record_exposure(self, lexeme_id: str, known: bool) -> None:
        existing = self.conn.execute(
            "SELECT * FROM knowledge_estimates WHERE lexeme_id = ?", (lexeme_id,)
        ).fetchone()

        now = _now_utc()
        if existing is None:
            new_id = str(uuid.uuid4())
            prob = 1.0 if known else 0.1
            with self.conn:
                self.conn.execute(
                    """
                    INSERT INTO knowledge_estimates
                        (id, lexeme_id, probability_known, exposure_count, last_updated)
                    VALUES (?, ?, ?, 1, ?)
                    """,
                    (new_id, lexeme_id, prob, now),
                )
        else:
            # Simple Bayesian-style update: weighted towards recent evidence
            count = existing["exposure_count"] + 1
            current_prob = existing["probability_known"]
            signal = 1.0 if known else 0.0
            new_prob = current_prob + (signal - current_prob) / count
            with self.conn:
                self.conn.execute(
                    """
                    UPDATE knowledge_estimates
                    SET probability_known = ?, exposure_count = ?, last_updated = ?
                    WHERE lexeme_id = ?
                    """,
                    (new_prob, count, now, lexeme_id),
                )

    def get_knowledge(self, lexeme_id: str) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM knowledge_estimates WHERE lexeme_id = ?", (lexeme_id,)
        ).fetchone()

    def probability_known(self, lexeme_id: str) -> float:
        row = self.get_knowledge(lexeme_id)
        return row["probability_known"] if row else 0.0

    # --- Review scheduling (simple SM-2 inspired) ---

    def schedule_review(self, lexeme_id: str, result: str, response_time: Optional[float] = None) -> None:
        from datetime import timedelta

        now_str = _now_utc()
        history_id = str(uuid.uuid4())
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO review_history (id, lexeme_id, review_date, result, response_time)
                VALUES (?, ?, ?, ?, ?)
                """,
                (history_id, lexeme_id, now_str, result, response_time),
            )

        existing = self.conn.execute(
            "SELECT * FROM review_schedule WHERE lexeme_id = ?", (lexeme_id,)
        ).fetchone()

        if existing is None:
            interval = 1.0 if result == "pass" else 0.5
            ease = 2.5
        else:
            ease = existing["ease_factor"]
            interval = existing["interval"]
            if result == "pass":
                interval = interval * ease
                ease = min(ease + 0.1, 3.0)
            else:
                interval = max(0.5, interval * 0.5)
                ease = max(1.3, ease - 0.2)

        from datetime import timedelta
        next_review = (datetime.now(timezone.utc) + timedelta(days=interval)).isoformat()

        sched_id = str(uuid.uuid4())
        with self.conn:
            if existing is None:
                self.conn.execute(
                    """
                    INSERT INTO review_schedule (id, lexeme_id, next_review, interval, ease_factor)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (sched_id, lexeme_id, next_review, interval, ease),
                )
            else:
                self.conn.execute(
                    """
                    UPDATE review_schedule
                    SET next_review = ?, interval = ?, ease_factor = ?
                    WHERE lexeme_id = ?
                    """,
                    (next_review, interval, ease, lexeme_id),
                )

    def due_for_review(self, limit: int = 20) -> list[sqlite3.Row]:
        now_str = _now_utc()
        return self.conn.execute(
            """
            SELECT * FROM review_schedule
            WHERE next_review <= ?
            ORDER BY next_review
            LIMIT ?
            """,
            (now_str, limit),
        ).fetchall()

    def log_progress(
        self,
        new_words: int,
        reviewed_words: int,
        ci_score: Optional[float] = None,
    ) -> str:
        progress_id = str(uuid.uuid4())
        today = datetime.now(timezone.utc).date().isoformat()
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO progress (id, date, new_words, reviewed_words, ci_score)
                VALUES (?, ?, ?, ?, ?)
                """,
                (progress_id, today, new_words, reviewed_words, ci_score),
            )
        return progress_id


# ---------------------------------------------------------------------------
# LearnerSentenceFitScorer
# ---------------------------------------------------------------------------

class LearnerSentenceFitScorer:
    """
    Scores sentence candidates for a specific learner.

    Metrics:
    - known_word_pct  : fraction of tokens the learner probably knows
    - inference_score : estimated ability to infer unknowns from context
    - ci_plus_one     : whether the sentence sits at i+1 difficulty
    """

    def __init__(self, learner_model: LearnerModel, lexeme_repo: LexemeRepository):
        self.learner = learner_model
        self.repo = lexeme_repo

    def score(
        self,
        tokens: list[str],
        known_threshold: float = 0.8,
        target_known_pct: float = 0.95,
    ) -> dict:
        if not tokens:
            return {
                "known_word_pct": 0.0,
                "unknown_tokens": [],
                "inference_score": 0.0,
                "ci_plus_one": False,
            }

        probs = []
        unknown_tokens = []
        for token in tokens:
            lexeme = self.repo.get_lexeme_by_simplified(token)
            if lexeme is None:
                probs.append(0.0)
                unknown_tokens.append(token)
            else:
                p = self.learner.probability_known(lexeme["id"])
                probs.append(p)
                if p < known_threshold:
                    unknown_tokens.append(token)

        known_word_pct = sum(1 for p in probs if p >= known_threshold) / len(probs)

        # Inference score: higher when exactly 1 unknown in an otherwise known sentence
        unknown_count = len(unknown_tokens)
        if unknown_count == 0:
            inference_score = 1.0
        elif unknown_count == 1:
            inference_score = 0.9
        elif unknown_count <= 3:
            inference_score = max(0.0, 0.9 - (unknown_count - 1) * 0.2)
        else:
            inference_score = 0.0

        # CI+1: known_word_pct near target and exactly one unknown
        ci_plus_one = (known_word_pct >= target_known_pct * 0.95) and (unknown_count == 1)

        return {
            "known_word_pct": known_word_pct,
            "unknown_tokens": unknown_tokens,
            "inference_score": inference_score,
            "ci_plus_one": ci_plus_one,
        }


# ---------------------------------------------------------------------------
# MandarinPatchManager
# ---------------------------------------------------------------------------

class MandarinPatchManager(PatchManager):
    """
    Mandarin Learning Engine adapter for the generic PatchManager.

    Syncs five content tables in common_content.sqlite:
        lexemes, rankings, sentence_candidates, approved_content, analysis.

    Merge policy (Phase 01): INSERT OR IGNORE — safe for append-heavy tables.
    Timestamp domain: ISO 8601 strings stored as TEXT in SQLite.

    Note: approved_content uses 'approved_at' instead of 'created_at';
          this is handled via _TABLE_TS_COL.
    """

    TABLES_CREATED_AT = (
        "lexemes",
        "rankings",
        "sentence_candidates",
        "approved_content",
        "analysis",
    )

    # Per-table timestamp column override for export queries.
    # All tables use 'created_at' except approved_content which uses 'approved_at'.
    _TABLE_TS_COL = {
        "approved_content": "approved_at",
    }

    def schema_ensure(self, conn):
        """Ensure sync_state (via super) and all Mandarin content tables."""
        super().schema_ensure(conn)
        conn.executescript(_COMMON_CONTENT_DDL)
        conn.commit()

    def rows_export_since(self, conn, watermark):
        watermark_iso = self.iso_from_epoch(watermark)
        max_ts = watermark_iso
        tables = {}

        for table in self.TABLES_CREATED_AT:
            ts_col = self._TABLE_TS_COL.get(table, "created_at")
            rows = conn.execute(
                f"SELECT * FROM {table} WHERE {ts_col} > ? ORDER BY {ts_col}, id",
                (watermark_iso,),
            ).fetchall()
            row_dicts = [dict(row) for row in rows]
            tables[table] = row_dicts

            for row in row_dicts:
                ts_val = row.get(ts_col)
                if ts_val and ts_val > max_ts:
                    max_ts = ts_val

        return {
            "machine_id": self.machine_id,
            "exported_at": self.epoch_from_iso(max_ts),
            "watermark": watermark,
            "tables": tables,
        }

    def row_patch_apply(self, conn, patch):
        if patch.get("schema_version") != self.PATCH_SCHEMA_VERSION:
            return {
                "ok": False,
                "reason": "unsupported_schema_version",
                "schema_version": patch.get("schema_version"),
            }

        allowed_tables = set(self.TABLES_CREATED_AT)
        incoming_tables = set(patch.get("tables", {}).keys())
        unknown_tables = incoming_tables - allowed_tables
        if unknown_tables:
            return {
                "ok": False,
                "reason": "unknown_tables",
                "tables": sorted(unknown_tables),
            }

        inserted = {}

        with conn:
            for table, rows in patch.get("tables", {}).items():
                count = 0
                for row in rows:
                    if not row:
                        continue
                    columns = list(row.keys())
                    col_sql = ", ".join(columns)
                    placeholders = ", ".join("?" for _ in columns)
                    sql = f"INSERT OR IGNORE INTO {table} ({col_sql}) VALUES ({placeholders})"
                    cur = conn.execute(sql, [row[col] for col in columns])
                    count += cur.rowcount
                inserted[table] = count

        return {"ok": True, "inserted": inserted}

    @classmethod
    def iso_from_epoch(cls, epoch_value):
        from datetime import datetime, timezone

        try:
            epoch_float = float(epoch_value)
        except Exception:
            epoch_float = 0.0

        if epoch_float <= 0.0:
            return "0000-01-01T00:00:00+00:00"

        return datetime.fromtimestamp(epoch_float, tz=timezone.utc).isoformat()

    @classmethod
    def epoch_from_iso(cls, iso_value):
        from datetime import datetime, timezone

        if not iso_value:
            return 0.0
        if iso_value == "0000-01-01T00:00:00+00:00":
            return 0.0

        text = str(iso_value)
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"

        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return float(dt.timestamp())




# ---------------------------------------------------------------------------
# Engine facade
# ---------------------------------------------------------------------------

class MandarinLearningEngine:
    """
    Top-level facade that wires together all components for Phase 01.
    """

    def __init__(
        self,
        data_dir: Path = DATA_DIR,
        learner_id: str = "default",
        patch_remote_root: str = "",
        patch_machine_id: Optional[str] = None,
    ):
        global DATA_DIR, GENERATION_CACHE_DB, COMMON_CONTENT_DB
        DATA_DIR = data_dir
        GENERATION_CACHE_DB = data_dir / "generation_cache.sqlite"
        COMMON_CONTENT_DB = data_dir / "common_content.sqlite"

        self.corpus_cache = CorpusCache(GENERATION_CACHE_DB)
        self.ingestor = CorpusIngestor(self.corpus_cache)
        self.lexeme_repo = LexemeRepository(COMMON_CONTENT_DB)
        self.sentence_analyzer = SentenceAnalyzer(COMMON_CONTENT_DB)
        self.learner = LearnerModel(learner_id)
        self.fit_scorer = LearnerSentenceFitScorer(self.learner, self.lexeme_repo)
        self.patch_manager = MandarinPatchManager(
            db_path=COMMON_CONTENT_DB,
            patch_dir=data_dir / "patches",
            remote_root=patch_remote_root,
            machine_id=patch_machine_id,
        )

    def status(self) -> dict:
        lexeme_count = self.lexeme_repo.conn.execute(
            "SELECT COUNT(*) FROM lexemes"
        ).fetchone()[0]
        candidate_count = self.sentence_analyzer.conn.execute(
            "SELECT COUNT(*) FROM sentence_candidates"
        ).fetchone()[0]
        source_count = self.corpus_cache.conn.execute(
            "SELECT COUNT(*) FROM sources"
        ).fetchone()[0]
        due_count = len(self.learner.due_for_review())
        return {
            "schema_version":    SCHEMA_VERSION,
            "lexeme_count":      lexeme_count,
            "candidate_count":   candidate_count,
            "source_count":      source_count,
            "due_for_review":    due_count,
            "learner_id":        self.learner.learner_id,
        }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Mandarin Learning Engine - Phase 01")
    parser.add_argument("--data-dir", default="data", help="Directory for SQLite databases")
    parser.add_argument("--learner-id", default="default", help="Learner identifier")
    args = parser.parse_args()

    engine = MandarinLearningEngine(
        data_dir=Path(args.data_dir),
        learner_id=args.learner_id,
    )

    status = engine.status()
    print("Mandarin Learning Engine - Phase 01")
    print("=" * 40)
    for key, value in status.items():
        print(f"  {key:<20}: {value}")
    print("=" * 40)
    print("Databases initialised successfully.")
