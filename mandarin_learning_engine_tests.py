"""
mandarin_learning_engine_tests.py

Tests for MandarinPatchManager — the Mandarin Learning Engine adapter that
subclasses the generic PatchManager.

These tests are intentionally separate from patch_manager_tests.py.
No Mandarin-specific logic should appear in patch_manager_tests.py.

Conventions:
- Fixed ISO timestamps are used throughout; no time.time() calls.
- All tests use a fake filesystem remote (no real rclone).
- Tests focus on timestamp-domain correctness, export completeness, apply
  idempotency, and unknown-table rejection.
"""

import gzip
import json
import os
import pathlib
import shutil
import sqlite3
import tempfile
import unittest

from mandarin_learning_engine import MandarinPatchManager


# ---------------------------------------------------------------------------
# Fixed test timestamps
# ---------------------------------------------------------------------------

T0 = "0000-01-01T00:00:00+00:00"   # epoch zero sentinel
T1 = "2026-01-01T00:00:00+00:00"
T2 = "2026-01-01T00:01:00+00:00"
T3 = "2026-01-01T00:02:00+00:00"

# Corresponding epoch floats (for checking exported_at)
E1 = MandarinPatchManager.epoch_from_iso(T1)
E2 = MandarinPatchManager.epoch_from_iso(T2)
E3 = MandarinPatchManager.epoch_from_iso(T3)


# ---------------------------------------------------------------------------
# Fake remote (filesystem-backed, no rclone binary needed)
# ---------------------------------------------------------------------------

class _FakeRemote:
    """Minimal filesystem fake for rclone copyto operations.

    Remote paths start with 'fake_remote:' followed by the relative path.
    """

    def __init__(self, storage_dir):
        self.storage_dir = pathlib.Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)

    def _resolve_remote(self, value):
        value = str(value)
        prefix = "fake_remote:"
        if not value.startswith(prefix):
            return None
        rel = value[len(prefix):].lstrip("/").replace("\\", "/")
        return self.storage_dir / rel

    def copyto(self, src, dst, immutable=False):
        src_remote = self._resolve_remote(src)
        dst_remote = self._resolve_remote(dst)

        if src_remote is not None:
            # remote -> local download
            if not src_remote.exists():
                return {
                    "ok": False, "returncode": 1, "stdout": "",
                    "stderr": f"missing: {src}", "cmd": ["fake-rclone"],
                }
            dst_path = pathlib.Path(dst)
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            dst_path.write_bytes(src_remote.read_bytes())
            return {"ok": True, "returncode": 0, "stdout": "", "stderr": "", "cmd": ["fake-rclone"]}

        if dst_remote is not None:
            # local -> remote upload
            dst_remote.parent.mkdir(parents=True, exist_ok=True)
            if immutable and dst_remote.exists():
                return {
                    "ok": False, "returncode": 1, "stdout": "",
                    "stderr": "immutable", "cmd": ["fake-rclone"],
                }
            import shutil as _sh
            _sh.copy2(str(src), str(dst_remote))
            return {"ok": True, "returncode": 0, "stdout": "", "stderr": "", "cmd": ["fake-rclone"]}

        # local -> local fallback (shouldn't happen in these tests)
        import shutil as _sh
        dst_path = pathlib.Path(dst)
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        _sh.copy2(str(src), str(dst))
        return {"ok": True, "returncode": 0, "stdout": "", "stderr": "", "cmd": ["fake-rclone"]}


# ---------------------------------------------------------------------------
# Test subclass: MandarinPatchManager wired to a _FakeRemote
# ---------------------------------------------------------------------------

class _MandarinTestPM(MandarinPatchManager):
    """MandarinPatchManager with rclone_copyto routed through _FakeRemote."""

    def __init__(self, *args, remote=None, **kwargs):
        self.remote = remote
        super().__init__(*args, **kwargs)

    def rclone_copyto(self, src, dst, immutable=False):
        if self.remote is None:
            raise RuntimeError("_MandarinTestPM requires a remote")
        return self.remote.copyto(src, dst, immutable=immutable)


# ---------------------------------------------------------------------------
# Row insertion helpers
# ---------------------------------------------------------------------------

def _insert_lexeme(conn, lex_id, created_at):
    """Insert a minimal lexeme row."""
    conn.execute(
        """
        INSERT OR IGNORE INTO lexemes
            (id, simplified, traditional, pinyin, english,
             frequency_rank, hsk_level, source, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (lex_id, lex_id, None, "pin", "eng", 1, 1, "test", created_at),
    )
    conn.commit()


def _insert_sentence_candidate(conn, sc_id, created_at, content="test sentence"):
    conn.execute(
        """
        INSERT OR IGNORE INTO sentence_candidates
            (id, content, pinyin, english, grammar_tags,
             naturalness_score, ambiguity_score, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (sc_id, content, None, None, None, None, None, created_at),
    )
    conn.commit()


def _read_lexemes(db_path):
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        return {r["id"]: dict(r) for r in conn.execute("SELECT * FROM lexemes").fetchall()}
    finally:
        conn.close()


def _read_sentence_candidates(db_path):
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        return {r["id"]: dict(r) for r in conn.execute("SELECT * FROM sentence_candidates").fetchall()}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Base fixture
# ---------------------------------------------------------------------------

class _MandarinFixture(unittest.TestCase):
    def setUp(self):
        self.root = pathlib.Path(tempfile.mkdtemp())
        self.remote = _FakeRemote(self.root / "remote")
        self.remote_root = "fake_remote:mandarin_store"

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def _make_pm(self, name, machine_id):
        pm = _MandarinTestPM(
            db_path=self.root / f"{name}.sqlite",
            patch_dir=self.root / f"{name}_patches",
            remote_root=self.remote_root,
            machine_id=machine_id,
            rclone_bin="fake-rclone",
            remote=self.remote,
        )
        return pm

    def _open_conn(self, pm):
        """Return an open connection for direct row insertion."""
        return pm.conn_open()


# ===========================================================================
# Export tests
# ===========================================================================

class TestMandarinExport(_MandarinFixture):

    def test_mandarin_patch_exports_created_rows(self):
        pm = self._make_pm("a", "machine_a")
        conn = self._open_conn(pm)
        _insert_lexeme(conn, "lex_1", T1)
        conn.close()

        result = pm.patch_pack_create("export test")
        self.assertTrue(result["ok"])

        # Read the patch file back and verify the row is present.
        patch_path = pathlib.Path(result["patch_file"])
        with gzip.open(patch_path, "rb") as fh:
            patch = json.loads(fh.read().decode("utf-8"))

        lexemes = patch["tables"]["lexemes"]
        self.assertEqual(len(lexemes), 1)
        self.assertEqual(lexemes[0]["id"], "lex_1")

    def test_mandarin_patch_does_not_export_rows_at_watermark(self):
        # A row whose created_at equals the watermark must NOT be exported
        # (the query uses strict >, not >=).
        pm = self._make_pm("a", "machine_a")
        conn = self._open_conn(pm)

        # Set the watermark to T1 first by exporting that row.
        _insert_lexeme(conn, "lex_at_watermark", T1)
        conn.close()
        pm.patch_pack_create("advance watermark")

        # Now insert another row at exactly T1 — it should be excluded.
        conn = self._open_conn(pm)
        _insert_lexeme(conn, "lex_exactly_at_watermark", T1)
        conn.close()
        result2 = pm.patch_pack_create("second export")
        self.assertTrue(result2["ok"])

        patch_path = pathlib.Path(result2["patch_file"])
        with gzip.open(patch_path, "rb") as fh:
            patch = json.loads(fh.read().decode("utf-8"))

        ids = [r["id"] for r in patch["tables"]["lexemes"]]
        self.assertNotIn("lex_exactly_at_watermark", ids)

    def test_mandarin_patch_no_rows_preserves_watermark(self):
        pm = self._make_pm("a", "machine_a")
        conn = self._open_conn(pm)

        # First export with a real row so the watermark is set.
        _insert_lexeme(conn, "lex_1", T1)
        conn.close()
        result1 = pm.patch_pack_create("first")
        self.assertTrue(result1["ok"])
        watermark_after_first = result1["watermark_after"]

        # Second export: no new rows.
        result2 = pm.patch_pack_create("second empty")
        self.assertTrue(result2["ok"])

        # Watermark must not advance when there are no new rows.
        self.assertEqual(result2["watermark_before"], watermark_after_first)
        self.assertEqual(result2["watermark_after"], watermark_after_first)

    def test_mandarin_patch_exported_at_advances_to_max_created_at(self):
        pm = self._make_pm("a", "machine_a")
        conn = self._open_conn(pm)
        _insert_lexeme(conn, "lex_early", T1)
        _insert_lexeme(conn, "lex_late", T3)
        conn.close()

        result = pm.patch_pack_create("advance test")
        self.assertTrue(result["ok"])

        # exported_at (= watermark_after) should equal epoch(T3), not wall-clock.
        self.assertAlmostEqual(result["watermark_after"], E3, places=0)

    def test_mandarin_patch_missing_table_fails_loudly(self):
        pm = self._make_pm("a", "machine_a")

        # Open a raw connection that bypasses schema_ensure so the Mandarin
        # tables do NOT exist — rows_export_since must raise, not return [].
        raw_conn = sqlite3.connect(str(pm.db_path))
        raw_conn.row_factory = sqlite3.Row
        # Only create sync_state, not the Mandarin tables.
        raw_conn.execute(
            "CREATE TABLE IF NOT EXISTS sync_state (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        raw_conn.commit()

        with self.assertRaises(sqlite3.OperationalError):
            pm.rows_export_since(raw_conn, 0.0)

        raw_conn.close()


# ===========================================================================
# Apply tests
# ===========================================================================

class TestMandarinApply(_MandarinFixture):

    def _minimal_patch(self, machine_id="peer_a", seq="000001", tables=None):
        """Build a minimal valid patch dict for direct apply testing."""
        return {
            "schema_version": MandarinPatchManager.PATCH_SCHEMA_VERSION,
            "machine_id": machine_id,
            "seq": seq,
            "exported_at": E1,
            "watermark": 0.0,
            "tables": tables or {t: [] for t in MandarinPatchManager.TABLES_CREATED_AT},
        }

    def test_mandarin_patch_apply_insert_or_ignore(self):
        pm = self._make_pm("a", "machine_a")

        patch = self._minimal_patch(tables={
            "lexemes": [
                {"id": "lex_1", "simplified": "一", "traditional": None,
                 "pinyin": "yī", "english": "one",
                 "frequency_rank": 1, "hsk_level": 1,
                 "source": "test", "created_at": T1},
            ],
            "rankings": [],
            "sentence_candidates": [],
            "approved_content": [],
            "analysis": [],
        })

        # Write patch to a temp file and apply it.
        patch_path = self.root / "test.json.gz"
        with gzip.open(patch_path, "wb") as fh:
            fh.write(json.dumps(patch, sort_keys=True, ensure_ascii=False).encode("utf-8"))

        result = pm.patch_pack_apply(patch_path, source_machine_id="peer_a", seq="000001")
        self.assertTrue(result["ok"])

        lexemes = _read_lexemes(pm.db_path)
        self.assertIn("lex_1", lexemes)
        self.assertEqual(lexemes["lex_1"]["simplified"], "一")

    def test_mandarin_patch_apply_is_idempotent(self):
        pm = self._make_pm("a", "machine_a")

        patch = self._minimal_patch(tables={
            "lexemes": [
                {"id": "lex_1", "simplified": "一", "traditional": None,
                 "pinyin": "yī", "english": "one",
                 "frequency_rank": 1, "hsk_level": 1,
                 "source": "test", "created_at": T1},
            ],
            "rankings": [], "sentence_candidates": [], "approved_content": [], "analysis": [],
        })

        patch_path = self.root / "test.json.gz"
        with gzip.open(patch_path, "wb") as fh:
            fh.write(json.dumps(patch, sort_keys=True, ensure_ascii=False).encode("utf-8"))

        result1 = pm.patch_pack_apply(patch_path, source_machine_id="peer_a", seq="000001")
        self.assertTrue(result1["ok"])

        result2 = pm.patch_pack_apply(patch_path, source_machine_id="peer_a", seq="000001")
        self.assertTrue(result2["ok"])
        # Second apply must be skipped as already applied — not a duplicate insert.
        self.assertTrue(result2.get("skipped"))

        # Row count unchanged.
        self.assertEqual(len(_read_lexemes(pm.db_path)), 1)

    def test_mandarin_patch_apply_rejects_unknown_tables(self):
        pm = self._make_pm("a", "machine_a")

        patch = self._minimal_patch(tables={
            "lexemes": [],
            "unknown_mandarin_table": [{"id": "x", "created_at": T1}],
        })

        patch_path = self.root / "test.json.gz"
        with gzip.open(patch_path, "wb") as fh:
            fh.write(json.dumps(patch, sort_keys=True, ensure_ascii=False).encode("utf-8"))

        result = pm.patch_pack_apply(patch_path, source_machine_id="peer_a", seq="000001")
        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "unknown_tables")
        self.assertIn("unknown_mandarin_table", result["tables"])

    def test_mandarin_patch_apply_rejects_unsupported_schema_version(self):
        pm = self._make_pm("a", "machine_a")

        patch = self._minimal_patch()
        patch["schema_version"] = 999

        patch_path = self.root / "test.json.gz"
        with gzip.open(patch_path, "wb") as fh:
            fh.write(json.dumps(patch, sort_keys=True, ensure_ascii=False).encode("utf-8"))

        result = pm.patch_pack_apply(patch_path, source_machine_id="peer_a", seq="000001")
        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "unsupported_schema_version")


# ===========================================================================
# Integration tests
# ===========================================================================

class TestMandarinIntegration(_MandarinFixture):

    def test_mandarin_patch_two_databases_converge(self):
        a = self._make_pm("a", "machine_a")
        b = self._make_pm("b", "machine_b")

        # Machine A inserts a lexeme and pushes.
        conn_a = self._open_conn(a)
        _insert_lexeme(conn_a, "lex_shared", T1)
        conn_a.close()

        push_result = a.sync_push("add lex_shared")
        self.assertTrue(push_result["ok"])

        # Machine B pulls — should receive the lexeme.
        pull_result = b.sync_pull()
        self.assertTrue(pull_result["ok"])
        self.assertEqual(pull_result["per_machine"]["machine_a"]["applied"], 1)

        lexemes_b = _read_lexemes(b.db_path)
        self.assertIn("lex_shared", lexemes_b)

    def test_mandarin_patch_sync_with_generic_patch_manager_methods(self):
        a = self._make_pm("a", "machine_a")
        b = self._make_pm("b", "machine_b")

        # Insert rows across multiple tables in machine A.
        conn_a = self._open_conn(a)
        _insert_lexeme(conn_a, "lex_1", T1)
        _insert_lexeme(conn_a, "lex_2", T2)
        _insert_sentence_candidate(conn_a, "sc_1", T2, "这是一个句子")
        conn_a.close()

        self.assertTrue(a.sync_push("batch")["ok"])

        # Machine B syncs — all rows must appear.
        self.assertTrue(b.sync_pull()["ok"])

        lexemes_b = _read_lexemes(b.db_path)
        sentences_b = _read_sentence_candidates(b.db_path)

        self.assertIn("lex_1", lexemes_b)
        self.assertIn("lex_2", lexemes_b)
        self.assertIn("sc_1", sentences_b)

        # Machine B adds its own content and syncs back.
        conn_b = self._open_conn(b)
        _insert_lexeme(conn_b, "lex_b_only", T3)
        conn_b.close()

        self.assertTrue(b.sync_push("b content")["ok"])
        self.assertTrue(a.sync_pull()["ok"])

        lexemes_a = _read_lexemes(a.db_path)
        self.assertIn("lex_b_only", lexemes_a)


if __name__ == "__main__":
    unittest.main(verbosity=2)
