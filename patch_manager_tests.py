"""
patch_manager_tests.py

Battering tests for generic PatchManager.

Assumptions:
- patch_manager.py is beside this file.
- patch_manager.py exposes a generic PatchManager class.
- PatchManager has these methods/names:
    patch_pack_create()
    patch_pack_apply()
    patch_pack_push()
    patch_pack_pull()
    sync_push()
    sync_pull()
    sync_all()
    central_manifest_pull()
    central_manifest_push()
    machine_id_get()
    machine_id_validate()

These tests deliberately avoid real rclone and avoid third-party packages.
They use a local filesystem fake remote by overriding rclone_copyto().
"""

import gzip
import json
import os
import pathlib
import random
import shutil
import sqlite3
import tempfile
import unittest
import unittest.mock


from patch_manager import PatchManager


class LocalRemote:
    """Small filesystem-backed fake rclone remote.

    Remote paths are expected to look like:

        fake_remote:/some/root/central_manifest.json
        fake_remote:/some/root/machines/machine_a/patch-000001.json.gz

    The fake maps the path component after "fake_remote:" into storage_dir.
    """

    def __init__(self, storage_dir):
        self.storage_dir = pathlib.Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.ops = []
        self.fail_next_copyto = None

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

        self.ops.append({
            "op": "copyto",
            "src": str(src),
            "dst": str(dst),
            "immutable": bool(immutable),
            "fail": repr(self.fail_next_copyto),
        })

        fail = self.fail_next_copyto
        self.fail_next_copyto = None

        if fail == "return_error":
            return {
                "ok": False,
                "returncode": 1,
                "stdout": "",
                "stderr": "injected failure",
                "cmd": ["fake-rclone"],
            }

        if src_remote is not None and dst_remote is not None:
            raise AssertionError("remote-to-remote copy not expected in these tests")

        if src_remote is not None:
            # remote -> local
            src_path = src_remote
            dst_path = pathlib.Path(dst)
            if not src_path.exists():
                return {
                    "ok": False,
                    "returncode": 1,
                    "stdout": "",
                    "stderr": f"missing remote file: {src}",
                    "cmd": ["fake-rclone"],
                }
            data = src_path.read_bytes()
            if fail == "truncate_download":
                data = data[: max(0, len(data) // 2)]
            elif fail == "corrupt_download" and data:
                buf = bytearray(data)
                buf[len(buf) // 2] ^= 0xFF
                data = bytes(buf)
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            dst_path.write_bytes(data)
            return {"ok": True, "returncode": 0, "stdout": "", "stderr": "", "cmd": ["fake-rclone"]}

        if dst_remote is not None:
            # local -> remote
            src_path = pathlib.Path(src)
            dst_path = dst_remote
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            if immutable and dst_path.exists():
                return {
                    "ok": False,
                    "returncode": 1,
                    "stdout": "",
                    "stderr": "immutable upload refused",
                    "cmd": ["fake-rclone"],
                }
            data = src_path.read_bytes()
            if fail == "silent_drop":
                return {"ok": True, "returncode": 0, "stdout": "", "stderr": "", "cmd": ["fake-rclone"]}
            if fail == "truncate_upload":
                data = data[: max(0, len(data) // 2)]
            elif fail == "corrupt_upload" and data:
                buf = bytearray(data)
                buf[len(buf) // 2] ^= 0xFF
                data = bytes(buf)
            tmp = pathlib.Path(str(dst_path) + ".tmp")
            tmp.write_bytes(data)
            os.replace(str(tmp), str(dst_path))
            return {"ok": True, "returncode": 0, "stdout": "", "stderr": "", "cmd": ["fake-rclone"]}

        # local -> local fallback
        src_path = pathlib.Path(src)
        dst_path = pathlib.Path(dst)
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src_path, dst_path)
        return {"ok": True, "returncode": 0, "stdout": "", "stderr": "", "cmd": ["fake-rclone"]}

    def read_json(self, remote_path_after_root):
        path = self.storage_dir / remote_path_after_root
        return json.loads(path.read_text(encoding="utf-8"))

    def exists(self, remote_path_after_root):
        return (self.storage_dir / remote_path_after_root).exists()


class TestPatchManager(PatchManager):
    """Concrete PatchManager for tests.

    It syncs one table:

        items(id TEXT PRIMARY KEY, value TEXT, updated_at REAL)

    Conflict policy:
    - incoming row wins only if incoming.updated_at > existing.updated_at
    - identical/older rows are ignored
    """

    def __init__(self, *args, remote=None, **kwargs):
        self.remote = remote
        super().__init__(*args, **kwargs)

    def rclone_copyto(self, src, dst, immutable=False):
        if self.remote is None:
            raise RuntimeError("TestPatchManager requires remote")
        return self.remote.copyto(src, dst, immutable=immutable)

    def schema_ensure(self, conn):
        super().schema_ensure(conn)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS items (
                id         TEXT PRIMARY KEY,
                value      TEXT NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )
        conn.commit()

    def rows_export_since(self, conn, watermark):
        rows = conn.execute(
            "SELECT id, value, updated_at FROM items WHERE updated_at > ? ORDER BY updated_at, id",
            (float(watermark),),
        ).fetchall()
        rows_list = [dict(row) for row in rows]
        # Use the max domain-level timestamp as the watermark advance, not wall-clock time.
        # This keeps the watermark in the same units as updated_at so subsequent exports
        # correctly pick up rows inserted with logical (non-wall-clock) timestamps.
        exported_at = max((r["updated_at"] for r in rows_list), default=float(watermark))
        return {
            "exported_at": exported_at,
            "watermark": float(watermark),
            "tables": {
                "items": rows_list,
            },
        }

    def row_patch_apply(self, conn, patch):
        if patch.get("schema_version") != self.PATCH_SCHEMA_VERSION:
            return {"ok": False, "reason": "unsupported_schema_version"}

        inserted = 0
        updated = 0
        ignored = 0

        with conn:
            for row in patch.get("tables", {}).get("items", []):
                existing = conn.execute(
                    "SELECT updated_at FROM items WHERE id = ?",
                    (row["id"],),
                ).fetchone()

                if existing is None:
                    conn.execute(
                        "INSERT INTO items(id, value, updated_at) VALUES (?, ?, ?)",
                        (row["id"], row["value"], float(row["updated_at"])),
                    )
                    inserted += 1
                elif float(row["updated_at"]) > float(existing["updated_at"]):
                    conn.execute(
                        "UPDATE items SET value = ?, updated_at = ? WHERE id = ?",
                        (row["value"], float(row["updated_at"]), row["id"]),
                    )
                    updated += 1
                else:
                    ignored += 1

        return {
            "ok": True,
            "inserted": inserted,
            "updated": updated,
            "ignored": ignored,
        }


def insert_item(db_path, item_id, value, updated_at):
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS items (
                id         TEXT PRIMARY KEY,
                value      TEXT NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )
        # Use the same "newer timestamp wins" semantics as row_patch_apply so that
        # the test's expected dict and the actual DB stay consistent.
        conn.execute(
            """
            INSERT INTO items(id, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                value = excluded.value,
                updated_at = excluded.updated_at
            WHERE excluded.updated_at > items.updated_at
            """,
            (item_id, value, float(updated_at)),
        )
        conn.commit()
    finally:
        conn.close()


def read_items(db_path):
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute("SELECT id, value, updated_at FROM items ORDER BY id").fetchall()
        return {row["id"]: {"value": row["value"], "updated_at": row["updated_at"]} for row in rows}
    finally:
        conn.close()


class PatchManagerUnitTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        self.remote = LocalRemote(self.root / "remote")
        self.remote_root = "fake_remote:/patch_store"

    def tearDown(self):
        self.tmp.cleanup()

    def manager(self, name, machine_id):
        return TestPatchManager(
            db_path=self.root / f"{name}.sqlite",
            patch_dir=self.root / f"{name}_patches",
            remote_root=self.remote_root,
            machine_id=machine_id,
            rclone_bin="fake-rclone",
            remote=self.remote,
        )

    def test_machine_id_validate_rejects_unsafe_values(self):
        good = ["machine_a", "machine-1", "ABC123"]
        bad = ["", "machine a", "x/y", "../x", "x:y", "ümlaut"]

        for value in good:
            self.assertEqual(PatchManager.machine_id_validate(value), value)

        for value in bad:
            with self.assertRaises(ValueError):
                PatchManager.machine_id_validate(value)

    def test_machine_id_is_stable_when_persisted(self):
        patch_dir = self.root / "stable_patches"
        m1 = TestPatchManager(
            db_path=self.root / "stable.sqlite",
            patch_dir=patch_dir,
            remote_root=self.remote_root,
            rclone_bin="fake-rclone",
            remote=self.remote,
        )
        m2 = TestPatchManager(
            db_path=self.root / "stable.sqlite",
            patch_dir=patch_dir,
            remote_root=self.remote_root,
            rclone_bin="fake-rclone",
            remote=self.remote,
        )
        self.assertEqual(m1.machine_id, m2.machine_id)
        self.assertTrue((patch_dir / "machine_id.txt").exists())

    def test_patch_pack_create_writes_gzip_json_and_advances_sequence(self):
        m = self.manager("a", "machine_a")
        insert_item(m.db_path, "one", "v1", 100.0)

        r1 = m.patch_pack_create("first")
        r2 = m.patch_pack_create("second empty-ish")

        self.assertTrue(r1["ok"])
        self.assertEqual(r1["seq"], "000001")
        self.assertTrue(pathlib.Path(r1["patch_file"]).exists())
        self.assertEqual(r2["seq"], "000002")

        with gzip.open(r1["patch_file"], "rb") as fh:
            patch = json.loads(fh.read().decode("utf-8"))

        self.assertEqual(patch["machine_id"], "machine_a")
        self.assertEqual(patch["seq"], "000001")
        self.assertIn("items", patch["tables"])
        self.assertEqual(patch["tables"]["items"][0]["id"], "one")

    def test_patch_pack_apply_skips_own_patch(self):
        m = self.manager("a", "machine_a")
        insert_item(m.db_path, "one", "v1", 100.0)

        created = m.patch_pack_create()
        applied = m.patch_pack_apply(created["patch_file"])

        self.assertTrue(applied["ok"])
        self.assertTrue(applied["skipped"])
        self.assertEqual(applied["reason"], "own_patch")

    def test_patch_pack_apply_is_idempotent_for_peer_patch(self):
        a = self.manager("a", "machine_a")
        b = self.manager("b", "machine_b")

        insert_item(a.db_path, "one", "v1", 100.0)
        created = a.patch_pack_create()

        first = b.patch_pack_apply(
            created["patch_file"],
            source_machine_id="machine_a",
            seq="000001",
        )
        second = b.patch_pack_apply(
            created["patch_file"],
            source_machine_id="machine_a",
            seq="000001",
        )

        self.assertTrue(first["ok"])
        self.assertEqual(first["apply_result"]["inserted"], 1)
        self.assertTrue(second["ok"])
        self.assertTrue(second["skipped"])
        self.assertEqual(second["reason"], "already_applied")
        self.assertEqual(read_items(b.db_path)["one"]["value"], "v1")

    def test_conflict_resolution_newer_update_wins(self):
        a = self.manager("a", "machine_a")
        b = self.manager("b", "machine_b")

        insert_item(a.db_path, "same", "old", 100.0)
        insert_item(b.db_path, "same", "new", 200.0)

        a_patch = a.patch_pack_create()
        b_patch = b.patch_pack_create()

        b_apply = b.patch_pack_apply(a_patch["patch_file"], "machine_a", "000001")
        a_apply = a.patch_pack_apply(b_patch["patch_file"], "machine_b", "000001")

        self.assertTrue(a_apply["ok"])
        self.assertTrue(b_apply["ok"])
        self.assertEqual(read_items(a.db_path)["same"]["value"], "new")
        self.assertEqual(read_items(b.db_path)["same"]["value"], "new")


class PatchManagerRemoteSyncTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        self.remote = LocalRemote(self.root / "remote")
        self.remote_root = "fake_remote:/patch_store"

    def tearDown(self):
        self.tmp.cleanup()

    def manager(self, name, machine_id):
        return TestPatchManager(
            db_path=self.root / f"{name}.sqlite",
            patch_dir=self.root / f"{name}_patches",
            remote_root=self.remote_root,
            machine_id=machine_id,
            rclone_bin="fake-rclone",
            remote=self.remote,
        )

    def test_sync_push_uploads_immutable_patch_and_updates_central_manifest(self):
        a = self.manager("a", "machine_a")
        insert_item(a.db_path, "one", "v1", 100.0)

        result = a.sync_push("push one")

        self.assertTrue(result["ok"], result)
        self.assertTrue(self.remote.exists("patch_store/machines/machine_a/patch-000001.json.gz"))
        self.assertTrue(self.remote.exists("patch_store/central_manifest.json"))

        manifest = self.remote.read_json("patch_store/central_manifest.json")
        self.assertEqual(manifest["machines"]["machine_a"]["latest_seq"], "000001")

    def test_two_machines_sync_to_convergence(self):
        a = self.manager("a", "machine_a")
        b = self.manager("b", "machine_b")

        insert_item(a.db_path, "a1", "from_a", 100.0)
        insert_item(b.db_path, "b1", "from_b", 110.0)

        self.assertTrue(a.sync_push("a push")["ok"])
        self.assertTrue(b.sync_push("b push")["ok"])

        self.assertTrue(a.sync_pull()["ok"])
        self.assertTrue(b.sync_pull()["ok"])

        expected = {
            "a1": {"value": "from_a", "updated_at": 100.0},
            "b1": {"value": "from_b", "updated_at": 110.0},
        }
        self.assertEqual(read_items(a.db_path), expected)
        self.assertEqual(read_items(b.db_path), expected)

    def test_three_machines_multiple_rounds_converge(self):
        machines = [
            self.manager("a", "machine_a"),
            self.manager("b", "machine_b"),
            self.manager("c", "machine_c"),
        ]

        insert_item(machines[0].db_path, "shared", "a_old", 100.0)
        insert_item(machines[1].db_path, "shared", "b_new", 200.0)
        insert_item(machines[2].db_path, "c_only", "c", 150.0)

        for m in machines:
            self.assertTrue(m.sync_push("round1")["ok"])

        for m in machines:
            self.assertTrue(m.sync_pull()["ok"])

        for m in machines:
            items = read_items(m.db_path)
            self.assertEqual(items["shared"]["value"], "b_new")
            self.assertEqual(items["c_only"]["value"], "c")

        # Second round after convergence.
        insert_item(machines[0].db_path, "a2", "a_second", 300.0)
        insert_item(machines[2].db_path, "shared", "c_newest", 400.0)

        for m in machines:
            self.assertTrue(m.sync_push("round2")["ok"])

        for m in machines:
            self.assertTrue(m.sync_pull()["ok"])

        for m in machines:
            items = read_items(m.db_path)
            self.assertEqual(items["shared"]["value"], "c_newest")
            self.assertEqual(items["a2"]["value"], "a_second")
            self.assertEqual(items["c_only"]["value"], "c")

    def test_immutable_overwrite_conflict_is_reported(self):
        a = self.manager("a", "machine_a")
        insert_item(a.db_path, "one", "v1", 100.0)

        first = a.sync_push("first")
        second = a.patch_pack_push()  # tries to push same latest patch again.

        self.assertTrue(first["ok"])
        self.assertFalse(second["ok"])
        self.assertFalse(second["patch_upload"]["ok"])
        self.assertIn("immutable", second["patch_upload"]["stderr"])

    def test_missing_central_manifest_creates_default(self):
        a = self.manager("a", "machine_a")
        result = a.central_manifest_pull()

        self.assertTrue(result["ok"])
        self.assertTrue(result["created_default"])
        self.assertEqual(result["manifest"]["machines"], {})

    def test_missing_peer_patch_stops_without_advancing_progress(self):
        a = self.manager("a", "machine_a")
        b = self.manager("b", "machine_b")

        # Put manifest claiming machine_a has patch 1, but do not upload the patch.
        manifest = {
            "patch_schema_version": 1,
            "remote_root": self.remote_root,
            "machines": {"machine_a": {"latest_seq": "000001", "updated_at": 1.0}},
            "updated_at": 1.0,
        }
        manifest_path = self.remote.storage_dir / "patch_store" / "central_manifest.json"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        result = b.sync_pull()
        self.assertTrue(result["ok"])
        self.assertEqual(result["total_applied"], 0)
        self.assertEqual(result["per_machine"]["machine_a"]["stopped_reason"], "download_failed")

        conn = b.conn_open()
        try:
            self.assertEqual(b.sync_state_get(conn, "patch_applied_machine_a", "000000"), "000000")
        finally:
            conn.close()


class PatchManagerChaosTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        self.remote = LocalRemote(self.root / "remote")
        self.remote_root = "fake_remote:/patch_store"

    def tearDown(self):
        self.tmp.cleanup()

    def manager(self, name, machine_id):
        return TestPatchManager(
            db_path=self.root / f"{name}.sqlite",
            patch_dir=self.root / f"{name}_patches",
            remote_root=self.remote_root,
            machine_id=machine_id,
            rclone_bin="fake-rclone",
            remote=self.remote,
        )

    def test_corrupt_download_does_not_mark_patch_applied(self):
        a = self.manager("a", "machine_a")
        b = self.manager("b", "machine_b")

        insert_item(a.db_path, "one", "v1", 100.0)
        self.assertTrue(a.sync_push("a")["ok"])

        self.remote.fail_next_copyto = "corrupt_download"
        with self.assertRaises(Exception):
            b.sync_pull()

        conn = b.conn_open()
        try:
            self.assertEqual(b.sync_state_get(conn, "patch_applied_machine_a", "000000"), "000000")
        finally:
            conn.close()

    def test_truncated_download_does_not_mark_patch_applied(self):
        a = self.manager("a", "machine_a")
        b = self.manager("b", "machine_b")

        insert_item(a.db_path, "one", "v1", 100.0)
        self.assertTrue(a.sync_push("a")["ok"])

        self.remote.fail_next_copyto = "truncate_download"
        with self.assertRaises(Exception):
            b.sync_pull()

        conn = b.conn_open()
        try:
            self.assertEqual(b.sync_state_get(conn, "patch_applied_machine_a", "000000"), "000000")
        finally:
            conn.close()

    def test_randomized_multi_machine_stress_converges(self):
        rng = random.Random(12345)
        machines = [
            self.manager("a", "machine_a"),
            self.manager("b", "machine_b"),
            self.manager("c", "machine_c"),
            self.manager("d", "machine_d"),
        ]

        expected = {}

        # 8 rounds with overlapping keys.
        for round_no in range(8):
            for idx, manager in enumerate(machines):
                for _ in range(5):
                    item_id = f"item_{rng.randint(1, 12)}"
                    ts = round_no * 1000 + idx * 100 + rng.random()
                    value = f"{manager.machine_id}_{round_no}_{rng.randint(1, 999)}"
                    insert_item(manager.db_path, item_id, value, ts)

                    if item_id not in expected or ts > expected[item_id]["updated_at"]:
                        expected[item_id] = {"value": value, "updated_at": ts}

                self.assertTrue(manager.sync_push(f"round {round_no}")["ok"])

            # Pull in shuffled order to exercise non-symmetric convergence.
            shuffled = machines[:]
            rng.shuffle(shuffled)
            for manager in shuffled:
                result = manager.sync_pull()
                self.assertTrue(result["ok"], result)

        # Final full pull pass.
        for manager in machines:
            self.assertTrue(manager.sync_pull()["ok"])

        for manager in machines:
            self.assertEqual(read_items(manager.db_path), expected)


# ---------------------------------------------------------------------------
# Helpers shared by the new test classes
# ---------------------------------------------------------------------------

def _write_gz(path, data_bytes):
    """Write data_bytes into a gzip file at path."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wb") as fh:
        fh.write(data_bytes)


def _write_gz_patch(path, patch_dict):
    """Serialise patch_dict to JSON and write as gzip."""
    _write_gz(path, json.dumps(patch_dict, ensure_ascii=False).encode("utf-8"))


# ---------------------------------------------------------------------------
# 1. Malformed gzip does not advance sync_state
# ---------------------------------------------------------------------------

class TestMalformedGzip(unittest.TestCase):
    """
    patch_pack_apply must not advance sync_state when the patch file is not
    valid gzip.  The gzip read happens before any DB access in patch_pack_apply,
    so a corrupt file must raise before touching the database.
    """

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self._tmp.name)
        self.remote = LocalRemote(self.root / "remote")
        self.pm = TestPatchManager(
            db_path=self.root / "test.sqlite",
            patch_dir=self.root / "patches",
            remote_root="fake_remote:/store",
            machine_id="machine_a",
            rclone_bin="fake-rclone",
            remote=self.remote,
        )

    def tearDown(self):
        self._tmp.cleanup()

    def _peer_patch_path(self, peer_id, seq):
        p = self.root / "patches" / "machines" / peer_id / f"patch-{seq:06d}.json.gz"
        p.parent.mkdir(parents=True, exist_ok=True)
        return p

    def _state(self, key):
        conn = self.pm.conn_open()
        try:
            return self.pm.sync_state_get(conn, key, "000000")
        finally:
            conn.close()

    def test_raises_on_random_garbage(self):
        p = self._peer_patch_path("peer_b", 1)
        p.write_bytes(b"\x00\x01\x02\x03 this is not gzip")
        with self.assertRaises(Exception):
            self.pm.patch_pack_apply(p, source_machine_id="peer_b", seq="000001")

    def test_sync_state_not_advanced_after_garbage(self):
        p = self._peer_patch_path("peer_b", 1)
        p.write_bytes(b"\xff\xfe garbage bytes")
        try:
            self.pm.patch_pack_apply(p, source_machine_id="peer_b", seq="000001")
        except Exception:
            pass
        self.assertEqual(self._state("patch_applied_peer_b"), "000000")

    def test_raises_on_valid_gzip_but_invalid_json(self):
        p = self._peer_patch_path("peer_c", 1)
        _write_gz(p, b"this is valid gzip but not json {{{")
        with self.assertRaises(Exception):
            self.pm.patch_pack_apply(p, source_machine_id="peer_c", seq="000001")

    def test_sync_state_not_advanced_after_invalid_json_gzip(self):
        p = self._peer_patch_path("peer_c", 1)
        _write_gz(p, b"not json")
        try:
            self.pm.patch_pack_apply(p, source_machine_id="peer_c", seq="000001")
        except Exception:
            pass
        self.assertEqual(self._state("patch_applied_peer_c"), "000000")

    def test_raises_on_empty_file(self):
        p = self._peer_patch_path("peer_d", 1)
        p.write_bytes(b"")
        with self.assertRaises(Exception):
            self.pm.patch_pack_apply(p, source_machine_id="peer_d", seq="000001")

    def test_sync_state_not_advanced_after_empty_file(self):
        p = self._peer_patch_path("peer_d", 1)
        p.write_bytes(b"")
        try:
            self.pm.patch_pack_apply(p, source_machine_id="peer_d", seq="000001")
        except Exception:
            pass
        self.assertEqual(self._state("patch_applied_peer_d"), "000000")


# ---------------------------------------------------------------------------
# 2. Unsupported schema_version does not advance sync_state
# ---------------------------------------------------------------------------

class TestUnsupportedSchemaVersion(unittest.TestCase):
    """
    When row_patch_apply returns {"ok": False, "reason": "unsupported_schema_version"},
    patch_pack_apply must return that result without updating sync_state.
    The data rows in the patch must also not be applied.
    """

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self._tmp.name)
        self.remote = LocalRemote(self.root / "remote")
        self.pm = TestPatchManager(
            db_path=self.root / "test.sqlite",
            patch_dir=self.root / "patches",
            remote_root="fake_remote:/store",
            machine_id="machine_a",
            rclone_bin="fake-rclone",
            remote=self.remote,
        )

    def tearDown(self):
        self._tmp.cleanup()

    def _build_patch(self, schema_version, rows=None):
        return {
            "schema_version": schema_version,
            "machine_id": "peer_b",
            "seq": "000001",
            "exported_at": 100.0,
            "watermark": 0.0,
            "description": "test",
            "tables": {"items": rows or []},
        }

    def _peer_path(self, seq=1):
        p = self.root / "patches" / "machines" / "peer_b" / f"patch-{seq:06d}.json.gz"
        p.parent.mkdir(parents=True, exist_ok=True)
        return p

    def _state(self, key):
        conn = self.pm.conn_open()
        try:
            return self.pm.sync_state_get(conn, key, "000000")
        finally:
            conn.close()

    def _row_count(self):
        conn = self.pm.conn_open()
        try:
            return conn.execute("SELECT COUNT(*) FROM items").fetchone()[0]
        finally:
            conn.close()

    def test_result_is_not_ok(self):
        p = self._peer_path()
        _write_gz_patch(p, self._build_patch(schema_version=99))
        result = self.pm.patch_pack_apply(p, source_machine_id="peer_b", seq="000001")
        self.assertFalse(result.get("ok"))

    def test_reason_is_unsupported_schema_version(self):
        p = self._peer_path()
        _write_gz_patch(p, self._build_patch(schema_version=99))
        result = self.pm.patch_pack_apply(p, source_machine_id="peer_b", seq="000001")
        self.assertEqual(result.get("reason"), "unsupported_schema_version")

    def test_sync_state_not_advanced(self):
        p = self._peer_path()
        _write_gz_patch(p, self._build_patch(schema_version=99))
        self.pm.patch_pack_apply(p, source_machine_id="peer_b", seq="000001")
        self.assertEqual(self._state("patch_applied_peer_b"), "000000")

    def test_data_rows_not_written(self):
        rows = [{"id": "x1", "value": "should_not_appear", "updated_at": 100.0}]
        p = self._peer_path()
        _write_gz_patch(p, self._build_patch(schema_version=99, rows=rows))
        self.pm.patch_pack_apply(p, source_machine_id="peer_b", seq="000001")
        self.assertEqual(self._row_count(), 0)

    def test_second_apply_attempt_is_not_skipped_as_already_applied(self):
        # Because the first attempt did not advance state, the second attempt
        # must NOT be reported as "already_applied" — it is a fresh attempt.
        p = self._peer_path()
        _write_gz_patch(p, self._build_patch(schema_version=99))
        self.pm.patch_pack_apply(p, source_machine_id="peer_b", seq="000001")
        result = self.pm.patch_pack_apply(p, source_machine_id="peer_b", seq="000001")
        self.assertNotEqual(result.get("reason"), "already_applied")

    def test_correct_schema_version_applied_after_bad_attempt(self):
        # After a failed unsupported-schema attempt, a corrected patch with the
        # right schema version must apply successfully and advance state.
        p_bad = self._peer_path(seq=1)
        _write_gz_patch(p_bad, self._build_patch(schema_version=99))
        self.pm.patch_pack_apply(p_bad, source_machine_id="peer_b", seq="000001")

        rows = [{"id": "y1", "value": "hello", "updated_at": 200.0}]
        p_good = self._peer_path(seq=1)   # same seq — corrected reissue
        _write_gz_patch(p_good, self._build_patch(schema_version=PatchManager.PATCH_SCHEMA_VERSION, rows=rows))
        result = self.pm.patch_pack_apply(p_good, source_machine_id="peer_b", seq="000001")
        self.assertTrue(result.get("ok"))
        self.assertEqual(self._state("patch_applied_peer_b"), "000001")
        self.assertEqual(self._row_count(), 1)


# ---------------------------------------------------------------------------
# 3. patch_pack_create with no changed rows
# ---------------------------------------------------------------------------

class TestPatchPackCreateNoRows(unittest.TestCase):
    """
    patch_pack_create must behave predictably when there are no rows to export:
    - still writes a valid gzip file
    - still increments seq
    - returns ok=True
    - watermark_after == watermark_before (no rows → no advance)
    - empty rows list in the patch payload
    Multiple empty creates must each produce a distinct file with incrementing seqs.
    """

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self._tmp.name)
        self.remote = LocalRemote(self.root / "remote")
        self.pm = TestPatchManager(
            db_path=self.root / "test.sqlite",
            patch_dir=self.root / "patches",
            remote_root="fake_remote:/store",
            machine_id="machine_a",
            rclone_bin="fake-rclone",
            remote=self.remote,
        )

    def tearDown(self):
        self._tmp.cleanup()

    def _read_patch(self, patch_file):
        with gzip.open(patch_file, "rb") as fh:
            return json.loads(fh.read().decode("utf-8"))

    def test_returns_ok(self):
        result = self.pm.patch_pack_create()
        self.assertTrue(result["ok"])

    def test_file_created(self):
        result = self.pm.patch_pack_create()
        self.assertTrue(pathlib.Path(result["patch_file"]).exists())

    def test_file_is_valid_gzip_json(self):
        result = self.pm.patch_pack_create()
        data = self._read_patch(result["patch_file"])
        self.assertIsInstance(data, dict)

    def test_rows_list_is_empty(self):
        result = self.pm.patch_pack_create()
        data = self._read_patch(result["patch_file"])
        self.assertEqual(data["tables"]["items"], [])

    def test_seq_is_1(self):
        result = self.pm.patch_pack_create()
        self.assertEqual(result["seq"], "000001")

    def test_watermark_not_advanced_when_no_rows(self):
        # With no rows, exported_at falls back to the current watermark value.
        result = self.pm.patch_pack_create()
        self.assertEqual(result["watermark_before"], result["watermark_after"])

    def test_multiple_empty_creates_increment_seq(self):
        r1 = self.pm.patch_pack_create()
        r2 = self.pm.patch_pack_create()
        r3 = self.pm.patch_pack_create()
        self.assertEqual(r1["seq"], "000001")
        self.assertEqual(r2["seq"], "000002")
        self.assertEqual(r3["seq"], "000003")

    def test_multiple_empty_creates_produce_distinct_files(self):
        r1 = self.pm.patch_pack_create()
        r2 = self.pm.patch_pack_create()
        self.assertNotEqual(r1["patch_file"], r2["patch_file"])
        self.assertTrue(pathlib.Path(r1["patch_file"]).exists())
        self.assertTrue(pathlib.Path(r2["patch_file"]).exists())

    def test_empty_create_followed_by_row_create_exports_rows(self):
        # An empty create must not consume rows that arrive later.
        self.pm.patch_pack_create()
        insert_item(self.pm.db_path, "r1", "v1", 500.0)
        r2 = self.pm.patch_pack_create()
        data = self._read_patch(r2["patch_file"])
        ids = [row["id"] for row in data["tables"]["items"]]
        self.assertIn("r1", ids)

    def test_description_embedded_in_patch(self):
        result = self.pm.patch_pack_create(description="empty-export")
        data = self._read_patch(result["patch_file"])
        self.assertEqual(data["description"], "empty-export")


# ---------------------------------------------------------------------------
# 4. central_manifest.json with invalid JSON
# ---------------------------------------------------------------------------

class TestCorruptCentralManifest(unittest.TestCase):
    """
    If the local central_manifest.json contains invalid JSON:
    - central_manifest_load_local raises json.JSONDecodeError
    - central_manifest_pull raises when rclone succeeds but delivers corrupt JSON
      (the downloaded bytes are invalid JSON)
    - sync_push and sync_pull raise or propagate the error rather than silently
      swallowing it or writing bad state

    This behaviour is intentional: a corrupt manifest is an operator error that
    must surface loudly, not be silently ignored.
    """

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self._tmp.name)
        self.remote = LocalRemote(self.root / "remote")
        self.pm = TestPatchManager(
            db_path=self.root / "test.sqlite",
            patch_dir=self.root / "patches",
            remote_root="fake_remote:/store",
            machine_id="machine_a",
            rclone_bin="fake-rclone",
            remote=self.remote,
        )

    def tearDown(self):
        self._tmp.cleanup()

    def _corrupt_local_manifest(self, content=b"{ not valid json !!!"):
        self.pm.central_manifest_path.parent.mkdir(parents=True, exist_ok=True)
        self.pm.central_manifest_path.write_bytes(content)

    def test_load_local_raises_on_corrupt_file(self):
        self._corrupt_local_manifest()
        with self.assertRaises(json.JSONDecodeError):
            self.pm.central_manifest_load_local()

    def test_load_local_raises_on_empty_file(self):
        self._corrupt_local_manifest(b"")
        with self.assertRaises(Exception):
            self.pm.central_manifest_load_local()

    def test_load_local_raises_on_truncated_json(self):
        self._corrupt_local_manifest(b'{"machines": {')
        with self.assertRaises(json.JSONDecodeError):
            self.pm.central_manifest_load_local()

    def test_manifest_pull_raises_when_downloaded_file_is_corrupt(self):
        # Simulate rclone "succeeding" but writing corrupt JSON locally.
        def fake_copyto(src, dst, immutable=False):
            pathlib.Path(dst).parent.mkdir(parents=True, exist_ok=True)
            pathlib.Path(dst).write_bytes(b"totally not json")
            return {"ok": True, "returncode": 0, "stdout": "", "stderr": "", "cmd": []}

        with unittest.mock.patch.object(self.pm, "rclone_copyto", side_effect=fake_copyto):
            with self.assertRaises(Exception):
                self.pm.central_manifest_pull()

    def test_save_load_roundtrip_of_valid_manifest_not_affected(self):
        # Sanity check: valid manifest save/load still works after the above.
        manifest = self.pm.central_manifest_default()
        manifest["machines"]["peer_x"] = {"latest_seq": "000001"}
        self.pm.central_manifest_save_local(manifest)
        loaded = self.pm.central_manifest_load_local()
        self.assertIn("peer_x", loaded["machines"])

    def test_corrupt_manifest_does_not_silently_return_empty_machines(self):
        # Regression guard: must not swallow the error and pretend machines={}
        self._corrupt_local_manifest()
        raised = False
        try:
            self.pm.central_manifest_load_local()
        except Exception:
            raised = True
        self.assertTrue(raised, "Expected an exception but none was raised")


# ---------------------------------------------------------------------------
# 5. patch_pack_pull continues other machines if one machine has a bad patch
# ---------------------------------------------------------------------------

class TestPullContinuesOnBadPatch(unittest.TestCase):
    """
    patch_pack_pull iterates over all peer machines independently.
    When one machine's patch returns ok=False from row_patch_apply (e.g.
    unsupported schema version), the puller must:
    - record stopped_reason="apply_failed" for that machine
    - NOT advance sync_state for that machine
    - continue and successfully apply patches from the remaining machines

    Note: if patch_pack_apply raises an uncaught exception (e.g. malformed gzip),
    the whole pull aborts.  The "continues" guarantee applies only to the
    handled-failure path (row_patch_apply returning ok=False).
    """

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self._tmp.name)
        self.remote = LocalRemote(self.root / "remote")
        self.remote_root = "fake_remote:/patch_store"

    def tearDown(self):
        self._tmp.cleanup()

    def _manager(self, name, machine_id):
        return TestPatchManager(
            db_path=self.root / f"{name}.sqlite",
            patch_dir=self.root / f"{name}_patches",
            remote_root=self.remote_root,
            machine_id=machine_id,
            rclone_bin="fake-rclone",
            remote=self.remote,
        )

    def _build_bad_schema_patch(self, machine_id, seq=1, rows=None):
        return {
            "schema_version": 99,       # unsupported
            "machine_id": machine_id,
            "seq": f"{seq:06d}",
            "exported_at": 100.0,
            "watermark": 0.0,
            "description": "bad schema",
            "tables": {"items": rows or []},
        }

    def test_bad_machine_reported_as_apply_failed(self):
        puller = self._manager("c", "machine_c")
        bad_peer = "machine_a"

        # Place bad patch in remote.
        remote_patch = self.remote.storage_dir / "patch_store" / "machines" / bad_peer / "patch-000001.json.gz"
        _write_gz_patch(remote_patch, self._build_bad_schema_patch(bad_peer))

        manifest = {
            "patch_schema_version": PatchManager.PATCH_SCHEMA_VERSION,
            "remote_root": self.remote_root,
            "machines": {bad_peer: {"latest_seq": "000001", "updated_at": 1.0}},
            "updated_at": 1.0,
        }
        (self.remote.storage_dir / "patch_store" / "central_manifest.json").parent.mkdir(parents=True, exist_ok=True)
        (self.remote.storage_dir / "patch_store" / "central_manifest.json").write_text(
            json.dumps(manifest), encoding="utf-8"
        )

        result = puller.sync_pull()
        self.assertTrue(result["ok"])
        self.assertEqual(result["per_machine"][bad_peer]["stopped_reason"], "apply_failed")

    def test_sync_state_not_advanced_for_bad_machine(self):
        puller = self._manager("c", "machine_c")
        bad_peer = "machine_a"

        remote_patch = self.remote.storage_dir / "patch_store" / "machines" / bad_peer / "patch-000001.json.gz"
        _write_gz_patch(remote_patch, self._build_bad_schema_patch(bad_peer))

        manifest = {
            "patch_schema_version": PatchManager.PATCH_SCHEMA_VERSION,
            "remote_root": self.remote_root,
            "machines": {bad_peer: {"latest_seq": "000001", "updated_at": 1.0}},
            "updated_at": 1.0,
        }
        manifest_path = self.remote.storage_dir / "patch_store" / "central_manifest.json"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        puller.sync_pull()
        conn = puller.conn_open()
        try:
            state = puller.sync_state_get(conn, f"patch_applied_{bad_peer}", "000000")
        finally:
            conn.close()
        self.assertEqual(state, "000000")

    def test_good_machine_still_applied_when_earlier_peer_fails(self):
        puller = self._manager("c", "machine_c")
        bad_peer = "machine_a"    # 'a' sorts before 'b'
        good_peer = "machine_b"

        # machine_a: bad schema version patch
        bad_dir = self.remote.storage_dir / "patch_store" / "machines" / bad_peer
        _write_gz_patch(bad_dir / "patch-000001.json.gz", self._build_bad_schema_patch(bad_peer))

        # machine_b: valid patch with a real row
        good_dir = self.remote.storage_dir / "patch_store" / "machines" / good_peer
        good_dir.mkdir(parents=True, exist_ok=True)
        good_patch = {
            "schema_version": PatchManager.PATCH_SCHEMA_VERSION,
            "machine_id": good_peer,
            "seq": "000001",
            "exported_at": 200.0,
            "watermark": 0.0,
            "description": "good patch",
            "tables": {"items": [{"id": "g1", "value": "from_b", "updated_at": 200.0}]},
        }
        _write_gz_patch(good_dir / "patch-000001.json.gz", good_patch)

        manifest = {
            "patch_schema_version": PatchManager.PATCH_SCHEMA_VERSION,
            "remote_root": self.remote_root,
            "machines": {
                bad_peer:  {"latest_seq": "000001", "updated_at": 1.0},
                good_peer: {"latest_seq": "000001", "updated_at": 1.0},
            },
            "updated_at": 1.0,
        }
        manifest_path = self.remote.storage_dir / "patch_store" / "central_manifest.json"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        result = puller.sync_pull()

        # machine_a failed, machine_b succeeded
        self.assertEqual(result["per_machine"][bad_peer]["stopped_reason"], "apply_failed")
        self.assertEqual(result["per_machine"][good_peer]["applied"], 1)

        # Row from machine_b is in the DB
        conn = puller.conn_open()
        try:
            row = conn.execute("SELECT value FROM items WHERE id='g1'").fetchone()
        finally:
            conn.close()
        self.assertIsNotNone(row)
        self.assertEqual(row["value"], "from_b")

    def test_good_machine_applied_when_later_peer_also_fails(self):
        puller = self._manager("c", "machine_c")
        good_peer = "machine_a"   # 'a' sorts before 'b'
        bad_peer = "machine_b"

        # machine_a: valid patch
        good_dir = self.remote.storage_dir / "patch_store" / "machines" / good_peer
        good_dir.mkdir(parents=True, exist_ok=True)
        good_patch = {
            "schema_version": PatchManager.PATCH_SCHEMA_VERSION,
            "machine_id": good_peer,
            "seq": "000001",
            "exported_at": 100.0,
            "watermark": 0.0,
            "description": "good",
            "tables": {"items": [{"id": "a1", "value": "from_a", "updated_at": 100.0}]},
        }
        _write_gz_patch(good_dir / "patch-000001.json.gz", good_patch)

        # machine_b: bad schema
        bad_dir = self.remote.storage_dir / "patch_store" / "machines" / bad_peer
        _write_gz_patch(bad_dir / "patch-000001.json.gz", self._build_bad_schema_patch(bad_peer))

        manifest = {
            "patch_schema_version": PatchManager.PATCH_SCHEMA_VERSION,
            "remote_root": self.remote_root,
            "machines": {
                good_peer: {"latest_seq": "000001", "updated_at": 1.0},
                bad_peer:  {"latest_seq": "000001", "updated_at": 1.0},
            },
            "updated_at": 1.0,
        }
        manifest_path = self.remote.storage_dir / "patch_store" / "central_manifest.json"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        result = puller.sync_pull()

        self.assertEqual(result["per_machine"][bad_peer]["stopped_reason"], "apply_failed")
        self.assertEqual(result["per_machine"][good_peer]["applied"], 1)

        conn = puller.conn_open()
        try:
            row = conn.execute("SELECT value FROM items WHERE id='a1'").fetchone()
        finally:
            conn.close()
        self.assertIsNotNone(row)
        self.assertEqual(row["value"], "from_a")

    def test_total_applied_counts_only_successful_machines(self):
        puller = self._manager("c", "machine_c")

        bad_dir = self.remote.storage_dir / "patch_store" / "machines" / "machine_a"
        _write_gz_patch(bad_dir / "patch-000001.json.gz", self._build_bad_schema_patch("machine_a"))

        good_dir = self.remote.storage_dir / "patch_store" / "machines" / "machine_b"
        good_dir.mkdir(parents=True, exist_ok=True)
        good_patch = {
            "schema_version": PatchManager.PATCH_SCHEMA_VERSION,
            "machine_id": "machine_b",
            "seq": "000001",
            "exported_at": 200.0,
            "watermark": 0.0,
            "description": "good",
            "tables": {"items": [{"id": "b1", "value": "v", "updated_at": 200.0}]},
        }
        _write_gz_patch(good_dir / "patch-000001.json.gz", good_patch)

        manifest = {
            "patch_schema_version": PatchManager.PATCH_SCHEMA_VERSION,
            "remote_root": self.remote_root,
            "machines": {
                "machine_a": {"latest_seq": "000001", "updated_at": 1.0},
                "machine_b": {"latest_seq": "000001", "updated_at": 1.0},
            },
            "updated_at": 1.0,
        }
        manifest_path = self.remote.storage_dir / "patch_store" / "central_manifest.json"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        result = puller.sync_pull()
        self.assertEqual(result["total_applied"], 1)


# ---------------------------------------------------------------------------
# 6. machine_id collision
# ---------------------------------------------------------------------------

class TestMachineIdCollision(unittest.TestCase):
    """
    If two separate machines are assigned the same machine_id (e.g. by copying
    machine_id.txt from one machine to another), they silently stop syncing with
    each other.

    Specific consequences documented here:
    - patch_pack_apply treats any patch whose machine_id matches self.machine_id
      as "own_patch" and skips it (ok=True, skipped=True, reason="own_patch").
    - The central manifest is keyed by machine_id, so the later push overwrites
      the earlier one's entry; the earlier machine's patches are unreachable via
      the manifest.
    - The colliding machines' DBs never converge with each other via sync_pull,
      even though third-party machines can distribute their individual patches.

    These tests document the observable behaviour so that it can be detected and
    operators can be alerted (e.g. by checking machine_id uniqueness at startup).
    """

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self._tmp.name)
        self.remote = LocalRemote(self.root / "remote")
        self.remote_root = "fake_remote:/patch_store"

    def tearDown(self):
        self._tmp.cleanup()

    def _manager(self, name, machine_id):
        return TestPatchManager(
            db_path=self.root / f"{name}.sqlite",
            patch_dir=self.root / f"{name}_patches",
            remote_root=self.remote_root,
            machine_id=machine_id,
            rclone_bin="fake-rclone",
            remote=self.remote,
        )

    def test_own_patch_skipped_when_machine_ids_collide(self):
        # Machine B has same id as machine A; A's patch looks like "own" to B.
        a = self._manager("a", "shared_id")
        b = self._manager("b", "shared_id")

        insert_item(a.db_path, "a1", "from_a", 100.0)
        a.sync_push("a data")

        # Build a pull by B: it will see patches from "shared_id" — its own id.
        result = b.sync_pull()
        self.assertTrue(result["ok"])
        # shared_id is B's own machine_id, so it must be skipped entirely.
        self.assertNotIn("shared_id", result["per_machine"])

    def test_colliding_machines_do_not_converge(self):
        a = self._manager("a", "shared_id")
        b = self._manager("b", "shared_id")

        insert_item(a.db_path, "a_only", "from_a", 100.0)
        insert_item(b.db_path, "b_only", "from_b", 200.0)

        a.sync_push("a")
        b.sync_push("b")    # overwrites manifest entry for shared_id

        a.sync_pull()
        b.sync_pull()

        a_items = read_items(a.db_path)
        b_items = read_items(b.db_path)

        # Neither sees the other's rows — DBs diverge.
        self.assertNotIn("b_only", a_items)
        self.assertNotIn("a_only", b_items)

    def test_manifest_entry_overwritten_by_later_push(self):
        a = self._manager("a", "shared_id")
        b = self._manager("b", "shared_id")

        insert_item(a.db_path, "x", "a_val", 100.0)
        insert_item(b.db_path, "y", "b_val", 200.0)

        a.sync_push("a push")       # manifest: shared_id -> seq 000001
        b.sync_push("b push")       # manifest: shared_id -> seq 000001 (overwritten)

        manifest = self.remote.read_json("patch_store/central_manifest.json")
        # Only one entry exists — second push clobbers the first.
        self.assertEqual(len(manifest["machines"]), 1)
        self.assertIn("shared_id", manifest["machines"])

    def test_third_party_machine_sees_only_latest_collision_patch(self):
        a = self._manager("a", "shared_id")
        b = self._manager("b", "shared_id")
        c = self._manager("c", "machine_c")

        insert_item(a.db_path, "a1", "from_a", 100.0)
        insert_item(b.db_path, "b1", "from_b", 200.0)

        a.sync_push("a")
        b.sync_push("b")    # b's patch-000001 overwrites the manifest entry

        c.sync_pull()

        c_items = read_items(c.db_path)
        # C can only receive patches from whoever pushed last under shared_id.
        # It cannot see both; exactly one of the two should be present.
        only_a = "a1" in c_items and "b1" not in c_items
        only_b = "b1" in c_items and "a1" not in c_items
        self.assertTrue(only_a or only_b, f"Expected exactly one collision side; got {c_items}")

    def test_unique_machine_ids_are_not_affected(self):
        # Sanity: two machines with distinct ids converge normally.
        a = self._manager("a", "machine_a")
        b = self._manager("b", "machine_b")

        insert_item(a.db_path, "a1", "from_a", 100.0)
        insert_item(b.db_path, "b1", "from_b", 200.0)

        a.sync_push("a")
        b.sync_push("b")
        a.sync_pull()
        b.sync_pull()

        self.assertIn("b1", read_items(a.db_path))
        self.assertIn("a1", read_items(b.db_path))


if __name__ == "__main__":
    unittest.main(verbosity=2)
