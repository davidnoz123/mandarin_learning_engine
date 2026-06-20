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


if __name__ == "__main__":
    unittest.main(verbosity=2)
