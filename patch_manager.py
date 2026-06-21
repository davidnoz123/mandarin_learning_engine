
"""
Generic rclone-backed patch manager plus Mandarin-specific wrapper.

Split:
- PatchManager: generic reusable SQLite/rclone row-patch infrastructure.

Constraints:
- No third-party Python dependencies.
- rclone is the only external runtime dependency.
- Imports are inside routines where practical.
- Use fully-qualified typing names if typing is later added.
"""


class PatchManager:
    """
    Generic reusable patch manager.

    Responsibilities:
    - stable machine_id
    - local sync_state table
    - gzip JSON patch files
    - immutable rclone uploads
    - central manifest pull/push
    - per-peer patch pull/apply
    - idempotent application tracking

    Subclasses must override:
    - rows_export_since(conn, watermark)
    - row_patch_apply(conn, patch)
    """

    PATCH_SCHEMA_VERSION = 1

    def __init__(self, db_path, patch_dir, remote_root, machine_id=None, rclone_bin="rclone"):
        import pathlib

        self.db_path = pathlib.Path(db_path)
        self.patch_dir = pathlib.Path(patch_dir)
        self.remote_root = str(remote_root).rstrip("/")
        self.rclone_bin = str(rclone_bin)

        self.patch_dir.mkdir(parents=True, exist_ok=True)

        self.machine_id = machine_id or self.machine_id_get()
        self.local_machine_dir = self.patch_dir / "machines" / self.machine_id
        self.local_machine_dir.mkdir(parents=True, exist_ok=True)

        self.central_manifest_path = self.patch_dir / "central_manifest.json"

    def schema_ensure(self, conn):
        """Ensure only the generic sync schema."""
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sync_state (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        conn.commit()

    def rows_export_since(self, conn, watermark):
        """Project-specific export hook. Subclasses must override."""
        raise NotImplementedError("Subclasses must implement rows_export_since()")

    def row_patch_apply(self, conn, patch):
        """Project-specific apply hook. Subclasses must override."""
        raise NotImplementedError("Subclasses must implement row_patch_apply()")

    def machine_id_get(self):
        """
        Return a stable filesystem-safe machine id.

        Priority:
        1. patch_dir/machine_id.txt
        2. MACHINE_ID environment variable
        3. generated UUID-based id

        Hostname fallback is deliberately avoided because it can collide.
        """
        import os
        import pathlib
        import uuid

        id_path = pathlib.Path(self.patch_dir) / "machine_id.txt"

        if id_path.exists():
            return self.machine_id_validate(id_path.read_text(encoding="utf-8").strip())

        env_mid = os.environ.get("MACHINE_ID", "").strip()
        if env_mid:
            mid = self.machine_id_validate(env_mid)
            id_path.write_text(mid, encoding="utf-8")
            return mid

        mid = "machine_" + uuid.uuid4().hex[:16]
        id_path.write_text(mid, encoding="utf-8")
        return mid

    @classmethod
    def machine_id_validate(cls, machine_id):
        import re

        if not machine_id:
            raise ValueError("machine_id is empty")
        if not re.match(r"^[A-Za-z0-9_-]+$", machine_id):
            raise ValueError("machine_id must contain only letters, digits, underscores, and hyphens")
        return machine_id

    def conn_open(self):
        import sqlite3

        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=10000")
        conn.execute("PRAGMA foreign_keys=ON")
        self.schema_ensure(conn)
        return conn

    def sync_state_get(self, conn, key, default=None):
        row = conn.execute("SELECT value FROM sync_state WHERE key = ?", (key,)).fetchone()
        return default if row is None else row["value"]

    def sync_state_set(self, conn, key, value):
        conn.execute(
            """
            INSERT INTO sync_state (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, str(value)),
        )

    def patch_pack_create(self, description=""):
        """Create a local gzip JSON patch from rows changed since watermark."""
        import gzip
        import json

        conn = self.conn_open()
        try:
            with conn:
                conn.execute("BEGIN DEFERRED")
                watermark = float(self.sync_state_get(conn, "patch_watermark", "0.0"))

                patch = self.rows_export_since(conn, watermark)
                patch["schema_version"] = self.PATCH_SCHEMA_VERSION
                patch["machine_id"] = self.machine_id
                patch["description"] = description
                # Use explicit None check — 0.0 is a valid exported_at (start-of-epoch
                # watermark) and must not be treated as "missing" by the 'or' operator.
                if patch.get("exported_at") is None:
                    patch["exported_at"] = self.time_now()

                seq = self.local_patch_seq_next()
                patch["seq"] = f"{seq:06d}"

                patch_path = self.local_machine_dir / f"patch-{seq:06d}.json.gz"
                payload = json.dumps(patch, ensure_ascii=False, sort_keys=True).encode("utf-8")
                with gzip.open(patch_path, "wb") as fh:
                    fh.write(payload)

                self.sync_state_set(conn, "patch_watermark", float(patch["exported_at"]))
                self.sync_state_set(conn, "patch_last_seq", f"{seq:06d}")

            return {
                "ok": True,
                "operation": "patch_pack_create",
                "machine_id": self.machine_id,
                "seq": f"{seq:06d}",
                "patch_file": str(patch_path),
                "watermark_before": watermark,
                "watermark_after": float(patch["exported_at"]),
                "bytes": patch_path.stat().st_size,
            }
        finally:
            conn.close()

    def patch_pack_apply(self, patch_path, source_machine_id=None, seq=None):
        """Apply one gzip JSON patch transactionally and idempotently."""
        import gzip
        import json
        import pathlib

        patch_path = pathlib.Path(patch_path)
        with gzip.open(patch_path, "rb") as fh:
            patch = json.loads(fh.read().decode("utf-8"))

        patch_machine = source_machine_id or patch.get("machine_id")
        patch_seq = seq or patch.get("seq")

        if not patch_machine:
            return {"ok": False, "reason": "missing_patch_machine_id"}
        if not patch_seq:
            return {"ok": False, "reason": "missing_patch_seq"}
        if patch_machine == self.machine_id:
            return {"ok": True, "skipped": True, "reason": "own_patch", "machine_id": patch_machine, "seq": patch_seq}

        state_key = f"patch_applied_{patch_machine}"
        conn = self.conn_open()
        try:
            current = self.sync_state_get(conn, state_key, "000000")
            if int(current) >= int(patch_seq):
                return {"ok": True, "skipped": True, "reason": "already_applied", "machine_id": patch_machine, "seq": patch_seq}

            with conn:
                result = self.row_patch_apply(conn, patch)
                if not result.get("ok"):
                    return result
                self.sync_state_set(conn, state_key, patch_seq)

            return {
                "ok": True,
                "operation": "patch_pack_apply",
                "machine_id": patch_machine,
                "seq": patch_seq,
                "apply_result": result,
            }
        finally:
            conn.close()

    def local_patch_seq_next(self):
        existing = sorted(self.local_machine_dir.glob("patch-*.json.gz"))
        if not existing:
            return 1
        try:
            return int(existing[-1].name.split("-")[1].split(".")[0]) + 1
        except Exception:
            return len(existing) + 1

    def rclone_run(self, *args):
        import subprocess

        return subprocess.run([self.rclone_bin, *args], capture_output=True, text=True)

    def rclone_copyto(self, src, dst, immutable=False):
        args = ["copyto", str(src), str(dst)]
        if immutable:
            args.append("--immutable")
        result = self.rclone_run(*args)
        return {
            "ok": result.returncode == 0,
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "cmd": [self.rclone_bin, *args],
        }

    def central_manifest_default(self):
        return {
            "patch_schema_version": self.PATCH_SCHEMA_VERSION,
            "remote_root": self.remote_root,
            "machines": {},
            "updated_at": self.time_now(),
        }

    def central_manifest_load_local(self):
        import json

        if not self.central_manifest_path.exists():
            return self.central_manifest_default()
        return json.loads(self.central_manifest_path.read_text(encoding="utf-8"))

    def central_manifest_save_local(self, manifest):
        import json

        manifest["updated_at"] = self.time_now()
        self.central_manifest_path.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False, sort_keys=True),
            encoding="utf-8",
        )

    def central_manifest_pull(self):
        remote = f"{self.remote_root}/central_manifest.json"
        result = self.rclone_copyto(remote, self.central_manifest_path)

        if not result["ok"]:
            manifest = self.central_manifest_default()
            self.central_manifest_save_local(manifest)
            return {"ok": True, "created_default": True, "manifest": manifest, "rclone": result}

        return {"ok": True, "created_default": False, "manifest": self.central_manifest_load_local(), "rclone": result}

    def central_manifest_push(self, manifest=None):
        manifest = manifest or self.central_manifest_load_local()
        self.central_manifest_save_local(manifest)
        remote = f"{self.remote_root}/central_manifest.json"
        result = self.rclone_copyto(self.central_manifest_path, remote)
        return {"ok": result["ok"], "manifest": manifest, "rclone": result}

    def patch_pack_push(self):
        """Upload latest local patch with --immutable, then update central manifest."""
        patches = sorted(self.local_machine_dir.glob("patch-*.json.gz"))
        if not patches:
            return {"ok": False, "reason": "no_local_patches"}

        latest = patches[-1]
        seq = latest.name.split("-")[1].split(".")[0]

        remote_patch = f"{self.remote_root}/machines/{self.machine_id}/{latest.name}"
        patch_upload = self.rclone_copyto(latest, remote_patch, immutable=True)

        manifest_pull = self.central_manifest_pull()
        manifest = manifest_pull["manifest"]
        manifest.setdefault("machines", {})[self.machine_id] = {
            "latest_seq": seq,
            "updated_at": self.time_now(),
        }
        manifest_push = self.central_manifest_push(manifest)

        return {
            "ok": bool(patch_upload["ok"] and manifest_push["ok"]),
            "operation": "patch_pack_push",
            "machine_id": self.machine_id,
            "seq": seq,
            "patch_upload": patch_upload,
            "manifest_pull": manifest_pull,
            "manifest_push": manifest_push,
        }

    def patch_pack_pull(self):
        """Pull/apply unapplied patches for all machines in central manifest."""
        manifest_pull = self.central_manifest_pull()
        manifest = manifest_pull["manifest"]

        total_applied = 0
        per_machine = {}

        for machine_id, info in sorted(manifest.get("machines", {}).items()):
            if machine_id == self.machine_id:
                continue

            remote_head = int(info.get("latest_seq") or 0)
            if remote_head <= 0:
                continue

            conn = self.conn_open()
            try:
                state_key = f"patch_applied_{machine_id}"
                local_seq = int(self.sync_state_get(conn, state_key, "000000"))
            finally:
                conn.close()

            if local_seq >= remote_head:
                per_machine[machine_id] = {
                    "already_current": True,
                    "local_seq": f"{local_seq:06d}",
                    "remote_head": f"{remote_head:06d}",
                }
                continue

            local_peer_dir = self.patch_dir / "machines" / machine_id
            local_peer_dir.mkdir(parents=True, exist_ok=True)

            applied = 0
            stopped_reason = None

            for seq in range(local_seq + 1, remote_head + 1):
                patch_name = f"patch-{seq:06d}.json.gz"
                remote_patch = f"{self.remote_root}/machines/{machine_id}/{patch_name}"
                local_patch = local_peer_dir / patch_name

                download = self.rclone_copyto(remote_patch, local_patch)
                if not download["ok"]:
                    stopped_reason = "download_failed"
                    break

                apply_result = self.patch_pack_apply(local_patch, source_machine_id=machine_id, seq=f"{seq:06d}")
                if not apply_result.get("ok"):
                    stopped_reason = "apply_failed"
                    per_machine[machine_id] = {
                        "remote_head": f"{remote_head:06d}",
                        "local_seq_before": f"{local_seq:06d}",
                        "applied": applied,
                        "stopped_reason": stopped_reason,
                        "apply_result": apply_result,
                    }
                    break

                applied += 1
                total_applied += 1

            if machine_id not in per_machine:
                per_machine[machine_id] = {
                    "remote_head": f"{remote_head:06d}",
                    "local_seq_before": f"{local_seq:06d}",
                    "applied": applied,
                    "stopped_reason": stopped_reason,
                }

        return {
            "ok": True,
            "operation": "patch_pack_pull",
            "my_machine": self.machine_id,
            "total_applied": total_applied,
            "per_machine": per_machine,
        }

    def sync_push(self, description=""):
        created = self.patch_pack_create(description=description)
        if not created.get("ok"):
            return {"ok": False, "create": created, "push": None}
        pushed = self.patch_pack_push()
        return {"ok": bool(created.get("ok") and pushed.get("ok")), "create": created, "push": pushed}

    def sync_pull(self):
        return self.patch_pack_pull()

    def sync_all(self, description=""):
        pulled = self.sync_pull()
        pushed = self.sync_push(description=description)
        return {"ok": bool(pulled.get("ok") and pushed.get("ok")), "pull": pulled, "push": pushed}

    @classmethod
    def time_now(cls):
        import time

        return time.time()

