#!/usr/bin/env python3
"""update_agents.py — Thin shim. Delegates to the real script in nielsoln_agent_standards.

Run from this repo's root:
    python update_agents.py              # write AGENTS.md
    python update_agents.py --dry-run    # preview only
    python update_agents.py --output PATH

Requires AGENT_STANDARDS_DIR in locals.txt pointing to the nielsoln_agent_standards repo.
"""
import sys
import os


def _read_locals():
    result = {}
    path = os.path.join(os.getcwd(), "locals.txt")
    if not os.path.exists(path):
        return result
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, _, v = line.partition("=")
                result[k.strip()] = v.strip()
    return result


def main():
    locals_data = _read_locals()
    standards_dir = locals_data.get("AGENT_STANDARDS_DIR", "")
    if not standards_dir:
        print("ERROR: AGENT_STANDARDS_DIR not found in locals.txt", file=sys.stderr)
        print("Add a line like:", file=sys.stderr)
        print("  AGENT_STANDARDS_DIR=..\\nielsoln_agent_standards", file=sys.stderr)
        sys.exit(1)
    if not os.path.isabs(standards_dir):
        standards_dir = os.path.normpath(os.path.join(os.getcwd(), standards_dir))
    real_script = os.path.join(standards_dir, "update_agents.py")
    if not os.path.exists(real_script):
        print(f"ERROR: update_agents.py not found at {real_script}", file=sys.stderr)
        print(f"Check AGENT_STANDARDS_DIR in locals.txt: {standards_dir}", file=sys.stderr)
        sys.exit(1)
    sys.path.insert(0, standards_dir)
    import update_agents as _ua  # noqa: E402
    raise SystemExit(_ua.main(sys.argv[1:]))


if __name__ == "__main__":
    main()
