#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Workspace self-check for prompt/skill/runtime pipeline.

Checks:
1) prompt/skill files exist, are UTF-8 readable, and contain key markers.
2) prompt/skill files have UTF-8 BOM (PowerShell 5.1 compatibility warning if missing).
3) data.py smoke run in strict mode and snapshot diagnostics validation.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
import sys
from typing import Any, Dict, List


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
LOG_DIR = os.path.join(ROOT, "logs")

TARGET_FILES = [
    "SKILL.md",
    "prompt/0_master_prompt.txt",
    "prompt/1_all_select.txt",
    "prompt/2_prompt.txt",
    "scripts/decision_guard.py",
]

REQUIRED_MARKERS: Dict[str, List[str]] = {
    "SKILL.md": ["Binance.US", "Step 0", "Step 9", "30~60", "decision_guard.py"],
    "prompt/0_master_prompt.txt": ["WATCHLIST_SYMBOLS", "SKILL.md", "data.py", "GUARD_PY"],
    "prompt/1_all_select.txt": ["WATCHLIST_SYMBOLS", "/api/v3/ticker/24hr", "/api/v3/depth"],
    "prompt/2_prompt.txt": ["SYMBOLS=", "SKILL_FILE=", "LOG_DIR=", "GUARD_PY"],
    "scripts/decision_guard.py": ["def compute_guard(", "ENFORCED_WAIT", "--inplace"],
}


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def has_utf8_bom(path: str) -> bool:
    with open(path, "rb") as f:
        head = f.read(3)
    return head == b"\xef\xbb\xbf"


def read_utf8(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def deep_get(obj: Any, path: str, default: Any = None) -> Any:
    cur = obj
    for part in path.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return default
    return cur


def run_data_smoke(symbol: str) -> Dict[str, Any]:
    run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_json = os.path.join(LOG_DIR, f"snapshot_{symbol}_selfcheck_{run_id}.json")
    cmd = [
        sys.executable,
        os.path.join(ROOT, "scripts", "data.py"),
        "--symbol",
        symbol,
        "--intervals",
        "1m",
        "3m",
        "5m",
        "15m",
        "30m",
        "1h",
        "--kline-limit",
        "240",
        "--hist-period",
        "15m",
        "--hist-limit",
        "128",
        "--depth",
        "120",
        "--trades-limit",
        "500",
        "--out",
        out_json,
        "--strict",
        "--no-text",
    ]
    proc = subprocess.run(
        cmd,
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    res: Dict[str, Any] = {
        "command": cmd,
        "return_code": proc.returncode,
        "stdout_tail": (proc.stdout or "")[-2000:],
        "stderr_tail": (proc.stderr or "")[-2000:],
        "snapshot_path": out_json,
    }
    if not os.path.exists(out_json):
        res["snapshot_exists"] = False
        return res
    res["snapshot_exists"] = True
    with open(out_json, "r", encoding="utf-8") as f:
        snap = json.load(f)
    errors = snap.get("errors", {}) if isinstance(snap, dict) else {}
    errors_count = len(errors) if isinstance(errors, dict) else 0
    self_check = deep_get(snap, "diagnostics.self_check", {}) or {}
    res["snapshot_summary"] = {
        "errors_count": errors_count,
        "self_check_status": self_check.get("status"),
        "self_check_failed_critical": self_check.get("failed_critical", []),
        "self_check_failed": self_check.get("failed", []),
        "symbol": deep_get(snap, "meta.symbol"),
        "server_time": deep_get(snap, "meta.server_time"),
    }
    return res


def run_guard_smoke() -> Dict[str, Any]:
    cmd = [
        sys.executable,
        os.path.join(ROOT, "scripts", "decision_guard.py"),
        "--help",
    ]
    proc = subprocess.run(
        cmd,
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    return {
        "command": cmd,
        "return_code": proc.returncode,
        "stdout_tail": (proc.stdout or "")[-2000:],
        "stderr_tail": (proc.stderr or "")[-2000:],
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Self-check for prompt/skill/data runtime.")
    ap.add_argument("--symbol", default="BTCUSDT", help="Smoke symbol, default BTCUSDT")
    ap.add_argument("--fail-on-warn", action="store_true", help="Exit non-zero when status is WARN")
    args = ap.parse_args()

    os.makedirs(LOG_DIR, exist_ok=True)
    report: Dict[str, Any] = {
        "run_time_utc": utc_now(),
        "status": "PASS",
        "files": {},
        "issues": {"fail": [], "warn": []},
        "smoke": {},
        "guard_smoke": {},
    }

    for rel in TARGET_FILES:
        path = os.path.join(ROOT, rel)
        file_rec: Dict[str, Any] = {"path": path, "exists": os.path.exists(path)}
        report["files"][rel] = file_rec
        if not file_rec["exists"]:
            report["issues"]["fail"].append(f"missing_file:{rel}")
            continue
        try:
            text = read_utf8(path)
            file_rec["utf8_readable"] = True
            file_rec["length"] = len(text)
        except Exception as ex:
            file_rec["utf8_readable"] = False
            file_rec["read_error"] = str(ex)
            report["issues"]["fail"].append(f"utf8_read_error:{rel}:{ex}")
            continue

        bom = has_utf8_bom(path)
        file_rec["utf8_bom"] = bom
        bom_required = rel.endswith(".md") or rel.startswith("prompt/")
        if bom_required and not bom:
            report["issues"]["warn"].append(f"no_utf8_bom:{rel}")

        for marker in REQUIRED_MARKERS.get(rel, []):
            if marker not in text:
                report["issues"]["fail"].append(f"missing_marker:{rel}:{marker}")

    smoke = run_data_smoke((args.symbol or "").strip().upper() or "BTCUSDT")
    report["smoke"] = smoke

    if smoke.get("return_code") != 0:
        report["issues"]["fail"].append(f"data_smoke_exit_nonzero:{smoke.get('return_code')}")
    if not smoke.get("snapshot_exists"):
        report["issues"]["fail"].append("data_smoke_no_snapshot")

    sm_sum = smoke.get("snapshot_summary", {})
    if sm_sum:
        sc_status = str(sm_sum.get("self_check_status") or "UNKNOWN").upper()
        if sc_status == "FAIL":
            report["issues"]["fail"].append("snapshot_self_check_fail")
        elif sc_status == "WARN":
            report["issues"]["warn"].append("snapshot_self_check_warn")
        if (sm_sum.get("errors_count") or 0) > 0:
            report["issues"]["warn"].append(f"snapshot_errors_count:{sm_sum.get('errors_count')}")

    guard_smoke = run_guard_smoke()
    report["guard_smoke"] = guard_smoke
    if guard_smoke.get("return_code") != 0:
        report["issues"]["fail"].append(f"guard_smoke_exit_nonzero:{guard_smoke.get('return_code')}")

    if report["issues"]["fail"]:
        report["status"] = "FAIL"
    elif report["issues"]["warn"]:
        report["status"] = "WARN"

    now = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_json = os.path.join(LOG_DIR, f"self_check_{now}.json")
    out_txt = os.path.join(LOG_DIR, f"self_check_{now}.txt")
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    lines = [
        f"status={report['status']}",
        f"fail_count={len(report['issues']['fail'])}",
        f"warn_count={len(report['issues']['warn'])}",
        f"report_json={out_json}",
    ]
    if report["issues"]["fail"]:
        lines.append("fail_issues:")
        lines.extend([f"- {x}" for x in report["issues"]["fail"]])
    if report["issues"]["warn"]:
        lines.append("warn_issues:")
        lines.extend([f"- {x}" for x in report["issues"]["warn"]])

    with open(out_txt, "w", encoding="utf-8-sig") as f:
        f.write("\n".join(lines) + "\n")

    print(f"status={report['status']}")
    print(f"report_json={out_json}")
    print(f"report_txt={out_txt}")

    if report["status"] == "FAIL":
        return 2
    if report["status"] == "WARN" and args.fail_on_warn:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
