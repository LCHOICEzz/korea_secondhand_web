#!/usr/bin/env python3
import argparse
import json
import re
from pathlib import Path
from typing import Dict, List, Tuple

BRAND_ORDER = ["더로랑", "라플라", "라벨르블랑", "헤이에스", "리즈"]

REGION_RE = re.compile(r"region scan progress (\d+)/(\d+), ongoing candidates=(\d+)")
DETAIL_RE = re.compile(r"detail progress (\d+)/(\d+)")
FINALIZE_RE = re.compile(r"finalize progress (\d+)/(\d+)")
FIX_RE = re.compile(r"fix progress (\d+)/(\d+)")


def latest_batch_log(log_dir: Path) -> Path:
    logs = sorted(log_dir.glob("karrot_batch_*.log"))
    return logs[-1] if logs else Path()


def parse_log(lines: List[str]) -> Dict:
    state: Dict = {
        "last_brand": "",
        "last_stage": "",
        "progress": "",
        "last_line": lines[-1].strip() if lines else "",
    }
    for line in lines:
        if "run_brand_pipeline.py --brand" in line:
            m = re.search(r"--brand\s+(\S+)", line)
            if m:
                state["last_brand"] = m.group(1)
                if "--mode scrape" in line:
                    state["last_stage"] = "scrape"
                elif "--mode postprocess" in line:
                    state["last_stage"] = "postprocess"
        if "multi_region_karrot_report.py" in line:
            state["last_stage"] = "scrape"
        if "fix_multi_region_output.py" in line:
            state["last_stage"] = "fix"
        for regex, stage in ((REGION_RE, "region_scan"), (DETAIL_RE, "detail"), (FINALIZE_RE, "finalize"), (FIX_RE, "fix")):
            m = regex.search(line)
            if m:
                state["last_stage"] = stage
                state["progress"] = "/".join(m.groups()[:2])
    return state


def detect_brand_stage(run_dir: Path, brand: str) -> Tuple[str, str]:
    raw_dir = run_dir / "raw"
    raw_candidates = raw_dir / "candidates.json"
    raw_details = raw_dir / "details.json"
    raw_local = raw_dir / "karrot_multi_region_ongoing_cn_local.json"
    fixed_json = run_dir / f"karrot_{brand}_ongoing_cn_local_fixed.json"
    final_json = run_dir / f"karrot_{brand}_ongoing_cn_by_time.json"

    if final_json.exists():
        try:
            count = len(json.loads(final_json.read_text("utf-8")))
        except Exception:
            count = -1
        return "done", str(count)
    if fixed_json.exists():
        try:
            count = len(json.loads(fixed_json.read_text("utf-8")))
        except Exception:
            count = -1
        return "postprocess", str(count)
    if raw_local.exists():
        try:
            count = len(json.loads(raw_local.read_text("utf-8")))
        except Exception:
            count = -1
        return "finalize", str(count)
    if raw_details.exists():
        try:
            count = len(json.loads(raw_details.read_text("utf-8")))
        except Exception:
            count = -1
        return "detail", str(count)
    if raw_candidates.exists():
        try:
            raw = json.loads(raw_candidates.read_text("utf-8"))
            count = len(raw)
        except Exception:
            count = -1
        return "region_scan", str(count)
    return "created", "0"


def main() -> int:
    parser = argparse.ArgumentParser(description="Show current Karrot batch pipeline status.")
    parser.add_argument("--log-dir", default="logs")
    parser.add_argument("--out-root", default="output/brand_runs")
    args = parser.parse_args()

    log_dir = Path(args.log_dir)
    out_root = Path(args.out_root)
    latest_log = latest_batch_log(log_dir)

    lines: List[str] = []
    if latest_log.exists():
        lines = latest_log.read_text("utf-8", errors="ignore").splitlines()
    log_state = parse_log(lines)

    latest_runs = sorted([p for p in out_root.iterdir() if p.is_dir() and "_" in p.name], key=lambda p: p.stat().st_mtime, reverse=True)
    active = None
    for run_dir in latest_runs:
        brand = run_dir.name.split("_", 1)[0]
        if brand in BRAND_ORDER:
            active = run_dir
            break

    result = {
        "latest_log": latest_log.as_posix() if latest_log.exists() else "",
        "running": False,
        "current_brand_index": 0,
        "current_brand_total": len(BRAND_ORDER),
        "current_brand": "",
        "stage": "idle",
        "count": "0",
        "log_progress": log_state.get("progress", ""),
        "last_log_line": log_state.get("last_line", ""),
    }

    if active is not None:
        brand = active.name.split("_", 1)[0]
        stage, count = detect_brand_stage(active, brand)
        result["current_brand"] = brand
        result["current_brand_index"] = BRAND_ORDER.index(brand) + 1 if brand in BRAND_ORDER else 0
        result["stage"] = stage
        result["count"] = count
        result["running"] = stage not in {"done"} and (log_state.get("last_brand") == brand or not lines)

    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
