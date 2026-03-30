#!/usr/bin/env python3
import argparse
import html
import json
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional


DEFAULT_BRANDS = [
    {"brand": "더로랑", "variants": ["the laurent"]},
    {"brand": "라플라", "variants": ["lapla"]},
    {"brand": "라벨르블랑", "variants": ["labelleblanc"]},
    {"brand": "헤이에스", "variants": ["heys"]},
    {"brand": "리즈", "variants": ["leeds"]},
    {"brand": "수제화", "variants": [], "max_age_days": 0},
    {"brand": "핸드메이드신발", "variants": ["handmade shoes"], "max_age_days": 0},
]
SAFE_MODE_PRESET = {
    "sleep_between_brands": 120.0,
    "region_workers": 2,
    "detail_workers": 1,
    "final_workers": 1,
    "fix_workers": 1,
    "request_interval": 1.2,
    "request_jitter": 0.6,
    "region_batch_size": 20,
    "region_batch_sleep": 10.0,
    "detail_batch_size": 10,
    "detail_batch_sleep": 8.0,
}


def run(cmd: List[str]) -> None:
    print("[RUN]", " ".join(cmd), flush=True)
    subprocess.run(cmd, check=True)


def spawn(cmd: List[str]) -> subprocess.Popen:
    print("[SPAWN]", " ".join(cmd), flush=True)
    return subprocess.Popen(cmd)


def build_index(run_root: Path, results: List[Dict], out_html: Path) -> None:
    cards = []
    for row in results:
        status_map = {
            "done": "完成",
            "scraped": "已抓取，预览已生成，后处理中",
            "preview_only": "已抓取，预览已生成",
            "failed": "失败",
        }
        preview_html = row.get("preview_html") or ""
        preview_json = row.get("preview_json") or ""
        status = status_map.get(row.get("status"), row.get("status") or "未知")
        final_html = row.get("final_html") or ""
        final_json = row.get("final_json") or ""
        run_dir = row.get("run_dir") or ""
        variants = ", ".join(row.get("variants") or [])
        cards.append(
            f"""
            <article class="card">
              <h2>{html.escape(row.get("brand") or "")}</h2>
              <p class="meta">状态: {status}</p>
              <p class="meta">韩文品牌: {html.escape(row.get("brand") or "")}</p>
              <p class="meta">英文变体: {html.escape(variants)}</p>
              <p class="meta">目录: <a href="{html.escape(run_dir)}">{html.escape(run_dir)}</a></p>
              <p class="meta">预览 HTML: <a href="{html.escape(preview_html)}">{html.escape(preview_html)}</a></p>
              <p class="meta">预览 JSON: <a href="{html.escape(preview_json)}">{html.escape(preview_json)}</a></p>
              <p class="meta">HTML: <a href="{html.escape(final_html)}">{html.escape(final_html)}</a></p>
              <p class="meta">JSON: <a href="{html.escape(final_json)}">{html.escape(final_json)}</a></p>
              <p class="meta">错误: {html.escape(row.get("error") or "-")}</p>
            </article>
            """
        )
    page = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Karrot 多品牌抓取总览</title>
  <style>
    body {{ font-family: -apple-system,BlinkMacSystemFont,'Segoe UI','PingFang SC','Microsoft YaHei',sans-serif; margin: 24px; background: #f5f6f8; color: #1f2328; }}
    .card {{ background:#fff; border:1px solid #e4e7ec; border-radius:14px; padding:16px; margin-bottom:16px; }}
    .card h2 {{ margin:0 0 10px; }}
    .meta {{ margin: 0 0 8px; line-height: 1.5; word-break: break-all; }}
    a {{ color:#175cd3; text-decoration:none; }}
  </style>
</head>
<body>
  <h1>Karrot 多品牌抓取总览</h1>
  <p>根目录: {html.escape(run_root.as_posix())}</p>
  {''.join(cards)}
</body>
</html>
"""
    out_html.write_text(page, encoding="utf-8")


def detect_latest_run(out_root: Path, brand: str, started_before: float) -> Path:
    candidates = []
    fallback_candidates = []
    for p in out_root.glob(f"{brand}_*"):
        try:
            stat = p.stat()
        except FileNotFoundError:
            continue
        if not p.is_dir():
            continue
        fallback_candidates.append((stat.st_mtime, p))
        if stat.st_mtime >= started_before:
            candidates.append((stat.st_mtime, p))
    if not candidates:
        if not fallback_candidates:
            raise FileNotFoundError(f"run dir not found for {brand}")
        fallback_candidates.sort()
        latest_mtime, latest_path = fallback_candidates[-1]
        print(
            f"[WARN] no run dir newer than start marker for {brand}; fallback to latest matching dir {latest_path} (mtime={latest_mtime})",
            flush=True,
        )
        return latest_path
    candidates.sort()
    return candidates[-1][1]


def build_brand_cmd(args, brand: str, variants: List[str], mode: str, run_dir: str = "", max_age_days: Optional[int] = None) -> List[str]:
    effective_max_age_days = args.max_age_days if max_age_days is None else max_age_days
    cmd = [
        "python3",
        "-u",
        "run_brand_pipeline.py",
        "--brand",
        brand,
        "--slug-file",
        args.slug_file,
        "--out-root",
        str(args.out_root),
        "--region-workers",
        str(args.region_workers),
        "--detail-workers",
        str(args.detail_workers),
        "--final-workers",
        str(args.final_workers),
        "--fix-workers",
        str(args.fix_workers),
        "--request-interval",
        str(args.request_interval),
        "--request-jitter",
        str(args.request_jitter),
        "--region-batch-size",
        str(args.region_batch_size),
        "--region-batch-sleep",
        str(args.region_batch_sleep),
        "--detail-batch-size",
        str(args.detail_batch_size),
        "--detail-batch-sleep",
        str(args.detail_batch_sleep),
        "--max-age-days",
        str(effective_max_age_days),
        "--mode",
        mode,
    ]
    if run_dir:
        cmd.extend(["--run-dir", run_dir])
    for term in variants:
        cmd.extend(["--search-variant", term])
    return cmd


def main() -> int:
    parser = argparse.ArgumentParser(description="Run multiple brand scrapes with pipelined post-processing.")
    parser.add_argument("--slug-file", default="output/daangn_region_slug_list.txt")
    parser.add_argument("--out-root", default="output/brand_runs")
    parser.add_argument("--sleep-between-brands", type=float, default=35.0)
    parser.add_argument("--region-workers", type=int, default=8)
    parser.add_argument("--detail-workers", type=int, default=5)
    parser.add_argument("--final-workers", type=int, default=8)
    parser.add_argument("--fix-workers", type=int, default=2)
    parser.add_argument("--request-interval", type=float, default=0.38)
    parser.add_argument("--request-jitter", type=float, default=0.06)
    parser.add_argument("--region-batch-size", type=int, default=120)
    parser.add_argument("--region-batch-sleep", type=float, default=3.5)
    parser.add_argument("--detail-batch-size", type=int, default=80)
    parser.add_argument("--detail-batch-sleep", type=float, default=2.3)
    parser.add_argument("--max-age-days", type=int, default=30, help="keep only items whose publish time is within the last N days; 0 disables the filter")
    parser.add_argument(
        "--preview-only",
        action="store_true",
        help="Only run scrape stage and keep preview HTML/JSON without launching translation postprocess",
    )
    parser.add_argument(
        "--safe-mode",
        action="store_true",
        help="Use much lower concurrency and slower pacing to reduce the chance of triggering site protections",
    )
    args = parser.parse_args()

    if args.safe_mode:
        for key, value in SAFE_MODE_PRESET.items():
            setattr(args, key, value)
        print("[INFO] safe mode enabled for multi-brand scrape", flush=True)

    out_root = Path(args.out_root)
    out_root.mkdir(parents=True, exist_ok=True)
    batch_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    summary_json = out_root / f"brand_batch_{batch_stamp}.json"
    summary_html = out_root / f"brand_batch_{batch_stamp}.html"

    results: List[Dict] = []
    active_post: List[Dict] = []
    for idx, spec in enumerate(DEFAULT_BRANDS, start=1):
        brand = spec["brand"]
        variants = spec["variants"]
        brand_max_age_days = spec.get("max_age_days")
        started_before = time.time()
        row = {
            "brand": brand,
            "variants": variants,
            "max_age_days": args.max_age_days if brand_max_age_days is None else brand_max_age_days,
            "status": "failed",
            "run_dir": "",
            "preview_html": "",
            "preview_json": "",
            "final_html": "",
            "final_json": "",
            "error": "",
        }
        try:
            run(build_brand_cmd(args, brand, variants, "scrape", max_age_days=brand_max_age_days))
            run_dir = detect_latest_run(out_root, brand, started_before)
            row["run_dir"] = run_dir.as_posix()
            row["preview_html"] = (run_dir / f"karrot_{brand}_ongoing_preview.html").as_posix()
            row["preview_json"] = (run_dir / f"karrot_{brand}_ongoing_preview.json").as_posix()
            row["status"] = "preview_only" if args.preview_only else "scraped"
            if not args.preview_only:
                post_proc = spawn(build_brand_cmd(args, brand, variants, "postprocess", run_dir.as_posix(), max_age_days=brand_max_age_days))
                active_post.append({"proc": post_proc, "row": row, "brand": brand, "run_dir": run_dir})
            results.append(row)
        except Exception as e:
            row["error"] = str(e)
            results.append(row)
        summary_json.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
        build_index(out_root, results, summary_html)
        if idx != len(DEFAULT_BRANDS):
            time.sleep(args.sleep_between_brands)

    for item in active_post:
        proc = item["proc"]
        row = item["row"]
        run_dir = item["run_dir"]
        ret = proc.wait()
        row["final_html"] = (run_dir / f"karrot_{row['brand']}_ongoing_cn_by_time.html").as_posix()
        row["final_json"] = (run_dir / f"karrot_{row['brand']}_ongoing_cn_by_time.json").as_posix()
        row["status"] = "done" if ret == 0 else "failed"
        if ret != 0 and not row.get("error"):
            row["error"] = f"postprocess exited with status {ret}"
        summary_json.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
        build_index(out_root, results, summary_html)

    print("[DONE] summary_json=", summary_json)
    print("[DONE] summary_html=", summary_html)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
