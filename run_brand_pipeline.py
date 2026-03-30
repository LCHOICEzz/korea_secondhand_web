#!/usr/bin/env python3
import argparse
import html
import importlib.util
import json
import os
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urljoin

KOREAN_RE = re.compile(r"[\uac00-\ud7a3]")
EN_RE = re.compile(r"[A-Za-z]")
ZH_RE = re.compile(r"[\u4e00-\u9fff]")
BASE_URL = "https://www.daangn.com"
SAFE_MODE_PRESET = {
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


def normalize_region_zh(item: Dict, cand: Dict) -> str:
    cur = (item.get("region_zh") or "").strip()
    bad = (not cur) or bool(KOREAN_RE.search(cur)) or bool(EN_RE.search(cur)) or (not ZH_RE.search(cur))
    if not bad:
        return cur
    dbid = (((cand.get("regionId") or {}).get("dbId")) or ((cand.get("region") or {}).get("dbId")) or "").strip()
    if dbid:
        return f"韩国地区{dbid}号"
    return "韩国未命名地区"


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, str(path))
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def sanitize_chinese_fields(items: List[Dict], cache: Dict[str, str], fixmod) -> List[Dict]:
    cleaned: List[Dict] = []
    for item in items:
        row = dict(item)
        region_zh = row.get("region_zh", "") or ""
        row["title_zh"] = fixmod.normalize_to_chinese(row.get("title_zh", ""), cache, row.get("title_ko", ""))
        row["description_zh"] = fixmod.normalize_to_chinese(row.get("description_zh", ""), cache, row.get("description_ko", ""))

        if (not region_zh.strip()) or KOREAN_RE.search(region_zh) or EN_RE.search(region_zh) or (not ZH_RE.search(region_zh)):
            translated = fixmod.translate_any_to_zh(row.get("region_ko", ""), cache) if row.get("region_ko") else ""
            row["region_zh"] = translated if translated and (not KOREAN_RE.search(translated)) and (not EN_RE.search(translated)) and ZH_RE.search(translated) else normalize_region_zh(row, {})

        cleaned.append(row)
    return cleaned


def created_at_sort_key(item: Dict) -> tuple:
    raw = (item.get("created_at") or "").strip()
    if raw:
        try:
            dt = datetime.fromisoformat(raw)
            return (dt.astimezone(timezone.utc).timestamp(), raw, item.get("title_ko") or item.get("title_zh") or "")
        except Exception:
            pass
    return (float("-inf"), raw, item.get("title_ko") or item.get("title_zh") or "")


def sort_items_by_recent(items: List[Dict]) -> List[Dict]:
    return sorted(items, key=created_at_sort_key, reverse=True)


def format_created_at(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return "-"
    try:
        dt = datetime.fromisoformat(raw)
        now = datetime.now(dt.tzinfo) if dt.tzinfo else datetime.now()
        delta = now - dt
        seconds = max(0, int(delta.total_seconds()))
        minutes = max(1, seconds // 60) if seconds else 0
        hours = seconds // 3600
        days = delta.days
        weeks = days // 7
        months = days // 30

        if seconds < 3600:
            relative = "刚刚" if minutes <= 1 else f"{minutes}分钟前"
        elif seconds < 86400:
            relative = f"{hours}小时前"
        elif days < 7:
            relative = f"{days}天前"
        elif days < 30:
            relative = f"{weeks}周前"
        else:
            relative = f"{months}个月前"
        return f"{dt.strftime('%Y-%m-%d %H:%M')} · {relative}"
    except Exception:
        return raw


def age_window_text(max_age_days: Optional[int]) -> str:
    if max_age_days is None or max_age_days <= 0:
        return "全部时间范围内"
    return f"最近{max_age_days}天内"


def build_html(items: List[Dict], brand: str, out_html: Path, max_age_days: Optional[int]) -> None:
    items = sort_items_by_recent(items)
    age_text = age_window_text(max_age_days)
    cards = []
    for it in items:
        imgs = it.get("display_images") or it.get("local_images") or []
        img_html = "".join(
            f'<a class="img-link" href="{html.escape(it["url"])}" target="_blank" rel="noopener">'
            f'<img src="{html.escape(p)}" alt="{html.escape(it.get("title_ko") or "")}"/></a>'
            for p in imgs
        )
        desc = html.escape(it.get("description_zh") or "").replace("\n", "<br/>")
        title = html.escape(it.get("title_zh") or it.get("title_ko") or "")
        region = html.escape(it.get("region_zh") or "")
        created = html.escape(format_created_at(it.get("created_at") or ""))
        price = html.escape(str(it.get("price") or "-"))
        cards.append(
            f"""
            <article class="card">
              <div class="images">{img_html}</div>
              <div class="content">
                <h3><a href="{html.escape(it["url"])}" target="_blank" rel="noopener">{title}</a></h3>
                <p class="meta">发布时间: {created}</p>
                <p class="meta">区域: {region}</p>
                <p class="meta">价格: {price}</p>
                <p class="desc">{desc}</p>
                <p class="link"><a href="{html.escape(it["url"])}" target="_blank" rel="noopener">打开原始链接</a></p>
              </div>
            </article>
            """
        )

    doc = f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="UTF-8"/><meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Karrot 品牌 {html.escape(brand)} 在售汇总</title>
<style>
body {{ font-family: -apple-system,BlinkMacSystemFont,'Segoe UI','PingFang SC','Microsoft YaHei',sans-serif; margin: 24px; background: #f5f6f8; color: #1f2328; }}
.card {{ background:#fff; border:1px solid #e4e7ec; border-radius:14px; overflow:hidden; margin-bottom:16px; }}
.images {{ display:grid; grid-template-columns: repeat(auto-fill,minmax(160px,1fr)); gap:8px; padding:12px; background:#fcfcfd; }}
.images img {{ width:100%; height:170px; object-fit:cover; border-radius:8px; border:1px solid #eaecf0; }}
.content {{ padding:14px 16px 16px; }}
.content h3 {{ margin:0 0 8px; font-size:18px; }}
.content h3 a {{ color:#0f172a; text-decoration:none; }}
.meta {{ margin:0 0 6px; color:#344054; font-weight:600; }}
.desc {{ margin:0 0 10px; line-height:1.55; }}
.link a {{ color:#175cd3; text-decoration:none; font-weight:600; }}
</style></head><body>
<h1>Karrot 品牌 {html.escape(brand)} 在售汇总</h1>
<p>仅包含{html.escape(age_text)}发布的在售（非售罄）信息，图片优先本地文件。</p>
{''.join(cards)}
</body></html>"""
    out_html.write_text(doc, encoding="utf-8")


def build_preview_items(raw_dir: Path) -> List[Dict]:
    raw_json = raw_dir / "karrot_multi_region_ongoing_cn_local.json"
    details_json = raw_dir / "details.json"
    candidates_json = raw_dir / "candidates.json"
    preview_items: List[Dict] = []
    details_by_url: Dict[str, Dict] = {}
    candidates_by_url: Dict[str, Dict] = {}
    if details_json.exists():
        details = json.loads(details_json.read_text("utf-8"))
        if isinstance(details, list):
            for detail in details:
                if not isinstance(detail, dict):
                    continue
                url = detail.get("url") or ""
                if isinstance(url, str) and url:
                    details_by_url[url] = detail
    if candidates_json.exists():
        candidates = json.loads(candidates_json.read_text("utf-8"))
        if isinstance(candidates, list):
            for cand in candidates:
                if not isinstance(cand, dict):
                    continue
                href = cand.get("href") or ""
                if isinstance(href, str) and href:
                    candidates_by_url[href] = cand
                    candidates_by_url[urljoin(BASE_URL, href)] = cand

    if raw_json.exists():
        items = json.loads(raw_json.read_text("utf-8"))
        for item in items:
            if not isinstance(item, dict):
                continue
            url = item.get("url") or ""
            detail = details_by_url.get(url, {})
            cand = candidates_by_url.get(url, {})
            preview_items.append(
                {
                    "url": url,
                    "title_ko": item.get("title_ko") or "",
                    "description_ko": item.get("description_ko") or "",
                    "region_ko": item.get("region_ko") or "",
                    "created_at": item.get("created_at") or detail.get("created_at") or cand.get("createdAt") or "",
                    "price": item.get("price") or "",
                    "display_images": item.get("local_images") or item.get("remote_images") or ([item.get("thumbnail")] if item.get("thumbnail") else []),
                }
            )
        return sort_items_by_recent(preview_items)
    if not details_json.exists():
        return []
    for item in details_by_url.values():
        if not isinstance(item, dict):
            continue
        url = item.get("url") or ""
        cand = candidates_by_url.get(url, {})
        remote_images = item.get("images") if isinstance(item.get("images"), list) else []
        thumbnail = item.get("thumbnail") or cand.get("thumbnail") or (remote_images[0] if remote_images else "")
        preview_items.append(
            {
                "url": url,
                "title_ko": item.get("title_ko") or "",
                "description_ko": item.get("description_ko") or "",
                "region_ko": item.get("region_ko") or "",
                "created_at": item.get("created_at") or cand.get("createdAt") or "",
                "price": item.get("price") or "",
                "display_images": remote_images or ([thumbnail] if thumbnail else []),
            }
        )
    return sort_items_by_recent(preview_items)


def build_preview_html(items: List[Dict], brand: str, out_html: Path, max_age_days: Optional[int]) -> None:
    items = sort_items_by_recent(items)
    age_text = age_window_text(max_age_days)
    cards = []
    for it in items:
        imgs = it.get("display_images") or []
        img_html = "".join(
            f'<a class="img-link" href="{html.escape(it["url"])}" target="_blank" rel="noopener">'
            f'<img src="{html.escape(p)}" alt="{html.escape(it.get("title_ko") or "")}"/></a>'
            for p in imgs
        )
        desc = html.escape(it.get("description_ko") or "").replace("\n", "<br/>")
        title = html.escape(it.get("title_ko") or "")
        region = html.escape(it.get("region_ko") or "")
        created = html.escape(format_created_at(it.get("created_at") or ""))
        price = html.escape(str(it.get("price") or "-"))
        cards.append(
            f"""
            <article class="card">
              <div class="images">{img_html}</div>
              <div class="content">
                <h3><a href="{html.escape(it["url"])}" target="_blank" rel="noopener">{title}</a></h3>
                <p class="meta">发布时间: {created}</p>
                <p class="meta">区域(KO): {region}</p>
                <p class="meta">价格: {price}</p>
                <p class="desc">{desc}</p>
                <p class="note">中文翻译处理中，当前为韩文预览页。</p>
                <p class="link"><a href="{html.escape(it["url"])}" target="_blank" rel="noopener">打开原始链接</a></p>
              </div>
            </article>
            """
        )

    doc = f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="UTF-8"/><meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Karrot 品牌 {html.escape(brand)} 预览页</title>
<style>
body {{ font-family: -apple-system,BlinkMacSystemFont,'Segoe UI','PingFang SC','Microsoft YaHei',sans-serif; margin: 24px; background: #f5f6f8; color: #1f2328; }}
.card {{ background:#fff; border:1px solid #e4e7ec; border-radius:14px; overflow:hidden; margin-bottom:16px; }}
.images {{ display:grid; grid-template-columns: repeat(auto-fill,minmax(160px,1fr)); gap:8px; padding:12px; background:#fcfcfd; }}
.images img {{ width:100%; height:170px; object-fit:cover; border-radius:8px; border:1px solid #eaecf0; }}
.content {{ padding:14px 16px 16px; }}
.content h3 {{ margin:0 0 8px; font-size:18px; }}
.content h3 a {{ color:#0f172a; text-decoration:none; }}
.meta {{ margin:0 0 6px; color:#344054; font-weight:600; }}
.desc {{ margin:0 0 10px; line-height:1.55; }}
.note {{ margin:0 0 10px; color:#b54708; font-weight:600; }}
.link a {{ color:#175cd3; text-decoration:none; font-weight:600; }}
</style></head><body>
<h1>Karrot 品牌 {html.escape(brand)} 预览页</h1>
<p>仅展示{html.escape(age_text)}发布的在售内容。抓取已完成，中文翻译和最终整理正在后台继续处理。</p>
{''.join(cards)}
</body></html>"""
    out_html.write_text(doc, encoding="utf-8")


def build_preview_artifacts(run_dir: Path, brand: str, max_age_days: Optional[int]) -> None:
    raw_dir = run_dir / "raw"
    items = build_preview_items(raw_dir)
    preview_json = run_dir / f"karrot_{brand}_ongoing_preview.json"
    preview_html = run_dir / f"karrot_{brand}_ongoing_preview.html"
    preview_json.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")
    build_preview_html(items, brand, preview_html, max_age_days)
    print("[DONE] preview_html=", preview_html)
    print("[DONE] preview_json=", preview_json)


def build_share_pdf(share_html: Path, share_pdf: Path) -> bool:
    if os.environ.get("KARROT_SKIP_SHARE_PDF", "").strip() == "1":
        print(f"[INFO] skip pdf export by env: {share_pdf}", flush=True)
        return False
    chrome_bin = Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
    if not chrome_bin.exists():
        print(f"[WARN] chrome not found, skip pdf: {share_pdf}", flush=True)
        return False
    cmd = [
        str(chrome_bin),
        "--headless=new",
        "--disable-gpu",
        "--allow-file-access-from-files",
        "--print-to-pdf-no-header",
        f"--print-to-pdf={share_pdf}",
        share_html.resolve().as_uri(),
    ]
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return share_pdf.exists()
    except Exception as exc:
        print(f"[WARN] pdf export failed: {share_pdf} ({exc})", flush=True)
        return False


def build_scan_cmd(args, raw_dir: Path) -> List[str]:
    scan_cmd = [
        "python3",
        "-u",
        "multi_region_karrot_report.py",
        "--slug-file",
        args.slug_file,
        "--search",
        args.brand,
        "--output-dir",
        str(raw_dir),
        "--region-workers",
        str(args.region_workers),
        "--detail-workers",
        str(args.detail_workers),
        "--final-workers",
        str(args.final_workers),
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
        "--images-per-item",
        "-1",
        "--skip-translation",
    ]
    if args.max_age_days is not None:
        scan_cmd.extend(["--max-age-days", str(args.max_age_days)])
    for term in args.search_variant:
        scan_cmd.extend(["--search-variant", term])
    return scan_cmd


def build_fix_cmd(args, run_dir: Path, raw_dir: Path) -> List[str]:
    return [
        "python3",
        "-u",
        "fix_multi_region_output.py",
        "--input-json",
        str(raw_dir / "karrot_multi_region_ongoing_cn_local.json"),
        "--details-json",
        str(raw_dir / "details.json"),
        "--out-json",
        str(run_dir / f"karrot_{args.brand}_ongoing_cn_local_fixed.json"),
        "--out-html",
        str(run_dir / f"karrot_{args.brand}_ongoing_cn_fixed.html"),
        "--cache",
        str(run_dir / "translation_cache.json"),
        "--workers",
        str(args.fix_workers),
        "--no-fix-region-zh",
    ]


def postprocess_brand(run_dir: Path, brand: str, max_age_days: Optional[int]) -> None:
    fixmod = load_module(Path(__file__).with_name("fix_multi_region_output.py"), f"fixmod_{brand}")
    raw_dir = run_dir / "raw"
    fixed_json = run_dir / f"karrot_{brand}_ongoing_cn_local_fixed.json"
    fixed_html = run_dir / f"karrot_{brand}_ongoing_cn_fixed.html"
    items = json.loads(fixed_json.read_text("utf-8"))
    candidates = json.loads((raw_dir / "candidates.json").read_text("utf-8"))
    details = json.loads((raw_dir / "details.json").read_text("utf-8"))
    cache_path = run_dir / "translation_cache.json"
    if cache_path.exists():
        try:
            cache = json.loads(cache_path.read_text("utf-8"))
        except Exception:
            cache = {}
    else:
        cache = {}

    by_url: Dict[str, Dict] = {}
    details_by_url: Dict[str, Dict] = {}
    if isinstance(candidates, list):
        for cand in candidates:
            href = cand.get("href") if isinstance(cand, dict) else ""
            if isinstance(href, str) and href:
                by_url.setdefault(href, cand)
                by_url.setdefault(urljoin(BASE_URL, href), cand)
    elif isinstance(candidates, dict):
        for href, cand in candidates.items():
            if isinstance(href, str) and href:
                by_url.setdefault(href, cand)
                by_url.setdefault(urljoin(BASE_URL, href), cand)
    if isinstance(details, list):
        for detail in details:
            url = detail.get("url") if isinstance(detail, dict) else ""
            if isinstance(url, str) and url:
                details_by_url[url] = detail

    for it in items:
        cand = by_url.get(it.get("url") or "", {})
        detail = details_by_url.get(it.get("url") or "", {})
        it["created_at"] = it.get("created_at") or cand.get("createdAt") or ""
        it["region_zh"] = normalize_region_zh(it, cand)
        if not it.get("thumbnail"):
            it["thumbnail"] = cand.get("thumbnail") or detail.get("thumbnail") or ""
        remote_images = it.get("remote_images") or []
        if not remote_images:
            remote_images = detail.get("images") if isinstance(detail.get("images"), list) else []
            it["remote_images"] = remote_images
        if not it.get("display_images") and it.get("thumbnail"):
            it["display_images"] = [it["thumbnail"]]

    items = sanitize_chinese_fields(items, cache, fixmod)
    items = sort_items_by_recent(items)

    final_json = run_dir / f"karrot_{brand}_ongoing_cn_by_time.json"
    final_html = run_dir / f"karrot_{brand}_ongoing_cn_by_time.html"
    share_html = run_dir / f"karrot_{brand}_share_mobile.html"
    share_pdf = run_dir / f"karrot_{brand}_share_mobile.pdf"
    final_json.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")
    fixed_json.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")
    cache_path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    fixmod.build_html(items, fixed_html, max_age_days)
    fixmod.build_share_html(items, brand, share_html, max_age_days)
    pdf_ok = build_share_pdf(share_html, share_pdf)
    build_html(items, brand, final_html, max_age_days)
    print("[DONE] share_html=", share_html)
    if pdf_ok:
        print("[DONE] share_pdf=", share_pdf)


def main() -> int:
    p = argparse.ArgumentParser(description="Run brand scrape pipeline with brand+timestamp folder output.")
    p.add_argument("--brand", required=True, help="Korean brand keyword, e.g. 더로랑")
    p.add_argument("--search-variant", action="append", default=[], help="additional search terms used per region")
    p.add_argument("--slug-file", default="output/daangn_region_slug_list.txt")
    p.add_argument("--out-root", default="output/brand_runs")
    p.add_argument("--region-workers", type=int, default=8)
    p.add_argument("--detail-workers", type=int, default=5)
    p.add_argument("--final-workers", type=int, default=8)
    p.add_argument("--fix-workers", type=int, default=2)
    p.add_argument("--request-interval", type=float, default=0.38)
    p.add_argument("--request-jitter", type=float, default=0.06)
    p.add_argument("--region-batch-size", type=int, default=120)
    p.add_argument("--region-batch-sleep", type=float, default=3.5)
    p.add_argument("--detail-batch-size", type=int, default=80)
    p.add_argument("--detail-batch-sleep", type=float, default=2.3)
    p.add_argument(
        "--max-age-days",
        type=int,
        default=None,
        help="only keep items published within the last N days; omit this flag to fetch all available data",
    )
    p.add_argument(
        "--safe-mode",
        action="store_true",
        help="Use much lower concurrency and slower pacing to reduce the chance of triggering site protections",
    )
    p.add_argument("--mode", choices=["all", "scrape", "postprocess"], default="all")
    p.add_argument("--run-dir", default="", help="existing run directory used by postprocess mode")
    args = p.parse_args()

    if args.safe_mode:
        for key, value in SAFE_MODE_PRESET.items():
            setattr(args, key, value)
        print("[INFO] safe mode enabled for brand scrape", flush=True)

    if args.mode == "postprocess":
        if not args.run_dir:
            raise SystemExit("--run-dir is required when --mode=postprocess")
        run_dir = Path(args.run_dir)
    else:
        start = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_dir = Path(args.out_root) / f"{args.brand}_{start}"
    raw_dir = run_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    if args.mode in {"all", "scrape"}:
        run(build_scan_cmd(args, raw_dir))
        build_preview_artifacts(run_dir, args.brand, args.max_age_days)

    if args.mode in {"all", "postprocess"}:
        run(build_fix_cmd(args, run_dir, raw_dir))
        postprocess_brand(run_dir, args.brand, args.max_age_days)

    print("[DONE] run_dir=", run_dir)
    print("[DONE] preview_html=", run_dir / f"karrot_{args.brand}_ongoing_preview.html")
    print("[DONE] html=", run_dir / f"karrot_{args.brand}_ongoing_cn_by_time.html")
    print("[DONE] json=", run_dir / f"karrot_{args.brand}_ongoing_cn_by_time.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
