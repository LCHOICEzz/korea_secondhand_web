#!/usr/bin/env python3
import argparse
import html
import json
import re
import time
import hashlib
import threading
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote, urlencode, urljoin, urlparse
from urllib.request import Request, urlopen

BASE_URL = "https://www.daangn.com"
UA = "Mozilla/5.0"
REQUEST_LOCK = threading.Lock()
NEXT_REQUEST_TS = 0.0
REQUEST_INTERVAL_S = 0.35
REQUEST_JITTER_S = 0.05
ERROR_BACKOFF_S = 0.0

REGION_ZH_OVERRIDES = {
    "압구정동": "狎鸥亭洞",
}


def parse_created_at(value: Any) -> Optional[datetime]:
    raw = (value or "").strip() if isinstance(value, str) else ""
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def is_within_max_age(created_at: Any, max_age_days: Optional[int]) -> bool:
    if max_age_days is None or max_age_days <= 0:
        return True
    dt = parse_created_at(created_at)
    if dt is None:
        return True
    cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
    return dt >= cutoff


def configure_rate_limit(interval_s: float, jitter_s: float) -> None:
    global REQUEST_INTERVAL_S, REQUEST_JITTER_S
    REQUEST_INTERVAL_S = max(0.0, interval_s)
    REQUEST_JITTER_S = max(0.0, jitter_s)


def wait_for_request_slot() -> None:
    global NEXT_REQUEST_TS
    while True:
        with REQUEST_LOCK:
            now = time.time()
            if now >= NEXT_REQUEST_TS:
                NEXT_REQUEST_TS = now + REQUEST_INTERVAL_S + REQUEST_JITTER_S
                return
            sleep_s = NEXT_REQUEST_TS - now
        if sleep_s > 0:
            time.sleep(sleep_s)


def penalize_backoff(multiplier: float = 1.0) -> None:
    global NEXT_REQUEST_TS, ERROR_BACKOFF_S
    with REQUEST_LOCK:
        ERROR_BACKOFF_S = min(20.0, max(ERROR_BACKOFF_S * 1.5, 1.5 * multiplier))
        NEXT_REQUEST_TS = max(NEXT_REQUEST_TS, time.time() + ERROR_BACKOFF_S)


def relax_backoff() -> None:
    global ERROR_BACKOFF_S
    with REQUEST_LOCK:
        ERROR_BACKOFF_S = max(0.0, ERROR_BACKOFF_S * 0.5 - 0.2)


def fetch_url(url: str, timeout: int = 12, retries: int = 2) -> str:
    err: Optional[Exception] = None
    for i in range(retries):
        try:
            wait_for_request_slot()
            req = Request(
                url,
                headers={
                    "User-Agent": UA,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Encoding": "identity",
                },
            )
            with urlopen(req, timeout=timeout) as resp:
                data = resp.read()
            relax_backoff()
            return data.decode("utf-8", errors="ignore").replace("\x00", "")
        except Exception as e:
            err = e
            penalize_backoff(i + 1)
            time.sleep(0.6 * (i + 1))
    raise RuntimeError(f"fetch failed: {url} ({err})")


def extract_remix_context(html_text: str) -> Dict[str, Any]:
    marker = "window.__remixContext = "
    idx = html_text.find(marker)
    if idx < 0:
        raise ValueError("remix context not found")
    s = html_text[idx + len(marker) :]
    start = s.find("{")
    if start < 0:
        raise ValueError("remix context start not found")
    depth = 0
    in_string = False
    escaped = False
    end = None
    for i, ch in enumerate(s[start:], start):
        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
        else:
            if ch == '"':
                in_string = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    end = i + 1
                    break
    if end is None:
        raise ValueError("remix context end not found")
    return json.loads(s[start:end])


def search_url(region_slug: str, keyword: str) -> str:
    q = urlencode({"in": region_slug, "search": keyword})
    return f"{BASE_URL}/kr/buy-sell/s/?{q}"


def search_loader_url(region_slug: str, keyword: str) -> str:
    q = urlencode({"in": region_slug, "search": keyword, "_data": "routes/kr.buy-sell.s"})
    return f"{BASE_URL}/kr/buy-sell/s/?{q}"


def fetch_json(url: str, timeout: int = 12, retries: int = 2) -> Dict[str, Any]:
    err: Optional[Exception] = None
    for i in range(retries):
        try:
            wait_for_request_slot()
            req = Request(
                url,
                headers={
                    "User-Agent": UA,
                    "Accept": "application/json,text/plain,*/*",
                    "Accept-Encoding": "identity",
                },
            )
            with urlopen(req, timeout=timeout) as resp:
                data = resp.read().decode("utf-8", errors="ignore")
            loaded = json.loads(data)
            relax_backoff()
            return loaded if isinstance(loaded, dict) else {}
        except Exception as e:
            err = e
            penalize_backoff(i + 1)
            time.sleep(0.6 * (i + 1))
    raise RuntimeError(f"json fetch failed: {url} ({err})")


def build_search_terms(primary: str, extra_terms: List[str]) -> List[str]:
    seen = set()
    terms: List[str] = []
    for term in [primary, *extra_terms]:
        key = (term or "").strip()
        if key and all(ord(ch) < 128 for ch in key):
            key = key.lower()
        if key and key not in seen:
            seen.add(key)
            terms.append(key)
    return terms


def parse_search_articles(region_slug: str, keyword: str) -> List[Dict[str, Any]]:
    try:
        data = fetch_json(search_loader_url(region_slug, keyword))
        route = data
        all_page = route.get("allPage", {})
        articles = all_page.get("fleamarketArticles", [])
        if isinstance(articles, list):
            return articles
    except Exception:
        pass
    html_text = fetch_url(search_url(region_slug, keyword))
    ctx = extract_remix_context(html_text)
    route = ctx.get("state", {}).get("loaderData", {}).get("routes/kr.buy-sell.s", {})
    all_page = route.get("allPage", {})
    articles = all_page.get("fleamarketArticles", [])
    if not isinstance(articles, list):
        return []
    return articles


def parse_detail_product(detail_url: str) -> Dict[str, Any]:
    html_text = fetch_url(detail_url)
    ctx = extract_remix_context(html_text)
    route = ctx.get("state", {}).get("loaderData", {}).get("routes/kr.buy-sell.$buy_sell_id", {})
    product = route.get("product", {})
    return product if isinstance(product, dict) else {}


def download_image(url: str, path: Path, timeout: int = 15) -> bool:
    try:
        wait_for_request_slot()
        req = Request(url, headers={"User-Agent": UA, "Accept-Encoding": "identity"})
        with urlopen(req, timeout=timeout) as resp:
            data = resp.read()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        relax_backoff()
        return True
    except Exception:
        penalize_backoff(0.5)
        return False


def chunked(items: List[Any], size: int) -> List[List[Any]]:
    if size <= 0:
        return [items]
    return [items[i : i + size] for i in range(0, len(items), size)]


def translate_ko_to_zh(text: str, cache: Dict[str, str], timeout: int = 10) -> str:
    key = (text or "").strip()
    if not key:
        return ""
    if key in cache:
        return cache[key]
    url = f"https://api.mymemory.translated.net/get?q={quote(key)}&langpair=ko|zh-CN"
    try:
        req = Request(url, headers={"User-Agent": UA})
        with urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="ignore"))
        translated = ((data or {}).get("responseData") or {}).get("translatedText") or key
        cache[key] = translated
    except Exception:
        cache[key] = key
    return cache[key]


def region_zh(region_ko: str, cache: Dict[str, str]) -> str:
    region_ko = (region_ko or "").strip()
    if not region_ko:
        return "未知区域"
    if region_ko in REGION_ZH_OVERRIDES:
        return REGION_ZH_OVERRIDES[region_ko]
    return translate_ko_to_zh(region_ko, cache)


def build_html(items: List[Dict[str, Any]], out_path: Path) -> None:
    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for it in items:
        groups[it["region_zh"]].append(it)
    sections = []
    for rz in sorted(groups.keys()):
        cards = []
        for it in groups[rz]:
            image_html = "".join(
                f'<a class="img-link" href="{html.escape(it["url"])}" target="_blank" rel="noopener">'
                f'<img src="{html.escape(p)}" alt="{html.escape(it["title_ko"])}"/></a>'
                for p in it.get("local_images", [])
            )
            desc_zh = html.escape(it.get("description_zh") or "").replace("\n", "<br/>")
            desc_ko = html.escape(it.get("description_ko") or "").replace("\n", "<br/>")
            price = it.get("price")
            price_text = f"₩{int(float(price)):,}" if price not in (None, "") else "-"
            cards.append(
                f"""
                <article class="card">
                  <div class="images">{image_html}</div>
                  <div class="content">
                    <h3><a href="{html.escape(it["url"])}" target="_blank" rel="noopener">{html.escape(it["title_zh"] or it["title_ko"])}</a></h3>
                    <p class="region">区域: {html.escape(it["region_zh"])}</p>
                    <p class="price">价格: {price_text}</p>
                    <p class="desc">{desc_zh}</p>
                    <details>
                      <summary>查看韩文原文</summary>
                      <p><strong>标题(KO):</strong> {html.escape(it["title_ko"])}</p>
                      <p><strong>介绍(KO):</strong><br/>{desc_ko}</p>
                    </details>
                    <p class="link"><a href="{html.escape(it["url"])}" target="_blank" rel="noopener">打开原始链接</a></p>
                  </div>
                </article>
                """
            )
        sections.append(
            f"""
            <section class="region-section">
              <h2 class="region-title">区域: {html.escape(rz)} · {len(cards)} 条</h2>
              <div class="grid">{''.join(cards)}</div>
            </section>
            """
        )

    page = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Karrot 多区域在售中文整理</title>
  <style>
    body {{ font-family: -apple-system,BlinkMacSystemFont,'Segoe UI','PingFang SC','Microsoft YaHei',sans-serif; margin: 24px; background: #f5f6f8; color: #1f2328; }}
    h1 {{ margin: 0 0 8px; }}
    .meta {{ margin: 0 0 20px; color: #667085; }}
    .region-section {{ margin-bottom: 24px; }}
    .region-title {{ font-size: 20px; margin: 0 0 12px; }}
    .grid {{ display: grid; grid-template-columns: 1fr; gap: 16px; }}
    .card {{ background: #fff; border: 1px solid #e4e7ec; border-radius: 14px; overflow: hidden; }}
    .images {{ display: grid; grid-template-columns: repeat(auto-fill,minmax(160px,1fr)); gap: 8px; padding: 12px; background: #fcfcfd; }}
    .images img {{ width: 100%; height: 170px; object-fit: cover; border-radius: 8px; border: 1px solid #eaecf0; }}
    .content {{ padding: 14px 16px 16px; }}
    .content h3 {{ font-size: 18px; margin: 0 0 8px; }}
    .content h3 a {{ text-decoration: none; color: #0f172a; }}
    .region {{ margin: 0 0 6px; color: #344054; font-weight: 600; }}
    .price {{ font-weight: 600; margin: 0 0 8px; color: #b54708; }}
    .desc {{ margin: 0 0 10px; line-height: 1.55; }}
    .link a {{ color: #175cd3; text-decoration: none; font-weight: 600; }}
    details {{ margin-top: 8px; color: #475467; }}
  </style>
</head>
<body>
  <h1>Karrot 多区域在售中文整理</h1>
  <p class="meta">按区域分类，仅保留在售（Ongoing）并过滤售罄（Closed）。总条数: {len(items)}</p>
  {''.join(sections)}
</body>
</html>
"""
    out_path.write_text(page, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build multi-region karrot report from region slug list.")
    parser.add_argument("--slug-file", default="output/daangn_region_slug_list.txt")
    parser.add_argument("--search", default="라플라")
    parser.add_argument("--max-regions", type=int, default=0, help="0 means all")
    parser.add_argument("--region-workers", type=int, default=24)
    parser.add_argument("--detail-workers", type=int, default=12)
    parser.add_argument("--final-workers", type=int, default=12)
    parser.add_argument("--images-per-item", type=int, default=1, help="0 means no local image download")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--search-variant", action="append", default=[], help="additional search terms used per region")
    parser.add_argument("--request-interval", type=float, default=0.42, help="minimum seconds between outbound requests")
    parser.add_argument("--request-jitter", type=float, default=0.06, help="extra spacing added to each request")
    parser.add_argument("--region-batch-size", type=int, default=120, help="submit region search tasks in batches")
    parser.add_argument("--region-batch-sleep", type=float, default=3.5, help="sleep seconds between region search batches")
    parser.add_argument("--detail-batch-size", type=int, default=80, help="submit detail fetch tasks in batches")
    parser.add_argument("--detail-batch-sleep", type=float, default=2.3, help="sleep seconds between detail batches")
    parser.add_argument("--output-dir", default="output/multi_region")
    parser.add_argument(
        "--max-age-days",
        type=int,
        default=None,
        help="only keep items published within the last N days; omit this flag to fetch all available data",
    )
    parser.add_argument(
        "--skip-translation",
        action="store_true",
        help="keep Korean fields in the intermediate output and defer translation to a later stage",
    )
    args = parser.parse_args()
    configure_rate_limit(args.request_interval, args.request_jitter)

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    img_root = out_dir / "images"
    img_root.mkdir(parents=True, exist_ok=True)
    candidates_path = out_dir / "candidates.json"
    details_path = out_dir / "details.json"

    slugs = [x.strip() for x in Path(args.slug_file).read_text("utf-8").splitlines() if x.strip()]
    if args.max_regions and args.max_regions > 0:
        slugs = slugs[: args.max_regions]
    search_terms = build_search_terms(args.search, args.search_variant)
    print(f"[INFO] regions to scan: {len(slugs)}")
    print(f"[INFO] search terms per region: {len(search_terms)}", flush=True)

    # Step1: scan search pages
    if args.resume and candidates_path.exists():
        all_candidates = json.loads(candidates_path.read_text("utf-8"))
        print(f"[INFO] resume candidates: {len(all_candidates)}")
    else:
        all_candidates = []
        done = 0
        region_jobs: List[Tuple[str, str]] = []
        for slug in slugs:
            for term in search_terms:
                region_jobs.append((slug, term))
        total_region_jobs = len(region_jobs)
        with ThreadPoolExecutor(max_workers=max(1, args.region_workers)) as ex:
            for idx, batch in enumerate(chunked(region_jobs, args.region_batch_size), start=1):
                fut_map = {ex.submit(parse_search_articles, slug, term): (slug, term) for slug, term in batch}
                for fut in as_completed(fut_map):
                    done += 1
                    try:
                        arts = fut.result()
                    except Exception:
                        arts = []
                    for a in arts:
                        if a.get("status") != "Ongoing":
                            continue
                        if not is_within_max_age(a.get("createdAt"), args.max_age_days):
                            continue
                        item = dict(a)
                        item["_matched_search_term"] = fut_map[fut][1]
                        item["_matched_region_slug"] = fut_map[fut][0]
                        all_candidates.append(item)
                    if done % 100 == 0 or done == total_region_jobs:
                        print(f"[INFO] region scan progress {done}/{total_region_jobs}, ongoing candidates={len(all_candidates)}", flush=True)
                if idx * args.region_batch_size < total_region_jobs:
                    time.sleep(args.region_batch_sleep)
        candidates_path.write_text(json.dumps(all_candidates, ensure_ascii=False), encoding="utf-8")

    if not all_candidates:
        print("[WARN] no ongoing items found")
        return 0

    # Step2: fetch detail pages for full images/content
    if args.resume and details_path.exists():
        records = json.loads(details_path.read_text("utf-8"))
        print(f"[INFO] resume details: {len(records)}")
    else:
        records = []
        detail_done = 0
        candidate_items = list(all_candidates)
        total_detail_jobs = len(candidate_items)
        with ThreadPoolExecutor(max_workers=max(1, args.detail_workers)) as ex:
            for idx, batch in enumerate(chunked(candidate_items, args.detail_batch_size), start=1):
                fut_map = {}
                for a in batch:
                    href = a.get("href") or ""
                    if not isinstance(href, str) or not href:
                        continue
                    detail_url = urljoin(BASE_URL, href)
                    fut_map[ex.submit(parse_detail_product, detail_url)] = (a, detail_url)
                for fut in as_completed(fut_map):
                    a, detail_url = fut_map[fut]
                    detail_done += 1
                    try:
                        p = fut.result()
                    except Exception:
                        p = {}
                    images = p.get("images") if isinstance(p.get("images"), list) else []
                    rec = {
                        "id": p.get("id") or a.get("id"),
                        "url": p.get("href") or detail_url,
                        "title_ko": p.get("title") or a.get("title") or "",
                        "description_ko": p.get("content") or a.get("content") or "",
                        "price": p.get("price") or a.get("price"),
                        "region_ko": (p.get("region") or {}).get("name") or a.get("region", {}).get("name") or a.get("locationName") or "",
                        "images": images,
                        "thumbnail": a.get("thumbnail") or (images[0] if images else ""),
                        "created_at": p.get("createdAt") or a.get("createdAt") or "",
                        "matched_search_term": a.get("_matched_search_term") or "",
                        "matched_region_slug": a.get("_matched_region_slug") or "",
                    }
                    if not is_within_max_age(rec.get("created_at"), args.max_age_days):
                        continue
                    records.append(rec)
                    if detail_done % 100 == 0 or detail_done == total_detail_jobs:
                        print(f"[INFO] detail progress {detail_done}/{total_detail_jobs}", flush=True)
                if idx * args.detail_batch_size < total_detail_jobs:
                    time.sleep(args.detail_batch_sleep)
        details_path.write_text(json.dumps(records, ensure_ascii=False), encoding="utf-8")

    # Step3: download images and translate
    tcache_path = out_dir / "translation_cache_ko_zh.json"
    if tcache_path.exists():
        tcache = json.loads(tcache_path.read_text("utf-8"))
    else:
        tcache = {}

    def finalize_one(r: Dict[str, Any]) -> Dict[str, Any]:
        if args.skip_translation:
            title_zh = ""
            desc_zh = ""
            rzh = r["region_ko"]
        else:
            title_zh = translate_ko_to_zh(r["title_ko"], tcache)
            desc_zh = translate_ko_to_zh(r["description_ko"], tcache)
            rzh = region_zh(r["region_ko"], tcache)
        key = str(r["url"] or r["id"] or r["title_ko"])
        h = hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]
        item_dir = img_root / h
        local_images = []
        if args.images_per_item != 0:
            limit = len(r.get("images", [])) if args.images_per_item < 0 else min(args.images_per_item, len(r.get("images", [])))
            for i, u in enumerate((r.get("images") or [])[:limit], start=1):
                suf = Path(urlparse(u).path).suffix or ".jpg"
                path = item_dir / f"{i:02d}{suf}"
                if download_image(u, path, timeout=8):
                    local_images.append(path.resolve().as_posix())
        return {
            "id": r["id"],
            "url": r["url"],
            "title_ko": r["title_ko"],
            "title_zh": title_zh,
            "description_ko": r["description_ko"],
            "description_zh": desc_zh,
            "price": r["price"],
            "region_ko": r["region_ko"],
            "region_zh": rzh,
            "thumbnail": r.get("thumbnail") or "",
            "remote_images": list(r.get("images") or []),
            "local_images": local_images,
            "local_image_count": len(local_images),
        }

    final: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, args.final_workers)) as ex:
        futs = [ex.submit(finalize_one, r) for r in records]
        done = 0
        total = len(futs)
        for fut in as_completed(futs):
            done += 1
            try:
                final.append(fut.result())
            except Exception:
                pass
            if done % 100 == 0 or done == total:
                print(f"[INFO] finalize progress {done}/{total}", flush=True)

    json_path = out_dir / "karrot_multi_region_ongoing_cn_local.json"
    html_path = out_dir / "karrot_multi_region_ongoing_cn.html"
    final_json = json.dumps(final, ensure_ascii=False, indent=2)
    json_path.write_text(final_json, encoding="utf-8")
    tcache_path.write_text(json.dumps(tcache, ensure_ascii=False, indent=2), encoding="utf-8")
    build_html(final, html_path)

    print(f"[DONE] ongoing items: {len(final)}")
    print(f"[DONE] json: {json_path}")
    print(f"[DONE] html: {html_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
