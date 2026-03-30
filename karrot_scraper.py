#!/usr/bin/env python3
import argparse
import csv
import gzip
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urljoin, urlparse
from urllib.request import Request, urlopen

BASE_URL = "https://www.daangn.com"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)


def fetch_html(url: str, timeout: int = 30, retries: int = 3) -> str:
    last_error: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            req = Request(
                url,
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Encoding": "gzip, deflate",
                },
            )
            with urlopen(req, timeout=timeout) as resp:
                data = resp.read()
                encoding = (resp.headers.get("Content-Encoding") or "").lower()
            if encoding == "gzip":
                data = gzip.decompress(data)
            return data.decode("utf-8", errors="ignore").replace("\x00", "")
        except (HTTPError, URLError, TimeoutError, OSError) as err:
            last_error = err
            if attempt < retries:
                time.sleep(0.8 * attempt)
    raise RuntimeError(f"fetch failed: {url} ({last_error})")


def extract_remix_context(html: str) -> Dict[str, Any]:
    marker = "window.__remixContext = "
    idx = html.find(marker)
    if idx < 0:
        raise ValueError("window.__remixContext not found")
    s = html[idx + len(marker) :]
    start = s.find("{")
    if start < 0:
        raise ValueError("remix context opening brace not found")

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
        raise ValueError("remix context closing brace not found")

    raw = s[start:end]
    return json.loads(raw)


def build_search_url(region_slug: str, search_keyword: str) -> str:
    query = urlencode({"in": region_slug, "search": search_keyword})
    return f"{BASE_URL}/kr/buy-sell/s/?{query}"


def get_search_articles(search_url: str) -> List[Dict[str, Any]]:
    html = fetch_html(search_url)
    ctx = extract_remix_context(html)
    loader_data = ctx.get("state", {}).get("loaderData", {})
    route = loader_data.get("routes/kr.buy-sell.s", {})
    all_page = route.get("allPage", {})
    articles = all_page.get("fleamarketArticles", [])
    if not isinstance(articles, list):
        return []
    return articles


def normalize_price(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def extract_product_from_detail(detail_url: str) -> Dict[str, Any]:
    html = fetch_html(detail_url)
    ctx = extract_remix_context(html)
    loader_data = ctx.get("state", {}).get("loaderData", {})
    route = loader_data.get("routes/kr.buy-sell.$buy_sell_id", {})
    product = route.get("product", {}) if isinstance(route, dict) else {}
    if not isinstance(product, dict):
        product = {}
    return product


def slugify_for_fs(s: str) -> str:
    s = re.sub(r"[^\w\-\.]+", "_", s, flags=re.UNICODE).strip("_")
    return s[:80] or "item"


def download_images(image_urls: List[str], out_dir: Path, timeout: int = 30) -> List[str]:
    out_dir.mkdir(parents=True, exist_ok=True)
    saved_paths: List[str] = []
    for idx, image_url in enumerate(image_urls, start=1):
        parsed = urlparse(image_url)
        suffix = Path(parsed.path).suffix or ".jpg"
        filename = f"{idx:02d}{suffix}"
        target = out_dir / filename
        try:
            req = Request(image_url, headers={"User-Agent": USER_AGENT, "Accept-Encoding": "identity"})
            with urlopen(req, timeout=timeout) as resp:
                content = resp.read()
            target.write_bytes(content)
            saved_paths.append(str(target))
        except Exception:
            continue
    return saved_paths


def make_record(search_item: Dict[str, Any], product: Dict[str, Any]) -> Dict[str, Any]:
    href = product.get("href") or search_item.get("href") or ""
    full_url = urljoin(BASE_URL, href)
    images = product.get("images") or []
    if not isinstance(images, list):
        images = []
    return {
        "id": product.get("id") or search_item.get("id"),
        "title": product.get("title") or search_item.get("title"),
        "url": full_url,
        "status": product.get("status") or search_item.get("status"),
        "price": normalize_price(product.get("price") or search_item.get("price")),
        "description": product.get("content") or search_item.get("content"),
        "thumbnail": search_item.get("thumbnail"),
        "images": images,
        "image_count": len(images),
        "created_at": product.get("createdAt") or search_item.get("createdAt"),
        "boosted_at": product.get("boostedAt") or search_item.get("boostedAt"),
        "region_name": (product.get("region") or {}).get("name"),
        "category_name": (product.get("category") or {}).get("name"),
        "seller_nickname": (product.get("user") or {}).get("nickname"),
        "seller_profile_url": urljoin(BASE_URL, (product.get("user") or {}).get("href") or ""),
    }


def save_json(records: List[Dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")


def save_csv(records: List[Dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "id",
        "title",
        "url",
        "status",
        "price",
        "description",
        "thumbnail",
        "images",
        "image_count",
        "created_at",
        "boosted_at",
        "region_name",
        "category_name",
        "seller_nickname",
        "seller_profile_url",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for rec in records:
            row = rec.copy()
            row["images"] = "|".join(rec.get("images") or [])
            writer.writerow(row)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Scrape Karrot(daangn) search results and collect listing images/links/descriptions."
    )
    parser.add_argument("--region", required=True, help='Region slug, e.g. "압구정동-385"')
    parser.add_argument("--search", required=True, help='Search keyword, e.g. "라플라"')
    parser.add_argument("--workers", type=int, default=6, help="Concurrent workers for detail pages")
    parser.add_argument("--output-dir", default="output", help="Output directory")
    parser.add_argument(
        "--download-images",
        action="store_true",
        help="Download all detail images to output-dir/images/",
    )
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    search_url = build_search_url(args.region, args.search)
    print(f"[INFO] search url: {search_url}")
    search_items = get_search_articles(search_url)
    if not search_items:
        print("[WARN] no search results found.")
        return 1
    print(f"[INFO] search items: {len(search_items)}")

    deduped: Dict[str, Dict[str, Any]] = {}
    for item in search_items:
        href = item.get("href")
        if isinstance(href, str) and href:
            deduped[href] = item
    print(f"[INFO] unique items: {len(deduped)}")

    records: List[Dict[str, Any]] = []
    failed_urls: List[str] = []
    skipped_closed = 0

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        future_map = {}
        for href, item in deduped.items():
            detail_url = urljoin(BASE_URL, href)
            future_map[executor.submit(extract_product_from_detail, detail_url)] = (href, item)

        for fut in as_completed(future_map):
            href, item = future_map[fut]
            detail_url = urljoin(BASE_URL, href)
            try:
                product = fut.result()
                rec = make_record(item, product)
                if rec.get("status") != "Ongoing":
                    skipped_closed += 1
                    print(f"[SKIP] {rec.get('title')} ({rec.get('status')})")
                    continue
                records.append(rec)
                print(f"[OK] {rec.get('title')}")
            except Exception as err:
                failed_urls.append(detail_url)
                print(f"[FAIL] {detail_url} ({err})", file=sys.stderr)

    records.sort(key=lambda x: (x.get("created_at") or "", x.get("title") or ""), reverse=True)

    json_path = out_dir / "karrot_listings.json"
    csv_path = out_dir / "karrot_listings.csv"
    save_json(records, json_path)
    save_csv(records, csv_path)

    image_manifest: Dict[str, List[str]] = {}
    if args.download_images:
        image_root = out_dir / "images"
        for rec in records:
            item_id = str(rec.get("id") or slugify_for_fs(str(rec.get("title") or "item")))
            safe_id = slugify_for_fs(item_id)
            saved = download_images(rec.get("images") or [], image_root / safe_id)
            image_manifest[safe_id] = saved
        (out_dir / "downloaded_images_manifest.json").write_text(
            json.dumps(image_manifest, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    print(f"[DONE] ongoing records: {len(records)}")
    if skipped_closed:
        print(f"[DONE] skipped non-ongoing records: {skipped_closed}")
    print(f"[DONE] json: {json_path}")
    print(f"[DONE] csv : {csv_path}")
    if args.download_images:
        total_images = sum(len(v) for v in image_manifest.values())
        print(f"[DONE] downloaded images: {total_images}")
    if failed_urls:
        failed_path = out_dir / "failed_urls.txt"
        failed_path.write_text("\n".join(failed_urls), encoding="utf-8")
        print(f"[WARN] failed detail urls: {len(failed_urls)} -> {failed_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
