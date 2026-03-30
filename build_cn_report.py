#!/usr/bin/env python3
import argparse
import html
import json
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, List
from urllib.parse import quote
from urllib.request import Request, urlopen

REGION_ZH_OVERRIDES = {
    "압구정동": "狎鸥亭洞",
}


def translate_ko_to_zh(text: str, timeout: int = 10) -> str:
    text = (text or "").strip()
    if not text:
        return ""
    url_backup = (
        "https://api.mymemory.translated.net/get"
        f"?q={quote(text)}&langpair=ko|zh-CN"
    )
    try:
        req = Request(url_backup, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=timeout) as resp:
            data = resp.read().decode("utf-8", errors="ignore")
        payload = json.loads(data)
        translated = ((payload or {}).get("responseData") or {}).get("translatedText") or ""
        return translated.strip() or text
    except Exception:
        return text


def load_json(path: Path):
    return json.loads(path.read_text("utf-8"))


def normalize_local_path(path: str) -> str:
    p = Path(path).resolve()
    return p.as_posix()

def translate_region_ko_to_zh(region_ko: str, translator) -> str:
    region_ko = (region_ko or "").strip()
    if not region_ko:
        return "未知区域"
    if region_ko in REGION_ZH_OVERRIDES:
        return REGION_ZH_OVERRIDES[region_ko]
    translated = (translator(region_ko) or "").strip()
    return translated or "未知区域"


def build_card(
    item: Dict,
    title_zh: str,
    desc_zh: str,
    image_paths: List[str],
    region_zh: str,
    region_ko: str,
) -> str:
    title_ko = item.get("title") or ""
    desc_ko = item.get("description") or ""
    url = item.get("url") or ""
    price = item.get("price")
    price_txt = f"₩{int(price):,}" if isinstance(price, (int, float)) else "-"
    desc_display = html.escape(desc_zh or desc_ko).replace("\n", "<br/>")
    desc_ko_display = html.escape(desc_ko).replace("\n", "<br/>")
    image_html = "".join(
        f'<a class="img-link" href="{html.escape(url)}" target="_blank" rel="noopener">'
        f'<img src="{html.escape(p)}" alt="{html.escape(title_ko)}"/></a>'
        for p in image_paths
    )
    return f"""
    <article class="card">
      <div class="images">{image_html}</div>
      <div class="content">
        <h2><a href="{html.escape(url)}" target="_blank" rel="noopener">{html.escape(title_zh or title_ko)}</a></h2>
        <p class="region">区域: {html.escape(region_zh)}</p>
        <p class="price">价格: {price_txt}</p>
        <p class="desc">{desc_display}</p>
        <details>
          <summary>查看韩文原文</summary>
          <p><strong>标题(KO):</strong> {html.escape(title_ko)}</p>
          <p><strong>介绍(KO):</strong><br/>{desc_ko_display}</p>
        </details>
        <p class="link"><a href="{html.escape(url)}" target="_blank" rel="noopener">打开原始链接</a></p>
      </div>
    </article>
    """


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Chinese HTML report from karrot listings.")
    parser.add_argument("--listings", default="output/karrot_listings.json")
    parser.add_argument("--manifest", default="output/downloaded_images_manifest.json")
    parser.add_argument("--output", default="output/karrot_report_cn.html")
    parser.add_argument("--cache", default="output/translation_cache_ko_zh.json")
    args = parser.parse_args()

    listings = load_json(Path(args.listings))
    manifest = load_json(Path(args.manifest))

    ongoing = [x for x in listings if x.get("status") == "Ongoing"]
    cache_path = Path(args.cache)
    cache = {}
    if cache_path.exists():
        cache = load_json(cache_path)

    def t(text: str) -> str:
        key = (text or "").strip()
        if not key:
            return ""
        if key in cache:
            return cache[key]
        translated = translate_ko_to_zh(key)
        cache[key] = translated
        time.sleep(0.25)
        return translated

    grouped_cards: Dict[str, List[str]] = defaultdict(list)
    region_map_ko: Dict[str, str] = {}

    ongoing_sorted = sorted(
        ongoing,
        key=lambda x: ((x.get("region_name") or ""), (x.get("title") or "")),
    )

    for item in ongoing_sorted:
        key = str(item.get("url") or item.get("id") or item.get("title") or "")
        item_hash = None
        for h, meta in manifest.items():
            if meta.get("url") == key:
                item_hash = h
                break
        local_images = []
        if item_hash:
            local_images = [normalize_local_path(p) for p in manifest.get(item_hash, {}).get("images", [])]
        if not local_images:
            local_images = item.get("images") or []

        region_ko = item.get("region_name") or "未知区域"
        region_zh = translate_region_ko_to_zh(region_ko, t)
        region_map_ko[region_zh] = region_ko

        grouped_cards[region_zh].append(
            build_card(
                item=item,
                title_zh=t(item.get("title") or ""),
                desc_zh=t(item.get("description") or ""),
                image_paths=local_images,
                region_zh=region_zh,
                region_ko=region_ko,
            )
        )

    sections = []
    for region_zh in sorted(grouped_cards.keys()):
        cards = grouped_cards[region_zh]
        region_ko = region_map_ko.get(region_zh, region_zh)
        sections.append(
            f"""
            <section class="region-section">
              <h2 class="region-title">区域: {html.escape(region_zh)} · {len(cards)} 条</h2>
              <div class="grid">{''.join(cards)}</div>
            </section>
            """
        )

    page = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Karrot 在售商品中文整理</title>
  <style>
    body {{ font-family: -apple-system,BlinkMacSystemFont,'Segoe UI','PingFang SC','Hiragino Sans GB','Microsoft YaHei',sans-serif; margin: 24px; background: #f5f6f8; color: #1f2328; }}
    h1 {{ margin: 0 0 8px; }}
    .meta {{ margin: 0 0 20px; color: #667085; }}
    .region-section {{ margin-bottom: 24px; }}
    .region-title {{ font-size: 20px; margin: 0 0 12px; }}
    .grid {{ display: grid; grid-template-columns: 1fr; gap: 16px; }}
    .card {{ background: #fff; border: 1px solid #e4e7ec; border-radius: 14px; overflow: hidden; }}
    .images {{ display: grid; grid-template-columns: repeat(auto-fill,minmax(160px,1fr)); gap: 8px; padding: 12px; background: #fcfcfd; }}
    .images img {{ width: 100%; height: 170px; object-fit: cover; border-radius: 8px; border: 1px solid #eaecf0; }}
    .content {{ padding: 14px 16px 16px; }}
    .content h2 {{ font-size: 18px; margin: 0 0 8px; }}
    .content h2 a {{ text-decoration: none; color: #0f172a; }}
    .region {{ margin: 0 0 6px; color: #344054; font-weight: 600; }}
    .price {{ font-weight: 600; margin: 0 0 8px; color: #b54708; }}
    .desc {{ margin: 0 0 10px; line-height: 1.55; }}
    .link a {{ color: #175cd3; text-decoration: none; font-weight: 600; }}
    details {{ margin-top: 8px; color: #475467; }}
  </style>
</head>
<body>
  <h1>Karrot 在售商品中文整理</h1>
  <p class="meta">已过滤售罄（Closed）条目，仅保留在售（Ongoing）: {len(ongoing)} 条；并按区域分类展示</p>
  {''.join(sections)}
</body>
</html>
"""

    out_path = Path(args.output)
    out_path.write_text(page, encoding="utf-8")
    cache_path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"ongoing={len(ongoing)}")
    print(f"report={out_path}")
    print(f"cache={cache_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
