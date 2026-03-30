#!/usr/bin/env python3
import argparse
import html
import json
import re
import time
from datetime import datetime
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import quote
from urllib.request import Request, urlopen

UA = "Mozilla/5.0"
KOREAN_RE = re.compile(r"[\uac00-\ud7a3]")
EN_RE = re.compile(r"[A-Za-z]")
ZH_RE = re.compile(r"[\u4e00-\u9fff]")


def has_korean(s: str) -> bool:
    return bool(KOREAN_RE.search(s or ""))


def has_english(s: str) -> bool:
    return bool(EN_RE.search(s or ""))


def has_chinese(s: str) -> bool:
    return bool(ZH_RE.search(s or ""))


def split_chunks(text: str, max_len: int = 180) -> List[str]:
    text = (text or "").strip()
    if not text:
        return [""]
    lines = [x.strip() for x in text.split("\n")]
    out: List[str] = []
    for ln in lines:
        if not ln:
            out.append("")
            continue
        if len(ln) <= max_len:
            out.append(ln)
            continue
        buf = ""
        for token in re.split(r"([,.!?;:])", ln):
            if not token:
                continue
            if len(buf) + len(token) > max_len and buf:
                out.append(buf)
                buf = token
            else:
                buf += token
        if buf:
            out.append(buf)
    return out


def tr_google(text: str, timeout: int = 4) -> str:
    u = (
        "https://translate.googleapis.com/translate_a/single"
        f"?client=gtx&sl=ko&tl=zh-CN&dt=t&q={quote(text)}"
    )
    req = Request(u, headers={"User-Agent": UA})
    with urlopen(req, timeout=timeout) as r:
        data = json.loads(r.read().decode("utf-8", errors="ignore"))
    parts = data[0] if isinstance(data, list) and data else []
    return "".join(seg[0] for seg in parts if seg and isinstance(seg, list) and seg[0])


def tr_google_auto_to_zh(text: str, timeout: int = 4) -> str:
    u = (
        "https://translate.googleapis.com/translate_a/single"
        f"?client=gtx&sl=auto&tl=zh-CN&dt=t&q={quote(text)}"
    )
    req = Request(u, headers={"User-Agent": UA})
    with urlopen(req, timeout=timeout) as r:
        data = json.loads(r.read().decode("utf-8", errors="ignore"))
    parts = data[0] if isinstance(data, list) and data else []
    return "".join(seg[0] for seg in parts if seg and isinstance(seg, list) and seg[0])


def tr_mymemory(text: str, timeout: int = 4) -> str:
    u = f"https://api.mymemory.translated.net/get?q={quote(text)}&langpair=ko|zh-CN"
    req = Request(u, headers={"User-Agent": UA})
    with urlopen(req, timeout=timeout) as r:
        data = json.loads(r.read().decode("utf-8", errors="ignore"))
    return (((data or {}).get("responseData") or {}).get("translatedText") or "").strip()


def translate_ko_to_zh(text: str, cache: Dict[str, str]) -> str:
    key = (text or "").strip()
    if not key:
        return ""
    if key in cache and not has_korean(cache[key]):
        return cache[key]

    chunks = split_chunks(key)
    translated_chunks: List[str] = []
    for ch in chunks:
        if not ch:
            translated_chunks.append("")
            continue
        if not has_korean(ch):
            translated_chunks.append(ch)
            continue
        done = ""
        try:
            done = tr_google(ch)
        except Exception:
            done = ""
        if (not done) or has_korean(done):
            try:
                done = tr_mymemory(ch)
            except Exception:
                done = ""
        translated_chunks.append(done or ch)
    out = "\n".join(translated_chunks).strip() or key
    cache[key] = out
    return out


def translate_any_to_zh(text: str, cache: Dict[str, str]) -> str:
    key = (text or "").strip()
    if not key:
        return ""
    if key in cache and has_chinese(cache[key]) and (not has_korean(cache[key])) and (not has_english(cache[key])):
        return cache[key]
    out = ""
    try:
        out = tr_google_auto_to_zh(key)
    except Exception:
        out = ""
    if (not out) or has_korean(out) or has_english(out) or (not has_chinese(out)):
        if has_korean(key):
            out = translate_ko_to_zh(key, cache)
    out = (out or key).strip()
    cache[key] = out
    return out


def normalize_to_chinese(text: str, cache: Dict[str, str], fallback: str) -> str:
    candidate = (text or "").strip()
    backup = (fallback or "").strip()
    if candidate and has_chinese(candidate) and (not has_korean(candidate)) and (not has_english(candidate)):
        return candidate
    source = candidate or backup
    translated = translate_any_to_zh(source, cache)
    if translated and has_chinese(translated) and (not has_korean(translated)) and (not has_english(translated)):
        return translated
    if backup:
        translated = translate_any_to_zh(backup, cache)
        if translated and has_chinese(translated) and (not has_korean(translated)) and (not has_english(translated)):
            return translated
    return "该字段翻译失败，请点击原帖查看。"


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


def build_html(items: List[Dict], html_path: Path, max_age_days: Optional[int] = None) -> None:
    age_text = age_window_text(max_age_days)
    groups = defaultdict(list)
    for it in items:
        groups[it["region_zh"]].append(it)

    sections = []
    for region in sorted(groups.keys()):
        cards = []
        for it in groups[region]:
            imgs = it.get("display_images", [])
            img_html = "".join(
                f'<a class="img-link" href="{html.escape(it["url"])}" target="_blank" rel="noopener">'
                f'<img src="{html.escape(p)}" alt="{html.escape(it["title_ko"])}"/></a>'
                for p in imgs
            )
            desc_zh = html.escape(it.get("description_zh", "")).replace("\n", "<br/>")
            desc_ko = html.escape(it.get("description_ko", "")).replace("\n", "<br/>")
            created = html.escape(format_created_at(it.get("created_at") or ""))
            price = it.get("price")
            try:
                price_txt = f"₩{int(float(price)):,}"
            except Exception:
                price_txt = "-"
            cards.append(
                f"""
                <article class="card">
                  <div class="images">{img_html}</div>
                  <div class="content">
                    <h3><a href="{html.escape(it["url"])}" target="_blank" rel="noopener">{html.escape(it["title_zh"])}</a></h3>
                    <p class="region">区域: {html.escape(it["region_zh"])}</p>
                    <p class="region">发布时间: {created}</p>
                    <p class="price">价格: {price_txt}</p>
                    <p class="desc">{desc_zh}</p>
                    <details><summary>查看韩文原文</summary>
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
              <h2 class="region-title">区域: {html.escape(region)} · {len(cards)} 条</h2>
              <div class="grid">{''.join(cards)}</div>
            </section>
            """
        )

    html_doc = f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="UTF-8"/><meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Karrot 多区域在售中文整理(修复版)</title>
<style>
body {{ font-family: -apple-system,BlinkMacSystemFont,'Segoe UI','PingFang SC','Microsoft YaHei',sans-serif; margin: 24px; background: #f5f6f8; color: #1f2328; }}
.region-section {{ margin-bottom: 24px; }} .region-title {{ font-size:20px; margin:0 0 12px; }}
.grid {{ display:grid; grid-template-columns:1fr; gap:16px; }} .card {{ background:#fff; border:1px solid #e4e7ec; border-radius:14px; overflow:hidden; }}
.images {{ display:grid; grid-template-columns: repeat(auto-fill,minmax(160px,1fr)); gap:8px; padding:12px; background:#fcfcfd; }}
.images img {{ width:100%; height:170px; object-fit:cover; border-radius:8px; border:1px solid #eaecf0; }}
.content {{ padding:14px 16px 16px; }} .content h3 {{ margin:0 0 8px; font-size:18px; }} .content h3 a {{ color:#0f172a; text-decoration:none; }}
.region {{ margin:0 0 6px; color:#344054; font-weight:600; }} .price {{ margin:0 0 8px; color:#b54708; font-weight:600; }}
.desc {{ margin:0 0 10px; line-height:1.55; }} .link a {{ color:#175cd3; text-decoration:none; font-weight:600; }}
</style></head><body>
<h1>Karrot 多区域在售中文整理(修复版)</h1>
<p>仅展示{html.escape(age_text)}发布的在售内容。已修复：优先本地图片，不足时回退远程图片；韩文介绍二次翻译为中文。</p>
{''.join(sections)}
</body></html>"""
    html_path.write_text(html_doc, encoding="utf-8")


def summarize_desc(text: str, max_len: int = 120) -> str:
    clean = " ".join((text or "").split())
    if len(clean) <= max_len:
        return clean
    return clean[: max_len - 1].rstrip() + "…"


def build_share_html(items: List[Dict], brand: str, html_path: Path, max_age_days: Optional[int] = None) -> None:
    age_text = age_window_text(max_age_days)
    cards = []
    for it in items:
        share_image = (it.get("thumbnail") or "").strip()
        if not share_image:
            remote_images = it.get("remote_images") or []
            if remote_images:
                share_image = remote_images[0]
        title = html.escape(it.get("title_zh") or it.get("title_ko") or "")
        region = html.escape(it.get("region_zh") or "")
        desc = html.escape(summarize_desc(it.get("description_zh") or ""))
        created = html.escape(format_created_at(it.get("created_at") or ""))
        url = html.escape(it.get("url") or "")
        try:
            price_txt = f"₩{int(float(it.get('price'))):,}"
        except Exception:
            price_txt = "-"
        image_html = (
            f'<div class="thumb-wrap"><img class="thumb" src="{html.escape(share_image)}" alt="{html.escape(it.get("title_ko") or "")}"/></div>'
            if share_image
            else '<div class="thumb-wrap thumb-empty">暂无图片</div>'
        )
        cards.append(
            f"""
            <article class="card">
              {image_html}
              <div class="body">
                <h2>{title}</h2>
                <p class="meta">区域：{region}</p>
                <p class="meta">价格：{price_txt}</p>
                <p class="meta">发布时间：{created}</p>
                <p class="desc">{desc}</p>
                <p class="actions"><a href="{url}">打开原始链接</a></p>
              </div>
            </article>
            """
        )

    html_doc = f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="UTF-8"/><meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover"/>
<title>Karrot 品牌 {html.escape(brand)} 分享版</title>
<style>
:root {{ --bg:#f3f0e8; --paper:#fffdf8; --ink:#1f2328; --muted:#6b665c; --line:#e7dfd0; --accent:#b55d2d; }}
* {{ box-sizing:border-box; }}
body {{ margin:0; font-family:-apple-system,BlinkMacSystemFont,'PingFang SC','Microsoft YaHei',sans-serif; color:var(--ink); background:linear-gradient(180deg,#efe6d7 0%,#f7f3ec 180px,var(--bg) 100%); }}
.page {{ max-width:760px; margin:0 auto; padding:20px 14px 40px; }}
.hero {{ padding:18px 16px; border-radius:20px; background:rgba(255,253,248,0.88); border:1px solid rgba(181,93,45,0.12); box-shadow:0 10px 30px rgba(97,74,42,0.08); margin-bottom:16px; }}
.hero h1 {{ margin:0 0 8px; font-size:24px; line-height:1.2; }}
.hero p {{ margin:0; color:var(--muted); line-height:1.5; }}
.list {{ display:grid; grid-template-columns:1fr; gap:12px; }}
.card {{ display:grid; grid-template-columns:112px 1fr; gap:12px; background:var(--paper); border:1px solid var(--line); border-radius:18px; padding:12px; box-shadow:0 8px 24px rgba(50,44,33,0.05); break-inside:avoid; }}
.thumb-wrap {{ width:112px; min-height:112px; border-radius:14px; overflow:hidden; background:#f0ece4; display:flex; align-items:center; justify-content:center; color:#9a8d78; font-size:13px; }}
.thumb {{ width:112px; height:112px; object-fit:cover; display:block; }}
.body h2 {{ margin:0 0 8px; font-size:17px; line-height:1.35; }}
.meta {{ margin:0 0 4px; font-size:13px; color:var(--muted); }}
.desc {{ margin:8px 0 0; font-size:14px; line-height:1.5; }}
.actions {{ margin:10px 0 0; }}
.actions a {{ color:var(--accent); text-decoration:none; font-weight:700; }}
@media (max-width: 560px) {{
  .page {{ padding:14px 10px 28px; }}
  .card {{ grid-template-columns:88px 1fr; gap:10px; padding:10px; border-radius:16px; }}
  .thumb-wrap, .thumb {{ width:88px; height:88px; min-height:88px; }}
  .body h2 {{ font-size:16px; }}
}}
</style></head>
<body><main class="page">
  <section class="hero">
    <h1>Karrot 品牌 {html.escape(brand)} 分享版</h1>
    <p>仅展示{html.escape(age_text)}发布的在售内容。适合手机查看与微信转发，图片使用远程缩略图，链接可直接打开原始 Karrot 页面。</p>
  </section>
  <section class="list">{''.join(cards)}</section>
</main></body></html>"""
    html_path.write_text(html_doc, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-json", default="output/multi_region_pass2/karrot_multi_region_ongoing_cn_local.json")
    parser.add_argument("--details-json", default="output/multi_region_pass2/details.json")
    parser.add_argument("--out-json", default="output/multi_region_pass2/karrot_multi_region_ongoing_cn_local_fixed.json")
    parser.add_argument("--out-html", default="output/multi_region_pass2/karrot_multi_region_ongoing_cn_fixed.html")
    parser.add_argument("--cache", default="output/multi_region_pass2/translation_cache_fix.json")
    parser.add_argument("--workers", type=int, default=12)
    parser.add_argument("--fix-region-zh", dest="fix_region_zh", action="store_true")
    parser.add_argument("--no-fix-region-zh", dest="fix_region_zh", action="store_false")
    parser.set_defaults(fix_region_zh=True)
    args = parser.parse_args()

    items = json.loads(Path(args.input_json).read_text("utf-8"))
    details = json.loads(Path(args.details_json).read_text("utf-8"))
    by_url = {d.get("url"): d for d in details if d.get("url")}

    cache_path = Path(args.cache)
    if cache_path.exists():
        cache = json.loads(cache_path.read_text("utf-8"))
    else:
        cache = {}

    def fix_one(it: Dict) -> Dict:
        r = dict(it)
        d = by_url.get(r.get("url"), {})
        remote_images = d.get("images") if isinstance(d.get("images"), list) else []
        local_images = r.get("local_images") or []
        existing_display = r.get("display_images") or []
        if existing_display:
            r["display_images"] = existing_display
        elif local_images:
            r["display_images"] = local_images
        else:
            r["display_images"] = remote_images[:3]

        title_ko = r.get("title_ko", "")
        desc_ko = r.get("description_ko", "")
        title_zh = r.get("title_zh", "")
        desc_zh = r.get("description_zh", "")

        title_zh = normalize_to_chinese(title_zh, cache, title_ko)
        desc_zh = normalize_to_chinese(desc_zh, cache, desc_ko)
        if args.fix_region_zh:
            region_ko = r.get("region_ko", "")
            region_zh = r.get("region_zh", "")
            if region_ko and ((not region_zh.strip()) or has_korean(region_zh) or has_english(region_zh) or (not has_chinese(region_zh))):
                region_zh = translate_any_to_zh(region_ko, cache)
            if (not has_chinese(region_zh)) or has_korean(region_zh) or has_english(region_zh):
                region_zh = translate_any_to_zh(region_zh or region_ko, cache)
            r["region_zh"] = region_zh

        r["title_zh"] = title_zh
        r["description_zh"] = desc_zh
        return r

    fixed: List[Dict] = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        futs = [ex.submit(fix_one, it) for it in items]
        total = len(futs)
        done = 0
        for fut in as_completed(futs):
            done += 1
            fixed.append(fut.result())
            if done % 50 == 0 or done == total:
                print(f"[INFO] fix progress {done}/{total}", flush=True)

    Path(args.out_json).write_text(json.dumps(fixed, ensure_ascii=False, indent=2), encoding="utf-8")
    cache_path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    build_html(fixed, Path(args.out_html))
    print(f"[DONE] json={args.out_json}")
    print(f"[DONE] html={args.out_html}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
