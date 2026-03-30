#!/usr/bin/env python3
import argparse
import importlib.util
import json
import re
from pathlib import Path
from typing import Dict, List

KOREAN_RE = re.compile(r"[\uac00-\ud7a3]")
EN_RE = re.compile(r"[A-Za-z]")
ZH_RE = re.compile(r"[\u4e00-\u9fff]")


def load_module(path: str, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def needs_region_fix(value: str) -> bool:
    text = (value or "").strip()
    return (not text) or bool(KOREAN_RE.search(text)) or bool(EN_RE.search(text)) or (not ZH_RE.search(text))


def brand_from_filename(path: Path) -> str:
    stem = path.stem
    if stem.startswith("karrot_") and "_ongoing_cn_" in stem:
        return stem[len("karrot_") : stem.index("_ongoing_cn_")]
    return path.parent.name.split("_", 1)[0]


def repair_items(items: List[Dict], cache: Dict[str, str], fixmod) -> List[Dict]:
    repaired: List[Dict] = []
    for item in items:
        row = dict(item)
        title_ko = row.get("title_ko", "")
        desc_ko = row.get("description_ko", "")
        region_ko = row.get("region_ko", "")

        if KOREAN_RE.search(row.get("title_zh", "")) or not (row.get("title_zh") or "").strip():
            translated = fixmod.translate_ko_to_zh(title_ko, cache)
            row["title_zh"] = translated if translated else "该条标题翻译失败，请点开原帖查看。"

        if KOREAN_RE.search(row.get("description_zh", "")) or not (row.get("description_zh") or "").strip():
            translated = fixmod.translate_ko_to_zh(desc_ko, cache)
            row["description_zh"] = translated if translated else "该条介绍翻译失败，请点击原帖查看。"

        if KOREAN_RE.search(row.get("title_zh", "")):
            row["title_zh"] = "该条标题翻译失败，请点开原帖查看。"

        if KOREAN_RE.search(row.get("description_zh", "")):
            row["description_zh"] = "该条介绍翻译失败，请点击原帖查看。"

        if needs_region_fix(row.get("region_zh", "")):
            translated = fixmod.translate_any_to_zh(region_ko, cache) if region_ko else ""
            row["region_zh"] = translated if translated and not needs_region_fix(translated) else "韩国未命名地区"

        repaired.append(row)
    return repaired


def main() -> int:
    parser = argparse.ArgumentParser(description="Repair existing brand result JSON/HTML to Chinese.")
    parser.add_argument("--root", default="output/brand_runs")
    args = parser.parse_args()

    root = Path(args.root)
    fixmod = load_module(str(Path(__file__).with_name("fix_multi_region_output.py")), "fixmod")
    brandmod = load_module(str(Path(__file__).with_name("run_brand_pipeline.py")), "brandmod")

    json_paths = sorted(root.glob("*/karrot_*_ongoing_cn_by_time.json"))
    json_paths += sorted(root.glob("*/karrot_*_ongoing_cn_fixed.json"))
    seen = set()

    for json_path in json_paths:
        if json_path.as_posix() in seen:
            continue
        seen.add(json_path.as_posix())
        items = json.loads(json_path.read_text("utf-8"))
        cache_path = json_path.parent / "translation_cache.json"
        if cache_path.exists():
            try:
                cache = json.loads(cache_path.read_text("utf-8"))
            except Exception:
                cache = {}
        else:
            cache = {}

        repaired = repair_items(items, cache, fixmod)

        if json_path.name.endswith("_by_time.json"):
            repaired.sort(key=lambda x: x.get("created_at") or "", reverse=True)
            brand = brand_from_filename(json_path)
            brandmod.build_html(repaired, brand, json_path.with_suffix(".html"))
        else:
            repaired.sort(key=lambda x: (x.get("region_zh", ""), x.get("title_zh", "")))
            fixmod.build_html(repaired, json_path.with_suffix(".html"))

        json_path.write_text(json.dumps(repaired, ensure_ascii=False, indent=2), encoding="utf-8")
        cache_path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")

        title_ko = sum(1 for x in repaired if KOREAN_RE.search(x.get("title_zh", "")))
        desc_ko = sum(1 for x in repaired if KOREAN_RE.search(x.get("description_zh", "")))
        region_bad = sum(1 for x in repaired if needs_region_fix(x.get("region_zh", "")))
        print(f"[DONE] {json_path} items={len(repaired)} title_ko={title_ko} desc_ko={desc_ko} region_bad={region_bad}", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
