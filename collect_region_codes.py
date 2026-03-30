#!/usr/bin/env python3
import csv
import json
import time
from urllib.error import HTTPError
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urlencode
from urllib.request import Request, urlopen

API = "https://www.daangn.com/v1/api/search/kr/location"
UA = "Mozilla/5.0"

PROVINCES = [
    "서울특별시",
    "부산광역시",
    "대구광역시",
    "인천광역시",
    "광주광역시",
    "대전광역시",
    "울산광역시",
    "세종특별자치시",
    "경기도",
    "강원특별자치도",
    "충청북도",
    "충청남도",
    "전북특별자치도",
    "전라남도",
    "경상북도",
    "경상남도",
    "제주특별자치도",
]

SUFFIX_HINTS = ["시", "군", "구", "동", "읍", "면", "가", "로"]


def fetch_locations(keyword: str, retries: int = 5, timeout: int = 10) -> List[Dict]:
    q = urlencode({"keyword": keyword})
    url = f"{API}?{q}"
    err: Optional[Exception] = None
    for i in range(retries):
        try:
            req = Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
            with urlopen(req, timeout=timeout) as resp:
                body = resp.read().decode("utf-8", errors="ignore")
            data = json.loads(body)
            locations = data.get("locations", [])
            return locations if isinstance(locations, list) else []
        except HTTPError as e:
            err = e
            if e.code == 429:
                sleep_s = min(20.0, 2.0 * (i + 1))
            else:
                sleep_s = 0.5 * (i + 1)
            time.sleep(sleep_s)
        except Exception as e:
            err = e
            time.sleep(0.5 * (i + 1))
    print(f"[WARN] fetch failed keyword={keyword!r} err={err}")
    return []


def region_slug(loc: Dict) -> str:
    name = (loc.get("name") or "").strip()
    rid = loc.get("id")
    return f"{name}-{rid}" if name and rid is not None else ""


def add_locations(target: Dict[int, Dict], locs: List[Dict], province: Optional[str] = None, district: Optional[str] = None) -> None:
    for x in locs:
        try:
            rid = int(x["id"])
        except Exception:
            continue
        if province and x.get("name1") != province:
            continue
        if district is not None and (x.get("name2") or None) != district:
            continue
        if rid not in target:
            target[rid] = x


def build_keywords_for_province(p: str) -> List[str]:
    kws = [p]
    kws.extend([f"{p} {s}" for s in SUFFIX_HINTS])
    return kws


def build_keywords_for_district(province: str, district: str) -> List[str]:
    kws = [
        f"{province} {district}",
        district,
    ]
    for suffix in SUFFIX_HINTS:
        kws.append(f"{province} {district} {suffix}")
        kws.append(f"{district} {suffix}")
    # A small set of direct location-type hints catches cases where bare district
    # queries are rank-limited and do not expose all depth-3 neighborhoods.
    kws.extend(
        [
            f"{province} {district} 동",
            f"{province} {district} 읍",
            f"{province} {district} 면",
            f"{province} {district} 가",
            f"{district} 동",
            f"{district} 읍",
            f"{district} 면",
            f"{district} 가",
        ]
    )
    return list(dict.fromkeys(kws))


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Collect Daangn Korea location ids/slugs.")
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()

    out_dir = Path("output")
    out_dir.mkdir(parents=True, exist_ok=True)
    cache_path = out_dir / "daangn_region_query_cache.json"

    all_regions: Dict[int, Dict] = {}
    districts_by_province: Dict[str, Set[Optional[str]]] = {}
    if cache_path.exists():
        try:
            cache: Dict[str, List[Dict]] = json.loads(cache_path.read_text("utf-8"))
        except Exception:
            cache = {}
    else:
        cache = {}

    # Rehydrate previously discovered locations so incremental reruns do not
    # start from zero when most queries are already cached.
    for locs in cache.values():
        if isinstance(locs, list):
            add_locations(all_regions, locs)

    # Step 1: discover districts (name2) for each province
    print("[INFO] Step1: discover province/district candidates", flush=True)
    for province in PROVINCES:
        districts: Set[Optional[str]] = set()
        for kw in build_keywords_for_province(province):
            if kw not in cache:
                cache[kw] = fetch_locations(kw)
            locs = cache[kw]
            add_locations(all_regions, locs, province=province)
            for loc in locs:
                if loc.get("name1") == province:
                    districts.add(loc.get("name2"))
        districts_by_province[province] = districts
        print(
            f"[INFO] {province}: districts={len([d for d in districts if d])} (raw={len(districts)})",
            flush=True,
        )

    # Step 2: for each district, query deeper to enumerate name3 regions
    print("[INFO] Step2: enumerate regions by province+district", flush=True)
    jobs: List[Tuple[str, Optional[str], str]] = []
    for province, districts in districts_by_province.items():
        if districts == {None}:
            # Special case such as Sejong
            for s in ["", *SUFFIX_HINTS]:
                kw = province if s == "" else f"{province} {s}"
                jobs.append((province, None, kw))
            continue
        for d in sorted([x for x in districts if x]):
            for kw in build_keywords_for_district(province, d):
                jobs.append((province, d, kw))

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        fut_map = {}
        for province, district, kw in jobs:
            if kw in cache:
                continue
            fut_map[ex.submit(fetch_locations, kw)] = (province, district, kw)

        done = 0
        total = len(fut_map)
        for fut in as_completed(fut_map):
            province, district, kw = fut_map[fut]
            locs = fut.result()
            cache[kw] = locs
            add_locations(all_regions, locs, province=province, district=district)
            done += 1
            if done % 50 == 0 or done == total:
                print(f"[INFO] Step2 progress: {done}/{total}, regions={len(all_regions)}", flush=True)

    # Build outputs
    rows = []
    for rid, loc in sorted(all_regions.items(), key=lambda t: (t[1].get("name1") or "", t[1].get("name2") or "", t[1].get("name3") or "", t[0])):
        row = {
            "id": rid,
            "name": loc.get("name"),
            "name1": loc.get("name1"),
            "name2": loc.get("name2"),
            "name3": loc.get("name3"),
            "depth": loc.get("depth"),
            "name1Id": loc.get("name1Id"),
            "name2Id": loc.get("name2Id"),
            "name3Id": loc.get("name3Id"),
            "region_slug": region_slug(loc),
            "in_param_example": f"in={region_slug(loc)}" if region_slug(loc) else "",
        }
        rows.append(row)

    json_path = out_dir / "daangn_region_codes_kr.json"
    csv_path = out_dir / "daangn_region_codes_kr.csv"
    slug_list_path = out_dir / "daangn_region_slug_list.txt"

    json_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "id",
                "name",
                "name1",
                "name2",
                "name3",
                "depth",
                "name1Id",
                "name2Id",
                "name3Id",
                "region_slug",
                "in_param_example",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    slug_list_path.write_text(
        "\n".join(row["region_slug"] for row in rows if row.get("region_slug")) + "\n",
        encoding="utf-8",
    )

    # cache is useful for incremental reruns / verification
    cache_path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[DONE] regions={len(rows)}", flush=True)
    print(f"[DONE] json={json_path}", flush=True)
    print(f"[DONE] csv={csv_path}", flush=True)
    print(f"[DONE] cache={cache_path}", flush=True)
    print(f"[DONE] slug_list={slug_list_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
