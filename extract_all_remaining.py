#!/usr/bin/env python3
import requests
import json
import csv
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

print("="*60)
print("EXTRACTING ALL REMAINING PARCELS (294,382)")
print("="*60)

BASE_URL = "https://mapservice.alriyadh.gov.sa/wa_maps/rest/services/BaseMap/Riyadh_BaseMap_V3/MapServer/71/query"
HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://mapservice.alriyadh.gov.sa/geoportal/geomap"}

def centroid(rings):
    if not rings or not rings[0]: return None, None
    p = rings[0]
    return sum(x[1] for x in p)/len(p), sum(x[0] for x in p)/len(p)

def fetch(args):
    where, = args
    rows = []
    try:
        r = requests.get(BASE_URL, params={
            "where": where,
            "outFields": "OBJECTID,PARCELID,PARCELNO,BLOCKNO,PLANNO,DISTRICT,LANDUSEADETAILED",
            "returnGeometry": "true", "outSR": "4326", "f": "json"
        }, headers=HEADERS, timeout=120)
        for ft in r.json().get("features", []):
            a = ft["attributes"]
            lat, lng = centroid(ft.get("geometry", {}).get("rings", []))
            rows.append([
                a.get("OBJECTID"), a.get("PARCELID"), a.get("PARCELNO"),
                a.get("BLOCKNO"), a.get("PLANNO"), a.get("DISTRICT"),
                a.get("LANDUSEADETAILED"),
                round(lat, 6) if lat else "", round(lng, 6) if lng else ""
            ])
    except Exception as e:
        pass
    return rows

# Get all codes we haven't extracted yet
params = {
    "where": "LANDUSEADETAILED NOT IN (1000, 1012, 1100, 0) AND LANDUSEADETAILED IS NOT NULL",
    "groupByFieldsForStatistics": "LANDUSEADETAILED",
    "outStatistics": json.dumps([{"statisticType": "count", "onStatisticField": "OBJECTID", "outStatisticFieldName": "COUNT"}]),
    "f": "json"
}
r = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=120)

missing_codes = []
for f in r.json().get("features", []):
    a = f["attributes"]
    code = a.get("LANDUSEADETAILED")
    count = int(a.get("COUNT") or a.get("count") or 0)
    if code and count > 0:
        missing_codes.append((code, count))

missing_codes.sort(key=lambda x: -x[1])
total_expected = sum(c for _, c in missing_codes)
print(f"Codes to extract: {len(missing_codes)}")
print(f"Expected parcels: {total_expected:,}")

# Build all tasks
print("Building task list...", flush=True)
all_tasks = []

for code, _ in missing_codes:
    where_base = f"LANDUSEADETAILED = {code}"
    try:
        params = {
            "where": where_base,
            "outStatistics": json.dumps([
                {"statisticType": "min", "onStatisticField": "OBJECTID", "outStatisticFieldName": "MIN_OID"},
                {"statisticType": "max", "onStatisticField": "OBJECTID", "outStatisticFieldName": "MAX_OID"}
            ]),
            "f": "json"
        }
        d = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=60).json()
        a = d["features"][0]["attributes"]
        mn = a.get("MIN_OID") or a.get("min_oid")
        mx = a.get("MAX_OID") or a.get("max_oid")
        if mn and mx:
            cur = mn
            while cur <= mx:
                all_tasks.append((f"{where_base} AND OBJECTID >= {cur} AND OBJECTID < {cur + 2000}",))
                cur += 2000
    except:
        pass

print(f"Total batches: {len(all_tasks)}")
print("Starting parallel extraction with 8 threads...", flush=True)

filepath = "/workspace/riyadh_all_other_parcels.csv"
total = 0
start = time.time()

with open(filepath, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["objectid", "parcel_id", "parcel_no", "block_no", "plan_no", "district", "land_use_code", "latitude", "longitude"])
    
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(fetch, t): t for t in all_tasks}
        done = 0
        for future in as_completed(futures):
            rows = future.result()
            for row in rows:
                w.writerow(row)
            total += len(rows)
            done += 1
            if done % 20 == 0:
                elapsed = time.time() - start
                rate = total / elapsed * 60 if elapsed > 0 else 0
                pct = 100 * total / total_expected
                print(f"[{done}/{len(all_tasks)}] {total:,} parcels ({pct:.1f}%) | {rate:,.0f}/min", flush=True)
                f.flush()

print(f"\n{'='*60}")
print(f"DONE: {total:,} parcels extracted")
print(f"File: {filepath}")
print(f"Time: {(time.time()-start)/60:.1f} min")
