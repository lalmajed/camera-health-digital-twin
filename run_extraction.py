#!/usr/bin/env python3
"""
RIYADH PARCEL EXTRACTOR - Run as background process
Extracts remaining parcels with OBJECTID pagination
Progress is logged to extraction_log.txt
"""

import sys
import requests
import json
import csv
import time
import os
from datetime import datetime

# Unbuffered output
sys.stdout = open('/workspace/extraction_log.txt', 'w', buffering=1)
sys.stderr = sys.stdout

print("="*70)
print("RIYADH PARCEL EXTRACTOR - Completing remaining parcels")
print("="*70)
print(f"Started: {datetime.now()}")

BASE_URL = "https://mapservice.alriyadh.gov.sa/wa_maps/rest/services/BaseMap/Riyadh_BaseMap_V3/MapServer/71/query"
HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://mapservice.alriyadh.gov.sa/geoportal/geomap"}

LAND_USE_TYPES = {1000: "VILLA", 1012: "APARTMENT", 1100: "COMPLEX"}

def calc_centroid(rings):
    if not rings or not rings[0]:
        return None, None
    pts = rings[0]
    return sum(p[1] for p in pts)/len(pts), sum(p[0] for p in pts)/len(pts)

def get_oid_range(where):
    params = {
        "where": where,
        "outStatistics": json.dumps([
            {"statisticType": "min", "onStatisticField": "OBJECTID", "outStatisticFieldName": "MIN_OID"},
            {"statisticType": "max", "onStatisticField": "OBJECTID", "outStatisticFieldName": "MAX_OID"}
        ]),
        "f": "json"
    }
    try:
        r = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=60)
        d = r.json()
        if d.get("features"):
            a = d["features"][0]["attributes"]
            return a.get("MIN_OID") or a.get("min_oid"), a.get("MAX_OID") or a.get("max_oid")
    except Exception as e:
        print(f"OID range error: {e}")
    return None, None

def fetch_batch(where, existing_oids):
    params = {
        "where": where,
        "outFields": "OBJECTID,PARCELID,PARCELNO,BLOCKNO,PLANNO,DISTRICT,LANDUSEADETAILED",
        "returnGeometry": "true",
        "outSR": "4326",
        "f": "json"
    }
    try:
        r = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=180)
        d = r.json()
        parcels = []
        for f in d.get("features", []):
            a = f.get("attributes", {})
            oid = str(a.get("OBJECTID"))
            if oid in existing_oids:
                continue
            g = f.get("geometry", {})
            lat, lng = calc_centroid(g.get("rings", [])) if "rings" in g else (None, None)
            luc = a.get("LANDUSEADETAILED")
            parcels.append({
                "objectid": a.get("OBJECTID"),
                "parcel_id": a.get("PARCELID"),
                "parcel_no": a.get("PARCELNO"),
                "block_no": a.get("BLOCKNO"),
                "plan_no": a.get("PLANNO"),
                "district": a.get("DISTRICT"),
                "land_use_code": luc,
                "building_type": LAND_USE_TYPES.get(luc, "OTHER"),
                "latitude": round(lat, 6) if lat else None,
                "longitude": round(lng, 6) if lng else None
            })
        return parcels
    except Exception as e:
        print(f"Fetch error: {e}")
        return []

# Load existing
print("Loading existing data...")
existing = {}
filepath = "/workspace/riyadh_residential_parcels_geo.csv"
with open(filepath, 'r') as f:
    for row in csv.DictReader(f):
        d = row['district']
        if d not in existing:
            existing[d] = set()
        existing[d].add(row['objectid'])

initial_count = sum(len(v) for v in existing.values())
print(f"Loaded {initial_count:,} existing")

# Get expected
print("Getting expected counts...")
params = {
    "where": "LANDUSEADETAILED IN (1000, 1012, 1100)",
    "groupByFieldsForStatistics": "DISTRICT",
    "outStatistics": json.dumps([{"statisticType": "count", "onStatisticField": "OBJECTID", "outStatisticFieldName": "COUNT"}]),
    "f": "json"
}
r = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=120)
expected = {}
for f in r.json().get("features", []):
    a = f.get("attributes", {})
    c = a.get("DISTRICT")
    if c:
        expected[c] = int(a.get("COUNT", 0) or a.get("count", 0) or 0)

# Find truncated
truncated = []
for code, exp in expected.items():
    got = len(existing.get(code, set()))
    if got < exp:
        truncated.append({"code": code, "exp": exp, "got": got})
truncated.sort(key=lambda x: -(x["exp"]-x["got"]))

total_missing = sum(d['exp']-d['got'] for d in truncated)
print(f"Districts to process: {len(truncated)}")
print(f"Missing parcels: {total_missing:,}")
print("-"*70)

# Process
fieldnames = ["objectid", "parcel_id", "parcel_no", "block_no", "plan_no",
              "district", "land_use_code", "building_type", "latitude", "longitude"]

new_total = 0
start_time = time.time()

with open(filepath, "a", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    
    for i, district in enumerate(truncated):
        code = district["code"]
        exp = district["exp"]
        got = district["got"]
        existing_oids = existing.get(code, set())
        
        print(f"[{i+1}/{len(truncated)}] District {code}: need {exp-got:,} more...", end=" ")
        
        new_count = 0
        
        for land_use in [1000, 1012, 1100]:
            where_base = f"LANDUSEADETAILED={land_use} AND DISTRICT='{code}'"
            min_oid, max_oid = get_oid_range(where_base)
            
            if not min_oid:
                continue
            
            batch = 2000
            cur = min_oid
            retries = 0
            while cur <= max_oid:
                where = f"{where_base} AND OBJECTID >= {cur} AND OBJECTID < {cur + batch}"
                try:
                    parcels = fetch_batch(where, existing_oids)
                    
                    for p in parcels:
                        writer.writerow(p)
                        existing_oids.add(str(p["objectid"]))
                    
                    new_count += len(parcels)
                    cur += batch
                    retries = 0
                    time.sleep(0.05)
                except Exception as e:
                    retries += 1
                    if retries > 3:
                        print(f"Skipping batch after 3 retries: {e}")
                        cur += batch
                        retries = 0
                    else:
                        time.sleep(2)
        
        new_total += new_count
        print(f"+{new_count:,} (total: {got+new_count:,})")
        
        # Progress
        if (i + 1) % 10 == 0:
            elapsed = time.time() - start_time
            rate = new_total / elapsed * 60 if elapsed > 0 else 0
            remaining = total_missing - new_total
            eta_min = remaining / rate if rate > 0 else 0
            print(f"    --- Progress: {new_total:,}/{total_missing:,} ({100*new_total/total_missing:.1f}%), Rate: {rate:,.0f}/min, ETA: {eta_min:.0f} min ---")

print("\n" + "="*70)
print("EXTRACTION COMPLETE")
print("="*70)
print(f"New parcels: {new_total:,}")
print(f"Total time: {(time.time()-start_time)/60:.1f} min")

# Verify
with open(filepath, 'r') as f:
    total_lines = sum(1 for _ in f) - 1
print(f"Total in file: {total_lines:,}")
print(f"File size: {os.path.getsize(filepath)/(1024*1024):.1f} MB")
print(f"Finished: {datetime.now()}")
