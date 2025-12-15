#!/usr/bin/env python3
import requests, json, csv, time
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL = "https://mapservice.alriyadh.gov.sa/wa_maps/rest/services/BaseMap/Riyadh_BaseMap_V3/MapServer/71/query"
HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://mapservice.alriyadh.gov.sa/geoportal/geomap"}

def centroid(rings):
    if not rings or not rings[0]: return None, None
    p = rings[0]
    return sum(x[1] for x in p)/len(p), sum(x[0] for x in p)/len(p)

def fetch(args):
    where, code = args
    rows = []
    try:
        r = requests.get(BASE_URL, params={"where": where,
            "outFields": "OBJECTID,PARCELID,PARCELNO,BLOCKNO,PLANNO,DISTRICT,LANDUSEADETAILED",
            "returnGeometry": "true", "outSR": "4326", "f": "json"}, headers=HEADERS, timeout=120)
        for ft in r.json().get("features", []):
            at = ft["attributes"]
            lat, lng = centroid(ft.get("geometry", {}).get("rings", []))
            rows.append([at.get("OBJECTID"), at.get("PARCELID"), at.get("PARCELNO"), at.get("BLOCKNO"),
                at.get("PLANNO"), code, at.get("LANDUSEADETAILED"), "NEEDS_VERIFICATION",
                round(lat,6) if lat else "", round(lng,6) if lng else ""])
    except: pass
    return rows

print("Getting districts...", flush=True)
r = requests.get(BASE_URL, params={"where": "LANDUSEADETAILED IS NULL OR LANDUSEADETAILED = 0", 
    "groupByFieldsForStatistics": "DISTRICT",
    "outStatistics": json.dumps([{"statisticType": "count", "onStatisticField": "OBJECTID", "outStatisticFieldName": "COUNT"}]), 
    "f": "json"}, headers=HEADERS, timeout=120)
districts = [(a.get("DISTRICT"), int(a.get("COUNT") or a.get("count") or 0)) 
    for f in r.json().get("features", []) for a in [f["attributes"]] if a.get("DISTRICT")]
districts.sort(key=lambda x: -x[1])
print(f"Found {len(districts)} districts, {sum(c for _,c in districts):,} parcels", flush=True)

print("Building task list...", flush=True)
all_tasks = []
for code, _ in districts:
    where = f"(LANDUSEADETAILED IS NULL OR LANDUSEADETAILED = 0) AND DISTRICT='{code}'"
    try:
        d = requests.get(BASE_URL, params={"where": where, "outStatistics": json.dumps([
            {"statisticType": "min", "onStatisticField": "OBJECTID", "outStatisticFieldName": "MIN_OID"},
            {"statisticType": "max", "onStatisticField": "OBJECTID", "outStatisticFieldName": "MAX_OID"}]), "f": "json"}, 
            headers=HEADERS, timeout=60).json()
        a = d["features"][0]["attributes"]
        mn, mx = a.get("MIN_OID") or a.get("min_oid"), a.get("MAX_OID") or a.get("max_oid")
        if mn:
            cur = mn
            while cur <= mx:
                all_tasks.append((f"{where} AND OBJECTID >= {cur} AND OBJECTID < {cur+2000}", code))
                cur += 2000
    except: pass

print(f"Total batches: {len(all_tasks)}", flush=True)
print("Starting parallel extraction with 8 threads...", flush=True)

total = 0
start = time.time()
with open("/workspace/riyadh_verification_needed_parcels.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["objectid","parcel_id","parcel_no","block_no","plan_no","district","land_use_code","status","latitude","longitude"])
    
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(fetch, t): t for t in all_tasks}
        done = 0
        for future in as_completed(futures):
            rows = future.result()
            for row in rows:
                w.writerow(row)
            total += len(rows)
            done += 1
            if done % 10 == 0:
                elapsed = time.time() - start
                rate = total / elapsed * 60 if elapsed > 0 else 0
                print(f"[{done}/{len(all_tasks)}] {total:,} parcels | {rate:,.0f}/min", flush=True)
                f.flush()

print(f"\nDONE: {total:,} parcels in {(time.time()-start)/60:.1f} min", flush=True)
