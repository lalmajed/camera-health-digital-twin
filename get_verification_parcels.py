#!/usr/bin/env python3
import requests, json, csv, time

print("="*50)
print("Extracting 'Needs Verification' Parcels")
print("="*50)

BASE_URL = "https://mapservice.alriyadh.gov.sa/wa_maps/rest/services/BaseMap/Riyadh_BaseMap_V3/MapServer/71/query"
HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://mapservice.alriyadh.gov.sa/geoportal/geomap"}

def calc_centroid(rings):
    if not rings or not rings[0]: return None, None
    pts = rings[0]
    return sum(p[1] for p in pts)/len(pts), sum(p[0] for p in pts)/len(pts)

# Get districts
params = {"where": "LANDUSEADETAILED IS NULL OR LANDUSEADETAILED = 0", 
    "groupByFieldsForStatistics": "DISTRICT",
    "outStatistics": json.dumps([{"statisticType": "count", "onStatisticField": "OBJECTID", "outStatisticFieldName": "COUNT"}]), 
    "f": "json"}
r = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=120)
districts = sorted([(a.get("DISTRICT"), int(a.get("COUNT", 0) or a.get("count", 0) or 0)) 
    for f in r.json().get("features", []) for a in [f.get("attributes", {})] if a.get("DISTRICT")], key=lambda x: -x[1])

total_expected = sum(c for _,c in districts)
print(f"Found {len(districts)} districts, {total_expected:,} parcels")

filepath = "/workspace/riyadh_verification_needed_parcels.csv"
fieldnames = ["objectid", "parcel_id", "parcel_no", "block_no", "plan_no", "district", "land_use_code", "status", "latitude", "longitude"]
total = 0

with open(filepath, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    
    for i, (code, expected) in enumerate(districts):
        where = f"(LANDUSEADETAILED IS NULL OR LANDUSEADETAILED = 0) AND DISTRICT='{code}'"
        
        try:
            r = requests.get(BASE_URL, params={
                "where": where,
                "outFields": "OBJECTID,PARCELID,PARCELNO,BLOCKNO,PLANNO,DISTRICT,LANDUSEADETAILED",
                "returnGeometry": "true", "outSR": "4326", "f": "json"
            }, headers=HEADERS, timeout=180)
            
            data = r.json()
            features = data.get("features", [])
            exceeded = data.get("exceededTransferLimit", False)
            
            count = 0
            for feat in features:
                a = feat.get("attributes", {})
                g = feat.get("geometry", {})
                lat, lng = calc_centroid(g.get("rings", [])) if "rings" in g else (None, None)
                writer.writerow({
                    "objectid": a.get("OBJECTID"), "parcel_id": a.get("PARCELID"),
                    "parcel_no": a.get("PARCELNO"), "block_no": a.get("BLOCKNO"),
                    "plan_no": a.get("PLANNO"), "district": a.get("DISTRICT"),
                    "land_use_code": a.get("LANDUSEADETAILED"), "status": "NEEDS_VERIFICATION",
                    "latitude": round(lat, 6) if lat else None, "longitude": round(lng, 6) if lng else None
                })
                count += 1
            
            if exceeded:
                params2 = {"where": where, "outStatistics": json.dumps([
                    {"statisticType": "min", "onStatisticField": "OBJECTID", "outStatisticFieldName": "MIN_OID"},
                    {"statisticType": "max", "onStatisticField": "OBJECTID", "outStatisticFieldName": "MAX_OID"}]), "f": "json"}
                d = requests.get(BASE_URL, params=params2, headers=HEADERS, timeout=60).json()
                a = d["features"][0]["attributes"]
                min_oid, max_oid = a.get("MIN_OID") or a.get("min_oid"), a.get("MAX_OID") or a.get("max_oid")
                
                cur = min_oid + 2000
                while cur <= max_oid:
                    r2 = requests.get(BASE_URL, params={
                        "where": f"{where} AND OBJECTID >= {cur} AND OBJECTID < {cur + 2000}",
                        "outFields": "OBJECTID,PARCELID,PARCELNO,BLOCKNO,PLANNO,DISTRICT,LANDUSEADETAILED",
                        "returnGeometry": "true", "outSR": "4326", "f": "json"
                    }, headers=HEADERS, timeout=180)
                    for feat in r2.json().get("features", []):
                        a = feat.get("attributes", {})
                        g = feat.get("geometry", {})
                        lat, lng = calc_centroid(g.get("rings", [])) if "rings" in g else (None, None)
                        writer.writerow({
                            "objectid": a.get("OBJECTID"), "parcel_id": a.get("PARCELID"),
                            "parcel_no": a.get("PARCELNO"), "block_no": a.get("BLOCKNO"),
                            "plan_no": a.get("PLANNO"), "district": a.get("DISTRICT"),
                            "land_use_code": a.get("LANDUSEADETAILED"), "status": "NEEDS_VERIFICATION",
                            "latitude": round(lat, 6) if lat else None, "longitude": round(lng, 6) if lng else None
                        })
                        count += 1
                    cur += 2000
                    time.sleep(0.1)
            
            total += count
            pct = 100 * total / total_expected
            print(f"[{i+1}/{len(districts)}] District {code}: +{count:,} | Total: {total:,} ({pct:.1f}%)")
            
        except Exception as e:
            print(f"[{i+1}/{len(districts)}] District {code}: ERROR - {e}")
        
        time.sleep(0.15)

print(f"\n✅ DONE: {total:,} parcels saved to {filepath}")
