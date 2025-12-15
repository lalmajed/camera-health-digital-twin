#!/usr/bin/env python3
"""
RIYADH PARCEL COMPLETE EXTRACTOR - with OBJECTID pagination
Handles large districts by paginating through OBJECTID ranges
"""

import requests
import json
import csv
import time
import os
from datetime import datetime

BASE_URL = "https://mapservice.alriyadh.gov.sa/wa_maps/rest/services/BaseMap/Riyadh_BaseMap_V3/MapServer/71/query"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://mapservice.alriyadh.gov.sa/geoportal/geomap"
}

LAND_USE_TYPES = {
    1000: "VILLA",
    1012: "APARTMENT",
    1100: "COMPLEX"
}


def calculate_centroid(rings):
    """Calculate centroid of a polygon from its rings"""
    if not rings or not rings[0]:
        return None, None
    points = rings[0]
    lng = sum(p[0] for p in points) / len(points)
    lat = sum(p[1] for p in points) / len(points)
    return lat, lng


def get_objectid_range(where_clause):
    """Get min and max OBJECTID for a query"""
    params = {
        "where": where_clause,
        "outStatistics": json.dumps([
            {"statisticType": "min", "onStatisticField": "OBJECTID", "outStatisticFieldName": "MIN_OID"},
            {"statisticType": "max", "onStatisticField": "OBJECTID", "outStatisticFieldName": "MAX_OID"},
            {"statisticType": "count", "onStatisticField": "OBJECTID", "outStatisticFieldName": "TOTAL"}
        ]),
        "f": "json"
    }
    
    try:
        response = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=60)
        data = response.json()
        if data.get("features"):
            attrs = data["features"][0]["attributes"]
            return (
                attrs.get("MIN_OID") or attrs.get("min_oid"),
                attrs.get("MAX_OID") or attrs.get("max_oid"),
                attrs.get("TOTAL") or attrs.get("total") or 0
            )
    except Exception as e:
        print(f"Error getting OID range: {e}")
    return None, None, 0


def fetch_parcels_paginated(district_code, land_use_code):
    """Fetch all parcels for a district+land_use using OBJECTID pagination"""
    
    where_base = f"LANDUSEADETAILED={land_use_code} AND DISTRICT='{district_code}'"
    min_oid, max_oid, total = get_objectid_range(where_base)
    
    if not min_oid or total == 0:
        return []
    
    parcels = []
    batch_size = 2000
    current_min = min_oid
    
    while current_min <= max_oid:
        where = f"{where_base} AND OBJECTID >= {current_min} AND OBJECTID < {current_min + batch_size}"
        
        params = {
            "where": where,
            "outFields": "OBJECTID,PARCELID,PARCELNO,BLOCKNO,PLANNO,DISTRICT,LANDUSEADETAILED",
            "returnGeometry": "true",
            "outSR": "4326",
            "f": "json"
        }
        
        try:
            response = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=180)
            data = response.json()
            
            features = data.get("features", [])
            for f in features:
                attrs = f.get("attributes", {})
                geom = f.get("geometry", {})
                lat, lng = calculate_centroid(geom.get("rings", [])) if "rings" in geom else (None, None)
                
                parcels.append({
                    "objectid": attrs.get("OBJECTID"),
                    "parcel_id": attrs.get("PARCELID"),
                    "parcel_no": attrs.get("PARCELNO"),
                    "block_no": attrs.get("BLOCKNO"),
                    "plan_no": attrs.get("PLANNO"),
                    "district": attrs.get("DISTRICT"),
                    "land_use_code": land_use_code,
                    "building_type": LAND_USE_TYPES.get(land_use_code, "OTHER"),
                    "latitude": round(lat, 6) if lat else None,
                    "longitude": round(lng, 6) if lng else None
                })
            
        except Exception as e:
            print(f"      Error at OID {current_min}: {e}")
        
        current_min += batch_size
        time.sleep(0.1)
    
    return parcels


def get_district_expected_counts():
    """Get expected parcel counts per district"""
    print("Fetching expected counts per district...")
    
    params = {
        "where": "LANDUSEADETAILED IN (1000, 1012, 1100)",
        "groupByFieldsForStatistics": "DISTRICT",
        "outStatistics": json.dumps([
            {"statisticType": "count", "onStatisticField": "OBJECTID", "outStatisticFieldName": "COUNT"}
        ]),
        "f": "json"
    }
    
    response = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=120)
    data = response.json()
    
    districts = {}
    for f in data.get("features", []):
        attrs = f.get("attributes", {})
        code = attrs.get("DISTRICT")
        count = int(attrs.get("COUNT", 0) or attrs.get("count", 0) or 0)
        if code:
            districts[code] = count
    
    return districts


def load_existing_data(filepath):
    """Load existing extracted data to check what's already done"""
    existing = {}
    if os.path.exists(filepath):
        with open(filepath, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                district = row['district']
                if district not in existing:
                    existing[district] = set()
                existing[district].add(row['objectid'])
    return existing


def main():
    print("="*70)
    print("RIYADH COMPLETE PARCEL EXTRACTOR (OBJECTID Pagination)")
    print("="*70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    # Get expected counts
    expected = get_district_expected_counts()
    total_expected = sum(expected.values())
    print(f"\nTotal expected: {total_expected:,} parcels across {len(expected)} districts")
    
    # Load existing data
    output_file = "/workspace/riyadh_residential_parcels_geo.csv"
    existing = load_existing_data(output_file)
    existing_total = sum(len(v) for v in existing.values())
    print(f"Already extracted: {existing_total:,} parcels")
    
    # Find truncated districts (where extracted < expected)
    truncated_districts = []
    for code, exp_count in sorted(expected.items(), key=lambda x: -x[1]):
        got_count = len(existing.get(code, set()))
        if got_count < exp_count:
            truncated_districts.append({
                "code": code,
                "expected": exp_count,
                "got": got_count,
                "missing": exp_count - got_count
            })
    
    if not truncated_districts:
        print("\n✓ All parcels already extracted!")
        return
    
    total_missing = sum(d["missing"] for d in truncated_districts)
    print(f"\nDistricts needing more extraction: {len(truncated_districts)}")
    print(f"Missing parcels: {total_missing:,}")
    print("-"*70)
    
    # Open file in append mode
    fieldnames = [
        "objectid", "parcel_id", "parcel_no", "block_no", "plan_no",
        "district", "land_use_code", "building_type", "latitude", "longitude"
    ]
    
    new_parcels = 0
    
    with open(output_file, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        for i, district in enumerate(truncated_districts):
            code = district["code"]
            exp = district["expected"]
            got = district["got"]
            
            print(f"[{i+1}/{len(truncated_districts)}] District {code}: expected {exp:,}, got {got:,}, missing ~{exp-got:,}")
            
            existing_oids = existing.get(code, set())
            
            # Fetch villas with pagination
            villas = fetch_parcels_paginated(code, 1000)
            new_villas = [p for p in villas if str(p["objectid"]) not in existing_oids]
            
            # Fetch apartments with pagination
            apartments = fetch_parcels_paginated(code, 1012)
            new_apartments = [p for p in apartments if str(p["objectid"]) not in existing_oids]
            
            # Fetch complexes
            complexes = fetch_parcels_paginated(code, 1100)
            new_complexes = [p for p in complexes if str(p["objectid"]) not in existing_oids]
            
            all_new = new_villas + new_apartments + new_complexes
            
            for p in all_new:
                writer.writerow(p)
            
            new_parcels += len(all_new)
            print(f"    Added {len(all_new):,} new parcels (V:{len(new_villas)}, A:{len(new_apartments)}, C:{len(new_complexes)})")
            
            # Progress
            if (i + 1) % 5 == 0:
                print(f"    --- Progress: +{new_parcels:,} new parcels ---")
            
            time.sleep(0.2)
    
    print("\n" + "="*70)
    print("EXTRACTION COMPLETE")
    print("="*70)
    print(f"New parcels added: {new_parcels:,}")
    print(f"Total in file: {existing_total + new_parcels:,}")
    print(f"Output file: {output_file}")
    print(f"File size: {os.path.getsize(output_file) / (1024*1024):.1f} MB")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
