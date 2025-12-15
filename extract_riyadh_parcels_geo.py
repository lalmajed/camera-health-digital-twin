#!/usr/bin/env python3
"""
================================================================================
RIYADH PARCEL GEOGRAPHIC DATA EXTRACTOR
================================================================================

Extracts geographic coordinates (latitude/longitude) for all residential 
parcels (villas and apartments) in Riyadh from the GeoPortal.

Output: CSV file with parcel details and centroid coordinates

Due to ArcGIS server limits (2000 records per query), we query by district
to extract all data.

Author: Riyadh Digital Twin Project
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
    1000: "VILLA",      # سكني - فلل
    1012: "APARTMENT",  # سكني تجاري - عمائر
    1100: "COMPLEX"     # مجمعات سكنية
}

def get_districts_with_residential():
    """Get list of districts that have residential parcels"""
    print("Fetching districts with residential parcels...")
    
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
    
    districts = []
    for f in data.get("features", []):
        attrs = f.get("attributes", {})
        district = attrs.get("DISTRICT")
        count = int(attrs.get("COUNT", 0) or 0)
        if district and count > 0:
            districts.append({"code": district, "count": count})
    
    districts = sorted(districts, key=lambda x: x["count"], reverse=True)
    total = sum(d["count"] for d in districts)
    
    print(f"Found {len(districts)} districts with {total:,} residential parcels")
    return districts


def calculate_centroid(rings):
    """Calculate centroid of a polygon from its rings"""
    if not rings or not rings[0]:
        return None, None
    
    points = rings[0]
    lng = sum(p[0] for p in points) / len(points)
    lat = sum(p[1] for p in points) / len(points)
    return lat, lng


def extract_parcels_for_district(district_code, land_use_filter="LANDUSEADETAILED IN (1000, 1012, 1100)"):
    """Extract all parcels for a specific district"""
    
    parcels = []
    
    # Query parcels for this district
    params = {
        "where": f"{land_use_filter} AND DISTRICT='{district_code}'",
        "outFields": "OBJECTID,PARCELID,PARCELNO,BLOCKNO,PLANNO,DISTRICT,LANDUSEADETAILED",
        "returnGeometry": "true",
        "outSR": "4326",  # WGS84 coordinate system
        "f": "json"
    }
    
    try:
        response = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=180)
        data = response.json()
        
        if "error" in data:
            print(f"    Error: {data['error'].get('message', 'Unknown')}")
            return parcels
        
        features = data.get("features", [])
        exceeded = data.get("exceededTransferLimit", False)
        
        for f in features:
            attrs = f.get("attributes", {})
            geom = f.get("geometry", {})
            
            lat, lng = None, None
            if "rings" in geom:
                lat, lng = calculate_centroid(geom["rings"])
            
            land_use_code = attrs.get("LANDUSEADETAILED")
            building_type = LAND_USE_TYPES.get(land_use_code, "OTHER")
            
            parcels.append({
                "objectid": attrs.get("OBJECTID"),
                "parcel_id": attrs.get("PARCELID"),
                "parcel_no": attrs.get("PARCELNO"),
                "block_no": attrs.get("BLOCKNO"),
                "plan_no": attrs.get("PLANNO"),
                "district": attrs.get("DISTRICT"),
                "land_use_code": land_use_code,
                "building_type": building_type,
                "latitude": round(lat, 6) if lat else None,
                "longitude": round(lng, 6) if lng else None
            })
        
        # If exceeded, we need to query separately for villas and apartments
        if exceeded:
            print(f"    ⚠ District {district_code} exceeded limit, splitting query...")
            parcels = []
            
            # Query villas
            params["where"] = f"LANDUSEADETAILED=1000 AND DISTRICT='{district_code}'"
            response = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=180)
            data = response.json()
            for f in data.get("features", []):
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
                    "land_use_code": 1000,
                    "building_type": "VILLA",
                    "latitude": round(lat, 6) if lat else None,
                    "longitude": round(lng, 6) if lng else None
                })
            
            time.sleep(0.2)
            
            # Query apartments
            params["where"] = f"LANDUSEADETAILED=1012 AND DISTRICT='{district_code}'"
            response = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=180)
            data = response.json()
            for f in data.get("features", []):
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
                    "land_use_code": 1012,
                    "building_type": "APARTMENT",
                    "latitude": round(lat, 6) if lat else None,
                    "longitude": round(lng, 6) if lng else None
                })
        
        return parcels
        
    except Exception as e:
        print(f"    Exception: {e}")
        return parcels


def main():
    print("="*70)
    print("RIYADH RESIDENTIAL PARCELS GEOGRAPHIC DATA EXTRACTOR")
    print("="*70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    # Get districts
    districts = get_districts_with_residential()
    
    # Output file
    output_file = "/workspace/riyadh_residential_parcels_geo.csv"
    
    # CSV columns
    fieldnames = [
        "objectid", "parcel_id", "parcel_no", "block_no", "plan_no",
        "district", "land_use_code", "building_type", "latitude", "longitude"
    ]
    
    total_extracted = 0
    total_expected = sum(d["count"] for d in districts)
    
    print(f"\nExtracting {total_expected:,} parcels from {len(districts)} districts...")
    print(f"Output: {output_file}")
    print("-"*70)
    
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for i, district in enumerate(districts):
            code = district["code"]
            expected = district["count"]
            
            print(f"[{i+1}/{len(districts)}] District {code}: expecting {expected:,} parcels...", end=" ", flush=True)
            
            parcels = extract_parcels_for_district(code)
            
            # Write to CSV
            for p in parcels:
                writer.writerow(p)
            
            total_extracted += len(parcels)
            print(f"got {len(parcels):,}")
            
            # Progress update
            if (i + 1) % 10 == 0:
                pct = 100 * total_extracted / total_expected
                print(f"    --- Progress: {total_extracted:,} / {total_expected:,} ({pct:.1f}%) ---")
            
            time.sleep(0.1)  # Be nice to the server
    
    print("\n" + "="*70)
    print("EXTRACTION COMPLETE")
    print("="*70)
    print(f"Total parcels extracted: {total_extracted:,}")
    print(f"Output file: {output_file}")
    print(f"File size: {os.path.getsize(output_file) / (1024*1024):.1f} MB")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    return total_extracted


if __name__ == "__main__":
    main()
