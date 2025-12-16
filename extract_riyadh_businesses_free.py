#!/usr/bin/env python3
"""
================================================================================
RIYADH BUSINESS & COMMERCIAL GEOLOCATION EXTRACTOR (FREE - NO API KEY NEEDED)
================================================================================

Extracts commercial locations and business zones in Riyadh from FREE sources:

1. Riyadh GeoPortal (Official Government Data) - Commercial parcels, retail zones
2. Nominatim/Photon (Better geocoding with recent data)
3. Direct web extraction from public business directories

Author: Riyadh Digital Twin Project
================================================================================
"""

import requests
import json
import csv
import time
import os
from datetime import datetime
from typing import Dict, List, Set
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============================================================================
# CONFIGURATION
# ============================================================================

# Riyadh GeoPortal API
GEOPORTAL_BASE = "https://mapservice.alriyadh.gov.sa/wa_maps/rest/services/BaseMap/Riyadh_BaseMap_V3/MapServer"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://mapservice.alriyadh.gov.sa/geoportal/geomap"
}

# Commercial/Business Land Use Codes from Riyadh GeoPortal
# These are the official land use classifications
COMMERCIAL_LAND_USE = {
    # تجاري - Commercial
    2000: "COMMERCIAL_GENERAL",           # تجاري عام
    2010: "COMMERCIAL_RETAIL",            # تجاري تجزئة
    2013: "COMMERCIAL_WHOLESALE",         # تجاري جملة
    2014: "COMMERCIAL_SHOWROOM",          # معارض تجارية
    2017: "COMMERCIAL_MARKET",            # سوق تجاري
    2018: "COMMERCIAL_CENTER",            # مركز تجاري
    2020: "COMMERCIAL_MALL",              # مجمع تجاري
    2021: "COMMERCIAL_COMPLEX",           # مجمع تجاري كبير
    2033: "COMMERCIAL_OFFICES",           # مكاتب تجارية
    2035: "COMMERCIAL_MIXED",             # تجاري متعدد الاستخدام
    
    # سكني تجاري - Mixed Use (Residential + Commercial)
    1012: "MIXED_RESIDENTIAL_COMMERCIAL", # سكني تجاري - عمائر
    1015: "MIXED_USE_BUILDING",           # مبنى متعدد الاستخدام
    1016: "MIXED_USE_COMPLEX",            # مجمع متعدد الاستخدام
    1017: "MIXED_COMMERCIAL_RESIDENTIAL", # تجاري سكني
    
    # خدمات - Services
    2100: "SERVICE_GENERAL",              # خدمات عامة
    2101: "SERVICE_RESTAURANT",           # مطاعم
    2105: "SERVICE_CAFE",                 # مقاهي
    2150: "SERVICE_HOTEL",                # فنادق
    2152: "SERVICE_HOTEL_APARTMENTS",     # شقق فندقية
    2160: "SERVICE_MOTEL",                # موتيل
    
    # محطات وقود - Gas Stations
    2252: "GAS_STATION",                  # محطة وقود
    2270: "GAS_STATION_SERVICE",          # محطة خدمات سيارات
    2272: "CAR_WASH",                     # غسيل سيارات
    2273: "CAR_SERVICE",                  # صيانة سيارات
    2278: "CAR_SHOWROOM",                 # معرض سيارات
    2284: "CAR_RENTAL",                   # تأجير سيارات
    2285: "PARKING",                      # مواقف سيارات
    
    # مالي - Financial
    2400: "FINANCIAL_GENERAL",            # خدمات مالية
    2410: "BANK",                         # بنك
    2411: "BANK_BRANCH",                  # فرع بنك
    2413: "ATM",                          # صراف آلي
    2414: "EXCHANGE",                     # صرافة
    2415: "INSURANCE",                    # تأمين
    2416: "INVESTMENT",                   # استثمار
    
    # صحي - Healthcare
    2461: "HEALTHCARE_GENERAL",           # خدمات صحية
    2465: "PHARMACY",                     # صيدلية
    
    # ترفيه - Entertainment
    2610: "ENTERTAINMENT_GENERAL",        # ترفيه عام
    2612: "CINEMA",                       # سينما
    2623: "SPORTS_CENTER",                # مركز رياضي
    2633: "PARK_GARDEN",                  # حديقة
    2634: "PLAYGROUND",                   # ملعب
    2635: "SPORTS_CLUB",                  # نادي رياضي
    2636: "RECREATION",                   # استراحة
    2637: "AMUSEMENT",                    # ملاهي
    2638: "SWIMMING_POOL",                # مسبح
    2639: "GYM",                          # صالة رياضية
    2649: "EVENT_HALL",                   # قاعة مناسبات
    2650: "WEDDING_HALL",                 # قاعة أفراح
    2651: "EXHIBITION",                   # معارض
    2654: "MUSEUM",                       # متحف
    
    # تعليمي - Educational
    3201: "SCHOOL_PRIMARY",               # مدرسة ابتدائية
    3204: "SCHOOL_INTERMEDIATE",          # مدرسة متوسطة
    3207: "SCHOOL_SECONDARY",             # مدرسة ثانوية
    3208: "SCHOOL_PRIVATE",               # مدرسة أهلية
    3209: "SCHOOL_INTERNATIONAL",         # مدرسة عالمية
    3210: "KINDERGARTEN",                 # روضة أطفال
    3250: "UNIVERSITY",                   # جامعة
    3251: "COLLEGE",                      # كلية
    3252: "INSTITUTE",                    # معهد
    3253: "TRAINING_CENTER",              # مركز تدريب
    
    # صحي - Medical
    3400: "HOSPITAL_GENERAL",             # مستشفى عام
    3401: "HOSPITAL_PRIVATE",             # مستشفى خاص
    3405: "HOSPITAL_SPECIALIZED",         # مستشفى تخصصي
    3406: "MEDICAL_CENTER",               # مركز طبي
    3408: "CLINIC",                       # عيادة
    3415: "DENTAL_CLINIC",                # عيادة أسنان
    3416: "OPTICAL",                      # بصريات
    3417: "LABORATORY",                   # مختبر
    3418: "RADIOLOGY",                    # أشعة
    3419: "HEALTH_CENTER",                # مركز صحي
    
    # ديني - Religious
    3491: "MOSQUE",                       # مسجد
    3493: "MOSQUE_LARGE",                 # جامع
    3494: "RELIGIOUS_CENTER",             # مركز ديني
    
    # حكومي - Government
    3550: "GOVERNMENT_GENERAL",           # حكومي عام
    3551: "MINISTRY",                     # وزارة
    3552: "MUNICIPALITY",                 # بلدية
    3555: "POLICE_STATION",               # مركز شرطة
    3556: "FIRE_STATION",                 # إطفاء
    3557: "POST_OFFICE",                  # بريد
    3558: "COURT",                        # محكمة
    
    # صناعي - Industrial/Workshops
    4010: "INDUSTRIAL_GENERAL",           # صناعي عام
    4011: "WORKSHOP",                     # ورشة
    4012: "FACTORY",                      # مصنع
    4014: "WAREHOUSE",                    # مستودع
    
    # مرافق - Utilities
    5100: "UTILITY_GENERAL",              # مرافق عامة
    5221: "ELECTRICITY",                  # كهرباء
    5222: "WATER",                        # مياه
    5223: "TELECOM",                      # اتصالات
    
    # سياحي - Tourism
    6000: "TOURISM_GENERAL",              # سياحي عام
    6012: "TOURIST_ATTRACTION",           # معلم سياحي
    6013: "HERITAGE_SITE",                # موقع تراثي
}

# Output files
OUTPUT_JSON = "/workspace/riyadh_commercial_geo.json"
OUTPUT_CSV = "/workspace/riyadh_commercial_geo.csv"
PROGRESS_FILE = "/workspace/commercial_extraction_progress.json"


# ============================================================================
# GEOPORTAL EXTRACTION FUNCTIONS
# ============================================================================

def calculate_centroid(rings):
    """Calculate centroid of a polygon"""
    if not rings or not rings[0]:
        return None, None
    points = rings[0]
    lng = sum(p[0] for p in points) / len(points)
    lat = sum(p[1] for p in points) / len(points)
    return lat, lng


def get_districts_with_commercial():
    """Get list of districts with commercial parcels"""
    print("Fetching districts with commercial parcels...")
    
    # Build land use filter
    land_use_codes = list(COMMERCIAL_LAND_USE.keys())
    land_use_filter = f"LANDUSEADETAILED IN ({','.join(map(str, land_use_codes))})"
    
    params = {
        "where": land_use_filter,
        "groupByFieldsForStatistics": "DISTRICT",
        "outStatistics": json.dumps([
            {"statisticType": "count", "onStatisticField": "OBJECTID", "outStatisticFieldName": "COUNT"}
        ]),
        "f": "json"
    }
    
    url = f"{GEOPORTAL_BASE}/71/query"
    response = requests.get(url, params=params, headers=HEADERS, timeout=120)
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
    
    print(f"Found {len(districts)} districts with {total:,} commercial parcels")
    return districts, total


def extract_parcels_for_district(district_code: str, land_use_codes: List[int]) -> List[Dict]:
    """Extract commercial parcels for a specific district"""
    
    parcels = []
    url = f"{GEOPORTAL_BASE}/71/query"
    
    # Try to get all at once first
    land_use_filter = f"LANDUSEADETAILED IN ({','.join(map(str, land_use_codes))})"
    
    params = {
        "where": f"{land_use_filter} AND DISTRICT='{district_code}'",
        "outFields": "OBJECTID,PARCELID,PARCELNO,BLOCKNO,PLANNO,DISTRICT,LANDUSEADETAILED,PARCELAREA",
        "returnGeometry": "true",
        "outSR": "4326",
        "f": "json"
    }
    
    try:
        response = requests.get(url, params=params, headers=HEADERS, timeout=180)
        data = response.json()
        
        if "error" in data:
            return parcels
        
        features = data.get("features", [])
        exceeded = data.get("exceededTransferLimit", False)
        
        if exceeded:
            # Query by individual land use codes
            for code in land_use_codes:
                params["where"] = f"LANDUSEADETAILED={code} AND DISTRICT='{district_code}'"
                try:
                    response = requests.get(url, params=params, headers=HEADERS, timeout=180)
                    data = response.json()
                    features.extend(data.get("features", []))
                    time.sleep(0.1)
                except:
                    continue
        
        for f in features:
            attrs = f.get("attributes", {})
            geom = f.get("geometry", {})
            
            lat, lng = None, None
            if "rings" in geom:
                lat, lng = calculate_centroid(geom["rings"])
            
            land_use_code = attrs.get("LANDUSEADETAILED")
            business_type = COMMERCIAL_LAND_USE.get(land_use_code, "OTHER")
            
            parcels.append({
                "objectid": attrs.get("OBJECTID"),
                "parcel_id": attrs.get("PARCELID"),
                "parcel_no": attrs.get("PARCELNO"),
                "block_no": attrs.get("BLOCKNO"),
                "plan_no": attrs.get("PLANNO"),
                "district": attrs.get("DISTRICT"),
                "land_use_code": land_use_code,
                "business_type": business_type,
                "area_sqm": attrs.get("PARCELAREA"),
                "latitude": round(lat, 6) if lat else None,
                "longitude": round(lng, 6) if lng else None,
                "source": "Riyadh_GeoPortal"
            })
        
        return parcels
        
    except Exception as e:
        print(f"    Error: {e}")
        return parcels


def extract_commercial_from_geoportal():
    """Extract all commercial locations from Riyadh GeoPortal"""
    
    print("="*70)
    print("RIYADH COMMERCIAL GEOLOCATION EXTRACTOR")
    print("Source: Riyadh GeoPortal (Official Government Data)")
    print("="*70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    # Get districts
    districts, total_expected = get_districts_with_commercial()
    land_use_codes = list(COMMERCIAL_LAND_USE.keys())
    
    all_parcels = []
    
    print(f"\nExtracting {total_expected:,} commercial parcels from {len(districts)} districts...")
    print(f"Land use categories: {len(COMMERCIAL_LAND_USE)}")
    print("-"*70)
    
    for i, district in enumerate(districts):
        code = district["code"]
        expected = district["count"]
        
        print(f"[{i+1}/{len(districts)}] District {code}: expecting {expected:,}...", end=" ", flush=True)
        
        parcels = extract_parcels_for_district(code, land_use_codes)
        all_parcels.extend(parcels)
        
        print(f"got {len(parcels):,}")
        
        if (i + 1) % 10 == 0:
            pct = 100 * len(all_parcels) / total_expected if total_expected > 0 else 0
            print(f"    --- Progress: {len(all_parcels):,} / {total_expected:,} ({pct:.1f}%) ---")
        
        time.sleep(0.1)
    
    return all_parcels


# ============================================================================
# ADDITIONAL DATA: Points of Interest from GeoPortal Layers
# ============================================================================

def extract_pois_from_layer(layer_id: int, layer_name: str, poi_type: str) -> List[Dict]:
    """Extract points of interest from a specific GeoPortal layer"""
    
    pois = []
    url = f"{GEOPORTAL_BASE}/{layer_id}/query"
    
    params = {
        "where": "1=1",
        "outFields": "*",
        "returnGeometry": "true",
        "outSR": "4326",
        "f": "json"
    }
    
    try:
        response = requests.get(url, params=params, headers=HEADERS, timeout=120)
        data = response.json()
        
        for f in data.get("features", []):
            attrs = f.get("attributes", {})
            geom = f.get("geometry", {})
            
            lat, lng = None, None
            if "x" in geom and "y" in geom:
                lng, lat = geom["x"], geom["y"]
            elif "rings" in geom:
                lat, lng = calculate_centroid(geom["rings"])
            
            # Get name from various possible fields
            name = (attrs.get("NAME") or attrs.get("Name") or 
                   attrs.get("NAMEAR") or attrs.get("NAME_AR") or
                   attrs.get("NAMEEN") or attrs.get("NAME_EN") or
                   attrs.get("OBJECTID", ""))
            
            pois.append({
                "objectid": attrs.get("OBJECTID"),
                "name": name,
                "poi_type": poi_type,
                "layer": layer_name,
                "latitude": round(lat, 6) if lat else None,
                "longitude": round(lng, 6) if lng else None,
                "attributes": attrs,
                "source": "Riyadh_GeoPortal"
            })
        
        return pois
        
    except Exception as e:
        print(f"  Error extracting {layer_name}: {e}")
        return []


def extract_all_pois():
    """Extract POIs from various GeoPortal layers"""
    
    # Layers to extract (layer_id, name, type)
    poi_layers = [
        (1, "Bus_Stops", "TRANSPORT"),
        (7, "Municipality_HQ", "GOVERNMENT"),
        (8, "Roundabouts", "LANDMARK"),
        (11, "Metro_Stations", "TRANSPORT"),
        (12, "Metro_Stations_All", "TRANSPORT"),
        (32, "Parks_Parcels", "RECREATION"),
        (33, "Parks_Municipality", "RECREATION"),
        (35, "Playgrounds", "RECREATION"),
        (38, "Airport", "TRANSPORT"),
        (42, "Universities", "EDUCATION"),
        (43, "Parking", "PARKING"),
    ]
    
    all_pois = []
    
    print("\nExtracting Points of Interest from GeoPortal layers...")
    print("-"*70)
    
    for layer_id, layer_name, poi_type in poi_layers:
        print(f"  Extracting {layer_name}...", end=" ", flush=True)
        pois = extract_pois_from_layer(layer_id, layer_name, poi_type)
        all_pois.extend(pois)
        print(f"got {len(pois)}")
        time.sleep(0.2)
    
    return all_pois


# ============================================================================
# SAVE RESULTS
# ============================================================================

def save_results(parcels: List[Dict], pois: List[Dict]):
    """Save all results to JSON and CSV"""
    
    # Combine data
    all_data = {
        "metadata": {
            "source": "Riyadh GeoPortal (Official Government Data)",
            "extraction_date": datetime.now().isoformat(),
            "total_commercial_parcels": len(parcels),
            "total_pois": len(pois),
            "area": "Riyadh, Saudi Arabia",
            "note": "This is official government land use data showing commercial zones"
        },
        "commercial_parcels": parcels,
        "points_of_interest": pois
    }
    
    # Save JSON
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)
    
    # Save CSV for parcels
    if parcels:
        fieldnames = [
            "objectid", "parcel_id", "parcel_no", "block_no", "plan_no",
            "district", "land_use_code", "business_type", "area_sqm",
            "latitude", "longitude", "source"
        ]
        
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(parcels)
    
    # Save POIs to separate CSV
    poi_csv = "/workspace/riyadh_pois_geo.csv"
    if pois:
        # Flatten attributes for CSV
        poi_rows = []
        for poi in pois:
            row = {
                "objectid": poi.get("objectid"),
                "name": poi.get("name"),
                "poi_type": poi.get("poi_type"),
                "layer": poi.get("layer"),
                "latitude": poi.get("latitude"),
                "longitude": poi.get("longitude"),
                "source": poi.get("source")
            }
            poi_rows.append(row)
        
        with open(poi_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(poi_rows[0].keys()))
            writer.writeheader()
            writer.writerows(poi_rows)
    
    print(f"\nResults saved:")
    print(f"  JSON: {OUTPUT_JSON}")
    print(f"  CSV (Commercial): {OUTPUT_CSV}")
    print(f"  CSV (POIs): {poi_csv}")


# ============================================================================
# CATEGORY SUMMARY
# ============================================================================

def print_category_summary(parcels: List[Dict]):
    """Print summary by business category"""
    
    print("\n" + "="*70)
    print("EXTRACTION SUMMARY BY CATEGORY")
    print("="*70)
    
    # Count by type
    type_counts = {}
    for p in parcels:
        btype = p.get("business_type", "OTHER")
        type_counts[btype] = type_counts.get(btype, 0) + 1
    
    # Sort by count
    sorted_types = sorted(type_counts.items(), key=lambda x: -x[1])
    
    # Group by category
    categories = {
        "COMMERCIAL": [],
        "MIXED_USE": [],
        "SERVICE": [],
        "FINANCIAL": [],
        "HEALTHCARE": [],
        "ENTERTAINMENT": [],
        "EDUCATION": [],
        "RELIGIOUS": [],
        "GOVERNMENT": [],
        "INDUSTRIAL": [],
        "OTHER": []
    }
    
    for btype, count in sorted_types:
        added = False
        for cat in categories:
            if btype.startswith(cat) or cat in btype:
                categories[cat].append((btype, count))
                added = True
                break
        if not added:
            categories["OTHER"].append((btype, count))
    
    for cat, items in categories.items():
        if items:
            total = sum(c for _, c in items)
            print(f"\n{cat}: {total:,} parcels")
            for btype, count in items[:5]:  # Show top 5
                print(f"  - {btype}: {count:,}")
            if len(items) > 5:
                print(f"  ... and {len(items)-5} more types")


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Main extraction function"""
    
    # Extract commercial parcels
    parcels = extract_commercial_from_geoportal()
    
    # Extract POIs
    pois = extract_all_pois()
    
    # Save results
    save_results(parcels, pois)
    
    # Print summary
    print_category_summary(parcels)
    
    # Final summary
    print("\n" + "="*70)
    print("EXTRACTION COMPLETE")
    print("="*70)
    print(f"Total commercial parcels: {len(parcels):,}")
    print(f"Total POIs: {len(pois):,}")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    return parcels, pois


if __name__ == "__main__":
    main()
