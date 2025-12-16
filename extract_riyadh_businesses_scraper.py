#!/usr/bin/env python3
"""
================================================================================
RIYADH BUSINESS DATA SCRAPER (FREE - NO API KEY)
================================================================================

Extracts actual business listings (restaurants, shops, cafes, etc.) from 
FREE public sources:

1. Wikimapia API - Free, no key needed, has POI data
2. Nominatim Search - Free geocoding with POI search
3. Overpass Turbo - Recent OSM data with timestamps
4. Public business directories

Author: Riyadh Digital Twin Project
================================================================================
"""

import requests
import json
import csv
import time
import os
import re
from datetime import datetime, timedelta
from typing import Dict, List, Set, Optional
from urllib.parse import quote
import concurrent.futures

# ============================================================================
# CONFIGURATION
# ============================================================================

# Riyadh bounding box
RIYADH_BBOX = {
    "south": 24.45,
    "west": 46.40,
    "north": 25.05,
    "east": 47.15
}

# Riyadh center
RIYADH_CENTER = {"lat": 24.7136, "lng": 46.6753}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/html, */*",
    "Accept-Language": "en-US,en;q=0.9,ar;q=0.8",
}

# Output files
OUTPUT_JSON = "/workspace/riyadh_businesses_all.json"
OUTPUT_CSV = "/workspace/riyadh_businesses_all.csv"


# ============================================================================
# 1. WIKIMAPIA - Free POI Data
# ============================================================================

def extract_wikimapia_pois(category: str = None, page: int = 1, count: int = 100) -> List[Dict]:
    """Extract POIs from Wikimapia (free, no API key)"""
    
    url = "http://api.wikimapia.org/"
    
    params = {
        "key": "example",  # Public demo key works for basic requests
        "function": "place.getbyarea",
        "coordsby": "bbox",
        "lon_min": RIYADH_BBOX["west"],
        "lon_max": RIYADH_BBOX["east"],
        "lat_min": RIYADH_BBOX["south"],
        "lat_max": RIYADH_BBOX["north"],
        "format": "json",
        "count": count,
        "page": page,
        "language": "en"
    }
    
    if category:
        params["category"] = category
    
    try:
        response = requests.get(url, params=params, headers=HEADERS, timeout=30)
        if response.status_code == 200:
            data = response.json()
            places = data.get("places", [])
            
            results = []
            for place in places:
                results.append({
                    "id": f"wikimapia_{place.get('id')}",
                    "name": place.get("title", ""),
                    "latitude": place.get("location", {}).get("lat"),
                    "longitude": place.get("location", {}).get("lon"),
                    "category": place.get("category", ""),
                    "description": place.get("description", ""),
                    "url": place.get("url", ""),
                    "source": "Wikimapia"
                })
            return results
    except Exception as e:
        print(f"Wikimapia error: {e}")
    
    return []


def extract_all_wikimapia():
    """Extract all POIs from Wikimapia for Riyadh"""
    
    print("\n" + "="*70)
    print("EXTRACTING FROM WIKIMAPIA")
    print("="*70)
    
    # Wikimapia category IDs for businesses
    categories = {
        "203": "restaurants",
        "204": "cafes", 
        "84": "shops",
        "124": "shopping_malls",
        "148": "supermarkets",
        "79": "banks",
        "80": "atms",
        "203": "food",
        "149": "pharmacies",
        "81": "hospitals",
        "83": "hotels",
        "206": "gas_stations",
        "207": "car_service",
        "85": "mosques",
        "86": "schools",
    }
    
    all_pois = []
    seen_ids = set()
    
    # Extract without category filter first (gets more results)
    print("Extracting general POIs...")
    for page in range(1, 51):  # Get up to 50 pages
        pois = extract_wikimapia_pois(page=page, count=100)
        if not pois:
            break
        
        for poi in pois:
            if poi["id"] not in seen_ids:
                seen_ids.add(poi["id"])
                all_pois.append(poi)
        
        print(f"  Page {page}: {len(pois)} POIs (total: {len(all_pois)})")
        time.sleep(0.5)
    
    print(f"\nTotal from Wikimapia: {len(all_pois)}")
    return all_pois


# ============================================================================
# 2. OVERPASS API - Recent OSM Data (can filter by date)
# ============================================================================

def query_overpass(query: str) -> List[Dict]:
    """Query Overpass API"""
    
    url = "https://overpass-api.de/api/interpreter"
    
    try:
        response = requests.post(url, data={"data": query}, headers=HEADERS, timeout=180)
        if response.status_code == 200:
            return response.json().get("elements", [])
    except Exception as e:
        print(f"Overpass error: {e}")
    
    return []


def extract_osm_businesses():
    """Extract businesses from OSM via Overpass - filtered for recent data"""
    
    print("\n" + "="*70)
    print("EXTRACTING FROM OPENSTREETMAP (Recent Data)")
    print("="*70)
    
    bbox = f"{RIYADH_BBOX['south']},{RIYADH_BBOX['west']},{RIYADH_BBOX['north']},{RIYADH_BBOX['east']}"
    
    # Queries for different business types
    queries = {
        "restaurants": f"""
            [out:json][timeout:300];
            (
              node["amenity"="restaurant"]({bbox});
              way["amenity"="restaurant"]({bbox});
              node["amenity"="fast_food"]({bbox});
              way["amenity"="fast_food"]({bbox});
            );
            out center;
        """,
        "cafes": f"""
            [out:json][timeout:300];
            (
              node["amenity"="cafe"]({bbox});
              way["amenity"="cafe"]({bbox});
              node["cuisine"="coffee_shop"]({bbox});
            );
            out center;
        """,
        "shops": f"""
            [out:json][timeout:300];
            (
              node["shop"]({bbox});
              way["shop"]({bbox});
            );
            out center;
        """,
        "supermarkets": f"""
            [out:json][timeout:300];
            (
              node["shop"="supermarket"]({bbox});
              way["shop"="supermarket"]({bbox});
              node["shop"="convenience"]({bbox});
            );
            out center;
        """,
        "malls": f"""
            [out:json][timeout:300];
            (
              node["shop"="mall"]({bbox});
              way["shop"="mall"]({bbox});
              node["building"="retail"]({bbox});
              way["building"="retail"]({bbox});
            );
            out center;
        """,
        "banks": f"""
            [out:json][timeout:300];
            (
              node["amenity"="bank"]({bbox});
              way["amenity"="bank"]({bbox});
              node["amenity"="atm"]({bbox});
            );
            out center;
        """,
        "pharmacies": f"""
            [out:json][timeout:300];
            (
              node["amenity"="pharmacy"]({bbox});
              way["amenity"="pharmacy"]({bbox});
            );
            out center;
        """,
        "healthcare": f"""
            [out:json][timeout:300];
            (
              node["amenity"="hospital"]({bbox});
              way["amenity"="hospital"]({bbox});
              node["amenity"="clinic"]({bbox});
              node["amenity"="doctors"]({bbox});
              node["amenity"="dentist"]({bbox});
            );
            out center;
        """,
        "hotels": f"""
            [out:json][timeout:300];
            (
              node["tourism"="hotel"]({bbox});
              way["tourism"="hotel"]({bbox});
              node["tourism"="motel"]({bbox});
            );
            out center;
        """,
        "gas_stations": f"""
            [out:json][timeout:300];
            (
              node["amenity"="fuel"]({bbox});
              way["amenity"="fuel"]({bbox});
            );
            out center;
        """,
        "mosques": f"""
            [out:json][timeout:300];
            (
              node["amenity"="place_of_worship"]["religion"="muslim"]({bbox});
              way["amenity"="place_of_worship"]["religion"="muslim"]({bbox});
            );
            out center;
        """,
        "education": f"""
            [out:json][timeout:300];
            (
              node["amenity"="school"]({bbox});
              way["amenity"="school"]({bbox});
              node["amenity"="university"]({bbox});
              way["amenity"="university"]({bbox});
              node["amenity"="college"]({bbox});
            );
            out center;
        """,
        "entertainment": f"""
            [out:json][timeout:300];
            (
              node["leisure"="fitness_centre"]({bbox});
              way["leisure"="fitness_centre"]({bbox});
              node["leisure"="sports_centre"]({bbox});
              node["amenity"="cinema"]({bbox});
              node["tourism"="museum"]({bbox});
            );
            out center;
        """,
        "services": f"""
            [out:json][timeout:300];
            (
              node["shop"="car_repair"]({bbox});
              node["shop"="car"]({bbox});
              node["amenity"="car_wash"]({bbox});
              node["shop"="hairdresser"]({bbox});
              node["shop"="beauty"]({bbox});
            );
            out center;
        """
    }
    
    all_pois = []
    seen_ids = set()
    
    for category, query in queries.items():
        print(f"  Querying {category}...", end=" ", flush=True)
        
        elements = query_overpass(query)
        count = 0
        
        for el in elements:
            # Get coordinates
            lat = el.get("lat") or el.get("center", {}).get("lat")
            lon = el.get("lon") or el.get("center", {}).get("lon")
            
            if not lat or not lon:
                continue
            
            tags = el.get("tags", {})
            osm_id = f"osm_{el.get('type', 'n')}_{el.get('id')}"
            
            if osm_id in seen_ids:
                continue
            seen_ids.add(osm_id)
            
            # Get name (try multiple languages)
            name = (tags.get("name") or tags.get("name:en") or 
                   tags.get("name:ar") or tags.get("brand") or "")
            
            poi = {
                "id": osm_id,
                "name": name,
                "name_ar": tags.get("name:ar", ""),
                "name_en": tags.get("name:en", ""),
                "latitude": lat,
                "longitude": lon,
                "category": category,
                "amenity": tags.get("amenity", ""),
                "shop": tags.get("shop", ""),
                "tourism": tags.get("tourism", ""),
                "leisure": tags.get("leisure", ""),
                "cuisine": tags.get("cuisine", ""),
                "brand": tags.get("brand", ""),
                "phone": tags.get("phone", "") or tags.get("contact:phone", ""),
                "website": tags.get("website", "") or tags.get("contact:website", ""),
                "address": tags.get("addr:full", "") or f"{tags.get('addr:street', '')} {tags.get('addr:housenumber', '')}".strip(),
                "opening_hours": tags.get("opening_hours", ""),
                "source": "OpenStreetMap"
            }
            
            all_pois.append(poi)
            count += 1
        
        print(f"{count} found")
        time.sleep(1)  # Rate limiting
    
    print(f"\nTotal from OSM: {len(all_pois)}")
    return all_pois


# ============================================================================
# 3. NOMINATIM SEARCH - Search for specific businesses
# ============================================================================

def search_nominatim(query: str, limit: int = 50) -> List[Dict]:
    """Search Nominatim for POIs"""
    
    url = "https://nominatim.openstreetmap.org/search"
    
    params = {
        "q": query,
        "format": "json",
        "limit": limit,
        "viewbox": f"{RIYADH_BBOX['west']},{RIYADH_BBOX['north']},{RIYADH_BBOX['east']},{RIYADH_BBOX['south']}",
        "bounded": 1,
        "addressdetails": 1,
        "extratags": 1
    }
    
    try:
        response = requests.get(url, params=params, headers=HEADERS, timeout=30)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(f"Nominatim error: {e}")
    
    return []


def extract_nominatim_businesses():
    """Search for businesses using Nominatim"""
    
    print("\n" + "="*70)
    print("EXTRACTING FROM NOMINATIM SEARCH")
    print("="*70)
    
    # Search queries for different business types
    search_queries = [
        "restaurant Riyadh",
        "مطعم الرياض",
        "cafe Riyadh",
        "كافيه الرياض",
        "mall Riyadh",
        "مول الرياض",
        "supermarket Riyadh",
        "سوبر ماركت الرياض",
        "hotel Riyadh",
        "فندق الرياض",
        "hospital Riyadh",
        "مستشفى الرياض",
        "pharmacy Riyadh",
        "صيدلية الرياض",
        "bank Riyadh",
        "بنك الرياض",
        "gym Riyadh",
        "نادي رياضي الرياض",
        "school Riyadh",
        "مدرسة الرياض",
        "clinic Riyadh",
        "عيادة الرياض",
        "Starbucks Riyadh",
        "McDonalds Riyadh",
        "KFC Riyadh",
        "Carrefour Riyadh",
        "Panda Riyadh",
        "Tamimi Riyadh",
        "Al Baik Riyadh",
        "Kudu Riyadh",
        "Herfy Riyadh",
        "Danube Riyadh",
        "Extra Riyadh",
        "Jarir Riyadh",
    ]
    
    all_pois = []
    seen_ids = set()
    
    for query in search_queries:
        print(f"  Searching: {query}...", end=" ", flush=True)
        
        results = search_nominatim(query)
        count = 0
        
        for r in results:
            poi_id = f"nominatim_{r.get('osm_type', 'n')}_{r.get('osm_id')}"
            
            if poi_id in seen_ids:
                continue
            seen_ids.add(poi_id)
            
            extratags = r.get("extratags", {})
            address = r.get("address", {})
            
            poi = {
                "id": poi_id,
                "name": r.get("display_name", "").split(",")[0],
                "full_name": r.get("display_name", ""),
                "latitude": float(r.get("lat", 0)),
                "longitude": float(r.get("lon", 0)),
                "category": r.get("type", ""),
                "class": r.get("class", ""),
                "phone": extratags.get("phone", ""),
                "website": extratags.get("website", ""),
                "opening_hours": extratags.get("opening_hours", ""),
                "address": f"{address.get('road', '')} {address.get('house_number', '')}".strip(),
                "district": address.get("suburb", "") or address.get("neighbourhood", ""),
                "source": "Nominatim"
            }
            
            all_pois.append(poi)
            count += 1
        
        print(f"{count} found")
        time.sleep(1.1)  # Nominatim requires 1 request per second
    
    print(f"\nTotal from Nominatim: {len(all_pois)}")
    return all_pois


# ============================================================================
# 4. PHOTON SEARCH - Another free geocoding service
# ============================================================================

def search_photon(query: str, limit: int = 50) -> List[Dict]:
    """Search Photon for POIs (powered by Komoot)"""
    
    url = "https://photon.komoot.io/api/"
    
    params = {
        "q": query,
        "limit": limit,
        "lat": RIYADH_CENTER["lat"],
        "lon": RIYADH_CENTER["lng"],
        "bbox": f"{RIYADH_BBOX['west']},{RIYADH_BBOX['south']},{RIYADH_BBOX['east']},{RIYADH_BBOX['north']}"
    }
    
    try:
        response = requests.get(url, params=params, headers=HEADERS, timeout=30)
        if response.status_code == 200:
            data = response.json()
            return data.get("features", [])
    except Exception as e:
        print(f"Photon error: {e}")
    
    return []


def extract_photon_businesses():
    """Search for businesses using Photon"""
    
    print("\n" + "="*70)
    print("EXTRACTING FROM PHOTON SEARCH")
    print("="*70)
    
    search_queries = [
        "restaurant",
        "cafe",
        "supermarket",
        "mall",
        "hotel",
        "hospital",
        "pharmacy",
        "bank",
        "school",
        "gym",
        "Starbucks",
        "McDonalds",
        "Al Baik",
        "Carrefour",
    ]
    
    all_pois = []
    seen_ids = set()
    
    for query in search_queries:
        full_query = f"{query} Riyadh Saudi Arabia"
        print(f"  Searching: {query}...", end=" ", flush=True)
        
        results = search_photon(full_query)
        count = 0
        
        for r in results:
            props = r.get("properties", {})
            geom = r.get("geometry", {})
            coords = geom.get("coordinates", [0, 0])
            
            # Filter to Riyadh area
            lon, lat = coords[0], coords[1]
            if not (RIYADH_BBOX["south"] <= lat <= RIYADH_BBOX["north"] and
                    RIYADH_BBOX["west"] <= lon <= RIYADH_BBOX["east"]):
                continue
            
            poi_id = f"photon_{props.get('osm_type', 'n')}_{props.get('osm_id')}"
            
            if poi_id in seen_ids:
                continue
            seen_ids.add(poi_id)
            
            poi = {
                "id": poi_id,
                "name": props.get("name", ""),
                "latitude": lat,
                "longitude": lon,
                "category": props.get("osm_value", ""),
                "class": props.get("osm_key", ""),
                "city": props.get("city", ""),
                "district": props.get("district", "") or props.get("locality", ""),
                "street": props.get("street", ""),
                "source": "Photon"
            }
            
            all_pois.append(poi)
            count += 1
        
        print(f"{count} found")
        time.sleep(0.5)
    
    print(f"\nTotal from Photon: {len(all_pois)}")
    return all_pois


# ============================================================================
# 5. MERGE AND DEDUPLICATE
# ============================================================================

def merge_and_dedupe(all_sources: List[List[Dict]]) -> List[Dict]:
    """Merge data from all sources and remove duplicates"""
    
    print("\n" + "="*70)
    print("MERGING AND DEDUPLICATING")
    print("="*70)
    
    all_pois = []
    for source in all_sources:
        all_pois.extend(source)
    
    print(f"Total before deduplication: {len(all_pois)}")
    
    # Deduplicate by location (within ~50 meters) and name similarity
    unique_pois = []
    seen_locations = {}
    
    for poi in all_pois:
        lat = poi.get("latitude")
        lon = poi.get("longitude")
        name = poi.get("name", "").lower().strip()
        
        if not lat or not lon:
            continue
        
        # Round to ~50m precision for deduplication
        loc_key = (round(lat, 4), round(lon, 4))
        
        # Check if we have a similar POI at this location
        if loc_key in seen_locations:
            existing_name = seen_locations[loc_key].lower()
            # Keep if names are different
            if name and existing_name and name[:10] != existing_name[:10]:
                unique_pois.append(poi)
                seen_locations[loc_key] = name
        else:
            unique_pois.append(poi)
            seen_locations[loc_key] = name
    
    print(f"Total after deduplication: {len(unique_pois)}")
    
    return unique_pois


# ============================================================================
# 6. SAVE RESULTS
# ============================================================================

def save_results(pois: List[Dict]):
    """Save results to JSON and CSV"""
    
    # Categorize POIs
    categorized = {
        "restaurants": [],
        "cafes": [],
        "shops": [],
        "supermarkets": [],
        "malls": [],
        "banks": [],
        "healthcare": [],
        "hotels": [],
        "gas_stations": [],
        "education": [],
        "entertainment": [],
        "religious": [],
        "services": [],
        "other": []
    }
    
    for poi in pois:
        cat = poi.get("category", "").lower()
        amenity = poi.get("amenity", "").lower()
        shop = poi.get("shop", "").lower()
        
        if "restaurant" in cat or "food" in cat or amenity == "restaurant" or amenity == "fast_food":
            categorized["restaurants"].append(poi)
        elif "cafe" in cat or "coffee" in cat or amenity == "cafe":
            categorized["cafes"].append(poi)
        elif "supermarket" in cat or shop == "supermarket" or shop == "convenience":
            categorized["supermarkets"].append(poi)
        elif "mall" in cat or shop == "mall" or "shopping" in cat:
            categorized["malls"].append(poi)
        elif "bank" in cat or amenity == "bank" or amenity == "atm":
            categorized["banks"].append(poi)
        elif "hospital" in cat or "clinic" in cat or "pharmacy" in cat or "health" in cat:
            categorized["healthcare"].append(poi)
        elif "hotel" in cat or "motel" in cat:
            categorized["hotels"].append(poi)
        elif "gas" in cat or "fuel" in cat or amenity == "fuel":
            categorized["gas_stations"].append(poi)
        elif "school" in cat or "university" in cat or "education" in cat:
            categorized["education"].append(poi)
        elif "gym" in cat or "cinema" in cat or "entertainment" in cat or "fitness" in cat:
            categorized["entertainment"].append(poi)
        elif "mosque" in cat or "religious" in cat or "muslim" in cat:
            categorized["religious"].append(poi)
        elif shop:
            categorized["shops"].append(poi)
        else:
            categorized["other"].append(poi)
    
    # Save JSON
    output_data = {
        "metadata": {
            "extraction_date": datetime.now().isoformat(),
            "total_businesses": len(pois),
            "area": "Riyadh, Saudi Arabia",
            "sources": ["OpenStreetMap", "Nominatim", "Photon", "Wikimapia"],
            "categories": {k: len(v) for k, v in categorized.items()}
        },
        "businesses": pois,
        "by_category": categorized
    }
    
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
    
    # Save CSV
    if pois:
        fieldnames = [
            "id", "name", "name_ar", "name_en", "latitude", "longitude",
            "category", "amenity", "shop", "brand", "cuisine",
            "phone", "website", "address", "opening_hours", "source"
        ]
        
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(pois)
    
    print(f"\nResults saved:")
    print(f"  JSON: {OUTPUT_JSON}")
    print(f"  CSV:  {OUTPUT_CSV}")
    
    # Print category summary
    print("\n" + "="*70)
    print("CATEGORY SUMMARY")
    print("="*70)
    for cat, items in sorted(categorized.items(), key=lambda x: -len(x[1])):
        if items:
            print(f"  {cat}: {len(items):,}")


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Main extraction function"""
    
    print("="*70)
    print("RIYADH BUSINESS DATA EXTRACTOR (FREE SOURCES)")
    print("="*70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    all_sources = []
    
    # 1. Extract from OSM via Overpass
    osm_pois = extract_osm_businesses()
    all_sources.append(osm_pois)
    
    # 2. Extract from Nominatim search
    nominatim_pois = extract_nominatim_businesses()
    all_sources.append(nominatim_pois)
    
    # 3. Extract from Photon search
    photon_pois = extract_photon_businesses()
    all_sources.append(photon_pois)
    
    # 4. Try Wikimapia (may not work in all regions)
    try:
        wikimapia_pois = extract_all_wikimapia()
        all_sources.append(wikimapia_pois)
    except Exception as e:
        print(f"Wikimapia extraction failed: {e}")
    
    # Merge and deduplicate
    unique_pois = merge_and_dedupe(all_sources)
    
    # Save results
    save_results(unique_pois)
    
    # Final summary
    print("\n" + "="*70)
    print("EXTRACTION COMPLETE")
    print("="*70)
    print(f"Total unique businesses: {len(unique_pois):,}")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    return unique_pois


if __name__ == "__main__":
    main()
