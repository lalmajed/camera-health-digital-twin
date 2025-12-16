#!/usr/bin/env python3
"""
================================================================================
FOURSQUARE PLACES EXTRACTOR - FREE TIER
================================================================================

Foursquare offers a FREE tier with 99,000 API calls per month.
This is one of the BEST free sources for up-to-date business data.

To get a free API key:
1. Go to https://foursquare.com/developers/
2. Sign up for a free account
3. Create a new project
4. Get your API key

Then run: python extract_foursquare.py --api-key YOUR_KEY

================================================================================
"""

import os
import sys
import json
import csv
import time
import requests
from datetime import datetime
from typing import List, Dict

# Riyadh bounds
RIYADH_CENTER = {"lat": 24.7136, "lng": 46.6753}
RIYADH_BOUNDS = {
    "ne": {"lat": 25.05, "lng": 47.15},
    "sw": {"lat": 24.45, "lng": 46.40}
}

# Foursquare API v3
FSQ_API_BASE = "https://api.foursquare.com/v3/places/search"

# Business categories (Foursquare category IDs)
# Full list: https://docs.foursquare.com/data-products/docs/categories
CATEGORIES = {
    "13000": "Dining and Drinking",
    "13065": "Restaurant", 
    "13032": "Café",
    "13034": "Coffee Shop",
    "13145": "Fast Food",
    "17000": "Retail",
    "17069": "Shopping Mall",
    "17142": "Supermarket",
    "17027": "Clothing Store",
    "17044": "Electronics Store",
    "11000": "Financial Services",
    "11045": "Bank",
    "15000": "Health and Medicine",
    "15014": "Hospital",
    "15054": "Pharmacy",
    "19000": "Travel and Transportation",
    "19014": "Gas Station",
    "19009": "Hotel",
    "12000": "Sports and Recreation",
    "12003": "Fitness Center",
    "10000": "Arts and Entertainment",
    "14000": "Landmarks and Outdoors",
    "12078": "Spa",
    "11057": "Real Estate",
    "18000": "Government",
    "16000": "Professional Services",
}

OUTPUT_JSON = "/workspace/riyadh_businesses_foursquare.json"
OUTPUT_CSV = "/workspace/riyadh_businesses_foursquare.csv"


def get_api_key():
    """Get Foursquare API key"""
    api_key = os.environ.get("FOURSQUARE_API_KEY") or os.environ.get("FSQ_API_KEY")
    if not api_key:
        print("="*70)
        print("FOURSQUARE API KEY REQUIRED (FREE)")
        print("="*70)
        print("\nFoursquare offers 99,000 FREE API calls per month!")
        print("\nTo get your free API key:")
        print("1. Go to https://foursquare.com/developers/")
        print("2. Sign up (free)")
        print("3. Create a project")
        print("4. Copy your API key")
        print("\nThen run:")
        print("  export FOURSQUARE_API_KEY='your_key'")
        print("  python extract_foursquare.py")
        print("\nOr:")
        print("  python extract_foursquare.py --api-key YOUR_KEY")
        print("="*70)
        return None
    return api_key


def search_foursquare(api_key: str, lat: float, lng: float, 
                      category: str = None, query: str = None,
                      radius: int = 5000, limit: int = 50) -> List[Dict]:
    """Search Foursquare Places API"""
    
    headers = {
        "Authorization": api_key,
        "Accept": "application/json"
    }
    
    params = {
        "ll": f"{lat},{lng}",
        "radius": radius,
        "limit": limit,
        "sort": "RELEVANCE"
    }
    
    if category:
        params["categories"] = category
    if query:
        params["query"] = query
    
    try:
        response = requests.get(FSQ_API_BASE, headers=headers, params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            return data.get("results", [])
        elif response.status_code == 401:
            print("Invalid API key")
            return []
        elif response.status_code == 429:
            print("Rate limited, waiting...")
            time.sleep(5)
            return []
        else:
            return []
            
    except Exception as e:
        print(f"Error: {e}")
        return []


def parse_place(place: Dict) -> Dict:
    """Parse Foursquare place into standard format"""
    
    location = place.get("location", {})
    geocodes = place.get("geocodes", {})
    main_geo = geocodes.get("main", {})
    categories = place.get("categories", [])
    
    return {
        "id": f"fsq_{place.get('fsq_id', '')}",
        "name": place.get("name", ""),
        "latitude": main_geo.get("latitude") or location.get("lat"),
        "longitude": main_geo.get("longitude") or location.get("lng"),
        "address": location.get("formatted_address", ""),
        "locality": location.get("locality", ""),
        "region": location.get("region", ""),
        "postcode": location.get("postcode", ""),
        "country": location.get("country", ""),
        "categories": [c.get("name") for c in categories],
        "primary_category": categories[0].get("name") if categories else "",
        "chain": place.get("chains", [{}])[0].get("name", "") if place.get("chains") else "",
        "distance": place.get("distance"),
        "source": "Foursquare"
    }


def extract_businesses(api_key: str):
    """Extract businesses from Foursquare"""
    
    print("="*70)
    print("FOURSQUARE BUSINESS EXTRACTOR")
    print("="*70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    # Grid of search points to cover Riyadh
    grid_points = []
    lat = RIYADH_BOUNDS["sw"]["lat"]
    while lat <= RIYADH_BOUNDS["ne"]["lat"]:
        lng = RIYADH_BOUNDS["sw"]["lng"]
        while lng <= RIYADH_BOUNDS["ne"]["lng"]:
            grid_points.append({"lat": lat, "lng": lng})
            lng += 0.05  # ~5km grid
        lat += 0.05
    
    print(f"Search grid: {len(grid_points)} points")
    print(f"Categories: {len(CATEGORIES)}")
    
    all_businesses = {}
    api_calls = 0
    
    # Search by category at each grid point
    for cat_id, cat_name in CATEGORIES.items():
        print(f"\nSearching: {cat_name}...")
        
        for point in grid_points:
            places = search_foursquare(
                api_key,
                point["lat"],
                point["lng"],
                category=cat_id,
                radius=5000
            )
            api_calls += 1
            
            for place in places:
                parsed = parse_place(place)
                pid = parsed["id"]
                if pid not in all_businesses:
                    all_businesses[pid] = parsed
            
            time.sleep(0.1)  # Rate limiting
        
        print(f"  Total so far: {len(all_businesses):,} | API calls: {api_calls}")
    
    return list(all_businesses.values()), api_calls


def save_results(businesses: List[Dict]):
    """Save results"""
    
    # JSON
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump({
            "metadata": {
                "source": "Foursquare Places API",
                "extraction_date": datetime.now().isoformat(),
                "total": len(businesses),
                "area": "Riyadh, Saudi Arabia"
            },
            "businesses": businesses
        }, f, ensure_ascii=False, indent=2)
    
    # CSV
    if businesses:
        fieldnames = [
            "id", "name", "latitude", "longitude", "address",
            "locality", "region", "primary_category", "chain", "source"
        ]
        
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(businesses)
    
    print(f"\nSaved: {OUTPUT_JSON}")
    print(f"Saved: {OUTPUT_CSV}")


def main():
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-key", help="Foursquare API key")
    args = parser.parse_args()
    
    if args.api_key:
        os.environ["FOURSQUARE_API_KEY"] = args.api_key
    
    api_key = get_api_key()
    if not api_key:
        sys.exit(1)
    
    businesses, calls = extract_businesses(api_key)
    save_results(businesses)
    
    print("\n" + "="*70)
    print("COMPLETE")
    print("="*70)
    print(f"Total businesses: {len(businesses):,}")
    print(f"API calls used: {calls}")
    print("="*70)


if __name__ == "__main__":
    main()
