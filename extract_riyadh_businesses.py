#!/usr/bin/env python3
"""
================================================================================
RIYADH BUSINESS GEOLOCATION DATA EXTRACTOR
================================================================================

Extracts all businesses (restaurants, shops, cafes, supermarkets, etc.) in Riyadh
using Google Places API (New) - the most up-to-date source for business data.

Features:
- Grid-based search to cover entire Riyadh metropolitan area
- Multiple business categories (restaurants, retail, services, etc.)
- Automatic deduplication
- Progress saving and resume capability
- Output to both JSON and CSV formats

Requirements:
- Google Cloud API Key with Places API enabled
- Set environment variable: export GOOGLE_PLACES_API_KEY="your_key"

Author: Riyadh Digital Twin Project
================================================================================
"""

import os
import sys
import json
import csv
import time
import requests
from datetime import datetime
from typing import Dict, List, Optional, Set
import math

# ============================================================================
# CONFIGURATION
# ============================================================================

# Riyadh bounding box (covers entire metropolitan area)
RIYADH_BOUNDS = {
    "north": 25.0500,   # Northern limit
    "south": 24.4500,   # Southern limit  
    "east": 47.1500,    # Eastern limit
    "west": 46.4000,    # Western limit
}

# Grid cell size in degrees (approximately 2km x 2km)
GRID_SIZE = 0.02

# Search radius in meters (1500m to ensure overlap)
SEARCH_RADIUS = 1500

# Business types to extract (Google Places API types)
# Full list: https://developers.google.com/maps/documentation/places/web-service/place-types
BUSINESS_TYPES = [
    # Food & Dining
    "restaurant",
    "cafe", 
    "bakery",
    "bar",
    "meal_delivery",
    "meal_takeaway",
    "fast_food_restaurant",
    "coffee_shop",
    "ice_cream_shop",
    "pizza_restaurant",
    "seafood_restaurant",
    "steak_house",
    "sushi_restaurant",
    "vegetarian_restaurant",
    "middle_eastern_restaurant",
    "indian_restaurant",
    "italian_restaurant",
    "chinese_restaurant",
    "japanese_restaurant",
    "korean_restaurant",
    "thai_restaurant",
    "mexican_restaurant",
    "american_restaurant",
    "french_restaurant",
    "lebanese_restaurant",
    "turkish_restaurant",
    "breakfast_restaurant",
    "brunch_restaurant",
    "hamburger_restaurant",
    "sandwich_shop",
    "juice_shop",
    
    # Shopping & Retail
    "shopping_mall",
    "department_store",
    "supermarket",
    "grocery_store",
    "convenience_store",
    "clothing_store",
    "shoe_store",
    "jewelry_store",
    "electronics_store",
    "furniture_store",
    "home_goods_store",
    "hardware_store",
    "book_store",
    "gift_shop",
    "florist",
    "pet_store",
    "sporting_goods_store",
    "toy_store",
    "liquor_store",
    "discount_store",
    "wholesale_store",
    
    # Services
    "bank",
    "atm",
    "car_dealer",
    "car_rental",
    "car_repair",
    "car_wash",
    "gas_station",
    "electric_vehicle_charging_station",
    "parking",
    "pharmacy",
    "hospital",
    "doctor",
    "dentist",
    "veterinary_care",
    "hair_salon",
    "beauty_salon",
    "spa",
    "gym",
    "laundry",
    "real_estate_agency",
    "insurance_agency",
    "travel_agency",
    "lawyer",
    "accountant",
    
    # Entertainment & Leisure
    "movie_theater",
    "amusement_park",
    "bowling_alley",
    "night_club",
    "casino",
    "museum",
    "art_gallery",
    "zoo",
    "aquarium",
    "stadium",
    "park",
    
    # Accommodation
    "hotel",
    "motel",
    "lodging",
    
    # Education
    "school",
    "university",
    "library",
    
    # Religious
    "mosque",
    "church",
    
    # Government & Public Services
    "post_office",
    "police",
    "fire_station",
    "embassy",
    "local_government_office",
]

# API Configuration
GOOGLE_PLACES_API_BASE = "https://places.googleapis.com/v1/places:searchNearby"
GOOGLE_PLACES_TEXT_SEARCH = "https://places.googleapis.com/v1/places:searchText"

# Rate limiting
REQUESTS_PER_SECOND = 10
REQUEST_DELAY = 1 / REQUESTS_PER_SECOND

# Output files
OUTPUT_JSON = "/workspace/riyadh_businesses_geo.json"
OUTPUT_CSV = "/workspace/riyadh_businesses_geo.csv"
PROGRESS_FILE = "/workspace/business_extraction_progress.json"


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_api_key() -> str:
    """Get Google Places API key from environment"""
    api_key = os.environ.get("GOOGLE_PLACES_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        print("="*70)
        print("ERROR: Google Places API Key not found!")
        print("="*70)
        print("\nTo use this script, you need a Google Cloud API key with Places API enabled.")
        print("\nSteps to get an API key:")
        print("1. Go to https://console.cloud.google.com/")
        print("2. Create a new project or select existing one")
        print("3. Enable 'Places API' in APIs & Services")
        print("4. Create credentials (API Key)")
        print("5. Set the environment variable:")
        print("   export GOOGLE_PLACES_API_KEY='your_api_key_here'")
        print("\nNote: Google offers $200 free credit monthly for Maps Platform.")
        print("="*70)
        sys.exit(1)
    return api_key


def generate_grid_points() -> List[Dict]:
    """Generate grid of search points covering Riyadh"""
    points = []
    
    lat = RIYADH_BOUNDS["south"]
    while lat <= RIYADH_BOUNDS["north"]:
        lng = RIYADH_BOUNDS["west"]
        while lng <= RIYADH_BOUNDS["east"]:
            points.append({
                "lat": round(lat, 4),
                "lng": round(lng, 4)
            })
            lng += GRID_SIZE
        lat += GRID_SIZE
    
    return points


def search_nearby_places(api_key: str, lat: float, lng: float, 
                         place_types: List[str], radius: int = SEARCH_RADIUS) -> List[Dict]:
    """Search for places near a location using Google Places API (New)"""
    
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": "places.id,places.displayName,places.formattedAddress,places.location,places.types,places.primaryType,places.primaryTypeDisplayName,places.rating,places.userRatingCount,places.priceLevel,places.businessStatus,places.currentOpeningHours,places.internationalPhoneNumber,places.websiteUri,places.googleMapsUri"
    }
    
    body = {
        "locationRestriction": {
            "circle": {
                "center": {
                    "latitude": lat,
                    "longitude": lng
                },
                "radius": radius
            }
        },
        "includedTypes": place_types,
        "maxResultCount": 20,
        "languageCode": "en"
    }
    
    try:
        response = requests.post(GOOGLE_PLACES_API_BASE, headers=headers, json=body, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            return data.get("places", [])
        elif response.status_code == 429:
            print("Rate limited, waiting...")
            time.sleep(5)
            return []
        else:
            # print(f"API Error {response.status_code}: {response.text[:200]}")
            return []
            
    except Exception as e:
        print(f"Request error: {e}")
        return []


def text_search_places(api_key: str, query: str, lat: float, lng: float, 
                       radius: int = 5000) -> List[Dict]:
    """Search for places using text query"""
    
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": "places.id,places.displayName,places.formattedAddress,places.location,places.types,places.primaryType,places.primaryTypeDisplayName,places.rating,places.userRatingCount,places.priceLevel,places.businessStatus,places.internationalPhoneNumber,places.websiteUri,places.googleMapsUri"
    }
    
    body = {
        "textQuery": query,
        "locationBias": {
            "circle": {
                "center": {
                    "latitude": lat,
                    "longitude": lng
                },
                "radius": radius
            }
        },
        "maxResultCount": 20,
        "languageCode": "en"
    }
    
    try:
        response = requests.post(GOOGLE_PLACES_TEXT_SEARCH, headers=headers, json=body, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            return data.get("places", [])
        else:
            return []
            
    except Exception as e:
        return []


def parse_place(place: Dict) -> Dict:
    """Parse a Google Places API response into standardized format"""
    
    location = place.get("location", {})
    display_name = place.get("displayName", {})
    primary_type_display = place.get("primaryTypeDisplayName", {})
    opening_hours = place.get("currentOpeningHours", {})
    
    return {
        "place_id": place.get("id", ""),
        "name": display_name.get("text", ""),
        "name_language": display_name.get("languageCode", ""),
        "latitude": location.get("latitude"),
        "longitude": location.get("longitude"),
        "address": place.get("formattedAddress", ""),
        "types": place.get("types", []),
        "primary_type": place.get("primaryType", ""),
        "primary_type_display": primary_type_display.get("text", ""),
        "rating": place.get("rating"),
        "rating_count": place.get("userRatingCount"),
        "price_level": place.get("priceLevel", ""),
        "business_status": place.get("businessStatus", ""),
        "phone": place.get("internationalPhoneNumber", ""),
        "website": place.get("websiteUri", ""),
        "google_maps_url": place.get("googleMapsUri", ""),
        "open_now": opening_hours.get("openNow"),
    }


def load_progress() -> Dict:
    """Load extraction progress from file"""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {"completed_points": [], "businesses": {}}


def save_progress(progress: Dict):
    """Save extraction progress to file"""
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f)


def save_results(businesses: Dict):
    """Save results to JSON and CSV files"""
    
    business_list = list(businesses.values())
    
    # Save JSON
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump({
            "metadata": {
                "source": "Google Places API",
                "extraction_date": datetime.now().isoformat(),
                "total_businesses": len(business_list),
                "area": "Riyadh, Saudi Arabia"
            },
            "businesses": business_list
        }, f, ensure_ascii=False, indent=2)
    
    # Save CSV
    if business_list:
        # Flatten types list for CSV
        csv_data = []
        for b in business_list:
            row = b.copy()
            row["types"] = "|".join(row.get("types", []))
            csv_data.append(row)
        
        fieldnames = [
            "place_id", "name", "name_language", "latitude", "longitude",
            "address", "types", "primary_type", "primary_type_display",
            "rating", "rating_count", "price_level", "business_status",
            "phone", "website", "google_maps_url", "open_now"
        ]
        
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(csv_data)
    
    print(f"\nResults saved:")
    print(f"  JSON: {OUTPUT_JSON}")
    print(f"  CSV:  {OUTPUT_CSV}")


# ============================================================================
# MAIN EXTRACTION
# ============================================================================

def extract_businesses():
    """Main extraction function"""
    
    print("="*70)
    print("RIYADH BUSINESS GEOLOCATION DATA EXTRACTOR")
    print("="*70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Data source: Google Places API (most up-to-date)")
    print("="*70)
    
    # Get API key
    api_key = get_api_key()
    print("✓ API Key found")
    
    # Generate grid points
    grid_points = generate_grid_points()
    total_points = len(grid_points)
    print(f"✓ Generated {total_points} search grid points covering Riyadh")
    
    # Calculate area
    lat_range = RIYADH_BOUNDS["north"] - RIYADH_BOUNDS["south"]
    lng_range = RIYADH_BOUNDS["east"] - RIYADH_BOUNDS["west"]
    area_km2 = lat_range * 111 * lng_range * 85  # Approximate conversion
    print(f"✓ Search area: ~{area_km2:.0f} km²")
    
    # Load previous progress
    progress = load_progress()
    completed_points = set(tuple(p) for p in progress.get("completed_points", []))
    businesses = progress.get("businesses", {})
    
    if businesses:
        print(f"✓ Resuming from previous run: {len(businesses)} businesses already found")
    
    # Business type groups (to batch similar types)
    type_groups = [
        # Food
        ["restaurant", "cafe", "bakery", "fast_food_restaurant", "coffee_shop"],
        ["meal_delivery", "meal_takeaway", "pizza_restaurant", "hamburger_restaurant"],
        ["middle_eastern_restaurant", "indian_restaurant", "italian_restaurant"],
        ["chinese_restaurant", "japanese_restaurant", "korean_restaurant", "thai_restaurant"],
        ["seafood_restaurant", "steak_house", "sushi_restaurant", "vegetarian_restaurant"],
        ["breakfast_restaurant", "brunch_restaurant", "sandwich_shop", "ice_cream_shop", "juice_shop"],
        
        # Shopping
        ["shopping_mall", "department_store", "supermarket", "grocery_store"],
        ["clothing_store", "shoe_store", "jewelry_store", "electronics_store"],
        ["furniture_store", "home_goods_store", "hardware_store", "book_store"],
        ["convenience_store", "gift_shop", "florist", "pet_store", "sporting_goods_store"],
        
        # Services
        ["bank", "atm", "pharmacy", "hospital", "doctor", "dentist"],
        ["car_dealer", "car_rental", "car_repair", "car_wash", "gas_station"],
        ["hair_salon", "beauty_salon", "spa", "gym", "laundry"],
        ["hotel", "lodging"],
        
        # Entertainment
        ["movie_theater", "amusement_park", "museum", "park", "stadium"],
        
        # Other
        ["mosque", "school", "university", "post_office"],
    ]
    
    print(f"\nSearching for {len(BUSINESS_TYPES)} business types...")
    print("-"*70)
    
    # Track stats
    new_businesses = 0
    api_calls = 0
    start_time = time.time()
    
    try:
        for i, point in enumerate(grid_points):
            point_key = (point["lat"], point["lng"])
            
            # Skip already completed points
            if point_key in completed_points:
                continue
            
            # Search each type group at this location
            for type_group in type_groups:
                places = search_nearby_places(
                    api_key, 
                    point["lat"], 
                    point["lng"],
                    type_group
                )
                api_calls += 1
                
                for place in places:
                    parsed = parse_place(place)
                    place_id = parsed["place_id"]
                    
                    if place_id and place_id not in businesses:
                        businesses[place_id] = parsed
                        new_businesses += 1
                
                time.sleep(REQUEST_DELAY)
            
            # Mark point as completed
            completed_points.add(point_key)
            
            # Progress update
            if (i + 1) % 50 == 0 or i == total_points - 1:
                elapsed = time.time() - start_time
                rate = api_calls / elapsed if elapsed > 0 else 0
                remaining = total_points - i - 1
                eta = remaining * len(type_groups) / rate / 60 if rate > 0 else 0
                
                print(f"Progress: {i+1}/{total_points} points ({100*(i+1)/total_points:.1f}%) | "
                      f"Businesses: {len(businesses):,} | "
                      f"API calls: {api_calls} | "
                      f"ETA: {eta:.0f} min")
                
                # Save progress periodically
                progress["completed_points"] = [list(p) for p in completed_points]
                progress["businesses"] = businesses
                save_progress(progress)
    
    except KeyboardInterrupt:
        print("\n\nInterrupted! Saving progress...")
    
    except Exception as e:
        print(f"\nError: {e}")
        print("Saving progress...")
    
    finally:
        # Save final progress
        progress["completed_points"] = [list(p) for p in completed_points]
        progress["businesses"] = businesses
        save_progress(progress)
        
        # Save results
        save_results(businesses)
    
    # Summary
    elapsed = time.time() - start_time
    print("\n" + "="*70)
    print("EXTRACTION COMPLETE")
    print("="*70)
    print(f"Total businesses found: {len(businesses):,}")
    print(f"New businesses this run: {new_businesses:,}")
    print(f"Total API calls: {api_calls}")
    print(f"Time elapsed: {elapsed/60:.1f} minutes")
    print(f"Output files:")
    print(f"  - {OUTPUT_JSON}")
    print(f"  - {OUTPUT_CSV}")
    print("="*70)
    
    return businesses


# ============================================================================
# ALTERNATIVE: Text-based search for specific business types
# ============================================================================

def search_by_text_queries():
    """Alternative approach using text search for specific business categories"""
    
    api_key = get_api_key()
    
    # Riyadh center coordinates
    riyadh_center = {"lat": 24.7136, "lng": 46.6753}
    
    # Search queries for different business types
    queries = [
        "restaurants in Riyadh",
        "cafes in Riyadh",
        "coffee shops in Riyadh",
        "fast food in Riyadh",
        "supermarkets in Riyadh",
        "malls in Riyadh",
        "shopping centers in Riyadh",
        "grocery stores in Riyadh",
        "pharmacies in Riyadh",
        "banks in Riyadh",
        "hotels in Riyadh",
        "gyms in Riyadh",
        "salons in Riyadh",
        "car service in Riyadh",
        "gas stations in Riyadh",
        "hospitals in Riyadh",
        "clinics in Riyadh",
        "schools in Riyadh",
        "mosques in Riyadh",
    ]
    
    businesses = {}
    
    print("Searching by text queries...")
    for query in queries:
        print(f"  Searching: {query}...")
        places = text_search_places(api_key, query, riyadh_center["lat"], riyadh_center["lng"])
        
        for place in places:
            parsed = parse_place(place)
            place_id = parsed["place_id"]
            if place_id:
                businesses[place_id] = parsed
        
        time.sleep(REQUEST_DELAY)
    
    print(f"\nFound {len(businesses)} businesses via text search")
    return businesses


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Extract Riyadh business geolocation data")
    parser.add_argument("--mode", choices=["full", "text", "test"], default="full",
                       help="Extraction mode: full (grid search), text (query search), test (quick test)")
    parser.add_argument("--api-key", help="Google Places API key (or set GOOGLE_PLACES_API_KEY env var)")
    
    args = parser.parse_args()
    
    if args.api_key:
        os.environ["GOOGLE_PLACES_API_KEY"] = args.api_key
    
    if args.mode == "full":
        extract_businesses()
    elif args.mode == "text":
        businesses = search_by_text_queries()
        save_results(businesses)
    elif args.mode == "test":
        # Quick test with smaller area
        print("Running quick test (smaller area)...")
        RIYADH_BOUNDS["north"] = 24.75
        RIYADH_BOUNDS["south"] = 24.70
        RIYADH_BOUNDS["east"] = 46.72
        RIYADH_BOUNDS["west"] = 46.65
        extract_businesses()
