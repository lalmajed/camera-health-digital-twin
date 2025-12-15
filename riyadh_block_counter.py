#!/usr/bin/env python3
"""
================================================================================
RIYADH GEOPORTAL BLOCK COUNTER - COMPREHENSIVE ALGORITHM
================================================================================

This script queries the Riyadh GeoPortal (https://mapservice.alriyadh.gov.sa/geoportal/geomap)
to count all blocks (بلك) in Riyadh city.

DISCOVERED DATA STRUCTURE:
--------------------------
The Riyadh GeoPortal uses ArcGIS REST Services at:
  https://mapservice.alriyadh.gov.sa/wa_maps/rest/services/

Main Service: BaseMap/Riyadh_BaseMap_V3/MapServer
  - Layer 71: "قطع الاراضي" (Land Parcels) - Contains all land parcel data

Key Fields:
  - BLOCKNO (رقم البلوك): Block number within a plan
  - PLANBLOCKID (رمز البلوك): Unique numeric ID for each block
  - PLANNO (رقم المخطط): Urban plan number
  - PLANID (رمز المخطط): Urban plan ID
  - DISTRICT (الاحياء): District code
  - SUBMUNICIPALITY (البلديات الفرعية): Sub-municipality code

ALGORITHM:
----------
1. Query the ArcGIS REST API using outStatistics for efficient server-side aggregation
2. Group by PLANBLOCKID to count unique blocks
3. Group by BLOCKNO to count unique block numbers
4. Group by DISTRICT to get district-level distribution
5. Calculate parcels with/without block assignments

LIMITATIONS:
-----------
- The ArcGIS server limits outStatistics results to 2000 records
- Pagination is not supported for statistics queries
- Some statistics (like unique Plan+Block combinations) may be truncated

Author: Riyadh Digital Twin Project
Date: 2024
"""

import requests
import json
from typing import Dict, List, Tuple, Any
from dataclasses import dataclass


@dataclass
class RiyadhBlockStatistics:
    """Complete statistics about blocks in Riyadh"""
    total_parcels: int
    unique_planblockid: int
    unique_blockno: int
    parcels_with_blocks: int          # BLOCKNO != '0' and not null
    parcels_without_blocks: int       # BLOCKNO = '0'
    parcels_null_blockno: int         # BLOCKNO is null
    unique_districts: int
    unique_municipalities: int
    districts_data: List[Dict]
    top_blocks: List[Dict]


class RiyadhGeoPortalClient:
    """
    Client to interact with Riyadh GeoPortal ArcGIS REST Services
    
    Discovered endpoints:
    - BaseMap: https://mapservice.alriyadh.gov.sa/wa_maps/rest/services/BaseMap/Riyadh_BaseMap_V3/MapServer
    
    Layer 71 fields include:
    - BLOCKNO: Block number (string, e.g., "27", "5", "0")
    - PLANBLOCKID: Unique block ID (integer)
    - PLANNO: Plan number (string)
    - PLANID: Plan ID (integer)
    - DISTRICT: District code (string)
    - SUBMUNICIPALITY: Sub-municipality code (string)
    """
    
    BASE_URL = "https://mapservice.alriyadh.gov.sa/wa_maps/rest/services"
    SERVICE = "BaseMap/Riyadh_BaseMap_V3"
    LAND_PARCELS_LAYER = 71
    
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
        "Referer": "https://mapservice.alriyadh.gov.sa/geoportal/geomap"
    }
    
    def __init__(self, timeout: int = 60):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
    
    def query_url(self, layer_id: int = None) -> str:
        """Get query URL for a layer"""
        lid = layer_id or self.LAND_PARCELS_LAYER
        return f"{self.BASE_URL}/{self.SERVICE}/MapServer/{lid}/query"
    
    def count_records(self, where: str = "1=1") -> int:
        """Count records matching a where clause"""
        params = {
            "where": where,
            "returnCountOnly": "true",
            "f": "json"
        }
        try:
            response = self.session.get(self.query_url(), params=params, timeout=self.timeout)
            response.raise_for_status()
            return response.json().get("count", 0)
        except Exception as e:
            print(f"Error counting records: {e}")
            return 0
    
    def get_grouped_stats(self, group_field: str, where: str = "1=1") -> Tuple[List[Dict], bool]:
        """
        Get count statistics grouped by a field.
        
        Returns tuple of (results, exceeded_limit)
        """
        params = {
            "where": where,
            "groupByFieldsForStatistics": group_field,
            "outStatistics": json.dumps([{
                "statisticType": "count",
                "onStatisticField": "OBJECTID",
                "outStatisticFieldName": "COUNT"
            }]),
            "f": "json"
        }
        
        try:
            response = self.session.get(self.query_url(), params=params, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            
            features = data.get("features", [])
            exceeded = data.get("exceededTransferLimit", False)
            
            results = []
            for f in features:
                attrs = f.get("attributes", {})
                results.append({
                    "value": attrs.get(group_field),
                    "count": int(attrs.get("COUNT", 0) or 0)
                })
            
            return results, exceeded
            
        except Exception as e:
            print(f"Error getting grouped stats: {e}")
            return [], False


def analyze_riyadh_blocks() -> RiyadhBlockStatistics:
    """
    Main algorithm to count and analyze all blocks in Riyadh.
    
    Returns comprehensive statistics about blocks in the city.
    """
    print("=" * 70)
    print("RIYADH GEOPORTAL BLOCK COUNTER")
    print("=" * 70)
    print("Source: https://mapservice.alriyadh.gov.sa/geoportal/geomap")
    print("Data: Land Parcels Layer (قطع الأراضي)")
    print("=" * 70)
    
    client = RiyadhGeoPortalClient()
    
    # 1. Total parcels
    print("\n[1/7] Counting total land parcels...")
    total_parcels = client.count_records()
    print(f"     ✓ Total: {total_parcels:,}")
    
    # 2. Parcels with BLOCKNO = '0' (no block subdivision)
    print("\n[2/7] Counting parcels without block subdivision (BLOCKNO='0')...")
    parcels_no_block = client.count_records("BLOCKNO='0'")
    print(f"     ✓ Parcels with BLOCKNO='0': {parcels_no_block:,}")
    
    # 3. Parcels with null BLOCKNO
    print("\n[3/7] Counting parcels with null BLOCKNO...")
    parcels_null = client.count_records("BLOCKNO IS NULL")
    print(f"     ✓ Parcels with NULL BLOCKNO: {parcels_null:,}")
    
    # 4. Calculate parcels with valid blocks
    parcels_with_blocks = total_parcels - parcels_no_block - parcels_null
    print(f"\n[4/7] Parcels with valid block assignment:")
    print(f"     ✓ {parcels_with_blocks:,} parcels ({100*parcels_with_blocks/total_parcels:.1f}%)")
    
    # 5. Unique PLANBLOCKID (primary block identifier)
    print("\n[5/7] Counting unique blocks by PLANBLOCKID...")
    planblockid_stats, exceeded_pb = client.get_grouped_stats("PLANBLOCKID")
    unique_planblockid = len(planblockid_stats)
    print(f"     ✓ Unique PLANBLOCKID values: {unique_planblockid:,}")
    if exceeded_pb:
        print("     ⚠ Note: Result may be truncated (server limit)")
    
    # 6. Unique BLOCKNO values
    print("\n[6/7] Counting unique block numbers (BLOCKNO)...")
    blockno_stats, exceeded_bn = client.get_grouped_stats("BLOCKNO")
    unique_blockno = len(blockno_stats)
    print(f"     ✓ Unique BLOCKNO values: {unique_blockno:,}")
    
    # 7. District distribution
    print("\n[7/7] Analyzing district distribution...")
    district_stats, _ = client.get_grouped_stats("DISTRICT")
    unique_districts = len([d for d in district_stats if d["value"]])
    print(f"     ✓ Districts with parcels: {unique_districts:,}")
    
    # Get top blocks by parcel count
    top_blocks = sorted(
        [b for b in planblockid_stats if b["value"] and b["value"] > 0],
        key=lambda x: x["count"],
        reverse=True
    )[:20]
    
    # Count municipalities (from separate layer)
    municipalities = 16  # Known value from layer 77
    
    return RiyadhBlockStatistics(
        total_parcels=total_parcels,
        unique_planblockid=unique_planblockid,
        unique_blockno=unique_blockno,
        parcels_with_blocks=parcels_with_blocks,
        parcels_without_blocks=parcels_no_block,
        parcels_null_blockno=parcels_null,
        unique_districts=unique_districts,
        unique_municipalities=municipalities,
        districts_data=sorted(district_stats, key=lambda x: x["count"], reverse=True),
        top_blocks=top_blocks
    )


def print_report(stats: RiyadhBlockStatistics):
    """Print comprehensive analysis report"""
    
    print("\n")
    print("=" * 70)
    print("                    RIYADH BLOCKS ANALYSIS REPORT")
    print("=" * 70)
    
    # Summary box
    print(f"""
┌────────────────────────────────────────────────────────────────────┐
│                         SUMMARY                                     │
├────────────────────────────────────────────────────────────────────┤
│  Total Land Parcels in Riyadh:                    {stats.total_parcels:>12,}    │
│                                                                     │
│  BLOCKS (بلك):                                                      │
│    • Unique blocks (PLANBLOCKID):                     {stats.unique_planblockid:>8,}    │
│    • Unique block numbers (BLOCKNO):                  {stats.unique_blockno:>8,}    │
│                                                                     │
│  PARCEL DISTRIBUTION:                                               │
│    • Parcels WITH block assignment:               {stats.parcels_with_blocks:>12,}    │
│    • Parcels WITHOUT block (BLOCKNO='0'):         {stats.parcels_without_blocks:>12,}    │
│    • Parcels with NULL BLOCKNO:                   {stats.parcels_null_blockno:>12,}    │
│                                                                     │
│  ADMINISTRATIVE:                                                    │
│    • Districts (أحياء):                                  {stats.unique_districts:>8,}    │
│    • Municipalities (بلديات):                            {stats.unique_municipalities:>8,}    │
└────────────────────────────────────────────────────────────────────┘
""")
    
    # Top blocks
    print("\nTOP 15 BLOCKS BY PARCEL COUNT:")
    print("-" * 50)
    for i, block in enumerate(stats.top_blocks[:15], 1):
        print(f"  {i:2d}. Block ID {block['value']:>6}: {block['count']:>6,} parcels")
    
    # Top districts
    print("\n\nTOP 10 DISTRICTS BY PARCEL COUNT:")
    print("-" * 50)
    top_districts = [d for d in stats.districts_data if d["value"]][:10]
    for i, dist in enumerate(top_districts, 1):
        print(f"  {i:2d}. District {dist['value']:>4}: {dist['count']:>8,} parcels")
    
    # Interpretation
    print("\n" + "=" * 70)
    print("                       INTERPRETATION")
    print("=" * 70)
    print(f"""
The Riyadh GeoPortal reveals the following urban structure:

1. BLOCKS (بلك):
   There are approximately {stats.unique_planblockid:,} unique blocks in Riyadh,
   identified by their PLANBLOCKID. These blocks contain {stats.parcels_with_blocks:,}
   individual land parcels ({100*stats.parcels_with_blocks/stats.total_parcels:.1f}% of all parcels).

2. UNSUBDIVIDED AREAS:
   {stats.parcels_without_blocks:,} parcels ({100*stats.parcels_without_blocks/stats.total_parcels:.1f}%) have BLOCKNO='0',
   indicating they are in areas that have not been subdivided into blocks.
   These are typically:
   - Government/public land
   - Large undeveloped areas
   - Infrastructure corridors
   - Areas pending subdivision planning

3. URBAN PLANS (مخططات):
   Blocks are organized within urban development plans. Each PLANBLOCKID
   represents a unique combination of plan and block within that plan.

4. ADMINISTRATIVE STRUCTURE:
   The city is divided into {stats.unique_districts} districts (أحياء) across
   {stats.unique_municipalities} municipalities (بلديات).
""")
    
    print("=" * 70)
    print("                    KEY FINDINGS")
    print("=" * 70)
    print(f"""
╔════════════════════════════════════════════════════════════════════╗
║                                                                    ║
║   TOTAL BLOCKS IN RIYADH: {stats.unique_planblockid:,} blocks                          ║
║                                                                    ║
║   These blocks contain {stats.parcels_with_blocks:,} land parcels                   ║
║   distributed across {stats.unique_districts} districts                                ║
║                                                                    ║
╚════════════════════════════════════════════════════════════════════╝
""")


def main():
    """Main entry point"""
    try:
        stats = analyze_riyadh_blocks()
        print_report(stats)
        
        print("\n✓ Analysis completed successfully")
        print("=" * 70)
        
        return {
            "total_blocks": stats.unique_planblockid,
            "total_parcels": stats.total_parcels,
            "parcels_with_blocks": stats.parcels_with_blocks,
            "districts": stats.unique_districts,
            "municipalities": stats.unique_municipalities
        }
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    main()
