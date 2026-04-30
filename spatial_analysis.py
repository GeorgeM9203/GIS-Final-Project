

import geopandas as gpd

from pathlib import Path

BASE_DIR = Path(__file__).parent
PROCESSED_DIR = BASE_DIR / "data" / "processed"

# 0.5 miles in US Survey Feet (EPSG:2229)
BUFFER_DIST_FEET = 2640

def run_spatial_analysis():
    tracts_path = PROCESSED_DIR / "tracts_with_vehicles.gpkg"
    stops_path = PROCESSED_DIR / "highfreq_stops.gpkg"

    tracts = gpd.read_file(tracts_path)
    stops = gpd.read_file(stops_path)
    print(f"   Tracts CRS: {tracts.crs.name}, Stops CRS: {stops.crs.name}")
    stops["geometry"] = stops.geometry.buffer(BUFFER_DIST_FEET)
    
    # Dissolving all overlapping buffers into a single geometry to speed up difference operations
    coverage_area = stops.dissolve()

    # Spatial difference: keep only parts of tracts that do NOT intersect the coverage area
    deserts = tracts.overlay(coverage_area, how="difference")
    
    # Calculate the new area (square miles)
    deserts["desert_area_sqmi"] = (deserts.geometry.area / 5280**2).round(4)
    
    # Merge with original tracts to get original area and compute percentage
    # (The overlay difference operation drops any tract completely covered by transit)
    final_deserts = tracts.merge(
        deserts[["GEOID", "desert_area_sqmi"]], 
        on="GEOID", 
        how="left"
    )
    

    final_deserts["desert_area_sqmi"] = final_deserts["desert_area_sqmi"].fillna(0)
    
    # Calculate percentage of tract area that is a desert
    final_deserts["pct_desert_area"] = (
        (final_deserts["desert_area_sqmi"] / final_deserts["area_sqmi"]) * 100
    ).round(2)
    

    final_deserts["pct_desert_area"] = final_deserts["pct_desert_area"].clip(upper=100)
    
    out_path = PROCESSED_DIR / "transit_deserts_final.gpkg"
    final_deserts.to_file(out_path, driver="GPKG")
    
    print(f"   (Average tract desert coverage: {final_deserts['pct_desert_area'].mean():.1f}%)")

if __name__ == "__main__":
    run_spatial_analysis()
