import warnings
warnings.filterwarnings("ignore")

import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

BASE_DIR = Path(__file__).parent
PROCESSED_DIR = BASE_DIR / "data" / "processed"
RESULTS_DIR = BASE_DIR / "data" / "results"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

# Thresholds
HIGH_NEED_THRESHOLD = 10.0 # > 10% zero vehicle households
LOW_ACCESS_THRESHOLD = 50.0 # > 50% of the tract is a transit desert

def run_visualization():
    print("=" * 55)
    print("  Transit Deserts LA — Step 4: Statistics & Maps")
    print("=" * 55)

    data_path = PROCESSED_DIR / "transit_deserts_final.gpkg"
    if not data_path.exists():
        raise FileNotFoundError("transit_deserts_final.gpkg not found. Run spatial_analysis.py first.")

    print("1️⃣  Loading finalized transit deserts spatial data...")
    df = gpd.read_file(data_path)

    # Filter out Catalina and San Clemente Islands
    # Their massive ocean boundaries distort the map scale
    df = df[~df["GEOID"].isin(["06037599000", "06037599100"])]

    print("2️⃣  Classifying tracts into 4 categories...")
    
    def classify(row):
        is_high_need = row["pct_zero_veh"] > HIGH_NEED_THRESHOLD
        is_low_access = row["pct_desert_area"] > LOW_ACCESS_THRESHOLD
        
        if is_high_need and is_low_access:
            return "High Need / Low Access (Desert)"
        elif is_high_need and not is_low_access:
            return "High Need / High Access"
        elif not is_high_need and is_low_access:
            return "Low Need / Low Access"
        else:
            return "Low Need / High Access"
            
    df["classification"] = df.apply(classify, axis=1)

    print("3️⃣  Generating Summary Statistics...")
    summary = df.groupby("classification").agg({
        "GEOID": "count",
        "total_households": "sum",
        "zero_veh_households": "sum"
    }).rename(columns={"GEOID": "tract_count"})
    
    summary["pct_of_total_tracts"] = (summary["tract_count"] / summary["tract_count"].sum() * 100).round(1)
    
    csv_out = RESULTS_DIR / "summary_statistics.csv"
    summary.to_csv(csv_out)
    print(f"   Saved summary statistics to {csv_out}")
    print("\n   --- Summary Statistics ---")
    print(summary.to_string())
    print("   --------------------------\n")

    print("4️⃣  Generating Maps...")
    
    # Map 1: Choropleth of Zero Vehicle Households
    print("   Generating Zero-Vehicle Choropleth Map...")
    fig, ax = plt.subplots(figsize=(10, 10))
    df.plot(column="pct_zero_veh", cmap="OrRd", legend=True, 
            legend_kwds={'label': "% Zero-Vehicle Households", 'shrink': 0.5},
            ax=ax, edgecolor="none")
    ax.set_title("Density of Zero-Vehicle Households (Los Angeles County)", fontsize=16)
    ax.axis("off")
    plt.savefig(RESULTS_DIR / "zero_vehicle_households_map.png", dpi=300, bbox_inches="tight")
    plt.close()

    # Map 2: Hotspot Map (High Need / Low Access)
    print("   Generating Transit Deserts Hotspot Map...")
    fig, ax = plt.subplots(figsize=(10, 10))
    
    # Plot base map in light gray
    df.plot(ax=ax, color="lightgray", edgecolor="none")
    
    # Overlay the hotspots in red
    hotspots = df[df["classification"] == "High Need / Low Access (Desert)"]
    hotspots.plot(ax=ax, color="red", edgecolor="none", label="Transit Desert")
    
    ax.set_title("Transit Deserts in Los Angeles County: High Carless Pop. & Low Access", fontsize=16)
    ax.axis("off")
    plt.savefig(RESULTS_DIR / "transit_deserts_hotspot_map.png", dpi=300, bbox_inches="tight")
    plt.close()

    # Map 3: Combined Overlay (Choropleth + Hatched Deserts)
    print("   Generating Combined Overlay Map...")
    import matplotlib.patches as mpatches
    fig, ax = plt.subplots(figsize=(12, 12))
    
    df.plot(column="pct_zero_veh", cmap="OrRd", legend=True, 
            legend_kwds={'label': "% Zero-Vehicle Households", 'shrink': 0.5},
            ax=ax, edgecolor="none")
            
    if not hotspots.empty:
        # Use a solid black border for a clean overlay
        hotspots.plot(ax=ax, facecolor="none", edgecolor="black", linewidth=1.5)
        
    ax.set_title("Transit Deserts Overlay (Los Angeles County): Density + Lack of Access", fontsize=16)
    ax.axis("off")
    
    border_patch = mpatches.Patch(facecolor='none', edgecolor='black', linewidth=1.5, label='Identified Transit Desert')
    ax.legend(handles=[border_patch], loc='lower left', fontsize=12)
    
    plt.savefig(RESULTS_DIR / "combined_overlay_map.png", dpi=300, bbox_inches="tight")
    plt.close()

    print(f"✅  Done! Maps saved to {RESULTS_DIR}")

if __name__ == "__main__":
    run_visualization()
