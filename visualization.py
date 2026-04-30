

import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import numpy as np

BASE_DIR = Path(__file__).parent
PROCESSED_DIR = BASE_DIR / "data" / "processed"
RESULTS_DIR = BASE_DIR / "data" / "results"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

# Thresholds
HIGH_NEED_THRESHOLD = 10.0 # > 10% zero vehicle households
LOW_ACCESS_THRESHOLD = 50.0 # > 50% of the tract is a transit desert

def run_visualization():
    data_path = PROCESSED_DIR / "transit_deserts_final.gpkg"
    df = gpd.read_file(data_path)

    # Filter out Catalina and San Clemente Islands
    df = df[~df["GEOID"].isin(["06037599000", "06037599100"])]
    
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
    
    hotspots = df[df["classification"] == "High Need / Low Access (Desert)"]

    # Map 1: Combined Overlay
    import matplotlib.patches as mpatches
    fig, ax = plt.subplots(figsize=(12, 12))
    
    df.plot(column="pct_zero_veh", cmap="OrRd", legend=True, 
            legend_kwds={'label': "% Zero-Vehicle Households", 'shrink': 0.5},
            ax=ax, edgecolor="#666666", linewidth=0.2)
            
    if not hotspots.empty:
        # Use a solid black border for a clean overlay
        hotspots.plot(ax=ax, facecolor="none", edgecolor="black", linewidth=1.5)
        
    ax.set_title("Transit Deserts Overlay (Los Angeles County): Density + Lack of Access", fontsize=16)
    ax.axis("off")
    
    border_patch = mpatches.Patch(facecolor='none', edgecolor='black', linewidth=1.5, label='Identified Transit Desert')
    ax.legend(handles=[border_patch], loc='lower left', fontsize=12)
    
    plt.savefig(RESULTS_DIR / "combined_overlay_map.png", dpi=300, bbox_inches="tight")
    plt.close()

    # Graph 1: Bar chart of tract count by classification
    fig, ax = plt.subplots(figsize=(10, 6))
    class_counts = df["classification"].value_counts().reindex(summary.index)
    class_counts.plot(kind="bar", ax=ax, color=["#d73027", "#fc8d59", "#91bfdb", "#4575b4"])
    ax.set_title("Census Tract Count by Classification", fontsize=14)
    ax.set_xlabel("Classification")
    ax.set_ylabel("Number of Census Tracts")
    ax.tick_params(axis="x", rotation=25)
    plt.tight_layout()
    plt.savefig(RESULTS_DIR / "bar_tract_count_by_classification.png", dpi=300)
    plt.close()

    # Graph 2: Bar chart of total zero-vehicle households by classification
    fig, ax = plt.subplots(figsize=(10, 6))
    summary["zero_veh_households"].plot(kind="bar", ax=ax, color="#ef8a62")
    ax.set_title("Zero-Vehicle Households by Classification", fontsize=14)
    ax.set_xlabel("Classification")
    ax.set_ylabel("Zero-Vehicle Households")
    ax.tick_params(axis="x", rotation=25)
    plt.tight_layout()
    plt.savefig(RESULTS_DIR / "bar_zero_vehicle_households_by_classification.png", dpi=300)
    plt.close()

    # Graph 3: Histogram distribution of transit desert area %
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.hist(df["pct_desert_area"].dropna(), bins=25, color="#74add1", edgecolor="white")
    ax.axvline(LOW_ACCESS_THRESHOLD, color="black", linestyle="--", linewidth=1.5, label=f"Low Access Threshold ({LOW_ACCESS_THRESHOLD}%)")
    ax.set_title("Distribution of % Transit Desert Area", fontsize=14)
    ax.set_xlabel("% of Tract in Transit Desert")
    ax.set_ylabel("Number of Census Tracts")
    ax.legend()
    plt.tight_layout()
    plt.savefig(RESULTS_DIR / "hist_pct_desert_area.png", dpi=300)
    plt.close()

if __name__ == "__main__":
    run_visualization()
