import warnings
warnings.filterwarnings("ignore")

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

    print("5️⃣  Generating Additional Visualizations...")

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

    # Graph 3: Histogram distribution of percent zero-vehicle households
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.hist(df["pct_zero_veh"].dropna(), bins=25, color="#f46d43", edgecolor="white")
    ax.axvline(HIGH_NEED_THRESHOLD, color="black", linestyle="--", linewidth=1.5, label=f"High Need Threshold ({HIGH_NEED_THRESHOLD}%)")
    ax.set_title("Distribution of % Zero-Vehicle Households", fontsize=14)
    ax.set_xlabel("% Zero-Vehicle Households")
    ax.set_ylabel("Number of Census Tracts")
    ax.legend()
    plt.tight_layout()
    plt.savefig(RESULTS_DIR / "hist_pct_zero_vehicle_households.png", dpi=300)
    plt.close()

    # Graph 4: Histogram distribution of transit desert area %
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

    # Graph 5: Scatter plot of need vs access
    fig, ax = plt.subplots(figsize=(10, 7))
    scatter = ax.scatter(
        df["pct_zero_veh"],
        df["pct_desert_area"],
        c=df["zero_veh_households"],
        cmap="viridis",
        alpha=0.75,
        s=25,
    )
    ax.axvline(HIGH_NEED_THRESHOLD, color="red", linestyle="--", linewidth=1.2)
    ax.axhline(LOW_ACCESS_THRESHOLD, color="red", linestyle="--", linewidth=1.2)
    ax.set_title("Need vs Access by Census Tract", fontsize=14)
    ax.set_xlabel("% Zero-Vehicle Households")
    ax.set_ylabel("% Transit Desert Area")
    cbar = plt.colorbar(scatter, ax=ax)
    cbar.set_label("Zero-Vehicle Households")
    plt.tight_layout()
    plt.savefig(RESULTS_DIR / "scatter_need_vs_access.png", dpi=300)
    plt.close()

    # Graph 6: Line chart for percentile profile across two indicators
    percentiles = [10, 25, 50, 75, 90]
    zero_veh_profile = [np.percentile(df["pct_zero_veh"].dropna(), p) for p in percentiles]
    desert_profile = [np.percentile(df["pct_desert_area"].dropna(), p) for p in percentiles]
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(percentiles, zero_veh_profile, marker="o", linewidth=2, label="% Zero-Vehicle Households")
    ax.plot(percentiles, desert_profile, marker="o", linewidth=2, label="% Transit Desert Area")
    ax.set_title("Indicator Profile Across Tract Percentiles", fontsize=14)
    ax.set_xlabel("Percentile")
    ax.set_ylabel("Percent")
    ax.set_xticks(percentiles)
    ax.legend()
    plt.tight_layout()
    plt.savefig(RESULTS_DIR / "line_percentile_profile.png", dpi=300)
    plt.close()

    # Graph 7: Top 15 highest-need tracts
    top_need = df.nlargest(15, "pct_zero_veh")[["GEOID", "pct_zero_veh"]].sort_values("pct_zero_veh")
    fig, ax = plt.subplots(figsize=(10, 8))
    ax.barh(top_need["GEOID"], top_need["pct_zero_veh"], color="#d73027")
    ax.set_title("Top 15 Tracts by % Zero-Vehicle Households", fontsize=14)
    ax.set_xlabel("% Zero-Vehicle Households")
    ax.set_ylabel("Census Tract GEOID")
    plt.tight_layout()
    plt.savefig(RESULTS_DIR / "bar_top15_high_need_tracts.png", dpi=300)
    plt.close()

    # Graph 8: Top 15 lowest-access tracts
    top_low_access = df.nlargest(15, "pct_desert_area")[["GEOID", "pct_desert_area"]].sort_values("pct_desert_area")
    fig, ax = plt.subplots(figsize=(10, 8))
    ax.barh(top_low_access["GEOID"], top_low_access["pct_desert_area"], color="#4575b4")
    ax.set_title("Top 15 Tracts by % Transit Desert Area", fontsize=14)
    ax.set_xlabel("% Transit Desert Area")
    ax.set_ylabel("Census Tract GEOID")
    plt.tight_layout()
    plt.savefig(RESULTS_DIR / "bar_top15_low_access_tracts.png", dpi=300)
    plt.close()

    print(f"✅  Done! Maps and graphs saved to {RESULTS_DIR}")

if __name__ == "__main__":
    run_visualization()
