import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import geopandas as gpd
from pathlib import Path

BASE_DIR     = Path(__file__).parent
RAW_DIR      = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

TARGET_CRS = "EPSG:2229"

PEAK_WINDOWS = [
    ("06:00:00", "09:00:00"),
    ("16:00:00", "19:00:00"),
]
MAX_HEADWAY_MINUTES = 15


def build_tract_geodataframe() -> gpd.GeoDataFrame:
    print("🔧  Building census tract GeoDataFrame...")

    acs_csv = RAW_DIR / "census" / "acs_b25044_la.csv"
    acs = pd.read_csv(acs_csv, dtype={"GEOID": str})

    shp_dirs = sorted((RAW_DIR / "shapefiles").glob("tl_*_06_tract"))
    if not shp_dirs:
        raise FileNotFoundError("No TIGER shapefile found. Run 01_download_data.py first.")
    shp_file = list(shp_dirs[0].glob("*.shp"))[0]

    tracts = gpd.read_file(shp_file)
    tracts = tracts[tracts["COUNTYFP"] == "037"].copy()
    print(f"   Filtered to LA County: {len(tracts)} tracts")
    tracts["GEOID"] = tracts["GEOID"].astype(str).str.zfill(11)

    merged = tracts.merge(acs, on="GEOID", how="left")
    print(f"   Joined {len(merged)} tracts (unmatched rows get NaN values)")

    merged = merged.to_crs(TARGET_CRS)

    merged["area_sqmi"] = (merged.geometry.area / 5280**2).round(4)

    out = PROCESSED_DIR / "tracts_with_vehicles.gpkg"
    merged.to_file(out, driver="GPKG")
    print(f"   Saved → {out}  ({len(merged)} features)")
    return merged

def _time_to_seconds(t: str) -> int:
    h, m, s = map(int, t.split(":"))
    return h * 3600 + m * 60 + s


def build_highfreq_stops() -> gpd.GeoDataFrame:
    print("🔧  Processing GTFS to find high-frequency stops...")

    gtfs_dir = RAW_DIR / "gtfs"

    stops      = pd.read_csv(gtfs_dir / "stops.txt",      dtype=str)
    stop_times = pd.read_csv(gtfs_dir / "stop_times.txt", dtype=str)
    trips      = pd.read_csv(gtfs_dir / "trips.txt",      dtype=str)

    st = stop_times.merge(trips[["trip_id", "route_id", "service_id"]], on="trip_id")

    st["arr_sec"] = st["arrival_time"].apply(_time_to_seconds)

    peaks_sec = [
        (_time_to_seconds(s), _time_to_seconds(e))
        for s, e in PEAK_WINDOWS
    ]

    peak_mask = pd.Series(False, index=st.index)
    for start, end in peaks_sec:
        peak_mask |= st["arr_sec"].between(start, end)
    st_peak = st[peak_mask].copy()

    st_peak = st_peak.sort_values(["stop_id", "route_id", "arr_sec"])
    st_peak["headway_sec"] = (
        st_peak.groupby(["stop_id", "route_id"])["arr_sec"].diff()
    )

    min_hw = (
        st_peak.groupby("stop_id")["headway_sec"]
        .min()
        .reset_index()
        .rename(columns={"headway_sec": "min_headway_sec"})
    )
    min_hw["min_headway_min"] = (min_hw["min_headway_sec"] / 60).round(1)

    highfreq_ids = min_hw[min_hw["min_headway_min"] <= MAX_HEADWAY_MINUTES]["stop_id"]
    print(f"   {len(highfreq_ids):,} high-frequency stops out of {len(stops):,} total")

    stops_hf = stops[stops["stop_id"].isin(highfreq_ids)].copy()
    stops_hf["stop_lat"] = pd.to_numeric(stops_hf["stop_lat"])
    stops_hf["stop_lon"] = pd.to_numeric(stops_hf["stop_lon"])

    gdf = gpd.GeoDataFrame(
        stops_hf,
        geometry=gpd.points_from_xy(stops_hf["stop_lon"], stops_hf["stop_lat"]),
        crs="EPSG:4326",
    )

    gdf = gdf.merge(min_hw, on="stop_id", how="left")

    gdf = gdf.to_crs(TARGET_CRS)

    out = PROCESSED_DIR / "highfreq_stops.gpkg"
    gdf.to_file(out, driver="GPKG")
    print(f"   Saved → {out}  ({len(gdf)} features)")
    return gdf


def sanity_check(tracts: gpd.GeoDataFrame, stops: gpd.GeoDataFrame) -> None:
    print("\n📊  Sanity check:")
    print(f"   CRS tracts : {tracts.crs}")
    print(f"   CRS stops  : {stops.crs}")
    assert tracts.crs == stops.crs, "CRS mismatch! Fix before proceeding."

    pct_valid = tracts["pct_zero_veh"].notna().mean() * 100
    print(f"   Tracts with valid vehicle data: {pct_valid:.1f}%")

    top5 = tracts.nlargest(5, "pct_zero_veh")[["GEOID", "NAME_x", "pct_zero_veh"]]
    print("\n   Top 5 tracts by % zero-vehicle households:")
    print(top5.to_string(index=False))


if __name__ == "__main__":
    print("=" * 55)
    print("  Transit Deserts LA — Step 2: ETL & Clean")
    print("=" * 55)

    tracts = build_tract_geodataframe()
    stops  = build_highfreq_stops()
    sanity_check(tracts, stops)

    print("\n✅  Processed data saved to data/processed/")
