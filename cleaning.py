

import pandas as pd
import geopandas as gpd
from pathlib import Path

BASE_DIR = Path(__file__).parent
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

TARGET_CRS = "EPSG:2229"

PEAK_WINDOWS = [
    ("06:00:00", "09:00:00"),
    ("16:00:00", "19:00:00"),
]
MAX_HEADWAY_MINUTES = 15


def build_tract_geodataframe():
    acs_csv = RAW_DIR / "census" / "acs_b25044_la.csv"
    acs = pd.read_csv(acs_csv, dtype={"GEOID": str})

    shp_dirs = sorted((RAW_DIR / "shapefiles").glob("tl_*_06_tract"))
    shp_file = list(shp_dirs[0].glob("*.shp"))[0]

    tracts = gpd.read_file(shp_file)
    tracts = tracts[tracts["COUNTYFP"] == "037"].copy()

    tracts["GEOID"] = tracts["GEOID"].astype(str).str.zfill(11)

    merged = tracts.merge(acs, on="GEOID", how="left")
    merged = merged.to_crs(TARGET_CRS)

    merged["area_sqmi"] = (merged.geometry.area / 5280**2).round(4)

    out = PROCESSED_DIR / "tracts_with_vehicles.gpkg"
    merged.to_file(out, driver="GPKG")

    return merged

def time_to_seconds(t):
    h, m, s = map(int, t.split(":"))
    return h * 3600 + m * 60 + s


def build_highfreq_stops():
    all_stops_hf = []

    for feed_name in ["gtfs_bus", "gtfs_rail"]:
        gtfs_dir = RAW_DIR / feed_name
        if not (gtfs_dir / "stops.txt").exists():
            continue

        stops = pd.read_csv(gtfs_dir / "stops.txt", dtype=str)
        stop_times = pd.read_csv(gtfs_dir / "stop_times.txt", dtype=str)
        trips = pd.read_csv(gtfs_dir / "trips.txt", dtype=str)

        st = stop_times.merge(trips[["trip_id", "route_id"]], on="trip_id")

        st["arr_sec"] = st["arrival_time"].apply(time_to_seconds)

        peaks_sec = [
            (time_to_seconds(s), time_to_seconds(e))
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

        stops_hf = stops[stops["stop_id"].isin(highfreq_ids)].copy()
        stops_hf["stop_lat"] = pd.to_numeric(stops_hf["stop_lat"])
        stops_hf["stop_lon"] = pd.to_numeric(stops_hf["stop_lon"])

        stops_hf = stops_hf.merge(min_hw, on="stop_id", how="left")
        all_stops_hf.append(stops_hf)

    combined_stops_hf = pd.concat(all_stops_hf, ignore_index=True)

    gdf = gpd.GeoDataFrame(
        combined_stops_hf,
        geometry=gpd.points_from_xy(combined_stops_hf["stop_lon"], combined_stops_hf["stop_lat"]),
        crs="EPSG:4326",
    )

    gdf = gdf.to_crs(TARGET_CRS)

    out = PROCESSED_DIR / "highfreq_stops.gpkg"
    gdf.to_file(out, driver="GPKG")

    return gdf

if __name__ == "__main__":
    build_tract_geodataframe()
    build_highfreq_stops()
