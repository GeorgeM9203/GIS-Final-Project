
import zipfile
import requests
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR  = DATA_DIR / "raw"

for d in [RAW_DIR / "census", RAW_DIR / "shapefiles"]:
    d.mkdir(parents=True, exist_ok=True)
    
def download_acs_vehicles(year=2022):
    variables = ",".join([
        "NAME",
        "B25044_001E",
        "B25044_003E",
        "B25044_010E",
    ])

    url = (
        f"https://api.census.gov/data/{year}/acs/acs5"
        f"?get={variables}"
        f"&for=tract:*"
        f"&in=state:06%20county:037"
    )

    resp = requests.get(url, timeout=60)

    if resp.status_code != 200:
        print(f"Census API returned HTTP {resp.status_code}:\n{resp.text[:500]}")
        exit(1)

    raw = resp.text.strip()
    if not raw or raw.startswith("<"):
        print("Invalid Census API response. Please try again later.")
        exit(1)

    data = resp.json()
    df = pd.DataFrame(data[1:], columns=data[0])

    df["GEOID"] = df["state"] + df["county"] + df["tract"]

    num_cols = [c for c in df.columns if c.endswith("E") and c != "NAME"]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")

    df["zero_veh_households"] = df["B25044_003E"].fillna(0) + df["B25044_010E"].fillna(0)
    df["total_households"] = df["B25044_001E"]
    df["pct_zero_veh"] = (df["zero_veh_households"] / df["total_households"] * 100).round(2)

    out = RAW_DIR / "census" / "acs_b25044_la.csv"
    df.to_csv(out, index=False)

    return df


def download_tiger_tracts(year=2022):
    url = (
        f"https://www2.census.gov/geo/tiger/TIGER{year}/TRACT/"
        f"tl_{year}_06_tract.zip"
    )

    zip_path = RAW_DIR / "shapefiles" / f"tl_{year}_06_tract.zip"
    out_dir  = RAW_DIR / "shapefiles" / f"tl_{year}_06_tract"

    if not out_dir.exists():

        resp = requests.get(url, timeout=180, stream=True)
        resp.raise_for_status()

        with open(zip_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=65536):
                f.write(chunk)
        with zipfile.ZipFile(zip_path) as z:
            z.extractall(out_dir)

    return out_dir


def download_metro_gtfs():
    feeds = {
        "bus": "https://gitlab.com/LACMTA/gtfs_bus/raw/master/gtfs_bus.zip",
        "rail": "https://gitlab.com/LACMTA/gtfs_rail/raw/master/gtfs_rail.zip",
    }
    
    out_dirs = {}

    for name, url in feeds.items():
        out_dir = RAW_DIR / f"gtfs_{name}"
        out_dir.mkdir(exist_ok=True)
        out_dirs[name] = out_dir
        zip_path = out_dir.parent / f"metro_gtfs_{name}.zip"

        if not (out_dir / "stops.txt").exists():
            try:
                resp = requests.get(url, timeout=180, stream=True)
                resp.raise_for_status()
                with open(zip_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=65536):
                        f.write(chunk)
                with zipfile.ZipFile(zip_path) as z:
                    z.extractall(out_dir)

            except Exception as e:
                print(f"   Auto-download failed for {name}: {e}")

    return out_dirs


if __name__ == "__main__":
    acs_df = download_acs_vehicles()
    shp_dir = download_tiger_tracts()
    gtfs_dir = download_metro_gtfs()

    print("\nAll data downloaded.")
    print(f"Census tracts: {len(acs_df)} rows")
