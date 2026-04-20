import os
import zipfile
import requests
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR  = DATA_DIR / "raw"

for d in [RAW_DIR / "census", RAW_DIR / "gtfs", RAW_DIR / "shapefiles"]:
    d.mkdir(parents=True, exist_ok=True)
    
CENSUS_API_KEY = "8bd55aefea1b0e2f7e7143786525de329451ff87"

def download_acs_vehicles(year: int = 2022) -> pd.DataFrame:
    print("⬇  Downloading ACS B25044 data...")

    variables = ",".join([
        "NAME",
        "B25044_001E",
        "B25044_002E",
        "B25044_003E",
        "B25044_009E",
        "B25044_010E",
    ])

    url = (
        f"https://api.census.gov/data/{year}/acs/acs5"
        f"?get={variables}"
        f"&for=tract:*"
        f"&in=state:06%20county:037"
        f"&key={CENSUS_API_KEY}"
    )

    resp = requests.get(url, timeout=60)

    if resp.status_code != 200:
        raise RuntimeError(f"Census API returned HTTP {resp.status_code}:\n{resp.text[:500]}")

    raw = resp.text.strip()
    if not raw or raw.startswith("<"):
        raise RuntimeError(
            "Census API returned non-JSON response — key is likely missing or invalid.\n"
            f"Response preview: {raw[:300]}\n\n"
            "Get a free key at: https://api.census.gov/data/key_signup.html\n"
            "Then set CENSUS_API_KEY = 'your_key' at the top of this script."
        )

    data = resp.json()
    df = pd.DataFrame(data[1:], columns=data[0])

    df["GEOID"] = df["state"] + df["county"] + df["tract"]

    num_cols = [c for c in df.columns if c.endswith("E") and c != "NAME"]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")

    df["zero_veh_households"] = df["B25044_003E"].fillna(0) + df["B25044_010E"].fillna(0)
    df["total_households"]    = df["B25044_001E"]
    df["pct_zero_veh"]        = (df["zero_veh_households"] / df["total_households"] * 100).round(2)

    out = RAW_DIR / "census" / "acs_b25044_la.csv"
    df.to_csv(out, index=False)
    print(f"   Saved {len(df)} tracts → {out}")
    return df


def download_tiger_tracts(year: int = 2022) -> Path:
    print("⬇  Downloading TIGER/Line census tract shapefile (California state file, ~25 MB)...")

    url = (
        f"https://www2.census.gov/geo/tiger/TIGER{year}/TRACT/"
        f"tl_{year}_06_tract.zip"
    )

    zip_path = RAW_DIR / "shapefiles" / f"tl_{year}_06_tract.zip"
    out_dir  = RAW_DIR / "shapefiles" / f"tl_{year}_06_tract"

    if not out_dir.exists():
        print(f"   URL: {url}")
        resp = requests.get(url, timeout=180, stream=True)
        resp.raise_for_status()

        total = 0
        with open(zip_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=65536):
                f.write(chunk)
                total += len(chunk)
        print(f"   Downloaded {total / 1e6:.1f} MB")

        with zipfile.ZipFile(zip_path) as z:
            z.extractall(out_dir)
        print(f"   Extracted → {out_dir}")
    else:
        print(f"   Shapefile already exists, skipping.")

    return out_dir


def download_metro_gtfs() -> Path:
    print("⬇  Downloading LA Metro GTFS data...")

    url      = "https://gitlab.com/LACMTA/gtfs_bus/raw/master/gtfs_bus.zip"
    out_dir  = RAW_DIR / "gtfs"
    zip_path = out_dir / "metro_gtfs.zip"

    if not (out_dir / "stops.txt").exists():
        try:
            resp = requests.get(url, timeout=180, stream=True)
            resp.raise_for_status()
            with open(zip_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    f.write(chunk)
            with zipfile.ZipFile(zip_path) as z:
                z.extractall(out_dir)
            print(f"   Extracted GTFS → {out_dir}")
        except Exception as e:
            print(f"   ⚠  Auto-download failed: {e}")
            print(
                "\n   Manual steps:\n"
                "   1. Go to https://developer.metro.net/gtfs-schedule-data/\n"
                "   2. Download the Bus GTFS zip\n"
                f"  3. Unzip it into: {out_dir}\n"
                "   Needed files: stops.txt, stop_times.txt, trips.txt, routes.txt\n"
            )
    else:
        print("   GTFS already exists, skipping.")

    return out_dir


if __name__ == "__main__":
    print("=" * 55)
    print("  Transit Deserts LA — Step 1: Data Download")
    print("=" * 55)

    if CENSUS_API_KEY == "YOUR_KEY_HERE":
        print("\n❌  No Census API key set!")
        print("   1. Get a free key at: https://api.census.gov/data/key_signup.html")
        print("   2. Open this script and replace YOUR_KEY_HERE with your actual key:")
        print('      CENSUS_API_KEY = "abc123yourkeyhere"')
        exit(1)

    acs_df   = download_acs_vehicles()
    shp_dir  = download_tiger_tracts()
    gtfs_dir = download_metro_gtfs()

    print("\n✅  All data downloaded.")
    print(f"   Census tracts : {len(acs_df)} rows")
