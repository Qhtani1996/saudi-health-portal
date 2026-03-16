"""
IHME data connector — loads local CSV into the database.
File: backend/connectors/ihme.py
"""

import pandas as pd
import os
from datetime import datetime, UTC

# Path to your IHME CSV — relative to project root
IHME_CSV = os.path.join(
    os.path.dirname(__file__),       # backend/connectors/
    "..", "..", "..",                 # up to project root
    "data", "IHME", "ihme_2021.2023_allcause.csv"
)

# We'll also accept an absolute path override
DATA_DIR = "/Users/meshalalqhtani/Dropbox/Office/opne_source/data"
IHME_FILE = os.path.join(DATA_DIR, "IHME", "ihme_2021.2023_allcause.csv")


def load_ihme_data(verbose: bool = True) -> list[dict]:
    """
    Read IHME CSV and convert to the standard record format
    used by the rest of the portal.

    Returns a list of dicts ready to pass to database.save_records()
    """

    if not os.path.exists(IHME_FILE):
        print(f"IHME file not found at: {IHME_FILE}")
        return []

    if verbose:
        print(f"Reading IHME data from: {IHME_FILE}")

    df = pd.read_csv(IHME_FILE)

    if verbose:
        print(f"Raw rows: {len(df):,}")

    # Keep only Number metric (not Rate) to avoid double-counting
    # You can change this to "Rate" or keep both later
    df = df[df["metric_name"] == "Number"].copy()

    # Build standard records
    fetched = datetime.now(UTC).isoformat()
    records = []

    for _, row in df.iterrows():
        records.append({
            "indicator_code": f"IHME_{row['cause_id']}_{row['measure_id']}",
            "indicator_name": f"{row['cause_name']} — {row['measure_name']}",
            "year":           int(row["year"]),
            "sex":            row["sex_name"],
            "value":          round(float(row["val"]), 4),
            "country":        "SAU",
            "source":         "IHME GBD",
            "fetched_at":     fetched,
            # Extra fields we'll store as part of indicator_name context
        })

    if verbose:
        print(f"Records prepared: {len(records):,}")
        print(f"Years covered: {sorted(df['year'].unique())}")
        print(f"Unique causes: {df['cause_name'].nunique()}")
        print(f"Age groups: {df['age_name'].nunique()}")

    return records


if __name__ == "__main__":
    records = load_ihme_data(verbose=True)

    if records:
        print("\nSample record:")
        import json
        print(json.dumps(records[0], indent=2))
        print(f"\nTotal records ready to load: {len(records):,}")
