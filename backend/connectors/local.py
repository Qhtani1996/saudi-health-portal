"""
Local data connector — loads GSTAT Excel files into the database.
File: backend/connectors/local.py
"""

import pandas as pd
import os
from datetime import datetime, UTC

DATA_DIR = "/Users/meshalalqhtani/Dropbox/Office/opne_source/data"
GSTAT_DIR = os.path.join(DATA_DIR, "GSTAT")


def load_population_estimates(verbose: bool = True) -> list[dict]:
    """
    Load population estimates from GSTAT Excel file.
    Covers 2023 and 2024 by age group, sex, and nationality.
    """
    file = os.path.join(GSTAT_DIR, "Population Estimates Statistics 2024 EN(1).xlsx")

    if not os.path.exists(file):
        print(f"File not found: {file}")
        return []

    if verbose:
        print(f"Reading: {os.path.basename(file)}")

    df = pd.read_excel(file, header=None)

    # Row 0 = merged headers (Saudi 2024, Non-Saudi 2024, Total 2024, ...)
    # Row 1 = sub-headers (Female, Male, Total)
    # Row 2+ = data (age groups)

    # Build column map manually based on what we saw in R
    records = []
    fetched = datetime.now(UTC).isoformat()

    # Column layout (0-indexed):
    # 0=Age, 1=Saudi F 2024, 2=Saudi M 2024, 3=Saudi T 2024,
    # 4=NonSaudi F 2024, 5=NonSaudi M 2024, 6=NonSaudi T 2024,
    # 7=Total F 2024, 8=Total M 2024, 9=Total T 2024,
    # 10=Saudi F 2023, 11=Saudi M 2023, 12=Saudi T 2023,
    # 13=NonSaudi F 2023, 14=NonSaudi M 2023, 15=NonSaudi T 2023,
    # 16=Total F 2023, 17=Total M 2023, 18=Total T 2023

    col_map = {
        1:  ("Saudi",     "Female", 2024),
        2:  ("Saudi",     "Male",   2024),
        3:  ("Saudi",     "Total",  2024),
        4:  ("Non-Saudi", "Female", 2024),
        5:  ("Non-Saudi", "Male",   2024),
        6:  ("Non-Saudi", "Total",  2024),
        7:  ("All",       "Female", 2024),
        8:  ("All",       "Male",   2024),
        9:  ("All",       "Total",  2024),
        10: ("Saudi",     "Female", 2023),
        11: ("Saudi",     "Male",   2023),
        12: ("Saudi",     "Total",  2023),
        13: ("Non-Saudi", "Female", 2023),
        14: ("Non-Saudi", "Male",   2023),
        15: ("Non-Saudi", "Total",  2023),
        16: ("All",       "Female", 2023),
        17: ("All",       "Male",   2023),
        18: ("All",       "Total",  2023),
    }

    # Data starts from row 2 (skip 2 header rows)
    for _, row in df.iloc[2:].iterrows():
        age_group = str(row.iloc[0]).strip()

        # Skip empty or non-age rows
        if not age_group or age_group in ["nan", "None", "Total"]:
            continue
        # Skip rows that don't look like age groups
        if not any(c.isdigit() for c in age_group) and age_group != "Total":
            continue

        for col_idx, (nationality, sex, year) in col_map.items():
            try:
                val = row.iloc[col_idx]
                if pd.isna(val):
                    continue
                val = float(str(val).replace(",", ""))
                if val <= 0:
                    continue

                records.append({
                    "indicator_code": f"GSTAT_POP_{nationality.replace('-','').upper()}",
                    "indicator_name": f"Population — {nationality}",
                    "year":           year,
                    "sex":            sex,
                    "value":          val,
                    "country":        "SAU",
                    "source":         "GSTAT",
                    "fetched_at":     fetched,
                })
            except (ValueError, IndexError):
                continue

    if verbose:
        print(f"  Records prepared: {len(records):,}")

    return records


def load_all_gstat(verbose: bool = True) -> list[dict]:
    """Load all available GSTAT files."""
    all_records = []

    if verbose:
        print("Loading GSTAT data...")
        print("-" * 45)

    # Population estimates
    pop = load_population_estimates(verbose=verbose)
    all_records.extend(pop)

    if verbose:
        print("-" * 45)
        print(f"Total GSTAT records: {len(all_records):,}")

    return all_records


if __name__ == "__main__":
    records = load_all_gstat(verbose=True)
    if records:
        import json
        print("\nSample record:")
        print(json.dumps(records[0], indent=2))
