"""
WHO Global Health Observatory connector.
Fetches live health data for Saudi Arabia via the WHO GHO API.
"""

import requests
from datetime import datetime

WHO_BASE = "https://ghoapi.azureedge.net/api"
COUNTRY  = "SAU"  # ISO code for Saudi Arabia

# Curated list of indicators relevant to Saudi public health research.
# Key: WHO indicator code | Value: human-readable name
INDICATORS = {
    "WHOSIS_000001":  "Life expectancy at birth (years)",
    "WHOSIS_000015":  "Healthy life expectancy (HALE)",
    "MDG_0000000026": "Adult mortality rate (per 1000)",
    "NCD_BMI_30A":    "Obesity prevalence - adults (%)",
    "NCD_BMI_PLUS2C": "Overweight children under 5 (%)",
    "NUTRITION_ANAEMIApreg": "Anaemia in pregnant women (%)",
    "MDG_0000000007": "Under-5 mortality rate",
    "MDG_0000000003": "Infant mortality rate",
    "MDG_0000000001": "Maternal mortality ratio",
    "AIR_10":         "Air pollution - DALYs children under 5",
    "AIR_11":         "Household air pollution deaths",
    "Adult_curr_tob_use": "Tobacco use prevalence - adults (%)",
    "SDGPM25":        "PM2.5 air pollution (µg/m³)",
    "WSH_SANITATION_SAFELY_MANAGED": "Safely managed sanitation (%)",
    "WSH_WATER_SAFELY_MANAGED":      "Safely managed drinking water (%)",
}

SEX_LABELS = {
    "SEX_BTSX": "Both sexes",
    "SEX_MLE":  "Male",
    "SEX_FMLE": "Female",
}


def fetch_indicator(code: str, years: int = 10) -> list[dict]:
    """
    Fetch the most recent data points for one indicator for Saudi Arabia.

    Returns a list of records, each with:
      indicator_code, indicator_name, year, sex, value, source, fetched_at
    """
    url = f"{WHO_BASE}/{code}"
    params = {
        "$filter": f"SpatialDim eq '{COUNTRY}'",
        "$orderby": "TimeDim desc",
        "$top": years * 3,  # x3 to account for male/female/both splits
    }

    try:
        response = requests.get(url, params=params, timeout=15)

        if response.status_code == 404:
            return []  # Indicator not available for this country
        if response.status_code != 200:
            print(f"  Warning: {code} returned HTTP {response.status_code}")
            return []

        raw = response.json().get("value", [])
        records = []

        for row in raw:
            val = row.get("NumericValue")
            if val is None:
                continue  # Skip rows with no numeric value

            records.append({
                "indicator_code": code,
                "indicator_name": INDICATORS.get(code, code),
                "year":           row.get("TimeDim"),
                "sex":            SEX_LABELS.get(row.get("Dim1"), row.get("Dim1", "Both sexes")),
                "value":          round(float(val), 4),
                "country":        COUNTRY,
                "source":         "WHO GHO",
                "fetched_at":     datetime.utcnow().isoformat(),
            })

        return records

    except requests.exceptions.Timeout:
        print(f"  Timeout fetching {code}")
        return []
    except Exception as e:
        print(f"  Error fetching {code}: {e}")
        return []


def fetch_all_indicators(verbose: bool = True) -> list[dict]:
    """
    Fetch all curated indicators for Saudi Arabia.
    Returns a flat list of all records across all indicators.
    """
    all_records = []

    if verbose:
        print(f"Fetching {len(INDICATORS)} WHO indicators for Saudi Arabia...")
        print("-" * 55)

    for code, name in INDICATORS.items():
        records = fetch_indicator(code)
        all_records.extend(records)
        if verbose:
            status = f"{len(records)} records" if records else "no data"
            print(f"  {'OK' if records else '--'}  {name:<45} {status}")

    if verbose:
        print("-" * 55)
        print(f"Total records fetched: {len(all_records)}")

    return all_records


if __name__ == "__main__":
    # Quick test — run this file directly to verify the connector works
    data = fetch_all_indicators(verbose=True)

    if data:
        print("\nSample record:")
        import json
        print(json.dumps(data[0], indent=2))
