"""
Saudi Health Portal — FastAPI backend
Run with: uvicorn backend.main:app --reload
"""

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
import sys, os

sys.path.insert(0, os.path.dirname(__file__))
from database import init_db, query_records, get_summary, save_records
from connectors.who import fetch_all_indicators

app = FastAPI(
    title="Saudi Health Data Portal",
    description="Open access to Saudi Arabia public health data from WHO, IHME, GASTAT and MOH.",
    version="0.1.0",
)

# Allow the frontend (any origin for now) to call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# Initialise DB tables on startup
@app.on_event("startup")
def startup():
    init_db()


# ------------------------------------------------------------------
# ROUTES
# ------------------------------------------------------------------

@app.get("/")
def root():
    return {
        "name": "Saudi Health Data Portal API",
        "version": "0.1.0",
        "docs": "/docs",
        "endpoints": ["/health", "/indicators", "/data", "/refresh"],
    }


@app.get("/health")
def health_check():
    """Returns database summary — useful to verify the API is live."""
    summary = get_summary()
    return {"status": "ok", **summary}


@app.get("/indicators")
def list_indicators():
    """List all available indicator codes and names in the database."""
    records = query_records()
    seen = {}
    for r in records:
        code = r["indicator_code"]
        if code not in seen:
            seen[code] = {
                "code":   code,
                "name":   r["indicator_name"],
                "source": r["source"],
            }
    return {"count": len(seen), "indicators": list(seen.values())}


@app.get("/data")
def get_data(
    source:         Optional[str] = Query(None, description="Filter by source, e.g. 'WHO GHO'"),
    indicator_code: Optional[str] = Query(None, description="Filter by indicator code, e.g. 'NCD_BMI_30A'"),
    year_from:      Optional[int] = Query(None, description="Start year, e.g. 2010"),
    year_to:        Optional[int] = Query(None, description="End year, e.g. 2022"),
    sex:            Optional[str] = Query(None, description="Filter by sex: 'Both sexes', 'Male', 'Female'"),
):
    """
    Main data endpoint. Returns filtered health records.

    Examples:
      /data?indicator_code=NCD_BMI_30A
      /data?indicator_code=WHOSIS_000001&sex=Both+sexes
      /data?source=WHO+GHO&year_from=2010&year_to=2022
    """
    records = query_records(
        source=source,
        indicator_code=indicator_code,
        year_from=year_from,
        year_to=year_to,
        sex=sex,
    )

    if not records:
        raise HTTPException(status_code=404, detail="No records found for the given filters.")

    return {
        "count":   len(records),
        "filters": {
            "source": source,
            "indicator_code": indicator_code,
            "year_from": year_from,
            "year_to": year_to,
            "sex": sex,
        },
        "data": records,
    }


@app.get("/refresh")
def refresh_data():
    """
    Re-fetch all data from live sources and update the database.
    Call this to refresh WHO data (runs in ~15 seconds).
    """
    results = {}

    # Refresh WHO data
    try:
        records = fetch_all_indicators(verbose=False)
        save_records(records, source="WHO GHO")
        results["WHO GHO"] = f"{len(records)} records updated"
    except Exception as e:
        results["WHO GHO"] = f"Error: {e}"

    return {"status": "done", "results": results}
