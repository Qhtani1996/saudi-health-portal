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

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET"], allow_headers=["*"])

@app.on_event("startup")
def startup():
    init_db()
    # Auto-load WHO data if database is empty
    from database import get_summary
    summary = get_summary()
    if summary["total_records"] == 0:
        print("Database empty — loading WHO data...")
        records = fetch_all_indicators(verbose=True)
        save_records(records, source="WHO GHO")

@app.get("/")
def root():
    return {"name": "Saudi Health Data Portal API", "version": "0.1.0", "docs": "/docs"}

@app.get("/health")
def health_check():
    return {"status": "ok", **get_summary()}

@app.get("/indicators")
def list_indicators():
    records = query_records()
    seen = {}
    for r in records:
        code = r["indicator_code"]
        if code not in seen:
            seen[code] = {"code": code, "name": r["indicator_name"], "source": r["source"]}
    return {"count": len(seen), "indicators": list(seen.values())}

@app.get("/data")
def get_data(
    source:         Optional[str] = Query(None),
    indicator_code: Optional[str] = Query(None),
    year_from:      Optional[int] = Query(None),
    year_to:        Optional[int] = Query(None),
    sex:            Optional[str] = Query(None),
):
    records = query_records(source=source, indicator_code=indicator_code,
                            year_from=year_from, year_to=year_to, sex=sex)
    if not records:
        raise HTTPException(status_code=404, detail="No records found.")
    return {"count": len(records), "data": records}

@app.get("/refresh")
def refresh():
    records = fetch_all_indicators(verbose=False)
    save_records(records, source="WHO GHO")
    return {"status": "done", "records": len(records)}
