"""
Database setup and operations for Saudi Health Portal.
Uses SQLite for local development. Can be swapped for PostgreSQL in production.
"""

import sqlite3
import os
from datetime import datetime, UTC

# Database lives inside the backend/data folder
DB_PATH = os.path.join(os.path.dirname(__file__), "data", "health.db")


def get_connection():
    """Return a connection to the SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # Rows behave like dicts
    return conn


def init_db():
    """Create tables if they don't exist yet."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.executescript("""
        -- Main table: one row per indicator/year/sex/source combination
        CREATE TABLE IF NOT EXISTS health_data (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            indicator_code TEXT    NOT NULL,
            indicator_name TEXT    NOT NULL,
            year           INTEGER NOT NULL,
            sex            TEXT    DEFAULT 'Both sexes',
            value          REAL    NOT NULL,
            country        TEXT    DEFAULT 'SAU',
            source         TEXT    NOT NULL,
            fetched_at     TEXT    NOT NULL
        );

        -- Index for fast filtering by indicator and year
        CREATE INDEX IF NOT EXISTS idx_indicator
            ON health_data (indicator_code, year);

        -- Index for fast filtering by source
        CREATE INDEX IF NOT EXISTS idx_source
            ON health_data (source);

        -- Metadata table: tracks when each source was last refreshed
        CREATE TABLE IF NOT EXISTS sync_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            source      TEXT NOT NULL,
            records     INTEGER NOT NULL,
            synced_at   TEXT NOT NULL,
            status      TEXT DEFAULT 'success'
        );
    """)

    conn.commit()
    conn.close()
    print(f"Database ready at: {DB_PATH}")


def save_records(records: list[dict], source: str):
    """
    Save a list of records into health_data table.
    Clears existing records from the same source first (full refresh).
    """
    if not records:
        print(f"No records to save for {source}.")
        return

    conn = get_connection()
    cursor = conn.cursor()

    # Remove old records from this source before inserting fresh ones
    cursor.execute("DELETE FROM health_data WHERE source = ?", (source,))
    deleted = cursor.rowcount

    # Insert all new records
    cursor.executemany("""
        INSERT INTO health_data
            (indicator_code, indicator_name, year, sex, value, country, source, fetched_at)
        VALUES
            (:indicator_code, :indicator_name, :year, :sex, :value, :country, :source, :fetched_at)
    """, records)

    inserted = cursor.rowcount

    # Log the sync
    cursor.execute("""
        INSERT INTO sync_log (source, records, synced_at, status)
        VALUES (?, ?, ?, 'success')
    """, (source, inserted, datetime.now(UTC).isoformat()))

    conn.commit()
    conn.close()

    print(f"  Removed {deleted} old records from '{source}'")
    print(f"  Inserted {inserted} new records from '{source}'")


def query_records(
    source: str = None,
    indicator_code: str = None,
    year_from: int = None,
    year_to: int = None,
    sex: str = None,
) -> list[dict]:
    """
    Query health_data with optional filters.
    Returns a list of dicts — ready to send as JSON from the API.
    """
    conn = get_connection()
    cursor = conn.cursor()

    sql = "SELECT * FROM health_data WHERE 1=1"
    params = []

    if source:
        sql += " AND source = ?"
        params.append(source)
    if indicator_code:
        sql += " AND indicator_code = ?"
        params.append(indicator_code)
    if year_from:
        sql += " AND year >= ?"
        params.append(year_from)
    if year_to:
        sql += " AND year <= ?"
        params.append(year_to)
    if sex:
        sql += " AND sex = ?"
        params.append(sex)

    sql += " ORDER BY indicator_code, year DESC, sex"

    cursor.execute(sql, params)
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


def get_summary() -> dict:
    """Return a summary of what's in the database — used by the API health check."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) as total FROM health_data")
    total = cursor.fetchone()["total"]

    cursor.execute("""
        SELECT source, COUNT(*) as count
        FROM health_data GROUP BY source
    """)
    by_source = {row["source"]: row["count"] for row in cursor.fetchall()}

    cursor.execute("""
        SELECT source, synced_at FROM sync_log
        ORDER BY synced_at DESC LIMIT 10
    """)
    recent_syncs = [dict(row) for row in cursor.fetchall()]

    cursor.execute("""
        SELECT COUNT(DISTINCT indicator_code) as n FROM health_data
    """)
    n_indicators = cursor.fetchone()["n"]

    conn.close()

    return {
        "total_records":  total,
        "indicators":     n_indicators,
        "by_source":      by_source,
        "recent_syncs":   recent_syncs,
    }


if __name__ == "__main__":
    # Test: initialise DB, load WHO data, print summary
    print("Step 1: Initialising database...")
    init_db()

    print("\nStep 2: Fetching WHO data...")
    import sys
    sys.path.insert(0, os.path.dirname(__file__))
    from connectors.who import fetch_all_indicators

    records = fetch_all_indicators(verbose=True)

    print("\nStep 3: Saving to database...")
    save_records(records, source="WHO GHO")

    print("\nStep 4: Database summary:")
    summary = get_summary()
    for key, val in summary.items():
        print(f"  {key}: {val}")

    print("\nStep 5: Sample query — obesity data, all years:")
    results = query_records(indicator_code="NCD_BMI_30A")
    for r in results[:5]:
        print(f"  {r['year']} | {r['sex']:<15} | {r['value']}")
