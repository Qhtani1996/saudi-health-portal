import sqlite3
import os
from datetime import datetime, UTC

# Works on both local Mac and Render server
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH  = os.path.join(DATA_DIR, "health.db")


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    os.makedirs(DATA_DIR, exist_ok=True)  # Create data/ folder if it doesn't exist
    conn = get_connection()
    cursor = conn.cursor()
    cursor.executescript("""
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
        CREATE INDEX IF NOT EXISTS idx_indicator ON health_data (indicator_code, year);
        CREATE INDEX IF NOT EXISTS idx_source ON health_data (source);
        CREATE TABLE IF NOT EXISTS sync_log (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            source    TEXT NOT NULL,
            records   INTEGER NOT NULL,
            synced_at TEXT NOT NULL,
            status    TEXT DEFAULT 'success'
        );
    """)
    conn.commit()
    conn.close()
    print(f"Database ready at: {DB_PATH}")


def save_records(records: list[dict], source: str):
    if not records:
        return
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM health_data WHERE source = ?", (source,))
    deleted = cursor.rowcount
    cursor.executemany("""
        INSERT INTO health_data
            (indicator_code, indicator_name, year, sex, value, country, source, fetched_at)
        VALUES
            (:indicator_code, :indicator_name, :year, :sex, :value, :country, :source, :fetched_at)
    """, records)
    inserted = cursor.rowcount
    cursor.execute("""
        INSERT INTO sync_log (source, records, synced_at, status)
        VALUES (?, ?, ?, 'success')
    """, (source, inserted, datetime.now(UTC).isoformat()))
    conn.commit()
    conn.close()
    print(f"  Removed {deleted} old records from '{source}'")
    print(f"  Inserted {inserted} new records from '{source}'")


def query_records(source=None, indicator_code=None, year_from=None, year_to=None, sex=None):
    conn = get_connection()
    cursor = conn.cursor()
    sql = "SELECT * FROM health_data WHERE 1=1"
    params = []
    if source:         sql += " AND source = ?";           params.append(source)
    if indicator_code: sql += " AND indicator_code = ?";   params.append(indicator_code)
    if year_from:      sql += " AND year >= ?";            params.append(year_from)
    if year_to:        sql += " AND year <= ?";            params.append(year_to)
    if sex:            sql += " AND sex = ?";              params.append(sex)
    sql += " ORDER BY indicator_code, year DESC, sex"
    cursor.execute(sql, params)
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


def get_summary():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) as total FROM health_data")
    total = cursor.fetchone()["total"]
    cursor.execute("SELECT source, COUNT(*) as count FROM health_data GROUP BY source")
    by_source = {row["source"]: row["count"] for row in cursor.fetchall()}
    cursor.execute("SELECT source, synced_at FROM sync_log ORDER BY synced_at DESC LIMIT 10")
    recent_syncs = [dict(row) for row in cursor.fetchall()]
    cursor.execute("SELECT COUNT(DISTINCT indicator_code) as n FROM health_data")
    n_indicators = cursor.fetchone()["n"]
    conn.close()
    return {"total_records": total, "indicators": n_indicators, "by_source": by_source, "recent_syncs": recent_syncs}
