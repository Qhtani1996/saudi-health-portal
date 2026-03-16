cd ~/Dropbox/Office/Portofolio/saudi_health_portal

cat > backend/load_data.py << 'EOF'
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from database import init_db, save_records, get_summary
from connectors.who import fetch_all_indicators
from connectors.ihme import load_ihme_data
from connectors.local import load_all_gstat

def run():
    print("=" * 55)
    print("Saudi Health Portal — Data Loader")
    print("=" * 55)

    print("\n[1/4] Initialising database...")
    init_db()

    print("\n[2/4] Fetching WHO live data...")
    try:
        records = fetch_all_indicators(verbose=True)
        save_records(records, source="WHO GHO")
        print(f"WHO: {len(records)} records saved.")
    except Exception as e:
        print(f"WHO failed: {e}")

    print("\n[3/4] Loading IHME data...")
    try:
        records = load_ihme_data(verbose=True)
        if records:
            save_records(records, source="IHME GBD")
            print(f"IHME: {len(records):,} records saved.")
    except Exception as e:
        print(f"IHME failed: {e}")

    print("\n[4/4] Loading GSTAT data...")
    try:
        records = load_all_gstat(verbose=True)
        if records:
            save_records(records, source="GSTAT")
            print(f"GSTAT: {len(records):,} records saved.")
        else:
            print("GSTAT: no records loaded.")
    except Exception as e:
        print(f"GSTAT failed: {e}")

    print("\n" + "=" * 55)
    summary = get_summary()
    print(f"  Total records : {summary['total_records']:,}")
    print(f"  By source     : {summary['by_source']}")
    print("=" * 55)

if __name__ == "__main__":
    run()
EOF

python3 backend/load_data.py