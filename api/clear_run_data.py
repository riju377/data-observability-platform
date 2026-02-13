"""
Clear Run/Pipeline Data from Database
Keeps: users, organizations, api_keys, alert_rules
Clears: datasets, metrics, anomalies, alerts, schema versions, lineage, jobs
"""
import os
import sys

# Load .env from api/ directory
def load_env():
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
    if not os.path.exists(env_path):
        print(f"ERROR: {env_path} not found")
        sys.exit(1)
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                k, v = line.split('=', 1)
                if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
                    v = v[1:-1]
                if k not in os.environ:
                    os.environ[k] = v

load_env()

import psycopg

DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_SSLMODE = os.getenv('DB_SSLMODE', 'require')

if not all([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD]):
    print("ERROR: Missing DB_HOST, DB_NAME, DB_USER, or DB_PASSWORD in api/.env")
    sys.exit(1)

CONNINFO = f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} sslmode={DB_SSLMODE}"

TABLES_TO_CLEAR = [
    'alert_history',
    'anomalies',
    'data_quality_results',
    'data_quality_rules',
    'quality_violations',
    'quality_rules',
    'stage_metrics',
    'job_executions',
    'column_lineage_edges',
    'schema_versions',
    'dataset_metrics',
    'lineage_edges',
    'datasets',
    'freshness_sla',
]

def main():
    print("=" * 50)
    print("  CLEAR RUN/PIPELINE DATA")
    print("=" * 50)
    print(f"\nTables to clear: {', '.join(TABLES_TO_CLEAR)}")
    print("Tables preserved: users, organizations, api_keys, alert_rules")
    
    confirm = input("\nAre you sure? (yes/no): ").strip().lower()
    if confirm != 'yes':
        print("Cancelled.")
        return

    try:
        with psycopg.connect(CONNINFO) as conn:
            table_list = ', '.join(TABLES_TO_CLEAR)
            conn.execute(f"TRUNCATE TABLE {table_list} CASCADE")
            conn.commit()
            print(f"\n✅ Cleared {len(TABLES_TO_CLEAR)} tables successfully!")
            print("   Users, organizations, API keys, and alert rules are preserved.")
    except Exception as e:
        print(f"\n❌ Error: {e}")

if __name__ == '__main__':
    main()
