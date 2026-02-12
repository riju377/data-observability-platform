
import os
import time
import psycopg
from database import DB_CONFIG

def init_db():
    print("Initializing database...")
    
    # Retry logic for database connection (it might take a few seconds to start)
    max_retries = 5
    for i in range(max_retries):
        try:
            with psycopg.connect(**DB_CONFIG) as conn:
                print("Connected to database provided via DATABASE_URL or env vars.")
                with conn.cursor() as cur:
                    # check if schema already exists (simple check for organizations table)
                    cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'organizations');")
                    exists = cur.fetchone()[0]
                    
                    if exists:
                        print("Schema appears to exist. Skipping initialization.")
                        return

                    print("Schema not found. Applying schema.sql...")
                    with open("schema.sql", "r") as f:
                        schema_sql = f.read()
                        cur.execute(schema_sql)
                    
                    conn.commit()
                    print("Database initialization completed successfully! 🚀")
                    return
        except Exception as e:
            print(f"Attempt {i+1}/{max_retries} failed: {e}")
            if i < max_retries - 1:
                time.sleep(2)
            else:
                print("Could not initialize database. Exiting.")
                exit(1)

if __name__ == "__main__":
    init_db()
