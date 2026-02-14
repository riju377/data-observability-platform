
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
                        print("Schema tables exist.")
                    else:
                        print("Schema not found. Applying schema.sql...")
                        with open("schema.sql", "r") as f:
                            cur.execute(f.read())
                    
                    # Always apply functions (safe because of CREATE OR REPLACE)
                    print("Updating stored functions...")
                    functions_sql = """
                    -- Function to get all upstream column dependencies
                    CREATE OR REPLACE FUNCTION get_upstream_columns(dataset_name_param VARCHAR, column_name_param VARCHAR, p_org_id UUID)
                    RETURNS TABLE(dataset_name VARCHAR, column_name VARCHAR, transform_type VARCHAR, expression TEXT, depth INTEGER) AS $$
                    WITH RECURSIVE upstream AS (
                        -- Base case: find direct parents
                        SELECT
                            ds.name as dataset_name,
                            cle.source_column as column_name,
                            cle.transform_type,
                            cle.expression,
                            1 as depth
                        FROM column_lineage_edges cle
                        JOIN datasets dt ON cle.target_dataset_id = dt.id
                        JOIN datasets ds ON cle.source_dataset_id = ds.id
                        WHERE dt.name = dataset_name_param AND cle.target_column = column_name_param
                          AND dt.organization_id = p_org_id

                        UNION

                        -- Recursive case: find parents of parents
                        SELECT
                            ds.name as dataset_name,
                            cle.source_column as column_name,
                            cle.transform_type,
                            cle.expression,
                            u.depth + 1 as depth
                        FROM upstream u
                        JOIN datasets dt ON u.dataset_name = dt.name AND dt.organization_id = p_org_id
                        JOIN column_lineage_edges cle ON dt.id = cle.target_dataset_id AND u.column_name = cle.target_column
                        JOIN datasets ds ON cle.source_dataset_id = ds.id
                        WHERE u.depth < 10
                    )
                    SELECT DISTINCT dataset_name, column_name, transform_type, expression, MIN(depth) as depth
                    FROM upstream
                    GROUP BY dataset_name, column_name, transform_type, expression
                    ORDER BY depth, dataset_name;
                    $$ LANGUAGE SQL;

                    -- Function to get all downstream column dependencies
                    CREATE OR REPLACE FUNCTION get_downstream_columns(dataset_name_param VARCHAR, column_name_param VARCHAR, p_org_id UUID)
                    RETURNS TABLE(dataset_name VARCHAR, column_name VARCHAR, transform_type VARCHAR, expression TEXT, depth INTEGER) AS $$
                    WITH RECURSIVE downstream AS (
                        -- Base case: find direct children
                        SELECT
                            dt.name as dataset_name,
                            cle.target_column as column_name,
                            cle.transform_type,
                            cle.expression,
                            1 as depth
                        FROM column_lineage_edges cle
                        JOIN datasets ds ON cle.source_dataset_id = ds.id
                        JOIN datasets dt ON cle.target_dataset_id = dt.id
                        WHERE ds.name = dataset_name_param AND cle.source_column = column_name_param
                          AND ds.organization_id = p_org_id

                        UNION

                        -- Recursive case: find children of children
                        SELECT
                            dt.name as dataset_name,
                            cle.target_column as column_name,
                            cle.transform_type,
                            cle.expression,
                            d.depth + 1 as depth
                        FROM downstream d
                        JOIN datasets ds ON d.dataset_name = ds.name AND ds.organization_id = p_org_id
                        JOIN column_lineage_edges cle ON ds.id = cle.source_dataset_id AND d.column_name = cle.source_column
                        JOIN datasets dt ON cle.target_dataset_id = dt.id
                        WHERE d.depth < 10
                    )
                    SELECT DISTINCT dataset_name, column_name, transform_type, expression, MIN(depth) as depth
                    FROM downstream
                    GROUP BY dataset_name, column_name, transform_type, expression
                    ORDER BY depth, dataset_name;
                    $$ LANGUAGE SQL;
                    """
                    cur.execute(functions_sql)
                    
                    conn.commit()
                    print("Database initialization/update completed successfully! 🚀")
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
