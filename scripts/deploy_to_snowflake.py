import os
from pathlib import Path
import snowflake.connector

def get_applied_scripts(conn):
    """Get the set of already applied scripts from Snowflake"""
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ETL_DEPLOY_LOG_NEW (
            SCRIPT_NAME STRING PRIMARY KEY,
            APPLIED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("SELECT SCRIPT_NAME FROM ETL_DEPLOY_LOG_NEW")
    return set(row[0] for row in cursor.fetchall())

def run_new_sql_scripts(directory, conn, applied_scripts):
    """Run only new SQL files in a given directory"""
    sql_files = sorted(Path(directory).glob("*.sql"))
    for sql_file in sql_files:
        if f"{directory}/{sql_file.name}" in applied_scripts:
            print(f"Skipping {directory}/{sql_file.name} (already applied)")
            continue

        print(f"Running {sql_file.name}...")
        with open(sql_file, 'r') as f:
            sql_commands = f.read()
        try:
            for stmt in sql_commands.split(";"):
                stmt = stmt.strip()
                if stmt:
                    conn.cursor().execute(stmt)
            # Mark as applied
            insert_to_etl_log = f"{directory}/{sql_file.name}"
            conn.cursor().execute(
                "INSERT INTO ETL_DEPLOY_LOG_NEW (SCRIPT_NAME) VALUES (%s)", (insert_to_etl_log,)
            )
            print(f"{directory}/{sql_file.name} executed successfully.")
        except Exception as e:
            print(f"Error in {directory}/{sql_file.name}: {e}")
            raise

def main():
    conn = snowflake.connector.connect(
        user=os.environ["SNOW_USER"],
        password=os.environ["SNOW_PASSWORD"],
        account=os.environ["SNOW_ACCOUNT"],
        warehouse=os.environ["SNOW_WAREHOUSE"],
        database=os.environ["SNOW_DATABASE"],
        schema=os.environ["SNOW_SCHEMA"],
        role=os.environ.get("SNOW_ROLE")
    )

    applied_scripts = get_applied_scripts(conn)

    # Run snowDDL first, then snowDML
    run_new_sql_scripts("snowDDL", conn, applied_scripts)
    run_new_sql_scripts("snowDML", conn, applied_scripts)

    conn.close()
    print("Deployment finished successfully.")

if __name__ == "__main__":
    main()
