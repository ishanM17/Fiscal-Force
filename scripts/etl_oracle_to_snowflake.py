import oracledb
import snowflake.connector
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
import sys
import datetime

# -------------------------
# CONFIGURATION
# -------------------------

TABLES = [
    "FF_CUSTOMERS",
    "FF_ASSET_OBJECTIVES",
    "FF_CUSTOMER_ASSETS",
    "FF_QUESTIONS",
    "FF_RISK_PROFILES",
    "FF_ANSWERS",
    "FF_CUSTOMER_ANSWERS",
    "FF_ASSET_CLASSES",
    "FF_POTENTIAL_FUNDS",
    "FF_FUND_ASSETS",
    "FF_CUSTOMER_FUNDS",
    "FF_ENGAGEMENT_FREQUENCIES",
    "FF_ENGAGEMENT_TYPES",
    "FF_FUND_TARGETS",
    "FF_CEP"
]

# Oracle connection info
ORACLE_USER = "HR"
ORACLE_PASSWORD = "admin"
ORACLE_DSN = "localhost:1521/xepdb1"

# Snowflake connection info (use env vars in GitHub Actions)
# SNOW_ACCOUNT = "your_snowflake_account"
# SNOW_USER = "etl_user"
# SNOW_PASSWORD = "your_password"
# SNOW_WAREHOUSE = "COMPUTE_WH"
# SNOW_DATABASE = "MY_DB"
# SNOW_SCHEMA = "PROD"
# SNOW_ROLE = "ETL_ROLE"

SNOW_ACCOUNT = "KPQCAYB-AA44293"
SNOW_USER = "prod_user"
SNOW_PASSWORD = "Admin@123"
SNOW_WAREHOUSE = "ETL_WH"
SNOW_DATABASE = "FF_DB"
SNOW_SCHEMA = "PROD_FF"
SNOW_ROLE = "PROD_ROLE"


# -------------------------
# CONNECT TO DATABASES
# -------------------------
try:
    oracle_conn = oracledb.connect(
        user=ORACLE_USER,
        password=ORACLE_PASSWORD,
        dsn=ORACLE_DSN
    )
    print("✅ Connected to Oracle.")
except Exception as e:
    print(f"❌ Failed to connect to Oracle: {e}")
    sys.exit(1)

try:
    snow_conn = snowflake.connector.connect(
        user=SNOW_USER,
        password=SNOW_PASSWORD,
        account=SNOW_ACCOUNT,
        warehouse=SNOW_WAREHOUSE,
        database=SNOW_DATABASE,
        schema=SNOW_SCHEMA,
        role=SNOW_ROLE
    )
    print("✅ Connected to Snowflake.")
except Exception as e:
    print(f"❌ Failed to connect to Snowflake: {e}")
    sys.exit(1)

cursor_snow = snow_conn.cursor()
cursor_oracle = oracle_conn.cursor()

# -------------------------
# LOG TABLE SETUP
# -------------------------
try:
    cursor_snow.execute(f"""
        CREATE TABLE IF NOT EXISTS ETL_RUN_LOGS (
            RUN_ID STRING,
            TABLE_NAME STRING,
            STATUS STRING,
            ROW_COUNT INT,
            START_TIME TIMESTAMP_NTZ,
            END_TIME TIMESTAMP_NTZ,
            ERROR_MESSAGE STRING
        );
    """)
    print("🪵 Log table ready.")
except Exception as e:
    print(f"⚠️ Failed to verify/create log table: {e}")

# -------------------------
# ETL PROCESS
# -------------------------
run_id = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")

for table in TABLES:
    print(f"\n🚀 Processing table: {table}")
    start_time = datetime.datetime.utcnow()

    try:
        # Fetch data from Oracle
        cursor_oracle.execute(f"SELECT * FROM {table}")
        columns = [col[0] for col in cursor_oracle.description]
        rows = cursor_oracle.fetchall()

        if not rows:
            print(f"⚠️ No data found in {table}. Skipping...")
            cursor_snow.execute(f"""
                INSERT INTO ETL_RUN_LOGS 
                VALUES ('{run_id}', '{table}', 'SKIPPED', 0, '{start_time}', CURRENT_TIMESTAMP(), NULL);
            """)
            continue

        df = pd.DataFrame(rows, columns=columns)

        # Truncate Snowflake table
        cursor_snow.execute(f"TRUNCATE TABLE IF EXISTS {table}")
        print(f"🧹 Truncated {table}")

        # Load into Snowflake
        success, nchunks, nrows, _ = write_pandas(
            conn=snow_conn,
            df=df,
            table_name=table,
            quote_identifiers=False
        )

        if success:
            cursor_snow.execute(f"""
                INSERT INTO ETL_RUN_LOGS 
                VALUES ('{run_id}', '{table}', 'SUCCESS', {nrows}, '{start_time}', CURRENT_TIMESTAMP(), NULL);
            """)
            print(f"✅ Loaded {nrows} rows into {table}")
        else:
            raise Exception("write_pandas returned False")

    except Exception as e:
        error_msg = str(e).replace("'", "")
        cursor_snow.execute(f"""
            INSERT INTO ETL_RUN_LOGS 
            VALUES ('{run_id}', '{table}', 'FAILED', 0, '{start_time}', CURRENT_TIMESTAMP(), '{error_msg}');
        """)
        print(f"❌ Error loading {table}: {e}")

# -------------------------
# CLEANUP
# -------------------------
cursor_oracle.close()
cursor_snow.close()
oracle_conn.close()
snow_conn.close()

print("\n🎯 ETL run completed.")
print(f"Run ID: {run_id}")
