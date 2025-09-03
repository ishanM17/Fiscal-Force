import cx_Oracle
import pandas as pd
import sys
import os

# ======================
# Connection Parameters
# ======================
HOST = "localhost"          # e.g., "localhost" or IP
PORT = 1521                 # default Oracle port
SERVICE = "xepdb1"    # e.g., "ORCLPDB1"
USER = "HR"
PASSWORD = "admin"

dsn = cx_Oracle.makedsn(HOST, PORT, service_name=SERVICE)

try:
    conn = cx_Oracle.connect(user=USER, password=PASSWORD, dsn=dsn)
    cursor = conn.cursor()
    cursor.execute("SELECT sysdate FROM dual")
    db_time = cursor.fetchone()[0]
    print(f"✅ Connection successful. Oracle sysdate = {db_time}")
except Exception as e:
    print("❌ Failed to connect to Oracle:", e)
    sys.exit(1)


# ======================
# Step 2: Excel Data Load
# ======================
excel_file = "data_schema_v3_new_final.xlsx" # <------------------Provide excel file name
xls = pd.ExcelFile(excel_file)

sheet_to_table = {
    "Risk_Profile": "FF_RISK_PROFILES",
    "Asset_Objectives": "FF_ASSET_OBJECTIVES",
    "Questions": "FF_QUESTIONS",
    "Asset_Classes_new": "FF_ASSET_CLASSES",
    # "Fee_Structures": "FF_FEE_STRUCTURES",
    "Engagement_Frequencies": "FF_ENGAGEMENT_FREQUENCIES",
    "Engagement_Types": "FF_ENGAGEMENT_TYPES",
    "Potential_Funds": "FF_POTENTIAL_FUNDS",
    "Customers": "FF_CUSTOMERS",
    "Customer_Assets": "FF_CUSTOMER_ASSETS",
    "Answers": "FF_ANSWERS",
    "Customer_Answers": "FF_CUSTOMER_ANSWERS",
    "Fund_Assets": "FF_FUND_ASSETS",
    "Customer_Funds": "FF_CUSTOMER_FUNDS",
    "Fund_Targets": "FF_FUND_TARGETS",
    "Customer_Engagement_Preferences": "FF_CEP"
}

# Parent→Child load order
load_order = [
    "Risk_Profile",
    "Asset_Objectives",
    "Questions",
    "Asset_Classes_new",
    # "Fee_Structures",
    "Engagement_Frequencies",
    "Engagement_Types",
    "Potential_Funds",
    "Customers",
    "Customer_Assets",
    "Answers",
    "Customer_Answers",
    "Fund_Assets",
    "Customer_Funds",
    "Fund_Targets",
    "Customer_Engagement_Preferences"
]

def to_python_type(val):
    """Convert Excel/Pandas/numpy types into Python-native for Oracle"""
    if pd.isna(val):
        return None
    if isinstance(val, (int, float, str)):
        return val
    if hasattr(val, "item"):   # numpy scalar
        return val.item()
    return str(val)  # fallback

print("📥 Starting data load...")
for sheet in load_order:
    table = sheet_to_table[sheet]
    if sheet not in xls.sheet_names:
        print(f"⚠️ Skipping {sheet}, not found in Excel")
        continue

    df = pd.read_excel(excel_file, sheet_name=sheet)

    # Get DB columns
    cursor.execute(f"SELECT column_name FROM user_tab_columns WHERE table_name=UPPER('{table}')")
    db_cols = [r[0] for r in cursor.fetchall()]

    common_cols = [c for c in df.columns if c.upper() in db_cols]
    if not common_cols:
        print(f"⚠️ No matching columns for {table}")
        continue

    placeholders = ",".join([f":{i+1}" for i in range(len(common_cols))])
    sql = f"INSERT INTO {table} ({','.join([c.upper() for c in common_cols])}) VALUES ({placeholders})"

    data = [tuple(to_python_type(row[c]) for c in common_cols) for _, row in df.iterrows()]

    try:
        cursor.executemany(sql, data)
        conn.commit()
        print(f"✅ Loaded {len(data)} rows into {table}")
    except Exception as e:
        print(f"❌ Error loading {table}: {e}")
        conn.rollback()

cursor.close()
conn.close()
print("🎉 Data load completed successfully.")
