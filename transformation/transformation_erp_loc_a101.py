import pandas as pd
import pyodbc
import datetime

conn = pyodbc.connect(
    "DRIVER={PostgreSQL Unicode};"
    "SERVER=localhost;"
    "PORT=5432;"
    "DATABASE=dwh;"
    "UID=postgres;"
    "PWD=*****;"
)
conn.autocommit = True
cursor = conn.cursor()

# Create transformation schema if it doesn't exist
cursor.execute("CREATE SCHEMA IF NOT EXISTS transformation")

df = pd.read_sql_query(sql="SELECT * FROM ingestion.erp_loc_a101", con=conn)

# Keep only the last 5 characters from the customer id
df["cid"] = df["cid"].str.strip()
df["cid"] = df["cid"].str[-5:]

# Make the countries consistent
df["cntry"] = df["cntry"].str.strip()
df["cntry"] = df["cntry"].replace("US", "United States")
df["cntry"] = df["cntry"].replace("USA", "United States")
df["cntry"] = df["cntry"].replace("DE", "Germany")
df["cntry"] = df["cntry"].replace("", "NULL")
df["cntry"] = df["cntry"].fillna("NULL")

print(df["cntry"].unique())

