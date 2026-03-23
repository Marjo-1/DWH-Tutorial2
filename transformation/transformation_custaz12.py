import pandas as pd
import pyodbc
import datetime

conn = pyodbc.connect(
    "DRIVER={PostgreSQL Unicode};"
    "SERVER=localhost;"
    "PORT=5432;"
    "DATABASE=dwh;"
    "UID=postgres;"
    "PWD=7002anuN;"
)
conn.autocommit = True
cursor = conn.cursor()

# Create transformation schema if it doesn't exist
cursor.execute("CREATE SCHEMA IF NOT EXISTS transformation")

df = pd.read_sql_query(sql="SELECT * FROM ingestion.erp_cust_az12", con=conn)

# Keep only the last 5 characters from the customer id
df["cid"] = df["cid"].str[-5:]


# Change M to Male, F to Female, and all empty columns to NA
df["gen"] = df["gen"].str.strip()
df["gen"] = df["gen"].replace("M", "Male")
df["gen"] = df["gen"].replace("F", "Female")
df["gen"] = df["gen"].replace("", pd.NA)
df["gen"] = df["gen"].fillna(pd.NA)

# Replace birth dates in the future with NA
df["bdate"] = pd.to_datetime(df["bdate"], errors="coerce")
today = pd.Timestamp.today().normalize()
df.loc[df["bdate"] > today, "bdate"] = pd.NaT


print(df["gen"].unique())

df.to_csv("cust_az12_clean.csv", index = False)

