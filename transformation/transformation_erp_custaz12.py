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

df = pd.read_sql_query(sql="SELECT * FROM ingestion.erp_cust_az12", con=conn)

# Keep only the last 5 characters from the customer id
df["cid"] = df["cid"].str[-5:]


# Change M to Male, F to Female, and all empty columns to NULL
df["gen"] = df["gen"].str.strip()
df["gen"] = df["gen"].replace("M", "Male")
df["gen"] = df["gen"].replace("F", "Female")
df["gen"] = df["gen"].replace("", "NULL")
df["gen"] = df["gen"].fillna("NULL")

# Replace birth dates in the future with NULL
today = datetime.date.today()
df.loc[df["bdate"] > today, "bdate"] = "NULL"


print(df["gen"].unique())
