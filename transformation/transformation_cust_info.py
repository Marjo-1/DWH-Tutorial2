import pyodbc
import pandas as pd

#connect to dwh
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

df = pd.read_sql_query(sql="SELECT * FROM ingestion.crm_cust_info", con=conn)

print(df)
#

# Drop NA
df = df.dropna()

# Delete duplicates, keep most recent created row
df["cst_create_date"] = pd.to_datetime(df["cst_create_date"], errors="coerce")
df = df.sort_values(by="cst_create_date", ascending=False)
df = df.drop_duplicates(subset="cst_id", keep="first")

# Strip the blanks before and after first and last name
df["cst_firstname"] = df["cst_firstname"].str.strip()
df["cst_lastname"] = df["cst_lastname"].str.strip()

# For marital status change M --> Married and S --> Single
df["cst_marital_status"] = df["cst_marital_status"].replace("M", "Married")
df["cst_marital_status"] = df["cst_marital_status"].replace("S", "Single")

# For gender change M --> Male and F --> Female
df["cst_gndr"] = df["cst_gndr"].replace("M", "Male")
df["cst_gndr"] = df["cst_gndr"].replace("F", "Female")

df["cst_id"] = df["cst_id"].astype("Int64")

print(df.head())

df.to_csv("cust_info_clean.csv", index = False)


