import pandas as pd
import pyodbc

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

df = pd.read_sql_query(sql="SELECT * FROM ingestion.crm_sales_details", con=conn)

# Set the missing order date to same order date as rest of the order
df.loc[df["sls_ord_num"] == "SO64338", "sls_order_dt"] = 20130816
df.loc[df["sls_ord_num"] == "SO64340", "sls_order_dt"] = 20130816
df.loc[df["sls_ord_num"] == "SO64377", "sls_order_dt"] = 20130817
df.loc[df["sls_ord_num"] == "SO64379", "sls_order_dt"] = 20130817
df.loc[df["sls_ord_num"] == "SO64381", "sls_order_dt"] = 20130817
df.loc[df["sls_ord_num"] == "SO64623", "sls_order_dt"] = 20130820
df.loc[df["sls_ord_num"] == "SO64624", "sls_order_dt"] = 20130820
df.loc[df["sls_ord_num"] == "SO64625", "sls_order_dt"] = 20130820
df.loc[df["sls_ord_num"] == "SO64743", "sls_order_dt"] = 20130822
df.loc[df["sls_ord_num"] == "SO64744", "sls_order_dt"] = 20130822
df.loc[df["sls_ord_num"] == "SO65337", "sls_order_dt"] = 20130829
df.loc[df["sls_ord_num"] == "SO65338", "sls_order_dt"] = 20130829
df.loc[df["sls_ord_num"] == "SO69215", "sls_order_dt"] = 20131026

# when all order dates missing in the order set it to shipping date -1
df.loc[df["sls_ord_num"] == "SO64339", "sls_order_dt"] = 20130822
df.loc[df["sls_ord_num"] == "SO65340", "sls_order_dt"] = 20130904

# Make all the total sales and sales quantity positive
df["sls_sales"] = df["sls_sales"].abs()
df["sls_quantity"] = df["sls_quantity"].abs()

# Set the sales price equal to the total sales value / the quantity
df["sls_price"] = df["sls_sales"] / df["sls_quantity"]

print(df.head())

df.to_csv("sales_details_clean.csv", index = False)
