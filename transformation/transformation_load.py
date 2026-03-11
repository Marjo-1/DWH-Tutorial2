import pyodbc
import csv

# Connect to postgres to create database if needed
conn = pyodbc.connect(
    "DRIVER={PostgreSQL Unicode};"
    "SERVER=localhost;"
    "PORT=5432;"
    "DATABASE=postgres;"
    "UID=postgres;"
    "PWD=*****;"
)
conn.autocommit = True
cursor = conn.cursor()

cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'dwh'")
if not cursor.fetchone():
    cursor.execute("CREATE DATABASE dwh")
    print("Database 'dwh' created!")
else:
    print("Database 'dwh' already exists.")

cursor.close()
conn.close()

# Connect to dwh
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

# Create transformation schema
cursor.execute("CREATE SCHEMA IF NOT EXISTS transformation")
print("Schema transformation created!")

# Drop and create table
cursor.execute("DROP TABLE IF EXISTS transformation.crm_cust_info")
cursor.execute("""
    CREATE TABLE transformation.crm_cust_info (
        cst_id INTEGER,
        cst_key VARCHAR(50),
        cst_firstname VARCHAR(50),
        cst_lastname VARCHAR(50),
        cst_marital_status VARCHAR(50),
        cst_gndr VARCHAR(50),
        cst_create_date DATE
    )
""")
print("Table transformation.crm_cust_info created!")

# Load cleaned CSV
table_name = "transformation.crm_cust_info"
file_path = "cust_info_clean.csv"
placeholders = ",".join(["?" for _ in range(7)])

cursor.execute(f"TRUNCATE TABLE {table_name}")

with open(file_path, "r", encoding="utf-8") as f:
    reader = csv.reader(f)
    next(reader)  # skip header
    rows = 0
    for row in reader:
        row = [None if val == "" else val for val in row]
        cursor.execute(f"INSERT INTO {table_name} VALUES ({placeholders})", row)
        rows += 1

print(f"Loaded {rows} rows into {table_name}")

cursor.close()
conn.close()
print("All tables loaded!")
