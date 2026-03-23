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
cursor.fast_executemany = True

# Clear curated tables before reloading
cursor.execute("DELETE FROM curated.fact_sales;")
cursor.execute("DELETE FROM curated.dim_products;")
cursor.execute("DELETE FROM curated.dim_customers;")
conn.commit()

# Load source tables into Dataframes
customer_crm_df = pd.read_sql("SELECT * FROM transformation.crm_cust_info", conn)
customer_erp_df = pd.read_sql("SELECT * FROM transformation.erp_cust_az12", conn)
location_erp_df = pd.read_sql("SELECT * FROM transformation.erp_loc_a101", conn)

product_crm_df = pd.read_sql("SELECT * FROM transformation.crm_prd_info", conn)
category_erp_df = pd.read_sql("SELECT * FROM transformation.erp_px_cat_g1v2", conn)

sales_df = pd.read_sql("SELECT * FROM transformation.crm_sales_details", conn)

# Build dim_customers

# Left joins to merge the Dataframes
df = pd.merge(
    left = customer_crm_df,
    right = customer_erp_df,
    how = "left",
    left_on = "cst_key",
    right_on = "cid"
)

df = pd.merge(
    left = df,
    right = location_erp_df,
    how = "left",
    left_on = "cst_key",
    right_on = "cid",
    suffixes = ("", "_loc")
)

dim_customers = pd.DataFrame({
    "customer_id" : df["cst_id"],
    "customer_number" : df["cst_key"],
    "first_name" : df["cst_firstname"],
    "last_name" : df["cst_lastname"],
    "country" : df["cntry"],
    "marital_status" : df["cst_marital_status"],
    "gender" : df["cst_gndr"],
    "birthdate" : df["bdate"],
    "create_date" : df["cst_create_date"]
})

dim_customers.insert(0, "customer_key", dim_customers.index + 1)

sql_customers = """
INSERT INTO curated.dim_customers
(customer_key, customer_id, customer_number, first_name, last_name, country, 
 marital_status, gender, birthdate, create_date)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

dim_customers = dim_customers.astype(object).where(pd.notnull(dim_customers), None)
customer_rows = dim_customers.itertuples(index=False, name=None)

cursor.executemany(sql_customers, customer_rows)
conn.commit()

print("Inserted into curated.dim_customers")

# Build dim_products

# Left join products with categories
df = pd.merge(
    left=product_crm_df,
    right=category_erp_df,
    how="left",
    left_on="prd_subcategory",
    right_on="id",
)

# Build final dataframe

dim_products = pd.DataFrame({
    "product_number": df["prd_key"],
    "product_name": df["prd_nm"],
    "category_id": df["prd_subcategory"],
    "category": df["cat"],
    "subcategory": df["subcat"],
    "maintenance": df["maintenance"],
    "cost": df["prd_cost"],
    "product_line": df["prd_line"],
    "start_date": df["prd_start_dt"],
    "end_date": df["prd_end_dt"]
})

print(dim_products.head(10))
dim_products = dim_products.sort_values(by="product_number").reset_index(drop=True)
dim_products.insert(0, "product_key", dim_products.index + 1)

print(dim_products.head(10))


sql_products = """
INSERT INTO curated.dim_products
(product_key, product_number, product_name, category_id, category, subcategory,
 maintenance, cost, product_line, start_date, end_date)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

dim_products = dim_products.astype(object).where(pd.notnull(dim_products), None)
product_rows = dim_products.itertuples(index=False, name=None)

cursor.executemany(sql_products, product_rows)
conn.commit()

print("Inserted into curated.dim_products")

# Build fact_sales

# Bring customer_key into sales
fact_sales = pd.merge(
    left=sales_df,
    right=dim_customers[["customer_key", "customer_id"]],
    how="left",
    left_on="sls_cust_id",
    right_on="customer_id"
)

# Left join sales with products key
fact_sales = pd.merge(
    left=fact_sales,
    right=product_crm_df[["prd_id", "prd_key"]],
    how="left",
    left_on="sls_prd_key",
    right_on="prd_key"
)

fact_sales = pd.merge(
    left=fact_sales,
    right=dim_products[["product_key", "product_number"]],
    how="left",
    left_on="prd_key",
    right_on="product_number"
)

# Final fact table
fact_sales = pd.DataFrame({
    "order_number": fact_sales["sls_ord_num"],
    "product_key": fact_sales["product_key"],
    "customer_key": fact_sales["customer_key"],
    "order_date": fact_sales["sls_order_dt"],
    "shipping_date": fact_sales["sls_ship_dt"],
    "due_date": fact_sales["sls_due_dt"],
    "sales_amount": fact_sales["sls_sales"],
    "quantity": fact_sales["sls_quantity"],
    "price": fact_sales["sls_price"]
})

# Remove rows where dimension keys are missing
fact_sales = fact_sales.dropna(subset=["product_key", "customer_key"]).reset_index(drop=True)

# Make sure key columns are integers
fact_sales["product_key"] = fact_sales["product_key"].astype(int)
fact_sales["customer_key"] = fact_sales["customer_key"].astype(int)
fact_sales.insert(0, "sales_key", fact_sales.index + 1)



sql_fact = """
INSERT INTO curated.fact_sales
(sales_key, order_number, product_key, customer_key, order_date, shipping_date, due_date,
 sales_amount, quantity, price)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

fact_sales = fact_sales.astype(object).where(pd.notnull(fact_sales), None)
fact_rows = fact_sales.itertuples(index=False, name=None)

cursor.executemany(sql_fact, fact_rows)
conn.commit()

print("Inserted into curated.fact_sales")

cursor.close()
conn.close()

print("Curated layer loaded successfully!")




