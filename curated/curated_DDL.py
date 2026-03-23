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

# Create curated schema
cursor.execute("""
CREATE SCHEMA IF NOT EXISTS curated;
""")

# Drop existing curated tables for a clean rebuild
cursor.execute("DROP TABLE IF EXISTS curated.fact_sales;")
cursor.execute("DROP TABLE IF EXISTS curated.dim_products;")
cursor.execute("DROP TABLE IF EXISTS curated.dim_customers;")

# Create customer dimension
cursor.execute("""
CREATE TABLE curated.dim_customers (
    customer_key INTEGER PRIMARY KEY,
    customer_id INTEGER,
    customer_number VARCHAR(50),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    country VARCHAR(50),
    marital_status VARCHAR(50),
    gender VARCHAR(50),
    birthdate DATE,
    create_date DATE
);
""")

# Create product dimension
cursor.execute("""
CREATE TABLE curated.dim_products (
    product_key INTEGER PRIMARY KEY,
    product_number VARCHAR(50),
    product_name VARCHAR(100),
    category_id VARCHAR(50),
    category VARCHAR(50),
    subcategory VARCHAR(50),
    maintenance VARCHAR(50),
    cost NUMERIC(10,2),
    product_line VARCHAR(50),
    start_date DATE,
    end_date DATE
);
""")

# Create sales fact table
cursor.execute("""
CREATE TABLE curated.fact_sales (
    sales_key INTEGER PRIMARY KEY,
    order_number VARCHAR(50),
    product_key INTEGER,
    customer_key INTEGER,
    order_date DATE,
    shipping_date DATE,
    due_date DATE,
    sales_amount NUMERIC(10,2),
    quantity INTEGER,
    price NUMERIC(10,2),
    CONSTRAINT fk_fact_sales_product
        FOREIGN KEY (product_key)
        REFERENCES curated.dim_products(product_key),
    CONSTRAINT fk_fact_sales_customer
        FOREIGN KEY (customer_key)
        REFERENCES curated.dim_customers(customer_key)
);
""")

print("Curated schema and tables created successfully!")

cursor.close()
conn.close()