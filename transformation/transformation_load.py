import csv
import pyodbc


CONN_STR_POSTGRES = (
    "DRIVER={PostgreSQL Unicode};"
    "SERVER=localhost;"
    "PORT=5432;"
    "DATABASE=postgres;"
    "UID=postgres;"
    "PWD=7002anuN;"
)

CONN_STR_DWH = (
    "DRIVER={PostgreSQL Unicode};"
    "SERVER=localhost;"
    "PORT=5432;"
    "DATABASE=dwh;"
    "UID=postgres;"
    "PWD=7002anuN;"
)


def ensure_database_exists() -> None:
    """Create the dwh database if it does not already exist."""
    conn = pyodbc.connect(CONN_STR_POSTGRES)
    conn.autocommit = True
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'dwh'")
        if not cursor.fetchone():
            cursor.execute("CREATE DATABASE dwh")
            print("Database 'dwh' created!")
        else:
            print("Database 'dwh' already exists.")
    finally:
        cursor.close()
        conn.close()


def load_csv_to_table(cursor, table_name: str, file_path: str, column_count: int) -> None:
    """Truncate a table and load a CSV file into it."""
    placeholders = ",".join(["?" for _ in range(column_count)])
    rows_loaded = 0
    rows_skipped = 0

    cursor.execute(f"TRUNCATE TABLE {table_name}")

    with open(file_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)

        if header is None:
            print(f"{file_path} is empty. Skipping {table_name}.")
            return

        if len(header) != column_count:
            print(
                f"Warning: header in {file_path} has {len(header)} columns, "
                f"but {table_name} expects {column_count}."
            )

        columns_sql = ",".join(header[:column_count])

        for line_number, row in enumerate(reader, start=2):
            if len(row) != column_count:
                print(
                    f"Skipping row {line_number} in {file_path}: "
                    f"expected {column_count} values, got {len(row)} -> {row}"
                )
                rows_skipped += 1
                continue

            row = [None if val.strip() == "" else val.strip() for val in row]

            cursor.execute(
                f"INSERT INTO {table_name} ({columns_sql}) VALUES ({placeholders})",
                row
            )
            rows_loaded += 1

    print(f"Loaded {rows_loaded} rows into {table_name}")
    if rows_skipped > 0:
        print(f"Skipped {rows_skipped} bad rows in {file_path}")


def main() -> None:
    ensure_database_exists()

    tables = [
        {"table": "transformation.crm_cust_info", "file": "cust_info_clean.csv", "columns": 7},
        {"table": "transformation.crm_prd_info", "file": "prd_info_clean.csv", "columns": 8},
        {"table": "transformation.crm_sales_details", "file": "sales_details_clean.csv", "columns": 9},
        {"table": "transformation.erp_cust_az12", "file": "cust_az12_clean.csv", "columns": 3},
        {"table": "transformation.erp_loc_a101", "file": "loc_a101_clean.csv", "columns": 2},
        {"table": "transformation.erp_px_cat_g1v2", "file": "PX_CAT_G1V2.csv", "columns": 4},
    ]

    conn = pyodbc.connect(CONN_STR_DWH)
    conn.autocommit = False
    cursor = conn.cursor()

    try:
        for table in tables:
            load_csv_to_table(
                cursor=cursor,
                table_name=table["table"],
                file_path=table["file"],
                column_count=table["columns"],
            )
            conn.commit()

        print("All tables loaded!")
    except Exception as e:
        conn.rollback()
        print(f"Error occurred: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()