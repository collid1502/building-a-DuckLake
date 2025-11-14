# imports 
import os
import duckdb
import argparse
from src.utilities.data_generators import get_raw_customer_data, get_product_catalog, get_stores, get_transactions
from src.utilities.ducklake_helpers import connectToDucklake


# collect env variables for connection to DuckLake as ETL admin
def etl(extract_date: str, src: str): 
    """
    Process the ETL stage of loading raw data to bronze layer of DuckLake.
    """
    print(f"generating fake {src} data ...")

    process_src = {
        "customers": get_raw_customer_data,
        "products": get_product_catalog,
        "stores": get_stores,
        "transactions": get_transactions
    }

    # depending on the source chosen, run that function and collect the data to a pandas DF
    src_df = process_src[src](extract_date=extract_date, show_progress=True)

    # specify ducklake connection creds for ETL workloads
    ducklake_creds = {
        "catalog": "ducklake_catalog",
        "pg_host": os.getenv('PG_HOST'),
        "pg_user": os.getenv('PG_USER'),
        "pg_password": os.getenv('PG_PASSWORD')
    }

    print("connecting to ducklake ...")
    con = connectToDucklake("retail_ducklake", ducklake_creds) # provide a connection object for Ducklake
    con.register(f"{src}_df", src_df)  # register pandas DataFrame as DuckDB table

    print(f"IF Table: `retail_bronze.{src}_src_raw` does not yet exist. Creating ...")
    con.execute(f"CREATE TABLE IF NOT EXISTS retail_bronze.{src}_src_raw AS SELECT * FROM {src}_df LIMIT 0 ;")
    print(f"Table: `retail_bronze.{src}_src_raw` created")
    
    # execute write of data to table
    print(f"load {src} data to bronze layer")
    con.execute(f"DELETE FROM retail_bronze.{src}_src_raw WHERE extract_date = '{extract_date}' ;")
    con.execute(f"INSERT INTO retail_bronze.{src}_src_raw SELECT * FROM {src}_df ;")
    print("Data loaded")

    con.execute("USE memory ;")
    con.close() # closes connection


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run ETL process for raw data")
    parser.add_argument("--extract-date", type=str, help="Extract date in YYYY-MM-DD format")
    parser.add_argument("--src", type=str, help="Target data source")
    args = parser.parse_args()
    extract_date = args.extract_date
    target_src = args.src

    print(f"Running ETL process for BRONZE -- Raw {target_src} Data ...")
    etl(extract_date=extract_date, src=target_src) # process ETL
    print(f"Data Load to `retail_bronze.{target_src}_src_raw` completed")
