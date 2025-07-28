# =============================================
# This script will use SQL Frame to mock the
# PySpark API, but backed by DuckDB & DuckLake
# =============================================

# imports
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[2]))  # adjust depth to reach root of Project Root

import os
import duckdb
import datetime
import pandas as pd
from sqlframe.duckdb import DuckDBSession
from sqlframe.duckdb import functions as F

from ETL.utils.data_sourcing.products import get_product_catalog


# =============================================================================================================
# collect env variables for connection to DuckLake as ETL admin
def etl():
    """
    Process the ETL stage of loading raw product data to bronze layer of DuckLake.
    Automatically manages connection context to ensure clean closure.
    """
    pg_host = os.getenv('PG_HOST')
    pg_user = os.getenv('PG_USER')
    pg_password = os.getenv('PG_PASSWORD')

    # collect customer_data first (so it's available even if we need to infer schema)
    print("generating fake product data ...")
    products_df = get_product_catalog(show_progress=True)
    products_df['extract_date'] = datetime.date.today().strftime("%Y-%m-%d")

    # DuckDB connection with context manager for auto-close
    with duckdb.connect(database=":memory:") as con:
        print("connecting to ducklake ...")
        con.execute(f"""
        ATTACH 'ducklake:postgres:dbname=ducklake_catalog host={pg_host} user={pg_user} password={pg_password}' AS retail_ducklake ;
        USE retail_ducklake ;
        """)

        # create table if not exists (based on schema of df)
        try:
            con.execute("SELECT 1 FROM retail_bronze.products_src_raw ;")
        except:
            print("Table: `retail_bronze.products_src_raw` does not yet exist. Creating ...")
            con.register("products_df", products_df)  # register pandas DataFrame
            con.execute("""
            CREATE TABLE retail_bronze.products_src_raw AS SELECT * FROM products_df LIMIT 0 ;
            ALTER TABLE retail_bronze.products_src_raw SET PARTITIONED BY (extract_date) ;
            """)
            print("Table: `retail_bronze.products_src_raw` created")
        # Start PySpark-like session
        spark = DuckDBSession(conn=con)

        # clear existing extract_date if exists
        print("Clearing existing partition of extract date if exists ...")
        trgt_tbl = spark.table("retail_bronze.products_src_raw")
        dt = (datetime.date.today()).strftime('%Y-%m-%d')
        trgt_tbl.delete(where=trgt_tbl["extract_date"] == dt).execute()

        # create spark dataframe and insert
        print("Writing latest extract date of products data to target table ...")
        prd_df = spark.createDataFrame(products_df.to_dict(orient='records'))
        prd_df.write.mode("append").insertInto("retail_bronze.products_src_raw")

        # clean up the context (optional inside `with`, but for completeness)
        con.execute("USE memory ;")
        con.execute("DETACH retail_ducklake ;")


if __name__ == "__main__":
    print("Running ETL process for BRONZE -- Raw Products Data ...")
    etl() # process ETL
    print("Data Load to `retail_bronze.products_src_raw` completed")
    exit()
