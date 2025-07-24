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

from ETL.utils.data_sourcing.customers import generate_base_customers, randomly_update_customers

# ============================================================================================================
# for this section, i'm going to mock some data as if it came from some database or API
# so that we have a flow of data we can use in our project
def get_raw_customer_data() -> pd.DataFrame:
    base_df = generate_base_customers(seed=101, num_customers=10000) # leave seed as static
    customer_df = randomly_update_customers(base_df, update_rate=0.05) # customer data now in memory
    dt = (datetime.date.today()).strftime('%Y-%m-%d')
    customer_df['extract_date'] = dt
    return customer_df


# =============================================================================================================
# collect env variables for connection to DuckLake as ETL admin
def etl():
    """
    Process the ETL stage of loading raw customer data to bronze layer of DuckLake.
    Automatically manages connection context to ensure clean closure.
    """
    pg_host = os.getenv('PG_HOST')
    pg_user = os.getenv('PG_USER')
    pg_password = os.getenv('PG_PASSWORD')

    # collect customer_data first (so it's available even if we need to infer schema)
    customer_df = get_raw_customer_data()

    # DuckDB connection with context manager for auto-close
    with duckdb.connect(database=":memory:") as con:
        con.execute(f"""
        ATTACH 'ducklake:postgres:dbname=ducklake_catalog host={pg_host} user={pg_user} password={pg_password}' AS retail_ducklake ;
        USE retail_ducklake ;
        """)

        # create table if not exists (based on schema of customer_df)
        try:
            con.execute("SELECT 1 FROM retail_bronze.customer_src_raw ;")
        except:
            con.register("customer_df", customer_df)  # register pandas DataFrame
            con.execute("""
            CREATE TABLE retail_bronze.customer_src_raw AS SELECT * FROM customer_df LIMIT 0 ;
            ALTER TABLE retail_bronze.customer_src_raw SET PARTITIONED BY (extract_date) ;
            """)

        # Start PySpark-like session
        spark = DuckDBSession(conn=con)

        # clear existing extract_date if exists
        trgt_tbl = spark.table("retail_bronze.customer_src_raw")
        dt = (datetime.date.today()).strftime('%Y-%m-%d')
        trgt_tbl.delete(where=trgt_tbl["extract_date"] == dt).execute()

        # create spark dataframe and insert
        cust_df = spark.createDataFrame(customer_df)
        cust_df.write.mode("append").insertInto("retail_bronze.customer_src_raw")

        # clean up the context (optional inside `with`, but for completeness)
        con.execute("USE memory ;")
        con.execute("DETACH retail_ducklake ;")


if __name__ == "__main__":
    print("Running ETL process for BRONZE -- Raw Customer Data ...")
    etl() # process ETL
    print("Data Load to `retail_bronze.customer_src_raw` completed")
    exit()
