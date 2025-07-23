# =============================================
# This script will use SQL Frame to mock the
# PySpark API, but backed by DuckDB & DuckLake
# =============================================

# imports
import os
import duckdb
import datetime
import pandas as pd

from local_development.data_sources.customers import generate_base_customers, randomly_update_customers

# ==============================================================================================
# for this section, i'm going to mock some data as if it came from some database or API
# so that we have a flow of data we can use in our project
base_df = generate_base_customers(seed=101, num_customers=5000) # leave seed as static
customer_df = randomly_update_customers(base_df, update_rate=0.05) # customer data now in memory
dt = (datetime.date.today()).strftime('%Y-%m-%d')
customer_df['extract_date'] = dt

# ==============================================================================================
# collect env variables for connection to DuckLake as ETL admin
pg_host = os.getenv('PG_HOST')
pg_user = os.getenv('PG_USER')
pg_password = os.getenv('PG_PASSWORD')

# Start by connecting to an "In-Memory" DuckDB connection, and connecting to DuckLake
# NOTE - build_ducklake.py must have been executed already 
ducklake_conn = duckdb.connect(database=":memory:")
ducklake_conn.execute(f"""
ATTACH 'ducklake:postgres:dbname=ducklake_catalog host={pg_host} user={pg_user} password={pg_password}' AS retail_ducklake;

USE retail_ducklake ;
""")

# create table if not already exists
# add some logic here to check for existing table in ducklake:
if x > 0:
    ducklake_conn.execute("""
    CREATE TABLE IF NOT EXISTS retail_bronze.customer_src_raw
    AS SELECT * FROM customer_df LIMIT 0 ;
    """)
    ducklake_conn.execute("alter table retail_bronze.customer_src_raw SET PARTITIONED BY (extract_date);")


# Write latest extract into ducklake table (wupe any existing extract date first)
ducklake_conn.execute(f"DELETE FROM retail_bronze.customer_src_raw WHERE extract_date = '{dt}' ;")
ducklake_conn.execute("INSERT INTO retail_bronze.customer_src_raw SELECT * FROM customer_df ;")


ducklake_conn.execute("""
USE memory;
DETACH retail_ducklake;       
""")
ducklake_conn.close()

exit()