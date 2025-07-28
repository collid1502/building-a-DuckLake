# imports
import os
import duckdb
import pandas as pd
pd.set_option('display.max_colwidth', None)


# connect to Ducklake 
pg_host = os.getenv('PG_HOST')
pg_user = os.getenv('PG_USER')
pg_password = os.getenv('PG_PASSWORD')

# DuckDB connection with context manager for auto-close
con = duckdb.connect(database=":memory:")

con.execute(f"""
ATTACH 'ducklake:postgres:dbname=ducklake_catalog host={pg_host} user={pg_user} password={pg_password}' AS retail_ducklake ;
USE retail_ducklake ;
""")

# test RAW customer data
cust = con.execute("SELECT * FROM retail_bronze.customer_src_raw LIMIT 10").fetch_df()
cust.head(10)

# test RAW transaction data
txns = con.execute("SELECT * FROM retail_bronze.transactions_src_raw LIMIT 10").fetch_df()
txns.head(10)

# test stores
strs = con.execute("SELECT * FROM retail_bronze.stores_src_raw LIMIT 10").fetch_df()
strs.head(10)

# products
prd = con.execute("SELECT * FROM retail_bronze.products_src_raw LIMIT 20").fetch_df()
prd.head(20)

# close connection
con.close()
exit()
