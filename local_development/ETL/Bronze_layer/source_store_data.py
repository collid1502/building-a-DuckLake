# imports
import sys
import os
import duckdb
import datetime
import pandas as pd
from faker import Faker
import random
from typing import Optional
from tqdm import tqdm


UK_CITIES = [
    "London", "Birmingham", "Manchester", "Glasgow", "Liverpool",
    "Leeds", "Sheffield", "Bristol", "Edinburgh", "Newcastle"
]


def get_stores(
    seed: Optional[int] = 100,
    show_progress: bool = False
) -> pd.DataFrame:
    """Generate a fixed set of 10 fake retail stores using UK city names.

    Args:
        seed (Optional[int]): Random seed for reproducibility
        show_progress (bool): Whether to show a progress bar

    Returns:
        pd.DataFrame: Store metadata
    """
    fake = Faker("en_GB")
    Faker.seed(seed)
    random.seed(seed)

    rows = []
    iterator = tqdm(enumerate(UK_CITIES, start=1), total=10, desc="Generating stores") if show_progress else enumerate(UK_CITIES, start=1)
    for store_id, city in iterator:
        rows.append({
            "store_id": store_id,
            "store_name": f"{city} Store",
            "manager": fake.name(),
            "opened_date": fake.date_between(start_date='-10y', end_date='-1y')
        })
    return pd.DataFrame(rows)


# =============================================================================================================
# collect env variables for connection to DuckLake as ETL admin
def etl():
    """
    Process the ETL stage of loading raw store data to bronze layer of DuckLake.
    Automatically manages connection context to ensure clean closure.
    """
    pg_host = os.getenv('PG_HOST')
    pg_user = os.getenv('PG_USER')
    pg_password = os.getenv('PG_PASSWORD')

    # collect customer_data first (so it's available even if we need to infer schema)
    print("generating fake stores data ...")
    stores_df = get_stores(
        seed=123,
        show_progress=True
    )
    stores_df['extract_date'] = datetime.date.today().strftime("%Y-%m-%d")

    # DuckDB connection with context manager for auto-close
    with duckdb.connect(database=":memory:") as con:
        print("connecting to ducklake ...")
        con.execute(f"""
        ATTACH 'ducklake:postgres:dbname=ducklake_catalog host={pg_host} user={pg_user} password={pg_password}' AS retail_ducklake ;
        USE retail_ducklake ;
        """)

        # create table if not exists (based on schema of df)
        try:
            con.execute("SELECT 1 FROM retail_bronze.stores_src_raw ;")
        except:
            print("Table: `retail_bronze.stores_src_raw` does not yet exist. Creating ...")
            con.register("stores_df", stores_df)  # register pandas DataFrame
            con.execute("""
            CREATE TABLE retail_bronze.stores_src_raw AS SELECT * FROM stores_df LIMIT 0 ;
            ALTER TABLE retail_bronze.stores_src_raw SET PARTITIONED BY (extract_date) ;
            """)
            print("Table: `retail_bronze.stores_src_raw` created")
        
        # execute write of data to table
        print("load stores data to bronze layer")
        dt = (datetime.date.today()).strftime('%Y-%m-%d')
        con.execute(f"""
        DELETE FROM retail_bronze.stores_src_raw WHERE extract_date = '{dt}' ;
        INSERT INTO retail_bronze.stores_src_raw SELECT * FROM stores_df ;
        """)
        print("Data loaded")       

        # clean up the context (optional inside `with`, but for completeness)
        con.execute("USE memory ;")
        con.execute("DETACH retail_ducklake ;")


if __name__ == "__main__":
    print("Running ETL process for BRONZE -- Raw Stores Data ...")
    etl() # process ETL
    print("Data Load to `retail_bronze.stores_src_raw` completed")
    exit()
