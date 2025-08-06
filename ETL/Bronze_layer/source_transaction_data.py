# imports
import sys
import os
import duckdb
import pandas as pd
from faker import Faker
from faker.providers import DynamicProvider
import datetime
import random
import uuid
from typing import Optional
from tqdm import tqdm

# Constants
PRODUCT_PRICES = {
    "Laptop": 399.99,
    "Desktop": 599.99,
    "Monitor": 120,
    "Keyboard": 35,
    "Mouse": 8,
    "Docking Station": 70,
    "HDMI Cable": 14.98,
    "Office Chair Premium": 250,
    "Office Chair Standard": 160,
    "Desk": 400,
    "Laptop Bag": 55,
    "Laptop Stand": 12.99,
    "Extension Cable": 4.99,
    "USB Flash Drive 16gb": 3.99,
    "Tablet": 115,
    "Printer": 70,
    "Projector": 300,
    "WiFi Range Extender": 30,
}


def get_transactions(
    start: datetime.date,
    end: datetime.date,
    num_transactions: int = 100_000,
    seed: Optional[int] = None,
    show_progress: bool = False
) -> pd.DataFrame:
    """Generate fake multi-product transaction data with channel and store_id.

    Args:
        start (datetime.date): Start date for transaction timestamps
        end (datetime.date): End date for transaction timestamps
        num_transactions (int): Number of unique transactions to generate
        seed (Optional[int]): Seed value for reproducibility
        show_progress (bool): Whether to show a progress bar

    Returns:
        pd.DataFrame: DataFrame containing line-item transactions
    """
    fake = Faker(locale="en_GB")

    # Seed everything for reproducibility
    if seed is None:
        seed = int(start.strftime("%Y%m%d"))
    fake.seed_instance(seed)
    random.seed(seed)

    # Add dynamic product provider
    product_provider = DynamicProvider(
        provider_name="product",
        elements=list(PRODUCT_PRICES.keys())
    )
    fake.add_provider(product_provider)

    txn_list = []
    iterator = tqdm(range(num_transactions), desc="Generating transactions") if show_progress else range(num_transactions)

    for _ in iterator:
        customer_id = random.randint(10000, 150000)
        transaction_ts = fake.date_time_between(start_date=start, end_date=end)
        transaction_id = str(uuid.uuid4())

        num_items = random.randint(1, 5)
        products = random.sample(list(PRODUCT_PRICES.keys()), k=num_items)

        channel = random.choices(["Online", "In-Store"], weights=[0.7, 0.3])[0]
        store_id = random.randint(1, 10) if channel == "In-Store" else None

        for product in products:
            txn_list.append({
                "transaction_id": transaction_id,
                "customerID": customer_id,
                "transaction_TS": transaction_ts,
                "Product": product,
                "volume": random.randint(1, 6),
                "channel": channel,
                "store_id": store_id
            })

    df = pd.DataFrame(txn_list)
    # Join prices
    prices_df = pd.DataFrame(PRODUCT_PRICES.items(), columns=["Product", "Price"])
    df = df.merge(prices_df, on="Product", how="left")
    # Calculate line-item total
    df["txn_amount"] = df["volume"] * df["Price"]
    return df


# =============================================================================================================
# collect env variables for connection to DuckLake as ETL admin
def etl():
    """
    Process the ETL stage of loading raw transaction data to bronze layer of DuckLake.
    Automatically manages connection context to ensure clean closure.
    """
    pg_host = os.getenv('PG_HOST')
    pg_user = os.getenv('PG_USER')
    pg_password = os.getenv('PG_PASSWORD')

    # collect customer_data first (so it's available even if we need to infer schema)
    print("generating fake transaction data ...")
    today = datetime.date.today()
    tomorrow = today + datetime.timedelta(days=1)
    txns_df = get_transactions(
        start=today,
        end=tomorrow,
        num_transactions=5_000,
        show_progress=True
    )
    txns_df['extract_date'] = datetime.date.today().strftime("%Y-%m-%d")

    # DuckDB connection with context manager for auto-close
    with duckdb.connect(database=":memory:") as con:
        print("connecting to ducklake ...")
        con.execute(f"""
        ATTACH 'ducklake:postgres:dbname=ducklake_catalog host={pg_host} user={pg_user} password={pg_password}' AS retail_ducklake ;
        USE retail_ducklake ;
        """)

        # create table if not exists (based on schema of df)
        try:
            con.execute("SELECT 1 FROM retail_bronze.transactions_src_raw ;")
        except:
            print("Table: `retail_bronze.transactions_src_raw` does not yet exist. Creating ...")
            con.register("txns_df", txns_df)  # register pandas DataFrame
            con.execute("""
            CREATE TABLE retail_bronze.transactions_src_raw AS SELECT * FROM txns_df LIMIT 0 ;
            ALTER TABLE retail_bronze.transactions_src_raw SET PARTITIONED BY (extract_date) ;
            """)
            print("Table: `retail_bronze.transactions_src_raw` created")

        # execute write of data to table
        print("load txn data to bronze layer")
        dt = (datetime.date.today()).strftime('%Y-%m-%d')
        con.execute(f"""
        DELETE FROM retail_bronze.transactions_src_raw WHERE extract_date = '{dt}' ;
        INSERT INTO retail_bronze.transactions_src_raw SELECT * FROM txns_df ;
        """)
        print("Data loaded")

        # clean up the context (optional inside `with`, but for completeness)
        con.execute("USE memory ;")
        con.execute("DETACH retail_ducklake ;")


if __name__ == "__main__":
    print("Running ETL process for BRONZE -- Raw Transaction Data ...")
    etl() # process ETL
    print("Data Load to `retail_bronze.transactions_src_raw` completed")
    exit()
