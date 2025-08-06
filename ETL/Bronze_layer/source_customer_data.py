# imports 
import os
import duckdb
import pandas as pd
import random
from faker import Faker
from faker.providers import DynamicProvider
import datetime
from tqdm import tqdm
import argparse


def generate_base_customers(seed: int = 12345, num_customers: int = 1000) -> pd.DataFrame:
    """Generate consistent base customer data using Faker."""
    fake = Faker('en_GB')
    Faker.seed(seed)
    random.seed(seed)

    # Add dynamic profession provider
    professions_provider = DynamicProvider(
        provider_name="profession",
        elements=[
            "Engineer", "Graphic Designer", "Architect", "Civil engineer", "Software Developer",
            "Laboratory Technician", "Mechanical engineer", "Scientist", "Veterinarian", "Artist",
            "Bricklayer", "Producers and Directors", "Plasterer", "Nurse", "Roofer", "Musician", "Social Worker",
            "Physiotherapist", "Health professional", "Teacher", "Radiographer", "Paramedic", "Physician", "Welder",
            "Archaeologist", "Association football manager", "Technician", "Electrician", "Engineering technician",
            "Accountant", "Painter and decorator", "Librarian", "Private investigator", "Pharmacy Technician",
            "Technology specialist", "Quantity surveyor", "Air traffic controller", "Financial Manager",
            "Official", "Chef", "Plumber", "Aviator", "Broker", "Police officer", "Designer", "Optician",
            "Adviser", "Trader", "Consultant", "Chartered Surveyor", "Pipefitter"
        ]
    )
    fake.add_provider(professions_provider)

    bday_start = datetime.date(1950, 1, 1)
    bday_end = datetime.date(2005, 1, 1)
    joined_start = datetime.date(1990, 1, 1)
    joined_end = datetime.date(2024, 12, 31)

    customers = []
    for cid in tqdm(range(10000, 10000 + num_customers), desc="Generating customers"):
        customers.append({
            'customerID': cid,
            'firstName': fake.first_name(),
            'lastName': fake.last_name(),
            'rewardsMember': fake.boolean(),
            'emailAddress': fake.email(),
            'postcode': fake.postcode(),
            'profession': fake.profession(),
            'dob': fake.date_between(bday_start, bday_end),
            'customerJoined': fake.date_time_between(start_date=joined_start, end_date=joined_end)
        })

    return pd.DataFrame(customers)


def randomly_update_customers(df: pd.DataFrame, update_rate: float = 0.05, seed: int = None) -> pd.DataFrame:
    """Randomly update fields for a small subset of customers."""
    fake = Faker('en_GB')
    if seed is not None:
        Faker.seed(seed)
        random.seed(seed)

    professions_provider = DynamicProvider(
        provider_name="profession",
        elements=[
            "Engineer", "Graphic Designer", "Architect", "Civil engineer", "Software Developer",
            "Laboratory Technician", "Mechanical engineer", "Scientist", "Veterinarian", "Artist",
            "Bricklayer", "Producers and Directors", "Plasterer", "Nurse", "Roofer", "Musician", "Social Worker",
            "Physiotherapist", "Health professional", "Teacher", "Radiographer", "Paramedic", "Physician", "Welder",
            "Archaeologist", "Association football manager", "Technician", "Electrician", "Engineering technician",
            "Accountant", "Painter and decorator", "Librarian", "Private investigator", "Pharmacy Technician",
            "Technology specialist", "Quantity surveyor", "Air traffic controller", "Financial Manager",
            "Official", "Chef", "Plumber", "Aviator", "Broker", "Police officer", "Designer", "Optician",
            "Adviser", "Trader", "Consultant", "Chartered Surveyor", "Pipefitter"
        ]
    )
    fake.add_provider(professions_provider)

    df_updated = df.copy()
    num_to_update = int(len(df) * update_rate)
    update_ids = random.sample(list(df['customerID']), num_to_update)

    for cid in tqdm(update_ids, desc="Applying customer updates"):
        idx = df_updated[df_updated['customerID'] == cid].index[0]
        df_updated.at[idx, 'profession'] = fake.profession()
        df_updated.at[idx, 'postcode'] = fake.postcode()
        df_updated.at[idx, 'emailAddress'] = fake.email()

    return df_updated


# ============================================================================================================
# for this section, i'm going to mock some data as if it came from some database or API
# so that we have a flow of data we can use in our project
def get_raw_customer_data(extract_date: str = None) -> pd.DataFrame:
    base_df = generate_base_customers(seed=101, num_customers=10000) # leave seed as static
    customer_df = randomly_update_customers(base_df, update_rate=0.05) # customer data now in memory
    dt = extract_date or (datetime.date.today()).strftime('%Y-%m-%d')
    customer_df['extract_date'] = dt
    return customer_df


# =============================================================================================================
# collect env variables for connection to DuckLake as ETL admin
def etl(extract_date: str = None):
    """
    Process the ETL stage of loading raw customer data to bronze layer of DuckLake.
    Automatically manages connection context to ensure clean closure.
    """
    pg_host = os.getenv('PG_HOST')
    pg_user = os.getenv('PG_USER')
    pg_password = os.getenv('PG_PASSWORD')

    # collect customer_data first (so it's available even if we need to infer schema)
    print("generating fake customer data ...")
    customer_df = get_raw_customer_data(extract_date=extract_date)

    # DuckDB connection with context manager for auto-close
    with duckdb.connect(database=":memory:") as con:

        con = duckdb.connect(database=":memory:")
        print("connecting to ducklake ...")
        con.execute(f"""
        ATTACH 'ducklake:postgres:dbname=ducklake_catalog host={pg_host} user={pg_user} password={pg_password}' AS retail_ducklake ;
        USE retail_ducklake ;
        """)

        # create table if not exists (based on schema of customer_df)
        con.register("customer_df", customer_df)  # register pandas DataFrame
        con.execute("""CREATE TABLE IF NOT EXISTS retail_bronze.customer_src_raw AS SELECT * FROM customer_df LIMIT 0""")
        print("IF Table: `retail_bronze.customer_src_raw` does not yet exist. Creating ...")
        print("Table: `retail_bronze.customer_src_raw` created")
        
        # execute write of data to table
        print("load customer data to bronze layer")
        dt = (datetime.date.today()).strftime('%Y-%m-%d')
        con.execute(f"""
        DELETE FROM retail_bronze.customer_src_raw WHERE extract_date = '{dt}' ;
        INSERT INTO retail_bronze.customer_src_raw SELECT * FROM customer_df ;
        """)
        print("Data loaded")

        # clean up the context (optional inside `with`, but for completeness)
        con.execute("USE memory ;")
        con.execute("DETACH retail_ducklake ;")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run ETL process for raw customer data")
    parser.add_argument("--extract-date", type=str, help="Optional extract date in YYYY-MM-DD format")
    args = parser.parse_args()
    extract_date = args.extract_date

    print("Running ETL process for BRONZE -- Raw Customer Data ...")
    etl(extract_date=extract_date) # process ETL
    print("Data Load to `retail_bronze.customer_src_raw` completed")
