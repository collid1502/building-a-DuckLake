# imports
import typing as t
import pandas as pd
import random
from faker import Faker
from faker.providers import DynamicProvider
import datetime

from sqlmesh import ExecutionContext, model


@model(
    "retail_bronze.src_customers_raw",
    owner="collid",
    cron="@daily", # note - this is for `state` purposes, so everytime sqlmesh run is executed, it knows it doesn;t need to re-run this model yet
    dialect="duckdb",
    kind="INCREMENTAL_BY_PARTITION",
    columns={
        "customerid": "INT",
        "firstName": "TEXT",
        "lastName": "TEXT",
        "rewardsMember": "BOOLEAN",
        "emailAddress": "TEXT",
        "postcode": "TEXT",
        "profession": "TEXT",
        "dob": "DATE",
        "customerJoined": "TIMESTAMP",
        "extract_date": "TEXT"
    },
    partitioned_by="extract_date",
    physical_schema_override="retail_bronze"
)
def execute(
    context: ExecutionContext,
    **kwargs: t.Any
) -> pd.DataFrame:
    # logic for sourcing raw customer data via Python Model into bronze Layer

    # create some functions to mock data, as if it was coming from some source / API
    def generate_base_customers(seed: int = 12345, num_customers: int = 1000) -> pd.DataFrame:
        """Generate consistent base customer data using Faker."""
        fake = Faker('en_GB')
        Faker.seed(seed)
        random.seed(seed)

        # Add dynamic profession provider
        professions_provider = DynamicProvider(
            provider_name="profession",
            elements=[
                "Engineer","Graphic Designer","Architect","Civil engineer","Software Developer"
                ,"Laboratory Technician","Mechanical engineer","Scientist","Veterinarian","Artist"
                ,"Bricklayer","Producers and Directors","Plasterer","Nurse","Roofer","Musician","Social Worker"
                ,"Physiotherapist","Health professional","Teacher","Radiographer","Paramedic","Physician","Welder"
                ,"Archaeologist","Association football manager","Technician","Electrician","Engineering technician"
                ,"Accountant","Painter and decorator","Librarian","Private investigator","Pharmacy Technician"
                ,"Technology specialist","Quantity surveyor","Air traffic controller","Financial Manager"
                ,"Official","Chef","Plumber","Aviator","Broker","Police officer","Designer","Optician"
                ,"Adviser","Trader","Consultant","Chartered Surveyor","Pipefitter"
            ]
        )
        fake.add_provider(professions_provider)

        bday_start = datetime.date(1950, 1, 1)
        bday_end = datetime.date(2005, 1, 1)
        joined_start = datetime.date(1990, 1, 1)
        joined_end = datetime.date(2024, 12, 31)

        customers = []
        for cid in range(10000, 10000 + num_customers):
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


    def randomly_update_customers(df: pd.DataFrame, update_rate: float = 0.05, seed: int = None) -> pd.DataFrame: # type: ignore
        """Randomly update fields for a small subset of customers."""
        fake = Faker('en_GB')
        if seed is not None:
            Faker.seed(seed)
            random.seed(seed)

        # Same provider for consistency
        professions_provider = DynamicProvider(
            provider_name="profession",
            elements=[
                "Engineer","Graphic Designer","Architect","Civil engineer","Software Developer"
                ,"Laboratory Technician","Mechanical engineer","Scientist","Veterinarian","Artist"
                ,"Bricklayer","Producers and Directors","Plasterer","Nurse","Roofer","Musician","Social Worker"
                ,"Physiotherapist","Health professional","Teacher","Radiographer","Paramedic","Physician","Welder"
                ,"Archaeologist","Association football manager","Technician","Electrician","Engineering technician"
                ,"Accountant","Painter and decorator","Librarian","Private investigator","Pharmacy Technician"
                ,"Technology specialist","Quantity surveyor","Air traffic controller","Financial Manager"
                ,"Official","Chef","Plumber","Aviator","Broker","Police officer","Designer","Optician"
                ,"Adviser","Trader","Consultant","Chartered Surveyor","Pipefitter"
            ]
        )
        fake.add_provider(professions_provider)

        df_updated = df.copy()
        num_to_update = int(len(df) * update_rate)
        update_ids = random.sample(list(df['customerID']), num_to_update)

        for cid in update_ids:
            idx = df_updated[df_updated['customerID'] == cid].index[0]
            df_updated.at[idx, 'profession'] = fake.profession()
            df_updated.at[idx, 'postcode'] = fake.postcode()
            df_updated.at[idx, 'emailAddress'] = fake.email()

        return df_updated
    
    # call functions above to return generated pandas dataframe
    base_df = generate_base_customers(seed=101, num_customers=20000) # leave seed as static
    customer_df = randomly_update_customers(base_df, update_rate=0.05) # customer data now in memory
    dt = (datetime.date.today()).strftime('%Y-%m-%d')
    customer_df['extract_date'] = dt

    return customer_df
