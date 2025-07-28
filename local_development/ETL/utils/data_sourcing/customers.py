# imports
import pandas as pd
import random
from faker import Faker
from faker.providers import DynamicProvider
import datetime
from tqdm import tqdm


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


if __name__ == "__main__":
    base_df = generate_base_customers(seed=101, num_customers=10000)
    updated_df = randomly_update_customers(base_df, update_rate=0.05)

    # Display a few updated customers
    pd.set_option('display.max_colwidth', None)
    diffs = updated_df.compare(base_df)
    print("Updated records:\n", diffs.head(10))
