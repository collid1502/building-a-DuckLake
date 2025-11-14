# imports
import pandas as pd
import random
from faker import Faker
from faker.providers import DynamicProvider
import datetime
from tqdm import tqdm
from typing import Optional
import uuid



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


def randomly_update_customers(df: pd.DataFrame, update_rate: float = 0.05, seed: int | None = None) -> pd.DataFrame:
    """Create some random updates to the seeded customer data we have generated"""
    fake = Faker('en_GB')
    if seed is not None:
        fake.seed_instance(seed)      # seed only this Faker instance

    # use a local RNG so we don't touch global state
    rng = random.Random(seed) if seed is not None else random.Random()

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
    update_ids = rng.sample(list(df['customerID']), num_to_update)

    for cid in tqdm(update_ids, desc="Applying customer updates"):
        idx = df_updated[df_updated['customerID'] == cid].index[0]
        df_updated.at[idx, 'profession'] = fake.profession()
        df_updated.at[idx, 'postcode'] = fake.postcode()
        df_updated.at[idx, 'emailAddress'] = fake.email()

    return df_updated


def get_raw_customer_data(extract_date: str, show_progress: bool = False) -> pd.DataFrame: # type: ignore
    base_df = generate_base_customers(seed=101, num_customers=10000) # leave seed as static
    customer_df = randomly_update_customers(base_df, update_rate=0.05) # customer data now in memory
    dt = extract_date
    customer_df['extract_date'] = dt
    return customer_df


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


def get_product_catalog(
    extract_date: str,
    product_prices: dict = PRODUCT_PRICES,
    seed: Optional[int] = 123,
    base_product_id: int = 1000,
    show_progress: bool = False
) -> pd.DataFrame:
    """Generate a product catalog from a product price map.

    Args:
        product_prices (dict): Mapping of product name to price
        seed (Optional[int]): Seed for reproducibility
        base_product_id (int): Starting product ID number
        show_progress (bool): Whether to show progress bar

    Returns:
        pd.DataFrame: Product catalog
    """
    fake = Faker()
    Faker.seed(seed)
    random.seed(seed)

    # Category assignment map
    category_map = {
        "Laptop": "Electronics",
        "Desktop": "Electronics",
        "Monitor": "Electronics",
        "Keyboard": "Accessories",
        "Mouse": "Accessories",
        "Docking Station": "Accessories",
        "HDMI Cable": "Accessories",
        "Office Chair Premium": "Furniture",
        "Office Chair Standard": "Furniture",
        "Desk": "Furniture",
        "Laptop Bag": "Accessories",
        "Laptop Stand": "Accessories",
        "Extension Cable": "Accessories",
        "USB Flash Drive 16gb": "Storage",
        "Tablet": "Electronics",
        "Printer": "Electronics",
        "Projector": "Electronics",
        "WiFi Range Extender": "Networking"
    }

    rows = []
    product_names = list(product_prices.keys())
    iterator = tqdm(
        enumerate(product_names, start=base_product_id),
        total=len(product_names),
        desc="Generating product catalog"
    ) if show_progress else enumerate(product_names, start=base_product_id)

    for product_id, product_name in iterator:
        rows.append({
            "product_id": product_id,
            "product_name": product_name,
            "category": category_map.get(product_name, "General"),
            "price": product_prices[product_name],
            "launch_date": fake.date_between(start_date='-5y', end_date='today')
        })
    df = pd.DataFrame(rows)
    df['extract_date'] = extract_date
    return df


UK_CITIES = [
    "London", "Birmingham", "Manchester", "Glasgow", "Liverpool",
    "Leeds", "Sheffield", "Bristol", "Edinburgh", "Newcastle"
]


def get_stores(
    extract_date: str,
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
    iterator = tqdm(
        enumerate(UK_CITIES, start=1),
        total=10, 
        desc="Generating stores"
    ) if show_progress else enumerate(UK_CITIES, start=1)
    for store_id, city in iterator:
        rows.append({
            "store_id": store_id,
            "store_name": f"{city} Store",
            "manager": fake.name(),
            "opened_date": fake.date_between(start_date='-10y', end_date='-1y')
        })
    df = pd.DataFrame(rows)
    df['extract_date'] = extract_date
    return df


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
    extract_date: str,
    num_transactions: int = 100_000,
    seed: Optional[int] = None,
    show_progress: bool = False
) -> pd.DataFrame:
    """Generate fake multi-product transaction data with channel and store_id.

    Args:
        extract_date (str): Date for the extract of transaction data
        num_transactions (int): Number of unique transactions to generate
        seed (Optional[int]): Seed value for reproducibility
        show_progress (bool): Whether to show a progress bar

    Returns:
        pd.DataFrame: DataFrame containing line-item transactions
    """
    # Parse extract-date into a date object
    end = datetime.datetime.strptime(extract_date, "%Y-%m-%d").date()
    start = end - datetime.timedelta(days=1)
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
    df["extract_date"] = extract_date
    return df
