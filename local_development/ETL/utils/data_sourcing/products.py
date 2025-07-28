# imports
import pandas as pd
from faker import Faker
import datetime
import random
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


def get_product_catalog(
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
    iterator = tqdm(enumerate(product_names, start=base_product_id), total=len(product_names), desc="Generating product catalog") if show_progress else enumerate(product_names, start=base_product_id)

    for product_id, product_name in iterator:
        rows.append({
            "product_id": product_id,
            "product_name": product_name,
            "category": category_map.get(product_name, "General"),
            "price": product_prices[product_name],
            "launch_date": fake.date_between(start_date='-5y', end_date='today')
        })

    return pd.DataFrame(rows)


# Example usage
if __name__ == "__main__":
    df = get_product_catalog(show_progress=True)
    print(df.head(20))
