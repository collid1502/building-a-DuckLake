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


# Example usage
if __name__ == "__main__":
    df = get_stores(
        seed=123,
        show_progress=True
    )

    print(df.to_string(index=False))

