"""
Generates a sample unclean dataset for Part 2 of the DE project.
Run this script once to create the test CSV files.

Dataset: Employee HR Records
- 12 columns, 200 rows
- Intentional issues: nulls, duplicates, wrong formats, negative salaries, bad emails
"""

import os
import random
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

random.seed(42)
np.random.seed(42)

DEPARTMENTS = ["Engineering", "Marketing", "Sales", "HR", "Finance", "Operations", None]
GENDERS = ["M", "F", "male", "FEMALE", "m", "f", None]  # intentionally messy
COUNTRIES = ["Belgium", "Netherlands", "Germany", "France", "Spain", None]
CONTRACT_TYPES = ["Full-time", "Part-time", "Freelance", "full time", "PART TIME", None]


def random_date(start_year=2015, end_year=2024):
    """Generates a random datetime between the given years."""
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))


def random_email(name):
    """Generates a messy email string for testing purposes."""
    domains = ["gmail.com", "company.com", "outlook.com", "notanemail", "", None]
    domain = random.choice(domains)
    if domain is None:
        return None
    if domain == "":
        return name.lower().replace(" ", ".")
    return f"{name.lower().replace(' ', '.')[:10]}@{domain}"


N = 200
names = [f"Employee_{i}" for i in range(1, N + 1)]

data = {
    "employee_id": list(range(1001, 1001 + N)),
    "full_name": names,
    "email": [random_email(name) for name in names],
    "department": [random.choice(DEPARTMENTS) for _ in range(N)],
    "gender": [random.choice(GENDERS) for _ in range(N)],
    "country": [random.choice(COUNTRIES) for _ in range(N)],
    "salary_eur": [
        round(random.uniform(-500, 12000), 2) for _ in range(N)
    ],  # some negatives
    "hire_date": [
        random_date().strftime("%Y-%m-%d") if random.random() > 0.05 else "not-a-date"
        for _ in range(N)
    ],
    "years_experience": [random.randint(-2, 35) for _ in range(N)],  # some negatives
    "performance_score": [
        random.choice([1, 2, 3, 4, 5, None, 99]) for _ in range(N)
    ],  # 99 is invalid
    "contract_type": [random.choice(CONTRACT_TYPES) for _ in range(N)],
    "weekly_hours": [
        random.choice([0, 20, 32, 38, 40, 60, None, -5]) for _ in range(N)
    ],
}

df = pd.DataFrame(data)

# Add ~20 duplicate rows
duplicates = df.sample(20, replace=True)
df = pd.concat([df, duplicates], ignore_index=True)
df = df.sample(frac=1, random_state=42).reset_index(drop=True)  # shuffle

output_dir = os.path.join(os.path.dirname(__file__), "sample_data")
os.makedirs(output_dir, exist_ok=True)

output_path = os.path.join(output_dir, "employee_records_unclean.csv")
df.to_csv(output_path, index=False)
print(f"Generated dataset: {output_path}")
print(f"Shape: {df.shape}")
print(df.head())
