import csv
import random
from datetime import datetime, timedelta

# Create sample customers
customers_data = []
segments = ["Enterprise", "Premium", "Regular"]
countries = ["USA", "UK", "Canada", "Germany", "France", "Japan", "Australia"]
cities = {
    "USA": ["New York", "Los Angeles", "Chicago", "Houston"],
    "UK": ["London", "Manchester", "Birmingham"],
    "Canada": ["Toronto", "Vancouver", "Montreal"],
    "Germany": ["Berlin", "Munich", "Hamburg"],
    "France": ["Paris", "Lyon", "Marseille"],
    "Japan": ["Tokyo", "Osaka", "Kyoto"],
    "Australia": ["Sydney", "Melbourne", "Brisbane"],
}
for i in range(1, 1001):
    country = random.choice(countries)
    city = random.choice(cities[country])
    first_name = f"First{i}"
    last_name = f"Last{i}"
    customers_data.append(
        [
            i,  # customerNumber
            f"{first_name} {last_name}",  # customerName
            first_name,  # firstName
            last_name,  # lastName
            f"+1-555-{i:04d}",  # phone
            f"{i} Main Street",  # addressLine1
            city,  # city
            "",  # state
            country,  # country
            random.randint(10000, 100000),  # creditLimit
            random.choice(segments),  # segment
        ]
    )
# Write to CSV
with open("spark-data/ecommerce/customers.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(
        [
            "customerNumber",
            "customerName",
            "firstName",
            "lastName",
            "phone",
            "addressLine1",
            "city",
            "state",
            "country",
            "creditLimit",
            "segment",
        ]
    )
    writer.writerows(customers_data)
print("Sample data created successfully!")
print(f"Created {len(customers_data)} customer records")
