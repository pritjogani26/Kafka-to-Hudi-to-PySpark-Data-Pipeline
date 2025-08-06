# pip install faker tqdm

import json
import uuid
import random
from faker import Faker
from datetime import datetime

from state_city import INDIAN_CITY_STATE, USA_CITY_STATE

# Country to locale mapping
COUNTRY_LOCALES = {
    "India": "en_IN",
    # "Australia": "en_AU",
    "USA": "en_US"
    # "UK": "en_GB"
}

def generate_user_record(country):
    fake = Faker(COUNTRY_LOCALES[country])
    if country == "India":
        city = random.choice(list(INDIAN_CITY_STATE.keys()))
        state = INDIAN_CITY_STATE[city]
    elif country == "USA":
        city = random.choice(list(USA_CITY_STATE.keys()))
        state = USA_CITY_STATE[city]
    return {
        "id": str(uuid.uuid4()),
        "firstname": fake.first_name(),
        "lastname": fake.last_name(),
        "email": fake.email(),
        "phone": fake.phone_number(),
        "dob": fake.date_of_birth(minimum_age=20, maximum_age=30).strftime("%Y-%m-%d"),
        "address": fake.address().replace("\n", ", "),
        "city": city,
        "state": state,
        "zipcode": fake.postcode(),
        "country": country
    }

def generate_jsonl_data(file_path, total_records, log_every=100000):
    with open(file_path, "w", encoding="utf-8") as f:
        for i in range(1, total_records + 1):
            country = random.choice(list(COUNTRY_LOCALES.keys()))
            record = generate_user_record(country)
            f.write(json.dumps(record) + "\n")
            if i % log_every == 0:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Generated: {i} records")
    print(f"\nDone! {total_records} records saved to {file_path}")

def main():
    try:
        n = int(input("Enter number of records to generate: "))
        if n <= 0:
            print("Please enter a number greater than 0.")
            return
    except ValueError:
        print("Invalid input. Please enter an integer.")
        return

    output_path = "random_users.jsonl"
    generate_jsonl_data(output_path, n)

if __name__ == "__main__":
    main()
