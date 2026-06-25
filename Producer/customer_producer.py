import json
import random
import time
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

TOPIC_NAME = "kyc.customer.profile.raw.v1"
BOOTSTRAP_SERVERS = "localhost:9092"

USER_JOBS = [
    "Software Engineer",
    "Data Analyst",
    "Business Owner",
    "Doctor",
    "Lawyer",
    "Real Estate Agent",
    "Accountant",
    "Student",
    "Consultant",
    "Cashier"
]

USER_COUNTRIES = [
    "CANADA",
    "UNITED_STATES",
    "CHINA",
    "INDIA",
    "UNITED_KINGDOM",
    "MEXICO",
    "BRAZIL",
    "IRAN",
    "RUSSIA",
    "UNITED_ARAB_EMIRATES"
]

SOURCE_SYSTEMS = [
    "TD_BANK",
    "RBC",
    "HSBC",
    "WEALTHSIMPLE",
    "AMERICAN_BANK"
]

ACCOUNT_CHANNELS = [
    "ONLINE",
    "BRANCH"
]

ACCOUNT_TYPES = [
    "CHECKING",
    "SAVINGS",
    "CREDIT_CARD",
    "INVESTMENT",
    "MORTGAGE",
    "LOAN",
    "BUSINESS_ACCOUNT"
]


def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def generate_customer_event():
    user_id = f"USR{random.randint(100000, 999999)}"

    account_types = random.sample(
        ACCOUNT_TYPES,
        random.randint(1, 3)
    )

    event = {
        "event_id": f"evt_{uuid.uuid4()}",
        "event_type": "CUSTOMER_PROFILE_CREATED",
        "event_time": utc_now(),
        "source_system": random.choice(SOURCE_SYSTEMS),
        "schema_version": "1.0",
        "payload": {
            "user_id": user_id,
            "user_address": fake.address().replace("\n", ", "),
            "user_job": random.choice(USER_JOBS),
            "user_account_types": account_types,
            "user_jurisdiction": random.choice(["CA-ON", "CA-BC", "US-NY", "US-CA", "GB-LDN"]),
            "risk_flag": random.choice(["Y", "N", "N", "N"]),
            "pep_flag": random.choice(["Y", "N", "N", "N"]),
            "user_country": random.choice(USER_COUNTRIES),
            "user_account_channel": random.choice(ACCOUNT_CHANNELS),
            "user_last_review_time": fake.date_time_between(
                start_date="-3y",
                end_date="now",
                tzinfo=timezone.utc
            ).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "user_accounts": [
                {
                    "account_id": f"ACC{random.randint(100000, 999999)}",
                    "account_type": account_type,
                    "account_status": "ACTIVE",
                    "currency": random.choice(["CAD", "USD", "GBP"]),
                    "open_date": str(fake.date_between(start_date="-5y", end_date="today"))
                }
                for account_type in account_types
            ]
        }
    }

    return user_id, event


def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=3
    )

    print(f"Producing messages to topic: {TOPIC_NAME}")

    while True:
        key, event = generate_customer_event()

        producer.send(
            TOPIC_NAME,
            key=key,
            value=event
        )

        producer.flush()

        print(f"Sent event for user_id={key}")

        time.sleep(2)


if __name__ == "__main__":
    main()