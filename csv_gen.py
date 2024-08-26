import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
import random

# Constants
NUM_SHOPS = 10
NUM_RECORDS = 1000
START_DATE = datetime(2024, 8, 1)
STATUS_OPTIONS = ['active', 'inactive']
TIMEZONE_OPTIONS = [
    'America/New_York', 'America/Los_Angeles', 'America/Chicago',
    'America/Denver', 'Europe/London', 'Europe/Berlin', 'Asia/Tokyo',
    'Australia/Sydney', 'Asia/Singapore', 'Asia/Kolkata'
]

# Generate store_status.csv
store_status_records = []
for store_id in range(1, NUM_SHOPS + 1):
    for _ in range(NUM_RECORDS // NUM_SHOPS):
        timestamp_utc = START_DATE + timedelta(minutes=random.randint(0, 60*24*30))
        status = random.choice(STATUS_OPTIONS)
        store_status_records.append({
            "store_id": store_id,
            "timestamp_utc": timestamp_utc.strftime('%Y-%m-%dT%H:%M:%SZ'),
            "status": status
        })

store_status_df = pd.DataFrame(store_status_records)
store_status_df.to_csv("data/store_status.csv", index=False)

# Generate business_hours.csv
business_hours_records = []
for store_id in range(1, NUM_SHOPS + 1):
    business_hours_records.append({
        "store_id": store_id,
        "day_of_week": random.randint(0, 6),
        "start_time_local": "08:00:00",
        "end_time_local": "22:00:00"
    })

business_hours_df = pd.DataFrame(business_hours_records)
business_hours_df.to_csv("data/business_hours.csv", index=False)

# Generate store_timezones.csv
store_timezones_records = []
for store_id in range(1, NUM_SHOPS + 1):
    store_timezones_records.append({
        "store_id": store_id,
        "timezone_str": random.choice(TIMEZONE_OPTIONS)
    })

store_timezones_df = pd.DataFrame(store_timezones_records)
store_timezones_df.to_csv("data/store_timezones.csv", index=False)

print("CSV files generated successfully.")
