from datetime import datetime,timedelta
import pytz
import pandas as pd
from fastapi import HTTPException
from app.db import db


import pandas as pd
from datetime import datetime, timedelta
from fastapi import HTTPException
from bson import ObjectId

async def generate_filtered_data_table(store_id: int):
    try:
        # Load business hours for the store
        business_hours = pd.read_csv("data/business_hours.csv")
        
        # Fetch the latest polling data for the store from latest_polling_data
        latest_polling_record = db.latest_polling_data.find_one({"store_id": store_id})

        if not latest_polling_record:
            raise HTTPException(status_code=404, detail="No latest polling data found for the store.")

        # Extract the latest timestamp local time and convert to datetime object
        latest_time_str = latest_polling_record['timestamp_local']
        latest_timestamp_local = datetime.strptime(latest_time_str, '%H:%M:%S').time()
        one_hour_ago = (datetime.combine(datetime.today(), latest_timestamp_local) - timedelta(hours=1)).time()

        print("Latest", one_hour_ago)

        # Fetch all polling data for the store from all_polling_data within the last hour
        data_cursor = db.all_polling_data.find({
            "store_id": store_id,
            "timestamp_local": {"$gte": one_hour_ago.strftime('%H:%M:%S'), "$lte": latest_timestamp_local.strftime('%H:%M:%S')}
        }).sort("timestamp_local", 1)

        # Convert cursor to list and exclude ObjectId for debugging/logging
        polling_data = [{k: v for k, v in record.items() if k != '_id'} for record in data_cursor]
        print("poll", polling_data)
        
        if not polling_data:
            raise HTTPException(status_code=404, detail="No polling data found within the last hour.")

        # Get the business hours for the specific day of the week
        print("buz", business_hours)
        print("buz", latest_polling_record)
        store_business_hours = business_hours.loc[
            (business_hours['store_id'] == store_id) &
            (business_hours['day_of_week'] == latest_polling_record['day_of_week'])
        ]

        if store_business_hours.empty:
            raise HTTPException(status_code=404, detail="No business hours found for this store on the specified day.")

        # Extract business hours start and end time
        business_start = datetime.strptime(store_business_hours.iloc[0]['start_time_local'], '%H:%M:%S').time()
        business_end = datetime.strptime(store_business_hours.iloc[0]['end_time_local'], '%H:%M:%S').time()

        # Filter polling data within business hours
        polling_data_in_business_hours = [
            record for record in polling_data
            if business_start <= datetime.strptime(record['timestamp_local'], '%H:%M:%S').time() <= business_end
        ]

        if not polling_data_in_business_hours:
            raise HTTPException(status_code=404, detail="No polling data within business hours for the last hour.")

        return {
            "store_id": store_id,
            "filtered_data_table": polling_data_in_business_hours
        }

    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")


async def process_all_polling_data():
    try:
        # Load CSV files
        polling_data = pd.read_csv("data/store_status.csv")
        timezones = pd.read_csv("data/store_timezones.csv")

        # Convert timestamps
        polling_data['timestamp_utc'] = pd.to_datetime(polling_data['timestamp_utc'])

        # Function to convert UTC to local time and extract the required fields
        def convert_utc_to_local_time(utc_time, timezone_str):
            local_tz = pytz.timezone(timezone_str)
            local_time = utc_time.tz_convert(local_tz)
            return local_time.strftime('%H:%M:%S'), local_time.weekday()  # Return time up to seconds and day of week

        # Process data and add local time and day of week
        def process_row(row):
            local_time, day_of_week = convert_utc_to_local_time(
                row['timestamp_utc'],
                timezones.loc[timezones['store_id'] == row['store_id'], 'timezone_str'].values[0]
            )
            return {
                "store_id": row['store_id'],
                "timestamp_local": local_time,  # Only time up to seconds
                "day_of_week": day_of_week,  # Day of the week (0=Monday, 6=Sunday)
                "status": row['status']
            }

        # Convert all polling data
        processed_data = polling_data.apply(process_row, axis=1)

        # Insert data into MongoDB
        for data_to_insert in processed_data:
            result = db.all_polling_data.insert_one(data_to_insert)
            if not result.inserted_id:
                raise HTTPException(status_code=500, detail="Failed to insert data")

        return {"message": "All data processed and stored in all_polling_data collection in MongoDB"}
    
    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

async def process_latest_polling_data():
    try:
        # Load CSV files
        polling_data = pd.read_csv("data/store_status.csv")
        timezones = pd.read_csv("data/store_timezones.csv")

        # Convert timestamps
        polling_data['timestamp_utc'] = pd.to_datetime(polling_data['timestamp_utc'])

        # Ensure only the latest timestamp is kept for each store
        latest_polling_data = polling_data.sort_values('timestamp_utc').groupby('store_id').tail(1)

        # Function to convert UTC to local time and extract the required fields
        def convert_utc_to_local_time(utc_time, timezone_str):
            local_tz = pytz.timezone(timezone_str)
            local_time = utc_time.tz_convert(local_tz)
            return local_time.strftime('%H:%M:%S'), local_time.weekday()  # Return time up to seconds and day of week

        # Process data and add local time and day of week
        def process_row(row):
            local_time, day_of_week = convert_utc_to_local_time(
                row['timestamp_utc'],
                timezones.loc[timezones['store_id'] == row['store_id'], 'timezone_str'].values[0]
            )
            return {
                "store_id": row['store_id'],
                "timestamp_local": local_time,  # Only time up to seconds
                "day_of_week": day_of_week,  # Day of the week (0=Monday, 6=Sunday)
                "status": row['status']
            }

        # Convert only the latest polling data
        processed_data = latest_polling_data.apply(process_row, axis=1)

        # Insert data into MongoDB
        for data_to_insert in processed_data:
            result = db.latest_polling_data.insert_one(data_to_insert)
            if not result.inserted_id:
                raise HTTPException(status_code=500, detail="Failed to insert data")

        return {"message": "Latest data processed and stored in latest_polling_data collection in MongoDB"}
    
    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
