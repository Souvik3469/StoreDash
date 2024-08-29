from datetime import datetime,timedelta
import pytz
import pandas as pd
from fastapi import HTTPException
from app.db import db
from bson import ObjectId
import random
import string



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

async def generate_filtered_data_table_last_hour(store_id: int):
    try:
        # Load business hours for the store
        business_hours = pd.read_csv("data/business_hours.csv")
        
        # Fetch the latest polling data for the store
        latest_polling_record = db.latest_polling_data.find_one({"store_id": store_id})

        # If no latest polling record is found, return an empty result
        if not latest_polling_record:
            return {
                "store_id": store_id,
                "filtered_data_table": []
            }

        # Extract the latest timestamp local time and convert to datetime object
        latest_time_str = latest_polling_record['timestamp_local']
        latest_day_of_week = int(latest_polling_record['day_of_week'])
        latest_timestamp_local = datetime.strptime(latest_time_str, '%H:%M:%S').time()
        one_hour_ago = (datetime.combine(datetime.today(), latest_timestamp_local) - timedelta(hours=1)).time()

        # Fetch all polling data for the store from all_polling_data within the last hour and specific day
        data_cursor = db.all_polling_data.find({
            "store_id": store_id,
            "day_of_week": latest_day_of_week,
            "timestamp_local": {"$gte": one_hour_ago.strftime('%H:%M:%S'), "$lte": latest_timestamp_local.strftime('%H:%M:%S')}
        }).sort("timestamp_local", 1)

        # Convert cursor to list and exclude ObjectId for debugging/logging
        polling_data = [{k: v for k, v in record.items() if k != '_id'} for record in data_cursor]
        
        # If no polling data found within the last hour, return an empty result
        if not polling_data:
            return {
                "store_id": store_id,
                "filtered_data_table": []
            }

        # Get the business hours for the specific day of the week
        store_business_hours = business_hours.loc[
            (business_hours['store_id'] == store_id) &
            (business_hours['day_of_week'] == latest_day_of_week)
        ]
        
        # If no business hours found for this store on the specified day, return an empty result
        if store_business_hours.empty:
            return {
                "store_id": store_id,
                "filtered_data_table": []
            }

        # Extract business hours start and end time
        business_start = datetime.strptime(store_business_hours.iloc[0]['start_time_local'], '%H:%M:%S').time()
        business_end = datetime.strptime(store_business_hours.iloc[0]['end_time_local'], '%H:%M:%S').time()

        # Filter polling data within business hours
        polling_data_in_business_hours = [
            record for record in polling_data
            if business_start <= datetime.strptime(record['timestamp_local'], '%H:%M:%S').time() <= business_end
        ]

        # Prepare the records to be inserted into MongoDB
        last_hour_records = [
            {
                "store_id": store_id,
                "day_of_week": latest_day_of_week,
                "timestamp_local": record['timestamp_local'],
                "status": record.get('status', 'unknown')  # Assuming 'status' field is present; default to 'unknown'
            }
            for record in polling_data_in_business_hours
        ]

        # Store only records that meet all conditions in MongoDB
        if last_hour_records:
            db.last_hour_records.insert_many(last_hour_records)

        return {
            "store_id": store_id,
            "filtered_data_table": polling_data_in_business_hours
        }

    except Exception as e:
        # Print error message and raise HTTPException with a generic internal server error
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

async def generate_filtered_data_table_last_day(store_id: int):
    try:
        # Load business hours for the store
        business_hours = pd.read_csv("data/business_hours.csv")
        # print("Loaded business hours:\n", business_hours)
        
        # Fetch the latest polling data for the store
        latest_polling_record = db.latest_polling_data.find_one({"store_id": store_id})

        # If no latest polling record is found, return an empty result
        if not latest_polling_record:
            # print(f"No latest polling record found for store_id: {store_id}")
            return {
                "store_id": store_id,
                "filtered_data_table": []
            }

        # Extract the latest timestamp local time and convert to datetime object
        latest_time_str = latest_polling_record['timestamp_local']
        latest_day_of_week = int(latest_polling_record['day_of_week'])
        latest_timestamp_local = datetime.strptime(latest_time_str, '%H:%M:%S').time()
        # print(f"Latest timestamp: {latest_time_str}, Day of Week: {latest_day_of_week}")

        # Calculate the start time 24 hours earlier
        previous_day_of_week = (latest_day_of_week - 1) % 7
        latest_datetime = datetime.combine(datetime.today(), latest_timestamp_local)
        start_datetime = latest_datetime - timedelta(days=1)
        start_time_local = start_datetime.time()
        # print(f"Start time: {start_time_local.strftime('%H:%M:%S')}, Previous Day of Week: {previous_day_of_week}")

        # Fetch all polling data for the store from all_polling_data within the calculated time range
        data_cursor = db.all_polling_data.find({
            "store_id": store_id,
            "$or": [
                {
                    "day_of_week": latest_day_of_week,
                    "timestamp_local": {"$lte": latest_timestamp_local.strftime('%H:%M:%S')}
                },
                {
                    "day_of_week": previous_day_of_week,
                    "timestamp_local": {"$gte": start_time_local.strftime('%H:%M:%S')}
                }
            ]
        }).sort("timestamp_local", 1)

        # Convert cursor to list and exclude ObjectId for debugging/logging
        polling_data = [{k: v for k, v in record.items() if k != '_id'} for record in data_cursor]
        # print("Fetched polling data:\n", polling_data)
        
        # If no polling data found within the calculated range, return an empty result
        if not polling_data:
            print("No polling data found within the calculated range.")
            return {
                "store_id": store_id,
                "filtered_data_table": []
            }

        # Get business hours for the specific days of the week
        store_business_hours = business_hours[
            (business_hours['store_id'] == store_id) &
            (business_hours['day_of_week'].isin([previous_day_of_week, latest_day_of_week]))
        ]
        # print("Business hours for the store:\n", store_business_hours)
        
        # If no business hours found for this store on the specified days, return an empty result
        if store_business_hours.empty:
            print("No business hours found for the specified days.")
            return {
                "store_id": store_id,
                "filtered_data_table": []
            }

        # Extract business hours for the two days
        business_hours_dict = store_business_hours.set_index('day_of_week').to_dict(orient='index')
        # print("Business hours dictionary:\n", business_hours_dict)
        
        def is_within_business_hours(record):
            record_time = datetime.strptime(record['timestamp_local'], '%H:%M:%S').time()
            record_day = int(record['day_of_week'])
            if record_day in business_hours_dict:
                business_start = datetime.strptime(business_hours_dict[record_day]['start_time_local'], '%H:%M:%S').time()
                business_end = datetime.strptime(business_hours_dict[record_day]['end_time_local'], '%H:%M:%S').time()
                # Check if record_time is within the business hours
                within_hours = business_start <= record_time <= business_end
                # print(f"Checking {record_time} within hours ({business_start} to {business_end}): {within_hours}")
                return within_hours
            return False
        
        # Filter polling data within business hours
        polling_data_in_business_hours = [record for record in polling_data if is_within_business_hours(record)]
        # print("Filtered polling data within business hours:\n", polling_data_in_business_hours)

        # Prepare the records to be inserted into MongoDB
        last_day_records = [
            {
                "store_id": store_id,
                "day_of_week": int(record['day_of_week']),
                "timestamp_local": record['timestamp_local'],
                "status": record.get('status', 'unknown')  # Assuming 'status' field is present; default to 'unknown'
            }
            for record in polling_data_in_business_hours
        ]

        # Store only records that meet all conditions in MongoDB
        if last_day_records:
            db.last_day_records.insert_many(last_day_records)
            # print("Inserted records into MongoDB:\n", last_day_records)

        return {
            "store_id": store_id,
            "filtered_data_table": polling_data_in_business_hours
        }

    except Exception as e:
        # Print error message and raise HTTPException with a generic internal server error
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    
async def generate_filtered_data_table_last_week(store_id: int):
    try:
        # Load business hours for the store
        business_hours = pd.read_csv("data/business_hours.csv")
        
        # Calculate the start time 7 days earlier
        latest_day_of_week = datetime.now().weekday()
        start_datetime = datetime.now() - timedelta(days=7)
        start_day_of_week = start_datetime.weekday()
        
        # Fetch all polling data for the store within the last 7 days
        data_cursor = db.all_polling_data.find({
            "store_id": store_id,
            # "$or": [
            #     {
            #         "day_of_week": {"$gte": start_day_of_week, "$lte": latest_day_of_week},
            #     }
            # ]
        }).sort("timestamp_local", 1)

        # Convert cursor to list and exclude ObjectId
        polling_data = [{k: v for k, v in record.items() if k != '_id'} for record in data_cursor]
        
        # If no polling data found within the calculated range, return an empty result
        if not polling_data:
            return {
                "store_id": store_id,
                "filtered_data_table": []
            }

        # Get business hours for the specific days of the week
        store_business_hours = business_hours[
            (business_hours['store_id'] == store_id)
        ]
        
        # If no business hours found for this store on the specified days, return an empty result
        if store_business_hours.empty:
            return {
                "store_id": store_id,
                "filtered_data_table": []
            }

        # Extract business hours for the week
        business_hours_dict = store_business_hours.set_index('day_of_week').to_dict(orient='index')
        
        def is_within_business_hours(record):
            record_time = datetime.strptime(record['timestamp_local'], '%H:%M:%S').time()
            record_day = int(record['day_of_week'])
            if record_day in business_hours_dict:
                business_start = datetime.strptime(business_hours_dict[record_day]['start_time_local'], '%H:%M:%S').time()
                business_end = datetime.strptime(business_hours_dict[record_day]['end_time_local'], '%H:%M:%S').time()
                # Check if record_time is within the business hours
                return business_start <= record_time <= business_end
            return False
        
        # Filter polling data within business hours
        polling_data_in_business_hours = [record for record in polling_data if is_within_business_hours(record)]

        # Prepare the records to be returned
        last_week_records = [
            {
                "store_id": store_id,
                "day_of_week": int(record['day_of_week']),
                "timestamp_local": record['timestamp_local'],
                "status": record.get('status', 'unknown')  # Assuming 'status' field is present; default to 'unknown'
            }
            for record in polling_data_in_business_hours
        ]
        if last_week_records:
            db.last_week_records.insert_many(last_week_records)
        return {
            "store_id": store_id,
            "filtered_data_table": polling_data_in_business_hours
        }

    except Exception as e:
        # Print error message and raise HTTPException with a generic internal server error
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

   



async def calculate_uptime_downtime_last_hour(store_id: int):
    try:
        # Fetch the filtered data from MongoDB for the last hour
        filtered_data_cursor = db.last_hour_records.find({"store_id": store_id}).sort("timestamp_local", 1)
        filtered_data = [{k: v for k, v in record.items() if k != '_id'} for record in filtered_data_cursor]

        if not filtered_data:
            last_hour_records =[ {
                "store_id": store_id,
                "uptime_minutes": 0,
                "downtime_minutes": 0,
                "estimated_uptime_minutes": 0,
                "estimated_downtime_minutes": 0,
                "full_data": []
            }]

            if last_hour_records:
                db.up_down_hour.insert_many(last_hour_records)


            return {
                "store_id": store_id,
                "uptime_minutes": 0,
                "downtime_minutes": 0,
                "estimated_uptime_minutes": 0,
                "estimated_downtime_minutes": 0,
                "full_data": []
            }

        # Convert filtered data to DataFrame for easier manipulation
        df = pd.DataFrame(filtered_data)
        df['timestamp_local'] = pd.to_datetime(df['timestamp_local'], format='%H:%M:%S').dt.time

        # Define a base date for time calculations
        base_date = datetime(2000, 1, 1)  # Arbitrary base date

        # Convert time to datetime for calculations
        df['timestamp_local'] = df['timestamp_local'].apply(lambda t: datetime.combine(base_date, t))

        # Ensure data is sorted by timestamp
        df = df.sort_values(by='timestamp_local').reset_index(drop=True)

        # Define the start and end of the hour
        start_time = datetime.combine(base_date, df['timestamp_local'].iloc[0].time())
        end_time = start_time + timedelta(hours=1)

        # Create a list to hold the full dataset with estimated records
        full_data = []

        # Add the start_time if it is not in the DataFrame
        if df['timestamp_local'].iloc[0] > start_time:
            full_data.append({'timestamp_local': start_time, 'status': df['status'].iloc[0]})
        
        # Add the actual records
        for index, row in df.iterrows():
            full_data.append({'timestamp_local': row['timestamp_local'], 'status': row['status']})
        
        # Add the end_time if it is not in the DataFrame
        if df['timestamp_local'].iloc[-1] < end_time:
            full_data.append({'timestamp_local': end_time, 'status': df['status'].iloc[-1]})

        # Convert full_data list to DataFrame
        full_df = pd.DataFrame(full_data)
        full_df['timestamp_local'] = pd.to_datetime(full_df['timestamp_local']).dt.time

        # Convert times back to datetime for time difference calculations
        full_df['timestamp_local'] = full_df['timestamp_local'].apply(lambda t: datetime.combine(base_date, t))

        # Calculate the time differences between consecutive records
        full_df['time_diff'] = full_df['timestamp_local'].diff().fillna(pd.Timedelta(seconds=0)).dt.total_seconds() / 60

        # Calculate uptime and downtime by summing the time differences where status is 'active' or 'inactive'
        uptime_minutes = full_df[full_df['status'] == 'active']['time_diff'].sum()
        downtime_minutes = full_df[full_df['status'] == 'inactive']['time_diff'].sum()

        # Estimate missing data
        estimated_uptime_minutes = 0
        estimated_downtime_minutes = 0

        # Check for gaps between the first record and the start_time
        if full_df['timestamp_local'].iloc[0] > start_time:
            time_gap = (full_df['timestamp_local'].iloc[0] - start_time).total_seconds() / 60
            if full_df['status'].iloc[0] == 'active':
                estimated_uptime_minutes += time_gap
            else:
                estimated_downtime_minutes += time_gap

        # Check for gaps between records
        for i in range(len(full_df) - 1):
            gap_duration = (full_df['timestamp_local'].iloc[i + 1] - full_df['timestamp_local'].iloc[i]).total_seconds() / 60
            if full_df['status'].iloc[i] == 'active':
                estimated_uptime_minutes += gap_duration
            else:
                estimated_downtime_minutes += gap_duration

        # Check for gaps between the last record and end_time
        if full_df['timestamp_local'].iloc[-1] < end_time:
            time_gap = (end_time - full_df['timestamp_local'].iloc[-1]).total_seconds() / 60
            if full_df['status'].iloc[-1] == 'active':
                estimated_uptime_minutes += time_gap
            else:
                estimated_downtime_minutes += time_gap

        # Convert timestamps to string format for the response
        for record in full_data:
            record['timestamp_local'] = record['timestamp_local'].strftime('%H:%M:%S')
        
        last_hour_records =[ {
                "store_id": store_id,
                "uptime_minutes": uptime_minutes,
                "downtime_minutes": downtime_minutes,
                "estimated_uptime_minutes": estimated_uptime_minutes,
                "estimated_downtime_minutes": estimated_downtime_minutes,
                "full_data": full_data
        }]
          
        

        # Store only records that meet all conditions in MongoDB
        if last_hour_records:
            db.up_down_hour.insert_many(last_hour_records)


        return {
            "store_id": store_id,
            "uptime_minutes": uptime_minutes,
            "downtime_minutes": downtime_minutes,
            "estimated_uptime_minutes": estimated_uptime_minutes,
            "estimated_downtime_minutes": estimated_downtime_minutes,
            "full_data": full_data
        }

    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

async def calculate_uptime_downtime_last_day(store_id: int):
    try:
        # Fetch the filtered data from MongoDB for the last hour
        filtered_data_cursor = db.last_day_records.find({"store_id": store_id}).sort("timestamp_local", 1)
        filtered_data = [{k: v for k, v in record.items() if k != '_id'} for record in filtered_data_cursor]

        if not filtered_data:
            last_day_records =[ {
                "store_id": store_id,
                "uptime_hours": 0,
                "downtime_hours": 0,
                "estimated_uptime_hours": 0,
                "estimated_downtime_hours": 0,
                "full_data": []
            }]

            if last_day_records:
                db.up_down_day.insert_many(last_day_records)


            return {
                "store_id": store_id,
                "uptime_hours": 0,
                "downtime_hours": 0,
                "estimated_uptime_hours": 0,
                "estimated_downtime_hours": 0,
                "full_data": []
            }

            # Convert filtered data to DataFrame
        df = pd.DataFrame(filtered_data)
        df['timestamp_local'] = pd.to_datetime(df['timestamp_local'], format='%H:%M:%S').dt.time

        # Define a base date for time calculations
        base_date = datetime(2000, 1, 1)  # Arbitrary base date

        # Convert time to datetime for calculations
        df['timestamp_local'] = df['timestamp_local'].apply(lambda t: datetime.combine(base_date, t))

        # Ensure data is sorted by timestamp
        df = df.sort_values(by='timestamp_local').reset_index(drop=True)

        # Define the start and end of the period
        start_time = datetime.combine(base_date, datetime.min.time())
        end_time = start_time + timedelta(days=1)

        # Create a list to hold the full dataset with estimated records
        full_data = []

        # Add the start_time if it is not in the DataFrame
        if df['timestamp_local'].iloc[0] > start_time:
            full_data.append({'timestamp_local': start_time, 'status': df['status'].iloc[0]})
        
        # Add the actual records
        for index, row in df.iterrows():
            full_data.append({'timestamp_local': row['timestamp_local'], 'status': row['status']})
        
        # Add the end_time if it is not in the DataFrame
        if df['timestamp_local'].iloc[-1] < end_time:
            full_data.append({'timestamp_local': end_time, 'status': df['status'].iloc[-1]})

        # Convert full_data list to DataFrame
        full_df = pd.DataFrame(full_data)
        full_df['timestamp_local'] = pd.to_datetime(full_df['timestamp_local'])

        # Calculate the time differences between consecutive records
        full_df['time_diff'] = full_df['timestamp_local'].diff().fillna(pd.Timedelta(seconds=0)).dt.total_seconds() / 3600

        # Calculate uptime and downtime by summing the time differences where status is 'active' or 'inactive'
        uptime_hours = full_df[full_df['status'] == 'active']['time_diff'].sum()
        downtime_hours = full_df[full_df['status'] == 'inactive']['time_diff'].sum()

        # Estimate missing data
        estimated_uptime_hours = 0
        estimated_downtime_hours = 0

        # Check for gaps between the first record and the start_time
        if full_df['timestamp_local'].iloc[0] > start_time:
            time_gap = (full_df['timestamp_local'].iloc[0] - start_time).total_seconds() / 3600
            if full_df['status'].iloc[0] == 'active':
                estimated_uptime_hours += time_gap
            else:
                estimated_downtime_hours += time_gap

        # Check for gaps between records
        for i in range(len(full_df) - 1):
            gap_duration = (full_df['timestamp_local'].iloc[i + 1] - full_df['timestamp_local'].iloc[i]).total_seconds() / 3600
            if full_df['status'].iloc[i] == 'active':
                estimated_uptime_hours += gap_duration
            else:
                estimated_downtime_hours += gap_duration

        # Check for gaps between the last record and end_time
        if full_df['timestamp_local'].iloc[-1] < end_time:
            time_gap = (end_time - full_df['timestamp_local'].iloc[-1]).total_seconds() / 3600
            if full_df['status'].iloc[-1] == 'active':
                estimated_uptime_hours += time_gap
            else:
                estimated_downtime_hours += time_gap

        # Convert timestamps to string format for the response
        for record in full_data:
            record['timestamp_local'] = record['timestamp_local'].strftime('%H:%M:%S')
        
        last_day_records =[ {
                "store_id": store_id,
                "uptime_hours": uptime_hours,
                "downtime_hours": downtime_hours,
                "estimated_uptime_hours": estimated_uptime_hours,
                "estimated_downtime_hours": estimated_downtime_hours,
                "full_data": full_data
        }]
          
        

        # Store only records that meet all conditions in MongoDB
        if last_day_records:
            db.up_down_day.insert_many(last_day_records)


        return {
            "store_id": store_id,
                "uptime_hours": uptime_hours,
                "downtime_hours": downtime_hours,
                "estimated_uptime_hours": estimated_uptime_hours,
                "estimated_downtime_hours": estimated_downtime_hours,
                "full_data": full_data
        }

    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    


async def calculate_uptime_downtime_last_week(store_id: int):
    try:
        # Fetch the filtered data from MongoDB for the last week
        filtered_data_cursor = db.last_week_records.find({"store_id": store_id}).sort("timestamp_local", 1)
        filtered_data = [{k: v for k, v in record.items() if k != '_id'} for record in filtered_data_cursor]

        if not filtered_data:
            last_week_records =[ {
                "store_id": store_id,
                "uptime_hours": 0,
                "downtime_hours": 0,
                "estimated_uptime_hours": 0,
                "estimated_downtime_hours": 0,
                "full_data": []
            }]

            if last_week_records:
                db.up_down_week.insert_many(last_week_records)

            return {
                "store_id": store_id,
                "uptime_hours": 0,
                "downtime_hours": 0,
                "estimated_uptime_hours": 0,
                "estimated_downtime_hours": 0,
                "full_data": []
            }

        # Convert filtered data to DataFrame
        df = pd.DataFrame(filtered_data)
        df['timestamp_local'] = pd.to_datetime(df['timestamp_local'], format='%H:%M:%S').dt.time

        # Define a base date for time calculations
        base_date = datetime(2000, 1, 1)  # Arbitrary base date

        # Convert time to datetime for calculations
        df['timestamp_local'] = df['timestamp_local'].apply(lambda t: datetime.combine(base_date, t))

        # Ensure data is sorted by timestamp
        df = df.sort_values(by='timestamp_local').reset_index(drop=True)

        # Define the start and end of the period
        start_time = datetime.combine(base_date, datetime.min.time())
        end_time = start_time + timedelta(days=7)

        # Create a list to hold the full dataset with estimated records
        full_data = []

        # Add the start_time if it is not in the DataFrame
        if df['timestamp_local'].iloc[0] > start_time:
            full_data.append({'timestamp_local': start_time, 'status': df['status'].iloc[0]})

        # Add the actual records
        for index, row in df.iterrows():
            full_data.append({'timestamp_local': row['timestamp_local'], 'status': row['status']})

        # Add the end_time if it is not in the DataFrame
        if df['timestamp_local'].iloc[-1] < end_time:
            full_data.append({'timestamp_local': end_time, 'status': df['status'].iloc[-1]})

        # Convert full_data list to DataFrame
        full_df = pd.DataFrame(full_data)
        full_df['timestamp_local'] = pd.to_datetime(full_df['timestamp_local'])

        # Calculate the time differences between consecutive records
        full_df['time_diff'] = full_df['timestamp_local'].diff().fillna(pd.Timedelta(seconds=0)).dt.total_seconds() / 3600

        # Calculate uptime and downtime by summing the time differences where status is 'active' or 'inactive'
        uptime_hours = full_df[full_df['status'] == 'active']['time_diff'].sum()
        downtime_hours = full_df[full_df['status'] == 'inactive']['time_diff'].sum()

        # Estimate missing data
        estimated_uptime_hours = 0
        estimated_downtime_hours = 0

        # Check for gaps between the first record and the start_time
        if full_df['timestamp_local'].iloc[0] > start_time:
            time_gap = (full_df['timestamp_local'].iloc[0] - start_time).total_seconds() / 3600
            if full_df['status'].iloc[0] == 'active':
                estimated_uptime_hours += time_gap
            else:
                estimated_downtime_hours += time_gap

        # Check for gaps between records
        for i in range(len(full_df) - 1):
            gap_duration = (full_df['timestamp_local'].iloc[i + 1] - full_df['timestamp_local'].iloc[i]).total_seconds() / 3600
            if full_df['status'].iloc[i] == 'active':
                estimated_uptime_hours += gap_duration
            else:
                estimated_downtime_hours += gap_duration

        # Check for gaps between the last record and end_time
        if full_df['timestamp_local'].iloc[-1] < end_time:
            time_gap = (end_time - full_df['timestamp_local'].iloc[-1]).total_seconds() / 3600
            if full_df['status'].iloc[-1] == 'active':
                estimated_uptime_hours += time_gap
            else:
                estimated_downtime_hours += time_gap

        # Convert timestamps to string format for the response
        for record in full_data:
            record['timestamp_local'] = record['timestamp_local'].strftime('%H:%M:%S')

        last_week_records =[ {
                "store_id": store_id,
                "uptime_hours": uptime_hours,
                "downtime_hours": downtime_hours,
                "estimated_uptime_hours": estimated_uptime_hours,
                "estimated_downtime_hours": estimated_downtime_hours,
                "full_data": full_data
        }]

        # Store only records that meet all conditions in MongoDB
        if last_week_records:
            db.up_down_week.insert_many(last_week_records)

        return {
            "store_id": store_id,
            "uptime_hours": uptime_hours,
            "downtime_hours": downtime_hours,
            "estimated_uptime_hours": estimated_uptime_hours,
            "estimated_downtime_hours": estimated_downtime_hours,
            "full_data": full_data
        }

    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

store_ids_arr=[]

async def extract_store_ids():
    global store_ids_arr  
    try:
        
        df = pd.read_csv("data/store_status.csv")


        if 'store_id' not in df.columns:
            raise ValueError("CSV file must contain a 'store_id' column.")

        
        store_ids = df['store_id'].unique()

    
        store_ids_arr = store_ids.tolist()  

        return {"store_ids": store_ids_arr}

    except pd.errors.EmptyDataError:
        raise HTTPException(status_code=500, detail="CSV file is empty.")
    except pd.errors.ParserError:
        raise HTTPException(status_code=500, detail="Error parsing CSV file.")
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")


async def generate_report():
    # Ensure all data is processed
    await process_all_polling_data()
    await process_latest_polling_data()
    
    
    if not store_ids_arr:
        await extract_store_ids()
    final_results = []

    for store_id in store_ids_arr:
        
        await generate_filtered_data_table_last_hour(store_id)
        hour_data = await calculate_uptime_downtime_last_hour(store_id)
        

        await generate_filtered_data_table_last_day(store_id)
        day_data = await calculate_uptime_downtime_last_day(store_id)

        await generate_filtered_data_table_last_week(store_id)
        week_data = await calculate_uptime_downtime_last_week(store_id)
        
        
        report_entry = {
            "store_id": store_id,
            "uptime_last_hour": hour_data['estimated_uptime_minutes'],
            "uptime_last_day": day_data['estimated_uptime_hours'],
            "uptime_last_week": week_data['estimated_uptime_hours'],
            "downtime_last_hour": hour_data['estimated_downtime_minutes'],
            "downtime_last_day": day_data['estimated_downtime_hours'],
            "downtime_last_week": week_data['estimated_downtime_hours']
        }
        
        final_results.append(report_entry)
    

    final_report_df = pd.DataFrame(final_results)
    
    
    report_id = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
    
    
    file_path = f"{report_id}.csv"
    
    
    final_report_df.to_csv(file_path, index=False)
    
    
    db.reports.insert_one({"report_id": report_id, "status": "Complete", "file_path": file_path})
    
    return report_id

async def get_report(report_id: str):
    report = db.reports.find_one({"report_id": report_id})
    if report:
        if report["status"] == "Running":
            return {"status": "Running"}
        elif report["status"] == "Complete":
            return {"status": "Complete", "file_path": report.get("file_path")}
    return {"status": "Report not found"}
