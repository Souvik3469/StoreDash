# StoreDash (Backend)
Assignment for Loop. Python Backend with FastApi.
## Problem Statement:
Loop monitors several restaurants in the US and needs to monitor if the store is online or not. All restaurants are supposed to be online during their business hours. Due to some unknown reasons, a store might go inactive for a few hours. Restaurant owners want to get a report of the how often this happened in the past.  

## Data sources

(Note: For better representation I have used my dummy csvs)

We will have 3 sources of data 

1. We poll every store roughly every hour and have data about whether the store was active or not in a CSV.  The CSV has 3 columns (`store_id, timestamp_utc, status`) where status is active or inactive.  All timestamps are in **UTC**
    1. Data can be found in CSV format [here](https://github.com/Souvik3469/loop/blob/main/data/store_status.csv)
2. We have the business hours of all the stores - schema of this data is `store_id, dayOfWeek(0=Monday, 6=Sunday), start_time_local, end_time_local`
    1. These times are in the **local time zone**
    2. If data is missing for a store, assume it is open 24*7
    3. Data can be found in CSV format [here](https://github.com/Souvik3469/loop/blob/main/data/business_hours.csv)
3. Timezone for the stores - schema is `store_id, timezone_str` 
    1. If data is missing for a store, assume it is America/Chicago
    2. This is used so that data sources 1 and 2 can be compared against each other. 
    3. Data can be found in CSV format [here](https://github.com/Souvik3469/loop/blob/main/data/store_timezones.csv)
   
## Installation

1. **Clone the Repository**

   ```bash
   git clone https://github.com/Souvik3469/StoreDash.git
   cd StoreDash

2. **Install Dependencies**

   ```bash
   pip install -r requirements.txt
   
3. **Configure MongoDb**

   Create an env file in root directory and add your MONGODB atlas uri.
   ```bash
   MONGODB_URI="Your MONGODB URI"

4. **Run the Api**

   The API will be available at http://127.0.0.1:8000
   ```bash
   uvicorn main:app --reload
5. **Final API Endpoints**
    - Open http://127.0.0.1:8000/docs where all the endpoints can be tested.
    ```bash
   /trigger_report
    ```
   - POST /trigger_report endpoint that will trigger report generation from the data provided (stored in DB)
    1. No input 
    2. Output - report_id (random string) 
    3. report_id will be used for polling the status of report completion

    ```bash
   /get_report
    ```
   - GET /get_report endpoint that will return the status of the report or the csv
    1. Input - report_id
    2. Output
        - if report generation is not complete, return “Running” as the output
        - if report generation is complete, return “Complete” along with the CSV file with the schema described above.
        
6. **Testing API Endpoints**

    Endpoints used for step by step testing.

     ```bash
   /process_all_polling_data/
    ```
   - POST
    /process_all_polling_data/
    To convert all data from store_status.csv from timestamp_utc to local time and also converting date to weekdays, and store it in database.
    
    ```bash
   /process_latest_polling_data/
    ```
   - POST
    /process_latest_polling_data/
    To convert only the latest timestamp data from store_status.csv from timestamp_utc to local time and also converting date to weekdays, and store it in database.
    
    ```bash
   /stores/filtered_data_last_hour/
    ```
    - GET
    /stores/filtered_data_last_hour
    To store the data for each store for the last hour(from latest timestamp of that store) and store it in database.
    
    ```bash
   /stores/filtered_data_last_day/
    ```
    - GET
    /stores/filtered_data_last_day
    To store the data for each store for the last day(from latest timestamp of that store) and store it in database.
    
    ```bash
   /stores/filtered_data_last_week/
    ```
    - GET
    /stores/filtered_data_last_week
    To store the data for each store for the last week(from latest timestamp of that store) and store it in database.
    
    ```bash
   /stores/uptime_downtime_last_hour/
    ```
    - GET
    /stores/uptime_downtime_last_hour
    Calculate Uptime Downtime For All Stores Last hour (using interpolation logic to fill missing data for accurate analysis) and store it in database.
    
    ```bash
   /stores/uptime_downtime_last_day/
    ```
   - GET
    /stores/uptime_downtime_last_day
    Calculate Uptime Downtime For All Stores Last day (using interpolation logic to fill missing data for accurate analysis) and store it in database.
    
    ```bash
   /stores/uptime_downtime_last_week/
    ```
   - GET
    /stores/uptime_downtime_last_week
    Calculate Uptime Downtime For All Stores Last week (using interpolation logic to fill missing data for accurate analysis) and store it in database.
    
    - Using all those tables the final report is created.

   ![APIs](https://github.com/Souvik3469/loop/blob/main/data/apis.png)

   
