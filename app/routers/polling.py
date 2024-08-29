from fastapi import APIRouter
from app.services.polling_service import process_all_polling_data,process_latest_polling_data,generate_filtered_data_table_last_hour,calculate_uptime_downtime_last_hour,generate_filtered_data_table_last_day,calculate_uptime_downtime_last_day,generate_filtered_data_table_last_week,calculate_uptime_downtime_last_week,generate_report,get_report
from app.db import db
from fastapi import HTTPException
import os
import pandas as pd
router = APIRouter()
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




## Testing API Endpoints
@router.post("/process_all_polling_data/")
async def process_all_polling_data_endpoint():

    return await process_all_polling_data()

@router.post("/process_latest_polling_data/")
async def process_latest_polling_data_endpoint():

    return await process_latest_polling_data()

@router.get("/stores/filtered_data_last_hour")
async def get_filtered_data_for_all_stores_last_hour():

    if not store_ids_arr:
        await extract_store_ids()
    results = []
    
    for store_id in store_ids_arr:
        result = await generate_filtered_data_table_last_hour(store_id)
        results.append(result)
    
    return results

@router.get("/stores/filtered_data_last_day")
async def get_filtered_data_for_all_stores_last_day():
    if not store_ids_arr:
        await extract_store_ids()
    results = []
    
    for store_id in store_ids_arr:
        result = await generate_filtered_data_table_last_day(store_id)
        results.append(result)
    
    return results
@router.get("/stores/filtered_data_last_week")
async def get_filtered_data_for_all_stores_last_week():
    if not store_ids_arr:
        await extract_store_ids()
    results = []
    
    for store_id in store_ids_arr:
        result = await generate_filtered_data_table_last_week(store_id)
        results.append(result)
    
    return results


@router.get("/stores/uptime_downtime_last_hour")
async def calculate_uptime_downtime_for_all_stores_last_hour():
    if not store_ids_arr:
        await extract_store_ids()
    results = []
    
    for store_id in store_ids_arr:
        result = await calculate_uptime_downtime_last_hour(store_id)
        results.append(result)
    
    return results
@router.get("/stores/uptime_downtime_last_day")
async def calculate_uptime_downtime_for_all_stores_last_day():
    if not store_ids_arr:
        await extract_store_ids()
    results = []
    
    for store_id in store_ids_arr:
        result = await calculate_uptime_downtime_last_day(store_id)
        results.append(result)
    
    return results

@router.get("/stores/uptime_downtime_last_week")
async def calculate_uptime_downtime_for_all_stores_last_week():
    if not store_ids_arr:
        await extract_store_ids()
    results = []
    
    for store_id in store_ids_arr:
        result = await calculate_uptime_downtime_last_week(store_id)
        results.append(result)
    
    return results



### Final API Endpoints

@router.post("/trigger_report")
async def trigger_report_endpoint():
    report_id = await generate_report()
    return {"report_id": report_id}

@router.get("/get_report")
async def get_report_endpoint(report_id: str):
    report = await get_report(report_id)
    if report["status"] == "Running":
        return {"status": "Running"}
    elif report["status"] == "Complete":
        file_path = report.get("file_path")
        if file_path and os.path.exists(file_path):
            return {"status": "Complete", "file_path": file_path}
        else:
            return {"status": "Complete", "message": "File not found"}
    else:
        raise HTTPException(status_code=404, detail="Report not found")