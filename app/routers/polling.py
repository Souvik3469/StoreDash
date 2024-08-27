from fastapi import APIRouter
from app.services.polling_service import process_all_polling_data,process_latest_polling_data,generate_filtered_data_table_last_hour,calculate_uptime_downtime_last_hour,generate_filtered_data_table_last_day,calculate_uptime_downtime_last_day,generate_report,get_report
from app.db import db
from fastapi import HTTPException
import os
router = APIRouter()

@router.post("/process_all_polling_data/")
async def process_all_polling_data_endpoint():

    return await process_all_polling_data()

@router.post("/process_latest_polling_data/")
async def process_latest_polling_data_endpoint():

    return await process_latest_polling_data()

@router.get("/stores/filtered_data_last_hour")
async def get_filtered_data_for_all_stores_last_hour():
    # Fetch all store IDs from the database
    store_ids = [1,2,3]
    results = []
    
    for store_id in store_ids:
        result = await generate_filtered_data_table_last_hour(store_id)
        results.append(result)
    
    return results

@router.get("/stores/filtered_data_last_day")
async def get_filtered_data_for_all_stores_last_day():
    # Fetch all store IDs from the database
    store_ids = [1,2,3]
    results = []
    
    for store_id in store_ids:
        result = await generate_filtered_data_table_last_day(store_id)
        results.append(result)
    
    return results


@router.get("/stores/uptime_downtime_last_hour")
async def calculate_uptime_downtime_for_all_stores_last_hour():
    # Mock store IDs; Replace this with dynamic fetching if needed
    store_ids = [1, 2, 3]
    results = []
    
    for store_id in store_ids:
        result = await calculate_uptime_downtime_last_hour(store_id)
        results.append(result)
    
    return results
@router.get("/stores/uptime_downtime_last_day")
async def calculate_uptime_downtime_for_all_stores_last_day():

    # Mock store IDs; Replace this with dynamic fetching if needed
    store_ids = [1, 2, 3]
    results = []
    
    for store_id in store_ids:
        result = await calculate_uptime_downtime_last_day(store_id)
        results.append(result)
    
    return results


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