from fastapi import APIRouter
from app.services.polling_service import process_all_polling_data,process_latest_polling_data,generate_filtered_data_table
from app.db import db
router = APIRouter()

@router.post("/process_all_polling_data/")
async def process_all_polling_data_endpoint():

    return await process_all_polling_data()

@router.post("/process_latest_polling_data/")
async def process_latest_polling_data_endpoint():

    return await process_latest_polling_data()

@router.get("/stores/filtered_data")
async def get_filtered_data_for_all_stores():
    # Fetch all store IDs from the database
    store_ids = [1,2,3]
    results = []
    
    for store_id in store_ids:
        result = await generate_filtered_data_table(store_id)
        results.append(result)
    
    return results