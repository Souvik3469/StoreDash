from fastapi import APIRouter
from app.services.polling_service import process_all_polling_data,process_latest_polling_data,generate_filtered_data_table

router = APIRouter()

@router.post("/process_all_polling_data/")
async def process_all_polling_data_endpoint():

    return await process_all_polling_data()

@router.post("/process_latest_polling_data/")
async def process_latest_polling_data_endpoint():

    return await process_latest_polling_data()

@router.get("/store/{store_id}/filtered_data")
async def get_filtered_data(store_id: int):
    result = await generate_filtered_data_table(store_id)
    return result