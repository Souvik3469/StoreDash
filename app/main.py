from fastapi import FastAPI
from app.routers import polling

app = FastAPI()

app.include_router(polling.router)

@app.get("/")
async def root():
    return {"message": "Welcome to the Restaurant Monitoring API"}
