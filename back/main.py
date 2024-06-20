from contextlib import asynccontextmanager
from fastapi import FastAPI
from db import database
from api_calls.routes import api_router
from starlette.middleware.cors import CORSMiddleware
from decouple import config


@asynccontextmanager
async def app_lifespan(app: FastAPI):
    await database.connect()
    try:
        yield
    finally:
        await database.disconnect()
 
app = FastAPI(lifespan=app_lifespan)
app.include_router(api_router)

origins = [
    "http://localhost:3000",
    "https://create-react-app-site-production-def0.up.railway.app",
    "https://react-front-refoenergy-production.up.railway.app",
    config("NGROK")
]

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

