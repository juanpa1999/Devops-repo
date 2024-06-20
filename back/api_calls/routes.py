from fastapi import APIRouter
from api_calls import auth, temperature_sensor, weight_sensor,admin, humidity_sensor

api_router = APIRouter()
api_router.include_router(auth.router)
api_router.include_router(temperature_sensor.router)
api_router.include_router(humidity_sensor.router)
api_router.include_router(weight_sensor.router)
api_router.include_router(admin.router)
