from fastapi import APIRouter, HTTPException, WebSocket,WebSocketDisconnect
from managers.humidity_sensor import HumiditySensor
from schemas.request.sensor_input_data import HumidityRegistrationData
import asyncio
import logging


router = APIRouter(prefix="/humidity_sensor", tags=["humidity_sensor"],
                   responses={401: {"user": "Not authorized"}})

"""@router.get("/show_all_humidities/", description="Allows users to get all cases")
async def get_all_cases(current_user: dict = Depends(AuthManager.get_current_user)):
    if current_user is None:
        raise HTTPException(status_code=403, detail="User Not Recognize")
    elif current_user["user_role"] in [UserRole.master, UserRole.admin, UserRole.operator]:
        humidities = await HumiditySensor.get_all_humidities()
        return humidities
    else:
        raise HTTPException(status_code=403, detail="Not authorized to view cases")"""


@router.get("/show_all_humidities/", description="Allows users to get all humidities")
async def get_all_humidities():
    try:
        humidities = await HumiditySensor.get_all_humidities()
        return humidities
    except:
        raise HTTPException(status_code=403, detail="Not authorized to view humidities")


@router.post("/create_humidity/", description="Allows users to create a new humidity")
async def create_humidity(humidity_data: HumidityRegistrationData):
    try:
        result = await HumiditySensor.create_humidity(humidity_data)
        return result
    except:
        raise HTTPException(status_code=403, detail="Unexpected error while uploading humidity sensor data -- Check if the sensor is registered and active.")

#websocket
@router.websocket("/ws/humidity")
async def websocket_humidity(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            humidities = await HumiditySensor.get_all_humidities()
            json_data = [{"humidity": item.humidity, "creation_date": item.creation_date.isoformat()} for item in humidities]
            await websocket.send_json(json_data)
            await asyncio.sleep(10)
    except WebSocketDisconnect:
        logging.info("Client disconnected")
    except Exception as e:
        logging.error(f"WebSocket error: {e}")
        await websocket.close(code=1000, reason=str(e))