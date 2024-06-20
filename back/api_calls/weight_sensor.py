from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect
from managers.weight_sensor import WeightSensor
from schemas.request.sensor_input_data import WeightRegistrationData
import asyncio
import logging

router = APIRouter(prefix="/weight_sensor", tags=["weight_sensor"],
                   responses={401: {"user": "Not authorized"}})


@router.get("/show_all_weights/", description="Allows users to get all weights")
async def get_all_weights():
    try:
        weights = await WeightSensor.get_all_weights()
        return weights
    except:
        raise HTTPException(status_code=403, detail="Not authorized to view weights")


@router.post("/create_weight/", description="Allows users to create a new weight")
async def create_weights(weight_data: WeightRegistrationData):
    try:
        result = await WeightSensor.create_weight(weight_data)
        return result
    except:
        raise HTTPException(status_code=403, detail="Unexpected error while uploading weight sensor data -- Check if the sensor is registered and active.")


#websocket
@router.websocket("/ws/weight")
async def websocket_weight(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            weights = await WeightSensor.get_all_weights()
            json_data = [{"weight": item.weight, "creation_date": item.creation_date.isoformat()} for item in weights]
            await websocket.send_json(json_data)
            await asyncio.sleep(10)
    except WebSocketDisconnect:
        logging.info("Client disconnected")
    except Exception as e:
        logging.error(f"WebSocket error: {e}")
        await websocket.close(code=1000, reason=str(e))