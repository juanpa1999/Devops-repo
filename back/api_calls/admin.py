from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from managers.auth import AuthManager
from managers.admin import AdminManager
from managers.sensor import SensorManager
from managers.humidity_sensor import HumiditySensor
from managers.temperature_sensor import TemperatureSensor
from managers.weight_sensor import WeightSensor
from schemas.request.user_input_data import UserRole
from schemas.request.sensor_input_data import SensorCreateData, SensorStatus, HumidityRegistrationData, TemperatureRegistrationData, WeightRegistrationData
import pandas as pd
import io

router = APIRouter(prefix="/admin", tags=["admin"],
                   responses={401: {"user": "Not authorized"}})

@router.get("/show_all_users/", description="Allows admin to get all users")
async def get_all_users(current_user: dict = Depends(AuthManager.get_current_user)):
    if current_user is None:
        raise HTTPException(status_code=403, detail="User Not Recognize")
    elif current_user["user_role"] in [UserRole.master, UserRole.admin]:
        try:
            users = await AdminManager.get_all_users()
            return users
        except:
            raise HTTPException(status_code=403, detail="Users not found")
    else:
        raise HTTPException(status_code=403, detail="Not authorized to view users")

@router.put("/update_user_status/{user_id}", description="Updates the status of a specific user")
async def update_user_status(user_id: int, status: str,
                             current_user: dict = Depends(AuthManager.get_current_user)):
    if current_user is None:
        raise HTTPException(status_code=403, detail="User Not Recognized")
    elif current_user["user_role"] in [UserRole.master, UserRole.admin]:
        try:
            user = await AdminManager.update_user_status(user_id, status)
            return user
        except Exception as e:
            raise HTTPException(status_code=403, detail="User not found")
    else:
        raise HTTPException(status_code=403, detail="Not authorized to update user status")

@router.delete("/delete_user/{user_id}", description="Deletes a specific user")
async def delete_user(user_id: int, current_user: dict = Depends(AuthManager.get_current_user)):
    if current_user is None:
        raise HTTPException(status_code=403, detail="User Not Recognized")
    elif current_user["user_role"] in [UserRole.master, UserRole.admin]:
        try:
            user = await AdminManager.delete_user(user_id)
            return user
        except Exception:
            raise HTTPException(status_code=403, detail="User not found")
    else:
        raise HTTPException(status_code=403, detail="Not authorized to delete user")

# Sensor Logic

@router.get("/sensor/", description="Get all sensors")
async def get_all_sensors(current_user: dict = Depends(AuthManager.get_current_user)):
    if current_user is None:
        raise HTTPException(status_code=403, detail="User Not Recognized")
    try:
        sensors = await SensorManager.get_all_sensors()
        return sensors
    except:
        raise HTTPException(status_code=500, detail="Failed to fetch sensors")

@router.post("/sensor/", description="Create a new sensor")
async def create_sensor(sensor_data: SensorCreateData, current_user: dict = Depends(AuthManager.get_current_user)):
    if current_user is None:
        raise HTTPException(status_code=403, detail="User Not Recognized")
    try:
        return await SensorManager.create_sensor(sensor_data)
    except:
        raise HTTPException(status_code=500, detail="Failed to create sensor")

@router.put("/sensor/{sensor_id}", description="Update a sensor's status")
async def update_sensor_status(sensor_id: int, status: SensorStatus, current_user: dict = Depends(AuthManager.get_current_user)):
    if current_user is None:
        raise HTTPException(status_code=403, detail="User Not Recognized")
    try:
        return await SensorManager.update_sensor_status(sensor_id, status)
    except:
        raise HTTPException(status_code=500, detail="Failed to update sensor status")

@router.delete("/sensor/{sensor_id}", description="Delete a sensor")
async def delete_sensor(sensor_id: int, current_user: dict = Depends(AuthManager.get_current_user)):
    if current_user is None:
        raise HTTPException(status_code=403, detail="User Not Recognized")
    try:
        return await SensorManager.delete_sensor(sensor_id)
    except:
        raise HTTPException(status_code=500, detail="Failed to delete sensor")

@router.post("/upload-csv", description="Upload and process a CSV file")
async def upload_csv(file: UploadFile = File(...), current_user: dict = Depends(AuthManager.get_current_user)):
    if current_user is None:
        raise HTTPException(status_code=403, detail="User Not Recognized")
    elif current_user["user_role"] not in [UserRole.master, UserRole.admin, UserRole.operator]:
        raise HTTPException(status_code=403, detail="Not authorized to upload CSV")

    try:
        # Leer el contenido del archivo CSV
        contents = await file.read()
        data = pd.read_csv(io.BytesIO(contents))

        # Procesar cada fila del CSV
        for index, row in data.iterrows():
            sensor_type = row['tipo']
            sensor_id = row['sensor_id']

            if sensor_type == 'temperatura':
                temperature_data = TemperatureRegistrationData(
                    temperature=row['valor'],
                    sensor_id=sensor_id
                )
                await TemperatureSensor.create_temperature(temperature_data)

            elif sensor_type == 'humedad':
                humidity_data = HumidityRegistrationData(
                    humidity=row['valor'],
                    sensor_id=sensor_id
                )
                await HumiditySensor.create_humidity(humidity_data)

            elif sensor_type == 'peso':
                weight_data = WeightRegistrationData(
                    weight=row['valor'],
                    sensor_id=sensor_id
                )
                await WeightSensor.create_weight(weight_data)

        return {"message": "Archivo procesado correctamente"}
    except Exception as e:
        error_message = f"Error al procesar el archivo: {str(e)}"
        print(error_message)
        raise HTTPException(status_code=500, detail=error_message)
