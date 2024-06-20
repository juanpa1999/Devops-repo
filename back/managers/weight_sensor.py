import sqlalchemy
from db import database
from fastapi import HTTPException
from models import weight_sensor, sensor
from datetime import datetime
import pytz
from schemas.request.sensor_input_data import WeightRegistrationData, SensorStatus


class WeightSensor:

    @staticmethod
    async def get_all_weights():
        try:
            query = sqlalchemy.select([weight_sensor]).order_by(weight_sensor.c.creation_date)
            result = await database.fetch_all(query)
            return result
        except Exception as e:
            error_message = f"Failed to get weights: {str(e)}"
            print(error_message)
            raise HTTPException(status_code=500, detail=error_message)

    @staticmethod
    async def create_weight(weight_data: WeightRegistrationData):
        try:
            sensor_id = weight_data.sensor_id
            query = sensor.select().where(sensor.c.sensor_number == sensor_id)
            sensor_record = await database.fetch_one(query)

            if not sensor_record:
                raise HTTPException(status_code=404, detail="Sensor not found")

            if sensor_record["status"] != SensorStatus.active:
                raise HTTPException(status_code=400, detail="Sensor is not active")

            weight_dict = weight_data.model_dump()

            # Crear el objeto timezone para Bogotá
            bogota_tz = pytz.timezone('America/Bogota')
            # Obtener la fecha y hora actual en la zona horaria de Bogotá
            current_time_bogota = datetime.now(bogota_tz)

            weight_dict["creation_date"] = current_time_bogota
            weight_dict["sensor_id"] = sensor_id

            query = weight_sensor.insert().values(**weight_dict)
            last_record_id = await database.execute(query)
            return {**weight_dict, "id": last_record_id}

        except Exception as e:
            error_message = f"Failed to create weight: {str(e)}"
            print(error_message)
            raise HTTPException(status_code=500, detail=error_message)
