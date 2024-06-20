import sqlalchemy
from db import database
from fastapi import HTTPException
from models import humidity_sensor, sensor
from datetime import datetime
import pytz
from schemas.request.sensor_input_data import HumidityRegistrationData, SensorStatus


class HumiditySensor:

    @staticmethod
    async def get_all_humidities():
        try:
            query = sqlalchemy.select([humidity_sensor]).order_by(humidity_sensor.c.creation_date)
            result = await database.fetch_all(query)
            return result
        except Exception as e:
            error_message = f"Failed to get humidities: {str(e)}"
            print(error_message)
            raise HTTPException(status_code=500, detail=error_message)

    @staticmethod
    async def create_humidity(humidity_data: HumidityRegistrationData):
        try:
            sensor_id = humidity_data.sensor_id
            query = sensor.select().where(sensor.c.sensor_number == sensor_id)
            sensor_record = await database.fetch_one(query)

            if not sensor_record:
                raise HTTPException(status_code=404, detail="Sensor not found")

            if sensor_record["status"] != SensorStatus.active:
                raise HTTPException(status_code=400, detail="Sensor is not active")

            humidity_dict = humidity_data.model_dump()

            # Crear el objeto timezone para Bogotá
            bogota_tz = pytz.timezone('America/Bogota')
            # Obtener la fecha y hora actual en la zona horaria de Bogotá
            current_time_bogota = datetime.now(bogota_tz)

            humidity_dict["creation_date"] = current_time_bogota
            humidity_dict["sensor_id"] = sensor_id

            query = humidity_sensor.insert().values(**humidity_dict)
            last_record_id = await database.execute(query)
            return {**humidity_dict, "id": last_record_id}

        except Exception as e:
            error_message = f"Failed to create humidity: {str(e)}"
            print(error_message)
            raise HTTPException(status_code=500, detail=error_message)
