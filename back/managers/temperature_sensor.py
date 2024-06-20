import sqlalchemy
from db import database
from fastapi import HTTPException
from models import temperature_sensor, sensor
from datetime import datetime
import pytz
from schemas.request.sensor_input_data import TemperatureRegistrationData, SensorStatus


class TemperatureSensor:

    @staticmethod
    async def get_all_temperatures():
        try:
            query = sqlalchemy.select([temperature_sensor]).order_by(temperature_sensor.c.creation_date)
            result = await database.fetch_all(query)
            return result
        except Exception as e:
            error_message = f"Failed to get temperatures: {str(e)}"
            print(error_message)
            raise HTTPException(status_code=500, detail=error_message)

    @staticmethod
    async def create_temperature(temperature_data: TemperatureRegistrationData):
        try:
            sensor_id = temperature_data.sensor_id
            query = sensor.select().where(sensor.c.sensor_number == sensor_id)
            sensor_record = await database.fetch_one(query)

            if not sensor_record:
                raise HTTPException(status_code=404, detail="Sensor not found")

            if sensor_record["status"] != SensorStatus.active:
                raise HTTPException(status_code=400, detail="Sensor is not active")

            temperature_dict = temperature_data.model_dump()

            # Crear el objeto timezone para Bogotá
            bogota_tz = pytz.timezone('America/Bogota')
            # Obtener la fecha y hora actual en la zona horaria de Bogotá
            current_time_bogota = datetime.now(bogota_tz)

            temperature_dict["creation_date"] = current_time_bogota
            temperature_dict["sensor_id"] = sensor_id

            query = temperature_sensor.insert().values(**temperature_dict)
            last_record_id = await database.execute(query)
            return {**temperature_dict, "id": last_record_id}

        except Exception as e:
            error_message = f"Failed to create temperature: {str(e)}"
            print(error_message)
            raise HTTPException(status_code=500, detail=error_message)
