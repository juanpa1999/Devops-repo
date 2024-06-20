import sqlalchemy
from db import database
from fastapi import HTTPException
from models import sensor
from datetime import datetime
from schemas.request.sensor_input_data import SensorCreateData, SensorStatus


class SensorManager:

    @staticmethod
    async def get_all_sensors():
        try:
            query = sqlalchemy.select([sensor])
            result = await database.fetch_all(query)
            return result
        except Exception as e:
            error_message = f"Failed to get all sensors: {str(e)}"
            print(error_message)
            raise HTTPException(status_code=500, detail=error_message)

    @staticmethod
    async def create_sensor(sensor_data: SensorCreateData):
        try:
            query = sensor.insert().values(
                sensor_number=sensor_data.sensor_number,
                sensor_location=sensor_data.sensor_location,
                status=sensor_data.status,
                creation_date=datetime.now().date()
            )
            sensor_id = await database.execute(query)
            return {"message": "Sensor created successfully", "sensor_id": sensor_id}
        except Exception as e:
            error_message = f"Failed to create sensor: {str(e)}"
            print(error_message)
            raise HTTPException(status_code=500, detail=error_message)

    @staticmethod
    async def update_sensor_status(sensor_id: int, new_status: SensorStatus):
        select_query = sqlalchemy.select([sensor]).where(sensor.c.id == sensor_id)
        sensor_result = await database.fetch_one(select_query)

        if sensor_result is None:
            raise HTTPException(status_code=404, detail="Sensor not found")

        try:
            update_query = sensor.update().where(sensor.c.id == sensor_id).values(status=new_status)
            await database.execute(update_query)
            return {"message": "Sensor status updated successfully"}
        except Exception as e:
            error_message = f"Failed to update sensor status: {str(e)}"
            print(error_message)
            raise HTTPException(status_code=500, detail=error_message)

    @staticmethod
    async def delete_sensor(sensor_id: int):
        select_query = sqlalchemy.select([sensor]).where(sensor.c.id == sensor_id)
        sensor_result = await database.fetch_one(select_query)

        if sensor_result is None:
            raise HTTPException(status_code=404, detail="Sensor not found")

        try:
            delete_query = sensor.delete().where(sensor.c.id == sensor_id)
            await database.execute(delete_query)
            return {"message": "Sensor deleted successfully"}
        except Exception as e:
            error_message = f"Failed to delete sensor: {str(e)}"
            print(error_message)
            raise HTTPException(status_code=500, detail=error_message)

