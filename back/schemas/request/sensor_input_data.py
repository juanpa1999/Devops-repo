from pydantic import BaseModel, field_validator
from enum import Enum
from typing import Union

class SensorStatus(str, Enum):
    active = "active"
    inactive = "inactive"
    pending = "pending"

class SensorCreateData(BaseModel):
    sensor_number: int
    sensor_location: str
    status: SensorStatus

class BaseSensorData(BaseModel):
    sensor_id: int

    @staticmethod
    def int_validator(value: Union[str, int], field_name: str):
        try:
            return int(value)
        except ValueError:
            raise ValueError(f'{field_name} must be an integer value')

class TemperatureRegistrationData(BaseSensorData):
    temperature: Union[str, int]

    @field_validator('temperature', mode='before')
    def validate_temperature(cls, value):
        return cls.int_validator(value, 'Temperature')

class HumidityRegistrationData(BaseSensorData):
    humidity: Union[str, int]

    @field_validator('humidity', mode='before')
    def validate_humidity(cls, value):
        return cls.int_validator(value, 'Humidity')

class WeightRegistrationData(BaseSensorData):
    weight: Union[str, int]

    @field_validator('weight', mode='before')
    def validate_weight(cls, value):
        return cls.int_validator(value, 'Weight')
