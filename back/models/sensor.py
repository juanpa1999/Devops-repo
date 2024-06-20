import sqlalchemy
from db import metadata
from schemas.request.sensor_input_data import SensorStatus

"""
sensors: Table contains structure for sensors
"""
sensor = sqlalchemy.Table(
    "sensors",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("sensor_number", sqlalchemy.Integer, unique=True),
    sqlalchemy.Column("sensor_location", sqlalchemy.String(120), unique=False),
    sqlalchemy.Column("status", sqlalchemy.Enum(SensorStatus), nullable=False, server_default=SensorStatus.pending.name),
    sqlalchemy.Column("creation_date", sqlalchemy.Date, nullable=False)
)