import sqlalchemy
from db import metadata

"""
humidity_sensor: Table contains structure for sensor(temperature)
"""
humidity_sensor = sqlalchemy.Table(
    "humidity_sensors",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("humidity", sqlalchemy.Integer, nullable=False, unique=False),
    sqlalchemy.Column("creation_date", sqlalchemy.TIMESTAMP(timezone=True), nullable=False),
    sqlalchemy.Column("sensor_id", sqlalchemy.Integer, sqlalchemy.ForeignKey("sensors.sensor_number"), nullable=False, unique=False)
)