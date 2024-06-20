import sqlalchemy
from db import metadata

"""
weight_sensors: Table contains structure for sensor(weight)
"""
weight_sensor = sqlalchemy.Table(
    "weight_sensors",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("weight", sqlalchemy.Integer, nullable=False, unique=False),
    sqlalchemy.Column("creation_date", sqlalchemy.TIMESTAMP(timezone=True), nullable=False),
    sqlalchemy.Column("sensor_id", sqlalchemy.Integer, sqlalchemy.ForeignKey("sensors.sensor_number"), nullable=False,unique=False)
)