import sqlalchemy
from db import metadata
from schemas.request.user_input_data import UserStatus, UserRole

"""
users: Table contains structure for users
"""
user = sqlalchemy.Table(
    "users",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("username", sqlalchemy.String(60), unique=True),
    sqlalchemy.Column("password", sqlalchemy.String(120), nullable=False),
    sqlalchemy.Column("status", sqlalchemy.Enum(UserStatus), nullable=False, server_default=UserStatus.pending.name),
    sqlalchemy.Column("user_role", sqlalchemy.Enum(UserRole), nullable=False, server_default=UserRole.operator.name),
    sqlalchemy.Column("creation_date", sqlalchemy.Date, nullable=False)
)