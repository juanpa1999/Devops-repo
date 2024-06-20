import jwt
from typing import Optional
from fastapi.security import OAuth2PasswordBearer
from schemas.request.user_input_data import UserRole,UserStatus
from passlib.context import CryptContext
from datetime import datetime, timedelta, timezone
from decouple import config
from fastapi import Request
from db import database
from models import user

bcrypt_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_bearer = OAuth2PasswordBearer(tokenUrl="token")


def get_password_hash(password):
    return bcrypt_context.hash(password)


def verify_password(plain_password, hashed_password):
    return bcrypt_context.verify(plain_password, hashed_password)


class AuthManager:

    @staticmethod
    async def register(user_data):
        # Verificar si se pasa el código especial
        if user_data.get("special_code") == "refoenergy123!":
            user_data["status"] = "active"
            user_data["user_role"] = "master"
        else:
            user_data["status"] = UserStatus.pending  # O el estado por defecto que prefieras
            user_data["user_role"] = UserRole.operator  # O el rol por defecto que prefieras

        user_data["password"] = get_password_hash(user_data["password"])
        user_data["creation_date"] = datetime.now().date()
        user_data.pop("special_code", None)  # Eliminar el código especial de los datos del usuario
        try:
            id_ = await database.execute(user.insert().values(**user_data))
        except Exception as e:
            print(e)  # Agregar logging o manejo de errores según sea necesario
            return None
        user_do = await database.fetch_one(user.select().where(user.c.id == id_))
        return AuthManager.create_access_token(user_do)

    @staticmethod
    def create_access_token(user, expires_delta: Optional[timedelta] = None):
        username = user["username"]
        user_id = user["id"]
        user_role = user["user_role"]
        encode = {"sub": user_id, "name": username, "user_role": user_role}
        if expires_delta:
            expire = datetime.now(timezone.utc) + expires_delta
        else:
            expire = datetime.now(timezone.utc) + timedelta(minutes=30)
        encode.update({"exp": expire})
        return jwt.encode(encode, config("SECRET_KEY"), algorithm="HS256")

    @staticmethod
    async def authenticate_user(username: str, password: str):
        user_do = await database.fetch_one(user.select().where(user.c.username == username))
        if not user_do:
            return False
        if not verify_password(password, user_do["password"]):
            return False
        return user_do

    @staticmethod
    async def get_current_user(request: Request):
        try:
            token = request.cookies.get("access_token")
            if token is None:
                return None
            payload = jwt.decode(token, config("SECRET_KEY"), algorithms=["HS256"])
            username: str = payload.get("name")
            user_id: int = payload.get("sub")
            user_role: str = payload.get("user_role")
            if username is None or user_id is None:
                return None
            return {"name": username, "id": user_id, "user_role": user_role}
        except (jwt.DecodeError, jwt.ExpiredSignatureError):
            return None
