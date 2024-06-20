import sqlalchemy
from db import database
from fastapi import HTTPException
from models import user


class AdminManager:

    @staticmethod
    async def get_all_users():
        try:
            query = sqlalchemy.select([user])
            result = await database.fetch_all(query)
            return result
        except Exception as e:
            error_message = f"Failed to get all users: {str(e)}"
            print(error_message)
            raise HTTPException(status_code=500, detail=error_message)

    @staticmethod
    async def update_user_status(user_id: int, new_status: str):
        # Check if the user exists
        select_query = sqlalchemy.select([user]).where(user.c.id == user_id)
        user_result = await database.fetch_one(select_query)

        if user_result is None:
            # User not found
            raise HTTPException(status_code=404, detail="User not found")

        try:
            # User exists, proceed with updating the status
            update_query = sqlalchemy.update(user).where(user.c.id == user_id).values(status=new_status)
            await database.execute(update_query)
            return {"message": "User status updated successfully"}
        except Exception as e:
            error_message = f"Failed to update user status: {str(e)}"
            print(error_message)
            raise HTTPException(status_code=500, detail=error_message)

    @staticmethod
    async def delete_user(user_id: int):
        select_query = sqlalchemy.select([user]).where(user.c.id == user_id)
        user_result = await database.fetch_one(select_query)

        if user_result is None:
            raise HTTPException(status_code=404, detail="User not found")

        try:
            delete_query = sqlalchemy.delete(user).where(user.c.id == user_id)
            await database.execute(delete_query)
            return {"message": "User deleted successfully"}
        except Exception as e:
            error_message = f"Failed to delete user: {str(e)}"
            print(error_message)
            raise HTTPException(status_code=500, detail=error_message)



