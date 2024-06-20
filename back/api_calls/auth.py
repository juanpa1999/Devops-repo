from fastapi import APIRouter, Response, HTTPException, status
from managers.auth import AuthManager
from datetime import timedelta
from schemas.request.user_input_data import UserRegistrationData, UserLoginData

router = APIRouter(prefix="/auth", tags=["auth"], responses={401: {"user": "Not authorized"}})


@router.post("/register", description="Allows to register new users {operators} only")
async def register_user(user_data: UserRegistrationData):
    user_dict = user_data.model_dump()
    user = await AuthManager.register(user_dict)

    if user is not None:
        return {"message": "user created successfully", "access_token": user}
    else:
        raise HTTPException(status_code=400, detail="Unable to register user or username already exists")


@router.post("/login", description="Allows to users to login")
async def login(user_data: UserLoginData, response: Response):
    return await login_for_access_token(user_data, response)


@router.post("/token", description="Generates the jwt access token in cookie {check payload content}")
async def login_for_access_token(user_data: UserLoginData, response: Response):
    user = await AuthManager.authenticate_user(user_data.username, user_data.password)
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Incorrect username or password")

    # Check if user's status is 'active'
    if user["status"] != "pending":
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not active, please contact admin")

    token_expires = timedelta(minutes=120)
    token = AuthManager.create_access_token(user, expires_delta=token_expires)
    response.set_cookie(key="access_token", value=token, httponly=True, samesite="none", secure=True)
    return {"access_token": token, "token_type": "bearer"}


@router.post("/logout", description="Deletes user cookie with JWT")
async def logout(response: Response):
    response.delete_cookie(key="access_token")
    return {"message": "Logout successful"}