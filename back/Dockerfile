FROM python:3.9

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

#RUN alembic upgrade head


#RUN alembic revision --autogenerate -m "Initial migration trough docker file in FastAPI"

EXPOSE 8000

CMD ["uvicorn", "main:app", "--reload", "--host", "0.0.0.0", "--port", "8000"]


#docker build -t back_img .

#con variables de entorno para la base de datos y bind mount este es el comando

#docker run -d --name back_cont -p 8000:8000 -e DB_HOST=db_cont -v /home/jpcc141999/project/back/:/app back_img

#resuerde agregarlo a la misma red del contenedor

#docker network connect #red# #contenedor#
