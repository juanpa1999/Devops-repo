#!/bin/bash

cd db
docker build -t db_img .
docker run -d --name db_cont -p 5432:5432 db_img
docker network create dnet
docker network connect dnet db_cont


cd ../back
docker build -t back_img .
docker run -d --name back_cont -p 8000:8000 -e DB_HOST=db_cont -v "$PWD:/app" back_img
docker network connect dnet back_cont

docker stop back_cont
docker start back_cont

docker exec -it back_cont alembic init alembic
docker exec -it back_cont alembic revision --autogenerate
docker exec -it back_cont alembic upgrade head


cd ../front
docker build -t front_img .
docker run -d --name front_cont -p 3000:3000 -v "$PWD:/usr/src/app" front_img
docker stop front_cont
docker start front_cont
