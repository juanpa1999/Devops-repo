docker rm -f $(docker ps -aq)
sleep 1
echo "contnedor eliminado"
docker ps -a
sleep 1

docker rmi $(docker images -aq)

sleep 1
docker images
echo "imagenes eliminadas"
sleep 1
