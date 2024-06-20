#!/bin/bash

sed -i '/^NGROK=/d' back/.env

# Iniciar Ngrok y redirigir la salida a un archivo
# ngrok start --config (INGRESA LA RUTA DEL ARCHIVO DE CONFIGURACION)/ngrok2.yml api > /dev/null &
ngrok start --config /home/jpcc141999/.config/ngrok/ngrok2.yml api > /dev/null &

# Esperar un momento para que Ngrok se inicie
sleep 2

# Obtener la URL pública de Ngrok
NGROK=$(curl -s http://localhost:4040/api/tunnels | jq -r '.tunnels[0].public_url')

# Imprimir la URL pública
echo "Ngrok URL: $NGROK"

# Guardar la URL en un archivo back/.env
echo "NGROK=$NGROK" >> back/.env

# Imprimir el contenido del archivo .env para confirmar
cat back/.env

sleep 10

echo -e "\n\n"

echo "##########  BACK RUNNING ###########"

sleep 3

sed -i '/^REACT_APP_API_URL=/d' front/.env
sed -i '/^REACT_APP_WS_BASE_URL=/d' front/.env

sleep 2

# Iniciar Ngrok y redirigir la salida a un archivo
ngrok start --config /home/jpcc141999/.config/ngrok/ngrok.yml web > /dev/null &

# Esperar un momento para que Ngrok se inicie
sleep 2

# Obtener la URL pública de Ngrok
REACT_APP_API_URL=$(curl -s http://localhost:4041/api/tunnels | jq -r '.tunnels[0].public_url')

REACT_APP_WS_BASE_URL=$(echo $REACT_APP_API_URL | sed 's/https/wss/')

# Imprimir las URLs públicas
echo "Ngrok URL: $REACT_APP_API_URL"
echo "WebSocket URL: $REACT_APP_WS_BASE_URL"

# Guardar las URLs en un archivo front/.env
echo "REACT_APP_API_URL=$REACT_APP_API_URL" >> front/.env
echo "REACT_APP_WS_BASE_URL=$REACT_APP_WS_BASE_URL" >> front/.env

# Imprimir el contenido del archivo .env para confirmar
cat front/.env

docker stop back_cont
docker start back_cont 


docker stop front_cont
docker start front_cont 