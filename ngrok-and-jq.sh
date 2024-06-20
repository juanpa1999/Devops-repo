echo -e "\n\n"

echo "#############INSTALANDO NGROK################"

echo -e "\n\n"

snap install ngrok

echo -e "\n\n"

echo "#############INSTALANDO JQ################"

echo -e "\n\n"

sudo apt-get install -y jq

echo -e "\n\n"

echo "#############INSTALANDO NPM################"

echo -e "\n\n"

sudo apt-get install -y npm

cd front/

npm install
