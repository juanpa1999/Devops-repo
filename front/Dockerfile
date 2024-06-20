FROM node:latest

WORKDIR /usr/src/app


COPY package*.json ./
RUN npm install

COPY . .

EXPOSE 3000

CMD ["npm", "start"]

#docker build -t front_img .

#docker run -d --name front_cont -p 3000:3000 -v /home/jpcc141999/project/front/:/usr/src/app front_img
