FROM node:23-slim

RUN apt-get update && apt-get install -y curl


WORKDIR /app

COPY . .

RUN npm install

RUN chmod +x entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]