ARG NODE_VERSION=18.17.0

FROM node:${NODE_VERSION}-slim

ADD . /dspace-statistics-api-js

RUN mv /dspace-statistics-api-js/envExample /dspace-statistics-api-js/.env

WORKDIR /dspace-statistics-api-js

RUN npm i
