FROM node:22

WORKDIR /app
COPY package.json package-lock.json /app/
RUN npm install

COPY . /app/

ARG NODE_ENV=staging
ENV NODE_ENV=$NODE_ENV
CMD ["/app/entrypoint.sh"]
