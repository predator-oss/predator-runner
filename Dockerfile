# The load generator: artillery driven by app/app.js, reporting stats back to
# predator. node:24-alpine publishes linux/amd64 and linux/arm64.
FROM node:24-alpine

RUN mkdir -p /usr/app
WORKDIR /usr

COPY package.json package-lock.json /usr/

# git: artillery and two plugins are installed from git URLs.
RUN apk add --no-cache bash git openssh && \
    npm ci --omit=dev --omit=optional

COPY /app /usr/app

EXPOSE 8080

CMD [ "node", "--max_old_space_size=192", "./app/app.js" ]
