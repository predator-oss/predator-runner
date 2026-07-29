# The load generator: artillery driven by app/app.js, reporting stats back to
# predator. node:24-alpine publishes linux/amd64 and linux/arm64.
FROM node:24-alpine

RUN mkdir -p /usr/app
WORKDIR /usr

COPY package.json package-lock.json /usr/
# artillery-plugin-predator is a file: dependency — it must exist before npm ci
# or the install leaves a dead symlink and artillery silently runs without it.
COPY plugin /usr/plugin

# Predator never uses artillery's Playwright engine — skip the Chromium
# download its postinstall attempts (unsupported on alpine/arm64 anyway).
ENV PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1

RUN apk add --no-cache bash && \
    npm ci --omit=dev --omit=optional && \
    # artillery v2's tree ships TypeScript sources, source maps and test dirs —
    # hundreds of MB that the runner never reads.
    find node_modules \( -name '*.ts' -o -name '*.js.map' -o -name '*.md' \) -type f -delete && \
    npm cache clean --force

COPY /app /usr/app

EXPOSE 8080

CMD [ "node", "--max_old_space_size=192", "./app/app.js" ]
