# The load generator: artillery driven by app/app.js, reporting stats back to
# predator. node:24-alpine publishes linux/amd64 and linux/arm64.
FROM node:24-alpine

RUN mkdir -p /usr/app
WORKDIR /usr

COPY package.json pnpm-lock.yaml pnpm-workspace.yaml /usr/
# artillery-plugin-predator is a file: dependency — it must exist before install
# or the install leaves a dead symlink and artillery silently runs without it.
COPY plugin /usr/plugin
COPY engines /usr/engines

# Predator never uses artillery's Playwright engine — skip the Chromium
# download its postinstall attempts (unsupported on alpine/arm64 anyway).
ENV PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1

# git: pnpm clones artillery and its plugins from their git URLs.
RUN corepack enable && \
    apk add --no-cache bash git && \
    pnpm install --prod --no-optional --frozen-lockfile && \
    # artillery v2's tree ships TypeScript sources, source maps and test dirs —
    # hundreds of MB that the runner never reads.
    find node_modules \( -name '*.ts' -o -name '*.js.map' -o -name '*.md' \) -type f -delete && \
    pnpm store prune

COPY /app /usr/app

EXPOSE 8080

CMD [ "node", "--max_old_space_size=192", "./app/app.js" ]
