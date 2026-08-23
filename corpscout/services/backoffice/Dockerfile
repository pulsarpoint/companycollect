FROM node:24-alpine AS development-dependencies-env
RUN npm install -g pnpm@9
COPY package.json pnpm-lock.yaml /app/
WORKDIR /app
RUN pnpm install --frozen-lockfile

FROM node:24-alpine AS production-dependencies-env
RUN npm install -g pnpm@9
COPY package.json pnpm-lock.yaml /app/
WORKDIR /app
RUN pnpm install --frozen-lockfile --prod

FROM node:24-alpine AS build-env
RUN npm install -g pnpm@9
COPY . /app/
COPY --from=development-dependencies-env /app/node_modules /app/node_modules
WORKDIR /app
RUN pnpm build

FROM node:24-alpine
COPY package.json /app/
COPY --from=production-dependencies-env /app/node_modules /app/node_modules
COPY --from=build-env /app/build /app/build
COPY --from=build-env /app/content /app/content
WORKDIR /app
CMD ["./node_modules/.bin/react-router-serve", "./build/server/index.js"]
