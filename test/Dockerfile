FROM node:20 AS build
ENV NPX_CACHE=/cache/npx
ENV PKG_CACHE_PATH=/cache/pkg
WORKDIR /work
COPY . .
RUN yarn
RUN yarn build

# Final
FROM node:20
COPY --from=build /work/dist/server.js /server.js
COPY src/certs/localhost.key /src/certs/localhost.key
COPY src/certs/localhost.crt /src/certs/localhost.crt
ENTRYPOINT [ "node", "/server.js" ]
EXPOSE 2379