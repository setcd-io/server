FROM node:18 AS build
ENV NPX_CACHE=/cache/npx
ENV PKG_CACHE_PATH=/cache/pkg
WORKDIR /work
COPY . .
RUN npm install
RUN npm run build

# Final
FROM node:18
COPY --from=build /work/dist/server.js /server.js
COPY server/certs/localhost.key /server/certs/localhost.key
COPY server/certs/localhost.crt /server/certs/localhost.crt
ENTRYPOINT [ "node", "/server.js" ]
EXPOSE 2379