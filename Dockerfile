FROM node:22 AS build
WORKDIR /work
COPY . .
ENV PKG_CACHE_PATH=/usr/local/share/.cache/pkg
RUN --mount=type=cache,target=/usr/local/share/.cache \
    yarn && \
    yarn build:exe
RUN ARCH=$(node -e "console.log(process.arch)") && \
    cp dist/server-${ARCH} dist/server && \
    chmod +x dist/server && \
    ./dist/server --version

FROM scratch
COPY --from=build /work/dist/server /server
COPY src/certs/localhost.key /src/certs/localhost.key
COPY src/certs/localhost.crt /src/certs/localhost.crt
ENTRYPOINT [ "/server" ]
EXPOSE 2379
