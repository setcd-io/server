ARG BUILDPLATFORM=linux/amd64
FROM --platform=${BUILDPLATFORM} node:22 AS build
WORKDIR /work
COPY . .
ENV PKG_CACHE_PATH=/usr/local/share/.cache/pkg
RUN --mount=type=cache,target=/usr/local/share/.cache \
    yarn && \
    yarn build:exe

FROM node:22 AS reduce
WORKDIR /work
COPY --from=build /work/dist/* /work/
RUN ARCH=$(node -e "console.log(process.arch)") && \
    cp server-${ARCH} server && \
    chmod +x server && \
    ./server --version

FROM scratch
COPY --from=reduce /work/server /server
COPY src/certs/localhost.key /src/certs/localhost.key
COPY src/certs/localhost.crt /src/certs/localhost.crt
ENTRYPOINT [ "/server" ]
EXPOSE 2379
