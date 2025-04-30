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
COPY --from=reduce /work/server /usr/bin/server
COPY --from=build /work/src/certs/localhost.crt /etc/ssl/certs/localhost.crt
COPY --from=build /work/src/certs/localhost.key /etc/ssl/private/localhost.key
ENV CERTDIR=/etc/ssl
ENV CERTFILE=certs/localhost.crt
ENV KEYFILE=private/localhost.key
ENTRYPOINT [ "server" ]
EXPOSE 2379
