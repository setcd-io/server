ARG BUILDPLATFORM=linux/amd64
FROM --platform=${BUILDPLATFORM} node:22-alpine AS full
RUN apk add --no-cache git

WORKDIR /work
COPY . .

# The Server
RUN --mount=type=cache,target=/usr/local/share/.cache \
    yarn

RUN mkdir proto && cp /work/node_modules/etcd3/proto/* /work/proto/
COPY --from=bitnami/etcd:3.6.4 /opt/bitnami/etcd/bin/etcdctl /usr/bin/etcdctl

ENV CERTDIR=/work/src/certs
ENV CERTFILE=localhost.crt
ENV KEYFILE=localhost.key

ENTRYPOINT [ "yarn" ]
CMD [ "start:dev" ]
EXPOSE 2379

FROM --platform=${BUILDPLATFORM} node:22-alpine AS exe
WORKDIR /work
COPY --from=full /work /work
ENV PKG_CACHE_PATH=/usr/local/share/.cache/pkg
RUN --mount=type=cache,target=/usr/local/share/.cache \
    yarn && \
    yarn build:exe

FROM node:22-alpine AS arch
WORKDIR /work
COPY --from=exe /work/dist/* /work/
RUN ARCH=$(node -e "console.log(process.arch)") && \
    cp server-${ARCH} server && \
    chmod +x server && \
    ./server --version

FROM scratch
COPY --from=bitnami/etcd:3.6.4 /opt/bitnami/etcd/bin/etcdctl /usr/bin/etcdctl
COPY --from=arch /work/server /usr/bin/server
COPY --from=full /work/src/certs/localhost.crt /etc/ssl/certs/localhost.crt
COPY --from=full /work/src/certs/localhost.key /etc/ssl/private/localhost.key
ENV CERTDIR=/etc/ssl
ENV CERTFILE=certs/localhost.crt
ENV KEYFILE=private/localhost.key
ENTRYPOINT [ "server" ]
EXPOSE 2379
