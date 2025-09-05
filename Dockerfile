ARG BUILDPLATFORM=linux/amd64
FROM --platform=${BUILDPLATFORM} node:22-alpine AS full
RUN apk add --no-cache git

WORKDIR /work
COPY . .

# The Server
RUN --mount=type=cache,target=/usr/local/share/.cache \
    yarn

RUN mkdir proto && cp /work/node_modules/etcd3/proto/* /work/proto/
COPY --from=gcr.io/etcd-development/etcd:v3.6.4 /usr/local/bin/etcdctl /usr/local/bin/etcdctl

ENV CERTFILE=/work/src/certs/localhost.crt
ENV KEYFILE=/work/src/certs/localhost.key

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
    cp server-linuxstatic-${ARCH} server && \
    chmod +x server && \
    ./server --version

FROM scratch
COPY --from=gcr.io/etcd-development/etcd:v3.6.4 /usr/local/bin/etcdctl /usr/local/bin/etcdctl
COPY --from=arch /work/server /usr/local/bin/etcd
COPY --from=full /work/src/certs/localhost.crt /etc/ssl/certs/localhost.crt
COPY --from=full /work/src/certs/localhost.key /etc/ssl/private/localhost.key
ENV CERTFILE=/etc/ssl/certs/localhost.crt
ENV KEYFILE=/etc/ssl/private/localhost.key
ENTRYPOINT [ "etcd" ]
EXPOSE 2379
