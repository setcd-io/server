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

ENV ETCD_TRUSTED_CA_FILE=/work/src/certs/ca.crt
ENV ETCD_CERT_FILE=/work/src/certs/localhost.crt
ENV ETCD_KEY_FILE=/work/src/certs/localhost.key

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
COPY --from=arch /work/server /usr/local/bin/setcd
COPY --from=full /work/src/certs/ca.crt /etc/ssl/certs/ca.crt
COPY --from=full /work/src/certs/localhost.crt /etc/ssl/certs/localhost.crt
COPY --from=full /work/src/certs/localhost.key /etc/ssl/private/localhost.key
ENV ETCD_TRUSTED_CA_FILE=/etc/ssl/certs/ca.crt
ENV ETCD_CERT_FILE=/etc/ssl/certs/localhost.crt
ENV ETCD_KEY_FILE=/etc/ssl/private/localhost.key
ENV ETCDCTL_CACERT=/etc/ssl/certs/ca.crt
ENV ETCDCTL_CERT=/etc/ssl/certs/localhost.crt
ENV ETCDCTL_KEY=/etc/ssl/private/localhost.key
ENTRYPOINT [ "setcd" ]
EXPOSE 2379
