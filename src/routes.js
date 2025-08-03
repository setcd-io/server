"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.createRouter = void 0;
const connectrpc_etcd_1 = require("@setcd-io/connectrpc-etcd");
const const_1 = require("./util/const");
const rxjs_1 = require("rxjs");
const schedule = (incoming) => {
    const task = (0, rxjs_1.defer)(() => incoming).pipe((0, rxjs_1.observeOn)(rxjs_1.asyncScheduler));
    return (0, rxjs_1.firstValueFrom)(task);
};
const createRouter = (handlers) => {
    return (router) => {
        return router
            .service(connectrpc_etcd_1.Auth, {
            authenticate(req, ctx) {
                return schedule(handlers.auth.authenticate(req));
            },
        })
            .service(connectrpc_etcd_1.Cluster, {
            memberList(req, ctx) {
                return schedule(handlers.cluster.members(ctx.values.get(const_1.TENANT), req, ctx));
            },
        })
            .service(connectrpc_etcd_1.KV, {
            put(req, ctx) {
                return schedule(handlers.kv.put(ctx.values.get(const_1.TENANT), req));
            },
            deleteRange(req, ctx) {
                return schedule(handlers.kv.deleteRange(ctx.values.get(const_1.TENANT), req));
            },
            range(req, ctx) {
                return schedule(handlers.kv.range(ctx.values.get(const_1.TENANT), req));
            },
            compact(req, ctx) {
                return schedule(handlers.kv.compact(ctx.values.get(const_1.TENANT), req));
            },
            txn(req, ctx) {
                return schedule(handlers.kv.transact(ctx.values.get(const_1.TENANT), req));
            },
        })
            .service(connectrpc_etcd_1.Lease, {
            leaseGrant(req, ctx) {
                return schedule(handlers.lease.grant(ctx, req));
            },
            leaseRevoke(req, ctx) {
                return schedule(handlers.lease.revoke(ctx, req));
            },
            leaseKeepAlive(req, ctx) {
                return handlers.lease.keepAlive(ctx, req);
            },
            leaseTimeToLive(req, ctx) {
                return schedule(handlers.lease.timeToLive(ctx, req, handlers.kv.kv));
            },
            leaseLeases(req, ctx) {
                return schedule(handlers.lease.listLeases(ctx, req));
            },
        })
            .service(connectrpc_etcd_1.Maintenance, {
            alarm(req, ctx) {
                return schedule(handlers.maintenance.alarm(ctx.values.get(const_1.TENANT), req));
            },
            status(req, ctx) {
                return schedule(handlers.maintenance.status(ctx.values.get(const_1.TENANT), req));
            },
        })
            .service(connectrpc_etcd_1.Watch, {
            watch: (req, ctx) => {
                return handlers.watch.watch(ctx, req);
            },
        });
    };
};
exports.createRouter = createRouter;
