import { ConnectRouter } from "@connectrpc/connect";
import {
  Auth,
  Cluster,
  KV,
  Lease,
  Maintenance,
  Watch,
} from "@setcd-io/connectrpc-etcd";
import { KVHandler } from "./handlers/kv";
import { WatchHandler } from "./handlers/watch";
import { LeaseHandler } from "./handlers/lease";
import { MaintenanceHandler } from "./handlers/maintenance";
import { ClusterHandler } from "./handlers/cluster";
// import { log } from "./handlers/base";
import { AuthHandler } from "./handlers/auth";
import { TENANT } from "./util/const";
import {
  asyncScheduler,
  defer,
  firstValueFrom,
  from,
  observeOn,
  queueScheduler,
} from "rxjs";

const schedule = <T>(incoming: Promise<T>): Promise<T> => {
  const task = defer(() => incoming).pipe(observeOn(asyncScheduler));
  return firstValueFrom(task);
};

export const createRouter = (handlers: {
  auth: AuthHandler;
  kv: KVHandler;
  lease: LeaseHandler;
  watch: WatchHandler;
  maintenance: MaintenanceHandler;
  cluster: ClusterHandler;
}) => {
  return (router: ConnectRouter) => {
    return router
      .service(Auth, {
        authenticate(req, ctx) {
          return schedule(handlers.auth.authenticate(req));
        },
      })
      .service(Cluster, {
        memberList(req, ctx) {
          return schedule(
            handlers.cluster.members(ctx.values.get(TENANT), req, ctx)
          );
        },
      })
      .service(KV, {
        put(req, ctx) {
          return schedule(handlers.kv.put(ctx.values.get(TENANT), req));
        },
        deleteRange(req, ctx) {
          return schedule(handlers.kv.deleteRange(ctx.values.get(TENANT), req));
        },
        range(req, ctx) {
          return schedule(handlers.kv.range(ctx.values.get(TENANT), req));
        },
        compact(req, ctx) {
          return schedule(handlers.kv.compact(ctx.values.get(TENANT), req));
        },
        txn(req, ctx) {
          return schedule(handlers.kv.transact(ctx.values.get(TENANT), req));
        },
      })
      .service(Lease, {
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
          return schedule(handlers.lease.timeToLive(ctx, req));
        },
        leaseLeases(req, ctx) {
          return schedule(handlers.lease.listLeases(ctx, req));
        },
      })
      .service(Maintenance, {
        alarm(req, ctx) {
          return schedule(
            handlers.maintenance.alarm(ctx.values.get(TENANT), req)
          );
        },
        status(req, ctx) {
          return schedule(
            handlers.maintenance.status(ctx.values.get(TENANT), req)
          );
        },
      })
      .service(Watch, {
        watch: (req, ctx) => {
          return handlers.watch.watch(ctx, req);
        },
      });
  };
};
