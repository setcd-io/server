import { BaseHandler, StreamResponse } from "./base";
import Context from "../context";
import {
  LeaseGrantRequest,
  LeaseGrantResponse,
  LeaseKeepAliveRequest,
  LeaseKeepAliveResponse,
  LeaseLeasesRequest,
  LeaseLeasesResponse,
  LeaseRevokeRequest,
  LeaseRevokeResponse,
  LeaseStatus,
  LeaseTimeToLiveRequest,
  LeaseTimeToLiveResponse,
} from "@setcd-io/connectrpc-etcd";
import _ from "lodash";
import {
  asyncScheduler,
  concat,
  concatAll,
  EMPTY,
  filter,
  firstValueFrom,
  from,
  interval,
  map,
  mergeAll,
  mergeMap,
  Observable,
  observeOn,
  OperatorFunction,
  share,
  switchMap,
  takeUntil,
  takeWhile,
  tap,
  toArray,
} from "rxjs";
import { deserialize, serialize } from "../storage/serde";
import { _INTERNAL, TenantHistory, TenantKVTable } from "../storage/kv";
import { ConnectError, HandlerContext } from "@connectrpc/connect";
import { CloudProvider, CloudReplaySubject } from "cloudrx";
import { log } from "../util/log";

export const ONE_DAY_SEC = 24 * 60 * 60;

export type AbsoluteLease = LeaseGrantResponse & { expires: number };
export type RelativeLease = AbsoluteLease & { ttlRelative: number };

export class LeaseHandler extends BaseHandler {
  private leases: CloudReplaySubject<TenantHistory<AbsoluteLease>>;

  constructor(ctx: Context) {
    super(ctx);
    this.leases = new CloudReplaySubject<TenantHistory<AbsoluteLease>>(
      ctx.leaseStorage,
      {
        hashFn: (value) => `${value.tenant}:${value.current.ID}`,
      }
    );
    this.leases.on("expired", (h) => {
      log(h.current, {
        level: "info",
        tenant: h.tenant,
        action: "LeaseExpire",
        context: {
          action: h.action,
        },
      });
      queueMicrotask(() =>
        this.revoke(h.tenant, {
          $typeName: "etcdserverpb.LeaseRevokeRequest",
          ID: BigInt(h.current.ID),
        })
      );
    });
  }

  private history$(tenant: string): Observable<TenantHistory<AbsoluteLease>[]> {
    return this.leases.pipe(
      filter((h) => h.tenant === tenant),
      tap((h) => {
        log(h.previous, {
          level: "info",
          action: "LeaseHistory",
          tenant: h.tenant,
          output: h.current,
          context: {
            action: h.action,
          },
        });
      }),
      map((h) => [h]),
      share()
    );
  }

  async grant(
    ctx: HandlerContext,
    req: LeaseGrantRequest
  ): Promise<LeaseGrantResponse> {
    const tenant = this.getTenant(ctx);

    if (req.TTL >= ONE_DAY_SEC) {
      throw new ConnectError("Lease TTL must be less than 1 day");
    }

    if (req.ID !== BigInt(0)) {
      throw new ConnectError("Unsupported: Client specified Lease ID");
    }

    const leaseId = req.ID || (await this.ctx.nextLease(tenant));
    const ttl = Number(req.TTL);
    const expires = CloudProvider.TIME() + ttl;

    const response: LeaseGrantResponse = {
      $typeName: "etcdserverpb.LeaseGrantResponse",
      header: await this.header(tenant),
      ID: BigInt(leaseId),
      TTL: BigInt(ttl),
      error: "",
    };

    const absolute: AbsoluteLease = {
      ...response,
      expires,
    };

    this.leases.next({ tenant, action: "PUT", current: absolute }, expires);

    return response;
  }

  public async revoke(
    ctx: HandlerContext | string,
    req: LeaseRevokeRequest
  ): Promise<LeaseRevokeResponse> {
    const tenant = typeof ctx === "string" ? ctx : this.getTenant(ctx);
    const lease = await this.getLease(tenant, Number(req.ID));
    const now = CloudProvider.TIME();

    this.leases.next(
      {
        tenant,
        action: "DELETE",
        current: { ...lease, expires: now, TTL: BigInt(0), error: "Revoked" },
        previous: lease,
      },
      now
    );

    return {
      $typeName: "etcdserverpb.LeaseRevokeResponse",
      header: await this.header(tenant),
    };
  }

  public async all(tenant: string): Promise<AbsoluteLease[]> {
    const pages = this.leases
      .snapshot()
      .pipe(map((l) => l.filter((l) => l.tenant === tenant)));

    return firstValueFrom(
      pages.pipe(
        concatAll(),
        toArray(),
        map((l) => l.map((h) => h.current))
      )
    );
  }

  public async getLease(
    tenant: string,
    leaseId: number
  ): Promise<RelativeLease> {
    const leases = await this.all(tenant);
    let lease = leases.find((l) => BigInt(l.ID) === BigInt(leaseId));
    const now = CloudProvider.TIME();

    if (!lease) {
      lease = {
        $typeName: "etcdserverpb.LeaseGrantResponse",
        ID: BigInt(leaseId),
        TTL: BigInt(0),
        expires: now,
        error: "Not Found",
      };
    }

    const relativeLease: RelativeLease = {
      ...lease,
      ttlRelative: Math.min(
        Math.max(lease.expires - now, 0),
        Number(lease.TTL)
      ),
    };

    return relativeLease;
  }

  public async timeToLive(
    ctx: HandlerContext,
    req: LeaseTimeToLiveRequest,
    kv: TenantKVTable
  ): Promise<LeaseTimeToLiveResponse> {
    const tenant = this.getTenant(ctx);
    const lease = await this.getLease(tenant, Number(req.ID));
    const now = CloudProvider.TIME();

    const ttlRelative = Math.min(
      Math.max(lease.expires - now, 0),
      Number(lease.TTL)
    );

    let keys: Uint8Array[] = [];
    if (req.keys) {
      // Full search with lease
      const { kvs } = await kv.range(
        tenant,
        {
          $typeName: "etcdserverpb.RangeRequest",
          key: new Uint8Array(1),
          rangeEnd: new Uint8Array(1),
        },
        { leaseId: Number(lease.ID) }
      );

      keys = kvs.map((kv) => deserialize(kv.key, true));
    }

    return {
      $typeName: "etcdserverpb.LeaseTimeToLiveResponse",
      header: await this.header(tenant),
      grantedTTL: BigInt(lease.TTL),
      ID: BigInt(lease.ID),
      TTL: BigInt(ttlRelative),
      keys,
    };
  }

  public async listLeases(
    ctx: HandlerContext,
    req: LeaseLeasesRequest
  ): Promise<LeaseLeasesResponse> {
    const tenant = this.getTenant(ctx);
    const leases = await this.all(tenant);

    return {
      $typeName: "etcdserverpb.LeaseLeasesResponse",
      header: await this.header(tenant),
      leases: leases.map((lease) => {
        const status: LeaseStatus = {
          $typeName: "etcdserverpb.LeaseStatus",
          ID: BigInt(lease.ID),
        };
        return status;
      }),
    };
  }

  public keepAlive(
    ctx: HandlerContext,
    requests: AsyncIterable<LeaseKeepAliveRequest>
  ): AsyncGenerator<LeaseKeepAliveResponse, void, unknown> {
    return this.bidi(
      ctx,
      {
        history: this.history$(this.getTenant(ctx)),
        requests,
      },
      {
        history: (his) => his.tenant === this.getTenant(ctx),
        response: (res) => res.tenant === this.getTenant(ctx),
      },
      {
        requestToResponse: (tenant, connectionId, requestId, signal) => {
          return this.mapRequestToResponse(
            tenant,
            connectionId,
            requestId,
            signal
          );
        },
        historyToResponse: (tenant, connectionId, requestId, signal) => {
          return this.mapHistoryToResponse(
            tenant,
            connectionId,
            requestId,
            signal
          );
        },
        errorToResponse: (tenant, connectionId, requestId, signal) => {
          return this.mapErrorToResponse(
            tenant,
            connectionId,
            requestId,
            signal
          );
        },
      },
      {
        response: async (tenant, connectionId, res) => {
          res = _.cloneDeep(res);
          res.response.header = await this.header(tenant);
          return res;
        },
      }
    );
  }

  mapRequestToResponse(
    tenant: string,
    connectionId: string,
    requestId: string,
    signal: AbortSignal
  ): OperatorFunction<
    LeaseKeepAliveRequest,
    StreamResponse<LeaseKeepAliveRequest, LeaseKeepAliveResponse>
  > {
    return (
      source: Observable<LeaseKeepAliveRequest>
    ): Observable<
      StreamResponse<LeaseKeepAliveRequest, LeaseKeepAliveResponse>
    > => {
      return new Observable<
        StreamResponse<LeaseKeepAliveRequest, LeaseKeepAliveResponse>
      >((subscriber) => {
        const subscription = source
          .pipe(
            switchMap((source) => {
              const immediate = from(
                this.getLease(tenant, Number(source.ID))
              ).pipe(
                map((lease) => {
                  const keepAlive: LeaseKeepAliveResponse = {
                    $typeName: "etcdserverpb.LeaseKeepAliveResponse",
                    ID: source.ID,
                    TTL: BigInt(lease.ttlRelative),
                  };
                  return { source, keepAlive };
                })
              );

              const loop = interval(1000).pipe(
                switchMap(() => this.getLease(tenant, Number(source.ID))),
                takeWhile((lease) => lease.ttlRelative > 0, true),
                map((lease) => {
                  const keepAlive: LeaseKeepAliveResponse = {
                    $typeName: "etcdserverpb.LeaseKeepAliveResponse",
                    ID: source.ID,
                    TTL: BigInt(lease.ttlRelative),
                  };
                  return { source, keepAlive };
                })
              );

              return concat(immediate, loop);
            }),
            map(({ source, keepAlive }) => {
              const response: StreamResponse<
                LeaseKeepAliveRequest,
                LeaseKeepAliveResponse
              > = {
                tenant,
                connectionId,
                requestId,
                request: source,
                response: keepAlive,
                signal,
              };
              return response;
            })
          )
          .subscribe({
            next(response) {
              subscriber.next(response);
            },
            error(err) {
              subscriber.error(err);
            },
            complete() {
              subscriber.complete();
            },
          });

        return () => {
          subscription.unsubscribe();
        };
      });
    };
  }

  mapHistoryToResponse(
    tenant: string,
    connectionId: string,
    requestId: string,
    signal: AbortSignal
  ): OperatorFunction<
    TenantHistory<AbsoluteLease>[],
    StreamResponse<LeaseKeepAliveRequest, LeaseKeepAliveResponse>
  > {
    return (
      source: Observable<TenantHistory<AbsoluteLease>[]>
    ): Observable<
      StreamResponse<LeaseKeepAliveRequest, LeaseKeepAliveResponse>
    > => {
      return new Observable<
        StreamResponse<LeaseKeepAliveRequest, LeaseKeepAliveResponse>
      >((subscriber) => {
        const subscription = source
          .pipe(
            mergeAll(),
            filter((h) => h.tenant === tenant && h.action === "DELETE"),
            map((history) => {
              const response: StreamResponse<
                LeaseKeepAliveRequest,
                LeaseKeepAliveResponse
              > = {
                tenant: tenant,
                connectionId,
                requestId,
                request: {
                  $typeName: "etcdserverpb.LeaseKeepAliveRequest",
                  ID: BigInt(history.current.ID),
                },
                response: {
                  $typeName: "etcdserverpb.LeaseKeepAliveResponse",
                  ID: BigInt(history.current.ID),
                  TTL: BigInt(0),
                },
                signal,
              };
              return response;
            })
          )
          .subscribe({
            next(response) {
              subscriber.next(response);
            },
            error(err) {
              subscriber.error(err);
            },
            complete() {
              subscriber.complete();
            },
          });

        return () => {
          subscription.unsubscribe();
        };
      });
    };
  }

  mapErrorToResponse(
    tenant: string,
    connectionId: string,
    requestId: string,
    signal: AbortSignal
  ): OperatorFunction<
    Error,
    StreamResponse<LeaseKeepAliveRequest, LeaseKeepAliveResponse>
  > {
    return (
      source: Observable<Error>
    ): Observable<
      StreamResponse<LeaseKeepAliveRequest, LeaseKeepAliveResponse>
    > => {
      return new Observable<
        StreamResponse<LeaseKeepAliveRequest, LeaseKeepAliveResponse>
      >((subscriber) => {
        const subscription = source.subscribe({
          next(err) {
            log("Lease Error", {
              level: "warn",
              tenant,
              action: "Lease",
              output: err.message,
              context: { con: connectionId, req: requestId },
            });
          },
          error(err) {
            subscriber.error(err);
          },
          complete() {
            subscriber.complete();
          },
        });

        return () => {
          subscription.unsubscribe();
        };
      });
    };
  }
}
