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
  concat,
  concatAll,
  filter,
  firstValueFrom,
  from,
  interval,
  map,
  mergeAll,
  NEVER,
  Observable,
  OperatorFunction,
  share,
  switchMap,
  takeWhile,
  toArray,
} from "rxjs";
import { deserialize } from "../storage/serde";
import { _INTERNAL, TenantHistory, TenantKVTable } from "../storage/kv";
import { ConnectError, HandlerContext } from "@connectrpc/connect";
import { CloudProvider, CloudReplaySubject } from "cloudrx";
import { log } from "../util/log";

export const ONE_DAY_SEC = 24 * 60 * 60;

export type StoredLease = LeaseGrantResponse & {
  tenant: string;
  expires: number;
};
export type RelativeLease = StoredLease & { ttlRelative: number };

const mapLease = (): OperatorFunction<StoredLease, RelativeLease> => {
  return (source: Observable<StoredLease>): Observable<RelativeLease> => {
    return new Observable<RelativeLease>((subscriber) => {
      const subscription = source
        .pipe(
          map((lease) => {
            const relative: RelativeLease = {
              ...lease,
              ttlRelative: Math.min(
                Math.max(lease.expires - CloudProvider.TIME(), 0),
                Number(lease.TTL)
              ),
            };

            if (lease.TTL <= BigInt(0)) {
              relative.ttlRelative = 0;
            }

            return relative;
          })
        )
        .subscribe(subscriber);

      return () => {
        subscription.unsubscribe();
      };
    });
  };
};

export class LeaseHandler extends BaseHandler {
  private _leases: CloudReplaySubject<StoredLease>;
  public readonly leases: Observable<RelativeLease>;

  constructor(ctx: Context) {
    super(ctx);
    this._leases = new CloudReplaySubject<StoredLease>(ctx.leaseStorage, {
      hashFn: (value) => `${value.tenant}:${value.ID}`,
    });
    this.leases = this._leases.pipe(mapLease(), share());

    this._leases.on("expired", (lease) => {
      log("Expired", {
        level: "info",
        tenant: lease.tenant,
        action: "Lease",
        output: lease,
      });
    });
    const subscription = this._leases.subscribe((lease) => {
      log("Updated", {
        level: "info",
        tenant: lease.tenant,
        action: "Lease",
        output: lease,
      });
    });

    ctx.on("abort", () => {
      subscription.unsubscribe();
      this._leases.complete();
    });
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

    const current: StoredLease = {
      $typeName: "etcdserverpb.LeaseGrantResponse",
      ID: BigInt(leaseId),
      TTL: BigInt(ttl),
      expires,
      error: "",
      tenant,
    };

    this._leases.next(current, expires);

    return response;
  }

  public async revoke(
    ctx: HandlerContext | string,
    req: LeaseRevokeRequest
  ): Promise<LeaseRevokeResponse> {
    const tenant = typeof ctx === "string" ? ctx : this.getTenant(ctx);
    const lease = await this.getLease(tenant, Number(req.ID));
    const now = CloudProvider.TIME();

    lease.TTL = BigInt(0);
    lease.expires = now;
    lease.error = "Revoked";

    this._leases.next(lease, now);

    await new Promise<void>((resolve) => {
      const handler = (expired: StoredLease) => {
        if (
          expired.tenant === tenant &&
          BigInt(expired.ID) === BigInt(req.ID)
        ) {
          this._leases.off("expired", handler);
          resolve();
        }
      };
      this._leases.on("expired", handler);
    });

    return {
      $typeName: "etcdserverpb.LeaseRevokeResponse",
      header: await this.header(tenant),
    };
  }

  public async all(tenant: string): Promise<RelativeLease[]> {
    const pages = this._leases
      .snapshot()
      .pipe(
        map((l) =>
          l.filter(
            (l) => l.tenant === tenant && l.expires > CloudProvider.TIME()
          )
        )
      );

    return firstValueFrom(pages.pipe(concatAll(), mapLease(), toArray()));
  }

  public async getLease(
    tenant: string,
    leaseId: number
  ): Promise<RelativeLease> {
    const leases = await this.all(tenant);
    let lease = leases.find((l) => BigInt(l.ID) === BigInt(leaseId));

    if (!lease) {
      return {
        $typeName: "etcdserverpb.LeaseGrantResponse",
        ID: BigInt(leaseId),
        TTL: BigInt(0),
        expires: CloudProvider.TIME(),
        error: "Lease not found",
        tenant,
        ttlRelative: 0,
      };
    }

    return lease;
  }

  public async timeToLive(
    ctx: HandlerContext | string,
    req: LeaseTimeToLiveRequest,
    kv: TenantKVTable
  ): Promise<LeaseTimeToLiveResponse> {
    const tenant = typeof ctx === "string" ? ctx : this.getTenant(ctx);
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
      "Lease",
      ctx,
      {
        // history: this.history$(this.getTenant(ctx)),
        history: NEVER, // No history for keepAlive
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
          return () => NEVER;
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
        onResponse: async (tenant, connectionId, res) => {
          res = _.cloneDeep(res);
          res.response.header = await this.header(tenant);
          return res;
        },
        onEnd: async (tenant, connectionId) => {},
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
