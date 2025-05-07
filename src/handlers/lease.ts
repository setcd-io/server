import { BaseHandler, StreamRequest, StreamResponse } from "./base";
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
  LeaseTimeToLiveRequest,
  LeaseTimeToLiveResponse,
} from "@setcd-io/connectrpc-etcd";
import _ from "lodash";
import {
  asyncScheduler,
  concatMap,
  from,
  interval,
  map,
  Observable,
  observeOn,
  OperatorFunction,
  share,
  Subject,
  switchMap,
  takeWhile,
} from "rxjs";
import { KVHandler } from "./kv";
import { deserialize } from "../storage/serde";
import { _INTERNAL, NotFoundError, RelativeLease } from "../storage/kv";
import { ConnectError, HandlerContext } from "@connectrpc/connect";
import { nanoid } from "nanoid";

export class LeaseHandler extends BaseHandler {
  constructor(ctx: Context, private kv: KVHandler) {
    super(ctx);
  }

  async grant(
    ctx: HandlerContext,
    req: LeaseGrantRequest
  ): Promise<LeaseGrantResponse> {
    const requestId = nanoid(8);

    const tenant = this.getTenant(ctx);

    if (req.ID !== BigInt(0)) {
      throw new ConnectError("Client specified Lease ID");
    }

    const leaseId = req.ID || (await this.ctx.nextLease(tenant));

    await this.kv.kv.putLease(tenant, Number(leaseId), Number(req.TTL));

    return {
      $typeName: "etcdserverpb.LeaseGrantResponse",
      header: await this.header(tenant),
      ID: BigInt(leaseId),
      TTL: req.TTL,
      error: "",
    };
  }

  async revoke(
    ctx: HandlerContext,
    req: LeaseRevokeRequest
  ): Promise<LeaseRevokeResponse> {
    const tenant = this.getTenant(ctx);

    await this.kv.kv.deleteLease(tenant, Number(req.ID));

    return {
      $typeName: "etcdserverpb.LeaseRevokeResponse",
      header: await this.header(tenant),
    };
  }

  public async timeToLive(
    ctx: HandlerContext,
    req: LeaseTimeToLiveRequest
  ): Promise<LeaseTimeToLiveResponse> {
    const tenant = this.getTenant(ctx);
    const lease = await this.kv.kv.getLease(tenant, Number(req.ID));

    if (!lease) {
      throw new NotFoundError();
    }

    let keys: Uint8Array[] = [];
    if (req.keys) {
      // Full search with lease
      const { kvs } = await this.kv.kv.range(
        tenant,
        {
          $typeName: "etcdserverpb.RangeRequest",
          key: new Uint8Array(1),
          rangeEnd: new Uint8Array(1),
          maxModRevision: BigInt(lease.revision),
        },
        { leaseId: lease.leaseId }
      );

      keys = kvs.map((kv) => deserialize(kv.key, true));
    }

    return {
      $typeName: "etcdserverpb.LeaseTimeToLiveResponse",
      header: await this.header(tenant),
      grantedTTL: BigInt(lease.ttl || 0),
      ID: BigInt(lease.leaseId),
      keys,
      TTL: BigInt(lease.ttlRelative),
    };
  }

  public async listLeases(
    ctx: HandlerContext,
    _req: LeaseLeasesRequest
  ): Promise<LeaseLeasesResponse> {
    const tenant = this.getTenant(ctx);
    return {
      $typeName: "etcdserverpb.LeaseLeasesResponse",
      header: await this.header(tenant),
      leases: await this.kv.kv.getLeases(tenant),
    };
  }

  public keepAlive(
    ctx: HandlerContext,
    requests: AsyncIterable<LeaseKeepAliveRequest>
  ): AsyncGenerator<LeaseKeepAliveResponse, void, unknown> {
    return this.bidi(
      ctx,
      {
        history: this.kv.kv.history$,
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
      },
      {
        onResponse: async (tenant, connectionId, res) => {
          if (res.response.TTL <= 0) {
            await this.revoke(ctx, {
              $typeName: "etcdserverpb.LeaseRevokeRequest",
              ID: res.request.ID,
            });
          }
          res.response.header = await this.header(tenant);
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
                this.kv.kv.getLease(tenant, Number(source.ID))
              ).pipe(
                map((lease) => {
                  const keepAlive: LeaseKeepAliveResponse = {
                    $typeName: "etcdserverpb.LeaseKeepAliveResponse",
                    ID: source.ID,
                    TTL: BigInt(lease?.ttlRelative || 0),
                  };
                  return { source, keepAlive };
                })
              );

              const loop = interval(1000, asyncScheduler).pipe(
                switchMap(() => this.kv.kv.getLease(tenant, Number(source.ID))),
                takeWhile((lease) => !!lease),
                map((lease) => {
                  const keepAlive: LeaseKeepAliveResponse = {
                    $typeName: "etcdserverpb.LeaseKeepAliveResponse",
                    ID: source.ID,
                    TTL: BigInt(lease.ttlRelative),
                  };
                  return { source, keepAlive };
                })
              );

              return from([immediate, loop]);
            }),
            concatMap((responses) => from(responses)),
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
}
