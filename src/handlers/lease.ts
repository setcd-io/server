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
  LeaseTimeToLiveRequest,
  LeaseTimeToLiveResponse,
} from "@setcd-io/connectrpc-etcd";
import _ from "lodash";
import {
  concat,
  EMPTY,
  filter,
  from,
  interval,
  map,
  mergeAll,
  Observable,
  OperatorFunction,
  switchMap,
  takeWhile,
  tap,
} from "rxjs";
import { KVHandler } from "./kv";
import { deserialize, serialize } from "../storage/serde";
import { _INTERNAL, Lease, NotFoundError, TenantHistory } from "../storage/kv";
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
        history: this.kv.kv.history$(this.getTenant(ctx)),
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
        errorToResponse: () => {
          return () => EMPTY;
        },
      },
      {
        mutateResponse: async (tenant, connectionId, res) => {
          if (res.response.TTL <= 0) {
            await this.revoke(ctx, {
              $typeName: "etcdserverpb.LeaseRevokeRequest",
              ID: res.request.ID,
            });
          }
          res.response.header = await this.header(tenant);
        },
        beforeComplete: async (tenant, connectionId, reqs, nextFn) => {
          await Promise.all(
            reqs.map(async (req) => {
              const lease = await this.kv.kv.getLease(tenant, Number(req.ID));

              console.log("!!! handling lease before complete !!!", {
                tenant,
                connectionId,
                req,
                lease,
              });

              const response: StreamResponse<
                LeaseKeepAliveRequest,
                LeaseKeepAliveResponse
              > = {
                tenant,
                connectionId,
                requestId: "unknown",
                request: req,
                response: {
                  $typeName: "etcdserverpb.LeaseKeepAliveResponse",
                  ID: req.ID,
                  TTL: BigInt(lease?.ttlRelative || 0),
                },
                signal: ctx.signal,
              };
              nextFn(response);
            })
          );
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

              const loop = interval(1000).pipe(
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
    TenantHistory[],
    StreamResponse<LeaseKeepAliveRequest, LeaseKeepAliveResponse>
  > {
    return (
      source: Observable<TenantHistory[]>
    ): Observable<
      StreamResponse<LeaseKeepAliveRequest, LeaseKeepAliveResponse>
    > => {
      return new Observable<
        StreamResponse<LeaseKeepAliveRequest, LeaseKeepAliveResponse>
      >((subscriber) => {
        const subscription = source
          .pipe(
            mergeAll(),
            // only handling DELETE actions for now
            //  - TODO: decide if we want to handle PUT actions
            filter((h) => h.tenant === tenant && h.action === "DELETE"),
            map((history) => serialize(history.current.key, "utf8", true)),
            filter((key) => key.startsWith("__lease:")),
            map((key) => parseInt(key.split(":")[1])),
            map((leaseId) => {
              const response: StreamResponse<
                LeaseKeepAliveRequest,
                LeaseKeepAliveResponse
              > = {
                tenant: tenant,
                connectionId,
                requestId,
                request: {
                  $typeName: "etcdserverpb.LeaseKeepAliveRequest",
                  ID: BigInt(leaseId),
                },
                response: {
                  $typeName: "etcdserverpb.LeaseKeepAliveResponse",
                  ID: BigInt(leaseId),
                  TTL: BigInt(0),
                },
                signal,
              };
              return response;
            })
          )
          .subscribe({
            next(response) {
              console.log("!!! history to response !!!", response);
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
