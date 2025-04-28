import Context from "../context";
import { HandlerContext } from "@connectrpc/connect";
import {
  WatchCreateRequest,
  WatchRequest,
  WatchResponse,
  Event_EventType,
  Event,
  RangeRequest,
  KeyValue,
} from "@setcd-io/connectrpc-etcd";
import { BaseHandler, StreamResponse } from "./base";
import { KVHandler } from "./kv";
import {
  asyncScheduler,
  combineLatest,
  concatMap,
  filter,
  from,
  groupBy,
  interval,
  map,
  mergeMap,
  Observable,
  of,
  OperatorFunction,
  switchMap,
  takeWhile,
  toArray,
} from "rxjs";
import { serialize } from "../storage/serde";
import { ErrGRPCCompacted, ErrGRPCWatchCanceled } from "../util/error";
import { TenantHistory } from "../storage/kv";
import Table from "cli-table3";
import util from "util";
import _ from "lodash";
import { nanoid } from "nanoid";

const cancelRequest = (watchId: bigint): WatchRequest => ({
  $typeName: "etcdserverpb.WatchRequest",
  requestUnion: {
    case: "cancelRequest",
    value: {
      $typeName: "etcdserverpb.WatchCancelRequest",
      watchId,
    },
  },
});

type Watch = WatchCreateRequest & {
  tenant: string;
  connectionId: string;
  createdAt: Date;
  cancelReason?: string;
};

type Stats = {
  [tenant: string]: {
    watchIds: string[];
    connectionIds: string[];
    age: number;
  };
};

const isWatched = (watch: Watch, kv?: KeyValue): boolean => {
  const key = serialize(kv?.key, "utf8", false);
  if (!key) {
    return false;
  }

  const startKey = serialize(watch.key, "utf8", true);

  if (watch.rangeEnd.length === 0 && key === startKey) {
    // Exact match
    return true;
  }

  if (
    watch.rangeEnd.length === 1 &&
    watch.rangeEnd[0] === 0 &&
    key.startsWith(startKey)
  ) {
    return true;
  }

  if (
    watch.key.length === watch.rangeEnd.length &&
    watch.key.slice(-1)[0] + 1 === watch.rangeEnd.slice(-1)[0] &&
    key.startsWith(startKey)
  ) {
    return true;
  }

  const endKey = serialize(watch.rangeEnd, "utf8", true);
  if (key.localeCompare(startKey) >= 0 && key.localeCompare(endKey) < 0) {
    return true;
  }

  return false;
};

export class WatchHandler extends BaseHandler {
  private watches: Map<string, Map<number, Watch>> = new Map();

  constructor(ctx: Context, private kv: KVHandler) {
    super(ctx);
  }

  public stats() {
    const _stats = Array.from(this.watches.entries()).reduce(
      (acc, [tenant, watches]) => {
        const watchIds = Array.from(watches.keys()).map((id) => id.toString());
        const connectionIds = Array.from(watches.values())
          .map((watch) => watch.connectionId)
          .filter((id) => !!id);
        acc[tenant] = {
          watchIds,
          connectionIds: Array.from(new Set(connectionIds)),
          age: Math.max(
            ...Array.from(watches.values()).map((watch) => {
              const createdAt = watch.createdAt.getTime();
              const now = new Date().getTime();
              return Math.floor((now - createdAt) / 1000);
            })
          ),
        };
        return acc;
      },
      {} as Stats
    );

    let table = new Table({ style: { head: [], border: [] } });
    table.push(["Tenant", "Connection IDs", "Watch IDs", "Age (s)"]);

    Object.entries(_stats).forEach(([tenant, stats]) => {
      if (!stats.watchIds.length || !stats.connectionIds.length) {
        return;
      }
      table.push([
        tenant,
        util.format(stats.connectionIds),
        util.format(stats.watchIds),
        util.format(stats.age),
      ]);
    });

    if (table.length === 1) {
      table.push([
        {
          colSpan: 4,
          content: "No active watches",
          vAlign: "center",
        },
      ]);
    }

    console.log(table.toString());
  }

  public watch(
    ctx: HandlerContext,
    requests: AsyncIterable<WatchRequest>
  ): AsyncGenerator<WatchResponse, void, unknown> {
    return this.bidi(
      ctx,
      {
        requests,
        history: this.kv.kv.history$,
      },
      {
        history: (his) => {
          return his.tenant === this.getTenant(ctx);
        },
        response: (res) => {
          return (
            res.tenant === this.getTenant(ctx) &&
            res.connectionId === this.getConnectionId(ctx)
          );
        },
      },
      {
        requestToResponse: (tenant, connectionId, signal) => {
          return this.mapRequestToResponse(tenant, connectionId, signal);
        },
        historyToResponse: (tenant, connectionId, signal) => {
          return this.mapHistoryToResponse(tenant, connectionId, signal);
        },
        errorToRequest: (tenant, connectionId, signal) => {
          return this.mapErrorToRequest(tenant, connectionId, signal);
        },
      },
      {
        onResponse: async (tenant, connectionId, res) => {
          res.response.header = await this.header(tenant);

          if (
            res.request.requestUnion.case === "createRequest" &&
            res.request.requestUnion.value.startRevision !== 0n &&
            res.request.requestUnion.value.startRevision <=
              (await this.ctx.minRevision(res.tenant))
          ) {
            throw new ErrGRPCCompacted();
          }

          this.watches.set(tenant, this.watches.get(tenant) || new Map());

          let watch = this.watches
            .get(tenant)
            ?.get(Number(res.response.watchId));

          if (!watch && res.request.requestUnion.case === "createRequest") {
            watch = {
              ..._.cloneDeep(res.request.requestUnion.value),
              tenant,
              connectionId,
              watchId: res.response.watchId,
              createdAt: new Date(),
              progressNotify: false,
              prevKv: false,
            };
          }

          if (!watch) {
            throw new ErrGRPCWatchCanceled();
          }

          if (res.request.requestUnion.case === "createRequest") {
            this.watches.get(tenant)?.set(Number(res.response.watchId), watch);
          }

          if (res.request.requestUnion.case === "cancelRequest") {
            this.watches.get(tenant)?.delete(Number(res.response.watchId));
          }
        },
      }
    );
  }

  private async history(
    tenant: string,
    req: Partial<RangeRequest>
  ): Promise<Event[]> {
    const events = await this.kv.kv
      .range(tenant, req)
      .then(({ kvs }) =>
        Promise.all(kvs.map((kv) => this.kv.kv.latest(kv.tenant, kv.key)))
      )
      .then((histories) => histories.filter((h) => !!h))
      .then((histories) => {
        return histories.map(({ current, previous, action }) => {
          const event: Event = {
            $typeName: "mvccpb.Event",
            type:
              action === "PUT" ? Event_EventType.PUT : Event_EventType.DELETE,
            kv: current,
            prevKv: previous,
          };
          return event;
        });
      });
    return events;
  }

  mapRequestToResponse(
    tenant: string,
    connectionId: string,
    signal: AbortSignal
  ): OperatorFunction<
    WatchRequest,
    StreamResponse<WatchRequest, WatchResponse>
  > {
    return (
      source: Observable<WatchRequest>
    ): Observable<StreamResponse<WatchRequest, WatchResponse>> => {
      return new Observable<StreamResponse<WatchRequest, WatchResponse>>(
        (subscriber) => {
          const subscription = source
            .pipe(
              switchMap((source) => {
                this.watches.set(tenant, this.watches.get(tenant) || new Map());
                switch (source.requestUnion.case) {
                  case "createRequest":
                    return combineLatest([
                      this.ctx.nextWatch(tenant),
                      this.ctx.currentRevision(tenant),
                    ]).pipe(
                      map(([watchId, currentRevision]) => {
                        const response: WatchResponse = {
                          $typeName: "etcdserverpb.WatchResponse",
                          watchId: BigInt(watchId),
                          compactRevision: 0n,
                          events: [],
                          canceled: false,
                          cancelReason: "",
                          created: true,
                          fragment: false,
                        };

                        if (
                          source.requestUnion.case === "createRequest" &&
                          source.requestUnion.value.startRevision === 0n
                        ) {
                          source.requestUnion.value.startRevision =
                            BigInt(currentRevision);
                        }

                        return {
                          tenant,
                          connectionId,
                          request: source,
                          response,
                          signal,
                        } as StreamResponse<WatchRequest, WatchResponse>;
                      })
                    );
                  case "cancelRequest":
                    return of(
                      this.watches
                        .get(tenant)
                        ?.get(Number(source.requestUnion.value.watchId))
                    )
                      .pipe(filter((watch) => !!watch))
                      .pipe(
                        map((watch) => {
                          const response: WatchResponse = {
                            $typeName: "etcdserverpb.WatchResponse",
                            watchId: watch.watchId,
                            compactRevision: 0n,
                            events: [],
                            canceled: true,
                            cancelReason:
                              watch.cancelReason ||
                              signal.reason?.message ||
                              `User Requested Cancel`,
                            created: false,
                            fragment: false,
                          };

                          return {
                            tenant,
                            connectionId,
                            request: source,
                            response,
                            signal,
                          } as StreamResponse<WatchRequest, WatchResponse>;
                        })
                      );
                  default:
                    throw new Error(
                      `Unimplemented watch case: ${source.requestUnion.case}`
                    );
                }
              })
            )
            .pipe(
              switchMap((res) => {
                // Conditional fanout for progressNotify
                res = _.cloneDeep(res);

                if (res.request.requestUnion.case !== "createRequest") {
                  return of([res]);
                }

                if (!res.request.requestUnion.value.progressNotify) {
                  // No progress notify, don't set an interval
                  return of([res]);
                }

                const progressNotify = interval(1000, asyncScheduler).pipe(
                  map(() =>
                    this.watches
                      .get(res.tenant)
                      ?.get(Number(res.response.watchId))
                  ),
                  takeWhile((watch) => !!watch && watch.progressNotify, true),
                  map(() => {
                    const response = _.cloneDeep(res);
                    response.response.created = false;
                    response.response.canceled = false;
                    response.response.events = [];
                    return response;
                  })
                );

                return from([of(res), progressNotify]);
              })
            )
            .pipe(mergeMap((responses) => from(responses)))
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
        }
      );
    };
  }

  mapHistoryToResponse(
    tenant: string,
    connectionId: string,
    signal: AbortSignal
  ): OperatorFunction<
    TenantHistory[],
    StreamResponse<WatchRequest, WatchResponse>
  > {
    return (
      source: Observable<TenantHistory[]>
    ): Observable<StreamResponse<WatchRequest, WatchResponse>> => {
      return new Observable<StreamResponse<WatchRequest, WatchResponse>>(
        (subscriber) => {
          const subscription = source
            .pipe(
              mergeMap((histories) =>
                from(histories).pipe(
                  filter((his) => his.tenant === tenant),
                  mergeMap((history) => {
                    const watchers = [
                      ...(this.watches.get(tenant)?.values() || []),
                    ]
                      .filter((watch) => isWatched(watch, history.current))
                      .map((watch) => ({ watch, history }));

                    return from(watchers);
                  }),
                  groupBy((x) => x.watch.watchId),
                  mergeMap((group$) =>
                    group$.pipe(
                      map((x) => x.history),
                      toArray(),
                      map((histories) => {
                        const watch = this.watches
                          .get(tenant)
                          ?.get(Number(group$.key));

                        return {
                          watchId: group$.key,
                          watch,
                          histories: histories.filter(
                            (history) =>
                              watch?.startRevision === 0n ||
                              history.current.modRevision >=
                                watch?.startRevision!
                          ),
                        };
                      })
                    )
                  ),
                  filter(({ watch, histories }) => {
                    return !!histories.length || !!watch?.progressNotify;
                  }),
                  map(({ watchId, histories }) => {
                    const response: StreamResponse<
                      WatchRequest,
                      WatchResponse
                    > = {
                      tenant,
                      connectionId,
                      signal,
                      request: {
                        $typeName: "etcdserverpb.WatchRequest",
                        requestUnion: {
                          case: "progressRequest",
                          value: {
                            $typeName: "etcdserverpb.WatchProgressRequest",
                          },
                        },
                      },
                      response: {
                        $typeName: "etcdserverpb.WatchResponse",
                        watchId: BigInt(watchId),
                        compactRevision: 0n,
                        events: histories.map((history) => ({
                          $typeName: "mvccpb.Event",
                          type:
                            history.action === "PUT"
                              ? Event_EventType.PUT
                              : Event_EventType.DELETE,
                          kv: history.current,
                          prevKv: history.previous,
                        })),
                        canceled: false,
                        cancelReason: "",
                        created: false,
                        fragment: false,
                      },
                    };

                    return response;
                  })
                )
              )
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
        }
      );
    };
  }

  mapErrorToRequest(
    tenant: string,
    connectionId: string,
    signal: AbortSignal
  ): OperatorFunction<Error, WatchRequest> {
    return (source: Observable<Error>): Observable<WatchRequest> => {
      return new Observable<WatchRequest>((subscriber) => {
        const subscription = source
          .pipe(
            concatMap((source) => {
              return from(
                Array.from(this.watches.get(tenant)?.values() || []).map(
                  (watch) => {
                    return {
                      ..._.cloneDeep(watch),
                      cancelReason: source.message,
                    };
                  }
                )
              );
            }),
            filter(
              (watch) =>
                watch.tenant === tenant && watch.connectionId === connectionId
            ),
            map((watch) => {
              const request: WatchRequest = {
                $typeName: "etcdserverpb.WatchRequest",
                requestUnion: {
                  case: "cancelRequest",
                  value: {
                    $typeName: "etcdserverpb.WatchCancelRequest",
                    watchId: watch.watchId,
                  },
                },
              };
              return request;
            })
          )
          .subscribe({
            next(request) {
              subscriber.next(request);
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
