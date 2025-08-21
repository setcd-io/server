import Context from "../context";
import { HandlerContext } from "@connectrpc/connect";
import {
  WatchCreateRequest,
  WatchRequest,
  WatchResponse,
  Event_EventType,
  KeyValue,
} from "@setcd-io/connectrpc-etcd";
import { BaseHandler, StreamResponse } from "./base";
import { KVHandler } from "./kv";
import {
  combineLatest,
  concatMap,
  filter,
  from,
  interval,
  map,
  Observable,
  of,
  OperatorFunction,
  switchMap,
  concat,
  tap,
  concatAll,
  lastValueFrom,
  takeUntil,
  fromEvent,
} from "rxjs";
import { serialize } from "../storage/serde";
import { ErrGRPCCompacted, ErrGRPCWatchCanceled } from "../util/error";
import _ from "lodash";
import { TenantHistory } from "../storage/kv";
import { log, stringify } from "../util/log";
import { CloudProvider, CloudReplaySubject } from "cloudrx";

type Watch = {
  tenant: string;
  watchId: number;
  connectionId: string;
  requestId: string;
  cancelReason?: string;
  spec?: WatchCreateRequest;
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

  if (!watch.spec) {
    return false;
  }

  if (Number(kv?.modRevision) < Number(watch.spec.startRevision)) {
    return false;
  }

  const startKey = serialize(watch.spec.key, "utf8", true);

  if (watch.spec.rangeEnd.length === 0 && key === startKey) {
    // Exact match
    return true;
  }

  if (
    watch.spec.rangeEnd.length === 1 &&
    watch.spec.rangeEnd[0] === 0 &&
    key.startsWith(startKey)
  ) {
    return true;
  }

  if (
    watch.spec.key.length === watch.spec.rangeEnd.length &&
    watch.spec.key.slice(-1)[0] + 1 === watch.spec.rangeEnd.slice(-1)[0] &&
    key.startsWith(startKey)
  ) {
    return true;
  }

  const endKey = serialize(watch.spec.rangeEnd, "utf8", true);
  if (key.localeCompare(startKey) >= 0 && key.localeCompare(endKey) < 0) {
    return true;
  }

  return false;
};

export class WatchHandler extends BaseHandler {
  private _watches: CloudReplaySubject<Watch>;
  private _aborts: Record<number, AbortController> = {};
  // private watches: Map<string, Map<number, Watch>> = new Map();

  constructor(ctx: Context, private kv: KVHandler) {
    super(ctx);
    this._watches = new CloudReplaySubject<Watch>(ctx.watchStorage, {
      hashFn: (watch: Watch) =>
        `${watch.tenant}:${watch.watchId}:${watch.connectionId}:${watch.requestId}`,
    });

    const sub = this._watches.subscribe((watch) => {
      console.log("!!! watch sub", watch);
    });

    this._watches.on("expired", (watch) => {
      console.log("!!! Watch expired:", watch);
      this._aborts[watch.watchId]?.abort(new ErrGRPCWatchCanceled());
    });

    ctx.on("abort", () => {
      sub.unsubscribe();
      // watches.unsubscribe();
      // Object.values(this._aborts).forEach((abort) => {
      //   abort.abort(new ErrGRPCWatchCanceled());
      // });
      // this._aborts = {};
      // this._watches.complete();
    });
  }

  // public stats() {
  //   const _stats = Array.from(this.watches.entries()).reduce(
  //     (acc, [tenant, watches]) => {
  //       const watchIds = Array.from(watches.keys()).map((id) => id.toString());
  //       const connectionIds = Array.from(watches.values())
  //         .map((watch) => watch.connectionId)
  //         .filter((id) => !!id);
  //       acc[tenant] = {
  //         watchIds,
  //         connectionIds: Array.from(new Set(connectionIds)),
  //         age: Math.max(
  //           ...Array.from(watches.values()).map((watch) => {
  //             const createdAt = watch.createdAt.getTime();
  //             const now = new Date().getTime();
  //             return Math.floor((now - createdAt) / 1000);
  //           })
  //         ),
  //       };
  //       return acc;
  //     },
  //     {} as Stats
  //   );

  //   let table = new Table({ style: { head: [], border: [] } });
  //   table.push(["Tenant", "Connection IDs", "Watch IDs", "Age (s)"]);

  //   Object.entries(_stats).forEach(([tenant, stats]) => {
  //     if (!stats.watchIds.length || !stats.connectionIds.length) {
  //       return;
  //     }
  //     table.push([
  //       tenant,
  //       util.format(stats.connectionIds),
  //       util.format(stats.watchIds),
  //       util.format(stats.age),
  //     ]);
  //   });

  //   if (table.length === 1) {
  //     table.push([
  //       {
  //         colSpan: 4,
  //         content: "No active watches",
  //         vAlign: "center",
  //       },
  //     ]);
  //   }

  //   log("Watches", {
  //     level: "success",
  //     tenant: "all",
  //     action: "Stats",
  //     output: `\n${table.toString()}`,
  //   });
  //   // console.log(table.toString());
  // }

  public watch(
    ctx: HandlerContext,
    requests: AsyncIterable<WatchRequest>
  ): AsyncGenerator<WatchResponse, void, unknown> {
    const abort = new AbortController();
    const abortEvent = fromEvent(abort.signal, "abort");
    return this.bidi(
      "Watch",
      ctx,
      {
        requests: from(requests).pipe(takeUntil(abortEvent)),
        history: this.kv.kv
          .history$(this.getTenant(ctx))
          .pipe(takeUntil(abortEvent)),
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
        onResponse: async (tenant, connectionId, res) => {
          res = _.cloneDeep(res);
          res.response.header = await this.header(tenant);

          if (
            res.request.requestUnion.case === "createRequest" &&
            res.request.requestUnion.value.startRevision !== 0n &&
            res.request.requestUnion.value.startRevision <=
              (await this.ctx.minRevision(res.tenant))
          ) {
            throw new ErrGRPCCompacted();
          }

          const watch: Watch = {
            tenant,
            watchId: Number(res.response.watchId),
            connectionId,
            requestId: res.requestId,
          };

          if (res.request.requestUnion.case === "createRequest") {
            this._aborts[watch.watchId] = abort;
            this._watches.next(
              {
                ...watch,
                spec: res.request.requestUnion.value,
              },
              CloudProvider.TIME() + 60
            );
          }

          if (res.request.requestUnion.case === "cancelRequest") {
            this._watches.next(
              {
                ...watch,
                cancelReason: res.response.cancelReason,
              },
              CloudProvider.TIME()
            );
          }

          return res;
        },
        onEnd: async (tenant, connectionId) => {
          await lastValueFrom(
            this._watches.snapshot({ tenant, connectionId }).pipe(
              concatAll(),
              tap((w) => this._watches.next(w, CloudProvider.TIME()))
            )
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
              concatMap((source) => {
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

                        if (source.requestUnion.case === "createRequest") {
                          source.requestUnion.value.watchId = BigInt(watchId);

                          if (source.requestUnion.value.startRevision === 0n) {
                            source.requestUnion.value.startRevision =
                              BigInt(currentRevision);
                          }
                        }

                        return {
                          tenant,
                          connectionId,
                          requestId,
                          request: source,
                          response,
                          signal,
                        } as StreamResponse<WatchRequest, WatchResponse>;
                      })
                    );
                  case "cancelRequest":
                    return this._watches
                      .snapshot({
                        tenant,
                        watchId: Number(source.requestUnion.value.watchId),
                      })
                      .pipe(
                        concatAll(),
                        map((watch) => {
                          const response: WatchResponse = {
                            $typeName: "etcdserverpb.WatchResponse",
                            watchId: BigInt(watch.watchId),
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

                          if (source.requestUnion.case === "cancelRequest") {
                            source.requestUnion.value.watchId = BigInt(
                              watch.watchId
                            );
                          }

                          return {
                            tenant,
                            connectionId,
                            requestId,
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
                  return of(res);
                }

                if (!res.request.requestUnion.value.progressNotify) {
                  // No progress notify, don't set an interval
                  return of(res);
                }

                const progressNotify = interval(1000).pipe(
                  switchMap(() =>
                    this._watches.snapshot({
                      tenant,
                      watchId: Number(res.response.watchId),
                      connectionId,
                      requestId,
                    })
                  ),
                  concatAll(),
                  map(() => {
                    const cloned = _.cloneDeep(res);
                    cloned.request = {
                      $typeName: "etcdserverpb.WatchRequest",
                      requestUnion: {
                        case: "progressRequest",
                        value: {
                          $typeName: "etcdserverpb.WatchProgressRequest",
                        },
                      },
                    };
                    cloned.response.created = false;
                    cloned.response.canceled = false;
                    cloned.response.events = [];
                    return cloned;
                  })
                );

                // Emit immediate response first, then start interval asynchronously
                return concat(of(res), progressNotify);
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
        }
      );
    };
  }

  mapHistoryToResponse(
    tenant: string,
    connectionId: string,
    requestId: string,
    signal: AbortSignal
  ): OperatorFunction<
    TenantHistory<KeyValue>[],
    StreamResponse<WatchRequest, WatchResponse>
  > {
    return (
      source: Observable<TenantHistory<KeyValue>[]>
    ): Observable<StreamResponse<WatchRequest, WatchResponse>> => {
      return new Observable<StreamResponse<WatchRequest, WatchResponse>>(
        (subscriber) => {
          const subscription = source
            .pipe(
              concatMap((histories) =>
                this._watches
                  .snapshot({ tenant, connectionId, requestId })
                  .pipe(
                    concatAll(),
                    map((watch) => ({
                      watch,
                      histories: histories.filter(
                        (h) =>
                          h.tenant === tenant && isWatched(watch, h.current)
                      ),
                    }))
                  )
              ),
              filter(({ watch, histories }) => {
                if (histories.length === 0 && !watch.spec?.progressNotify) {
                  return false;
                }
                return true;
              }),
              map(({ watch, histories }) => {
                const response: StreamResponse<WatchRequest, WatchResponse> = {
                  tenant,
                  connectionId,
                  requestId,
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
                    watchId: BigInt(watch.watchId),
                    compactRevision: 0n,
                    events: histories.map((history) => {
                      return {
                        $typeName: "mvccpb.Event",
                        type:
                          history.action === "PUT"
                            ? Event_EventType.PUT
                            : Event_EventType.DELETE,
                        kv: history.current,
                        prevKv: history.previous,
                      };
                    }),
                    canceled: false,
                    cancelReason: "",
                    created: false,
                    fragment: false,
                  },
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
        }
      );
    };
  }

  mapErrorToResponse(
    tenant: string,
    connectionId: string,
    requestId: string,
    signal: AbortSignal
  ): OperatorFunction<Error, StreamResponse<WatchRequest, WatchResponse>> {
    return (
      source: Observable<Error>
    ): Observable<StreamResponse<WatchRequest, WatchResponse>> => {
      return new Observable<StreamResponse<WatchRequest, WatchResponse>>(
        (subscriber) => {
          const subscription = source
            .pipe(
              concatMap((error) =>
                this._watches
                  .snapshot({ tenant, connectionId, requestId })
                  .pipe(
                    concatAll(),
                    map((watch) => ({ watch, error }))
                  )
              ),
              map(({ watch, error }) => {
                const response: StreamResponse<WatchRequest, WatchResponse> = {
                  tenant,
                  connectionId,
                  requestId,
                  signal,
                  request: {
                    $typeName: "etcdserverpb.WatchRequest",
                    requestUnion: {
                      case: "cancelRequest",
                      value: {
                        $typeName: "etcdserverpb.WatchCancelRequest",
                        watchId: BigInt(watch.watchId),
                      },
                    },
                  },
                  response: {
                    $typeName: "etcdserverpb.WatchResponse",
                    watchId: BigInt(watch.watchId),
                    compactRevision: 0n, // TODO: Probably need to observe minRevision
                    events: [],
                    canceled: true,
                    cancelReason: error.message,
                    created: false,
                    fragment: false,
                  },
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
        }
      );
    };
  }
}
