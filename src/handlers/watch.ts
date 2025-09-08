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
  asyncScheduler,
  combineLatest,
  concatMap,
  delay,
  filter,
  from,
  groupBy,
  interval,
  map,
  Observable,
  of,
  OperatorFunction,
  switchMap,
  takeWhile,
  toArray,
  concat,
  tap,
  Subject,
  ReplaySubject,
  Subscription,
  fromEvent,
  takeUntil,
  startWith,
  AsyncSubject,
  merge,
  mergeMap,
  shareReplay,
  bufferTime,
  observeOn,
  share,
  EMPTY,
  NEVER,
  defer,
} from "rxjs";
import { serialize } from "../storage/serde";
import { ErrGRPCCompacted, ErrGRPCWatchCanceled } from "../util/error";
import Table from "cli-table3";
import util from "util";
import _, { take } from "lodash";
import { TenantHistory } from "../storage/kv";
import { log, stringify } from "../util/log";
import { AsyncObservable } from "../util/async";
import { nanoid } from "nanoid";

const isWatched = (watch: WatchCreateRequest, kv?: KeyValue): boolean => {
  if (watch.startRevision !== 0n && kv?.modRevision! < watch.startRevision) {
    return false;
  }

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

type Abortable<T> = T & { abort: (reason?: string) => void };

export class WatchHandler extends BaseHandler {
  private _aborts: Map<string, AbortController> = new Map();

  constructor(ctx: Context, private kv: KVHandler) {
    super(ctx);
  }

  private getAbort(
    ctx: HandlerContext,
    tenant?: string,
    connectionId?: string,
    watchId?: number
  ): AbortController {
    const key = [tenant, connectionId, watchId].filter((v) => !!v).join(":");

    if (this._aborts.has(key)) {
      return this._aborts.get(key)!;
    }

    const abort = this._aborts.set(key, new AbortController()).get(key)!;

    if (tenant && connectionId && watchId) {
      const parent = this.getAbort(ctx, tenant, connectionId);
      parent.signal.addEventListener("abort", () => {
        abort.abort(`Connection abort: ${parent.signal.reason}`);
        this._aborts.delete(key);
      });
    } else if (tenant && connectionId) {
      const parent = this.getAbort(ctx, tenant);
      parent.signal.addEventListener("abort", () => {
        abort.abort(`Tenant abort: ${parent.signal.reason}`);
        this._aborts.delete(key);
      });
    } else if (tenant) {
      const parent = this.getAbort(ctx);
      parent.signal.addEventListener("abort", () => {
        abort.abort(`Handler abort: ${parent.signal.reason}`);
        this._aborts.delete(key);
      });
    } else {
      ctx.signal.addEventListener("abort", () => {
        abort.abort(`Context abort: ${ctx.signal.reason}`);
        this._aborts.delete(key);
      });
    }

    return abort;
  }

  public async *watch(
    ctx: HandlerContext,
    request: AsyncIterable<WatchRequest>
  ): AsyncGenerator<WatchResponse, void, unknown> {
    const tenant = this.getTenant(ctx);
    const connectionId = this.getConnectionId(ctx);
    const abort = this.getAbort(ctx, tenant, connectionId);
    const requests = from(request).pipe(observeOn(asyncScheduler), share());
    const history = this.kv.kv.history.pipe(
      filter((his) => his.tenant === tenant),
      bufferTime(10, undefined, 10), // Give a little breathing room to batch events
      shareReplay()
    );

    for await (const response of AsyncObservable.from(
      requests.pipe(this.mapRequestToResponse(ctx, history))
    )) {
      response.header = await this.header(tenant);
      yield response;

      if (response.canceled) {
        response.abort(`Watch ${response.watchId} canceled`);
      }
    }

    abort.abort(`Connection ${connectionId} completed`);
  }

  mapRequestToResponse(
    ctx: HandlerContext,
    history: Observable<TenantHistory<KeyValue>[]>
  ): OperatorFunction<WatchRequest, Abortable<WatchResponse>> {
    const tenant = this.getTenant(ctx);
    const connectionId = this.getConnectionId(ctx);

    return (
      source: Observable<WatchRequest>
    ): Observable<Abortable<WatchResponse>> => {
      return new Observable<Abortable<WatchResponse>>((subscriber) => {
        const subscription = source
          .pipe(
            mergeMap((source) => {
              const request = _.cloneDeep(source.requestUnion);

              if (!request.case || request.case === "cancelRequest") {
                subscriber.next({
                  $typeName: "etcdserverpb.WatchResponse",
                  watchId: request.value?.watchId || 0n,
                  compactRevision: 0n,
                  events: [],
                  canceled: true,
                  cancelReason: request.case || "Invalid Request",
                  created: false,
                  fragment: false,
                  abort: (reason) => {
                    this.getAbort(
                      ctx,
                      tenant,
                      connectionId,
                      Number(request.value?.watchId)
                    ).abort(
                      `Watch ${request.value?.watchId} aborted: ${
                        reason || request.case || "unknown reason"
                      }`
                    );
                  },
                });
                return NEVER;
              }

              if (request.case === "progressRequest") {
                // NOT IMPLEMENTED -- where are we supposed to get watchId from?
                return NEVER;
              }

              return combineLatest([
                this.ctx.nextWatch(tenant),
                this.ctx.minRevision(tenant),
                this.ctx.currentRevision(tenant),
              ]).pipe(
                map(([watchId, minRevision, currentRevision]) => {
                  request.value.watchId = BigInt(watchId);

                  if (request.value.startRevision === 0n) {
                    request.value.startRevision = BigInt(currentRevision);
                  }

                  let created = true;
                  let canceled = false;
                  let cancelReason = "";
                  let compactRevision = 0n;

                  if (request.value.startRevision <= BigInt(minRevision)) {
                    throw new ErrGRPCCompacted();
                  }

                  const response: WatchResponse = {
                    $typeName: "etcdserverpb.WatchResponse",
                    watchId: BigInt(watchId),
                    compactRevision,
                    events: [],
                    canceled,
                    cancelReason,
                    created,
                    fragment: false,
                  };

                  return {
                    request,
                    response,
                    abort: this.getAbort(ctx, tenant, connectionId, watchId),
                  };
                })
              );
            })
          )
          .pipe(
            mergeMap(({ request, response, abort }) => {
              // start with a nice hearty base of the initial response
              let response$ = of(_.cloneDeep(response));

              // sprinkle in history streaming
              response$ = concat(
                response$,
                history.pipe(this.mapHistoryToResponse(tenant, request.value))
              );

              // sprinkle in progress notifications
              if (request.value.progressNotify) {
                response$ = merge(
                  response$,
                  interval(15 * 1000).pipe(
                    map(() => {
                      const res = _.cloneDeep(response);
                      res.created = false;
                      res.canceled = false;
                      res.events = [];
                      return res;
                    })
                  )
                );
              }

              // out comes a delicious layer cake of abortable responses
              return response$.pipe(
                takeUntil(fromEvent(abort.signal, "abort")),
                map((response) => {
                  const abortable: Abortable<WatchResponse> = {
                    ...response,
                    abort: (reason) => {
                      abort.abort(
                        `Watch ${response.watchId} aborted: ${
                          reason || "unknown reason"
                        }`
                      );
                    },
                  };
                  return abortable;
                })
              );
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
    watch: WatchCreateRequest
  ): OperatorFunction<TenantHistory<KeyValue>[], WatchResponse> {
    return (
      source: Observable<TenantHistory<KeyValue>[]>
    ): Observable<WatchResponse> => {
      return new Observable<WatchResponse>((subscriber) => {
        const subscription = source
          .pipe(
            map((histories) =>
              histories.filter((his) => his.tenant === tenant)
            ),
            map((histories) =>
              histories.filter((his) => isWatched(watch, his.current))
            ),
            filter((histories) => !!histories.length),
            map((histories) => {
              const response: WatchResponse = {
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

  // mapErrorToResponse(
  //   tenant: string,
  //   connectionId: string,
  //   requestId: string,
  //   signal: AbortSignal
  // ): OperatorFunction<Error, StreamResponse<WatchRequest, WatchResponse>> {
  //   return (
  //     source: Observable<Error>
  //   ): Observable<StreamResponse<WatchRequest, WatchResponse>> => {
  //     return new Observable<StreamResponse<WatchRequest, WatchResponse>>(
  //       (subscriber) => {
  //         const subscription = source
  //           .pipe(
  //             concatMap((source) => {
  //               return from(
  //                 Array.from(this.watches.get(tenant)?.values() || []).map(
  //                   (watch) => {
  //                     return {
  //                       ..._.cloneDeep(watch),
  //                       cancelReason: source.message,
  //                     };
  //                   }
  //                 )
  //               );
  //             }),
  //             filter(
  //               (watch) =>
  //                 watch.tenant === tenant && watch.connectionId === connectionId
  //             ),
  //             map((watch) => {
  //               const request: WatchRequest = {
  //                 $typeName: "etcdserverpb.WatchRequest",
  //                 requestUnion: {
  //                   case: "cancelRequest",
  //                   value: {
  //                     $typeName: "etcdserverpb.WatchCancelRequest",
  //                     watchId: watch.watchId,
  //                   },
  //                 },
  //               };
  //               return request;
  //             }),
  //             this.mapRequestToResponse(tenant, connectionId, signal)
  //           )
  //           .subscribe({
  //             next(response) {
  //               subscriber.next(response);
  //             },
  //             error(err) {
  //               subscriber.error(err);
  //             },
  //             complete() {
  //               subscriber.complete();
  //             },
  //           });

  //         return () => {
  //           subscription.unsubscribe();
  //         };
  //       }
  //     );
  //   };
  // }
}
