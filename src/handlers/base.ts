import Context from "../context";
import { ResponseHeader } from "@setcd-io/connectrpc-etcd";
import {
  asapScheduler,
  asyncScheduler,
  catchError,
  concatMap,
  defer,
  EMPTY,
  filter,
  from,
  fromEvent,
  ignoreElements,
  map,
  mergeMap,
  MonoTypeOperatorFunction,
  NEVER,
  Observable,
  observeOn,
  of,
  OperatorFunction,
  repeat,
  share,
  Subject,
  Subscription,
  switchMap,
  take,
  takeUntil,
  tap,
} from "rxjs";
import { ConnectError, HandlerContext } from "@connectrpc/connect";
import chalk from "chalk";
import { CONNECTION_ID, TENANT } from "../util/const";
import _ from "lodash";
import { TenantHistory } from "../storage/kv";
import { nanoid } from "nanoid";
import { AsyncObservable, iterate } from "../util/async";
import { log } from "../util/log";

export type StreamRequest<Req> = {
  tenant: string;
  connectionId: string;
  requestId: string;
  request: Req;
  signal: AbortSignal;
};

export type StreamResponse<Req, Res> = {
  tenant: string;
  connectionId: string;
  requestId: string;
  request: Req;
  response: Res;
  signal: AbortSignal;
};

export abstract class BaseHandler {
  constructor(protected ctx: Context) {}

  async header(tenant: string): Promise<ResponseHeader> {
    return {
      $typeName: "etcdserverpb.ResponseHeader",
      revision: BigInt(await this.ctx.currentRevision(tenant)),
      raftTerm: 0n,
      memberId: 0n,
      clusterId: 0n,
    };
  }

  protected getConnectionId(ctx: HandlerContext): string {
    const connectionId = ctx.values.get(CONNECTION_ID);
    if (!connectionId) {
      throw new ConnectError("Connection ID not found");
    }
    return connectionId;
  }

  protected getTenant(ctx: HandlerContext): string {
    const tenant = ctx.values.get(TENANT);
    if (!tenant) {
      throw new ConnectError("Tenant not found");
    }
    return tenant;
  }

  public async *bidi<Req, Res, T>(
    name: string,
    ctx: HandlerContext,
    sources: {
      requests: AsyncIterable<Req>;
      history: Observable<TenantHistory<T>[]>;
    },
    filters: {
      history: (history: TenantHistory<T>) => boolean;
      response: (res: StreamResponse<Req, Res>) => boolean;
    },
    mappers: {
      requestToResponse: (
        tenant: string,
        connectionId: string,
        requestId: string,
        signal: AbortSignal
      ) => OperatorFunction<Req, StreamResponse<Req, Res>>;
      historyToResponse: (
        tenant: string,
        connectionId: string,
        requestId: string,
        signal: AbortSignal
      ) => OperatorFunction<TenantHistory<T>[], StreamResponse<Req, Res>>;
      errorToResponse: (
        tenant: string,
        connectionId: string,
        requestId: string,
        signal: AbortSignal
      ) => OperatorFunction<Error, StreamResponse<Req, Res>>;
    },
    mutators: {
      response: (
        tenant: string,
        connectionId: string,
        res: StreamResponse<Req, Res>
      ) => Promise<StreamResponse<Req, Res>>;
    }
  ): AsyncGenerator<Res, void, unknown> {
    const abort = new AbortController();
    const tenant = this.getTenant(ctx);
    const connectionId = this.getConnectionId(ctx);
    const requestIds: string[] = [];

    log("Stream Start", {
      level: "info",
      tenant,
      action: name,
      context: { con: connectionId },
    });

    const responses = new Subject<StreamResponse<Req, Res>>();
    const subscriptions: Subscription[] = [];

    subscriptions.push(
      responses
        .pipe(
          catchError((err) => {
            log("Stream error", {
              level: "warn",
              tenant,
              action: "Bidi",
              output: err.message,
              context: { con: connectionId },
            });
            return EMPTY;
          })
        )
        .subscribe({
          complete: () => {
            log("Stream completed", {
              level: "success",
              tenant,
              action: "Bidi",
              context: { con: connectionId },
            });
          },
        })
    );

    ctx.signal.addEventListener("abort", () => {
      log("Context Abort", {
        level: "warn",
        tenant,
        action: name,
        context: { con: connectionId, reqs: requestIds },
      });
      abort.abort(ctx.signal.reason);
    });

    abort.signal.addEventListener("abort", () => {
      log("Stream Abort", {
        level: "warn",
        tenant,
        action: name,
        context: { con: connectionId, reqs: requestIds },
      });
      responses.error(abort.signal.reason);
    });

    (async () => {
      for await (const request of sources.requests) {
        const requestId = nanoid(8);
        requestIds.push(requestId);
        log("Request Start", {
          level: "info",
          tenant,
          action: name,
          output: request,
          context: { con: connectionId, req: requestId },
        });

        if (abort.signal.aborted) {
          break;
        }

        subscriptions.push(
          of(request)
            .pipe(
              mappers.requestToResponse(
                tenant,
                connectionId,
                requestId,
                abort.signal
              )
            )
            .subscribe({
              next: (res) => responses.next(res),
              error: (err) => {
                log("Unable to map request", {
                  level: "warn",
                  tenant,
                  action: "Bidi",
                  output: err.message,
                  context: { con: connectionId, req: requestId },
                });
              },
              complete: () => {
                log("Request completed", {
                  level: "success",
                  tenant,
                  action: "Bidi",
                  context: { con: connectionId, req: requestId },
                });
              },
            })
        );

        subscriptions.push(
          sources.history
            .pipe(map((his) => his.filter(filters.history)))
            .pipe(
              mappers.historyToResponse(
                tenant,
                connectionId,
                requestId,
                abort.signal
              )
            )
            .subscribe({
              next: (response) => responses.next(response),
              error: (err) => {
                log("Unable to map history", {
                  level: "warn",
                  tenant,
                  action: "Bidi",
                  output: err.message,
                  context: { con: connectionId, req: requestId },
                });
              },
              complete: () => {
                log("History completed", {
                  level: "success",
                  tenant,
                  action: "Bidi",
                  context: { con: connectionId, req: requestId },
                });
              },
            })
        );

        subscriptions.push(
          responses
            .pipe(
              ignoreElements(),
              catchError((err) => of(err)),
              mappers.errorToResponse(
                tenant,
                connectionId,
                requestId,
                abort.signal
              )
            )
            .subscribe({
              next: (response) => {
                responses.next(response);
              },
              error: () => {
                log("Unable to map error", {
                  level: "warn",
                  tenant,
                  action: "Bidi",
                  context: { con: connectionId, req: requestId },
                });
              },
              complete: () => {
                log("Error mapping completed", {
                  level: "success",
                  tenant,
                  action: "Bidi",
                  context: { con: connectionId, req: requestId },
                });
              },
            })
        );
      }
    })()
      .catch((err) => {
        log("Requests Error", {
          level: "warn",
          tenant,
          action: "Bidi",
          output: err.message,
          context: {
            con: connectionId,
          },
        });
        responses.error(err);
      })
      .finally(() => {
        log("Requests Complete", {
          level: "info",
          tenant,
          action: name,
          context: { con: connectionId, reqs: requestIds },
        });
        responses.error(new Error("Requests Complete"));
      });

    try {
      for await (const response of AsyncObservable.from(
        responses.pipe(
          filter((req) => req.tenant === tenant),
          filter((res) => filters.response(res)),
          concatMap((res) => mutators.response(tenant, connectionId, res))
        )
      )) {
        yield response.response;
      }
    } catch (err) {
      log("Responses Error", {
        level: "warn",
        tenant,
        action: "Bidi",
        output: err.message,
        context: { con: connectionId },
      });
    }
    // finally {
    //   log("Responses Complete", {
    //     level: "info",
    //     tenant,
    //     action: name,
    //     context: { con: connectionId, reqs: requestIds },
    //   });
    // }

    log("Stream Complete", {
      level: "info",
      tenant,
      action: name,
      context: { con: connectionId, reqs: requestIds },
    });

    subscriptions.forEach((sub) => sub.unsubscribe());
  }
}
