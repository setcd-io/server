import Context from "../context";
import { ResponseHeader } from "@setcd-io/connectrpc-etcd";
import {
  filter,
  fromEvent,
  map,
  Observable,
  of,
  OperatorFunction,
  Subject,
  Subscription,
  take,
} from "rxjs";
import { ConnectError, HandlerContext } from "@connectrpc/connect";
import chalk from "chalk";
import { CONNECTION_ID, TENANT } from "../util/const";
import _ from "lodash";
import { TenantHistory } from "../storage/kv";
import { nanoid } from "nanoid";
import { iterate } from "../util/async";
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

  public async *bidi<Req, Res>(
    ctx: HandlerContext,
    sources: {
      requests: AsyncIterable<Req>;
      history: Observable<TenantHistory[]>;
    },
    filters: {
      history: (history: TenantHistory) => boolean;
      response: (res: StreamResponse<Req, Res>) => boolean;
    },
    mappers: {
      requestToResponse: (
        tenant: string,
        connectionId: string,
        requestId: string,
        signal: AbortSignal
      ) => OperatorFunction<Req, StreamResponse<Req, Res>>;
      historyToResponse?: (
        tenant: string,
        connectionId: string,
        requestId: string,
        signal: AbortSignal
      ) => OperatorFunction<TenantHistory[], StreamResponse<Req, Res>>;
      errorToRequest?: (
        tenant: string,
        connectionId: string,
        requestId: string,
        signal: AbortSignal
      ) => OperatorFunction<Error, Req>;
    },
    callbacks?: {
      onResponse?: (
        tenant: string,
        connectionId: string,
        res: StreamResponse<Req, Res>
      ) => Promise<void>;
    }
  ): AsyncGenerator<Res, void, unknown> {
    const abort = new AbortController();
    const tenant = this.getTenant(ctx);
    const connectionId = this.getConnectionId(ctx);
    const responses = new Subject<StreamResponse<Req, Res>>();

    const subscriptions: Map<
      string,
      {
        completion?: Subscription;
        requestToResponse?: Subscription;
        historyToResponse?: Subscription;
        errorToRequest?: Subscription;
      }
    > = new Map();

    subscriptions.set(connectionId, {
      completion: responses.subscribe({
        complete: () => {
          subscriptions.forEach((sub) => {
            sub.completion?.unsubscribe();
            sub.requestToResponse?.unsubscribe();
            sub.historyToResponse?.unsubscribe();
            sub.errorToRequest?.unsubscribe();
          });
        },
      }),
    });

    ctx.signal.addEventListener("abort", () => {
      abort.abort(new Error("Context aborted"));
    });

    (async () => {
      for await (const request of sources.requests) {
        if (abort.signal.aborted) {
          break;
        }

        const requestId = nanoid(8);

        subscriptions.set(requestId, {
          requestToResponse: of(request)
            .pipe(
              mappers.requestToResponse(
                tenant,
                connectionId,
                requestId,
                abort.signal
              )
            )
            .subscribe((res) => {
              responses.next(res);
            }),
          historyToResponse: sources.history
            .pipe(map((his) => his.filter(filters.history)))
            .pipe(
              mappers.historyToResponse!(
                tenant,
                connectionId,
                requestId,
                abort.signal
              )
            )
            .subscribe((res) => {
              responses.next(res);
            }),
          errorToRequest: mappers.errorToRequest
            ? fromEvent(abort.signal, "abort")
                .pipe(
                  take(1),
                  map(() => {
                    if (abort.signal.reason.name === "AbortError") {
                      log("Connection Closed", {
                        level: "warn",
                        tenant,
                        action: "Bidi",
                        context: {
                          con: connectionId,
                          req: requestId,
                        },
                      });
                    } else {
                      log("Connection Aborted", {
                        level: "error",
                        tenant,
                        action: "Bidi",
                        output: abort.signal.reason.meassage,
                        context: {
                          con: connectionId,
                          req: requestId,
                        },
                      });
                    }

                    return abort.signal.reason;
                  }),
                  mappers.errorToRequest(
                    tenant,
                    connectionId,
                    requestId,
                    abort.signal
                  ),
                  mappers.requestToResponse(
                    tenant,
                    connectionId,
                    requestId,
                    abort.signal
                  )
                )
                .subscribe((res) => {
                  responses.next(res);
                })
            : undefined,
        });
      }
    })()
      .catch((e) => {
        if (e.code === "ERR_STREAM_PREMATURE_CLOSE") {
          // Graceful shutdown
          return abort.abort();
        }
        abort.abort(e);
      })
      .finally(() => {
        if (!abort.signal.aborted) {
          abort.abort();
        }
      });

    for await (const response of iterate(
      responses
        .pipe(filter((req) => req.tenant === tenant))
        .pipe(filter((res) => filters.response(res)))
    )) {
      if (response.signal.aborted) {
        responses.complete();
      }

      if (callbacks?.onResponse) {
        await callbacks.onResponse(tenant, connectionId, response);
      }

      yield response.response;
    }

    log("Connection Finished", {
      level: "info",
      tenant,
      action: "Bidi",
      context: {
        con: connectionId,
      },
    });
  }
}
