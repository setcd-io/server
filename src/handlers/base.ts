import Context from "../context";
import { ResponseHeader } from "@setcd-io/connectrpc-etcd";
import {
  asapScheduler,
  asyncScheduler,
  catchError,
  defer,
  EMPTY,
  filter,
  from,
  fromEvent,
  ignoreElements,
  map,
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

const ignoreErrors = <T>(): OperatorFunction<T, T> => {
  return (source: Observable<T>) => source.pipe(catchError(() => EMPTY));
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
      historyToResponse: (
        tenant: string,
        connectionId: string,
        requestId: string,
        signal: AbortSignal
      ) => OperatorFunction<TenantHistory[], StreamResponse<Req, Res>>;
      errorToResponse: (
        tenant: string,
        connectionId: string,
        requestId: string,
        signal: AbortSignal
      ) => OperatorFunction<Error, StreamResponse<Req, Res>>;
    },
    interceptors?: {
      mutateResponse?: (
        tenant: string,
        connectionId: string,
        res: StreamResponse<Req, Res>
      ) => Promise<void>;
      beforeComplete?: (
        tenant: string,
        connectionId: string,
        reqs: Req[],
        nextFn: (value: StreamResponse<Req, Res>) => void
      ) => Promise<void>;
    }
  ): AsyncGenerator<Res, void, unknown> {
    const abort = new AbortController();
    const tenant = this.getTenant(ctx);
    const connectionId = this.getConnectionId(ctx);

    const requests: Req[] = [];
    const controller = new Subject<StreamResponse<Req, Res>>();
    const subscriptions: Subscription[] = [];

    // DEVNOTE: The only way to shutdown a stream is to call controller.error()
    // - This is so beforeComplete is guaranteed to be called
    const teardown = controller
      .pipe(
        // ignoreElements(),
        catchError(() =>
          defer(() =>
            (
              interceptors?.beforeComplete?.(
                tenant,
                connectionId,
                requests,
                (res) => controller.next(res)
              ) ?? Promise.resolve()
            ).then(() =>
              // subscriptions.push(
              //   asyncScheduler.schedule(
              //     () => ,
              //     1000 // Give some time for next calls to complete
              //   )
              // )
              controller.complete()
            )
          )
        )
      )
      .subscribe({
        complete: () => {
          log("Stream completed", {
            level: "info",
            tenant,
            action: "Bidi",
            context: { con: connectionId, reqs: requests.length },
          });
        },
      });

    // subscriptions.push(teardown);

    ctx.signal.addEventListener("abort", () => {
      controller.error(ctx.signal.reason);
    });

    abort.signal.addEventListener("abort", () => {
      controller.error(abort.signal.reason);
    });

    teardown.add(() => {
      // NO-OP: Purely informative for now
      log("Connection Teardown", {
        level: "info",
        tenant,
        action: "Bidi",
        context: { con: connectionId },
      });
      subscriptions.forEach((sub) => sub.unsubscribe());
    });

    (async () => {
      for await (const request of sources.requests) {
        if (abort.signal.aborted) {
          return;
        }

        const requestId = nanoid(8);

        teardown.add(() => {
          // NO-OP: Purely informative for now
          log("Request Teardown", {
            level: "info",
            tenant,
            action: "Bidi",
            context: { con: connectionId, req: requestId },
          });
        });

        subscriptions.push(
          of(request)
            .pipe(
              tap((req) => requests.push(req)),
              mappers.requestToResponse(
                tenant,
                connectionId,
                requestId,
                abort.signal
              )
            )
            .subscribe({
              next: (res) => controller.next(res),
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
              next: (res) => controller.next(res),
            })
        );

        let error: Error | undefined;

        subscriptions.push(
          controller
            .pipe(
              ignoreElements(),
              catchError((err) => of(err)),
              take(1),
              tap((err) => (error = err)),
              mappers.errorToResponse(
                tenant,
                connectionId,
                requestId,
                abort.signal
              )
            )
            .subscribe({
              next: (res) => {
                controller.next(res);
              },
              error: () => {},
              complete: () => {
                abort.abort(error);
              },
            })
        );
      }
    })()
      .catch((err) => {
        log("Error", {
          level: "info",
          tenant,
          action: "Bidi",
          output: err.message,
          context: {
            con: connectionId,
            reqs: requests.length,
          },
        });
        controller.error(err);
      })
      .finally(() => {
        log("Complete", {
          level: "info",
          tenant,
          action: "Bidi",
          context: { con: connectionId, reqs: requests.length },
        });
        controller.error("Stream completed");
      });

    for await (const response of iterate(
      controller.pipe(
        ignoreErrors(),
        filter((req) => req.tenant === tenant),
        filter((res) => filters.response(res))
      )
    )) {
      if (abort.signal.aborted) {
        return;
      }

      if (interceptors?.mutateResponse) {
        await interceptors.mutateResponse(tenant, connectionId, response);
      }

      yield response.response;
    }
  }
}
