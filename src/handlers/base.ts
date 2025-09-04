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
  request: Req;
  response: Res;
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
    handlers: {
      onResponse: (
        tenant: string,
        connectionId: string,
        res: StreamResponse<Req, Res>
      ) => Promise<StreamResponse<Req, Res>>;
      onEnd: (tenant: string, connectionId: string) => Promise<void>;
    }
  ): AsyncGenerator<Res, void, unknown> {
    const abort = new AbortController();
    const tenant = this.getTenant(ctx);
    const connectionId = this.getConnectionId(ctx);
    const requestIds: string[] = [];

    log("Start", {
      level: "info",
      tenant,
      action: name,
      context: { con: connectionId },
    });

    const requests = from(sources.requests).pipe(observeOn(asyncScheduler));
    const responses = new Subject<StreamResponse<Req, Res>>();
    const subscriptions: Subscription[] = [];

    subscriptions.push(
      requests
        .pipe(
          concatMap((request) => {
            const requestId = nanoid(8);
            requestIds.push(requestId);

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
                .subscribe(responses)
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
                .subscribe(responses)
            );

            return of(request).pipe(
              mappers.requestToResponse(
                tenant,
                connectionId,
                requestId,
                abort.signal
              )
            );
          })
        )
        .subscribe(responses)
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
      if (abort.signal.reason.name === "AbortError") {
        responses.complete();
      } else {
        responses.error(abort.signal.reason);
      }
    });

    try {
      for await (const response of AsyncObservable.from(
        responses.pipe(
          filter((req) => req.tenant === tenant),
          filter((res) => filters.response(res)),
          concatMap((res) => handlers.onResponse(tenant, connectionId, res))
        )
      )) {
        yield response.response;
      }
    } catch (err) {
      abort.abort(err);
    } finally {
      await handlers.onEnd(tenant, connectionId);
      subscriptions.forEach((sub) => sub.unsubscribe());
      log("End", {
        level: "info",
        tenant,
        action: name,
        output: abort.signal.reason,
        context: { con: connectionId, reqs: requestIds },
      });
    }
  }
}
