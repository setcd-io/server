import Context from "../context";
import { ResponseHeader } from "@setcd-io/connectrpc-etcd";
import {
  asapScheduler,
  asyncScheduler,
  AsyncSubject,
  delay,
  filter,
  firstValueFrom,
  fromEvent,
  lastValueFrom,
  map,
  Observable,
  observeOn,
  of,
  OperatorFunction,
  share,
  Subject,
  Subscription,
  take,
  tap,
} from "rxjs";
import { ConnectError, HandlerContext } from "@connectrpc/connect";
import chalk from "chalk";
import { iterate } from "../util/async";
import { CONNECTION_ID, TENANT } from "../util/const";
import _ from "lodash";
import { TenantHistory } from "../storage/kv";
import { nanoid } from "nanoid";

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

    const subscriptions: Subscription[] = [
      responses.subscribe({
        complete: () => {
          subscriptions.forEach((sub) => sub.unsubscribe());
        },
      }),
    ];

    ctx.signal.addEventListener("abort", () => {
      abort.abort(new Error("Context aborted"));
    });

    (async () => {
      for await (const request of sources.requests) {
        if (abort.signal.aborted) {
          break;
        }

        const requestId = nanoid();

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
            .subscribe((res) => responses.next(res))
        );

        if (mappers.errorToRequest) {
          subscriptions.push(
            fromEvent(abort.signal, "abort")
              .pipe(
                take(1),
                map(() => {
                  if (abort.signal.reason.name === "AbortError") {
                    console.log(
                      chalk.yellow(`[con:${connectionId}] Connection Closed`)
                    );
                  } else {
                    console.warn(
                      chalk.red(
                        `[con:${connectionId}] Connection Aborted: ${abort.signal.reason.message}`
                      )
                    );
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
              .subscribe((res) => responses.next(res))
          );
        }
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

      if (mappers.historyToResponse) {
        subscriptions.push(
          sources.history
            .pipe(map((his) => his.filter(filters.history)))
            .pipe(
              mappers.historyToResponse(
                tenant,
                connectionId,
                response.requestId,
                abort.signal
              )
            )
            .subscribe((res) => responses.next(res))
        );
      }

      yield response.response;
    }

    console.log(chalk.yellow(`[con:${connectionId}] Connection Finished`));
  }
}
