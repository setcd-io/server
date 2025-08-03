"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.BaseHandler = void 0;
const rxjs_1 = require("rxjs");
const connect_1 = require("@connectrpc/connect");
const const_1 = require("../util/const");
const nanoid_1 = require("nanoid");
const async_1 = require("../util/async");
const log_1 = require("../util/log");
class BaseHandler {
    constructor(ctx) {
        this.ctx = ctx;
    }
    async header(tenant) {
        return {
            $typeName: "etcdserverpb.ResponseHeader",
            revision: BigInt(await this.ctx.currentRevision(tenant)),
            raftTerm: 0n,
            memberId: 0n,
            clusterId: 0n,
        };
    }
    getConnectionId(ctx) {
        const connectionId = ctx.values.get(const_1.CONNECTION_ID);
        if (!connectionId) {
            throw new connect_1.ConnectError("Connection ID not found");
        }
        return connectionId;
    }
    getTenant(ctx) {
        const tenant = ctx.values.get(const_1.TENANT);
        if (!tenant) {
            throw new connect_1.ConnectError("Tenant not found");
        }
        return tenant;
    }
    async *bidi(ctx, sources, filters, mappers, mutators) {
        const abort = new AbortController();
        const tenant = this.getTenant(ctx);
        const connectionId = this.getConnectionId(ctx);
        const responses = new rxjs_1.Subject();
        const subscriptions = [];
        subscriptions.push(responses
            .pipe((0, rxjs_1.catchError)((err) => {
            (0, log_1.log)("Stream error", {
                level: "info",
                tenant,
                action: "Bidi",
                output: err.message,
                context: { con: connectionId },
            });
            return rxjs_1.EMPTY;
        }))
            .subscribe({
            complete: () => {
                (0, log_1.log)("Stream completed", {
                    level: "info",
                    tenant,
                    action: "Bidi",
                    context: { con: connectionId },
                });
            },
        }));
        ctx.signal.addEventListener("abort", () => {
            (0, log_1.log)("Context aborted", {
                level: "warn",
                tenant,
                action: "Bidi",
                context: { con: connectionId },
            });
            abort.abort(ctx.signal.reason);
        });
        abort.signal.addEventListener("abort", () => {
            (0, log_1.log)("Stream aborted", {
                level: "warn",
                tenant,
                action: "Bidi",
                context: { con: connectionId },
            });
            responses.error(abort.signal.reason);
        });
        (async () => {
            for await (const request of sources.requests) {
                if (abort.signal.aborted) {
                    return;
                }
                const requestId = (0, nanoid_1.nanoid)(8);
                subscriptions.push((0, rxjs_1.of)(request)
                    .pipe(mappers.requestToResponse(tenant, connectionId, requestId, abort.signal))
                    .subscribe({
                    next: (res) => responses.next(res),
                    error: (err) => {
                        (0, log_1.log)("Unable to map request", {
                            level: "warn",
                            tenant,
                            action: "Bidi",
                            output: err.message,
                            context: { con: connectionId, req: requestId },
                        });
                    },
                    complete: () => {
                        (0, log_1.log)("Request completed", {
                            level: "info",
                            tenant,
                            action: "Bidi",
                            context: { con: connectionId, req: requestId },
                        });
                    },
                }));
                subscriptions.push(sources.history
                    .pipe((0, rxjs_1.map)((his) => his.filter(filters.history)))
                    .pipe(mappers.historyToResponse(tenant, connectionId, requestId, abort.signal))
                    .subscribe({
                    next: (response) => responses.next(response),
                    error: (err) => {
                        (0, log_1.log)("Unable to map history", {
                            level: "warn",
                            tenant,
                            action: "Bidi",
                            output: err.message,
                            context: { con: connectionId, req: requestId },
                        });
                    },
                    complete: () => {
                        (0, log_1.log)("History completed", {
                            level: "info",
                            tenant,
                            action: "Bidi",
                            context: { con: connectionId, req: requestId },
                        });
                    },
                }));
                subscriptions.push(responses
                    .pipe((0, rxjs_1.ignoreElements)(), (0, rxjs_1.catchError)((err) => (0, rxjs_1.of)(err)), mappers.errorToResponse(tenant, connectionId, requestId, abort.signal))
                    .subscribe({
                    next: (response) => {
                        responses.next(response);
                    },
                    error: () => {
                        (0, log_1.log)("Unable to map error", {
                            level: "warn",
                            tenant,
                            action: "Bidi",
                            context: { con: connectionId, req: requestId },
                        });
                    },
                    complete: () => {
                        (0, log_1.log)("Error mapping completed", {
                            level: "info",
                            tenant,
                            action: "Bidi",
                            context: { con: connectionId, req: requestId },
                        });
                    },
                }));
            }
        })()
            .catch((err) => {
            (0, log_1.log)("Requests Error", {
                level: "info",
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
            (0, log_1.log)("Requests Complete", {
                level: "info",
                tenant,
                action: "Bidi",
                context: { con: connectionId },
            });
            responses.error(new Error("Requests Complete"));
        });
        try {
            for await (const response of async_1.AsyncObservable.from(responses.pipe((0, rxjs_1.filter)((req) => req.tenant === tenant), (0, rxjs_1.filter)((res) => filters.response(res)), (0, rxjs_1.concatMap)((res) => mutators.response(tenant, connectionId, res))))) {
                yield response.response;
            }
        }
        catch (err) {
            (0, log_1.log)("Responses Error", {
                level: "warn",
                tenant,
                action: "Bidi",
                output: err.message,
                context: { con: connectionId },
            });
        }
        finally {
            (0, log_1.log)("Responses Complete", {
                level: "info",
                tenant,
                action: "Bidi",
                context: { con: connectionId },
            });
            subscriptions.forEach((sub) => sub.unsubscribe());
        }
    }
}
exports.BaseHandler = BaseHandler;
