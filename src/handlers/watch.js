"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.WatchHandler = void 0;
const connectrpc_etcd_1 = require("@setcd-io/connectrpc-etcd");
const base_1 = require("./base");
const rxjs_1 = require("rxjs");
const serde_1 = require("../storage/serde");
const error_1 = require("../util/error");
const cli_table3_1 = __importDefault(require("cli-table3"));
const util_1 = __importDefault(require("util"));
const lodash_1 = __importDefault(require("lodash"));
const log_1 = require("../util/log");
const isWatched = (watch, requestId, kv) => {
    if (watch.requestId !== requestId) {
        return false;
    }
    const key = (0, serde_1.serialize)(kv?.key, "utf8", false);
    if (!key) {
        return false;
    }
    const startKey = (0, serde_1.serialize)(watch.key, "utf8", true);
    if (watch.rangeEnd.length === 0 && key === startKey) {
        // Exact match
        return true;
    }
    if (watch.rangeEnd.length === 1 &&
        watch.rangeEnd[0] === 0 &&
        key.startsWith(startKey)) {
        return true;
    }
    if (watch.key.length === watch.rangeEnd.length &&
        watch.key.slice(-1)[0] + 1 === watch.rangeEnd.slice(-1)[0] &&
        key.startsWith(startKey)) {
        return true;
    }
    const endKey = (0, serde_1.serialize)(watch.rangeEnd, "utf8", true);
    if (key.localeCompare(startKey) >= 0 && key.localeCompare(endKey) < 0) {
        return true;
    }
    return false;
};
class WatchHandler extends base_1.BaseHandler {
    constructor(ctx, kv) {
        super(ctx);
        this.kv = kv;
        this.watches = new Map();
    }
    stats() {
        const _stats = Array.from(this.watches.entries()).reduce((acc, [tenant, watches]) => {
            const watchIds = Array.from(watches.keys()).map((id) => id.toString());
            const connectionIds = Array.from(watches.values())
                .map((watch) => watch.connectionId)
                .filter((id) => !!id);
            acc[tenant] = {
                watchIds,
                connectionIds: Array.from(new Set(connectionIds)),
                age: Math.max(...Array.from(watches.values()).map((watch) => {
                    const createdAt = watch.createdAt.getTime();
                    const now = new Date().getTime();
                    return Math.floor((now - createdAt) / 1000);
                })),
            };
            return acc;
        }, {});
        let table = new cli_table3_1.default({ style: { head: [], border: [] } });
        table.push(["Tenant", "Connection IDs", "Watch IDs", "Age (s)"]);
        Object.entries(_stats).forEach(([tenant, stats]) => {
            if (!stats.watchIds.length || !stats.connectionIds.length) {
                return;
            }
            table.push([
                tenant,
                util_1.default.format(stats.connectionIds),
                util_1.default.format(stats.watchIds),
                util_1.default.format(stats.age),
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
    watch(ctx, requests) {
        return this.bidi(ctx, {
            requests,
            history: this.kv.kv.history$(this.getTenant(ctx)),
        }, {
            history: (his) => {
                return his.tenant === this.getTenant(ctx);
            },
            response: (res) => {
                return (res.tenant === this.getTenant(ctx) &&
                    res.connectionId === this.getConnectionId(ctx));
            },
        }, {
            requestToResponse: (tenant, connectionId, requestId, signal) => {
                return this.mapRequestToResponse(tenant, connectionId, requestId, signal);
            },
            historyToResponse: (tenant, connectionId, requestId, signal) => {
                return this.mapHistoryToResponse(tenant, connectionId, requestId, signal);
            },
            errorToResponse: (tenant, connectionId, requestId, signal) => {
                return this.mapErrorToResponse(tenant, connectionId, requestId, signal);
            },
        }, {
            response: async (tenant, connectionId, res) => {
                res = lodash_1.default.cloneDeep(res);
                res.response.header = await this.header(tenant);
                if (res.request.requestUnion.case === "createRequest" &&
                    res.request.requestUnion.value.startRevision !== 0n &&
                    res.request.requestUnion.value.startRevision <=
                        (await this.ctx.minRevision(res.tenant))) {
                    throw new error_1.ErrGRPCCompacted();
                }
                this.watches.set(tenant, this.watches.get(tenant) || new Map());
                let watch = this.watches
                    .get(tenant)
                    ?.get(Number(res.response.watchId));
                if (!watch && res.request.requestUnion.case === "createRequest") {
                    watch = {
                        ...lodash_1.default.cloneDeep(res.request.requestUnion.value),
                        tenant,
                        connectionId,
                        requestId: res.requestId,
                        watchId: res.response.watchId,
                        createdAt: new Date(),
                        progressNotify: false,
                        prevKv: false,
                    };
                }
                if (!watch) {
                    throw new error_1.ErrGRPCWatchCanceled();
                }
                if (res.request.requestUnion.case === "createRequest") {
                    this.watches.get(tenant)?.set(Number(res.response.watchId), watch);
                }
                if (res.request.requestUnion.case === "cancelRequest") {
                    this.watches.get(tenant)?.delete(Number(res.response.watchId));
                }
                return res;
            },
        });
    }
    mapRequestToResponse(tenant, connectionId, requestId, signal) {
        return (source) => {
            return new rxjs_1.Observable((subscriber) => {
                const subscription = source
                    .pipe((0, rxjs_1.concatMap)((source) => {
                    this.watches.set(tenant, this.watches.get(tenant) || new Map());
                    switch (source.requestUnion.case) {
                        case "createRequest":
                            return (0, rxjs_1.combineLatest)([
                                this.ctx.nextWatch(tenant),
                                this.ctx.currentRevision(tenant),
                            ]).pipe((0, rxjs_1.map)(([watchId, currentRevision]) => {
                                const response = {
                                    $typeName: "etcdserverpb.WatchResponse",
                                    watchId: BigInt(watchId),
                                    compactRevision: 0n,
                                    events: [],
                                    canceled: false,
                                    cancelReason: "",
                                    created: true,
                                    fragment: false,
                                };
                                if (source.requestUnion.case === "createRequest" &&
                                    source.requestUnion.value.startRevision === 0n) {
                                    source.requestUnion.value.startRevision =
                                        BigInt(currentRevision);
                                }
                                return {
                                    tenant,
                                    connectionId,
                                    requestId,
                                    request: source,
                                    response,
                                    signal,
                                };
                            }));
                        case "cancelRequest":
                            return (0, rxjs_1.of)(this.watches
                                .get(tenant)
                                ?.get(Number(source.requestUnion.value.watchId)))
                                .pipe((0, rxjs_1.filter)((watch) => !!watch))
                                .pipe((0, rxjs_1.map)((watch) => {
                                const response = {
                                    $typeName: "etcdserverpb.WatchResponse",
                                    watchId: watch.watchId,
                                    compactRevision: 0n,
                                    events: [],
                                    canceled: true,
                                    cancelReason: watch.cancelReason ||
                                        signal.reason?.message ||
                                        `User Requested Cancel`,
                                    created: false,
                                    fragment: false,
                                };
                                return {
                                    tenant,
                                    connectionId,
                                    requestId,
                                    request: source,
                                    response,
                                    signal,
                                };
                            }));
                        default:
                            throw new Error(`Unimplemented watch case: ${source.requestUnion.case}`);
                    }
                }))
                    .pipe((0, rxjs_1.switchMap)((res) => {
                    // Conditional fanout for progressNotify
                    res = lodash_1.default.cloneDeep(res);
                    if (res.request.requestUnion.case !== "createRequest") {
                        return (0, rxjs_1.of)(res);
                    }
                    if (!res.request.requestUnion.value.progressNotify) {
                        // No progress notify, don't set an interval
                        return (0, rxjs_1.of)(res);
                    }
                    const progressNotify = (0, rxjs_1.interval)(1000).pipe((0, rxjs_1.map)(() => this.watches
                        .get(res.tenant)
                        ?.get(Number(res.response.watchId))), (0, rxjs_1.takeWhile)((watch) => !!watch, false), (0, rxjs_1.map)(() => {
                        const response = lodash_1.default.cloneDeep(res);
                        response.response.created = false;
                        response.response.canceled = false;
                        response.response.events = [];
                        console.log("!!! progress notify response", response);
                        return response;
                    }));
                    // Emit immediate response first, then start interval asynchronously
                    return (0, rxjs_1.concat)((0, rxjs_1.of)(res), progressNotify);
                }))
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
    mapHistoryToResponse(tenant, connectionId, requestId, signal) {
        return (source) => {
            return new rxjs_1.Observable((subscriber) => {
                const subscription = source
                    .pipe((0, rxjs_1.mergeMap)((histories) => (0, rxjs_1.from)(histories).pipe((0, rxjs_1.filter)((his) => his.tenant === tenant), (0, rxjs_1.mergeMap)((history) => {
                    const watchers = [
                        ...(this.watches.get(tenant)?.values() || []),
                    ]
                        .filter((watch) => isWatched(watch, requestId, history.current))
                        .map((watch) => ({ watch, history }));
                    (0, log_1.log)(history.current, {
                        level: "info",
                        tenant,
                        action: "Watcher",
                        output: `${watchers.map((w) => (0, log_1.stringify)(w.watch).message)}`,
                        context: {
                            con: connectionId,
                            req: requestId,
                        },
                    });
                    return (0, rxjs_1.from)(watchers);
                }), (0, rxjs_1.groupBy)((x) => x.watch.watchId), (0, rxjs_1.mergeMap)((group$) => group$.pipe((0, rxjs_1.map)((x) => x.history), (0, rxjs_1.toArray)(), (0, rxjs_1.map)((histories) => {
                    const watch = this.watches
                        .get(tenant)
                        ?.get(Number(group$.key));
                    return {
                        watchId: group$.key,
                        watch,
                        histories: histories.filter((history) => {
                            // if (!watch?.prevKv) {
                            //   delete history.previous;
                            // }
                            return (history.current.modRevision >=
                                watch?.startRevision);
                        }),
                    };
                }))), (0, rxjs_1.filter)(({ watch, histories }) => {
                    return !!histories.length || !!watch?.progressNotify;
                }), (0, rxjs_1.map)(({ watchId, histories }) => {
                    const response = {
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
                            watchId: BigInt(watchId),
                            compactRevision: 0n,
                            events: histories.map((history) => {
                                return {
                                    $typeName: "mvccpb.Event",
                                    type: history.action === "PUT"
                                        ? connectrpc_etcd_1.Event_EventType.PUT
                                        : connectrpc_etcd_1.Event_EventType.DELETE,
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
                }))))
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
    mapErrorToResponse(tenant, connectionId, requestId, signal) {
        return (source) => {
            return new rxjs_1.Observable((subscriber) => {
                const subscription = source
                    .pipe((0, rxjs_1.concatMap)((source) => {
                    return (0, rxjs_1.from)(Array.from(this.watches.get(tenant)?.values() || []).map((watch) => {
                        return {
                            ...lodash_1.default.cloneDeep(watch),
                            cancelReason: source.message,
                        };
                    }));
                }), (0, rxjs_1.filter)((watch) => watch.tenant === tenant && watch.connectionId === connectionId), (0, rxjs_1.map)((watch) => {
                    const request = {
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
                }), this.mapRequestToResponse(tenant, connectionId, requestId, signal))
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
}
exports.WatchHandler = WatchHandler;
