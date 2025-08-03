"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.LeaseHandler = exports.ONE_DAY_SEC = void 0;
const base_1 = require("./base");
const lodash_1 = __importDefault(require("lodash"));
const rxjs_1 = require("rxjs");
const serde_1 = require("../storage/serde");
const connect_1 = require("@connectrpc/connect");
const cloudrx_1 = require("cloudrx");
const log_1 = require("../util/log");
exports.ONE_DAY_SEC = 24 * 60 * 60;
const mapLease = () => {
    return (source) => {
        return new rxjs_1.Observable((subscriber) => {
            const subscription = source
                .pipe((0, rxjs_1.map)((lease) => {
                const relative = {
                    ...lease,
                    ttlRelative: Math.min(Math.max(lease.expires - cloudrx_1.CloudProvider.TIME(), 0), Number(lease.TTL)),
                };
                if (lease.TTL <= BigInt(0)) {
                    relative.ttlRelative = 0;
                }
                return relative;
            }))
                .subscribe(subscriber);
            return () => {
                subscription.unsubscribe();
            };
        });
    };
};
class LeaseHandler extends base_1.BaseHandler {
    constructor(ctx) {
        super(ctx);
        this._leases = new cloudrx_1.CloudReplaySubject(ctx.leaseStorage, {
            hashFn: (value) => `${value.tenant}:${value.ID}`,
        });
        this.leases = this._leases.pipe(mapLease(), (0, rxjs_1.share)());
    }
    async grant(ctx, req) {
        const tenant = this.getTenant(ctx);
        if (req.TTL >= exports.ONE_DAY_SEC) {
            throw new connect_1.ConnectError("Lease TTL must be less than 1 day");
        }
        if (req.ID !== BigInt(0)) {
            throw new connect_1.ConnectError("Unsupported: Client specified Lease ID");
        }
        const leaseId = req.ID || (await this.ctx.nextLease(tenant));
        const ttl = Number(req.TTL);
        const expires = cloudrx_1.CloudProvider.TIME() + ttl;
        const response = {
            $typeName: "etcdserverpb.LeaseGrantResponse",
            header: await this.header(tenant),
            ID: BigInt(leaseId),
            TTL: BigInt(ttl),
            error: "",
        };
        const current = {
            $typeName: "etcdserverpb.LeaseGrantResponse",
            ID: BigInt(leaseId),
            TTL: BigInt(ttl),
            expires,
            error: "",
            tenant,
        };
        this._leases.next(current, expires);
        return response;
    }
    async revoke(ctx, req) {
        const tenant = typeof ctx === "string" ? ctx : this.getTenant(ctx);
        const lease = await this.getLease(tenant, Number(req.ID));
        const now = cloudrx_1.CloudProvider.TIME();
        lease.TTL = BigInt(0);
        lease.expires = now;
        lease.error = "Revoked";
        this._leases.next(lease, now);
        await new Promise((resolve) => {
            const handler = (expired) => {
                if (expired.tenant === tenant &&
                    BigInt(expired.ID) === BigInt(req.ID)) {
                    this._leases.off("expired", handler);
                    resolve();
                }
            };
            this._leases.on("expired", handler);
        });
        return {
            $typeName: "etcdserverpb.LeaseRevokeResponse",
            header: await this.header(tenant),
        };
    }
    async all(tenant) {
        const pages = this._leases
            .snapshot()
            .pipe((0, rxjs_1.map)((l) => l.filter((l) => l.tenant === tenant && l.expires > cloudrx_1.CloudProvider.TIME())));
        return (0, rxjs_1.firstValueFrom)(pages.pipe((0, rxjs_1.concatAll)(), mapLease(), (0, rxjs_1.toArray)()));
    }
    async getLease(tenant, leaseId) {
        const leases = await this.all(tenant);
        let lease = leases.find((l) => BigInt(l.ID) === BigInt(leaseId));
        if (!lease) {
            return {
                $typeName: "etcdserverpb.LeaseGrantResponse",
                ID: BigInt(leaseId),
                TTL: BigInt(0),
                expires: cloudrx_1.CloudProvider.TIME(),
                error: "Lease not found",
                tenant,
                ttlRelative: 0,
            };
        }
        return lease;
    }
    async timeToLive(ctx, req, kv) {
        const tenant = typeof ctx === "string" ? ctx : this.getTenant(ctx);
        const lease = await this.getLease(tenant, Number(req.ID));
        const now = cloudrx_1.CloudProvider.TIME();
        const ttlRelative = Math.min(Math.max(lease.expires - now, 0), Number(lease.TTL));
        let keys = [];
        if (req.keys) {
            // Full search with lease
            const { kvs } = await kv.range(tenant, {
                $typeName: "etcdserverpb.RangeRequest",
                key: new Uint8Array(1),
                rangeEnd: new Uint8Array(1),
            }, { leaseId: Number(lease.ID) });
            keys = kvs.map((kv) => (0, serde_1.deserialize)(kv.key, true));
        }
        return {
            $typeName: "etcdserverpb.LeaseTimeToLiveResponse",
            header: await this.header(tenant),
            grantedTTL: BigInt(lease.TTL),
            ID: BigInt(lease.ID),
            TTL: BigInt(ttlRelative),
            keys,
        };
    }
    async listLeases(ctx, req) {
        const tenant = this.getTenant(ctx);
        const leases = await this.all(tenant);
        return {
            $typeName: "etcdserverpb.LeaseLeasesResponse",
            header: await this.header(tenant),
            leases: leases.map((lease) => {
                const status = {
                    $typeName: "etcdserverpb.LeaseStatus",
                    ID: BigInt(lease.ID),
                };
                return status;
            }),
        };
    }
    keepAlive(ctx, requests) {
        return this.bidi(ctx, {
            // history: this.history$(this.getTenant(ctx)),
            history: rxjs_1.NEVER, // No history for keepAlive
            requests,
        }, {
            history: (his) => his.tenant === this.getTenant(ctx),
            response: (res) => res.tenant === this.getTenant(ctx),
        }, {
            requestToResponse: (tenant, connectionId, requestId, signal) => {
                return this.mapRequestToResponse(tenant, connectionId, requestId, signal);
            },
            historyToResponse: (tenant, connectionId, requestId, signal) => {
                return () => rxjs_1.NEVER;
            },
            errorToResponse: (tenant, connectionId, requestId, signal) => {
                return this.mapErrorToResponse(tenant, connectionId, requestId, signal);
            },
        }, {
            response: async (tenant, connectionId, res) => {
                res = lodash_1.default.cloneDeep(res);
                res.response.header = await this.header(tenant);
                return res;
            },
        });
    }
    mapRequestToResponse(tenant, connectionId, requestId, signal) {
        return (source) => {
            return new rxjs_1.Observable((subscriber) => {
                const subscription = source
                    .pipe((0, rxjs_1.switchMap)((source) => {
                    const immediate = (0, rxjs_1.from)(this.getLease(tenant, Number(source.ID))).pipe((0, rxjs_1.map)((lease) => {
                        const keepAlive = {
                            $typeName: "etcdserverpb.LeaseKeepAliveResponse",
                            ID: source.ID,
                            TTL: BigInt(lease.ttlRelative),
                        };
                        return { source, keepAlive };
                    }));
                    const loop = (0, rxjs_1.interval)(1000).pipe((0, rxjs_1.switchMap)(() => this.getLease(tenant, Number(source.ID))), (0, rxjs_1.takeWhile)((lease) => lease.ttlRelative > 0, true), (0, rxjs_1.map)((lease) => {
                        const keepAlive = {
                            $typeName: "etcdserverpb.LeaseKeepAliveResponse",
                            ID: source.ID,
                            TTL: BigInt(lease.ttlRelative),
                        };
                        return { source, keepAlive };
                    }));
                    return (0, rxjs_1.concat)(immediate, loop);
                }), (0, rxjs_1.map)(({ source, keepAlive }) => {
                    const response = {
                        tenant,
                        connectionId,
                        requestId,
                        request: source,
                        response: keepAlive,
                        signal,
                    };
                    return response;
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
    mapErrorToResponse(tenant, connectionId, requestId, signal) {
        return (source) => {
            return new rxjs_1.Observable((subscriber) => {
                const subscription = source.subscribe({
                    next(err) {
                        (0, log_1.log)("Lease Error", {
                            level: "warn",
                            tenant,
                            action: "Lease",
                            output: err.message,
                            context: { con: connectionId, req: requestId },
                        });
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
exports.LeaseHandler = LeaseHandler;
