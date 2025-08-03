"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.TenantKVTable = exports.NotFoundError = exports._INTERNAL = exports._INTERNAL_LEASE_ID__LEASES = void 0;
const base_1 = require("./base");
const connect_1 = require("@connectrpc/connect");
const connectrpc_etcd_1 = require("@setcd-io/connectrpc-etcd");
const serde_1 = require("./serde");
const rxjs_1 = require("rxjs");
const error_1 = require("../util/error");
const chalk_1 = __importDefault(require("chalk"));
const cloudrx_1 = require("cloudrx");
const log_1 = require("../util/log");
exports._INTERNAL_LEASE_ID__LEASES = -1;
exports._INTERNAL = {
    LEASE_ID: exports._INTERNAL_LEASE_ID__LEASES,
};
class NotFoundError extends connect_1.ConnectError {
    constructor() {
        super("Not Found");
    }
}
exports.NotFoundError = NotFoundError;
const intoKv = (item) => {
    return {
        $typeName: "mvccpb.KeyValue",
        key: (0, serde_1.deserialize)(item.key, true),
        value: (0, serde_1.deserialize)(item.value, true),
        createRevision: BigInt(item.createRevision),
        modRevision: BigInt(item.modRevision),
        version: BigInt(item.version),
        lease: BigInt(item.lease),
    };
};
const lastChar = (str) => {
    if (!str || str.length === 0) {
        return undefined;
    }
    return str.charCodeAt(str.length - 1);
};
const HISTORY_TIMEOUT = 1000;
const HISTORY_SIZE = HISTORY_TIMEOUT / 10;
class TenantKVTable extends base_1.TenantTable {
    constructor(ctx, leaseHandler) {
        super(ctx, "kv");
        this.leaseHandler = leaseHandler;
        this.history = new cloudrx_1.CloudReplaySubject(ctx.historyStorage);
        this.history.on("expired", (h) => {
            (0, log_1.log)(h.current, {
                level: "info",
                tenant: h.tenant,
                action: "KeyVaue Expired",
                context: {
                    action: h.action,
                },
            });
            queueMicrotask(() => this.deleteKey(h.tenant, h.current.key, Number(h.current.modRevision)));
        });
        ctx.on("abort", () => {
            // expiration.unsubscribe();
        });
    }
    history$(tenant) {
        return (0, rxjs_1.from)(this.ctx.minRevision(tenant)).pipe((0, rxjs_1.switchMap)((minRevision) => this.history.pipe((0, rxjs_1.filter)((h) => h.tenant === tenant && h.current.modRevision >= minRevision))), (0, rxjs_1.tap)((h) => {
            (0, log_1.log)(h.previous, {
                level: "info",
                action: "KeyValueHistory",
                tenant: h.tenant,
                output: h.current,
                context: {
                    action: h.action,
                },
            });
        }), 
        // bufferTime(HISTORY_TIMEOUT, undefined, HISTORY_SIZE),
        (0, rxjs_1.map)((h) => [h]), (0, rxjs_1.share)());
    }
    async putKey(tenant, key, value, revision, lease, opts) {
        const table = await this.table(tenant);
        let current = {
            pk: table.pk(),
            sk: table.sk(key),
            tenant,
            key: (0, serde_1.serialize)(key, "utf8", true),
            value: (0, serde_1.serialize)(value, "base64", true),
            createRevision: revision,
            modRevision: revision,
            version: 1,
            lease: lease,
            expires: opts?.expires,
            serial: "", // Calculated
        };
        if (lease > 0) {
            const relativeLease = await this.leaseHandler.getLease(tenant, lease);
            current.expires = relativeLease.expires;
            current.lease = Number(relativeLease.ID);
        }
        try {
            let update = table
                .update(current.pk, current.sk)
                .set("value", current.value)
                .set("modRevision", current.modRevision)
                .add("version", current.version)
                .condition((c) => c
                .attributeExists("key")
                .and((c) => c.eq("tenant", current.tenant))
                .and((c) => c.eq("lease", current.lease))
                .and((c) => c.eq("key", current.key))
                .and((c) => c.gte("version", current.version)));
            if (current.expires) {
                update = update.set("expires", current.expires);
            }
            else {
                update = update.remove("expires");
            }
            const { Attributes } = await update.exec({ ReturnValues: "ALL_OLD" });
            if (Attributes) {
                const previous = Attributes;
                current.createRevision = previous.createRevision;
                current.version = previous.version + 1;
                current.expires = previous.expires;
                this.history.next({
                    tenant,
                    action: "PUT",
                    current: intoKv(current),
                    previous: intoKv(previous),
                }, current.expires);
                return { current, previous };
            }
        }
        catch (e) {
            if (e.name !== "ConditionalCheckFailedException") {
                throw e;
            }
            let insert = table
                .update(current.pk, current.sk)
                .set("tenant", current.tenant)
                .set("lease", current.lease)
                .set("key", current.key)
                .set("value", current.value)
                .set("createRevision", current.createRevision)
                .set("modRevision", current.modRevision)
                .set("version", 1);
            if (current.expires) {
                insert = insert.set("expires", current.expires);
            }
            else {
                insert = insert.remove("expires");
            }
            const { Attributes } = await insert.exec({ ReturnValues: "ALL_NEW" });
            if (!Attributes) {
                throw new connect_1.ConnectError("Failed to put key: missing attributes");
            }
            current = Attributes;
            this.history.next({
                tenant,
                action: "PUT",
                current: intoKv(current),
            }, current.expires);
            return {
                current,
            };
        }
        throw new connect_1.ConnectError("Failed to put key");
    }
    async deleteKey(tenant, key, revision) {
        const table = await this.table(tenant);
        const value = (0, serde_1.serialize)(new Uint8Array(0), "base64", true);
        const modRevision = await this.ctx.nextRevision(tenant);
        const version = 0;
        const lease = 0;
        const expires = Math.floor(Date.now() / 1000);
        const query = table
            .update(table.pk(), table.sk(key))
            .set("value", (0, serde_1.serialize)(new Uint8Array(0), "base64", true))
            .set("modRevision", modRevision)
            .set("version", version)
            .set("lease", lease)
            .set("expires", expires)
            .condition((c) => c.lte("modRevision", revision));
        try {
            const { Attributes: previous } = await query.exec({
                ReturnValues: "ALL_OLD",
            });
            if (!previous) {
                console.debug(chalk_1.default.yellow("Key already deleted"), {
                    pk: table.pk(),
                    sk: table.sk(key),
                });
                return undefined;
            }
            const current = {
                ...previous,
                value,
                modRevision,
                version,
                lease,
                expires,
            };
            this.history.next({
                tenant,
                action: "DELETE",
                current: intoKv(current),
                previous: intoKv(previous),
            });
            return current;
        }
        catch (e) {
            if (e instanceof Error && e.name === "ConditionalCheckFailedException") {
                return undefined;
            }
            console.warn("Unable to delete key", {
                pk: table.pk(),
                sk: table.sk(key),
                message: e.message,
            });
            throw e;
        }
    }
    async range(tenant, rangeRequest, opts) {
        const table = await this.table(tenant);
        const { key, rangeEnd } = rangeRequest;
        if (!key || !rangeEnd) {
            throw new error_1.ErrGRPCEmptyKey();
        }
        const key$ = (0, serde_1.serialize)(key, "utf8", true);
        const rangeEnd$ = (0, serde_1.serialize)(rangeEnd, "utf8", true);
        // Track if we're using the between operator
        // - DDB is inclusive on rangeEnd, we need to filter to make it exclusive
        let between = false;
        let query = table
            .query()
            .keyCondition((c) => c.eq("pk", table.pk()))
            .keyCondition((c) => {
            /*
            Ruleset from rangeEnd:
             - 1) range_end is the upper bound on the requested range [key, range_end).
             - 2) If range_end is '\0', the range is all keys >= key.
             - 3) If range_end is key plus one (e.g., "aa"+1 == "ab", "a\xff"+1 == "b"), then the range request gets all keys prefixed with key.
             - 4) If both key and range_end are '\0', then the range request returns all keys.
             - 5) [default] if rangeEnd is empty or '\0' then we do an exact match on key
            */
            // Impl: 5) [default] if rangeEnd === [] then we do an exact match on key
            if (!!key$ && !rangeEnd$) {
                return c.eq("sk", table.sk(key$));
            }
            // Impl: 4) If both key and range_end are '\0', then the range request returns all keys.
            if (key.length == 1 &&
                rangeEnd.length === 1 &&
                key.at(0) === 0 &&
                rangeEnd.at(0) === 0) {
                return c.beginsWith("sk", table.sk(""));
            }
            // Impl: 3) If range_end is key plus one (e.g., "aa"+1 == "ab", "a\xff"+1 == "b"), then the range request gets all keys prefixed with key.
            if (key$ && rangeEnd$ && lastChar(rangeEnd$) === lastChar(key$) + 1) {
                return c.beginsWith("sk", table.sk(key$));
            }
            // Impl: 2) If range_end is '\0', the range is all keys >= key.
            if (rangeEnd.length === 1 || rangeEnd.at(0) === 0) {
                return c.gte("sk", table.sk(key$));
            }
            // Impl: 1) range_end is the upper bound on the requested range [key, range_end).
            between = true;
            return c.between("sk", table.sk(key$), table.sk(rangeEnd$));
        })
            .filter((f) => {
            // Impl 1) range_end is the upper bound on the requested range [key, range_end).
            if (between) {
                // Filter out the inclusive rangeEnd on "sk"
                return f.lt("key", rangeEnd$);
            }
            return f;
        })
            .filter((f) => {
            if (opts?.leaseId === exports._INTERNAL.LEASE_ID) {
                return f;
            }
            if (opts?.includeExpired) {
                return f;
            }
            return f
                .attributeNotExists("expires")
                .or((f) => f.gte("expires", Math.floor(Date.now() / 1000)));
        })
            .filter((f) => {
            if (opts?.includeExpired) {
                return f;
            }
            return f.gt("version", 0);
        })
            .filter((f) => {
            if (!rangeRequest.minModRevision) {
                return f;
            }
            return f.gte("modRevision", Number(rangeRequest.minModRevision));
        })
            .filter((f) => {
            if (!rangeRequest.maxModRevision) {
                return f;
            }
            return f.lte("modRevision", Number(rangeRequest.maxModRevision));
        })
            .filter((f) => {
            if (!rangeRequest.minCreateRevision) {
                return f;
            }
            return f.gte("createRevision", Number(rangeRequest.minCreateRevision));
        })
            .filter((f) => {
            if (!rangeRequest.maxCreateRevision) {
                return f;
            }
            return f.lte("createRevision", Number(rangeRequest.maxCreateRevision));
        });
        if (opts && !!opts.leaseId && opts.leaseId !== 0) {
            query = query.filter((f) => f.eq("lease", Number(opts.leaseId)));
        }
        else {
            // Internal KVs are tracked in negative space
            query = query.filter((f) => f.gte("lease", 0));
        }
        if (rangeRequest.sortOrder === connectrpc_etcd_1.RangeRequest_SortOrder.DESCEND) {
            query = query.reverseIndex();
        }
        const _q = query.serialize();
        const leases = opts?.leaseId !== exports._INTERNAL.LEASE_ID
            ? await this.leaseHandler.all(tenant)
            : [];
        let items = await (0, base_1.all)(query, (i) => {
            return (i.lease <= 0 || leases.some((l) => BigInt(l.ID) === BigInt(i.lease)));
        });
        if (opts?.handler) {
            items = (await Promise.all(items.map((item) => opts.handler(item)))).filter((item) => !!item);
        }
        const kvs = rangeRequest.limit
            ? items.slice(0, Number(rangeRequest.limit))
            : items;
        return {
            count: items.length,
            kvs,
            more: !!rangeRequest.limit && kvs.length !== items.length,
            _q,
        };
    }
    async leased(tenant, leaseId) {
        const table = await this.table(tenant);
        const { Items: items } = await table
            .query()
            .keyCondition((c) => c.eq("pk", table.pk()).and((c) => c.beginsWith("sk", table.sk(""))))
            .filter((f) => f.eq("lease", leaseId))
            .filter((f) => f.gt("version", 0))
            .exec({ ConsistentRead: true });
        return items || [];
    }
    async all(tenant, key) {
        const table = await this.table(tenant);
        const { Items: items } = await table
            .query()
            .keyCondition((c) => c.eq("pk", table.pk()).and((c) => {
            if (!key) {
                return c.beginsWith("sk", table.sk(""));
            }
            if (key instanceof Uint8Array) {
                return c.eq("sk", table.sk((0, serde_1.serialize)(key, "utf8", true)));
            }
            else {
                return c.eq("sk", table.sk(key));
            }
        }))
            .exec({ ConsistentRead: true });
        return items || [];
    }
    async latest(tenant, key, revision) {
        if (key instanceof Uint8Array) {
            key = (0, serde_1.serialize)(key, "utf8", true);
        }
        // Gather all pages
        const pages = this.history
            .snapshot()
            .pipe((0, rxjs_1.map)((h) => h.filter((h) => h.tenant === tenant)));
        // Flatten the pages and filter by key
        const all = pages.pipe((0, rxjs_1.concatAll)(), (0, rxjs_1.filter)((h) => (0, serde_1.serialize)(h.current.key, "utf8", true) === key), (0, rxjs_1.observeOn)(rxjs_1.asyncScheduler), (0, rxjs_1.share)());
        // If no revision is specified, return the latest history event
        if (!revision) {
            return (0, rxjs_1.firstValueFrom)(all.pipe(
            // IDK if i need this
            // filter(
            //   (h) => h.current.modRevision === BigInt(h.current.createRevision)
            // ),
            (0, rxjs_1.toArray)(), (0, rxjs_1.map)((histories) => histories.slice(-1)[0])));
        }
        // If a revision is specified, return the history event with that revision
        return (0, rxjs_1.firstValueFrom)(all.pipe((0, rxjs_1.filter)((h) => BigInt(h.current.modRevision) === BigInt(revision)), (0, rxjs_1.toArray)(), (0, rxjs_1.map)((histories) => histories[0])));
    }
}
exports.TenantKVTable = TenantKVTable;
