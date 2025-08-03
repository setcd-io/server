"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.KVHandler = void 0;
const connectrpc_etcd_1 = require("@setcd-io/connectrpc-etcd");
const kv_1 = require("../storage/kv");
const base_1 = require("./base");
const serde_1 = require("../storage/serde");
const rxjs_1 = require("rxjs");
const error_1 = require("../util/error");
const chalk_1 = __importDefault(require("chalk"));
class KVHandler extends base_1.BaseHandler {
    constructor(ctx, leaseHandler) {
        super(ctx);
        this.records = new rxjs_1.Subject();
        this.kv = new kv_1.TenantKVTable(ctx, leaseHandler);
        ctx.signal.addEventListener("abort", () => {
            // leases.unsubscribe();
            this.records.complete();
        });
    }
    // TODO: Replace this by putting an HTTP server in CloudRx
    dynamodbHandler() {
        return async (req, reply) => {
            if (req.host !== "dynamodb.amazonaws.com") {
                reply.status(401).send("Unauthorized");
                return;
            }
            const { Records } = req.body;
            // console.log(`Received ${Records.length} records`);
            Records.forEach((record) => {
                this.records.next(record);
            });
            reply.status(200).send();
        };
    }
    async put(tenant, req) {
        const revision = await this.ctx.nextRevision(tenant);
        const { current, previous } = await this.kv.putKey(tenant, req.key, req.value, revision, Number(req.lease));
        const kv = {
            $typeName: "mvccpb.KeyValue",
            key: (0, serde_1.deserialize)(current.key, true),
            value: (0, serde_1.deserialize)(current.value, true),
            createRevision: BigInt(current.createRevision),
            modRevision: BigInt(current.modRevision),
            version: BigInt(current.version),
            lease: BigInt(current.lease),
        };
        const prevKv = previous
            ? {
                $typeName: "mvccpb.KeyValue",
                key: (0, serde_1.deserialize)(previous.key, true),
                value: (0, serde_1.deserialize)(previous.value, true),
                createRevision: BigInt(previous.createRevision),
                modRevision: BigInt(previous.modRevision),
                version: BigInt(previous.version),
                lease: BigInt(previous.lease),
            }
            : undefined;
        return {
            $typeName: "etcdserverpb.PutResponse",
            header: await this.header(tenant),
            kv: kv || undefined,
            prevKv: req.prevKv ? prevKv : undefined,
        };
    }
    async range(tenant, req, options) {
        if (req.revision && req.revision > 0n) {
            const minRevision = await this.ctx.minRevision(tenant);
            if (req.revision < BigInt(minRevision)) {
                throw new error_1.ErrGRPCCompacted();
            }
        }
        if (req.minModRevision && req.minModRevision > 0n) {
            const minRevision = await this.ctx.minRevision(tenant);
            if (req.minModRevision < BigInt(minRevision)) {
                throw new error_1.ErrGRPCCompacted();
            }
        }
        const { count, kvs, more } = await this.kv.range(tenant, req, {
            includeExpired: options?.includeExpired,
            handler: async (kv) => {
                const { revision } = req;
                if (!revision || revision === 0n)
                    return kv;
                if (kv.modRevision > Number(revision)) {
                    const snapshot = await this.kv.latest(tenant, kv.key, Number(revision));
                    if (snapshot) {
                        return {
                            ...kv,
                            value: (0, serde_1.serialize)(snapshot.current.value, "base64", true),
                            createRevision: Number(snapshot.current.createRevision),
                            modRevision: Number(snapshot.current.modRevision),
                            version: Number(snapshot.current.version),
                            lease: Number(snapshot.current.lease),
                        };
                    }
                }
                if (kv.modRevision <= Number(revision)) {
                    return kv;
                }
                return undefined;
            },
        });
        return {
            $typeName: "etcdserverpb.RangeResponse",
            count: BigInt(count),
            kvs: kvs.map((kv) => ({
                $typeName: "mvccpb.KeyValue",
                key: (0, serde_1.deserialize)(kv.key, true),
                value: (0, serde_1.deserialize)(kv.value, true),
                createRevision: BigInt(kv.createRevision),
                modRevision: BigInt(kv.modRevision),
                version: BigInt(kv.version),
                lease: BigInt(kv.lease),
            })),
            more,
            header: await this.header(tenant),
        };
    }
    async deleteRange(tenant, req) {
        const revision = await this.ctx.currentRevision(tenant);
        const deleted = await this.kv.range(tenant, {
            key: req.key,
            rangeEnd: req.rangeEnd,
            maxModRevision: BigInt(revision),
        }, {
            handler: (kv) => this.kv
                .deleteKey(tenant, (0, serde_1.deserialize)(kv.key, true), kv.modRevision)
                .then(() => kv),
        });
        return {
            $typeName: "etcdserverpb.DeleteRangeResponse",
            header: await this.header(tenant),
            deleted: BigInt(deleted.count),
            prevKvs: req.prevKv
                ? deleted.kvs.map((kv) => {
                    return {
                        $typeName: "mvccpb.KeyValue",
                        key: (0, serde_1.deserialize)(kv.key, true),
                        value: (0, serde_1.deserialize)(kv.value, true),
                        createRevision: BigInt(kv.createRevision),
                        modRevision: BigInt(kv.modRevision),
                        version: BigInt(kv.version),
                        lease: BigInt(kv.lease),
                    };
                })
                : [],
        };
    }
    async compact(tenant, req) {
        // TODO: Compaction is supposed to clean up snapshots... do i care??
        // - Maybe maxModRevision should be the old minRevision or req.revision - 1n??
        // - Tests seem to be working without doing anything but simply setting minRevision so range queries fail
        // const deleted = await this.deleteRange(
        //   tenant,
        //   {
        //     $typeName: "etcdserverpb.DeleteRangeRequest",
        //     key: new Uint8Array(1),
        //     rangeEnd: new Uint8Array(1),
        //     prevKv: true,
        //   },
        //   Number(req.revision)
        // );
        const revision = await this.ctx.minRevision(tenant, Number(req.revision));
        console.log("Compacted", {
            tenant,
            revision,
        });
        // console.log(`Compacted ${deleted.prevKvs.length} keys`, {
        //   tenant,
        //   keys: deleted.prevKvs.map((kv) => serialize(kv.key, "utf8", true)),
        //   minRevision: await this.ctx.minRevision(tenant, Number(req.revision)),
        // });
        return {
            $typeName: "etcdserverpb.CompactionResponse",
            header: await this.header(tenant),
        };
    }
    async transact(tenant, req) {
        const revision = await this.ctx.currentRevision(tenant);
        const { success } = await req.compare.reduce(async (accP, c) => {
            return accP.then((acc) => {
                if (!acc.success) {
                    return acc;
                }
                const { key, rangeEnd, target, result, targetUnion } = c;
                const rangeReq = {
                    $typeName: "etcdserverpb.RangeRequest",
                    key,
                    rangeEnd,
                    maxModRevision: BigInt(revision),
                };
                if (result === connectrpc_etcd_1.Compare_CompareResult.EQUAL &&
                    (targetUnion.case === "createRevision" ||
                        targetUnion.case === "modRevision")) {
                    rangeReq.revision = targetUnion.value;
                }
                return this.kv
                    .range(tenant, rangeReq)
                    .then(({ kvs }) => {
                    if (rangeReq.revision && kvs.length === 0) {
                        // Early bail without a revision exact match
                        return { success: false };
                    }
                    return {
                        success: kvs.every((kv) => {
                            const desired = targetUnion.value;
                            const actual = target === connectrpc_etcd_1.Compare_CompareTarget.VERSION
                                ? BigInt(kv.version)
                                : target === connectrpc_etcd_1.Compare_CompareTarget.CREATE
                                    ? BigInt(kv.createRevision)
                                    : target === connectrpc_etcd_1.Compare_CompareTarget.MOD
                                        ? BigInt(kv.modRevision)
                                        : target === connectrpc_etcd_1.Compare_CompareTarget.VALUE
                                            ? (0, serde_1.deserialize)(kv.value)
                                            : target === connectrpc_etcd_1.Compare_CompareTarget.LEASE
                                                ? BigInt(kv.lease)
                                                : undefined;
                            if (typeof actual === "bigint" && typeof desired === "bigint") {
                                return result === connectrpc_etcd_1.Compare_CompareResult.EQUAL
                                    ? actual === desired
                                    : connectrpc_etcd_1.Compare_CompareResult.NOT_EQUAL
                                        ? actual !== desired
                                        : connectrpc_etcd_1.Compare_CompareResult.GREATER
                                            ? actual > desired
                                            : connectrpc_etcd_1.Compare_CompareResult.LESS
                                                ? actual < desired
                                                : false;
                            }
                            if (actual instanceof Uint8Array &&
                                desired instanceof Uint8Array) {
                                const a = (0, serde_1.serialize)(actual, "utf8", true);
                                const d = (0, serde_1.serialize)(desired, "utf8", true);
                                return result === connectrpc_etcd_1.Compare_CompareResult.EQUAL
                                    ? a.localeCompare(d) === 0
                                    : connectrpc_etcd_1.Compare_CompareResult.NOT_EQUAL
                                        ? a.localeCompare(d) !== 0
                                        : connectrpc_etcd_1.Compare_CompareResult.GREATER
                                            ? a.localeCompare(d) > 0
                                            : connectrpc_etcd_1.Compare_CompareResult.LESS
                                                ? a.localeCompare(d) < 0
                                                : false;
                            }
                            return false;
                        }),
                    };
                })
                    .catch((e) => {
                    console.warn(chalk_1.default.red("kv txn compare error"), e.message);
                    return { success: false };
                });
            });
        }, Promise.resolve({ success: true }));
        const response = await (success ? req.success : req.failure).reduce((accP, { request }) => {
            const chain = accP.then((acc) => {
                if (!acc.succeeded) {
                    return acc;
                }
                if (request.case === "requestPut") {
                    return this.put(tenant, request.value)
                        .then((r) => {
                        acc.responses.push({
                            $typeName: "etcdserverpb.ResponseOp",
                            response: { case: "responsePut", value: r },
                        });
                        return acc;
                    })
                        .catch((e) => {
                        console.warn("kv txn put error", e.message);
                        acc.succeeded = false;
                        return acc;
                    });
                }
                else if (request.case === "requestRange") {
                    return this.range(tenant, request.value)
                        .then((r) => {
                        acc.responses.push({
                            $typeName: "etcdserverpb.ResponseOp",
                            response: { case: "responseRange", value: r },
                        });
                        return acc;
                    })
                        .catch((e) => {
                        console.warn("kv txn range error", e.message);
                        acc.succeeded = false;
                        return acc;
                    });
                }
                else if (request.case === "requestDeleteRange") {
                    return this.deleteRange(tenant, request.value)
                        .then((r) => {
                        acc.responses.push({
                            $typeName: "etcdserverpb.ResponseOp",
                            response: { case: "responseDeleteRange", value: r },
                        });
                        return acc;
                    })
                        .catch((e) => {
                        console.warn("kv txn delete error", e.message);
                        acc.succeeded = false;
                        return acc;
                    });
                }
                else if (request.case === "requestTxn") {
                    return this.transact(tenant, request.value)
                        .then((r) => {
                        acc.responses.push({
                            $typeName: "etcdserverpb.ResponseOp",
                            response: { case: "responseTxn", value: r },
                        });
                        acc.succeeded = acc.succeeded && r.succeeded;
                        return acc;
                    })
                        .catch((e) => {
                        console.warn("kv txn txn error", e.message);
                        acc.succeeded = false;
                        return acc;
                    });
                }
                return acc;
            });
            return chain;
        }, Promise.resolve({
            $typeName: "etcdserverpb.TxnResponse",
            succeeded: true,
            responses: [],
        }));
        response.header = await this.header(tenant);
        response.succeeded = response.succeeded && success;
        return response;
    }
}
exports.KVHandler = KVHandler;
