"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.context = exports.RevisionError = void 0;
const dotenv_1 = __importDefault(require("dotenv"));
const dotenv_expand_1 = __importDefault(require("dotenv-expand"));
dotenv_expand_1.default.expand(dotenv_1.default.config());
const events_1 = __importDefault(require("events"));
const base_1 = require("./storage/base");
const connect_1 = require("@connectrpc/connect");
const package_json_1 = require("../package.json");
const path_1 = require("path");
const fs_1 = require("fs");
const cloudrx_1 = require("cloudrx");
const rxjs_1 = require("rxjs");
class RevisionError extends connect_1.ConnectError {
    constructor(message) {
        super(message);
    }
}
exports.RevisionError = RevisionError;
class Context extends events_1.default {
    constructor(opts) {
        super({ captureRejections: true });
        this._abort = opts?.abort || new AbortController();
        this.logger = opts?.logger || undefined;
        this._kvStorage = cloudrx_1.DynamoDB.from("kv", {
            hashKey: "pk",
            rangeKey: "sk",
        });
        this._revisionStorage = cloudrx_1.DynamoDB.from("rev", {
            hashKey: "pk",
            rangeKey: "sk",
        });
        this.leaseStorage = cloudrx_1.DynamoDB.from("lease", {
            hashKey: "pk",
            rangeKey: "sk",
        });
        this.historyStorage = cloudrx_1.DynamoDB.from("his", {
            hashKey: "pk",
            rangeKey: "sk",
        });
        this.on("abort", ({ reason, ctx }) => {
            let context = ctx;
            if (reason instanceof Error) {
                if (!context) {
                    context = `\n\n${`  ==> Context: ${reason.stack || "unknown"}`
                        .split("\n")
                        .join("\n    ")}\n`;
                }
                reason = `${reason.name}: ${reason.message}`;
            }
            if (context) {
                console.warn(`Abort Triggered: ${reason}`, context);
            }
            else {
                console.warn(`Abort Triggered: ${reason}`);
            }
            this._abort.abort(reason);
        });
        process.on("SIGINT", (signal) => {
            console.log("Received SIGINT");
            this.abort(signal, { reason: "SIGINT" }, 0);
        });
        process.on("SIGTERM", (signal) => {
            console.log("Received SIGTERM");
            this.abort(signal, { reason: "SIGTERM" }, 0);
        });
        process.on("uncaughtException", (err) => {
            this.abort("Uncaught Exception", { err }, -1);
        });
        process.on("unhandledRejection", (reason, promise) => {
            this.abort("Unhandled Rejection", { reason, promise }, -1);
        });
    }
    get kvStorage() {
        return (0, rxjs_1.firstValueFrom)(this._kvStorage);
    }
    get revisionStorage() {
        return (0, rxjs_1.firstValueFrom)(this._revisionStorage);
    }
    get unmarshalOptions() {
        return {
            convertWithoutMapWrapper: false,
            wrapNumbers: (value) => parseInt(value, 10),
        };
    }
    get signal() {
        return this._abort.signal;
    }
    get aborter() {
        return this._abort;
    }
    get aborted() {
        return this._abort.signal.aborted;
    }
    async minRevision(tenant, minRevision) {
        if (!this.revisions) {
            this.revisions = new base_1.RevisionTable(await this.revisionStorage);
        }
        const pk = this.revisions._pk(tenant);
        const sk = this.revisions._sk("revision");
        if (minRevision) {
            const { Attributes: item } = await this.revisions
                .update(pk, sk)
                .set("minRevision", minRevision)
                .exec({ ReturnValues: "ALL_NEW" });
            if (!item || !item.minRevision) {
                return 0;
            }
            return item.minRevision;
        }
        else {
            const { Item: item } = await this.revisions
                .get(pk, sk)
                .exec({ ConsistentRead: true });
            if (!item || !item.minRevision) {
                return 0;
            }
            return item.minRevision;
        }
    }
    async currentRevision(tenant) {
        if (!this.revisions) {
            this.revisions = new base_1.RevisionTable(await this.revisionStorage);
        }
        const pk = this.revisions._pk(tenant);
        const sk = this.revisions._sk("revision");
        const { Item } = await this.revisions
            .get(pk, sk)
            .exec({ ConsistentRead: true });
        if (!Item) {
            return this.nextRevision(tenant);
        }
        return Item.revision;
    }
    async nextRevision(tenant) {
        if (!this.revisions) {
            this.revisions = new base_1.RevisionTable(await this.revisionStorage);
        }
        const pk = this.revisions._pk(tenant);
        const sk = this.revisions._sk("revision");
        const updated = await this.revisions
            .update(pk, sk)
            .add("revision", 1)
            .exec({ ReturnValues: "UPDATED_NEW" });
        if (!updated || !updated.Attributes) {
            throw new RevisionError("Failed to increment revision");
        }
        return updated.Attributes.revision;
    }
    async nextLease(tenant) {
        if (!this.revisions) {
            this.revisions = new base_1.RevisionTable(await this.revisionStorage);
        }
        const pk = this.revisions._pk(tenant);
        const sk = this.revisions._sk("lease");
        const updated = await this.revisions
            .update(pk, sk)
            .add("lease", 1)
            .exec({ ReturnValues: "UPDATED_NEW" });
        if (!updated || !updated.Attributes || !updated.Attributes.lease) {
            throw new RevisionError("Failed to increment lease");
        }
        return updated.Attributes.lease;
    }
    async nextWatch(tenant) {
        if (!this.revisions) {
            this.revisions = new base_1.RevisionTable(await this.revisionStorage);
        }
        const pk = this.revisions._pk(tenant);
        const sk = this.revisions._sk("watch");
        const updated = await this.revisions
            .update(pk, sk)
            .add("watch", 1)
            .exec({ ReturnValues: "UPDATED_NEW" });
        if (!updated || !updated.Attributes || !updated.Attributes.watch) {
            throw new RevisionError("Failed to increment lease");
        }
        return updated.Attributes.watch;
    }
    abort(reason, ctx, code = 1) {
        if (!this) {
            // In case this is called from a static context
            Context.default.abort(reason, ctx);
            return;
        }
        if (!reason) {
            return this.emit("abort", {
                reason: "Unknown Reason",
                ctx,
                code,
            });
        }
        if (reason instanceof Error) {
            if (reason.name === "AggregateError" &&
                "errors" in reason &&
                reason.errors &&
                Array.isArray(reason.errors)) {
                return this.emit("abort", {
                    reason: reason.errors.map((err) => err.message).join(", "),
                    ctx,
                    code,
                });
            }
            else {
                return this.emit("abort", { reason: reason.message, ctx, code });
            }
        }
        if (typeof reason === "string") {
            return this.emit("abort", { reason, ctx, code });
        }
        const abort = Object.entries(reason || {}).reduce((acc, [key, value]) => {
            if (value instanceof Error) {
                acc.reason = value;
            }
            acc.ctx[key] = value;
            return acc;
        }, { ctx: ctx || {} });
        this.emit("abort", {
            reason: abort.reason || "Unknown Reason",
            ctx: abort.ctx,
            code,
        });
    }
    async version() {
        console.log(`${package_json_1.name} v${package_json_1.version}`);
    }
    // async status(): Promise<void> {
    //   const client = new Etcd3({
    //     hosts: ["https://127.0.0.1:2379"],
    //     credentials: {
    //       rootCertificate: await this.certfile(),
    //       certChain: await this.certfile(),
    //       privateKey: await this.keyfile(),
    //     },
    //   });
    //   const status = await client.maintenance.status();
    //   console.table(status);
    // }
    async certfile() {
        return (0, fs_1.readFileSync)(process.env.CERTDIR && process.env.CERTFILE
            ? (0, path_1.join)(process.env.CERTDIR, process.env.CERTFILE)
            : `src/certs/localhost.crt`);
    }
    async keyfile() {
        return (0, fs_1.readFileSync)(process.env.CERTDIR && process.env.KEYFILE
            ? (0, path_1.join)(process.env.CERTDIR, process.env.KEYFILE)
            : `src/certs/localhost.key`);
    }
}
Context.default = new Context();
exports.context = Context.default;
exports.default = Context;
