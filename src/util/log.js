"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.logRequest = exports.log = exports.stringify = void 0;
const connect_1 = require("@connectrpc/connect");
const chalk_1 = __importDefault(require("chalk"));
const const_1 = require("./const");
const stringify = (message) => {
    if (message instanceof connect_1.ConnectError) {
        return { message: message.message };
    }
    let revision = undefined;
    const replacer = (key, value) => {
        if (key === "$typeName") {
            return undefined;
        }
        if (key === "header" &&
            value &&
            "$typeName" in value &&
            value.$typeName === "etcdserverpb.ResponseHeader" &&
            "revision" in value) {
            revision = Number(value.revision);
            return undefined;
        }
        if (value instanceof Uint8Array) {
            if (key === "key") {
                return Buffer.from(value).toString("utf8");
            }
            if (key === "rangeEnd") {
                return Buffer.from(value).toString("utf8");
            }
            return `Uint8Array(${value.length})`;
        }
        if (Array.isArray(value) && value.length === 0) {
            return undefined;
        }
        if (!value) {
            return undefined;
        }
        if (typeof value === "bigint" || value instanceof BigInt) {
            return Number(value);
        }
        return value;
    };
    return { message: JSON.stringify(message, replacer), revision };
};
exports.stringify = stringify;
function peek(message, callback) {
    if ("$typeName" in message) {
        callback(message);
        return;
    }
    const wrapped = (next) => {
        return async (...args) => {
            const result = await next(...args);
            if (!result.done) {
                callback(result.value);
            }
            return result;
        };
    };
    if ("next" in message && typeof message.next === "function") {
        message.next = wrapped(message.next.bind(message));
    }
    if (Symbol.asyncIterator in message) {
        const iterator = message[Symbol.asyncIterator]();
        const nextFn = iterator.next.bind(iterator);
        const returnFn = iterator.return?.bind(iterator);
        const throwFn = iterator.throw?.bind(iterator);
        message[Symbol.asyncIterator] = () => {
            return {
                next: wrapped(nextFn),
                return: returnFn
                    ? (...args) => {
                        return returnFn(...args);
                    }
                    : undefined,
                throw: throwFn
                    ? (...args) => {
                        return throwFn(...args);
                    }
                    : undefined,
            };
        };
    }
}
const log = (message, { level = "info", tenant = const_1.DEFAULT_TENANT, action, output, context, } = { level: "info", tenant: const_1.DEFAULT_TENANT }) => {
    const severityColor = level === "error"
        ? chalk_1.default.red
        : level === "warn"
            ? chalk_1.default.yellow
            : level === "info"
                ? chalk_1.default.blue
                : chalk_1.default.green;
    const symbol = level === "error"
        ? severityColor("✘")
        : level === "warn"
            ? severityColor("▲")
            : level === "info"
                ? severityColor("ℹ︎")
                : severityColor("✔︎");
    tenant =
        tenant === const_1.DEFAULT_TENANT ? chalk_1.default.yellow(tenant) : chalk_1.default.cyan(tenant);
    action = chalk_1.default.green(action);
    if (message && typeof message !== "string") {
        message = (0, exports.stringify)(message).message;
    }
    if (output && typeof output !== "string") {
        const { message, revision } = (0, exports.stringify)(output);
        output = message;
        if (revision) {
            context = {
                ...context,
                rev: revision,
            };
        }
    }
    let io = chalk_1.default.yellow(message ? message : chalk_1.default.dim("[empty]"));
    if (output) {
        io = `${io} ${chalk_1.default.dim("==>")} ${severityColor(output)}`;
    }
    context = Object.entries(context || {}).reduce((acc, [key, value]) => {
        acc[key] = chalk_1.default.dim(`[${key}:${chalk_1.default.blue(value)}] `);
        return acc;
    }, {});
    console.log(`${symbol} [${tenant}] ${action}: ${io} ${Object.values(context).join("")}`);
};
exports.log = log;
const logRequest = async (req, next) => {
    const id = req.contextValues?.get(const_1.CONNECTION_ID);
    const tenant = req.contextValues?.get(const_1.TENANT);
    const _log = (incoming, outgoing) => {
        if (!incoming || !outgoing) {
            return;
        }
        const context = {
            con: id,
        };
        const { message: input } = (0, exports.stringify)(incoming);
        const { message: output, revision } = (0, exports.stringify)(outgoing);
        if (revision) {
            context["rev"] = revision;
        }
        let level = outgoing instanceof connect_1.ConnectError ? "error" : "success";
        (0, exports.log)(input, {
            level,
            tenant,
            action: req.method.name,
            output,
            context,
        });
    };
    return next(req)
        .then((res) => {
        let incoming;
        peek(req.message, (msg) => (incoming = msg));
        peek(res.message, (msg) => _log(incoming, msg));
        return res;
    })
        .catch((err) => {
        if (err instanceof connect_1.ConnectError) {
            peek(req.message, (incoming) => _log(incoming, err));
        }
        throw err;
    });
};
exports.logRequest = logRequest;
