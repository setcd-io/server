"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.serialize = serialize;
exports.deserialize = deserialize;
const protobuf_1 = require("@bufbuild/protobuf");
const connectrpc_etcd_1 = require("@setcd-io/connectrpc-etcd");
function serialize(value, encoding, required = false) {
    if (value === undefined) {
        if (!required)
            return undefined;
        throw new Error("serialize: value is required");
    }
    // If the value is a Message (has a $typeName property)
    if (typeof value === "object" && !(value instanceof Uint8Array)) {
        let typeName = undefined;
        let encoded = undefined;
        if ("$typeName" in value && value.$typeName === "mvccpb.KeyValue") {
            typeName = value.$typeName;
            encoded = Buffer.from((0, protobuf_1.toBinary)(connectrpc_etcd_1.KeyValueSchema, value)).toString(encoding);
        }
        else {
            throw new Error("serialize: unknown type");
        }
        return `${typeName}$${encoding}$${encoded}`;
    }
    // Otherwise, treat the value as a Uint8Array.
    if (value instanceof Uint8Array) {
        if (value.length) {
            const prefix = encoding === "utf8" ? "" : "base64$";
            return `${prefix}${Buffer.from(value).toString(encoding)}`;
        }
        if (required) {
            // For an empty Uint8Array, if required, return an empty string.
            return "";
        }
    }
    if (!required)
        return undefined;
    throw new Error("serialize: invalid value provided");
}
function deserialize(value, required = false) {
    if (typeof value !== "string") {
        if (!required)
            return undefined;
        throw new Error("deserialize: value is required");
    }
    // Check for a Uint8Array encoded in base64.
    if (value.startsWith("base64$")) {
        const data = value.slice("base64$".length);
        return new Uint8Array(Buffer.from(data, "base64"));
    }
    // If the string appears to be a serialized Message:
    const parts = value.split("$");
    if (parts.length >= 3) {
        const typeName = parts[0]; // e.g. "mvccpb.KeyValue" or "KvSchema"
        const encoding = parts[1];
        const encodedData = parts.slice(2).join("$");
        if (typeName === "mvccpb.KeyValue") {
            return (0, protobuf_1.fromBinary)(connectrpc_etcd_1.KeyValueSchema, new Uint8Array(Buffer.from(encodedData, encoding)));
        }
        if (required) {
            throw new Error(`deserialize: unknown type ${typeName}`);
        }
        return undefined;
    }
    // Fallback: treat the string as a UTF‑8 encoded Uint8Array.
    return new Uint8Array(Buffer.from(value, "utf8"));
}
