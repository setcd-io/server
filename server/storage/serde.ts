import { fromBinary, toBinary } from "@bufbuild/protobuf";
import { KeyValue, KeyValueSchema } from "@setcd-io/connectrpc-etcd";

export function serialize<T extends Uint8Array | KeyValue>(
  value: T,
  encoding: "utf8" | "base64",
  required: true
): string;

export function serialize<T extends Uint8Array | KeyValue>(
  value: T | undefined,
  encoding: "utf8" | "base64",
  required?: false
): string | undefined;

export function serialize<T extends Uint8Array | KeyValue>(
  value: T | undefined,
  encoding: "utf8" | "base64",
  required: boolean = false
): string | undefined {
  if (value === undefined) {
    if (!required) return undefined;
    throw new Error("serialize: value is required");
  }

  // If the value is a Message (has a $typeName property)
  if (typeof value === "object" && !(value instanceof Uint8Array)) {
    let typeName: string | undefined = undefined;
    let encoded: string | undefined = undefined;
    if ("$typeName" in value && value.$typeName === "mvccpb.KeyValue") {
      typeName = value.$typeName;
      encoded = Buffer.from(toBinary(KeyValueSchema, value)).toString(encoding);
    } else {
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

  if (!required) return undefined;
  throw new Error("serialize: invalid value provided");
}

export function deserialize<T extends Uint8Array | KeyValue>(
  value: string,
  required: true
): T;

export function deserialize<T extends Uint8Array | KeyValue>(
  value: string | undefined,
  required?: false
): T | undefined;

export function deserialize<T extends Uint8Array | KeyValue>(
  value: string | undefined,
  required: boolean = false
): T | undefined {
  if (typeof value !== "string") {
    if (!required) return undefined;
    throw new Error("deserialize: value is required");
  }

  // Check for a Uint8Array encoded in base64.
  if (value.startsWith("base64$")) {
    const data = value.slice("base64$".length);
    return new Uint8Array(Buffer.from(data, "base64")) as unknown as T;
  }

  // If the string appears to be a serialized Message:
  const parts = value.split("$");
  if (parts.length >= 3) {
    const typeName = parts[0]; // e.g. "mvccpb.KeyValue" or "KvSchema"
    const encoding = parts[1] as "utf8" | "base64";
    const encodedData = parts.slice(2).join("$");

    if (typeName === "mvccpb.KeyValue") {
      return fromBinary(
        KeyValueSchema,
        new Uint8Array(Buffer.from(encodedData, encoding))
      ) as T;
    }

    if (required) {
      throw new Error(`deserialize: unknown type ${typeName}`);
    }

    return undefined;
  }

  // Fallback: treat the string as a UTF‑8 encoded Uint8Array.
  return new Uint8Array(Buffer.from(value, "utf8")) as unknown as T;
}
