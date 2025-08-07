import { Message } from "@bufbuild/protobuf";
import {
  ConnectError,
  StreamRequest,
  StreamResponse,
  UnaryRequest,
  UnaryResponse,
} from "@connectrpc/connect";
import chalk from "chalk";
import { CONNECTION_ID, DEFAULT_TENANT, TENANT } from "./const";

export const stringify = (
  message: Message<string> | ConnectError
): { revision?: number; message?: string } => {
  if (message instanceof ConnectError) {
    return { message: message.message };
  }

  let revision: number | undefined = undefined;

  const replacer = (key: string, value: any) => {
    if (key === "$typeName") {
      return undefined;
    }
    if (
      key === "header" &&
      value &&
      "$typeName" in value &&
      value.$typeName === "etcdserverpb.ResponseHeader" &&
      "revision" in value
    ) {
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

function peek(
  message: Message<string> | AsyncIterable<Message<string>>,
  callback: (message: Message<string>) => void
): void {
  if ("$typeName" in message) {
    callback(message);
    return;
  }

  const wrapped = (next: any) => {
    return async (...args: [] | [undefined]) => {
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
          ? (...args: [] | [undefined]) => {
              return returnFn(...args);
            }
          : undefined,
        throw: throwFn
          ? (...args: [] | [undefined]) => {
              return throwFn(...args);
            }
          : undefined,
      };
    };
  }
}

type Level = "info" | "error" | "warn" | "success";
type ContextValue = string | number | bigint | boolean;
type Context = Record<string, ContextValue | ContextValue[] | undefined>;
type Options = {
  level: Level;
  tenant: string;
  action?: string;
  output?: string | Message<string> | ConnectError;
  context?: Context;
};

export const log = (
  message?: string | Message<string>,
  {
    level = "info",
    tenant = DEFAULT_TENANT,
    action,
    output,
    context,
  }: Options = { level: "info", tenant: DEFAULT_TENANT }
): void => {
  const verbose = process.env.SETCD_VERBOSE === "true";
  if (!verbose && level !== "error") {
    return;
  }

  const severityColor =
    level === "error"
      ? chalk.red
      : level === "warn"
      ? chalk.yellow
      : level === "info"
      ? chalk.blue
      : chalk.green;

  const symbol =
    level === "error"
      ? severityColor("✘")
      : level === "warn"
      ? severityColor("▲")
      : level === "info"
      ? severityColor("ℹ︎")
      : severityColor("✔︎");

  tenant =
    tenant === DEFAULT_TENANT ? chalk.yellow(tenant) : chalk.cyan(tenant);

  action = chalk.green(action);

  if (message && typeof message !== "string") {
    message = stringify(message).message;
  }

  if (output && typeof output !== "string") {
    const { message, revision } = stringify(output);
    output = message;
    if (revision) {
      context = {
        ...context,
        rev: revision,
      };
    }
  }

  let io = chalk.yellow(message ? message : chalk.dim("[empty]"));
  if (output) {
    io = `${io} ${chalk.dim("==>")} ${severityColor(output)}`;
  }

  context = Object.entries(context || {}).reduce((acc, [key, value]) => {
    acc[key] = chalk.dim(`[${key}:${chalk.blue(value)}] `);
    return acc;
  }, {} as Record<string, string>);

  console.log(
    `${symbol} [${tenant}] ${action}: ${io} ${Object.values(context).join("")}`
  );
};

export const logRequest = async (
  req: UnaryRequest | StreamRequest,
  next: (
    req: UnaryRequest | StreamRequest
  ) => Promise<UnaryResponse | StreamResponse>
): Promise<UnaryResponse | StreamResponse> => {
  const id = req.contextValues?.get(CONNECTION_ID);
  const tenant = req.contextValues?.get(TENANT);

  const _log = (
    incoming?: Message<string>,
    outgoing?: Message<string> | ConnectError
  ) => {
    if (!incoming || !outgoing) {
      return;
    }

    const context: Context = {
      con: id,
    };

    const { message: input } = stringify(incoming);
    const { message: output, revision } = stringify(outgoing);

    if (revision) {
      context["rev"] = revision;
    }

    let level: Level = outgoing instanceof ConnectError ? "error" : "success";

    log(input, {
      level,
      tenant,
      action: req.method.name,
      output,
      context,
    });
  };

  return next(req)
    .then((res) => {
      let incoming: Message<string> | undefined;
      peek(req.message, (msg) => (incoming = msg));
      peek(res.message, (msg) => _log(incoming, msg));
      return res;
    })
    .catch((err) => {
      if (err instanceof ConnectError) {
        peek(req.message, (incoming) => _log(incoming, err));
      }
      throw err;
    });
};
