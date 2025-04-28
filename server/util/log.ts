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

const stringify = (
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

export const log = async (
  req: UnaryRequest | StreamRequest,
  next: (
    req: UnaryRequest | StreamRequest
  ) => Promise<UnaryResponse | StreamResponse>
): Promise<UnaryResponse | StreamResponse> => {
  const id = req.contextValues?.get(CONNECTION_ID);
  const tenant = req.contextValues?.get(TENANT);

  const _num = (name: string, value: number | bigint | string): string => {
    return chalk.dim(`[${name}:${chalk.blue(value)}]`);
  };

  const _log = (
    incoming?: Message<string>,
    outgoing?: Message<string> | ConnectError
  ) => {
    if (!incoming || !outgoing) {
      return;
    }

    const { message: request } = stringify(incoming);
    const { message: response, revision } = stringify(outgoing);

    let emoji = chalk.green(`✔︎`);

    let prefix = tenant;
    if (tenant === DEFAULT_TENANT) {
      emoji = chalk.yellow(`▲`);
      prefix = `${chalk.yellow(prefix)}`;
    } else {
      prefix = `${chalk.cyan(prefix)}`;
    }

    prefix = `[${prefix}] ${_num("con", id)} ${chalk.green(
      req.method.name
    )}: ${chalk.yellow(request)}`;

    let suffix = "";
    if (response) {
      if (outgoing instanceof ConnectError) {
        suffix = chalk.red(response);
        emoji = chalk.red(`✘`);
      } else {
        suffix = chalk.green(response);
      }
    } else {
      suffix = chalk.dim(chalk.green("(empty)"));
    }

    if (revision) {
      suffix = `${suffix} ${_num("rev", revision)}`;
    }

    console.log(`${emoji} ${prefix} ${chalk.dim(`==>`)} ${suffix}`);
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
