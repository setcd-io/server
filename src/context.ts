import dotenv from "dotenv";
import dotenvExpand from "dotenv-expand";
dotenvExpand.expand(dotenv.config());

// import { Etcd3 } from "etcd3";
import { unmarshallOptions } from "@aws-sdk/lib-dynamodb";
import { Logger } from "pino";
import EventEmitter from "events";
import { BaseSchema, RevisionTable } from "./storage/base";
import { ConnectError } from "@connectrpc/connect";
import { TenantHistory } from "./storage/kv";
import { deserialize, serialize } from "./storage/serde";
import { name, version } from "../package.json";
import _ from "lodash";
import { join } from "path";
import { readFileSync } from "fs";
import { DynamoDB } from "cloudrx";
import { firstValueFrom, Observable } from "rxjs";
import { DynamoDBImpl } from "cloudrx/dist/providers/aws/provider";
import yargs from "yargs";
import { hideBin } from "yargs/helpers";
import parse from "parse-duration";

type ContextOpts = {
  logger?: Logger;
  abort?: AbortController;
};

export type AbortReason = string | Error | Record<string, unknown>;
export type AbortContext = Record<string, unknown>;

export class RevisionError extends ConnectError {
  constructor(message: string) {
    super(message);
  }
}

interface Context extends AbortController {
  minRevision(tenant: string, minRevision?: number): Promise<number>;
  currentRevision(tenant: string): Promise<number>;
  nextRevision(tenant: string): Promise<number>;
  nextLease(tenant: string): Promise<number>;
  nextWatch(tenant: string): Promise<number>;
  abort(reason?: AbortReason, ctx?: AbortContext): void;
  repr(): Promise<string>;
}

class Environment {
  private _argv = yargs(hideBin(process.argv))
    .command("help", "Show help information", {}, () => {
      yargs.showHelp();
      process.exit(0);
    })
    .option("name", {
      type: "string",
      description: "The name of the cluster",
      default: "setcd",
    })
    .option("cert-file", {
      type: "string",
      description: "Path to the certificate file",
      default: "./certs/localhost.crt",
    })
    .alias("cert-file", "certfile")
    .option("key-file", {
      type: "string",
      description: "Path to the private key file",
      default: "./certs/localhost.key",
    })
    .alias("key-file", "keyfile")
    .option("http2", {
      type: "boolean",
      description: "Enable HTTP/2 support (use --no-http2 to disable)",
      default: true,
    })
    .option("watch-progress-notify-interval", {
      type: "string",
      description: "Duration of periodical watch progress notification.",
      default: "10m",
    })
    .alias(
      "watch-progress-notify-interval",
      "experimental-watch-progress-notify-interval"
    )
    .env()
    .version()
    .help()
    .alias("help", "h")
    .parseSync();

  constructor() {}

  get name(): string {
    return this._argv.name;
  }

  get certfile(): string {
    return this._argv["cert-file"];
  }

  get keyfile(): string {
    return this._argv["key-file"];
  }

  get isHttp2(): boolean {
    return this._argv.http2;
  }

  get watchProgressNotifyInterval(): number {
    return parse(this._argv["watch-progress-notify-interval"], "ms") as number;
  }
}

class Context
  extends EventEmitter<{
    abort: [{ reason: string | Error; ctx?: AbortContext; code: number }];
  }>
  implements Context
{
  static default?: Context = new Context();

  private readonly _abort: AbortController;
  public readonly env: Environment;

  private _kvStorage: Observable<DynamoDBImpl<"pk", "sk">>;
  private _revisionStorage: Observable<DynamoDBImpl<"pk", "sk">>;
  private _leaseStorage: Observable<DynamoDBImpl<"pk", "sk">>;
  private _historyStorage: Observable<DynamoDBImpl<"pk", "sk">>;
  private revisions?: RevisionTable;

  protected constructor(opts?: ContextOpts) {
    super({ captureRejections: true });

    this.env = new Environment();
    this._abort = opts?.abort || new AbortController();

    this._kvStorage = DynamoDB.from("kv", {
      namespace: this.env.name,
      hashKey: "pk",
      rangeKey: "sk",
    });

    this._revisionStorage = DynamoDB.from("rev", {
      namespace: this.env.name,
      hashKey: "pk",
      rangeKey: "sk",
    });

    this._leaseStorage = DynamoDB.from("lease", {
      namespace: this.env.name,
      hashKey: "pk",
      rangeKey: "sk",
    });

    this._historyStorage = DynamoDB.from("his", {
      namespace: this.env.name,
      hashKey: "pk",
      rangeKey: "sk",
    });

    this.on("abort", ({ reason, ctx }) => {
      let context: string | AbortContext | undefined = ctx;

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
      } else {
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

  get namespace(): string {
    return this.env.name;
  }

  get kvStorage(): Promise<DynamoDBImpl<"pk", "sk">> {
    return firstValueFrom(this._kvStorage);
  }

  get revisionStorage(): Promise<DynamoDBImpl<"pk", "sk">> {
    return firstValueFrom(this._revisionStorage);
  }

  get leaseStorage(): Promise<DynamoDBImpl<"pk", "sk">> {
    return firstValueFrom(this._leaseStorage);
  }

  get historyStorage(): Promise<DynamoDBImpl<"pk", "sk">> {
    return firstValueFrom(this._historyStorage);
  }

  async minRevision(tenant: string, minRevision?: number): Promise<number> {
    if (!this.revisions) {
      this.revisions = new RevisionTable(await this.revisionStorage);
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
    } else {
      const { Item: item } = await this.revisions
        .get(pk, sk)
        .exec({ ConsistentRead: true });

      if (!item || !item.minRevision) {
        return 0;
      }

      return item.minRevision;
    }
  }

  async currentRevision(tenant: string): Promise<number> {
    if (!this.revisions) {
      this.revisions = new RevisionTable(await this.revisionStorage);
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

  async nextRevision(tenant: string): Promise<number> {
    if (!this.revisions) {
      this.revisions = new RevisionTable(await this.revisionStorage);
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

  async nextLease(tenant: string): Promise<number> {
    if (!this.revisions) {
      this.revisions = new RevisionTable(await this.revisionStorage);
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

  async nextWatch(tenant: string): Promise<number> {
    if (!this.revisions) {
      this.revisions = new RevisionTable(await this.revisionStorage);
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

  get unmarshalOptions(): unmarshallOptions {
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

  get aborted(): boolean {
    return this._abort.signal.aborted;
  }

  abort(reason?: AbortReason, ctx?: AbortContext, code = 1) {
    if (!this) {
      // In case this is called from a static context
      Context.default!.abort(reason, ctx);
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
      if (
        reason.name === "AggregateError" &&
        "errors" in reason &&
        reason.errors &&
        Array.isArray(reason.errors)
      ) {
        return this.emit("abort", {
          reason: reason.errors.map((err) => err.message).join(", "),
          ctx,
          code,
        });
      } else {
        return this.emit("abort", { reason: reason.message, ctx, code });
      }
    }

    if (typeof reason === "string") {
      return this.emit("abort", { reason, ctx, code });
    }

    const abort: { reason?: Error; ctx?: AbortContext } = Object.entries(
      reason || {}
    ).reduce(
      (acc, [key, value]) => {
        if (value instanceof Error) {
          acc.reason = value;
        }

        acc.ctx[key] = value;
        return acc;
      },
      { ctx: ctx || {} } as {
        reason?: Error;
        ctx: AbortContext;
      }
    );

    this.emit("abort", {
      reason: abort.reason || "Unknown Reason",
      ctx: abort.ctx,
      code,
    });
  }

  certfile(): Buffer {
    return readFileSync(this.env.certfile);
  }

  keyfile(): Buffer {
    return readFileSync(this.env.keyfile);
  }

  async repr(): Promise<string> {
    const lines = [
      `${name} v${version}:`,
      `→ Runtime:`,
      `  ╟ Node.js: ${process.version}`,
      `  ╟ Platform: ${process.platform} ${process.arch} ${process.release.name}`,
      `  ╟ PID: ${process.pid}`,
      `  ╙ Command: ${process.argv.join(" ")}`,
      `→ Environment:`,
      `  ╟ Cert File: ${this.env.certfile}`,
      `  ╟ Key File: ${this.env.keyfile}`,
      `  ╙ HTTP/2: ${this.env.isHttp2}`,
      `→ Storage:`,
      `  ╟ KV: ${(await this.kvStorage).tableArn}`,
      `  ╟ Revision: ${(await this.revisionStorage).tableArn}`,
      `  ╟ Lease: ${(await this.leaseStorage).tableArn}`,
      `  ╙ History: ${(await this.historyStorage).tableArn}`,
    ];

    return lines.join("\n");
  }
}

export const context = Context.default!;
export default Context;
