import dotenv from "dotenv";
import dotenvExpand from "dotenv-expand";
dotenvExpand.expand(dotenv.config());

// import { Etcd3 } from "etcd3";
import { unmarshallOptions } from "@aws-sdk/lib-dynamodb";
import { Logger, P } from "pino";
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

type ContextOpts = {
  logger?: Logger;
  table?: string;
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
  abort(reason?: AbortReason, ctx?: AbortContext): void;
}

class Context
  extends EventEmitter<{
    abort: [{ reason: string | Error; ctx?: AbortContext; code: number }];
  }>
  implements Context
{
  static default?: Context = new Context();

  private readonly logger?: Logger;
  private readonly _abort: AbortController;

  private _kvStorage: Observable<DynamoDBImpl<"pk", "sk">>;
  private _revisionStorage: Observable<DynamoDBImpl<"pk", "sk">>;
  public readonly leaseStorage: Observable<DynamoDBImpl<"pk", "sk">>;
  public readonly historyStorage: Observable<DynamoDBImpl<"pk", "sk">>;

  // TODO: Introduce a PersistentCounter Subject
  private revisions?: RevisionTable;

  constructor(opts?: ContextOpts) {
    super({ captureRejections: true });
    this._abort = opts?.abort || new AbortController();
    this.logger = opts?.logger || undefined;

    this._kvStorage = DynamoDB.from("kv", {
      hashKey: "pk",
      rangeKey: "sk",
    });

    this._revisionStorage = DynamoDB.from("rev", {
      hashKey: "pk",
      rangeKey: "sk",
    });

    this.leaseStorage = DynamoDB.from("lease", {
      hashKey: "pk",
      rangeKey: "sk",
    });

    this.historyStorage = DynamoDB.from("his", {
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

  get kvStorage(): Promise<DynamoDBImpl<"pk", "sk">> {
    return firstValueFrom(this._kvStorage);
  }

  get revisionStorage(): Promise<DynamoDBImpl<"pk", "sk">> {
    return firstValueFrom(this._revisionStorage);
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

  async version(): Promise<void> {
    console.log(`${name} v${version}`);
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

  async certfile(): Promise<Buffer> {
    return readFileSync(
      process.env.CERTDIR && process.env.CERTFILE
        ? join(process.env.CERTDIR, process.env.CERTFILE)
        : `src/certs/localhost.crt`
    );
  }

  async keyfile(): Promise<Buffer> {
    return readFileSync(
      process.env.CERTDIR && process.env.KEYFILE
        ? join(process.env.CERTDIR, process.env.KEYFILE)
        : `src/certs/localhost.key`
    );
  }
}

export const context = Context.default!;
export default Context;
