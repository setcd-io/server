import {
  CompactionRequest,
  CompactionResponse,
  Compare_CompareResult,
  Compare_CompareTarget,
  DeleteRangeRequest,
  DeleteRangeResponse,
  PutRequest,
  PutResponse,
  RangeRequest,
  RangeResponse,
  TxnRequest,
  TxnResponse,
} from "@setcd-io/connectrpc-etcd";
import { TenantKVTable } from "../storage/kv";
import { BaseHandler } from "./base";
import Context from "../context";
import { deserialize, serialize } from "../storage/serde";
import {
  concatAll,
  concatMap,
  filter,
  from,
  map,
  NEVER,
  Subject,
  switchMap,
  tap,
} from "rxjs";
import { DynamoDBRecord, DynamoDBStreamEvent } from "aws-lambda";
import { FastifyReply, FastifyRequest } from "fastify";
import { ErrGRPCCompacted, ErrGRPCKeyNotFound } from "../util/error";
import chalk from "chalk";
import { KeyValue } from "@setcd-io/connectrpc-etcd";
import { LeaseHandler } from "./lease";
import { log } from "../util/log";

export class KVHandler extends BaseHandler {
  private records = new Subject<DynamoDBRecord>();
  kv: TenantKVTable;

  constructor(ctx: Context, leaseHandler: LeaseHandler) {
    super(ctx);
    this.kv = new TenantKVTable(ctx, leaseHandler);

    ctx.signal.addEventListener("abort", () => {
      // leases.unsubscribe();
      this.records.complete();
    });
  }

  // TODO: Replace this by putting an HTTP server in CloudRx
  public dynamodbHandler() {
    return async (req: FastifyRequest, reply: FastifyReply) => {
      if (req.host !== "dynamodb.amazonaws.com") {
        reply.status(401).send("Unauthorized");
        return;
      }
      const { Records } = req.body as DynamoDBStreamEvent;
      Records.forEach((record) => {
        this.records.next(record);
      });
      reply.status(200).send();
    };
  }

  async put(
    tenant: string,
    req: PutRequest
  ): Promise<PutResponse & { kv?: KeyValue }> {
    const revision = await this.ctx.nextRevision(tenant);

    const { current, previous } = await this.kv.putKey(
      tenant,
      req.key,
      req.value,
      revision,
      Number(req.lease)
    );

    const kv: KeyValue = {
      $typeName: "mvccpb.KeyValue",
      key: deserialize(current.key, true),
      value: deserialize(current.value, true),
      createRevision: BigInt(current.createRevision),
      modRevision: BigInt(current.modRevision),
      version: BigInt(current.version),
      lease: BigInt(current.lease),
    };

    const prevKv: KeyValue | undefined = previous
      ? {
          $typeName: "mvccpb.KeyValue",
          key: deserialize(previous.key, true),
          value: deserialize(previous.value, true),
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

  async range(
    tenant: string,
    req: Partial<RangeRequest>
  ): Promise<RangeResponse> {
    if (req.revision && req.revision > 0n) {
      const minRevision = await this.ctx.minRevision(tenant);
      if (req.revision < BigInt(minRevision)) {
        throw new ErrGRPCCompacted();
      }
    }

    if (req.minModRevision && req.minModRevision > 0n) {
      const minRevision = await this.ctx.minRevision(tenant);
      if (req.minModRevision < BigInt(minRevision)) {
        throw new ErrGRPCCompacted();
      }
    }

    const { count, kvs, more } = await this.kv.range(tenant, req, {
      // A specific revision was requested, include expired keys so the handler below can filter them
      includeExpired: req.revision !== 0n,
      handler: async (kv) => {
        const { revision } = req;
        if (!revision || revision === 0n) return kv;

        if (BigInt(kv.modRevision) > revision) {
          const snapshot = await this.kv.atRevision(tenant, kv.key, revision);

          if (snapshot) {
            return {
              ...kv,
              value: serialize(snapshot.current.value, "base64", true),
              createRevision: Number(snapshot.current.createRevision),
              modRevision: Number(snapshot.current.modRevision),
              version: Number(snapshot.current.version),
              lease: Number(snapshot.current.lease),
            };
          }
        }

        // If the version is 0, the key was deleted, exclude it from the results
        if (kv.version === 0) {
          return undefined;
        }

        // The key is valid at the requested revision, return it
        if (BigInt(kv.modRevision) <= revision) {
          return kv;
        }

        // There is no valid key at the requested revision, return undefined
        return undefined;
      },
    });

    return {
      $typeName: "etcdserverpb.RangeResponse",
      count: BigInt(count),
      kvs: kvs.map((kv) => ({
        $typeName: "mvccpb.KeyValue",
        key: deserialize(kv.key, true),
        value: deserialize(kv.value, true),
        createRevision: BigInt(kv.createRevision),
        modRevision: BigInt(kv.modRevision),
        version: BigInt(kv.version),
        lease: BigInt(kv.lease),
      })),
      more,
      header: await this.header(tenant),
    };
  }

  async deleteRange(
    tenant: string,
    req: DeleteRangeRequest
  ): Promise<DeleteRangeResponse> {
    const revision = await this.ctx.currentRevision(tenant);

    const deleted = await this.kv.range(
      tenant,
      {
        key: req.key,
        rangeEnd: req.rangeEnd,
        maxModRevision: BigInt(revision),
      },
      {
        handler: (kv) =>
          this.kv
            .deleteKey(tenant, deserialize(kv.key, true), kv.modRevision)
            .then(() => kv),
      }
    );

    return {
      $typeName: "etcdserverpb.DeleteRangeResponse",
      header: await this.header(tenant),
      deleted: BigInt(deleted.count),
      prevKvs: req.prevKv
        ? deleted.kvs.map((kv) => {
            return {
              $typeName: "mvccpb.KeyValue",
              key: deserialize(kv.key, true),
              value: deserialize(kv.value, true),
              createRevision: BigInt(kv.createRevision),
              modRevision: BigInt(kv.modRevision),
              version: BigInt(kv.version),
              lease: BigInt(kv.lease),
            };
          })
        : [],
    };
  }

  async compact(
    tenant: string,
    req: CompactionRequest
  ): Promise<CompactionResponse> {
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

    log("Compacted", {
      level: "success",
      tenant,
      action: "Compact",
      context: {
        revision: Number(revision),
      },
    });

    return {
      $typeName: "etcdserverpb.CompactionResponse",
      header: await this.header(tenant),
    };
  }

  async transact(tenant: string, req: TxnRequest): Promise<TxnResponse> {
    const revision = await this.ctx.currentRevision(tenant);

    const { success } = await req.compare.reduce(async (accP, c) => {
      return accP.then((acc) => {
        if (!acc.success) {
          return acc;
        }

        const { key, rangeEnd, target, result, targetUnion } = c;

        const rangeReq: Partial<RangeRequest> = {
          $typeName: "etcdserverpb.RangeRequest",
          key,
          rangeEnd,
          maxModRevision: BigInt(revision),
        };

        if (
          result === Compare_CompareResult.EQUAL &&
          (targetUnion.case === "createRevision" ||
            targetUnion.case === "modRevision")
        ) {
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
                const actual =
                  target === Compare_CompareTarget.VERSION
                    ? BigInt(kv.version)
                    : target === Compare_CompareTarget.CREATE
                    ? BigInt(kv.createRevision)
                    : target === Compare_CompareTarget.MOD
                    ? BigInt(kv.modRevision)
                    : target === Compare_CompareTarget.VALUE
                    ? deserialize<Uint8Array>(kv.value)
                    : target === Compare_CompareTarget.LEASE
                    ? BigInt(kv.lease)
                    : undefined;

                if (typeof actual === "bigint" && typeof desired === "bigint") {
                  return result === Compare_CompareResult.EQUAL
                    ? actual === desired
                    : Compare_CompareResult.NOT_EQUAL
                    ? actual !== desired
                    : Compare_CompareResult.GREATER
                    ? actual > desired
                    : Compare_CompareResult.LESS
                    ? actual < desired
                    : false;
                }

                if (
                  actual instanceof Uint8Array &&
                  desired instanceof Uint8Array
                ) {
                  const a = serialize(actual, "utf8", true);
                  const d = serialize(desired, "utf8", true);
                  return result === Compare_CompareResult.EQUAL
                    ? a.localeCompare(d) === 0
                    : Compare_CompareResult.NOT_EQUAL
                    ? a.localeCompare(d) !== 0
                    : Compare_CompareResult.GREATER
                    ? a.localeCompare(d) > 0
                    : Compare_CompareResult.LESS
                    ? a.localeCompare(d) < 0
                    : false;
                }

                return false;
              }),
            };
          })
          .catch((e) => {
            log("KV Transaction Compare Error", {
              level: "error",
              tenant,
              action: "Txn",
              output: e.message,
            });
            return { success: false };
          });
      });
    }, Promise.resolve({ success: true } as { success: boolean }));

    const response = await (success ? req.success : req.failure).reduce(
      (accP, { request }) => {
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
                log("KV Transaction Put Error", {
                  level: "error",
                  tenant,
                  action: "Txn",
                  output: e.message,
                });
                acc.succeeded = false;
                return acc;
              });
          } else if (request.case === "requestRange") {
            return this.range(tenant, request.value)
              .then((r) => {
                acc.responses.push({
                  $typeName: "etcdserverpb.ResponseOp",
                  response: { case: "responseRange", value: r },
                });
                return acc;
              })
              .catch((e) => {
                log("KV Transaction Range Error", {
                  level: "error",
                  tenant,
                  action: "Txn",
                  output: e.message,
                });
                acc.succeeded = false;
                return acc;
              });
          } else if (request.case === "requestDeleteRange") {
            return this.deleteRange(tenant, request.value)
              .then((r) => {
                acc.responses.push({
                  $typeName: "etcdserverpb.ResponseOp",
                  response: { case: "responseDeleteRange", value: r },
                });
                return acc;
              })
              .catch((e) => {
                log("KV Transaction DeleteRange Error", {
                  level: "error",
                  tenant,
                  action: "Txn",
                  output: e.message,
                });
                acc.succeeded = false;
                return acc;
              });
          } else if (request.case === "requestTxn") {
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
                log("KV Transaction Error", {
                  level: "error",
                  tenant,
                  action: "Txn",
                  output: e.message,
                });
                acc.succeeded = false;
                return acc;
              });
          }
          return acc;
        });
        return chain;
      },
      Promise.resolve({
        $typeName: "etcdserverpb.TxnResponse",
        succeeded: true,
        responses: [],
      } as TxnResponse)
    );

    response.header = await this.header(tenant);
    response.succeeded = response.succeeded && success;
    return response;
  }
}
