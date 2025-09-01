import { all, Keys, TenantSchema, TenantTable } from "./base";
import { ConnectError } from "@connectrpc/connect";
import {
  LeaseStatus,
  RangeRequest,
  RangeRequest_SortOrder,
} from "@setcd-io/connectrpc-etcd";
import Context from "../context";
import { deserialize, serialize } from "./serde";
import {
  asyncScheduler,
  bufferTime,
  combineLatest,
  concatAll,
  concatMap,
  filter,
  firstValueFrom,
  from,
  lastValueFrom,
  map,
  Observable,
  observeOn,
  queueScheduler,
  share,
  shareReplay,
  Subscription,
  switchMap,
  takeUntil,
  tap,
  timer,
  toArray,
  windowTime,
} from "rxjs";
import { QueryCommandInput } from "@aws-sdk/lib-dynamodb";
import _ from "lodash";
import { ErrGRPCEmptyKey } from "../util/error";
import chalk from "chalk";
import { KeyValue } from "@setcd-io/connectrpc-etcd";
import { CloudReplaySubject } from "cloudrx";
import { log } from "../util/log";
import { LeaseHandler } from "../handlers/lease";

export const _INTERNAL_LEASE_ID__LEASES = -1;
export const _INTERNAL = {
  LEASE_ID: _INTERNAL_LEASE_ID__LEASES,
};

export class NotFoundError extends ConnectError {
  constructor() {
    super("Not Found");
  }
}

export type KVSchema = TenantSchema & {
  key: string;
  value: string;
  createRevision: number;
  modRevision: number;
  version: number;
  lease: number;
};

const intoKv = (item: KVSchema): KeyValue => {
  return {
    $typeName: "mvccpb.KeyValue",
    key: deserialize(item.key, true),
    value: deserialize(item.value, true),
    createRevision: BigInt(item.createRevision),
    modRevision: BigInt(item.modRevision),
    version: BigInt(item.version),
    lease: BigInt(item.lease),
  };
};

const lastChar = (str: string | undefined): number | undefined => {
  if (!str || str.length === 0) {
    return undefined;
  }
  return str.charCodeAt(str.length - 1);
};

const HISTORY_TIMEOUT = 1000;
const HISTORY_SIZE = HISTORY_TIMEOUT / 10;

export type TenantHistory<T> = {
  tenant: string;
  action: "PUT" | "DELETE";
  key: string;
  current: T;
  previous?: T;
};

export class TenantKVTable extends TenantTable<KVSchema, "kv"> {
  private history: CloudReplaySubject<TenantHistory<KeyValue>>;

  constructor(ctx: Context, private leaseHandler: LeaseHandler) {
    super(ctx, "kv");

    this.history = new CloudReplaySubject<TenantHistory<KeyValue>>(
      ctx.historyStorage
    );
    this.history.on("expired", (h) => {
      log("Expired", {
        level: "info",
        tenant: h.tenant,
        action: "KeyValue",
        output: h.current,
        context: {
          action: h.action,
        },
      });
      asyncScheduler.schedule(() =>
        this.deleteKey(h.tenant, h.current.key, Number(h.current.modRevision))
      );
      // queueMicrotask(() =>
      //   this.deleteKey(h.tenant, h.current.key, Number(h.current.modRevision))
      // );
    });

    ctx.on("abort", () => {
      // expiration.unsubscribe();
    });
  }

  public history$(tenant: string): Observable<TenantHistory<KeyValue>[]> {
    return from(this.ctx.minRevision(tenant)).pipe(
      switchMap((minRevision) =>
        this.history.pipe(
          filter(
            (h) => h.tenant === tenant && h.current.modRevision >= minRevision
          )
        )
      ),
      tap((h) => {
        log(h.previous, {
          level: "success",
          action: "KeyValueHistory",
          tenant: h.tenant,
          output: h.current,
          context: {
            action: h.action,
          },
        });
      }),
      // bufferTime(HISTORY_TIMEOUT, undefined, HISTORY_SIZE),
      map((h) => [h]),
      share()
    );
  }

  async putKey(
    tenant: string,
    key: Uint8Array,
    value: Uint8Array,
    revision: number,
    lease: number,
    opts?: {
      expires?: number;
    }
  ): Promise<{ current: KVSchema; previous?: KVSchema }> {
    const table = await this.table(tenant);

    let current: KVSchema = {
      pk: table.pk(),
      sk: table.sk(key),
      tenant,
      key: serialize(key, "utf8", true),
      value: serialize(value, "base64", true),
      createRevision: revision,
      modRevision: revision,
      version: 1,
      lease: lease,
      expires: opts?.expires,
      serial: "", // Calculated
    };

    if (lease > 0) {
      const relativeLease = await this.leaseHandler.getLease(tenant, lease);
      current.expires = relativeLease.expires;
      current.lease = Number(relativeLease.ID);
    }

    try {
      let update = table
        .update(current.pk, current.sk)
        .set("value", current.value)
        .set("modRevision", current.modRevision)
        .add("version", current.version)
        .condition((c) =>
          c
            .attributeExists("key")
            .and((c) => c.eq("tenant", current.tenant))
            .and((c) => c.eq("lease", current.lease))
            .and((c) => c.eq("key", current.key))
            .and((c) => c.gte("version", current.version))
        );

      if (current.expires) {
        update = update.set("expires", current.expires);
      } else {
        update = update.remove("expires");
      }

      const { Attributes } = await update.exec({ ReturnValues: "ALL_OLD" });
      if (Attributes) {
        const previous = Attributes;
        current.createRevision = previous.createRevision;
        current.version = previous.version + 1;
        current.expires = previous.expires;
        this.history.next(
          {
            tenant,
            action: "PUT",
            key: serialize(key, "utf8", true),
            current: intoKv(current),
            previous: intoKv(previous),
          },
          current.expires
        );
        return { current, previous };
      }
    } catch (e) {
      if (e.name !== "ConditionalCheckFailedException") {
        throw e;
      }

      let insert = table
        .update(current.pk, current.sk)
        .set("tenant", current.tenant)
        .set("lease", current.lease)
        .set("key", current.key)
        .set("value", current.value)
        .set("createRevision", current.createRevision)
        .set("modRevision", current.modRevision)
        .set("version", 1);

      if (current.expires) {
        insert = insert.set("expires", current.expires);
      } else {
        insert = insert.remove("expires");
      }

      const { Attributes } = await insert.exec({ ReturnValues: "ALL_NEW" });
      if (!Attributes) {
        throw new ConnectError("Failed to put key: missing attributes");
      }

      current = Attributes;

      this.history.next(
        {
          tenant,
          action: "PUT",
          key: serialize(key, "utf8", true),
          current: intoKv(current),
        },
        current.expires
      );

      return {
        current,
      };
    }

    throw new ConnectError("Failed to put key");
  }

  async deleteKey(
    tenant: string,
    key: Uint8Array,
    revision: number
  ): Promise<KVSchema | undefined> {
    const table = await this.table(tenant);

    const value = serialize(new Uint8Array(0), "base64", true);
    const modRevision = await this.ctx.nextRevision(tenant);
    const version = 0;
    const lease = 0;
    const expires = Math.floor(Date.now() / 1000);

    const query = table
      .update(table.pk(), table.sk(key))
      .set("value", serialize(new Uint8Array(0), "base64", true))
      .set("modRevision", modRevision)
      .set("version", version)
      .set("lease", lease)
      .set("expires", expires)
      .condition((c) => c.lte("modRevision", revision));

    try {
      const { Attributes: previous } = await query.exec({
        ReturnValues: "ALL_OLD",
      });

      if (!previous) {
        log("Key not found", {
          level: "warn",
          tenant,
          action: "Delete",
          context: {
            pk: table.pk(),
            sk: table.sk(key),
          },
          output: "Key not found",
        });
        return undefined;
      }

      const current: KVSchema = {
        ...previous,
        value,
        modRevision,
        version,
        lease,
        expires,
      };

      this.history.next({
        tenant,
        action: "DELETE",
        key: serialize(key, "utf8", true),
        current: intoKv(current),
        previous: intoKv(previous),
      });

      return current;
    } catch (e) {
      if (e instanceof Error && e.name === "ConditionalCheckFailedException") {
        return undefined;
      }
      log("Unable to delete key", {
        level: "error",
        tenant,
        action: "Delete",
        context: {
          pk: table.pk(),
          sk: table.sk(key),
        },
        output: e.message,
      });
      throw e;
    }
  }

  async range(
    tenant: string,
    rangeRequest: Partial<RangeRequest>,
    opts?: {
      includeExpired?: boolean;
      leaseId?: number;
      handler?: (kv: KVSchema) => Promise<KVSchema | undefined>;
    }
  ): Promise<{
    count: number;
    kvs: KVSchema[];
    more: boolean;
    _q: QueryCommandInput;
  }> {
    const table = await this.table(tenant);
    const { key, rangeEnd } = rangeRequest;

    if (!key || !rangeEnd) {
      throw new ErrGRPCEmptyKey();
    }

    const key$ = serialize(key, "utf8", true);
    const rangeEnd$ = serialize(rangeEnd, "utf8", true);

    // Track if we're using the between operator
    // - DDB is inclusive on rangeEnd, we need to filter to make it exclusive
    let between = false;

    let query = table
      .query()
      .keyCondition((c) => c.eq("pk", table.pk()))
      .keyCondition((c) => {
        /*
        Ruleset from rangeEnd:
         - 1) range_end is the upper bound on the requested range [key, range_end). 
         - 2) If range_end is '\0', the range is all keys >= key. 
         - 3) If range_end is key plus one (e.g., "aa"+1 == "ab", "a\xff"+1 == "b"), then the range request gets all keys prefixed with key. 
         - 4) If both key and range_end are '\0', then the range request returns all keys.
         - 5) [default] if rangeEnd is empty or '\0' then we do an exact match on key
        */

        // Impl: 5) [default] if rangeEnd === [] then we do an exact match on key
        if (!!key$ && !rangeEnd$) {
          return c.eq("sk", table.sk(key$));
        }

        // Impl: 4) If both key and range_end are '\0', then the range request returns all keys.
        if (
          key.length == 1 &&
          rangeEnd.length === 1 &&
          key.at(0) === 0 &&
          rangeEnd.at(0) === 0
        ) {
          return c.beginsWith("sk", table.sk(""));
        }

        // Impl: 3) If range_end is key plus one (e.g., "aa"+1 == "ab", "a\xff"+1 == "b"), then the range request gets all keys prefixed with key.
        if (key$ && rangeEnd$ && lastChar(rangeEnd$) === lastChar(key$)! + 1) {
          return c.beginsWith("sk", table.sk(key$));
        }

        // Impl: 2) If range_end is '\0', the range is all keys >= key.
        if (rangeEnd.length === 1 || rangeEnd.at(0) === 0) {
          return c.gte("sk", table.sk(key$));
        }

        // Impl: 1) range_end is the upper bound on the requested range [key, range_end).
        between = true;
        return c.between("sk", table.sk(key$), table.sk(rangeEnd$));
      })
      .filter((f) => {
        // Impl 1) range_end is the upper bound on the requested range [key, range_end).
        if (between) {
          // Filter out the inclusive rangeEnd on "sk"
          return f.lt("key", rangeEnd$);
        }
        return f;
      })
      .filter((f) => {
        if (opts?.leaseId === _INTERNAL.LEASE_ID) {
          return f;
        }
        if (opts?.includeExpired) {
          return f;
        }
        return f
          .attributeNotExists("expires")
          .or((f) => f.gte("expires", Math.floor(Date.now() / 1000)));
      })
      .filter((f) => {
        if (opts?.includeExpired) {
          return f;
        }
        return f.gt("version", 0);
      })
      .filter((f) => {
        if (!rangeRequest.minModRevision) {
          return f;
        }
        return f.gte("modRevision", Number(rangeRequest.minModRevision));
      })
      .filter((f) => {
        if (!rangeRequest.maxModRevision) {
          return f;
        }
        return f.lte("modRevision", Number(rangeRequest.maxModRevision));
      })
      .filter((f) => {
        if (!rangeRequest.minCreateRevision) {
          return f;
        }
        return f.gte("createRevision", Number(rangeRequest.minCreateRevision));
      })
      .filter((f) => {
        if (!rangeRequest.maxCreateRevision) {
          return f;
        }
        return f.lte("createRevision", Number(rangeRequest.maxCreateRevision));
      });

    if (opts && !!opts.leaseId && opts.leaseId !== 0) {
      query = query.filter((f) => f.eq("lease", Number(opts.leaseId)));
    } else {
      // Internal KVs are tracked in negative space
      query = query.filter((f) => f.gte("lease", 0));
    }

    if (rangeRequest.sortOrder === RangeRequest_SortOrder.DESCEND) {
      query = query.reverseIndex();
    }

    const _q = query.serialize();

    const leases =
      opts?.leaseId !== _INTERNAL.LEASE_ID
        ? await this.leaseHandler.all(tenant)
        : [];

    let items = await all(query, (i) => {
      return (
        i.lease <= 0 || leases.some((l) => BigInt(l.ID) === BigInt(i.lease))
      );
    });

    if (opts?.handler) {
      items = (
        await Promise.all(items.map((item) => opts.handler!(item)))
      ).filter((item) => !!item);
    }

    const kvs = rangeRequest.limit
      ? items.slice(0, Number(rangeRequest.limit))
      : items;

    return {
      count: items.length,
      kvs,
      more: !!rangeRequest.limit && kvs.length !== items.length,
      _q,
    };
  }

  async leased(tenant: string, leaseId: number): Promise<KVSchema[]> {
    const table = await this.table(tenant);
    const { Items: items } = await table
      .query()
      .keyCondition((c) =>
        c.eq("pk", table.pk()).and((c) => c.beginsWith("sk", table.sk("")))
      )
      .filter((f) => f.eq("lease", leaseId))
      .filter((f) => f.gt("version", 0))
      .exec({ ConsistentRead: true });

    return items || [];
  }

  async all(tenant: string, key?: Uint8Array | string): Promise<KVSchema[]> {
    const table = await this.table(tenant);
    const { Items: items } = await table
      .query()
      .keyCondition((c) =>
        c.eq("pk", table.pk()).and((c) => {
          if (!key) {
            return c.beginsWith("sk", table.sk(""));
          }
          if (key instanceof Uint8Array) {
            return c.eq("sk", table.sk(serialize(key, "utf8", true)));
          } else {
            return c.eq("sk", table.sk(key));
          }
        })
      )
      .exec({ ConsistentRead: true });

    return items || [];
  }

  async atRevision(
    tenant: string,
    key: Uint8Array | string,
    revision: number | bigint
  ): Promise<TenantHistory<KeyValue> | undefined> {
    if (key instanceof Uint8Array) {
      key = serialize(key, "utf8", true);
    }

    const all = await lastValueFrom(this.history.snapshot({ tenant, key }));

    const history = all
      .filter((h) => BigInt(h.current.modRevision) <= BigInt(revision))
      .sort((a, b) => {
        // Sort by modRevision descending
        return Number(
          BigInt(b.current.modRevision) - BigInt(a.current.modRevision)
        );
      });

    return history[0];
  }
}
