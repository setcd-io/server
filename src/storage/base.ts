import { QueryQuery, Table, UpdateQuery } from "ddb-table";
import { AttributeValue, DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { ulid } from "ulid";
import { TableKey } from "ddb-table/lib/TableIndex";
import { DynamoDBRecord } from "aws-lambda";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import {
  filter,
  from,
  map,
  Observable,
  OperatorFunction,
  switchMap,
} from "rxjs";
import Context from "../context";
import { serialize } from "./serde";
import { Item } from "ddb-table/lib/DocumentClient";
import { DynamoDB } from "cloudrx";
import { DynamoDBImpl } from "cloudrx/dist/providers/aws/provider";

export const KEY_SEPARATOR = "$";

export type Keys = {
  pk: string;
  sk: string;
};

export type BaseSchema = Keys & {
  expires?: number;
};

export type KeyPrefix = string;

export type ConditionExpression = { ConditionExpression: string };

export const preventOverwrite = (): ConditionExpression => ({
  ConditionExpression: "attribute_not_exists(pk) AND attribute_not_exists(sk)",
});

export type StreamRecord<T> = {
  keys: Keys;
  item?: Partial<T>;
  previous?: Partial<T>;
  sequence: string;
  operation: "PUT" | "DELETE";
};

export const all = async <T extends K, K extends Item>(
  query: QueryQuery<T, K>,
  filterFn?: (item: T) => boolean
): Promise<T[]> => {
  const next = async (items: T[] = [], startKey?: K | null): Promise<T[]> => {
    if (startKey === null) {
      return items;
    }

    if (startKey) {
      query = query.startKey(startKey);
    }

    const results = await query.exec({ ConsistentRead: true });
    const lastEvaluatedKey = results.LastEvaluatedKey;

    return next(
      [
        ...items,
        ...(results.Items || []).filter((i) => (filterFn ? filterFn(i) : true)),
      ],
      lastEvaluatedKey || null
    );
  };

  return next();
};

// export const paginate = async <T extends K, K extends Item>(
//   query: QueryQuery<T, K>,
//   limit?: number
// ): Promise<{ items: T[]; more: boolean }> => {
//   const hardLimit = query.serialize().Limit;

//   const next = async (
//     items: T[] = [],
//     startKey?: K
//   ): Promise<{ items: T[]; more: boolean }> => {
//     if (startKey) {
//       query = query.startKey(startKey);
//     }

//     if (!!hardLimit) {
//       const remaining = hardLimit - items.length;
//       if (remaining <= 0) {
//         return { items: items.slice(0, hardLimit), more: false };
//       }
//       query = query.limit(remaining);
//     }

//     const results = await query.exec({ ConsistentRead: true });

//     let lastEvaluatedKey = results.LastEvaluatedKey;
//     const lastItem = last(results.Items);

//     items.push(...(results.Items || []));

//     if (!lastEvaluatedKey) {
//       return {
//         items: hardLimit !== undefined ? items.slice(0, hardLimit) : items,
//         more: false,
//       };
//     }

//     if (!!hardLimit && items.length >= hardLimit) {
//       return { items: items.slice(0, hardLimit), more: true };
//     }

//     return next(items, lastEvaluatedKey);
//   };

//   return next();
// };

class BaseTable<
  T extends BaseSchema,
  PkPrefix extends KeyPrefix,
  SkPrefix extends KeyPrefix
> extends Table<T, "pk", "sk"> {
  constructor(
    provider: DynamoDBImpl<"pk", "sk">,
    private pkPrefix: PkPrefix,
    private skPrefix: SkPrefix
  ) {
    super({
      documentClient: provider.client,
      tableName: provider.tableName,
      primaryKey: provider.hashKey,
      sortKey: provider.rangeKey,
    });
  }

  public _pk(value: string = ""): string {
    const prefix = `${KEY_SEPARATOR}${this.pkPrefix}${KEY_SEPARATOR}`;
    return `${prefix}${value}`;
  }

  public _sk(value: string = ""): string {
    const prefix = `${KEY_SEPARATOR}${this.skPrefix}${KEY_SEPARATOR}`;
    return `${prefix}${value}`;
  }

  public isRecord(item: Partial<T> | DynamoDBRecord | undefined): item is T {
    if (!item) {
      return false;
    }
    if ("dynamodb" in item) {
      const { dynamodb = {} } = item;
      const { Keys = {} } = dynamodb;
      return this.isRecord(unmarshall(Keys as Record<string, AttributeValue>));
    }
    if ("pk" in item && "sk" in item) {
      return (
        ((item || {}).pk || "").startsWith(this._pk()) &&
        ((item || {}).sk || "").startsWith(this._sk())
      );
    }
    return false;
  }
}

type TenantIdentifiable<T extends TableKey<TenantSchema, "pk", "sk">> = {
  pk: () => string;
  sk: (value: string | Uint8Array | number) => string;
  rawUpdate: (
    pk: string,
    sk: string
  ) => UpdateQuery<T, TableKey<T, "pk", "sk">>;
};

export type TenantSchema = BaseSchema & {
  tenant: string;
  serial: string;
};

export abstract class TenantTable<
  T extends TenantSchema,
  SkPrefix extends KeyPrefix
> {
  private tables: { [tenant: string]: BaseTable<T, "tenant", SkPrefix> } = {};

  constructor(protected ctx: Context, private skPrefix: SkPrefix) {}

  async table(
    tenant?: string | Partial<Keys>
  ): Promise<BaseTable<T, "tenant", SkPrefix> & TenantIdentifiable<T>> {
    if (!tenant) {
      throw new Error("Tenant is required");
    }

    if (typeof tenant === "object") {
      const { pk } = tenant;
      if (!pk) {
        throw new Error(`Unable to extract tenant: ${JSON.stringify(tenant)}`);
      }
      tenant = pk.split(KEY_SEPARATOR)[1];
      if (!tenant) {
        throw new Error(`Unable to parse tenant: ${JSON.stringify(tenant)}`);
      }
    }

    if (this.tables[tenant]) {
      return this.tables[tenant] as BaseTable<T, "tenant", SkPrefix> &
        TenantIdentifiable<T>;
    }

    const internal = new BaseTable<T, "tenant", SkPrefix>(
      await this.ctx.kvStorage,
      "tenant",
      this.skPrefix
    ) as BaseTable<T, "tenant", SkPrefix> & TenantIdentifiable<T>;

    internal.pk = () => internal._pk(tenant);
    internal.sk = (value: string | Uint8Array | number) => {
      let val: string;
      if (value instanceof Uint8Array) {
        val = serialize(value, "utf8", true);
      } else {
        val = `${value}`;
      }
      return internal._sk(val);
    };

    const update = internal.update.bind(internal);
    internal.rawUpdate = internal.update.bind(internal);
    internal.update = (pk: string, sk: string) =>
      update(pk, sk).set("tenant", tenant).set("serial", ulid());

    this.tables[tenant] = internal;
    return internal;
  }

  mapRecord(): OperatorFunction<DynamoDBRecord, StreamRecord<T>> {
    return (
      source: Observable<DynamoDBRecord>
    ): Observable<StreamRecord<T>> => {
      return new Observable<{
        operation: "PUT" | "DELETE";
        sequence: string;
        keys: Keys;
        item?: Partial<T>;
        previous?: Partial<T>;
      }>((subscriber) => {
        const subscription = source
          .pipe(
            map((record) => {
              const { Keys = {} } = record.dynamodb || {};
              const keys = unmarshall(
                Keys as Record<string, AttributeValue>,
                this.ctx.unmarshalOptions
              ) as Partial<Keys>;

              return { keys, record };
            }),
            switchMap(({ keys, record }) => {
              return from(this.table(keys)).pipe(
                map((table) => ({ table, keys, record }))
              );
            }),
            filter(({ table, record }) => {
              const { dynamodb = {} } = record;

              return !!dynamodb?.SequenceNumber && table.isRecord(record);
            })
          )
          .pipe(
            map(({ keys, record }) => ({
              keys: keys as Keys,
              eventName: record.eventName,
              dynamodb: record.dynamodb,
            })),
            map(({ keys, eventName, dynamodb = {} }) => {
              const { OldImage, NewImage, SequenceNumber: sequence } = dynamodb;
              const previous = OldImage
                ? (unmarshall(
                    OldImage as Record<string, AttributeValue>,
                    this.ctx.unmarshalOptions
                  ) as Partial<T>)
                : undefined;
              const current = NewImage
                ? (unmarshall(
                    NewImage as Record<string, AttributeValue>,
                    this.ctx.unmarshalOptions
                  ) as Partial<T>)
                : undefined;
              return {
                keys,
                operation: (eventName === "REMOVE" ? "DELETE" : "PUT") as
                  | "PUT"
                  | "DELETE",
                sequence,
                previous,
                current,
              };
            })
          )
          .pipe(
            map(({ keys, operation, sequence, current, previous }) => {
              return {
                keys,
                operation: operation,
                sequence: sequence!,
                item: operation === "DELETE" ? previous : current,
                previous: operation === "DELETE" ? undefined : previous,
              };
            }),
            filter(({ keys, sequence }) => !!keys && !!sequence)
          )
          .subscribe({
            next(response) {
              subscriber.next(response);
            },
            error(err) {
              subscriber.error(err);
            },
            complete() {
              subscriber.complete();
            },
          });

        return () => {
          subscription.unsubscribe();
        };
      });
    };
  }
}

export type RevisionSchema = BaseSchema & {
  tenant: string;
  revision: number;
  minRevision?: number;
  lease?: number;
  watch?: number;
};

export class RevisionTable extends BaseTable<
  RevisionSchema,
  "tenant",
  "revision"
> {
  constructor(provider: DynamoDBImpl<"pk", "sk">) {
    super(provider, "tenant", "revision");
  }
}
