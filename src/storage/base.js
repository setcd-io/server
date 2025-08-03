"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.RevisionTable = exports.TenantTable = exports.all = exports.preventOverwrite = exports.KEY_SEPARATOR = void 0;
const ddb_table_1 = require("ddb-table");
const ulid_1 = require("ulid");
const util_dynamodb_1 = require("@aws-sdk/util-dynamodb");
const rxjs_1 = require("rxjs");
const serde_1 = require("./serde");
exports.KEY_SEPARATOR = "$";
const preventOverwrite = () => ({
    ConditionExpression: "attribute_not_exists(pk) AND attribute_not_exists(sk)",
});
exports.preventOverwrite = preventOverwrite;
const all = async (query, filterFn) => {
    const next = async (items = [], startKey) => {
        if (startKey === null) {
            return items;
        }
        if (startKey) {
            query = query.startKey(startKey);
        }
        const results = await query.exec({ ConsistentRead: true });
        const lastEvaluatedKey = results.LastEvaluatedKey;
        return next([
            ...items,
            ...(results.Items || []).filter((i) => (filterFn ? filterFn(i) : true)),
        ], lastEvaluatedKey || null);
    };
    return next();
};
exports.all = all;
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
class BaseTable extends ddb_table_1.Table {
    constructor(provider, pkPrefix, skPrefix) {
        super({
            documentClient: provider.client,
            tableName: provider.tableName,
            primaryKey: provider.hashKey,
            sortKey: provider.rangeKey,
        });
        this.pkPrefix = pkPrefix;
        this.skPrefix = skPrefix;
    }
    _pk(value = "") {
        const prefix = `${exports.KEY_SEPARATOR}${this.pkPrefix}${exports.KEY_SEPARATOR}`;
        return `${prefix}${value}`;
    }
    _sk(value = "") {
        const prefix = `${exports.KEY_SEPARATOR}${this.skPrefix}${exports.KEY_SEPARATOR}`;
        return `${prefix}${value}`;
    }
    isRecord(item) {
        if (!item) {
            return false;
        }
        if ("dynamodb" in item) {
            const { dynamodb = {} } = item;
            const { Keys = {} } = dynamodb;
            return this.isRecord((0, util_dynamodb_1.unmarshall)(Keys));
        }
        if ("pk" in item && "sk" in item) {
            return (((item || {}).pk || "").startsWith(this._pk()) &&
                ((item || {}).sk || "").startsWith(this._sk()));
        }
        return false;
    }
}
class TenantTable {
    constructor(ctx, skPrefix) {
        this.ctx = ctx;
        this.skPrefix = skPrefix;
        this.tables = {};
    }
    async table(tenant) {
        if (!tenant) {
            throw new Error("Tenant is required");
        }
        if (typeof tenant === "object") {
            const { pk } = tenant;
            if (!pk) {
                throw new Error(`Unable to extract tenant: ${JSON.stringify(tenant)}`);
            }
            tenant = pk.split(exports.KEY_SEPARATOR)[1];
            if (!tenant) {
                throw new Error(`Unable to parse tenant: ${JSON.stringify(tenant)}`);
            }
        }
        if (this.tables[tenant]) {
            return this.tables[tenant];
        }
        const internal = new BaseTable(await this.ctx.kvStorage, "tenant", this.skPrefix);
        internal.pk = () => internal._pk(tenant);
        internal.sk = (value) => {
            let val;
            if (value instanceof Uint8Array) {
                val = (0, serde_1.serialize)(value, "utf8", true);
            }
            else {
                val = `${value}`;
            }
            return internal._sk(val);
        };
        const update = internal.update.bind(internal);
        internal.rawUpdate = internal.update.bind(internal);
        internal.update = (pk, sk) => update(pk, sk).set("tenant", tenant).set("serial", (0, ulid_1.ulid)());
        this.tables[tenant] = internal;
        return internal;
    }
    mapRecord() {
        return (source) => {
            return new rxjs_1.Observable((subscriber) => {
                const subscription = source
                    .pipe((0, rxjs_1.map)((record) => {
                    const { Keys = {} } = record.dynamodb || {};
                    const keys = (0, util_dynamodb_1.unmarshall)(Keys, this.ctx.unmarshalOptions);
                    return { keys, record };
                }), (0, rxjs_1.switchMap)(({ keys, record }) => {
                    return (0, rxjs_1.from)(this.table(keys)).pipe((0, rxjs_1.map)((table) => ({ table, keys, record })));
                }), (0, rxjs_1.filter)(({ table, record }) => {
                    const { dynamodb = {} } = record;
                    return !!dynamodb?.SequenceNumber && table.isRecord(record);
                }))
                    .pipe((0, rxjs_1.map)(({ keys, record }) => ({
                    keys: keys,
                    eventName: record.eventName,
                    dynamodb: record.dynamodb,
                })), (0, rxjs_1.map)(({ keys, eventName, dynamodb = {} }) => {
                    const { OldImage, NewImage, SequenceNumber: sequence } = dynamodb;
                    const previous = OldImage
                        ? (0, util_dynamodb_1.unmarshall)(OldImage, this.ctx.unmarshalOptions)
                        : undefined;
                    const current = NewImage
                        ? (0, util_dynamodb_1.unmarshall)(NewImage, this.ctx.unmarshalOptions)
                        : undefined;
                    return {
                        keys,
                        operation: (eventName === "REMOVE" ? "DELETE" : "PUT"),
                        sequence,
                        previous,
                        current,
                    };
                }))
                    .pipe((0, rxjs_1.map)(({ keys, operation, sequence, current, previous }) => {
                    return {
                        keys,
                        operation: operation,
                        sequence: sequence,
                        item: operation === "DELETE" ? previous : current,
                        previous: operation === "DELETE" ? undefined : previous,
                    };
                }), (0, rxjs_1.filter)(({ keys, sequence }) => !!keys && !!sequence))
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
exports.TenantTable = TenantTable;
class RevisionTable extends BaseTable {
    constructor(provider) {
        super(provider, "tenant", "revision");
    }
}
exports.RevisionTable = RevisionTable;
