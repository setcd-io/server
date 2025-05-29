import {
  CreateTableCommand,
  DescribeTableCommand,
  DescribeTimeToLiveCommand,
  DynamoDBClient,
  TableDescription,
  TimeToLiveDescription,
  UpdateTimeToLiveCommand,
} from "@aws-sdk/client-dynamodb";
import {
  DescribeStreamCommand,
  DynamoDBStreamsClient,
  GetRecordsCommand,
  GetShardIteratorCommand,
} from "@aws-sdk/client-dynamodb-streams";
import { DynamoDBDocument } from "@aws-sdk/lib-dynamodb";
import {
  asyncScheduler,
  BehaviorSubject,
  catchError,
  concatAll,
  defer,
  EMPTY,
  filter,
  forkJoin,
  from,
  fromEvent,
  interval,
  lastValueFrom,
  map,
  Observable,
  of,
  ReplaySubject,
  startWith,
  Subject,
  switchMap,
  take,
  takeUntil,
  tap,
  throwError,
  timer,
} from "rxjs";
import {
  Consistency,
  Provider,
  Serializer,
  Stored,
  StoredKey,
  StoredPartition,
} from "../provider";
import { FatalError, RetryError } from "./errors";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import chalk from "chalk";
import { Shard, Shards } from "./shards";
import { observe, tail } from "../util";

export type DynamoDbOptions<T> = {
  hashKey: string;
  rangeKey: string;
  signal: AbortSignal;
  consistency?: Consistency;
  serializers?: Serializer<T>;
  region?: string;
};

export class DynamoDbProvider<T> extends Provider<T> {
  override async init(tableName: string): Promise<this> {
    this._id = tableName;
    return lastValueFrom(
      this.table.pipe(
        map(({ tableArn, streamArn }) => {
          this._tableArn = tableArn;
          this._streamArn = streamArn;
          if (this.consistency === "strong") {
            console.log(
              chalk.blue(
                `${chalk.yellow(
                  "NOTE:"
                )} Enabling shard observation for table ${chalk.bold(
                  this.tableName
                )}`
              )
            );
            this.shards.next(new Shards(this, streamArn, this.signal));
          }
          return this;
        })
      )
    );
  }

  override async put(item: Stored): Promise<Stored> {
    return this.documentClient
      .put({
        Item: item,
        TableName: this.tableName,
      })
      .then(() => item);
  }

  override async get(key: StoredKey): Promise<Stored> {
    return this.documentClient
      .get({
        Key: { partition: key.partition, timeflake: key.timeflake },
        TableName: this.tableName,
        ConsistentRead: this.consistency !== "none",
      })
      .then((res) => {
        return res.Item as Stored;
      });
  }

  override observe(): Observable<Stored> {
    return this.shards.pipe(switchMap((shards) => shards.records$));
  }

  override repr(): string {
    return `CloudRxDdb{ table=${this.tableName}, hash=${this.opts.hashKey}, range=${this.opts.rangeKey} }`;
  }

  public readonly client: DynamoDBClient;
  public readonly documentClient: DynamoDBDocument;
  public readonly streamClient: DynamoDBStreamsClient;

  private _tableArn?: string;
  private _streamArn?: string;

  private shards: ReplaySubject<Shards<T>> = new ReplaySubject(1);

  constructor(private opts: DynamoDbOptions<T>) {
    super(opts.signal, opts.consistency || "weak", opts.serializers);
    this.client = new DynamoDBClient({
      region: this.opts.region,
      // logger: console,
    });
    this.documentClient = DynamoDBDocument.from(this.client);
    this.streamClient = new DynamoDBStreamsClient({
      region: this.opts.region,
      // logger: console,
    });
  }

  get tableName(): string {
    const tableName = process.env.AWS_DYNAMODB_TABLE_ETCD__NAME;
    if (!tableName) {
      throw new FatalError(
        "AWS_DYNAMODB_TABLE_ETCD__NAME enviornment variable is not set"
      );
    }
    return `${tableName}-${this.id}`;
  }

  get tableArn(): string {
    if (!this._tableArn) {
      throw new RetryError(
        "Table ARN is not available yet. Please call init() first."
      );
    }
    return this._tableArn;
  }

  get streamArn(): string {
    if (!this._streamArn) {
      throw new RetryError(
        "Stream ARN is not available yet. Please call init() first."
      );
    }
    return this._streamArn;
  }

  private get table(): Observable<{
    tableArn: string;
    streamArn: string;
  }> {
    const describe$ = defer(() =>
      forkJoin([
        this.client.send(
          new DescribeTableCommand({ TableName: this.tableName }),
          {
            abortSignal: this.signal,
          }
        ),
        this.client.send(
          new DescribeTimeToLiveCommand({ TableName: this.tableName }),
          {
            abortSignal: this.signal,
          }
        ),
      ]).pipe(
        map(([table, ttl]) => ({
          table: table.Table,
          ttl: ttl.TimeToLiveDescription,
        }))
      )
    );

    const create$ = defer(() =>
      forkJoin([
        this.client.send(
          new CreateTableCommand({
            TableName: this.tableName,
            KeySchema: [
              { AttributeName: this.opts.hashKey, KeyType: "HASH" },
              { AttributeName: this.opts.rangeKey, KeyType: "RANGE" },
            ],
            AttributeDefinitions: [
              { AttributeName: this.opts.hashKey, AttributeType: "S" },
              { AttributeName: this.opts.rangeKey, AttributeType: "S" },
            ],
            BillingMode: "PAY_PER_REQUEST",
            StreamSpecification: {
              StreamEnabled: true,
              StreamViewType: "NEW_AND_OLD_IMAGES",
            },
          }),
          {
            abortSignal: this.signal,
          }
        ),
        this.client.send(
          new UpdateTimeToLiveCommand({
            TableName: this.tableName,
            TimeToLiveSpecification: {
              AttributeName: "expires",
              Enabled: true,
            },
          }),
          {
            abortSignal: this.signal,
          }
        ),
      ]).pipe(
        map(([table, ttl]) => ({
          table: table.TableDescription,
          ttl: ttl.TimeToLiveSpecification,
        }))
      )
    );

    const assert = (
      table?: TableDescription,
      ttl?: TimeToLiveDescription,
      error?: any
    ): {
      table: TableDescription;
      ttl: TimeToLiveDescription;
    } => {
      if (this.signal.aborted) {
        throw new FatalError("Aborted");
      }

      if (error) {
        if (error instanceof FatalError || error instanceof RetryError) {
          throw error;
        }

        console.warn(chalk.yellow(`WARN: ${error.name}: ${error.message}`));

        if (error.code === "ECONNREFUSED") {
          throw new RetryError("Connection refused");
        }
        if (error.name === "ResourceNotFoundException") {
          throw new RetryError("Resource not found");
        }
        if (error.name === "ResourceInUseException") {
          throw new RetryError("Resource in use");
        }
        if (error.name === "ValidationException") {
          throw new RetryError("Validation error");
        }

        throw new FatalError(`${error.name}: ${error.message}`);
      }

      if (!table || !ttl) {
        throw new FatalError("Table or TTL is not yet available");
      }

      if (table.TableStatus !== "ACTIVE") {
        throw new RetryError("Table is not yet active");
      }

      if (ttl.TimeToLiveStatus !== "ENABLED") {
        throw new RetryError("TTL is not yet enabled");
      }

      if (
        table.KeySchema?.find(
          (key) =>
            key.KeyType === "HASH" && key.AttributeName !== this.opts.hashKey
        )
      ) {
        throw new FatalError(
          `Hash key does not match desired name of \`${
            this.opts.hashKey
          }\`: ${JSON.stringify(table.KeySchema)}`
        );
      }
      if (
        table.KeySchema?.find(
          (key) =>
            key.KeyType === "RANGE" && key.AttributeName !== this.opts.rangeKey
        )
      ) {
        throw new FatalError(
          `Range key does not match desired name of \`${
            this.opts.rangeKey
          }\`: ${JSON.stringify(table.KeySchema)}`
        );
      }

      if (
        table.AttributeDefinitions?.find(
          (key) =>
            key.AttributeName === this.opts.hashKey && key.AttributeType !== "S"
        )
      ) {
        throw new FatalError(
          `Hash Key needs to be a string type: ${JSON.stringify(
            table.AttributeDefinitions
          )}`
        );
      }

      if (
        table.AttributeDefinitions?.find(
          (key) =>
            key.AttributeName === this.opts.rangeKey &&
            key.AttributeType !== "S"
        )
      ) {
        throw new FatalError(
          `Range Key needs to be a string type: ${JSON.stringify(
            table.AttributeDefinitions
          )}`
        );
      }

      if (
        table.AttributeDefinitions?.find(
          (key) => key.AttributeName === "expires" && key.AttributeType !== "N"
        )
      ) {
        throw new FatalError(
          `Table neesd a TTL attribute named "expires" of type number`
        );
      }

      if (table.StreamSpecification?.StreamEnabled !== true) {
        throw new FatalError(`Table needs to have streams enabled`);
      }

      if (table.StreamSpecification?.StreamViewType !== "NEW_AND_OLD_IMAGES") {
        throw new FatalError(
          `Table needs to have streams configured to emit new and old images`
        );
      }

      if (ttl?.AttributeName !== "expires") {
        throw new FatalError(`TTL attribute needs to be named expires`);
      }

      return { table, ttl };
    };

    const check = (
      delay: number
    ): Observable<{
      table: TableDescription;
      ttl: TimeToLiveDescription;
    }> =>
      timer(delay, asyncScheduler).pipe(
        takeUntil(fromEvent(this.signal, "abort")),
        switchMap(() =>
          describe$.pipe(switchMap(({ table, ttl }) => of(assert(table, ttl))))
        ),
        catchError((err) => {
          if (err instanceof FatalError) {
            return throwError(() => err);
          }
          return create$.pipe(
            switchMap(({ table, ttl }) => of(assert(table, ttl)))
          );
        }),
        catchError((err) => {
          if (err instanceof FatalError) {
            return throwError(() => err);
          }
          return of(assert(undefined, undefined, err));
        }),
        catchError((err) => {
          if (err instanceof RetryError) {
            return check(1000);
          }
          return throwError(() => err);
        })
      );

    return check(0).pipe(
      map(({ table }) => ({
        tableArn: table.TableArn!,
        streamArn: table.LatestStreamArn!,
      }))
    );
  }
}
