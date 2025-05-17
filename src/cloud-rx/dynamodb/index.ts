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
  catchError,
  concatAll,
  concatMap,
  defer,
  distinct,
  expand,
  firstValueFrom,
  forkJoin,
  from,
  fromEvent,
  interval,
  lastValueFrom,
  map,
  Observable,
  of,
  startWith,
  switchMap,
  takeUntil,
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

export type DynamoDbOptions<T> = {
  hashKey: string;
  rangeKey: string;
  consistency: Consistency;
  serializers: Serializer<T>;
  signal: AbortSignal;
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
        Key: key,
        TableName: this.tableName,
        ConsistentRead: this.consistency === "weak",
      })
      .then((res) => res.Item as Stored);
  }

  override async all(
    partition: StoredPartition,
    startKey?: StoredKey
  ): Promise<Stored[]> {
    return this.documentClient
      .query({
        TableName: this.tableName,
        KeyConditionExpression:
          "#partition = :partition AND begins_with(#timeflake, :sk)",
        ExpressionAttributeNames: {
          "#partition": this.opts.hashKey,
          "#timeflake": this.opts.rangeKey,
        },
        ExpressionAttributeValues: {
          ":partition": partition.partition,
          ":sk": "",
        },
        ExclusiveStartKey: startKey,
        ConsistentRead: this.consistency !== "none",
      })
      .then(async (res) => {
        const items = (res.Items || []) as Stored[];
        const next = await this.all(
          partition,
          res.LastEvaluatedKey as StoredKey
        );
        return [...items, ...next];
      });
  }

  override async oldest(
    partition: StoredPartition
  ): Promise<Stored | undefined> {
    return firstValueFrom(
      this.shard$()
        .pipe(switchMap((part) => this.iterator$(part, "TRIM_HORIZON")))
        .pipe(switchMap((it) => this.page$(it)))
        .pipe(
          map(({ items }) =>
            items.filter((i) => i.partition === partition.partition)
          )
        )
        .pipe(concatAll())
    );
  }

  override stream(): Observable<Stored> {
    return this.shard$().pipe(
      concatMap((shard) => this.iterator$(shard, "LATEST")),
      concatMap((it) =>
        this.page$(it).pipe(
          expand(({ next }) =>
            next
              ? this.page$(next)
              : timer(50, asyncScheduler).pipe(concatMap(() => this.page$(it)))
          )
        )
      ),
      concatMap(({ items }) => from(items || []))
    );
  }

  override repr(): string {
    return `CloudRxDdb{ table=${this.tableName}, hash=${this.opts.hashKey}, range=${this.opts.rangeKey} }`;
  }

  public readonly client: DynamoDBClient;
  public readonly documentClient: DynamoDBDocument;
  public readonly streamClient: DynamoDBStreamsClient;

  private _tableArn?: string;
  private _streamArn?: string;

  constructor(private opts: DynamoDbOptions<T>) {
    super(opts.consistency, opts.serializers, opts.signal);
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

  private shard$(): Observable<{ streamArn: string; shardId: string }> {
    return this.table.pipe(
      switchMap(({ streamArn }) =>
        interval(1000).pipe(
          startWith(0),
          switchMap(() =>
            from(
              this.streamClient.send(
                new DescribeStreamCommand({ StreamArn: streamArn })
              )
            )
          ),
          map((res) => res.StreamDescription?.Shards || []),
          concatAll(),
          map((shard) => shard.ShardId!),
          distinct((shardId) => shardId),
          map((shardId) => ({ streamArn, shardId }))
        )
      )
    );
  }

  private iterator$(
    shard: {
      streamArn: string;
      shardId?: string;
    },
    boundary: "TRIM_HORIZON" | "LATEST"
  ): Observable<string | undefined> {
    return defer(() =>
      from(
        this.streamClient.send(
          new GetShardIteratorCommand({
            ShardId: shard.shardId,
            StreamArn: shard.streamArn,
            ShardIteratorType: boundary,
          })
        )
      ).pipe(map((res) => res.ShardIterator))
    );
  }

  private page$(iterator?: string): Observable<{
    next?: string;
    items: Stored[];
  }> {
    return defer(() =>
      from(
        this.streamClient.send(
          new GetRecordsCommand({ ShardIterator: iterator })
        )
      ).pipe(
        map(({ NextShardIterator, Records = [] }) => ({
          next: NextShardIterator,
          items: Records.filter((r) => r.eventName !== "REMOVE")
            .map((r) => r.dynamodb?.NewImage)
            .filter((i) => !!i)
            .map((i) => unmarshall(i) as Stored),
        })),
        map(({ next, items }) => ({
          next: items.length ? next : undefined,
          items,
        }))
      )
    );
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
