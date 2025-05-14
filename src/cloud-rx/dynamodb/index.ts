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
  Shard,
} from "@aws-sdk/client-dynamodb-streams";
import { DynamoDBDocument } from "@aws-sdk/lib-dynamodb";
import {
  asyncScheduler,
  catchError,
  defer,
  EMPTY,
  expand,
  forkJoin,
  from,
  fromEvent,
  map,
  mergeAll,
  mergeMap,
  Observable,
  of,
  switchMap,
  takeUntil,
  throwError,
  timer,
} from "rxjs";
import { Consistency, Provider, Serializer, Stored } from "../provider";
import { FatalError, RetryError } from "./errors";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import chalk from "chalk";

export type Options<T> = {
  signal: AbortSignal;
  serializer: Serializer<T>;
  hashKey: string;
  rangeKey: string;
  region?: string;
};

export class DynamoDbProvider<T> extends Provider<T> {
  public readonly client: DynamoDBClient;
  public readonly documentClient: DynamoDBDocument;
  public readonly streamClient: DynamoDBStreamsClient;
  private _tableArn?: string;
  private _streamArn?: string;

  constructor(tableName: string, private opts: Options<T>) {
    super(tableName, opts.serializer, opts.signal);
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

  init(): Observable<this> {
    return this.table.pipe(
      map(({ tableArn, streamArn }) => {
        this._tableArn = tableArn;
        this._streamArn = streamArn;
        return this;
      })
    );
  }

  get tableName(): string {
    return `${this.id}`;
  }

  protected async put(item: Stored): Promise<Stored> {
    return this.documentClient
      .put({
        Item: item,
        TableName: this.tableName,
      })
      .then(() => item);
  }

  protected async get(
    flake: string,
    consistency: Consistency
  ): Promise<Stored> {
    return this.documentClient
      .get({
        Key: { id: this.id, flake },
        TableName: this.tableName,
        ConsistentRead: consistency !== "none",
      })
      .then((res) => res.Item as Stored);
  }

  public latest(): Observable<Stored> {
    throw new Error("Latest not implemented for DynamoDbProvider");
  }

  public all(): Observable<Stored> {
    return this.partition$().pipe(
      mergeMap((part) => this.iterator$(part, "TRIM_HORIZON")),
      mergeMap((it) =>
        this.page$(it).pipe(
          expand(({ next }) => (next ? this.page$(next) : EMPTY))
        )
      ),
      mergeMap(({ items }) => from(items || []))
    );
  }

  private partition$(): Observable<{ streamArn: string; shardId?: string }> {
    return this.table.pipe(
      switchMap(({ streamArn }) =>
        defer(() =>
          from(
            this.streamClient.send(
              new DescribeStreamCommand({ StreamArn: streamArn })
            )
          )
        ).pipe(
          map((res) => res.StreamDescription?.Shards || []),
          mergeAll(),
          map((shard) => ({
            shardId: shard.ShardId,
            streamArn,
          }))
        )
      )
    );
  }

  private iterator$(
    partition: {
      streamArn: string;
      shardId?: string;
    },
    boundary: "TRIM_HORIZON" | "LATEST"
  ): Observable<string | undefined> {
    return defer(() =>
      from(
        this.streamClient.send(
          new GetShardIteratorCommand({
            ShardId: partition.shardId,
            StreamArn: partition.streamArn,
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
      throw new Error(
        "Table ARN is not available yet. Please call init() first."
      );
    }
    return this._tableArn;
  }

  get streamArn(): string {
    if (!this._streamArn) {
      throw new Error(
        "Stream ARN is not available yet. Please call init() first."
      );
    }
    return this._streamArn;
  }

  private get table(): Observable<{
    tableArn: string;
    streamArn: string;
  }> {
    if (this._tableArn && this._streamArn) {
      return of({
        tableArn: this._tableArn,
        streamArn: this._streamArn,
      });
    }

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
        throw new FatalError("Table or TTL not found");
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
        switchMap(() => create$.pipe(catchError(() => describe$))),
        switchMap(({ table, ttl }) => {
          try {
            return of(assert(table, ttl));
          } catch (e) {
            if (!(e instanceof RetryError)) {
              return throwError(() => new FatalError(e.message));
            }
            return check(1000);
          }
        }),
        catchError((err) => {
          try {
            assert(undefined, undefined, err);
          } catch (e) {
            if (!(e instanceof RetryError)) {
              return throwError(() => err);
            }
          }
          return check(1000);
        })
      );

    return check(0).pipe(
      catchError((err) =>
        throwError(
          () => new Error(`Unable to use \`${this.tableName}\`: ${err.message}`)
        )
      ),
      map(({ table }) => ({
        tableArn: table.TableArn!,
        streamArn: table.LatestStreamArn!,
      }))
    );
  }

  repr(): string {
    return `CloudRxDdb{ table=${this.tableName}, hash=${this.opts.hashKey}, range=${this.opts.rangeKey} }`;
  }
}
