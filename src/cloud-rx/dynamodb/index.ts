import {
  CreateTableCommand,
  DescribeTableCommand,
  DescribeTimeToLiveCommand,
  DynamoDBClient,
  TableDescription,
  TimeToLiveDescription,
  UpdateTimeToLiveCommand,
} from "@aws-sdk/client-dynamodb";
import { DynamoDBStreamsClient } from "@aws-sdk/client-dynamodb-streams";
import { DynamoDBDocument } from "@aws-sdk/lib-dynamodb";
import {
  asyncScheduler,
  catchError,
  defer,
  forkJoin,
  fromEvent,
  map,
  Observable,
  of,
  switchMap,
  takeUntil,
  throwError,
  timer,
} from "rxjs";
import { Provider } from "../provider";
import { FatalError, RetryError } from "./errors";

export type Options<Pk extends string, Sk extends String> = {
  signal: AbortSignal;
  pk?: Pk;
  sk?: Sk;
  region?: string;
};

export class DynamoDbProvider<
  Pk extends string,
  Sk extends string
> extends Provider<DynamoDbProvider<Pk, Sk>> {
  public readonly client: DynamoDBClient;
  public readonly documentClient: DynamoDBDocument;
  public readonly streamClient: DynamoDBStreamsClient;
  private _tableArn?: string;
  private _streamArn?: string;

  constructor(public readonly name: string, private opts: Options<Pk, Sk>) {
    super(opts.signal);
    this.client = new DynamoDBClient({ region: this.opts.region });
    this.documentClient = DynamoDBDocument.from(this.client);
    this.streamClient = new DynamoDBStreamsClient({ region: this.opts.region });
  }

  get pk(): Pk {
    return this.opts.pk || ("pk" as Pk);
  }

  get sk(): Sk {
    return this.opts.sk || ("sk" as Sk);
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

  init(id?: string): Observable<DynamoDbProvider<Pk, Sk>> {
    const provider = id
      ? new DynamoDbProvider(`${this.name}-${id}`, this.opts)
      : this;

    return provider.table.pipe(
      map((res) => {
        provider._tableArn = res.tableArn;
        provider._streamArn = res.streamArn;
        return provider;
      })
    );
  }

  withKeys<DesiredPk extends Pk, DesiredSk extends Sk>(
    pk: DesiredPk,
    sk: DesiredSk
  ): DynamoDbProvider<DesiredPk, DesiredSk> {
    const options: Options<DesiredPk, DesiredSk> = {
      ...this.opts,
      pk,
      sk,
    };
    this.opts = options;
    return this as unknown as DynamoDbProvider<DesiredPk, DesiredSk>;
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
        this.client.send(new DescribeTableCommand({ TableName: this.name }), {
          abortSignal: this.signal,
        }),
        this.client.send(
          new DescribeTimeToLiveCommand({ TableName: this.name }),
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
            TableName: this.name,
            KeySchema: [
              { AttributeName: this.pk, KeyType: "HASH" },
              { AttributeName: this.sk, KeyType: "RANGE" },
            ],
            AttributeDefinitions: [
              { AttributeName: this.pk, AttributeType: "S" },
              { AttributeName: this.sk, AttributeType: "S" },
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
            TableName: this.name,
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

      if (error && error.code === "ECONNREFUSED") {
        throw new RetryError("Connection refused");
      }

      if (error && error.name === "ResourceNotFoundException") {
        throw new RetryError("Resource not found");
      }

      if (error && error.name === "ResourceInUseException") {
        throw new RetryError("Resource in use");
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
          (key) => key.KeyType === "HASH" && key.AttributeName !== this.pk
        )
      ) {
        throw new FatalError(`Hash key needs to be named ${this.pk}`);
      }
      if (
        table.KeySchema?.find(
          (key) => key.KeyType === "RANGE" && key.AttributeName !== this.sk
        )
      ) {
        throw new FatalError(`Range key needs to be named ${this.sk}`);
      }

      if (
        table.AttributeDefinitions?.find(
          (key) => key.AttributeName === this.pk && key.AttributeType !== "S"
        )
      ) {
        throw new FatalError(`Hash needs to be a string type`);
      }

      if (
        table.AttributeDefinitions?.find(
          (key) => key.AttributeName === this.sk && key.AttributeType !== "S"
        )
      ) {
        throw new FatalError(`Hash needs to be a string type`);
      }

      if (
        table.AttributeDefinitions?.find(
          (key) => key.AttributeName === "expires" && key.AttributeType !== "N"
        )
      ) {
        throw new FatalError(`Table neesd a TTL attribute of type number`);
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
      map(({ table }) => ({
        tableArn: table.TableArn!,
        streamArn: table.LatestStreamArn!,
      }))
    );
  }
}
