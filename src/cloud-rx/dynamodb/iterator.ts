import {
  Shard as DynamoDBShard,
  GetRecordsCommand,
  GetShardIteratorCommand,
  GetRecordsCommandOutput,
} from "@aws-sdk/client-dynamodb-streams";
import { DynamoDbProvider } from ".";
import {
  catchError,
  defer,
  delay,
  EMPTY,
  expand,
  from,
  map,
  mergeMap,
  Observable,
  Observer,
  Subject,
  Subscription,
  takeWhile,
  throwError,
} from "rxjs";
import _ from "lodash";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { Stored } from "../provider";
import chalk from "chalk";

export type Payload = {
  modified: Stored[];
  removed: Stored[];
};

export class ShardIterator extends Subject<Payload> {
  private records = 0;
  private iterations = 0;
  private done = false;
  private started = false;

  private fetchSub: Subscription | null = null;

  constructor(
    private readonly provider: DynamoDbProvider<unknown>,
    private readonly stream: string,
    private readonly shard?: DynamoDBShard
  ) {
    super();
  }

  get id(): string {
    return this.shard?.SequenceNumberRange?.StartingSequenceNumber || "_";
  }

  get shardId(): string {
    return this.shard?.ShardId || "root";
  }

  get parentShardId(): string | undefined {
    return this.shard?.ParentShardId;
  }

  override subscribe(
    observerOrNext?:
      | Partial<Observer<Payload>>
      | ((value: Payload) => void)
      | null,
    error?: (err: any) => void,
    complete?: () => void
  ): Subscription {
    // Normalize into a single Observer<Payload> object:
    const finalObserver: Partial<Observer<Payload>> =
      observerOrNext != null && typeof observerOrNext === "object"
        ? observerOrNext
        : {
            next: (observerOrNext as (v: Payload) => void) || undefined,
            error: error || undefined,
            complete: complete || undefined,
          };

    // 1. Register subscriber on the Subject (observer‐object form)
    const subscription = super.subscribe(finalObserver);

    // 2. Start fetching records if this is the first subscriber
    if (!this.started) {
      this.started = true;

      // If it's the root node, immediately complete
      if (this.shardId === "root") {
        this.complete();
        return subscription;
      }

      this.fetchSub = this.fetchAllRecords()
        .pipe(
          map((output) => this.convertToPayload(output)),
          catchError((err) => {
            console.warn(
              chalk.yellow(`WARN: ${err.name} for ${this.toString()}`)
            );
            if (
              err.name === "TrimmedDataAccessException" ||
              err.name === "ResourceNotFoundException"
            ) {
              this.done = true;
              this.complete();
              return EMPTY;
            }
            return throwError(() => err);
          }),
          takeWhile(() => !this.done, true)
        )
        .subscribe({
          next: (payload) => {
            this.next(payload);
          },
          error: (err) => this.error(err),
          complete: () => this.complete(),
        });
    }

    return subscription;
  }

  private fetchAllRecords(): Observable<GetRecordsCommandOutput> {
    return defer(() =>
      from(
        this.provider.streamClient.send(
          new GetShardIteratorCommand({
            ShardId: this.shardId,
            StreamArn: this.stream,
            ShardIteratorType: "LATEST",
          }),
          { abortSignal: this.provider.signal }
        )
      )
    ).pipe(
      mergeMap((initial) =>
        from(
          this.provider.streamClient.send(
            new GetRecordsCommand({
              ShardIterator: initial.ShardIterator!,
            }),
            { abortSignal: this.provider.signal }
          )
        )
      ),
      expand((output) => {
        if (!output.NextShardIterator) {
          this.done = true;
          return EMPTY;
        }

        // Add backoff when no records are returned to avoid overwhelming DynamoDB
        const hasRecords = output.Records && output.Records.length > 0;
        const nextRequest = from(
          this.provider.streamClient.send(
            new GetRecordsCommand({
              ShardIterator: output.NextShardIterator!,
            }),
            { abortSignal: this.provider.signal }
          )
        );

        // If no records, add a small delay before next request
        return hasRecords ? nextRequest : nextRequest.pipe(delay(250));
      })
    );
  }

  private convertToPayload(commandOutput: GetRecordsCommandOutput): Payload {
    this.records += commandOutput.Records?.length ?? 0;
    this.iterations++;

    const records = commandOutput.Records ?? [];
    const payload: Payload = { modified: [], removed: [] };

    for (const rec of records) {
      if (rec.eventName === "REMOVE") {
        if (rec.dynamodb?.OldImage) {
          const stored = unmarshall(rec.dynamodb.OldImage) as Stored;
          payload.removed.push(stored);
        }
      }
      if (rec.dynamodb?.NewImage) {
        const stored = unmarshall(rec.dynamodb.NewImage) as Stored;
        payload.modified.push(stored);
      }
    }

    return payload;
  }

  override complete(): void {
    super.complete();
    if (this.fetchSub) {
      this.fetchSub.unsubscribe();
      this.fetchSub = null;
    }
  }

  override error(err: any): void {
    super.error(err);
    if (this.fetchSub) {
      this.fetchSub.unsubscribe();
      this.fetchSub = null;
    }
  }

  override unsubscribe(): void {
    super.unsubscribe();
    if (this.fetchSub) {
      this.fetchSub.unsubscribe();
      this.fetchSub = null;
    }
  }

  toString(): string {
    const repr = `id=${this.shardId} iterations=${this.iterations} records=${this.records} done=${this.done}`;
    const type = this.parentShardId ? "Child" : "Parent";
    return `${type}ShardIterator[${this.provider.id}](${repr})`;
  }
}
