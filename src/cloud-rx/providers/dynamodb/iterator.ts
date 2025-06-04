import {
  Shard as DynamoDBShard,
  _Record as DynamoDBRecord,
  GetRecordsCommand,
  GetShardIteratorCommand,
  GetRecordsCommandOutput,
  DynamoDBStreamsClient,
} from "@aws-sdk/client-dynamodb-streams";
import { DynamoDbProvider } from ".";
import {
  asyncScheduler,
  AsyncSubject,
  BehaviorSubject,
  catchError,
  concatMap,
  defer,
  delay,
  delayWhen,
  EMPTY,
  expand,
  filter,
  from,
  fromEvent,
  map,
  mergeMap,
  Observable,
  observeOn,
  Observer,
  of,
  OperatorFunction,
  ReplaySubject,
  retry,
  share,
  Subject,
  Subscription,
  switchMap,
  takeUntil,
  takeWhile,
  throwError,
} from "rxjs";
import _ from "lodash";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { Stored } from "../provider";
import chalk from "chalk";
import { execa, execaNode, ExecaNodeMethod, ResultPromise } from "execa";

export type Payload = {
  modified: Stored[];
  removed: Stored[];
};

export type Position = {
  iterator: string;
  error?: Error;
};

export class ShardExecutor extends Subject<DynamoDBShard> {
  private subscriptions: Subscription[] = [];
  private record = new Subject<DynamoDBRecord>();
  private process?: ResultPromise;
  public readonly record$: Observable<DynamoDBRecord> = this.record.pipe(
    share(),
    observeOn(asyncScheduler)
  );

  get id(): string {
    return this.root.SequenceNumberRange?.StartingSequenceNumber || "0";
  }

  constructor(
    private root: DynamoDBShard,
    stream: string,
    private provider: DynamoDbProvider<unknown>
  ) {
    super();

    this.subscriptions.push(
      this.pipe(
        takeUntil(fromEvent(provider.signal, "abort")),
        // TODO Figure out how to take one at a time
        mergeMap((shard) => {
          console.log("!!! Starting ShardExecutor for:", shard.ShardId);
          return new Observable<DynamoDBShard>((subscriber) => {
            const timeout = setTimeout(() => {
              console.log(`!!! ShardExecutor for ${shard.ShardId} finished`);
              subscriber.next(shard);
              subscriber.complete();
            }, 5000);

            return () => clearTimeout(timeout);
          });
        }, 1)
        // concatMap((shard) =>
        //   from(
        //     this.provider.streamClient.send(
        //       new GetShardIteratorCommand({
        //         ShardId: shard.ShardId,
        //         StreamArn: this.stream,
        //         ShardIteratorType: "TRIM_HORIZON",
        //       })
        //     )
        //   ).pipe(
        //     map(({ ShardIterator }) => ShardIterator),
        //     filter((iterator) => !!iterator),
        //     map((iterator) => {
        //       const position: Position = {
        //         iterator: iterator!,
        //       };
        //       return position;
        //     })
        //   )
        // ),
        // switchMap((position) => {
        //   // TODO Get executable path from process
        //   this.process = execaNode({
        //     cancelSignal: signal,
        //     gracefulCancel: true,
        //   })`server --iterator ${position.iterator}`;

        //   this.process.on("error", (err) => {
        //     console.warn("ShardExecutor error:", err);
        //   });

        //   this.process.on("exit", (code) => {
        //     console.warn("ShardExecutor exited with code:", code);
        //   });

        //   const record$ = this.record;

        //   async function* readLines(process: ResultPromise) {
        //     for await (const line of process) {
        //       record$.next(
        //         JSON.parse(line.toString().trim()) as DynamoDBRecord
        //       );
        //     }
        //   }

        //   return from(readLines(this.process));
        // })
      ).subscribe((foo) => {
        this.process?.kill("SIGINT");
        this.process = undefined;
      })
    );
  }

  // override next(value: DynamoDBShard): void {
  //   console.log(
  //     `!!! Adding ${value.ShardId} to executor ${this.shard.ShardId}`
  //   );
  //   super.next(value);
  // }

  override complete(): void {
    super.complete();
    this.subscriptions.forEach((sub) => sub.unsubscribe());
    this.subscriptions = [];
    if (this.process) {
      this.process.kill("SIGINT");
      this.process = undefined;
    }
  }

  override error(err: any): void {
    super.error(err);
    this.subscriptions.forEach((sub) => sub.unsubscribe());
    this.subscriptions = [];
    if (this.process) {
      this.process.kill("SIGINT");
      this.process = undefined;
    }
  }

  static async iterate(
    provider: DynamoDbProvider<unknown>,
    iterator: string
  ): Promise<void> {
    const { Records = [], NextShardIterator } =
      await provider.streamClient.send(
        new GetRecordsCommand({
          ShardIterator: iterator,
        })
      );

    if (!Records.length && !NextShardIterator) {
      return;
    }

    Records.forEach((record) => {
      console.log(JSON.stringify(record));
    });

    if (NextShardIterator) {
      queueMicrotask(() => ShardExecutor.iterate(provider, NextShardIterator));
    }
  }

  // get id(): string {
  //   return this.shard?.SequenceNumberRange?.StartingSequenceNumber || "_";
  // }

  // get shardId(): string {
  //   return this.shard?.ShardId || "root";
  // }

  // get parentShardId(): string | undefined {
  //   return this.shard?.ParentShardId;
  // }

  // override subscribe(
  //   observerOrNext?:
  //     | Partial<Observer<Payload>>
  //     | ((value: Payload) => void)
  //     | null,
  //   error?: (err: any) => void,
  //   complete?: () => void
  // ): Subscription {
  //   // Normalize into a single Observer<Payload> object:
  //   const finalObserver: Partial<Observer<Payload>> =
  //     observerOrNext != null && typeof observerOrNext === "object"
  //       ? observerOrNext
  //       : {
  //           next: (observerOrNext as (v: Payload) => void) || undefined,
  //           error: error || undefined,
  //           complete: complete || undefined,
  //         };

  //   // 1. Register subscriber on the Subject (observer‐object form)
  //   const subscription = super.subscribe(finalObserver);

  //   // 2. Start fetching records if this is the first subscriber
  //   if (!this.started) {
  //     this.started = true;

  //     // If it's the root node, immediately complete
  //     if (this.shardId === "root") {
  //       this.complete();
  //       return subscription;
  //     }

  //     this.fetchSub = this.fetchAllRecords()
  //       .pipe(
  //         map((output) => this.convertToPayload(output)),
  //         catchError((err) => {
  //           console.warn(
  //             chalk.yellow(`WARN: ${err.name} for ${this.toString()}`)
  //           );
  //           if (
  //             err.name === "TrimmedDataAccessException" ||
  //             err.name === "ResourceNotFoundException"
  //           ) {
  //             this.done = true;
  //             this.complete();
  //             return EMPTY;
  //           }
  //           return throwError(() => err);
  //         }),
  //         takeWhile(() => !this.done, true)
  //       )
  //       .subscribe({
  //         next: (payload) => {
  //           this.next(payload);
  //         },
  //         error: (err) => this.error(err),
  //         complete: () => this.complete(),
  //       });
  //   }

  //   return subscription;
  // }

  // private fetchAllRecords(): Observable<GetRecordsCommandOutput> {
  //   return defer(() =>
  //     from(
  //       this.provider.streamClient.send(
  //         new GetShardIteratorCommand({
  //           ShardId: this.shardId,
  //           StreamArn: this.stream,
  //           ShardIteratorType: "LATEST",
  //         }),
  //         { abortSignal: this.provider.signal }
  //       )
  //     )
  //   ).pipe(
  //     mergeMap((initial) =>
  //       from(
  //         this.provider.streamClient.send(
  //           new GetRecordsCommand({
  //             ShardIterator: initial.ShardIterator!,
  //           }),
  //           { abortSignal: this.provider.signal }
  //         )
  //       )
  //     ),
  //     expand((output) => {
  //       if (!output.NextShardIterator) {
  //         this.done = true;
  //         return EMPTY;
  //       }

  //       // Add backoff when no records are returned to avoid overwhelming DynamoDB
  //       const hasRecords = output.Records && output.Records.length > 0;
  //       const nextRequest = from(
  //         this.provider.streamClient.send(
  //           new GetRecordsCommand({
  //             ShardIterator: output.NextShardIterator!,
  //           }),
  //           { abortSignal: this.provider.signal }
  //         )
  //       );

  //       // If no records, add a small delay before next request
  //       return hasRecords ? nextRequest : nextRequest.pipe(delay(250));
  //     })
  //   );
  // }

  // private convertToPayload(commandOutput: GetRecordsCommandOutput): Payload {
  //   this.records += commandOutput.Records?.length ?? 0;
  //   this.iterations++;

  //   const records = commandOutput.Records ?? [];
  //   const payload: Payload = { modified: [], removed: [] };

  //   for (const rec of records) {
  //     if (rec.eventName === "REMOVE") {
  //       if (rec.dynamodb?.OldImage) {
  //         const stored = unmarshall(rec.dynamodb.OldImage) as Stored;
  //         payload.removed.push(stored);
  //       }
  //     }
  //     if (rec.dynamodb?.NewImage) {
  //       const stored = unmarshall(rec.dynamodb.NewImage) as Stored;
  //       payload.modified.push(stored);
  //     }
  //   }

  //   return payload;
  // }

  // override complete(): void {
  //   super.complete();
  //   if (this.fetchSub) {
  //     this.fetchSub.unsubscribe();
  //     this.fetchSub = null;
  //   }
  // }

  // override error(err: any): void {
  //   super.error(err);
  //   if (this.fetchSub) {
  //     this.fetchSub.unsubscribe();
  //     this.fetchSub = null;
  //   }
  // }

  // override unsubscribe(): void {
  //   super.unsubscribe();
  //   if (this.fetchSub) {
  //     this.fetchSub.unsubscribe();
  //     this.fetchSub = null;
  //   }
  // }

  // toString(): string {
  //   const repr = `id=${this.shardId} iterations=${this.iterations} records=${this.records} done=${this.done}`;
  //   const type = this.parentShardId ? "Child" : "Parent";
  //   return `${type}ShardIterator[${this.provider.id}](${repr})`;
  // }
}
