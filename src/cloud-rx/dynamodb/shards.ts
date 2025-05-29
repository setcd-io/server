import TreeModel from "tree-model";
import {
  Shard as _Shard,
  DescribeStreamCommand,
  GetRecordsCommand,
  GetShardIteratorCommand,
} from "@aws-sdk/client-dynamodb-streams";
import { DynamoDbProvider } from ".";
import {
  asyncScheduler,
  BehaviorSubject,
  catchError,
  concatAll,
  concatMap,
  delay,
  distinct,
  distinctUntilChanged,
  filter,
  from,
  fromEvent,
  last,
  map,
  Observable,
  observeOn,
  of,
  ReplaySubject,
  share,
  shareReplay,
  startWith,
  Subject,
  Subscription,
  switchMap,
  take,
  takeUntil,
  tap,
  throwError,
  timer,
} from "rxjs";
import _, { concat } from "lodash";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { Stored } from "../provider";
import chalk from "chalk";

export type Position = {
  sequence?: string;
  iterator?: string;
};

export class Shard<T> {
  public readonly shardId: string;
  public readonly parentShardId?: string;
  private records: number = 0;
  private iterations: number = 0;

  private incoming: Subject<Stored> = new Subject();
  public readonly records$: Observable<Stored> = this.incoming.pipe(
    shareReplay({ refCount: false, scheduler: asyncScheduler })
  );

  constructor(
    public readonly provider: DynamoDbProvider<T>,
    private readonly tree: TreeModel.Node<Shard<T>>,
    public readonly stream: string,
    shard: _Shard
  ) {
    this.shardId = shard.ShardId!;
    this.parentShardId = shard.ParentShardId;

    console.log(
      chalk.blue(`Observation Starting: ${chalk.bold(this.toString())}`)
    );

    const observation = this.observe().subscribe({
      next: (stored) => {
        this.records++;
        this.incoming.next(stored);
      },
      complete: () => {
        console.log(
          chalk.green(`Observation Complete: ${chalk.bold(this.toString())}`)
        );
        this.incoming.complete();
        observation.unsubscribe();
      },
      error: (err) => {
        console.warn(
          chalk.red(`Error observing shard ${this.shardId}: ${err.message}`)
        );
        this.incoming.error(err);
        observation.unsubscribe();
      },
    });
  }

  private observe(): Observable<Stored> {
    const position = new BehaviorSubject<Position>({});
    // Position preference:
    // 1. iterator
    // 2. sequence
    // 3. boundary
    // Note: Position is updated in record$ mapping below
    // TODO: save/load position to/from table tags for restartability
    const command$ = position.pipe(
      concatMap((position) => {
        return from(
          position.iterator
            ? Promise.resolve(
                new GetRecordsCommand({
                  ShardIterator: position.iterator,
                })
              )
            : this.provider.streamClient
                .send(
                  new GetShardIteratorCommand({
                    ShardId: this.shardId,
                    StreamArn: this.stream,
                    ShardIteratorType: "TRIM_HORIZON",
                  }),
                  { abortSignal: this.provider.signal }
                )
                .then((res) => {
                  return new GetRecordsCommand({
                    ShardIterator: res.ShardIterator,
                  });
                })
        ).pipe(map((cmd) => ({ cmd, position })));
      })
    );

    return command$.pipe(
      concatMap(({ cmd, position: pos }) =>
        from(
          this.provider.streamClient.send(cmd, {
            abortSignal: this.provider.signal,
          })
        ).pipe(
          catchError((err) => {
            console.warn(
              chalk.yellow(`WARN: ${err.name} for ${this.toString()}`),
              { input: cmd.input, position: pos }
            );

            if (
              err.name === "TrimmedDataAccessException" ||
              err.name === "ResourceNotFoundException"
            ) {
              // DEVNOTE: This happens with DynamoDB Local when the table is brand new
              return of({
                Records: [],
                NextShardIterator: undefined,
              });
            }

            return throwError(() => err);
          }),
          map((res) => {
            this.iterations++;
            const records = (res.Records || []).sort((a, b) => {
              const aSeq = a.dynamodb?.SequenceNumber;
              const bSeq = b.dynamodb?.SequenceNumber;
              if (aSeq && bSeq) {
                return aSeq.localeCompare(bSeq);
              }
              return 0;
            });

            if (res.NextShardIterator) {
              position.next({
                iterator: res.NextShardIterator,
                sequence: records.slice(-1)[0]?.dynamodb?.SequenceNumber,
              });
            } else {
              position.complete();
            }

            return { records };
          }),
          map(({ records }) => {
            const items = records
              .filter((record) => {
                if (record.eventName === "REMOVE") {
                  // TODO: emit REMOVE (aka expire) events
                  //       Q: do i simply convert to a Stored DELETE object, or
                  //          should i emit a REMOVE event?
                  return false;
                }
                if (!record.dynamodb?.NewImage) {
                  return false;
                }
                return true;
              })
              .map(
                (record) => unmarshall(record.dynamodb?.NewImage!) as Stored
              );

            return items;
          })
        )
      ),
      concatAll()
    );
  }

  toString(): string {
    const repr = `id=${this.shardId} iterations=${this.iterations} records=${this.records}`;
    let type = this.parentShardId ? "Child" : "Parent";
    if (!this.tree.first((node) => node.model.shardId === this.parentShardId)) {
      type = `Orphaned${type}`;
    }
    const display = `${type}Shard[${this.provider.id}](${repr})`;
    return display;
  }
}

export class Shards<T> {
  // private root: Observable<TreeModel.Node<Shard<T>>>;
  public records$: Observable<Stored>;

  constructor(
    private readonly provider: DynamoDbProvider<T>,
    stream: string,
    signal: AbortSignal
  ) {
    const tree = new TreeModel();
    const root: BehaviorSubject<TreeModel.Node<Shard<T>>> = new BehaviorSubject(
      tree.parse({ id: "root", children: [] })
    );

    // const stats = root.subscribe((root) => {
    //   root.walk((node) => {
    //     if (node.isRoot()) {
    //       console.log("Shards:");
    //       return true;
    //     }

    //     const depth = node.getPath().length - 1;
    //     const indent = " ".repeat(depth * 3 - 3);

    //     const parent = node.parent as TreeModel.Node<any> | null;
    //     const isLast = parent
    //       ? node === parent.children[parent.children.length - 1]
    //       : true;

    //     const glyph = depth === 0 ? "" : isLast ? "└─ " : "├─ ";

    //     console.log(indent + glyph + node.model.toString());
    //     return true;
    //   });

    //   // root.walk({ strategy: "pre" }, (node) => {
    //   //   if (node.isRoot()) {
    //   //     console.log("ROOT");
    //   //     return true;
    //   //   }
    //   //   console.log(` -> ${node.model.shardId}`);
    //   //   return true;
    //   // });
    // });

    const shards = timer(0, 5000)
      .pipe(
        startWith(0),
        map(() => root.value),
        switchMap((root) =>
          from(
            provider.streamClient
              .send(new DescribeStreamCommand({ StreamArn: stream }))
              .then((data) => data.StreamDescription?.Shards || [])
          ).pipe(
            filter((shards) => !!shards.length),
            concatAll(),
            map((shard) => ({
              shard,
              root,
            })),
            map(({ shard, root }) => {
              let parent = shard.ParentShardId
                ? root.first(
                    (node) =>
                      (node.model as Shard<T>).shardId === shard.ParentShardId
                  )
                : root;

              const existing = root.first(
                (node) => (node.model as Shard<T>).shardId === shard.ShardId
              );

              if (!existing) {
                (parent || root).addChild(
                  tree.parse(new Shard<T>(this.provider, root, stream, shard))
                );
              }

              return root;
            }),
            last()
          )
        )
      )
      .subscribe((updated) => {
        if (
          !_.isEqual(
            root.value
              .all((node) => !node.isRoot())
              .map((node) => node.model as Shard<T>)
              .map((shard) => shard.shardId)
              .sort(),
            updated
              .all((node) => !node.isRoot())
              .map((node) => node.model as Shard<T>)
              .map((shard) => shard.shardId)
              .sort()
          )
        ) {
          root.next(updated);
        }
      });

    this.records$ = root.pipe(
      // NOTE: pre == timeline-ordered traversal
      map((root) => root.all({ strategy: "pre" }, (node) => !node.isRoot())),
      filter((shards) => !!shards.length),
      concatAll(),
      map((node) => node.model as Shard<T>),
      distinct((shard) => shard.shardId),
      tap((shard) =>
        console.log(
          chalk.green(`Shard Discovered: ${chalk.bold(shard.toString())}`)
        )
      ),
      concatMap((shard) => shard.records$),
      shareReplay({
        refCount: false,
        scheduler: asyncScheduler,
      })
    );

    signal.addEventListener("abort", () => {
      // stats.unsubscribe();
      shards.unsubscribe();
    });
  }
}
