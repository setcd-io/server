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
  concatAll,
  concatMap,
  delay,
  delayWhen,
  distinct,
  EMPTY,
  filter,
  from,
  fromEvent,
  interval,
  last,
  map,
  Observable,
  observeOn,
  of,
  ReplaySubject,
  skipUntil,
  startWith,
  Subject,
  switchMap,
  takeUntil,
  takeWhile,
  tap,
  throttle,
  throttleTime,
  timer,
} from "rxjs";
import _, { merge, take } from "lodash";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { Stored } from "../provider";
import { Get } from "@aws-sdk/client-dynamodb";
import chalk from "chalk";
import { throttleByQueueSize } from "../util";

export type Boundary = "ALL" | "LATEST";
export type Position = {
  sequence?: string;
  iterator?: string;
};

export const BATCH_SIZE = 10; // TODO: make this configurable

export class Shard<T> {
  public readonly shardId: string;
  public readonly parentShardId?: string;
  public start?: string;
  public end?: string;

  constructor(
    public readonly provider: DynamoDbProvider<T>,
    private readonly tree: TreeModel.Node<Shard<T>>,
    public readonly stream: string,
    shard: _Shard
  ) {
    this.shardId = shard.ShardId!;
    this.parentShardId = shard.ParentShardId;
    this.start = shard.SequenceNumberRange?.StartingSequenceNumber;
    this.end = shard.SequenceNumberRange?.EndingSequenceNumber;
  }

  observe(boundary: Boundary): Observable<Stored> {
    const position = new BehaviorSubject<Position>({});
    // Position preference:
    // 1. iterator
    // 2. sequence
    // 3. boundary
    // Note: Position is updated in record$ mapping below
    // TODO: save/load position to/from table tags for restartability
    const command$ = position.pipe(
      takeUntil(fromEvent(this.provider.signal, "abort")),
      // throttleByQueueSize<Position>(500),
      concatMap((position) => {
        return from(
          position.iterator
            ? Promise.resolve(
                new GetRecordsCommand({
                  ShardIterator: position.iterator,
                  Limit: BATCH_SIZE,
                })
              )
            : this.provider.streamClient
                .send(
                  new GetShardIteratorCommand({
                    ShardId: this.shardId,
                    StreamArn: this.stream,
                    ShardIteratorType: position?.sequence
                      ? "AFTER_SEQUENCE_NUMBER"
                      : boundary === "ALL"
                      ? "TRIM_HORIZON"
                      : "LATEST",
                    SequenceNumber: position?.sequence
                      ? position.sequence
                      : undefined,
                  })
                )
                .then((res) => {
                  return new GetRecordsCommand({
                    ShardIterator: res.ShardIterator,
                    Limit: BATCH_SIZE,
                  });
                })
        ).pipe(map((cmd) => ({ cmd, position })));
      })
    );

    return command$.pipe(
      concatMap(({ cmd, position: pos }) =>
        from(
          this.provider.streamClient.send(cmd).catch((e) => {
            console.warn(
              chalk.yellow(`WARN: ${e.name} for ${this.toString()}`),
              { input: cmd.input, position: pos }
            );

            if (e.name === "TrimmedDataAccessException") {
              // DEVNOTE: This happens with DynamoDB Local when the table is brand new
              return {
                Records: [],
                NextShardIterator: cmd.input.ShardIterator,
              };
            }

            throw e;
          })
        ).pipe(
          map((res) => {
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
          }),
          filter((items) => !!items.length),
          concatAll()
        )
      )
    );
  }

  toString(): string {
    const repr = `id=${this.shardId} start=${this.start} end=${this.end}`;
    let type = this.parentShardId ? "Child" : "Parent";
    if (!this.tree.first((node) => node.model.shardId === this.parentShardId)) {
      type = `Orphaned${type}`;
    }
    const display = `${type}Shard[${this.provider.id}](${repr})`;
    return display;
  }
}

export class Shards<T> {
  private root: Observable<TreeModel.Node<Shard<T>>>;

  constructor(
    private readonly provider: DynamoDbProvider<T>,
    stream: string,
    signal: AbortSignal
  ) {
    const tree = new TreeModel();
    const root: BehaviorSubject<TreeModel.Node<Shard<T>>> = new BehaviorSubject(
      tree.parse({ id: "root", children: [] })
    );
    this.root = root.asObservable();

    const stats = this.root.subscribe((root) => {
      root.walk((node) => {
        if (node.isRoot()) {
          console.log("Shards:");
          return true;
        }

        const depth = node.getPath().length - 1;
        const indent = " ".repeat(depth * 3 - 3);

        const parent = node.parent as TreeModel.Node<any> | null;
        const isLast = parent
          ? node === parent.children[parent.children.length - 1]
          : true;

        const glyph = depth === 0 ? "" : isLast ? "└─ " : "├─ ";

        console.log(indent + glyph + node.model.toString());
        return true;
      });

      // root.walk({ strategy: "pre" }, (node) => {
      //   if (node.isRoot()) {
      //     console.log("ROOT");
      //     return true;
      //   }
      //   console.log(` -> ${node.model.shardId}`);
      //   return true;
      // });
    });

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
              shard: new Shard<T>(this.provider, root, stream, shard),
              root,
            })),
            map(({ shard, root }) => {
              let parent = shard.parentShardId
                ? root.first(
                    (node) =>
                      (node.model as Shard<T>).shardId === shard.parentShardId
                  )
                : root;

              const existing = root.first(
                (node) => (node.model as Shard<T>).shardId === shard.shardId
              );

              if (!existing) {
                (parent || root).addChild(tree.parse(shard));
              }

              return root;
            }),
            last()
          )
        )
      )
      .subscribe((updated) => {
        root.next(updated);
      });

    signal.addEventListener("abort", () => {
      stats.unsubscribe();
      shards.unsubscribe();
    });
  }

  observe(boundary: Boundary): Observable<Stored> {
    return this.shard$().pipe(concatMap((shard) => shard.observe(boundary)));
  }

  private shard$(): Observable<Shard<T>> {
    // TODO Group by shards that are children of root
    // - Use concatMap to speed up parallelization
    return this.root
      .pipe(
        // NOTE: pre == timeline-ordered traversal
        map((root) => root.all({ strategy: "pre" }, (node) => !node.isRoot()))
      )
      .pipe(filter((shards) => !!shards.length))
      .pipe(concatAll())
      .pipe(map((node) => node.model as Shard<T>))
      .pipe(distinct((shard) => shard.shardId));
  }
}
