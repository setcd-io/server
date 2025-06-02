import TreeModel from "tree-model";
import {
  DescribeStreamCommand,
} from "@aws-sdk/client-dynamodb-streams";
import { DynamoDbProvider } from ".";
import {
  catchError,
  concatAll,
  concatMap,
  EMPTY,
  from,
  map,
  mergeAll,
  mergeMap,
  Observer,
  reduce,
  ReplaySubject,
  startWith,
  Subject,
  Subscription,
  switchMap,
  timer,
  toArray,
} from "rxjs";
import _ from "lodash";
import { ShardIterator } from "./iterator";

export class Shards<T> extends Subject<T> {
  private iterators = new Subject<ShardIterator>();

  constructor(
    private readonly provider: DynamoDbProvider<unknown>,
    stream: string,
    signal: AbortSignal
  ) {
    super();

    const tree = new TreeModel();
    const root = new ShardIterator(provider, stream);
    const rootNode = tree.parse(root);

    // this.root = root.withNode(rootNode);

    // const root = tree.parse<Shard<T>>(new Shard<T>(this.provider, stream));

    // root.walk((node) => {
    //   if (node.isRoot()) {
    //     console.log("Shards:");
    //     return true;
    //   }

    //   const depth = node.getPath().length - 1;
    //   const indent = " ".repeat(depth * 3 - 3);

    //   const parent = node.parent as TreeModel.Node<any> | null;
    //   const isLast = parent
    //     ? node === parent.children[parent.children.length - 1]
    //     : true;

    //   const glyph = depth === 0 ? "" : isLast ? "└─ " : "├─ ";

    //   const shard = node.model as Shard<T>;
    //   tailFrom(shard.records$).then((tail) => {
    //     console.log(
    //       `${indent}${glyph}${node.model.toString()}, actual: ${
    //         tail.length
    //       }`
    //     );
    //   });
    //   return true;
    // });

    const iterators = timer(0, 1000)
      .pipe(
        startWith(0),
        switchMap(() =>
          from(
            provider.streamClient
              .send(new DescribeStreamCommand({ StreamArn: stream }))
              .then((data) => {
                // console.log("!!! DescribeStream result:", {
                //   streamArn: stream,
                //   shardCount: data.StreamDescription?.Shards?.length || 0,
                //   shards: data.StreamDescription?.Shards?.map(s => ({
                //     id: s.ShardId,
                //     parent: s.ParentShardId,
                //     start: s.SequenceNumberRange?.StartingSequenceNumber,
                //     end: s.SequenceNumberRange?.EndingSequenceNumber
                //   })) || []
                // });
                return data.StreamDescription?.Shards || [];
              })
          ).pipe(
            reduce((acc, shards) => {
              const absent = shards.filter(
                (shard) => {
                  const exists = !!rootNode.first(
                    (node) =>
                      (node.model as ShardIterator).shardId === shard.ShardId
                  );
                  return !exists;
                }
              );

              const newIterators = absent.map(
                (shard) => {
                  return new ShardIterator(this.provider, stream, shard);
                }
              );
              acc.push(...newIterators);
              return acc;
            }, [] as ShardIterator[]),
            mergeAll(), // or concatAll()?
            map((shard) => {
              const parent =
                rootNode.first(
                  (node) =>
                    (node.model as ShardIterator).shardId ===
                    shard.parentShardId
                ) || rootNode;

              const node = parent.addChild(tree.parse(shard));

              return node;
            }),
            toArray()
          )
        )
      )
      .pipe(
        map((shards) => {
          const allNodes = rootNode.all({ strategy: "pre" }, (node) => !node.isRoot());
          
          const filteredNodes = allNodes.filter((node) => {
            const nodeShardId = (node.model as ShardIterator).shardId;
            const found = !!shards.find((s) => (s.model as ShardIterator).shardId === nodeShardId);
            return found;
          });
          
          return filteredNodes;
        }),
        concatAll() // Ensures sequential processing of shards in discovery order
      )
      .subscribe((node) => {
        // Process all shards in parallel - ordering maintained by DynamoDB sequence numbers
        this.iterators.next(node.model as ShardIterator);
      });

    // this.records$ = root.pipe(
    //   // NOTE: pre == timeline-ordered traversal
    //   map((root) => root.all({ strategy: "pre" }, (node) => !node.isRoot())),
    //   filter((shards) => !!shards.length),
    //   concatAll(),
    //   map((node) => node.model as Shard<T>),
    //   distinct((shard) => shard.shardId),
    //   tap((shard) =>
    //     console.log(
    //       chalk.green(`Shard Discovered: ${chalk.bold(shard.toString())}`)
    //     )
    //   ),
    //   concatMap((shard) => shard.records$),
    //   shareReplay({
    //     refCount: false,
    //     scheduler: asyncScheduler,
    //   })
    // );

    signal.addEventListener("abort", () => {
      iterators.unsubscribe();
    });
  }

  override subscribe(
    observerOrNext?: Partial<Observer<T>> | ((value: T) => void) | null,
    error?: (err: any) => void,
    complete?: () => void
  ): Subscription {
    // Start feeding the main Subject with data from iterators
    const subscription = this.iterators
      .pipe(
        mergeMap((iterator) => iterator), // Process leaf shards in parallel from TRIM_HORIZON
        concatMap((payload) => from(payload.modified)), // Extract individual records from payload
        catchError((err) => {
          this.error(err);
          return EMPTY;
        })
      )
      .subscribe({
        next: (record) => this.next(record as T),
        error: (err) => this.error(err),
        complete: () => this.complete()
      });

    // Subscribe to the main Subject
    let mainSubscription: Subscription;
    if (typeof observerOrNext === 'function') {
      mainSubscription = super.subscribe({
        next: observerOrNext,
        error: error || undefined,
        complete: complete || undefined
      });
    } else {
      mainSubscription = super.subscribe(observerOrNext || undefined);
    }

    return new Subscription(() => {
      subscription.unsubscribe();
      mainSubscription.unsubscribe();
    });
  }
}
