import {
  DynamoDBStreamsClient,
  ListStreamsCommand,
  DescribeStreamCommand,
  GetShardIteratorCommand,
  GetRecordsCommand,
  _Record,
  ShardIteratorType,
} from "@aws-sdk/client-dynamodb-streams";
import {
  Observable,
  from,
  combineLatest,
  interval,
  timer,
  forkJoin,
  asyncScheduler,
  of,
  merge,
  Subject,
  throwError,
} from "rxjs";
import {
  map,
  switchMap,
  expand,
  filter,
  startWith,
  distinctUntilChanged,
  share,
  catchError,
  tap,
  delayWhen,
  auditTime,
  observeOn,
} from "rxjs/operators";
import _ from "lodash";
import { DynamoDBRecord, DynamoDBStreamEvent } from "aws-lambda";
import {
  DescribeTableCommand,
  DynamoDBClient,
  TagResourceCommand,
  UntagResourceCommand,
  ListTagsOfResourceCommand,
  Tag,
  CreateTableCommand,
  TableDescription,
} from "@aws-sdk/client-dynamodb";
import Context from "../context";
import chalk from "chalk";
import promiseRetry from "promise-retry";

type Stat = {
  tableArn: string;
  shardId: string;
  active: boolean;
  sequence?: string;
  iterations?: number;
  records?: number;
};

export class TableMonitor {
  private readonly table: DynamoDBClient;
  private readonly client: DynamoDBStreamsClient;

  private tableArn?: string;
  private stats: Record<string, Stat | undefined> = {};
  private stat$ = new Subject<Stat>();

  constructor(private ctx: Context) {
    console.log("Starting Table Monitor:", ctx.tableName);
    this.table = ctx.ddbClient;
    this.client = ctx.streamsClient;

    const stats = this.stat$
      .pipe(
        observeOn(asyncScheduler),
        map((stat) => {
          const stats = _.cloneDeep(this.stats);
          const previousStat = _.cloneDeep(stats[stat.shardId!]);

          const iterations = (previousStat?.iterations || 0) + 1;
          const records = (previousStat?.records || 0) + (stat.records || 0);
          const sequence = stat.sequence || previousStat?.sequence;

          stat = {
            ...stat,
            iterations,
            records,
            sequence,
          };

          this.stats[stat.shardId!] = stat;

          return {
            stat: _.cloneDeep(stat),
            previousStat,
            stats,
          };
        })
      )
      .pipe(
        auditTime(1000, asyncScheduler),
        map(({ stats }) => {
          const { records, iterations } = Object.values(stats).reduce(
            (acc, stat) => {
              acc.records += stat?.records || 0;
              acc.iterations += stat?.iterations || 0;
              return acc;
            },
            { records: 0, iterations: 0 }
          );
          return { stats, records, iterations };
        }),
        tap(({ stats, records, iterations }) =>
          console.log(
            chalk.yellow(
              `Emitted ${chalk.bold(records)} record(s) in ${chalk.bold(
                iterations
              )} iteration(s) across ${chalk.bold(
                Object.values(stats).length
              )} shard(s)`
            )
          )
        ),
        auditTime(5000, asyncScheduler),
        switchMap(({ stats }) => forkJoin([of(stats), this.tags])),
        map(([stats, tags]) => {
          const addTags = Object.entries(stats).reduce(
            (acc, [shardId, stat]) => {
              if (!stat?.active) return acc;
              if (!!tags && tags[shardId] === stat.sequence) return acc;
              acc.push({ Key: shardId, Value: stat.sequence });
              return acc;
            },
            [] as Tag[]
          );

          const removeTags = Object.entries(tags || {}).reduce(
            (acc, [key, v]) => {
              if (!key.startsWith("shardId-")) return acc;
              if (stats[key]?.active) return acc;
              acc.push(key);
              return acc;
            },
            [] as string[]
          );

          return { stats, addTags, removeTags };
        }),
        tap(({ addTags, removeTags }) => {
          if (addTags.length === 0 && removeTags.length === 0) return;
          const total = addTags.length + removeTags.length;
          console.log(chalk.yellow(`Updating ${chalk.bold(total)} tag(s)`));
          addTags.map((t) =>
            console.log(chalk.green(` + ${chalk.bold(t.Key)}: ${t.Value}`))
          );
          removeTags.map((t) => console.log(chalk.red(` - ${chalk.bold(t)}`)));
        }),
        switchMap(({ addTags, removeTags }) => {
          return forkJoin([
            from(
              this.table.send(
                new TagResourceCommand({
                  ResourceArn: this.tableArn,
                  Tags: addTags,
                })
              )
            ).pipe(
              catchError((e) => {
                return of([]);
              })
            ),
            from(
              this.table.send(
                new UntagResourceCommand({
                  ResourceArn: this.tableArn,
                  TagKeys: removeTags,
                })
              )
            ).pipe(
              catchError((e) => {
                return of([]);
              })
            ),
          ]);
        })
      )
      .subscribe(({}) => {});

    this.ctx.on("abort", () => {
      console.log("Stopping Table Monitor:", ctx.tableName);
      stats.unsubscribe();
    });
  }

  get table$(): Observable<TableDescription> {
    return (
      from(
        promiseRetry((retry) =>
          this.table
            .send(new DescribeTableCommand({ TableName: this.ctx.tableName }), {
              abortSignal: this.ctx.signal,
            })
            .then((res) => res.Table)
            .catch((e) =>
              this.table
                .send(
                  new CreateTableCommand({
                    TableName: this.ctx.tableName,
                    AttributeDefinitions: [
                      {
                        AttributeName: "pk",
                        AttributeType: "S",
                      },
                      {
                        AttributeName: "sk",
                        AttributeType: "S",
                      },
                    ],
                    KeySchema: [
                      {
                        AttributeName: "pk",
                        KeyType: "HASH",
                      },
                      {
                        AttributeName: "sk",
                        KeyType: "RANGE",
                      },
                    ],
                    StreamSpecification: {
                      StreamEnabled: true,
                      StreamViewType: "NEW_AND_OLD_IMAGES",
                    },
                    BillingMode: "PAY_PER_REQUEST",
                    SSESpecification: {
                      Enabled: true,
                    },
                  }),
                  { abortSignal: this.ctx.signal }
                )
                .then((res) => res.TableDescription)
                .catch((e) => {
                  console.warn("Error creating table", e.message);
                  return retry(e);
                })
            )
        )
      )
        // TODO Assert table structure
        .pipe(filter((table) => !!table))
        .pipe(tap((table) => (this.tableArn = table.TableArn)))
        .pipe(share())
    );
  }

  get records$(): Observable<DynamoDBStreamEvent> {
    return this.shardIterators
      .pipe(
        switchMap((shardIterators) =>
          merge(
            ...shardIterators.map(({ tableArn, shardId, iterator }) =>
              from(
                this.client.send(
                  new GetRecordsCommand({ ShardIterator: iterator })
                )
              ).pipe(
                expand(({ Records = [], NextShardIterator }) => {
                  if (this.ctx.aborted) {
                    console.log("Aborted", { tableArn, shardId });
                    return [];
                  }
                  if (!NextShardIterator) {
                    console.log("Next Shard Iterator is null", {
                      tableArn,
                      shardId,
                    });
                    return [];
                  }
                  return of(null).pipe(
                    delayWhen(() =>
                      timer(Records.length ? 50 : 1000, asyncScheduler)
                    ),
                    switchMap(() =>
                      from(
                        this.client.send(
                          new GetRecordsCommand({
                            ShardIterator: NextShardIterator,
                          })
                        )
                      )
                    )
                  );
                }),
                tap(({ Records = [] }) => {
                  let sequence = Records.slice(-1)[0]?.dynamodb?.SequenceNumber;
                  if (sequence) sequence = `AFTER_SEQUENCE_NUMBER:${sequence}`;

                  this.stat$.next({
                    tableArn,
                    shardId,
                    records: Records.length,
                    sequence,
                    active: true,
                  });
                }),
                filter(({ Records = [] }) => !!Records.length),
                map(({ Records = [] }) => ({
                  Records: Records.map((record) => ({
                    ...record,
                    eventSourceARN: shardId,
                  })) as DynamoDBRecord[], // TODO: Fix types
                }))
              )
            )
          )
        )
      )
      .pipe(share());
  }

  private get tags(): Observable<
    Record<string, string | undefined> | undefined
  > {
    return this.arns
      .pipe(
        switchMap(({ tableArn }) =>
          from(
            this.table.send(
              new ListTagsOfResourceCommand({ ResourceArn: tableArn })
            )
          )
            .pipe(
              catchError((e) => {
                return of(undefined);
              })
            )
            .pipe(
              map((tags) => {
                if (!tags) {
                  return undefined;
                }
                return (tags.Tags || []).reduce((acc, { Key, Value }) => {
                  if (!Key) return acc;
                  acc[Key] = Value;
                  return acc;
                }, {} as Record<string, string | undefined>);
              })
            )
        )
      )
      .pipe(share());
  }

  private get arns(): Observable<{ tableArn?: string; streamArn?: string }> {
    return this.table$
      .pipe(
        switchMap((table) =>
          from(
            this.client.send(
              new ListStreamsCommand({ TableName: table.TableName })
            )
          ).pipe(map((streams) => ({ table, streams })))
        )
      )
      .pipe(
        map(({ table, streams }) => ({
          tableArn: table.TableArn,
          streamArn: streams.Streams?.[0].StreamArn,
        }))
      )
      .pipe(
        catchError((err) => {
          console.warn("Error getting ARNs", err.message);
          this.ctx.abort(err);
          return of({});
        })
      )
      .pipe(share());
  }

  private get shards(): Observable<Record<string, string>> {
    return combineLatest([this.arns, this.tags])
      .pipe(
        switchMap(([{ tableArn, streamArn }, tags]) =>
          interval(5000, asyncScheduler).pipe(
            startWith(0),
            switchMap(() =>
              from(
                this.client
                  .send(new DescribeStreamCommand({ StreamArn: streamArn }), {
                    abortSignal: this.ctx.signal,
                  })
                  .then((res) =>
                    (res.StreamDescription?.Shards || [])
                      .sort((a, b) => {
                        return Number(
                          BigInt(
                            b.SequenceNumberRange?.StartingSequenceNumber!
                          ) -
                            BigInt(
                              a.SequenceNumberRange?.StartingSequenceNumber!
                            )
                        );
                      })
                      .map(({ ShardId, SequenceNumberRange = {} }) => ({
                        shardId: ShardId,
                        sequence: tags
                          ? tags[ShardId!] ||
                            `AT_SEQUENCE_NUMBER:${SequenceNumberRange.StartingSequenceNumber}`
                          : "LATEST",
                        active: !SequenceNumberRange.EndingSequenceNumber,
                      }))
                      .reduce((acc, { shardId, sequence, active }) => {
                        if (!shardId) return acc;
                        acc[shardId] = {
                          tableArn: tableArn!,
                          sequence,
                          active,
                        };
                        return acc;
                      }, {} as Record<string, { tableArn: string; sequence: string; active: boolean }>)
                  )
              )
            )
          )
        ),
        tap((shards) => {
          const total = Object.values(shards).length;
          const active = Object.values(shards).filter(
            ({ active }) => active
          ).length;
          console.log(
            chalk.yellow(
              `Polling ${chalk.bold(active)} of ${chalk.bold(total)} shard(s)`
            )
          );
        }),
        map((shards) => {
          return Object.entries(shards).reduce((acc, [k, v]) => {
            if (!v) return acc;
            if (!v.active) return acc;
            acc[k] = v.sequence;
            return acc;
          }, {} as Record<string, string>);
        }),
        distinctUntilChanged((oldShards, newShards) => {
          if (!_.isEqual(oldShards, newShards)) {
            console.log("Shards changed", { oldShards, newShards });
            return false;
          }
          return true;
        })
      )
      .pipe(share());
  }

  private get shardIterators(): Observable<
    { tableArn: string; shardId: string; iterator: string }[]
  > {
    return combineLatest([this.shards, this.arns])
      .pipe(
        switchMap(([shards, { tableArn, streamArn }]) => {
          console.log("Getting Shard Iterators", {
            shards,
            tableArn,
            streamArn,
          });
          return forkJoin(
            Object.entries(shards).map(([shardId, sequence]) => {
              const [type, seq] = sequence.split(":");
              return from(
                this.client.send(
                  new GetShardIteratorCommand({
                    ShardId: shardId,
                    ShardIteratorType: type as ShardIteratorType,
                    StreamArn: streamArn,
                    SequenceNumber: seq,
                  })
                )
              ).pipe(
                map((res) => ({
                  tableArn: tableArn!,
                  shardId,
                  iterator: res.ShardIterator!,
                })),
                tap(() => {
                  this.stat$.next({
                    active: true,
                    tableArn: tableArn!,
                    shardId,
                    sequence,
                  });
                }),
                catchError((err) => {
                  if (err.name === "ResourceNotFoundException") {
                    console.log("Deactivating shard", shardId);
                    this.stat$.next({
                      active: false,
                      tableArn: tableArn!,
                      shardId,
                      sequence,
                    });
                    return of({ tableArn: tableArn!, shardId, iterator: "" });
                  }
                  return throwError(() => err);
                })
              );
            })
          );
        }),
        map((shardIterators) =>
          shardIterators.filter(({ iterator }) => !!iterator)
        )
      )
      .pipe(share());
  }
}
