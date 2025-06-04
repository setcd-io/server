import {
  asapScheduler,
  asyncScheduler,
  AsyncSubject,
  catchError,
  combineLatest,
  filter,
  first,
  firstValueFrom,
  forkJoin,
  from,
  lastValueFrom,
  map,
  Observable,
  observeOn,
  of,
  ReplaySubject,
  share,
  shareReplay,
  skipUntil,
  switchMap,
  take,
  tap,
  timeout,
} from "rxjs";
import util from "util";
import { random } from "timeflake";
import EventEmitter from "events";

export type StoredPartition = {
  partition: string;
};

export type StoredKey = StoredPartition & {
  timeflake: string;
};

export type Stored = StoredKey & {
  hash: string;
  data: string;
  createdMs: number;
  expires?: number;
};

export type Consistency = "strong" | "weak" | "none";

export interface Serializer<T> {
  partition: (value: T) => string;
  hash: (value: T) => string;
  serialize: (value: T) => string;
  deserialize: (value: string) => T;
}

export abstract class Provider<T> {
  abstract init(id: string): Promise<this>;
  abstract put(item: Stored): Promise<Stored>;
  abstract get(key: StoredKey): Promise<Stored>;
  abstract repr(): string;
  abstract observe(): Observable<Stored>;

  protected _id?: string;

  constructor(
    public readonly signal: AbortSignal,
    protected consistency: Consistency = "strong",
    protected serializer?: Serializer<T>
  ) {}

  get id(): string {
    if (!this._id) {
      throw new Error(
        "Provider ID is not available yet. Please call init() first."
      );
    }
    return this._id;
  }

  public convert(value: Stored): T {
    if (!this.serializer) {
      throw new Error("Serializer is not defined");
    }
    return this.serializer.deserialize(value.data);
  }

  public async persist(data: T): Promise<Stored> {
    if (!this.serializer) {
      throw new Error("Serializer is not defined");
    }

    const stored: Stored = {
      partition: this.serializer.partition(data),
      timeflake: random().base62,
      hash: this.serializer.hash(data),
      data: this.serializer.serialize(data),
      createdMs: Date.now(),
    };

    if (this.consistency === "none") {
      return this.put(stored);
    }

    if (this.consistency === "weak") {
      return this.put(stored).then(() => this.get(stored));
    }

    return lastValueFrom(
      this.observe()
        .pipe(skipUntil(this.put(stored)))
        .pipe(
          filter((item) => {
            return (
              item.partition === stored.partition &&
              item.timeflake === stored.timeflake &&
              item.hash === stored.hash
            );
          }),
          take(1)
        )
    );

    // return this.put(stored).then(() => this.get(stored));

    // if (!this.tail$) {
    //   this.tail$ = this.tail("ALL");
    //   const test = this.tail$.subscribe((item) => {
    //     console.log("!!! tail", item);
    //   });
    // }
    // if (!this.latest$) {
    //   this.latest$ = .pipe(
    //     shareReplay({
    //       windowTime: 1000,
    //       refCount: false,
    //     })
    //   );

    //   const test = this.latest$.subscribe((item) => {
    //     console.log("!!! Latest", item);
    //   });
    // }

    // console.log("!!! Persisting");
    // await this.put(stored);
    // console.log("!!! Persisted");

    // for await (const streamed of observe(this.tail$)) {
    //   console.log("!!! Streamed", streamed);
    //   if (
    //     streamed.partition === stored.partition &&
    //     streamed.timeflake === stored.timeflake &&
    //     streamed.hash === stored.hash
    //   ) {
    //     return streamed as Stored;
    //   }
    // }

    // return await firstValueFrom(
    //   this.tail("LATEST")
    //     .pipe(skipUntil(this.put(stored)))
    //     .pipe(
    //       filter((item) => {
    //         console.log("!!! Filtering", item);
    //         return true;
    //       })
    //     )
    // );

    // throw new Error("Consistency error: Item not streamed");
  }

  toString(): string {
    return `${this.repr()} [CloudRx{ id=${this.id} }]`;
  }

  [util.inspect.custom](
    depth: number,
    options: util.InspectOptionsStylized
  ): string {
    // `this` is the instance, so this.toString() is your override
    return this.toString();
  }
}
