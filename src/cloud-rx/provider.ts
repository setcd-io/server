import {
  asyncScheduler,
  combineLatest,
  filter,
  first,
  forkJoin,
  from,
  map,
  Observable,
  observeOn,
  of,
  share,
  shareReplay,
  skipUntil,
  switchMap,
  take,
  tap,
} from "rxjs";
import util from "util";
import { random } from "timeflake";
import { observe } from "./util";

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
  abstract oldest(partition: StoredPartition): Promise<Stored | undefined>;
  abstract all(partition: StoredPartition): Promise<Stored[]>;
  abstract observeAll(): Observable<Stored>;
  abstract observeLatest(): Observable<Stored>;
  abstract repr(): string;

  protected _id?: string;
  private latest$?: Observable<Stored>;

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

    asyncScheduler.schedule(() => {
      this.put(stored);
    });

    if (!this.latest$) {
      this.latest$ = this.observeLatest().pipe(
        // share()
        shareReplay({
          // scheduler: asyncScheduler,
          windowTime: 1000,
          refCount: false,
        })
      );
    }

    let iterations = 0;
    for await (const streamed of observe(this.latest$)) {
      iterations++;
      if (
        streamed.partition === stored.partition &&
        streamed.timeflake === stored.timeflake &&
        streamed.hash === stored.hash
      ) {
        return streamed;
      }
    }

    throw new Error("Consistency error: Item not streamed");
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
