import { first, from, Observable, of, skipUntil, switchMap, take } from "rxjs";
import util from "util";
import { random } from "timeflake";

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
  abstract stream(): Observable<Stored>; // TODO Change to *stream
  abstract repr(): string;

  protected _id?: string;

  constructor(
    protected consistency: Consistency,
    protected serializer: Serializer<T>,
    protected signal: AbortSignal
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
    return this.serializer.deserialize(value.data);
  }

  public persist(data: T): Observable<Stored> {
    return of({
      partition: this.serializer.partition(data),
      timeflake: random().base62,
      hash: this.serializer.hash(data),
      data: this.serializer.serialize(data),
      createdMs: Date.now(),
    } as Stored).pipe(
      switchMap((stored) => {
        if (this.consistency === "none") {
          return from(this.put(stored));
        }

        if (this.consistency === "weak") {
          return from(this.put(stored).then(() => this.get(stored)));
        }

        return this.stream().pipe(
          skipUntil(this.put(stored)),
          first((streamed) => {
            return (
              streamed.partition === stored.partition &&
              streamed.timeflake === stored.timeflake &&
              streamed.hash === stored.hash
            );
          }),
          take(1)
        );
      })
    );
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
