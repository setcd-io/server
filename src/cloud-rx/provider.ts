import {
  asyncScheduler,
  combineLatest,
  defer,
  filter,
  forkJoin,
  from,
  map,
  Observable,
  observeOn,
  of,
  switchMap,
  tap,
  TimestampProvider,
} from "rxjs";
import { ulid } from "ulid";
import util from "util";

export type Expireable = {
  expires?: number; // In seconds
};

export type Stored = {
  serial: string;
  source: string;
  expires?: number;
  data: string;
};

export type Consistency = "strong" | "weak" | "none";

export const timestampProvider: TimestampProvider = {
  now: () => Date.now(),
};

export interface Serializer<T> {
  serialize: (value: T) => string;
  deserialize: (value: string) => T;
}

export abstract class Provider<T extends Expireable> {
  private timestampProvider: TimestampProvider = timestampProvider;

  constructor(
    protected id: string,
    protected serializer: Serializer<T>,
    protected signal: AbortSignal
  ) {}

  public withTimestampProvider(
    timestampProvider: TimestampProvider
  ): Provider<T> {
    this.timestampProvider = timestampProvider;
    return this;
  }

  public withId(id: string): this {
    this.id = `${this.id}-${id}`;
    return this;
  }

  abstract init(): Observable<this>;

  protected serialize(value: T): string {
    return this.serializer.serialize(value);
  }

  protected deserialize(value: string): T {
    return this.serializer.deserialize(value);
  }

  private isExpired(value: Stored): boolean {
    if (!!value.expires) {
      const now = this.timestampProvider.now();
      return value.expires < now;
    }
    return false;
  }

  protected abstract oldest(): Promise<Stored>;

  protected abstract newest(): Promise<Stored>;

  protected abstract all(): Promise<Stored[]>;

  protected abstract put(item: Stored): Promise<Stored>;

  protected abstract get(
    sequence: string,
    source: string,
    consistency: Consistency
  ): Promise<Stored>;

  protected abstract stream(since: Date): Observable<Stored>;

  public persist(data: T, consistency: Consistency = "strong"): Observable<T> {
    return of({
      serial: ulid(), // Lexicographically sortable
      source: this.id,
      data: this.serializer.serialize(data),
      expires: data.expires,
    } as Stored).pipe(
      switchMap((item) => this.put(item)),
      switchMap((stored) => {
        if (consistency === "none") {
          return of(stored);
        } else if (consistency === "weak") {
          return from(this.get(stored.serial, stored.source, "weak"));
        } else {
          return forkJoin([
            this.stream(new Date(this.timestampProvider.now())).pipe(
              observeOn(asyncScheduler)
            ),
            from(this.get(stored.serial, stored.source, "weak")).pipe(
              observeOn(asyncScheduler)
            ),
          ]).pipe(
            filter(
              ([streamed, stored]) =>
                streamed.serial === stored.serial &&
                streamed.source === stored.source
            ),
            map(([_, stored]) => stored)
          );
        }
      }),
      map((stored) => {
        return this.serializer.deserialize(stored.data);
      })
    );
  }

  public expire(data: T, consistency: Consistency = "strong"): Observable<T> {
    throw new Error("Not implemented");
  }

  toString(): string {
    return `${this.repr()} [CloudRx{ id=${this.id} }]`;
  }

  abstract repr(): string;

  [util.inspect.custom](
    depth: number,
    options: util.InspectOptionsStylized
  ): string {
    // `this` is the instance, so this.toString() is your override
    return this.toString();
  }
}
