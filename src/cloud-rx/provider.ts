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
import { random } from "timeflake";
import { Timeflake } from "timeflake/dist/timeflake";
import { BN } from "bn.js";

export type Expireable = {
  expires?: number; // In seconds
};

export type Stored = {
  id: string;
  flake: string;
  hash: string;
  data: string;
  expires?: number;
};

export type Consistency = "strong" | "weak" | "none";

export interface Serializer<T> {
  hash: (value: T) => string;
  serialize: (value: T) => string;
  deserialize: (value: string) => T;
}

export abstract class Provider<T> implements TimestampProvider {
  constructor(
    protected id: string,
    protected serializer: Serializer<T>,
    protected signal: AbortSignal
  ) {}

  now(): number {
    throw new Error("Method not implemented.");
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

  protected abstract oldest(): Promise<Stored>;

  protected abstract newest(): Promise<Stored>;

  protected abstract all(): Promise<Stored[]>;

  protected abstract put(item: Stored): Promise<Stored>;

  protected abstract get(
    flake: string,
    consistency: Consistency
  ): Promise<Stored>;

  protected abstract stream(): Observable<Stored>;

  public persist(data: T, consistency: Consistency = "strong"): Observable<T> {
    return of({
      id: this.id,
      flake: random().base62,
      hash: this.serializer.hash(data),
      data: this.serializer.serialize(data),
      // expires: data.expires,
    } as Stored).pipe(
      switchMap((item) => this.put(item)),
      switchMap((stored) => {
        if (consistency === "none") {
          return of(stored);
        } else if (consistency === "weak") {
          return from(this.get(stored.flake, "weak"));
        } else {
          return forkJoin([
            this.stream().pipe(observeOn(asyncScheduler)),
            from(this.get(stored.flake, "weak")).pipe(
              observeOn(asyncScheduler)
            ),
          ]).pipe(
            filter(
              ([streamed, stored]) =>
                streamed.id === this.id && streamed.flake === stored.flake
            ),
            map(([streamed]) => streamed)
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
