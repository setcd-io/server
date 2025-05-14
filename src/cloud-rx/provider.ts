import {
  asyncScheduler,
  filter,
  forkJoin,
  from,
  map,
  Observable,
  observeOn,
  of,
  switchMap,
} from "rxjs";
import util from "util";
import { random } from "timeflake";

export type Stored = {
  id: string;
  flake: string;
  hash: string;
  data: string;
  createdMs: number;
  expires?: number;
};

export type Consistency = "strong" | "weak" | "none";

export interface Serializer<T> {
  hash: (value: T) => string;
  serialize: (value: T) => string;
  deserialize: (value: string) => T;
}

export abstract class Provider<T> {
  constructor(
    protected id: string,
    protected serializer: Serializer<T>,
    protected signal: AbortSignal
  ) {}

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

  public convert(value: Stored): T {
    return {
      id: value.id,
      flake: value.flake,
      hash: value.hash,
      data: this.serializer.deserialize(value.data),
      createdMs: value.createdMs,
    } as T;
  }

  protected abstract put(item: Stored): Promise<Stored>;

  protected abstract get(
    flake: string,
    consistency: Consistency
  ): Promise<Stored>;

  public abstract all(): Observable<Stored>;

  public abstract latest(): Observable<Stored>;

  public persist(data: T, consistency: Consistency = "strong"): Observable<T> {
    return of({
      id: this.id,
      flake: random().base62,
      hash: this.serializer.hash(data),
      data: this.serializer.serialize(data),
      createdMs: Date.now(),
    } as Stored).pipe(
      switchMap((item) => this.put(item)),
      switchMap((stored) => {
        if (consistency === "none") {
          return of(stored);
        } else if (consistency === "weak") {
          return from(this.get(stored.flake, "weak"));
        } else {
          return forkJoin([
            this.latest().pipe(observeOn(asyncScheduler)),
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
