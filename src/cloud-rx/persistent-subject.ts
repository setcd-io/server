import {
  ReplaySubject,
  asyncScheduler,
  filter,
  Observable,
  OperatorFunction,
  SchedulerLike,
  Subject,
  Subscriber,
  Subscription,
  timer,
  TimestampProvider,
  observeOn,
  tap,
  from,
  map,
  of,
  switchMap,
  windowTime,
  toArray,
  concatMap,
  mergeMap,
  share,
  Observer,
  Operator,
} from "rxjs";
import { Provider } from "./provider";

const timestampProvider: TimestampProvider = {
  now: () => Date.now(),
};

export interface Expireable {
  expires?: number; // In seconds
}

export class PersistentSubject<T extends Expireable> extends Subject<T> {
  private provider: Observable<Provider<unknown>>;
  private _expired: Subject<T> = new Subject<T>();
  private _incoming: Subject<T> = new Subject<T>();
  private _buffer: ReplaySubject<T>;
  private timestampProvider: TimestampProvider;
  private signal: AbortSignal | undefined;
  private subscriptions: Subscription[] = [];

  public readonly expired = this._expired.pipe(share());

  constructor(
    provider: Observable<Provider<unknown>>,
    config: {
      bufferSize?: number;
      windowTime?: number;
    },
    opts?: {
      signal?: AbortSignal;
      scheduler?: SchedulerLike;
      timestampProvider?: TimestampProvider;
    }
  ) {
    super();
    this.provider = provider;

    this._buffer = new ReplaySubject<T>(
      config.bufferSize || Infinity,
      config.windowTime || Infinity,
      opts?.timestampProvider || timestampProvider
    );

    if (opts?.timestampProvider) {
      this.timestampProvider = opts.timestampProvider;
    } else {
      this.timestampProvider = timestampProvider;
    }

    if (opts?.signal) {
      this.signal = opts.signal;
      this.signal.addEventListener("abort", () => {
        this.complete();
      });
    }

    this.pipe = this._buffer.pipe.bind(this._buffer);
    this.subscribe = this._buffer.subscribe.bind(this._buffer);

    this.init();
  }

  private isExpired(value: T): boolean {
    if (value.expires) {
      const now = this.timestampProvider.now();
      return value.expires < now;
    }
    return false;
  }

  private init(): void {
    this.provider
      .pipe(
        map(() => {
          this.subscriptions.push(
            this._incoming.pipe(this.persist()).subscribe({
              next: (value) => {
                if (!this.isExpired(value)) {
                  this._buffer.next(value);
                }
              },
              error: (err) => this.error(err),
              complete: () => this.complete(),
            })
          );
        })
      )
      .subscribe();
  }

  private persist(): OperatorFunction<T, T> {
    return (source: Observable<T>): Observable<T> => {
      return new Observable<T>((subscriber) => {
        const subscription = source
          .pipe(
            // TODO: Write to DynamoDB
            // TODO: Wait for item to come in from DDB Stream before emitting
            switchMap((value) => {
              if (!value.expires) {
                return of([value]);
              }
              const now = this.timestampProvider.now();
              const expires = Math.max(0, value.expires * 1000 - now);
              const delayed = timer(expires, asyncScheduler).pipe(
                map(() => {
                  this._expired.next(value);
                  return value;
                })
              );
              return of(from([value]), delayed);
            })
          )
          .pipe(mergeMap((sources) => from(sources)))
          .subscribe({
            next(source) {
              subscriber.next(source);
            },
            error(err) {
              subscriber.error(err);
            },
            complete() {
              subscriber.complete();
            },
          });

        return () => {
          subscription.unsubscribe();
        };
      });
    };
  }

  override next(value: T): void {
    this._incoming.next(value);
  }

  override complete(): void {
    this.subscriptions.forEach((sub) => sub.unsubscribe());
    this.subscriptions = [];
    this._buffer.complete();
    this._incoming.complete();
    super.complete();
  }

  override unsubscribe(): void {
    this.subscriptions.forEach((sub) => sub.unsubscribe());
    this.subscriptions = [];
    this._buffer.unsubscribe();
    this._incoming.unsubscribe();
    super.unsubscribe();
  }

  override error(err: any): void {
    this.subscriptions.forEach((sub) => sub.unsubscribe());
    this.subscriptions = [];
    this._buffer.error(err);
    this._incoming.error(err);
    super.error(err);
  }

  // override pipe(): Observable<T>;
  // override pipe<A>(op1: OperatorFunction<T, A>): Observable<A>;
  // override pipe<A, B>(
  //   op1: OperatorFunction<T, A>,
  //   op2: OperatorFunction<A, B>
  // ): Observable<B>;
  // override pipe<A, B, C>(
  //   op1: OperatorFunction<T, A>,
  //   op2: OperatorFunction<A, B>,
  //   op3: OperatorFunction<B, C>
  // ): Observable<C>;
  // override pipe<A, B, C, D>(
  //   op1: OperatorFunction<T, A>,
  //   op2: OperatorFunction<A, B>,
  //   op3: OperatorFunction<B, C>,
  //   op4: OperatorFunction<C, D>
  // ): Observable<D>;
  // override pipe<A, B, C, D, E>(
  //   op1: OperatorFunction<T, A>,
  //   op2: OperatorFunction<A, B>,
  //   op3: OperatorFunction<B, C>,
  //   op4: OperatorFunction<C, D>,
  //   op5: OperatorFunction<D, E>
  // ): Observable<E>;
  // override pipe<A, B, C, D, E, F>(
  //   op1: OperatorFunction<T, A>,
  //   op2: OperatorFunction<A, B>,
  //   op3: OperatorFunction<B, C>,
  //   op4: OperatorFunction<C, D>,
  //   op5: OperatorFunction<D, E>,
  //   op6: OperatorFunction<E, F>
  // ): Observable<F>;
  // override pipe<A, B, C, D, E, F, G>(
  //   op1: OperatorFunction<T, A>,
  //   op2: OperatorFunction<A, B>,
  //   op3: OperatorFunction<B, C>,
  //   op4: OperatorFunction<C, D>,
  //   op5: OperatorFunction<D, E>,
  //   op6: OperatorFunction<E, F>,
  //   op7: OperatorFunction<F, G>
  // ): Observable<G>;
  // override pipe<A, B, C, D, E, F, G, H>(
  //   op1: OperatorFunction<T, A>,
  //   op2: OperatorFunction<A, B>,
  //   op3: OperatorFunction<B, C>,
  //   op4: OperatorFunction<C, D>,
  //   op5: OperatorFunction<D, E>,
  //   op6: OperatorFunction<E, F>,
  //   op7: OperatorFunction<F, G>,
  //   op8: OperatorFunction<G, H>
  // ): Observable<H>;
  // override pipe<A, B, C, D, E, F, G, H, I>(
  //   op1: OperatorFunction<T, A>,
  //   op2: OperatorFunction<A, B>,
  //   op3: OperatorFunction<B, C>,
  //   op4: OperatorFunction<C, D>,
  //   op5: OperatorFunction<D, E>,
  //   op6: OperatorFunction<E, F>,
  //   op7: OperatorFunction<F, G>,
  //   op8: OperatorFunction<G, H>,
  //   op9: OperatorFunction<H, I>
  // ): Observable<I>;
  // override pipe<A, B, C, D, E, F, G, H, I>(
  //   op1: OperatorFunction<T, A>,
  //   op2: OperatorFunction<A, B>,
  //   op3: OperatorFunction<B, C>,
  //   op4: OperatorFunction<C, D>,
  //   op5: OperatorFunction<D, E>,
  //   op6: OperatorFunction<E, F>,
  //   op7: OperatorFunction<F, G>,
  //   op8: OperatorFunction<G, H>,
  //   op9: OperatorFunction<H, I>,
  //   ...operations: OperatorFunction<any, any>[]
  // ): Observable<unknown>;

  // override pipe(...operations: OperatorFunction<any, any>[]): Observable<any> {
  //   const injected: OperatorFunction<any, any>[] = [
  //     concatMap((v: any) => this.provider.init().pipe(map(() => v))),
  //   ];

  //   return (this._buffer.pipe as Function).apply(this._buffer, [
  //     ...injected,
  //     ...operations,
  //   ]) as Observable<any>;
  // }
}
