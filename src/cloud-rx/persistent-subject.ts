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
} from "rxjs";

export interface Expireable {
  expires?: number; // In seconds
}

const timestampProvider: TimestampProvider = {
  now: () => Date.now(),
};

export class PersistentSubject<T extends Expireable> extends Subject<T> {
  private _expired: Subject<T> = new Subject<T>();
  private _incoming: Subject<T> = new Subject<T>();
  private _buffer: ReplaySubject<T>;
  private timestampProvider: TimestampProvider;
  private signal: AbortSignal | undefined;
  private subscriptions: Subscription[] = [];

  public readonly expired = this._expired.pipe(share());

  constructor(
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

    this.subscriptions.push(this.setup());
    this.subscribe = this._buffer.subscribe.bind(this._buffer);
    this.pipe = this._buffer.pipe.bind(this._buffer);
  }

  private isExpired(value: T): boolean {
    if (value.expires) {
      const now = this.timestampProvider.now();
      return value.expires < now;
    }
    return false;
  }

  private setup(): Subscription {
    return from(this.backfill())
      .pipe(
        map(() => {
          const incoming = this._incoming.pipe(this.persist()).subscribe({
            next: (value) => {
              if (!this.isExpired(value)) {
                this._buffer.next(value);
              }
            },
            error: (err) => this.error(err),
            complete: () => this.complete(),
          });

          return incoming;
        })
      )
      .subscribe((incoming) => this.subscriptions.push(incoming));
  }

  private backfill(): Observable<undefined> {
    console.log("!!! TODO BACKFILL !!!");
    // TODO: re-emit everthing in the buffer
    // TODO: preset expired delays
    return of(undefined).pipe();
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

  next(value: T): void {
    this._incoming.next(value);
  }

  complete(): void {
    this.subscriptions.forEach((sub) => sub.unsubscribe());
    this.subscriptions = [];
    this._buffer.complete();
    this._incoming.complete();
    super.complete();
  }

  unsubscribe(): void {
    this.subscriptions.forEach((sub) => sub.unsubscribe());
    this.subscriptions = [];
    this._buffer.unsubscribe();
    this._incoming.unsubscribe();
    super.unsubscribe();
  }

  error(err: any): void {
    this.subscriptions.forEach((sub) => sub.unsubscribe());
    this.subscriptions = [];
    this._buffer.error(err);
    this._incoming.error(err);
    super.error(err);
  }
}
