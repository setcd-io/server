import {
  ReplaySubject,
  asyncScheduler,
  Subject,
  Subscription,
  timer,
  TimestampProvider,
  map,
  switchMap,
  share,
  observeOn,
  filter,
} from "rxjs";
import {
  Consistency,
  Expireable,
  Provider,
  timestampProvider,
} from "./provider";
import _ from "lodash";

export class PersistentSubject<T extends Expireable> extends Subject<T> {
  public readonly provider: Provider<T>;
  private consistency: Consistency;
  private _expired: Subject<T> = new Subject<T>();
  private _incoming: Subject<T> = new Subject<T>();
  private _buffer: ReplaySubject<T>;

  private incoming$ = this._incoming.pipe(observeOn(asyncScheduler), share());
  private timestampProvider: TimestampProvider;
  private signal: AbortSignal | undefined;
  private subscriptions: Subscription[] = [];

  public readonly expired = this._expired.pipe(share());

  constructor(
    provider: Provider<T>,
    private readonly config: {
      bufferSize?: number;
      windowTime?: number;
    },
    private readonly opts?: {
      signal?: AbortSignal;
      timestampProvider?: TimestampProvider;
      consistency?: Consistency;
    }
  ) {
    super();
    this.provider = provider.withTimestampProvider(
      opts?.timestampProvider || timestampProvider
    );

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

    if (opts?.consistency) {
      this.consistency = opts.consistency;
    } else {
      this.consistency = "weak"; // TODO: make strong
    }

    this.pipe = this._buffer.pipe.bind(this._buffer);
    this.subscribe = this._buffer.subscribe.bind(this._buffer); // TODO this should start up a DDB stream

    this.subscriptions.push(
      this.incoming$
        .pipe(switchMap((value) => provider.persist(value, this.consistency))) // TODO: make strong
        .subscribe({
          next: (value) => this._buffer.next(value),
          error: (err) => this.error(err),
          complete: () => this.complete(),
        })
    );

    this.subscriptions.push(
      this._buffer
        .pipe(
          filter((value) => !!value.expires),
          switchMap((value) => {
            value = _.cloneDeep(value);
            const now = this.timestampProvider.now();
            const expires = Math.max(0, value.expires! * 1000 - now);
            return timer(expires, asyncScheduler).pipe(map(() => value));
          })
        )
        .pipe(switchMap((value) => provider.expire(value, this.consistency)))
        .subscribe({
          next: (value) => this._expired.next(value),
          error: (err) => this.error(err),
          complete: () => this.complete(),
        })
    );
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
