import {
  ReplaySubject,
  asyncScheduler,
  Subject,
  Subscription,
  TimestampProvider,
  switchMap,
  share,
  observeOn,
} from "rxjs";
import { Consistency, Provider, Stored } from "./provider";
import _ from "lodash";

export class PersistentSubject<T>
  extends Subject<T>
  implements TimestampProvider
{
  private consistency: Consistency;
  private _buffer: ReplaySubject<T>;
  private _incoming: Subject<T> = new Subject<T>();
  private _last?: Stored;

  private incoming$ = this._incoming.pipe(observeOn(asyncScheduler), share());
  private signal: AbortSignal | undefined;
  private subscriptions: Subscription[] = [];

  constructor(
    provider: Provider<T>,
    private readonly config: {
      bufferSize?: number;
      windowTime?: number;
    },
    private readonly opts?: {
      signal?: AbortSignal;
      consistency?: Consistency;
    }
  ) {
    super();

    this._buffer = new ReplaySubject<T>(
      config.bufferSize || Infinity,
      config.windowTime || Infinity,
      this
    );

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
    this.subscribe = this._buffer.subscribe.bind(this._buffer);

    this.subscriptions.push(
      this.incoming$
        .pipe(switchMap((value) => provider.persist(value, this.consistency)))
        .subscribe({
          next: (value) => {
            this._buffer.next(value);
          },
          error: (err) => this.error(err),
          complete: () => this.complete(),
        })
    );

    this.subscriptions.push(
      provider.all().subscribe({
        next: (item) => {
          this._last = item;
          this._buffer.next(provider.convert(item));
        },
        error: (err) => this.error(err),
      })
    );
  }

  now(): number {
    return this._last?.createdMs || new Date().getTime();
  }

  next(value: T): void {
    this._incoming.next(value);
  }

  complete(): void {
    this.subscriptions.forEach((sub) => sub.unsubscribe());
    this.subscriptions = [];
    this._incoming.complete();
    this._buffer.complete();
    super.complete();
  }

  unsubscribe(): void {
    this.subscriptions.forEach((sub) => sub.unsubscribe());
    this.subscriptions = [];
    this._incoming.unsubscribe();
    this._buffer.unsubscribe();
    super.unsubscribe();
  }

  error(err: any): void {
    this.subscriptions.forEach((sub) => sub.unsubscribe());
    this.subscriptions = [];
    this._incoming.error(err);
    this._buffer.error(err);
    super.error(err);
  }
}
