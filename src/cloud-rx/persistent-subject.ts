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
import { Consistency, Expireable, Provider } from "./provider";
import _ from "lodash";

export class PersistentSubject<T> extends ReplaySubject<T> {
  private consistency: Consistency;
  private _incoming: Subject<T> = new Subject<T>();

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
      timestampProvider?: TimestampProvider;
      consistency?: Consistency;
    }
  ) {
    super(config.bufferSize, config.windowTime, provider);

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

    this.subscriptions.push(
      this.incoming$
        .pipe(switchMap((value) => provider.persist(value, this.consistency))) // TODO: make strong
        .subscribe({
          next: (value) => super.next(value),
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
    this._incoming.complete();
    super.complete();
  }

  override unsubscribe(): void {
    this.subscriptions.forEach((sub) => sub.unsubscribe());
    this.subscriptions = [];
    this._incoming.unsubscribe();
    super.unsubscribe();
  }

  override error(err: any): void {
    this.subscriptions.forEach((sub) => sub.unsubscribe());
    this.subscriptions = [];
    this._incoming.error(err);
    super.error(err);
  }
}
