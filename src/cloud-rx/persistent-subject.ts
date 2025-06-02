import {
  Subject,
  Subscription,
  switchMap,
  Subscriber,
  from,
  filter,
  map,
  Observable,
  OperatorFunction,
  concatMap,
  firstValueFrom,
  toArray,
  shareReplay,
  asyncScheduler,
  ReplaySubject,
} from "rxjs";
import { Consistency, Provider, Stored } from "./provider";
import _ from "lodash";
import { tailFrom } from "./util";

export class PersistentSubject<T> extends Subject<T> {
  private signal: AbortSignal | undefined;
  private subscriptions: Subscription[] = [];

  private _incoming = new Subject<T>();
  private observe$: Observable<Stored>;

  constructor(
    private readonly provider: Provider<T>,
    opts?: {
      signal?: AbortSignal;
      consistency?: Consistency; // TODO: provider.withConsistency: makes new provider
    }
  ) {
    super();

    this.observe$ = provider
      .observe()
      .pipe(
        shareReplay({ refCount: false, scheduler: asyncScheduler })
      );

    // Force initial subscription to start the stream
    this.subscriptions.push(this.observe$.subscribe());

    if (opts?.signal) {
      this.signal = opts.signal;
      this.signal.addEventListener("abort", () => {
        this.complete();
      });
    }

    this.subscriptions.push(
      this._incoming
        .pipe(concatMap((value) => from(provider.persist(value))))
        .subscribe()
    );
  }

  async all(): Promise<T[]> {
    return tailFrom(
      this.observe$.pipe(map((item) => this.provider.convert(item as Stored)))
    );
  }

  override next(value: T): void {
    this._incoming.next(value);
  }

  protected _subscribe(subscriber: Subscriber<T>): Subscription {
    return this.observe$.subscribe({
      next: (item) => {
        subscriber.next(this.provider.convert(item as Stored));
      },
      error: (err) => {
        subscriber.error(err);
      },
      complete: () => {
        subscriber.complete();
      },
    });
  }

  complete(): void {
    this.subscriptions.forEach((sub) => sub.unsubscribe());
    this.subscriptions = [];
    this._incoming.complete();
    super.complete();
  }

  unsubscribe(): void {
    this.subscriptions.forEach((sub) => sub.unsubscribe());
    this.subscriptions = [];
    this._incoming.unsubscribe();
    super.unsubscribe();
  }

  error(err: any): void {
    this.subscriptions.forEach((sub) => sub.unsubscribe());
    this.subscriptions = [];
    this._incoming.error(err);
    super.error(err);
  }
}
