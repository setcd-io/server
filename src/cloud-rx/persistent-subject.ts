import {
  Subject,
  Subscription,
  TimestampProvider,
  switchMap,
  Subscriber,
  tap,
} from "rxjs";
import { Consistency, Provider, Stored } from "./provider";
import _ from "lodash";

export class PersistentSubject<T>
  extends Subject<T>
  implements TimestampProvider
{
  private _last?: Stored;

  private signal: AbortSignal | undefined;
  private subscriptions: Subscription[] = [];

  private _incoming = new Subject<T>();

  constructor(
    private readonly provider: Provider<T>,
    private readonly opts?: {
      signal?: AbortSignal;
      consistency?: Consistency;
    }
  ) {
    super();

    if (opts?.signal) {
      this.signal = opts.signal;
      this.signal.addEventListener("abort", () => {
        this.complete();
      });
    }

    this.subscriptions.push(
      this._incoming
        .pipe(
          switchMap((value) => provider.persist(value)),
          tap((value) => (this._last = value))
        )
        .subscribe()
    );
  }

  now(): number {
    return this._last?.createdMs || new Date().getTime();
  }

  async all(partition: string): Promise<T[]> {
    return this.provider
      .all({ partition })
      .then((items) => items.map((item) => this.provider.convert(item)));
  }

  override next(value: T): void {
    this._incoming.next(value);
  }

  protected _subscribe(subscriber: Subscriber<T>): Subscription {
    return this.provider.stream().subscribe({
      next: (item) => {
        subscriber.next(this.provider.convert(item));
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
