import { Subject, Subscription, switchMap, Subscriber, from } from "rxjs";
import { Consistency, Provider } from "./provider";
import _ from "lodash";
import { subscribe } from "./util";

export class PersistentSubject<T> extends Subject<T> {
  private signal: AbortSignal | undefined;
  private subscriptions: Subscription[] = [];

  private _incoming = new Subject<T>();

  constructor(
    private readonly provider: Provider<T>,
    private readonly opts?: {
      signal?: AbortSignal;
      consistency?: Consistency; // TODO: provider.withConsistency: makes new provider
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
        .pipe(switchMap((value) => from(provider.persist(value))))
        .subscribe()
    );
  }

  async all(partition: string): Promise<T[]> {
    // TODO: delete old items
    return this.provider
      .all({ partition })
      .then((items) => items.map((item) => this.provider.convert(item)));
  }

  override next(value: T): void {
    this._incoming.next(value);
  }

  protected _subscribe(subscriber: Subscriber<T>): Subscription {
    return this.provider.observeLatest().subscribe({
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
