import {
  Subject,
  Subscription,
  from,
  mergeMap,
  shareReplay,
  asyncScheduler,
  ReplaySubject,
} from "rxjs";
import { Consistency, Provider, Stored } from "../providers/provider";

export class PersistentSubject<T> extends ReplaySubject<T> {
  private signal: AbortSignal | undefined;
  private subscriptions: Subscription[] = [];
  private _incoming = new Subject<T>();
  private _seenHashes = new Set<string>(); // Track items to prevent duplicates

  constructor(
    private readonly provider: Provider<T>,
    opts?: {
      signal?: AbortSignal;
      consistency?: Consistency;
      bufferSize?: number;
    }
  ) {
    // Initialize ReplaySubject with optional buffer size
    super(opts?.bufferSize);

    if (opts?.signal) {
      this.signal = opts.signal;
      this.signal.addEventListener("abort", () => {
        this.complete();
      });
    }

    this.initializeStreams();
  }

  private initializeStreams(): void {
    // Use mergeMap for concurrent persistence instead of concatMap for sequential
    this.subscriptions.push(
      this._incoming
        .pipe(
          mergeMap((value) => from(this.provider.persist(value)), 5) // Allow up to 5 concurrent persistence operations
        )
        .subscribe({
          error: (err) => {
            console.error('Persistence error:', err);
            // Don't fail the entire stream on persistence errors
          }
        })
    );

    // Load ALL items from provider (existing + new) and add to ReplaySubject buffer
    // Use deduplication to prevent adding the same item twice
    this.subscriptions.push(
      this.provider
        .observe()
        .subscribe({
          next: (stored) => {
            // Simple deduplication check using hash
            if (!this._seenHashes.has(stored.hash)) {
              this._seenHashes.add(stored.hash);
              const converted = this.provider.convert(stored as Stored);
              super.next(converted); // Add to ReplaySubject buffer
            }
          },
          error: (err) => {
            console.error('Observe error:', err);
            this.error(err);
          }
        })
    );
  }

  async all(): Promise<T[]> {
    // Get current snapshot of all accumulated items from ReplaySubject buffer
    return new Promise<T[]>((resolve, reject) => {
      const items: T[] = [];

      const subscription = this.subscribe({
        next: (item) => {
          items.push(item);
        },
        error: (err) => {
          reject(err);
        },
        complete: () => {
          resolve(items);
        },
      });

      // ReplaySubject emits all buffered values immediately, so we can unsubscribe right away
      setTimeout(() => {
        subscription.unsubscribe();
        resolve(items);
      }, 0);
    });
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