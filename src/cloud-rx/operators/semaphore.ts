import {
  asyncScheduler,
  AsyncSubject,
  from,
  Observable,
  ObservableInput,
  ObservedValueOf,
  OperatorFunction,
  Subscription,
} from "rxjs";

/**
 * Controls the concurrency of inner observables, similar to mergeMap but with enforced limits.
 * Processes items from the source with a maximum number of concurrent operations.
 *
 * @param project Function that transforms source values into ObservableInput (Observable, Promise, etc.)
 * @param concurrent Maximum number of concurrent operations
 * @returns OperatorFunction that emits results from inner observables with concurrency control
 *
 * @example
 * // Allow 3 concurrent operations
 * source.pipe(semaphore(value => processAsync(value), 3))
 *
 * // With index parameter
 * source.pipe(semaphore((value, index) => processWithIndex(value, index), 2))
 *
 * // Sequential processing (prefer using mutex() for this)
 * source.pipe(semaphore(value => httpRequest(value), 1))
 */
export function semaphore<T, O extends ObservableInput<any>>(
  project: (value: T, index: number) => O,
  concurrent: number
): OperatorFunction<T, ObservedValueOf<O>> {
  return (source: Observable<T>) => {
    return new Observable<ObservedValueOf<O>>((subscriber) => {
      const queue: { value: T; index: number }[] = [];
      let sourceCompleted = false;
      let activeCount = 0;
      let sourceIndex = 0;
      const activeSubs: Subscription[] = [];

      const processNext = () => {
        // Process items up to the concurrency limit
        while (activeCount < concurrent && queue.length > 0) {
          const item = queue.shift()!;
          const { value, index } = item;
          activeCount++;

          // Create AsyncSubject to control when this item completes
          const currentProcessing = new AsyncSubject<ObservedValueOf<O>>();

          // Subscribe to the current processing to know when it's done
          const processSub = currentProcessing.subscribe({
            next: (result) => subscriber.next(result),
            error: (err) => subscriber.error(err),
            complete: () => {
              activeCount--;

              // Schedule next processing on asyncScheduler to ensure proper timing
              asyncScheduler.schedule(() => {
                processNext();
              });
            },
          });

          // Start the actual work - convert ObservableInput to Observable
          let result: O;
          try {
            result = project(value, index);
          } catch (err) {
            currentProcessing.error(err);
            return;
          }
          const inner$ = from(result);
          const innerSub = inner$.subscribe({
            next: (result) => {
              currentProcessing.next(result);
            },
            error: (err) => {
              currentProcessing.error(err);
            },
            complete: () => {
              currentProcessing.complete();
            },
          });

          activeSubs.push(processSub, innerSub);
        }

        if (sourceCompleted && activeCount === 0 && queue.length === 0) {
          subscriber.complete();
        }
      };

      const subscription = source.subscribe({
        next: (value) => {
          queue.push({ value, index: sourceIndex++ });
          processNext();
        },
        error: (err) => subscriber.error(err),
        complete: () => {
          sourceCompleted = true;
          processNext();
        },
      });

      return () => {
        subscription.unsubscribe();
        activeSubs.forEach((sub) => sub.unsubscribe());
      };
    });
  };
}

// Copied from rxjs source
// + Added AbortSignal
export function iterate<T>(observable: Observable<T>): AsyncIterable<T> {
  return {
    [Symbol.asyncIterator](): AsyncGenerator<T, void, void> {
      let subscription: Subscription | undefined;
      let hasError = false;
      let error: unknown;
      let completed = false;
      const values: T[] = [];
      const deferreds: [
        (value: IteratorResult<T>) => void,
        (reason: unknown) => void
      ][] = [];

      const handleError = (err: unknown) => {
        hasError = true;
        error = err;
        while (deferreds.length) {
          const [_, reject] = deferreds.shift()!;
          reject(err);
        }
      };

      const handleComplete = () => {
        completed = true;
        while (deferreds.length) {
          const [resolve] = deferreds.shift()!;
          resolve({ value: undefined, done: true });
        }
      };

      return {
        next: (): Promise<IteratorResult<T>> => {
          if (!subscription) {
            // We only want to start the subscription when the user starts iterating.
            subscription = observable.subscribe({
              next: (value) => {
                if (deferreds.length) {
                  const [resolve] = deferreds.shift()!;
                  resolve({ value, done: false });
                } else {
                  values.push(value);
                }
              },
              error: handleError,
              complete: handleComplete,
            });
          }

          // If we already have some values in our buffer, we'll return the next one.
          if (values.length) {
            return Promise.resolve({ value: values.shift()!, done: false });
          }

          // This was already completed, so we're just going to return a done result.
          if (completed) {
            return Promise.resolve({ value: undefined, done: true });
          }

          // There was an error, so we're going to return an error result.
          if (hasError) {
            return Promise.reject(error);
          }

          // Otherwise, we need to make them wait for a value.
          return new Promise((resolve, reject) => {
            deferreds.push([resolve, reject]);
          });
        },
        throw: (err): Promise<IteratorResult<T>> => {
          subscription?.unsubscribe();

          handleError(err);
          return Promise.reject(err);
        },
        return: (): Promise<IteratorResult<T>> => {
          subscription?.unsubscribe();
          // NOTE: I did some research on this, and as of Feb 2023, Chrome doesn't seem to do
          // anything with pending promises returned from `next()` when `throw()` is called.
          // However, for consumption of observables, I don't want RxJS taking the heat for that
          // quirk/leak of the type. So we're going to resolve all pending promises we've nexted out here.
          handleComplete();
          return Promise.resolve({ value: undefined, done: true });
        },
        [Symbol.asyncIterator]() {
          return this;
        },
      };
    },
  };
}
