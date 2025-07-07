// Copied from rxjs source

import { Observable, Subscription } from "rxjs";

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
