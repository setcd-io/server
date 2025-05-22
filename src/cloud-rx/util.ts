import {
  asyncScheduler,
  BehaviorSubject,
  Observable,
  Subscription,
} from "rxjs";

export function throttleByQueueSize<T>(
  delayMs: number,
  scheduler = asyncScheduler
) {
  return (source: Observable<T>) =>
    new Observable<T>((subscriber) => {
      const queue: T[] = [];
      let running = false;
      let scheduledTask: Subscription | null = null;

      const scheduleDrain = (delay: number) => {
        scheduledTask = scheduler.schedule(() => drain(), delay);
      };

      const drain = () => {
        if (queue.length === 0) {
          running = false;
          return;
        }

        running = true;
        const item = queue.shift()!; // dequeue
        subscriber.next(item); // emit

        const wait = queue.length > 0 ? delayMs : 0;
        if (wait > 0) {
          console.log(`!!! Waiting ${wait}ms before next item...`);
        }
        scheduleDrain(wait);
      };

      const sub = source.subscribe({
        next(value) {
          queue.push(value);
          if (!running) {
            drain();
          }
        },
        error(err) {
          subscriber.error(err);
        },
        complete() {
          // kick off drain if needed, then complete when empty
          if (!running && queue.length) {
            drain();
          }
          const finalize = () => {
            if (!running && queue.length === 0) {
              subscriber.complete();
            } else {
              scheduler.schedule(finalize, delayMs);
            }
          };
          finalize();
        },
      });

      return () => {
        sub.unsubscribe();
        if (scheduledTask) {
          scheduledTask.unsubscribe();
        }
      };
    });
}

export function observe<T>(source: Observable<T>): AsyncGenerator<T> {
  const generator = (async function* () {
    const queue: T[] = [];
    let done = false;
    let error: any = null;
    const waiters: Array<() => void> = [];

    const sub = source.subscribe({
      next(value) {
        queue.push(value);
        waiters.splice(0).forEach((resolve) => resolve());
      },
      error(err) {
        error = err;
        done = true;
        waiters.splice(0).forEach((resolve) => resolve());
      },
      complete() {
        done = true;
        waiters.splice(0).forEach((resolve) => resolve());
      },
    });

    try {
      while (!done || queue.length) {
        if (queue.length === 0) {
          await new Promise<void>((resolve) => waiters.push(resolve));
        }
        if (error) throw error;
        while (queue.length) {
          yield queue.shift()!;
        }
      }
    } finally {
      sub.unsubscribe();
    }
  })();

  return {
    [Symbol.asyncIterator]() {
      return generator;
    },
    next: generator.next.bind(generator),
    throw: generator.throw?.bind(generator),
    return: generator.return?.bind(generator),
  };
}

/*
    const queue: T[] = [];
  let done = false;
  let error: any = null;
  const waiters: Array<() => void> = [];

  // subscribe to source
  const sub = source.subscribe({
    next(value) {
      queue.push(value);
      waiters.splice(0).forEach((resolve) => resolve());
    },
    error(err) {
      error = err;
      done = true;
      waiters.splice(0).forEach((resolve) => resolve());
    },
    complete() {
      done = true;
      waiters.splice(0).forEach((resolve) => resolve());
    },
  });

  try {
    while (!done || queue.length) {
      if (queue.length === 0) {
        // wait until something arrives (or error/complete)
        await new Promise<void>((resolve) => waiters.push(resolve));
      }
      // if an error happened, throw it
      if (error) throw error;
      // drain the queue
      while (queue.length) {
        yield queue.shift()!;
      }
    }
  } finally {
    sub.unsubscribe();
  }
    */
// }

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

export function subscribe<T>(
  source: AsyncGenerator<T, void, unknown>,
  next: (value: T) => void,
  error?: (err: any) => void,
  complete?: () => void
): Subscription {
  const subscription = new Subscription();
  let cancelled = false;

  // kick off the async loop
  (async () => {
    try {
      for await (const value of source) {
        if (cancelled) break;
        next(value);
      }
      if (!cancelled) {
        complete?.();
      }
    } catch (err) {
      if (!cancelled) {
        error?.(err);
      }
    }
  })();

  // when the user unsubscribes, stop the loop and signal to the generator
  subscription.add(() => {
    cancelled = true;
    source.return?.();
  });

  return subscription;
}
