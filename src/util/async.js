"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.AsyncObservable = void 0;
exports.iterate = iterate;
const rxjs_1 = require("rxjs");
// Copied from rxjs source
// - https://github.com/ReactiveX/rxjs/blob/c15b37f81ba5f5abea8c872b0189a70b150df4cb/packages/observable/src/observable.ts#L922
class AsyncObservable extends rxjs_1.Observable {
    // Added helper method to convert an Observable to an AsyncObservable
    static from(source) {
        return new AsyncObservable((subscriber) => {
            const subscription = source.subscribe(subscriber);
            return () => subscription.unsubscribe();
        });
    }
    [Symbol.asyncIterator]() {
        let subscription;
        let hasError = false;
        let error;
        let completed = false;
        const values = [];
        const deferreds = [];
        const handleError = (err) => {
            hasError = true;
            error = err;
            while (deferreds.length) {
                const [_, reject] = deferreds.shift();
                reject(err);
            }
        };
        const handleComplete = () => {
            completed = true;
            while (deferreds.length) {
                const [resolve] = deferreds.shift();
                resolve({ value: undefined, done: true });
            }
        };
        return {
            next: () => {
                if (!subscription) {
                    // We only want to start the subscription when the user starts iterating.
                    subscription = this.subscribe({
                        next: (value) => {
                            if (deferreds.length) {
                                const [resolve] = deferreds.shift();
                                resolve({ value, done: false });
                            }
                            else {
                                values.push(value);
                            }
                        },
                        error: handleError,
                        complete: handleComplete,
                    });
                }
                // If we already have some values in our buffer, we'll return the next one.
                if (values.length) {
                    return Promise.resolve({ value: values.shift(), done: false });
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
            throw: (err) => {
                subscription?.unsubscribe();
                // NOTE: I did some research on this, and as of Feb 2023, Chrome doesn't seem to do
                // anything with pending promises returned from `next()` when `throw()` is called.
                // However, for consumption of observables, I don't want RxJS taking the heat for that
                // quirk/leak of the type. So we're going to reject all pending promises we've nexted out here.
                handleError(err);
                return Promise.reject(err);
            },
            return: () => {
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
    }
}
exports.AsyncObservable = AsyncObservable;
// Copied from rxjs source
// - https://github.com/ReactiveX/rxjs/blob/c15b37f81ba5f5abea8c872b0189a70b150df4cb/packages/observable/src/observable.ts#L922
function iterate(observable) {
    return {
        [Symbol.asyncIterator]() {
            let subscription;
            let hasError = false;
            let error;
            let completed = false;
            const values = [];
            const deferreds = [];
            const handleError = (err) => {
                hasError = true;
                error = err;
                while (deferreds.length) {
                    const [_, reject] = deferreds.shift();
                    reject(err);
                }
            };
            const handleComplete = () => {
                completed = true;
                while (deferreds.length) {
                    const [resolve] = deferreds.shift();
                    resolve({ value: undefined, done: true });
                }
            };
            return {
                next: () => {
                    if (!subscription) {
                        // We only want to start the subscription when the user starts iterating.
                        subscription = observable.subscribe({
                            next: (value) => {
                                if (deferreds.length) {
                                    const [resolve] = deferreds.shift();
                                    resolve({ value, done: false });
                                }
                                else {
                                    values.push(value);
                                }
                            },
                            error: handleError,
                            complete: handleComplete,
                        });
                    }
                    // If we already have some values in our buffer, we'll return the next one.
                    if (values.length) {
                        return Promise.resolve({ value: values.shift(), done: false });
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
                throw: (err) => {
                    subscription?.unsubscribe();
                    // NOTE: I did some research on this, and as of Feb 2023, Chrome doesn't seem to do
                    // anything with pending promises returned from `next()` when `throw()` is called.
                    // However, for consumption of observables, I don't want RxJS taking the heat for that
                    // quirk/leak of the type. So we're going to reject all pending promises we've nexted out here.
                    handleError(err);
                    return Promise.reject(err);
                },
                return: () => {
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
