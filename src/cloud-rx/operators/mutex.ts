import { ObservableInput, ObservedValueOf, OperatorFunction } from "rxjs";
import { semaphore } from "./semaphore";

/**
 * Processes items sequentially (one at a time) ensuring mutual exclusion.
 * This is a convenience function that calls semaphore with concurrency=1.
 *
 * @param project Function that transforms source values into ObservableInput (Observable, Promise, etc.)
 * @returns OperatorFunction that emits results from inner observables sequentially
 *
 * @example
 * // Sequential processing (mutex behavior)
 * source.pipe(mutex(value => httpRequest(value)))
 *
 * // With index parameter
 * source.pipe(mutex((value, index) => processWithIndex(value, index)))
 */
export function mutex<T, O extends ObservableInput<any>>(
  project: (value: T, index: number) => O
): OperatorFunction<T, ObservedValueOf<O>> {
  return semaphore(project, 1);
}