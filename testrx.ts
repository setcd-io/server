import {
  asyncScheduler,
  BehaviorSubject,
  bufferCount,
  firstValueFrom,
  last,
  lastValueFrom,
  Observable,
  observeOn,
  OperatorFunction,
  ReplaySubject,
  shareReplay,
  take,
  toArray,
} from "rxjs";
import { tail, tailFrom } from "./src/cloud-rx/util";

const data = ["hello", "world", "this", "is", "a", "test"];

// export function tail<T>(count: number): OperatorFunction<T, T[]> {
//   return (source: Observable<T>) =>
//     new Observable<T[]>((observer) => {
//       const buffer = new ReplaySubject<T>(count);
//       const srcSub = source.subscribe({
//         next(value) {
//           buffer.next(value);
//         },
//         error(err) {
//           observer.error(err);
//         },
//       });

//       const arr: T[] = [];
//       const bufSub = buffer.subscribe((v) => {
//         arr.push(v);
//       });
//       bufSub.unsubscribe();

//       observer.next(arr);
//       observer.complete();

//       srcSub.unsubscribe();
//     });
// }

// export function tail<T>(count: number): OperatorFunction<T, T> {
//   return (source: Observable<T>) =>
//     new Observable<T>((observer) => {
//       // 1) internal buffer of the last `count` values
//       const buffer$ = new ReplaySubject<T>(count);

//       // 2) subscribe to the source *first*, feeding our buffer
//       const srcSub = source.subscribe({
//         next(value) {
//           buffer$.next(value);
//         },
//         error(err) {
//           observer.error(err);
//         },
//         complete() {
//           observer.complete();
//         },
//       });

//       // 3) now subscribe to the buffer$:
//       //    - this will *immediately* replay its up-to-`count` values (the "tail")
//       //    - and then continue to emit each new value
//       const bufSub = buffer$.subscribe({
//         next(value) {
//           observer.next(value);
//         },
//         error(err) {
//           observer.error(err);
//         },
//         // we don’t complete here; we rely on the source.complete above
//       });

//       // teardown both subscriptions when the outer unsubscribes
//       return () => {
//         srcSub.unsubscribe();
//         bufSub.unsubscribe();
//       };
//     });
// }

// export function tailFrom<T>(
//   source: Observable<T>,
//   count: number
// ): Promise<T[]> {
//   return new Promise<T[]>((resolve, reject) => {
//     const results: T[] = [];

//     const sub = source.pipe(tail(count)).subscribe({
//       next(value) {
//         results.push(value);
//       },
//       error(err) {
//         reject(err);
//       },
//     });

//     // Unsubscribe immediately after the synchronous replay of the buffer
//     sub.unsubscribe();

//     // Resolve with up to `count` items (or fewer, if that's all there was)
//     resolve(results);
//   });
// }

async function main() {
  const subj = new ReplaySubject<string>();
  //   subj.next(data[0]);
  //   subj.next(data[1]);
  //   subj.next(data[2]);
  //   subj.next(data[3]);
  //   subj.complete();

  //   const foo1 = subj.subscribe((value) => {
  //     console.log("Foo1 Received:", value);
  //   });

  //   const foo2 = subj.subscribe((value) => {
  //     console.log("Foo2 Received:", value);
  //   });

  //   const firstValue = await firstValueFrom(subj);
  //   console.log("FirstValue Received:", firstValue);

  //   const tailed = await firstValueFrom(subj.pipe(tail(2)));
  //   console.log("Tailed Received:", tailed);
  const tailed = subj.pipe(tail(1)).subscribe((value) => {
    console.log("Tailed Received:", value);
  });

  await new Promise<void>((resolve) => {
    setTimeout(() => {
      subj.next(data[0]);
      resolve();
    }, 1000);
  });

  const current1 = await tailFrom(subj, 10);
  console.log("Current1 Received:", current1);

  await new Promise<void>((resolve) => {
    setTimeout(() => {
      subj.next(data[1]);
      resolve();
    }, 1000);
  });

  const current2 = await tailFrom(subj, 10);
  console.log("Current2 Received:", current2);

  await new Promise<void>((resolve) => {
    setTimeout(() => {
      subj.next(data[2]);
      resolve();
    }, 1000);
  });

  const current3 = await tailFrom(subj, 10);
  console.log("Current3 Received:", current3);

  await new Promise<void>((resolve) => {
    setTimeout(() => {
      subj.next(data[3]);
      resolve();
    }, 1000);
  });

  const current4 = await tailFrom(subj, 10);
  console.log("Current4 Received:", current4);

  await new Promise<void>((resolve) => {
    setTimeout(() => {
      subj.next(data[4]);
      resolve();
    }, 1000);
  });

  const current5 = await tailFrom(subj, 10);
  console.log("Current5 Received:", current5);

  await new Promise<void>((resolve) => {
    setTimeout(() => {
      subj.next(data[5]);
      resolve();
    }, 1000);
  });

  const current6 = await tailFrom(subj, 10);
  console.log("Current6 Received:", current6);

  await new Promise<void>((resolve) => {
    setTimeout(() => {
      console.log("Donezo!");
      resolve();
    }, 5000);
  });

  const currentAll = await tailFrom(subj);
  console.log("CurrentAll Received:", currentAll);
}

void main().then(() => {});
