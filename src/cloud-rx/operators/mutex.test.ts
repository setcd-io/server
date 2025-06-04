import { expect } from 'chai';
import { Subject, timer, of, throwError } from 'rxjs';
import { take, toArray, tap, map } from 'rxjs/operators';
import { mutex } from './mutex';
import { semaphore } from './semaphore';

describe('mutex operator (sequential processing)', () => {
    it('should process items one at a time', (done) => {
      const testSubject = new Subject<number>();
      const startTimes: number[] = [];
      const endTimes: number[] = [];
      
      testSubject.pipe(
        mutex((value) => {
          startTimes.push(Date.now());
          return timer(50).pipe(
            take(1),
            tap(() => endTimes.push(Date.now())),
            map(() => value * 2) // Multiply by 2
          );
        }),
        toArray()
      ).subscribe({
        next: (results) => {
          // Should have processed 3 items
          expect(results).to.have.length(3);
          // Verify values were multiplied by 2: [1,2,3] -> [2,4,6]
          expect(results).to.deep.equal([2, 4, 6]);
          
          // Each operation should start after the previous one ends (allow small timing tolerance)
          expect(startTimes[1]).to.be.greaterThanOrEqual(endTimes[0]);
          expect(startTimes[2]).to.be.greaterThanOrEqual(endTimes[1]);
          
          done();
        },
        error: done
      });

      // Emit values immediately
      testSubject.next(1);
      testSubject.next(2);
      testSubject.next(3);
      testSubject.complete();
    });

    it('should work with Promises', (done) => {
      const testSubject = new Subject<number>();
      
      testSubject.pipe(
        mutex((value) => 
          new Promise<number>((resolve) => {
            setTimeout(() => resolve(value * 2), 10);
          })
        ),
        toArray()
      ).subscribe({
        next: (values) => {
          expect(values).to.deep.equal([2, 4, 6]);
          done();
        },
        error: done
      });

      testSubject.next(1);
      testSubject.next(2);
      testSubject.next(3);
      testSubject.complete();
    });

    it('should work with Observables', (done) => {
      const testSubject = new Subject<number>();
      
      testSubject.pipe(
        mutex((value) => of(value * 2)),
        toArray()
      ).subscribe({
        next: (values) => {
          expect(values).to.deep.equal([2, 4, 6]);
          done();
        },
        error: done
      });

      testSubject.next(1);
      testSubject.next(2);
      testSubject.next(3);
      testSubject.complete();
    });

    it('should pass index parameter', (done) => {
      const testSubject = new Subject<number>();
      const indices: number[] = [];
      
      testSubject.pipe(
        mutex((value, index) => {
          indices.push(index);
          return of(value * 2);
        }),
        toArray()
      ).subscribe({
        next: (values) => {
          expect(indices).to.deep.equal([0, 1, 2]);
          expect(values).to.deep.equal([20, 40, 60]); // 10*2, 20*2, 30*2
          done();
        },
        error: done
      });

      testSubject.next(10);
      testSubject.next(20);
      testSubject.next(30);
      testSubject.complete();
    });


    it('should be equivalent to semaphore with concurrency=1', (done) => {
      const testSubject1 = new Subject<number>();
      const testSubject2 = new Subject<number>();
      const mutexResults: number[] = [];
      const semaphoreResults: number[] = [];
      
      // Test mutex
      testSubject1.pipe(
        mutex((value) => of(value * 3)),
        toArray()
      ).subscribe({
        next: (values) => {
          mutexResults.push(...values);
          
          // Test semaphore with concurrency=1
          testSubject2.pipe(
            semaphore((value) => of(value * 3), 1),
            toArray()
          ).subscribe({
            next: (values) => {
              semaphoreResults.push(...values);
              
              // Should produce identical results
              expect(mutexResults).to.deep.equal(semaphoreResults);
              expect(mutexResults).to.deep.equal([3, 6, 9]); // [1,2,3] * 3
              done();
            },
            error: done
          });

          testSubject2.next(1);
          testSubject2.next(2);
          testSubject2.next(3);
          testSubject2.complete();
        },
        error: done
      });

      testSubject1.next(1);
      testSubject1.next(2);
      testSubject1.next(3);
      testSubject1.complete();
    });
});

describe('error handling', () => {
  it('should propagate errors from inner observables', (done) => {
      const testSubject = new Subject<number>();
      let valueReceived = false;
      
      testSubject.pipe(
        mutex((value) => {
          if (value === 2) {
            return throwError(() => new Error('Test error'));
          }
          return of(value * 2);
        })
      ).subscribe({
        next: (value) => {
          expect(value).to.equal(2); // 1 * 2
          valueReceived = true;
        },
        error: (err) => {
          expect(valueReceived).to.be.true;
          expect(err.message).to.equal('Test error');
          done();
        }
      });

      testSubject.next(1);
      testSubject.next(2);
      testSubject.next(3);
    });

    it('should propagate errors from project function', (done) => {
      const testSubject = new Subject<number>();
      let valueReceived = false;
      
      testSubject.pipe(
        mutex((value) => {
          if (value === 2) {
            throw new Error('Project error');
          }
          return of(value * 2);
        })
      ).subscribe({
        next: (value) => {
          expect(value).to.equal(2); // 1 * 2
          valueReceived = true;
        },
        error: (err) => {
          expect(valueReceived).to.be.true;
          expect(err.message).to.equal('Project error');
          done();
        }
      });

      testSubject.next(1);
      testSubject.next(2);
    });
  });

  describe('completion behavior', () => {
    it('should complete when source completes and all operations finish', (done) => {
      const testSubject = new Subject<number>();
      let completed = false;
      
      testSubject.pipe(
        mutex((value) => timer(30).pipe(take(1), map(() => value * 2)))
      ).subscribe({
        next: (value) => {
          // Expecting 2 or 4 since we multiply input by 2: [1,2] -> [2,4]
          expect([2, 4]).to.include(value);
        },
        complete: () => {
          completed = true;
          done();
        }
      });

      testSubject.next(1);
      testSubject.next(2);
      testSubject.complete();
      
      // Should not complete immediately
      setTimeout(() => {
        expect(completed).to.be.false;
      }, 15);
    });
  });

describe('subscription cleanup', () => {
  it('should clean up subscriptions on unsubscribe', () => {
    const testSubject = new Subject<number>();
    const subscription = testSubject.pipe(
      mutex((value) => timer(1000).pipe(take(1), map(() => value * 2)))
    ).subscribe({
      next: (value) => {
        // Expecting 2 or 4 since we multiply input by 2: [1,2] -> [2,4]
        expect([2, 4]).to.include(value);
      }
    });

    testSubject.next(1);
    testSubject.next(2);
    
    // Should not throw when unsubscribing
    expect(() => subscription.unsubscribe()).to.not.throw();
  });
});