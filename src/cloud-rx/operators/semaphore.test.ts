import { expect } from 'chai';
import { Subject, timer, of } from 'rxjs';
import { take, toArray, tap, map } from 'rxjs/operators';
import { semaphore } from './semaphore';

describe('semaphore operator (concurrent processing)', () => {
  it('should allow specified number of concurrent operations', (done) => {
    const testSubject = new Subject<number>();
    const activeTimes: number[][] = [];
    let activeCount = 0;
    
    testSubject.pipe(
      semaphore((value) => {
        const startActive = ++activeCount;
        activeTimes.push([startActive]);
        
        return timer(30).pipe(
          take(1),
          tap(() => {
            const endActive = --activeCount;
            activeTimes[activeTimes.length - 1].push(endActive);
          }),
          map(() => value * 2) // Multiply by 2
        );
      }, 2), // Allow 2 concurrent operations
      toArray()
    ).subscribe({
      next: (values) => {
        // Should have processed all 3 values
        expect(values).to.have.length(3);
        expect(values).to.deep.equal([2, 4, 6]); // [1,2,3] * 2
        
        // First two operations should start concurrently
        expect(activeTimes[0][0]).to.equal(1); // First starts with active=1
        expect(activeTimes[1][0]).to.equal(2); // Second starts with active=2
        
        // Third operation should wait
        expect(activeTimes[2][0]).to.be.at.most(2);
        
        done();
      },
      error: done
    });

    testSubject.next(1);
    testSubject.next(2);
    testSubject.next(3);
    testSubject.complete();
  });

  it('should pass index parameter with concurrency > 1', (done) => {
    const testSubject = new Subject<number>();
    const processedItems: Array<{value: number, index: number, result: number}> = [];
    
    testSubject.pipe(
      semaphore((value, index) => {
        // Process with both value and index (matching docstring example)
        const result = value * 2 + index; // Use both value and index
        processedItems.push({value, index, result});
        return of(result);
      }, 2), // Allow 2 concurrent operations (matching docstring)
      toArray()
    ).subscribe({
      next: (values) => {
        // Verify all items were processed
        expect(values).to.have.length(4);
        expect(processedItems).to.have.length(4);
        
        // Verify index parameter was passed correctly
        const indices = processedItems.map(item => item.index);
        expect(indices).to.deep.equal([0, 1, 2, 3]);
        
        // Verify results use both value and index: (value * 2) + index
        const expectedResults = [
          5 * 2 + 0,  // 10
          10 * 2 + 1, // 21  
          15 * 2 + 2, // 32
          20 * 2 + 3  // 43
        ];
        expect(values).to.deep.equal(expectedResults);
        
        done();
      },
      error: done
    });

    testSubject.next(5);
    testSubject.next(10);
    testSubject.next(15);
    testSubject.next(20);
    testSubject.complete();
  });

  it('should work with high concurrency', (done) => {
    const testSubject = new Subject<number>();
    const startTimes: number[] = [];
    
    testSubject.pipe(
      semaphore((value) => {
        startTimes.push(Date.now());
        return timer(20).pipe(
          take(1),
          map(() => value * 3)
        );
      }, 5), // Allow 5 concurrent operations
      toArray()
    ).subscribe({
      next: (values) => {
        expect(values).to.have.length(5);
        expect(values).to.deep.equal([3, 6, 9, 12, 15]); // [1,2,3,4,5] * 3
        
        // With high concurrency, all should start around the same time
        const timeDiffs = startTimes.slice(1).map((time, i) => time - startTimes[i]);
        timeDiffs.forEach(diff => {
          expect(diff).to.be.lessThan(50); // Should start within 50ms of each other
        });
        
        done();
      },
      error: done
    });

    testSubject.next(1);
    testSubject.next(2);
    testSubject.next(3);
    testSubject.next(4);
    testSubject.next(5);
    testSubject.complete();
  });
});