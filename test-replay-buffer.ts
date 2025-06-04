import { ReplaySubject } from 'rxjs';
import { replayBuffer } from './src/cloud-rx/operators';

// Test with active ReplaySubject
console.log('=== Testing replayBuffer with active ReplaySubject ===');
const activeSubject = new ReplaySubject<number>(10);

// Add some values
activeSubject.next(1);
activeSubject.next(2);
activeSubject.next(3);

// Get the buffer
activeSubject.pipe(replayBuffer()).subscribe({
  next: (value) => console.log('Active subject value:', value),
  complete: () => console.log('Active subject completed')
});

setTimeout(() => {
  console.log('\n=== Testing replayBuffer with completed ReplaySubject ===');
  
  // Test with completed ReplaySubject
  const completedSubject = new ReplaySubject<number>(10);
  completedSubject.next(4);
  completedSubject.next(5);
  completedSubject.next(6);
  completedSubject.complete();

  // Get the buffer from completed subject
  completedSubject.pipe(replayBuffer()).subscribe({
    next: (value) => console.log('Completed subject value:', value),
    complete: () => console.log('Completed subject completed')
  });
}, 100);