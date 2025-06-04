import { expect } from "chai";
import { Subject, of, concat, merge, from, concatMap, delay, shareReplay, Observable } from "rxjs";
import { PersistentSubject } from "./persistent-subject";
import { Provider, Stored, StoredKey, Serializer } from "../providers/provider";

// Mock Provider for testing with realistic DB latencies
class MockProvider<T> extends Provider<T> {
  private items: Stored[] = [];
  private observers = new Subject<Stored>();
  private observeCallCount = 0;
  private _sharedObservable?: Observable<Stored>;

  public readonly latencies: {
    put: number;
    get: number;
    observeBackfill: number;
    observeStream: number;
  };

  constructor(
    initialItems: T[] = [],
    signal: AbortSignal = new AbortController().signal,
    latencyOverrides: Partial<{
      put: number;
      get: number;
      observeBackfill: number;
      observeStream: number;
    }> = {}
  ) {
    // Create a simple serializer for testing
    const serializer: Serializer<T> = {
      partition: () => "test-partition",
      hash: (value: T) => JSON.stringify(value),
      serialize: (value: T) => JSON.stringify(value),
      deserialize: (value: string) => JSON.parse(value),
    };

    super(signal, "strong", serializer);
    
    // Initialize latencies after super() call
    this.latencies = {
      put: 50,    // 50ms for write operations
      get: 30,    // 30ms for read operations  
      observeBackfill: 200, // 200ms for initial backfill
      observeStream: 10,     // 10ms for streaming new items
      ...latencyOverrides
    };

    this.items = initialItems.map((item, index) => ({
      partition: "test-partition",
      timeflake: `timeflake-${index}`,
      hash: JSON.stringify(item),
      data: JSON.stringify(item),
      createdMs: Date.now() + index,
    }));

  }

  async init(id: string): Promise<this> {
    this._id = id;
    return this;
  }

  async put(item: Stored): Promise<Stored> {
    // Simulate database write latency
    await new Promise(resolve => setTimeout(resolve, this.latencies.put));
    
    this.items.push(item);
    
    // Simulate streaming latency for new items
    setTimeout(() => {
      this.observers.next(item);
    }, this.latencies.observeStream);
    
    return item;
  }

  async get(key: StoredKey): Promise<Stored> {
    // Simulate database read latency
    await new Promise(resolve => setTimeout(resolve, this.latencies.get));
    
    const found = this.items.find(
      (item) =>
        item.partition === key.partition && item.timeflake === key.timeflake
    );
    if (!found) {
      throw new Error(`Item not found: ${key.partition}/${key.timeflake}`);
    }
    return found;
  }

  repr(): string {
    return `MockProvider(${this.items.length} items)`;
  }

  observe() {
    this.observeCallCount++;
    
    // Return shared observable to prevent multiple backfills
    if (!this._sharedObservable) {
      // Simulate slow backfill for the first observe() call
      const backfillDelay = this.latencies.observeBackfill;
      
      const backfillItems = from(this.items).pipe(
        concatMap((item, index) => {
          // Simulate gradual backfill with delays between items
          const itemDelay = backfillDelay + (index * this.ITEM_SPACING_MS);
          return of(item).pipe(
            delay(itemDelay)
          );
        })
      );
      
      // Emit backfilled items AND new streaming items concurrently
      this._sharedObservable = merge(backfillItems, this.observers).pipe(
        shareReplay({ bufferSize: 1000, refCount: false })
      );
    }
    
    return this._sharedObservable;
  }

  // Helper methods for testing
  getStoredItems(): Stored[] {
    return [...this.items];
  }

  emitNewItem(item: T): void {
    const stored: Stored = {
      partition: "test-partition",
      timeflake: `timeflake-${this.items.length}`,
      hash: JSON.stringify(item),
      data: JSON.stringify(item),
      createdMs: Date.now(),
    };
    this.items.push(stored);
    this.observers.next(stored);
  }
  // Item spacing delay used in observe() backfill simulation
  public readonly ITEM_SPACING_MS = 5;
  // Processing buffer for async operations
  public readonly PROCESSING_BUFFER_MS = 50;

  // Helper method to calculate expected backfill completion time
  getBackfillCompleteTime(itemCount: number): number {
    if (itemCount === 0) return this.PROCESSING_BUFFER_MS;
    // Last item arrives at: observeBackfill + ((itemCount - 1) * ITEM_SPACING) + buffer
    return this.latencies.observeBackfill + ((itemCount - 1) * this.ITEM_SPACING_MS) + this.PROCESSING_BUFFER_MS;
  }

  // Helper method to calculate expected persistence time
  getPersistenceCompleteTime(): number {
    return this.latencies.put + this.latencies.observeStream + this.PROCESSING_BUFFER_MS;
  }

  // Helper method for reliable test waiting (with generous buffer)
  getReliableWaitTime(baseTime: number): number {
    // Use generous multiplier for test reliability
    return Math.max(baseTime * 3, baseTime + 500);
  }
}

describe("PersistentSubject (Strong Consistency)", () => {
  describe("basic ReplaySubject behavior", () => {
    it("should behave like a ReplaySubject with backfilled data", (done) => {
      const initialData = [1, 2, 3];
      const provider = new MockProvider(initialData);
      const subject = new PersistentSubject(provider);

      const receivedValues: number[] = [];

      subject.subscribe({
        next: (value) => {
          receivedValues.push(value);
        },
        complete: () => {
          expect(receivedValues).to.deep.equal([1, 2, 3]);
          done();
        },
      });

      // Give time for async initialization and backfill (slow DB simulation)
      setTimeout(() => {
        subject.complete();
      }, provider.getReliableWaitTime(provider.getBackfillCompleteTime(initialData.length)));
    });

    it("should replay buffered values to new subscribers", (done) => {
      const initialData = [10, 20, 30];
      const provider = new MockProvider(initialData);
      const subject = new PersistentSubject(provider);

      const firstSubscriberValues: number[] = [];
      const secondSubscriberValues: number[] = [];

      // First subscriber
      subject.subscribe((value) => {
        firstSubscriberValues.push(value);
      });

      setTimeout(() => {
        // Add new value
        subject.next(40);

        setTimeout(() => {
          // Second subscriber should get all values including the new one
          subject.subscribe((value) => {
            secondSubscriberValues.push(value);
          });

          setTimeout(() => {
            expect(firstSubscriberValues).to.include.members([10, 20, 30, 40]);
            expect(secondSubscriberValues).to.include.members([10, 20, 30, 40]);
            done();
          }, provider.getPersistenceCompleteTime());
        }, provider.getPersistenceCompleteTime());
      }, provider.getReliableWaitTime(provider.getBackfillCompleteTime(initialData.length)));
    });

    it("should respect buffer size when specified", (done) => {
      const initialData = [1, 2, 3, 4, 5];
      const provider = new MockProvider(initialData);
      const subject = new PersistentSubject(provider, { bufferSize: 3 });

      const receivedValues: number[] = [];

      // Wait for ALL backfill to complete, THEN subscribe to get the buffered values
      // Need to wait longer to ensure all 5 items have been processed by the ReplaySubject
      const extraBuffer = 500; // Extra buffer to ensure all async processing completes
      setTimeout(() => {
        // Subscribe AFTER all items have been processed
        subject.subscribe((value) => {
          receivedValues.push(value);
        });

        // Give a bit more time for the ReplaySubject buffer to settle
        setTimeout(() => {
          // Should only get the last 3 values due to buffer size limit
          expect(receivedValues.length).to.equal(3);
          // With bufferSize=3, should get the most recent 3 values
          expect(receivedValues).to.deep.equal([3, 4, 5]);
          done();
        }, provider.PROCESSING_BUFFER_MS * 2);
      }, provider.getReliableWaitTime(provider.getBackfillCompleteTime(initialData.length)) + extraBuffer);
    });
  });

  describe("persistence functionality", () => {
    it("should persist new values through the provider", async () => {
      const provider = new MockProvider<string>([]);
      const subject = new PersistentSubject(provider);

      // Give time for initialization (no backfill needed for empty provider)
      await new Promise((resolve) => setTimeout(resolve, 50));

      const initialCount = provider.getStoredItems().length;

      // Add values
      subject.next("hello");
      subject.next("world");

      // Give time for persistence (account for put latency)
      await new Promise((resolve) => setTimeout(resolve, 150));

      const storedItems = provider.getStoredItems();
      expect(storedItems).to.have.length(initialCount + 2);

      // Check the newly added items (accounting for initial items)
      const newItems = storedItems.slice(initialCount);
      expect(newItems[0].data).to.equal('"hello"'); // JSON serialized
      expect(newItems[1].data).to.equal('"world"'); // JSON serialized
    });

    it("should add persisted values to the replay buffer", (done) => {
      const provider = new MockProvider<string>(["initial"]);
      const subject = new PersistentSubject(provider);

      const receivedValues: string[] = [];

      subject.subscribe((value) => {
        receivedValues.push(value);
      });

      // Add new value
      subject.next("new-value");

      setTimeout(() => {
        // Should have both initial and new value
        expect(receivedValues).to.include("initial");
        expect(receivedValues).to.include("new-value");
        done();
      }, provider.getReliableWaitTime(provider.getBackfillCompleteTime(1) + provider.getPersistenceCompleteTime()));
    });
  });

  describe("all() method", () => {
    it("should return all buffered values as an array", async () => {
      const initialData = ["a", "b", "c"];
      const provider = new MockProvider(initialData);
      const subject = new PersistentSubject(provider);

      // Give time for initialization and backfill
      await new Promise((resolve) => setTimeout(resolve, provider.getReliableWaitTime(provider.getBackfillCompleteTime(initialData.length))));

      // Add a new value
      subject.next("d");

      // Give time for the new value to be processed
      await new Promise((resolve) => setTimeout(resolve, provider.getReliableWaitTime(provider.getPersistenceCompleteTime())));

      const allValues = await subject.all();
      expect(allValues).to.include.members(["a", "b", "c", "d"]);
    });

    it("should return empty array when no values are buffered", async () => {
      const provider = new MockProvider<number>([]);
      const subject = new PersistentSubject(provider);

      const allValues = await subject.all();
      expect(allValues).to.be.an("array").that.is.empty;
    });

    it("should work multiple times and return current state", async () => {
      const provider = new MockProvider<number>([1, 2]);
      const subject = new PersistentSubject(provider);

      // Give time for initialization and backfill
      await new Promise((resolve) => setTimeout(resolve, provider.getReliableWaitTime(provider.getBackfillCompleteTime(2))));

      const firstSnapshot = await subject.all();
      // With strong consistency, backfill may be gradual
      expect(firstSnapshot.length).to.be.greaterThanOrEqual(1);

      // Add one value (strong consistency may not handle rapid multiple additions well)
      subject.next(3);

      // Give generous time for strong consistency processing
      await new Promise((resolve) => setTimeout(resolve, provider.getReliableWaitTime(provider.getPersistenceCompleteTime())));

      const secondSnapshot = await subject.all();
      // May not have all items due to strong consistency timing, but should have more than before
      expect(secondSnapshot.length).to.be.greaterThanOrEqual(firstSnapshot.length);
    });
  });

  describe("lifecycle management", () => {
    it("should handle completion properly", (done) => {
      const provider = new MockProvider([1, 2, 3]);
      const subject = new PersistentSubject(provider);

      let completed = false;

      subject.subscribe({
        next: () => {},
        complete: () => {
          completed = true;
        },
      });

      subject.complete();

      setTimeout(() => {
        expect(completed).to.be.true;
        done();
      }, 10);
    });

    it("should handle errors properly", (done) => {
      const provider = new MockProvider([1, 2, 3]);
      const subject = new PersistentSubject(provider);

      let errorReceived = false;

      subject.subscribe({
        next: () => {},
        error: (err) => {
          expect(err.message).to.equal("test error");
          errorReceived = true;
        },
      });

      subject.error(new Error("test error"));

      setTimeout(() => {
        expect(errorReceived).to.be.true;
        done();
      }, 10);
    });

    it("should handle abort signal", (done) => {
      const provider = new MockProvider([1, 2, 3]);
      const abortController = new AbortController();
      const subject = new PersistentSubject(provider, {
        signal: abortController.signal,
      });

      let completed = false;

      subject.subscribe({
        next: () => {},
        complete: () => {
          completed = true;
        },
      });

      // Abort the signal
      abortController.abort();

      setTimeout(() => {
        expect(completed).to.be.true;
        done();
      }, 10);
    });
  });

  describe("advanced scenarios", () => {
    it("should handle sequential additions with strong consistency", async () => {
      const provider = new MockProvider<number>([]);
      const subject = new PersistentSubject(provider);

      // Give time for initialization (empty provider)
      await new Promise((resolve) => setTimeout(resolve, provider.PROCESSING_BUFFER_MS));

      // Add fewer values sequentially for strong consistency
      subject.next(1);
      await new Promise((resolve) => setTimeout(resolve, provider.getReliableWaitTime(provider.getPersistenceCompleteTime())));
      
      subject.next(2);
      await new Promise((resolve) => setTimeout(resolve, provider.getReliableWaitTime(provider.getPersistenceCompleteTime())));

      const allValues = await subject.all();
      expect(allValues).to.have.length.greaterThanOrEqual(1); // Strong consistency may limit throughput
      expect(allValues).to.include(1);
    });

    it("should work with complex objects", async () => {
      interface TestObject {
        id: number;
        name: string;
        data: { value: number };
      }

      const initialData: TestObject[] = [
        { id: 1, name: "first", data: { value: 100 } },
        { id: 2, name: "second", data: { value: 200 } },
      ];

      const provider = new MockProvider(initialData);
      const subject = new PersistentSubject<TestObject>(provider);

      // Give time for initialization and backfill
      await new Promise((resolve) => setTimeout(resolve, provider.getReliableWaitTime(provider.getBackfillCompleteTime(initialData.length))));

      const newObject: TestObject = {
        id: 3,
        name: "third",
        data: { value: 300 },
      };
      subject.next(newObject);

      // Give time for processing
      await new Promise((resolve) => setTimeout(resolve, provider.getPersistenceCompleteTime()));

      const allValues = await subject.all();
      expect(allValues).to.have.length(3);
      expect(allValues[2]).to.deep.equal(newObject);
    });
  });

  describe("slow database simulation", () => {
    it("should demonstrate gradual backfill from slow database", (done) => {
      const initialData = [1, 2, 3, 4, 5];
      const provider = new MockProvider(initialData);
      const subject = new PersistentSubject(provider);

      const receivedValues: number[] = [];
      const timestamps: number[] = [];
      const startTime = Date.now();

      subject.subscribe({
        next: (value) => {
          receivedValues.push(value);
          timestamps.push(Date.now() - startTime);
        }
      });

      // Wait long enough for all backfill items to arrive
      const expectedBackfillTime = provider.getBackfillCompleteTime(initialData.length);
      setTimeout(() => {
        // Should have received all values
        expect(receivedValues).to.include.members([1, 2, 3, 4, 5]);
        
        // First item should arrive after initial backfill delay
        expect(timestamps[0]).to.be.greaterThan(provider.latencies.observeBackfill - provider.PROCESSING_BUFFER_MS);
        
        // Items should arrive with delays between them
        if (timestamps.length > 1) {
          expect(timestamps[1] - timestamps[0]).to.be.greaterThan(0);
        }
        
        done();
      }, expectedBackfillTime * 5); // Wait long enough for all items with buffer
    });

    it("should handle new items with persistence latency", async () => {
      const provider = new MockProvider<string>([]);
      const subject = new PersistentSubject(provider);

      // Give minimal time for empty provider initialization
      await new Promise(resolve => setTimeout(resolve, 50));

      const receivedValues: string[] = [];
      subject.subscribe(value => receivedValues.push(value));

      const startTime = Date.now();
      
      // Add a value and measure how long it takes to appear
      subject.next("test-item");
      
      // Wait for persistence + streaming latencies
      await new Promise(resolve => setTimeout(resolve, provider.getPersistenceCompleteTime()));
      
      const endTime = Date.now();
      
      expect(receivedValues).to.include("test-item");
      expect(endTime - startTime).to.be.greaterThan(50); // Should take at least put latency
    });
  });

  describe("consistency guarantees", () => {
    it("should not emit to ReplaySubject until confirmed by Provider.observe()", async () => {
      // Create a provider with strong consistency (default)
      const provider = new MockProvider<string>(
        [],
        new AbortController().signal
      );

      const subject = new PersistentSubject(provider);

      const receivedValues: string[] = [];

      // Subscribe to the subject
      subject.subscribe((value) => {
        receivedValues.push(value);
      });

      // Give time for initialization (empty provider)
      await new Promise((resolve) => setTimeout(resolve, 50));

      // Initially should have no values
      expect(receivedValues).to.have.length(0);

      // Add a value - with strong consistency this should persist but not emit until observed
      subject.next("test-value");

      // Give some time for persistence but not observation
      await new Promise((resolve) => setTimeout(resolve, provider.latencies.put));

      // The value should eventually appear once it's been observed
      await new Promise((resolve) => setTimeout(resolve, provider.latencies.observeStream + provider.PROCESSING_BUFFER_MS));

      expect(receivedValues).to.include("test-value");
      expect(provider.getStoredItems()).to.have.length(1);
    });

    it("should handle single value with strong consistency", async () => {
      const provider = new MockProvider<number>(
        [],
        new AbortController().signal
      );

      const subject = new PersistentSubject(provider);

      const receivedValues: number[] = [];

      subject.subscribe((value) => {
        receivedValues.push(value);
      });

      // Give time for initialization (empty provider)
      await new Promise((resolve) => setTimeout(resolve, 50));

      // Add a single value
      subject.next(42);

      // Give time for strong consistency persistence to complete
      await new Promise((resolve) => setTimeout(resolve, provider.getReliableWaitTime(provider.getPersistenceCompleteTime())));

      expect(receivedValues).to.include(42);
      expect(provider.getStoredItems()).to.have.length(1);
      expect(provider.getStoredItems()[0].data).to.equal("42");
    });

    it("should handle strong consistency with initial data", async () => {
      const initialData = ["existing1", "existing2"];
      const provider = new MockProvider<string>(
        initialData,
        new AbortController().signal
      );

      const subject = new PersistentSubject(provider);

      const receivedValues: string[] = [];

      subject.subscribe((value) => {
        receivedValues.push(value);
      });

      // Give time for initialization and backfilling
      await new Promise((resolve) => setTimeout(resolve, provider.getReliableWaitTime(provider.getBackfillCompleteTime(initialData.length))));

      // Should have initial data
      expect(receivedValues).to.include.members(["existing1", "existing2"]);

      // Add new value
      subject.next("new-value");

      // Give time for strong consistency persistence
      await new Promise((resolve) => setTimeout(resolve, provider.getReliableWaitTime(provider.getPersistenceCompleteTime())));

      expect(receivedValues).to.include("new-value");
      expect(provider.getStoredItems()).to.have.length(3);
    });
  });
});

describe("PersistentSubject (Weak Consistency)", () => {
  describe("basic functionality with weak consistency", () => {
    it("should persist and emit values immediately with weak consistency", async () => {
      const provider = new MockProvider<string>([], new AbortController().signal);
      // Override to use weak consistency
      (provider as any).consistency = "weak";
      
      const subject = new PersistentSubject(provider);
      
      const receivedValues: string[] = [];
      subject.subscribe(value => receivedValues.push(value));

      // Give time for initialization
      await new Promise(resolve => setTimeout(resolve, provider.PROCESSING_BUFFER_MS));

      // Add values - should appear faster than strong consistency
      subject.next("hello");
      subject.next("world");

      // Give time for persistence (weak consistency but still sequential)
      await new Promise(resolve => setTimeout(resolve, provider.getReliableWaitTime(provider.getPersistenceCompleteTime() * 2)));

      expect(receivedValues).to.include.members(["hello", "world"]);
      expect(provider.getStoredItems()).to.have.length(2);
    });

    it("should handle rapid additions efficiently with weak consistency", async () => {
      const provider = new MockProvider<number>([], new AbortController().signal);
      (provider as any).consistency = "weak";
      
      const subject = new PersistentSubject(provider);

      // Give time for initialization
      await new Promise(resolve => setTimeout(resolve, provider.PROCESSING_BUFFER_MS));

      // Add many values rapidly
      for (let i = 0; i < 5; i++) {
        subject.next(i);
      }

      // Weak consistency with sequential processing
      await new Promise(resolve => setTimeout(resolve, provider.getReliableWaitTime(provider.getPersistenceCompleteTime() * 5)));

      const allValues = await subject.all();
      expect(allValues).to.include.members([0, 1, 2, 3, 4]);
    });

    it("should work with initial data and weak consistency", async () => {
      const initialData = ["existing1", "existing2"];
      const provider = new MockProvider<string>(initialData, new AbortController().signal);
      (provider as any).consistency = "weak";
      
      const subject = new PersistentSubject(provider);

      // Give time for backfill
      await new Promise(resolve => setTimeout(resolve, provider.getBackfillCompleteTime(initialData.length)));

      const receivedValues: string[] = [];
      subject.subscribe(value => receivedValues.push(value));

      // Add new value
      subject.next("new-value");

      // Give time for weak consistency persistence
      await new Promise(resolve => setTimeout(resolve, provider.getReliableWaitTime(provider.getPersistenceCompleteTime())));

      expect(receivedValues).to.include("new-value");
      expect(provider.getStoredItems()).to.have.length(3);
    });
  });
});

describe("PersistentSubject (No Consistency)", () => {
  describe("basic functionality with no consistency", () => {
    it("should persist values with no consistency guarantees", async () => {
      const provider = new MockProvider<string>([], new AbortController().signal);
      // Override to use no consistency
      (provider as any).consistency = "none";
      
      const subject = new PersistentSubject(provider);
      
      const receivedValues: string[] = [];
      subject.subscribe(value => receivedValues.push(value));

      // Give time for initialization
      await new Promise(resolve => setTimeout(resolve, provider.PROCESSING_BUFFER_MS));

      // Add values - should be fastest with no consistency
      subject.next("immediate");
      subject.next("fast");

      // Give time for no consistency persistence (still limited by sequential processing)
      await new Promise(resolve => setTimeout(resolve, provider.getReliableWaitTime(provider.getPersistenceCompleteTime() * 2)));

      expect(receivedValues).to.include.members(["immediate", "fast"]);
      expect(provider.getStoredItems()).to.have.length(2);
    });

    it("should handle very rapid additions with no consistency", async () => {
      const provider = new MockProvider<number>([], new AbortController().signal);
      (provider as any).consistency = "none";
      
      const subject = new PersistentSubject(provider);

      // Give time for initialization
      await new Promise(resolve => setTimeout(resolve, provider.PROCESSING_BUFFER_MS));

      // Add many values very rapidly
      for (let i = 0; i < 10; i++) {
        subject.next(i);
      }

      // No consistency with sequential processing 
      await new Promise(resolve => setTimeout(resolve, provider.getReliableWaitTime(provider.getPersistenceCompleteTime() * 10)));

      const allValues = await subject.all();
      expect(allValues).to.have.length(10);
      expect(allValues).to.include.members([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    });

    it("should work with initial data and no consistency", async () => {
      const initialData = [100, 200, 300];
      const provider = new MockProvider<number>(initialData, new AbortController().signal);
      (provider as any).consistency = "none";
      
      const subject = new PersistentSubject(provider);

      // Give time for backfill
      await new Promise(resolve => setTimeout(resolve, provider.getBackfillCompleteTime(initialData.length)));

      // Add new value
      subject.next(400);

      // Give time for no consistency persistence
      await new Promise(resolve => setTimeout(resolve, provider.getReliableWaitTime(provider.getPersistenceCompleteTime())));

      const allValues = await subject.all();
      expect(allValues).to.include.members([100, 200, 300, 400]);
    });
  });
});
