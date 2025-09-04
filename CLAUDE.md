# CLAUDE.md - Debugging Guide for sEtcd Server

This file contains instructions to help debug and work with the sEtcd codebase.

## Quick Testing

### Run DynamoDB Local Tests
```bash
make test SERVE=dynamodb-local
```

### Run Microsoft etcd3 Tests
```bash
make test SERVE=dynamodb-local RUN=microsoft-etcd3
```

### Run Kubernetes etcd3 Tests
```bash
make test SERVE=dynamodb-local RUN=kubernetes-etcd3
```

### Run Specific Tests
```bash
make test SERVE=dynamodb-local RUN=microsoft-etcd3 WHAT="crud|lease"
```

## Codebase Structure

### Core Components
- `src/server.ts` - Main server entry point
- `src/context.ts` - Application context and dependencies
- `src/routes.ts` - Route definitions

### Handlers (Business Logic)
- `src/handlers/kv.ts` - Key-value operations (get, put, delete, range)
- `src/handlers/watch.ts` - Watch/subscription functionality
- `src/handlers/lease.ts` - Lease management and keep-alive
- `src/handlers/auth.ts` - Authentication
- `src/handlers/cluster.ts` - Cluster operations
- `src/handlers/maintenance.ts` - Maintenance operations

### Storage Layer
- `src/storage/` - Storage abstraction layer
- `src/cloud-rx/` - Cloud-specific reactive extensions for DynamoDB

### Utilities
- `src/util/` - Common utilities (logging, errors, async helpers)

## Debugging Tips

### Watch Handler (`src/handlers/watch.ts`)
- The `progressNotify` feature uses `concat(of(res), progressNotify)` to emit immediate response then start interval
- Avoid `from([of(...), interval])` pattern as it blocks immediate responses
- Use `mergeMap` for concurrent operations, `switchMap` for cancellation

### Lease Handler (`src/handlers/lease.ts`)
- Keep-alive uses similar pattern: `concat(immediate, loop)` for immediate response + interval
- Lease TTL is tracked and auto-revoked when expired

### Storage Backend
- DynamoDB Local for testing: `make test SERVE=dynamodb-local`
- Remote DynamoDB: `make test SERVE=dynamodb-remote` (requires AWS credentials)
- **Debugging Tip**: If you're debugging with "dynamodb-remote" keep testing with it, only fall back to "dynamodb-local" once you've confirmed that "dynamodb-remote" is working

### Common RxJS Patterns
- `switchMap` - Cancel previous, start new (good for search/debounce)
- `mergeMap` - Run concurrently (good for independent operations)
- `concatMap` - Run sequentially (good for ordered operations)
- `concat` - Emit first observable, then second (good for immediate + delayed)
- `forkJoin` - Wait for all observables to complete, emit array of last values
- `combineLatest` - Emit whenever any source emits, with latest values from all

### Concurrency Control Operators (`src/cloud-rx/util.ts`)

When you need controlled concurrency in stream processing, use our custom operators:

#### `mutex(project)` - Sequential Processing
```typescript
// Process items one at a time (mutual exclusion)
source.pipe(
  mutex(shard => processShardSequentially(shard))
)

// Good for:
// - Database transactions that must not overlap
// - API calls that have strict rate limits
// - Operations that modify shared state
```

#### `semaphore(project, concurrent)` - Controlled Concurrency
```typescript
// Allow N concurrent operations
source.pipe(
  semaphore(shard => processShardConcurrently(shard), 5)
)

// Good for:
// - HTTP requests with connection pooling
// - CPU-bound tasks that benefit from parallelism
// - Batch processing with resource limits
```

#### When to Use Which
- **Use `mutex()`** when operations must be strictly sequential
- **Use `semaphore(n)`** when you want controlled parallelism
- **Use `mergeMap()`** when unlimited concurrency is acceptable
- **Use `concatMap()`** when order matters (different from mutex - concatMap preserves order)

### Common RxJS Pitfalls

#### Incorrect Higher-Order Operators
```typescript
// ❌ WRONG: switchMap cancels previous requests
source.pipe(
  switchMap(id => saveToDatabase(id)) // May cancel saves!
)

// ✅ CORRECT: mergeMap runs all requests concurrently
source.pipe(
  mergeMap(id => saveToDatabase(id))
)

// ❌ WRONG: mergeMap can overwhelm server with concurrent requests
source.pipe(
  mergeMap(query => searchAPI(query)) // Too many concurrent searches
)

// ✅ CORRECT: Use semaphore to limit concurrent searches
source.pipe(
  semaphore(query => searchAPI(query), 3) // Max 3 concurrent
)

// ✅ ALSO CORRECT: switchMap cancels outdated searches
source.pipe(
  switchMap(query => searchAPI(query))
)
```

#### Blocking Immediate Responses
```typescript
// ❌ WRONG: from([...]) blocks until first observable completes
return from([of(immediateResponse), interval(1000)])

// ✅ CORRECT: concat emits immediate response first, then starts interval
return concat(of(immediateResponse), interval(1000))
```

#### forkJoin vs combineLatest Confusion
```typescript
// ❌ WRONG: forkJoin waits for ALL to complete (intervals never complete)
forkJoin([userStream$, interval(1000)]) // Never emits!

// ✅ CORRECT: combineLatest emits whenever any source emits
combineLatest([userStream$, interval(1000)])

// ❌ WRONG: combineLatest needs ALL sources to emit at least once
combineLatest([userStream$, neverEmits$]) // Never emits!

// ✅ CORRECT: Use startWith or check if all sources will emit
combineLatest([userStream$, neverEmits$.pipe(startWith(null))])
```

#### Memory Leaks
```typescript
// ❌ WRONG: No takeUntil for cleanup
interval(1000).pipe(
  map(processData)
).subscribe() // Runs forever!

// ✅ CORRECT: Use takeUntil for cleanup
interval(1000).pipe(
  takeUntil(componentDestroy$),
  map(processData)
).subscribe()
```

#### Race Conditions in Initialization
```typescript
// ❌ WRONG: Immediate subscription before provider is ready
constructor(provider: Provider) {
  this.stream$ = provider.observe().pipe(shareReplay({ refCount: false }));
  this.stream$.subscribe(); // Race condition! Provider might not be initialized
}

// ✅ CORRECT: Idiomatic RxJS approach using AsyncSubject for provider initialization
constructor(provider: Provider) {
  this.stream$ = provider.observe().pipe(
    shareReplay({ refCount: false, scheduler: asyncScheduler })
  );
  
  // Start the shared connection only after provider is initialized
  this.initializeWhenReady();
}

private initializeWhenReady(): void {
  // Use AsyncSubject to wait for provider initialization
  // The provider emits on ready$ when init() completes
  this.subscriptions.push(
    this.provider.ready$.subscribe(() => {
      this.subscriptions.push(this.observe$.subscribe());
    })
  );
}
```

## Common Test Issues

### "subscribes before the connection is established"
- **Symptom**: Intermittent test failures, works when run individually
- **Cause**: Race condition between DynamoDB connection setup and stream subscription
- **Fix**: Use `AsyncSubject` pattern in provider to emit when initialization completes
- **Location**: `src/cloud-rx/persistent-subject.ts` and `src/cloud-rx/provider.ts`

### Environment Variables
Check `.cache/*.env` files for test environment configuration.

### Patches
Test patches are in `test/patches/` and applied automatically during testing.
To create new patches:
1. Run tests to checkout code
2. Edit code in `.cache/[test-name]/`
3. Run `make patches SERVE=dynamodb-local RUN=[test-name]`

## Lint & Type Checking

Always run lint and type checking after code changes:
```bash
# Check what commands are available
npm run --silent
```

## Git Workflow

Current branch: `etcd-3.6`
Main branch: `etcd-3.6`

## Debugging Reminders
- Make sure you remove the WHAT="..." (full test suite) before deciding that the fix you implemented is good
- test after making changes!

## AWS Credentials for Remote DynamoDB Testing

When testing with `SERVE=dynamodb-remote`, you need valid AWS credentials. If you get expired token errors:

1. **Get fresh credentials:**
   ```bash
   saml-to assume kingpin --headless
   ```
   This outputs export statements for the new credentials.

2. **Set environment variables in your shell:**
   ```bash
   export AWS_REGION=us-east-1 AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_SESSION_TOKEN=...
   ```
   Copy the exact export command from step 1.

3. **Run the test:**
   ```bash
   make test SERVE=dynamodb-remote RUN=microsoft-etcd3 WHAT="specific test name"
   ```

**Note:** The Makefile will create/update `.cache/dynamodb-remote.env` with your current environment variables, so make sure they're set before running the test.