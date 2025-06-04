import { from, Subject } from "rxjs";
import { mutex, semaphore } from "./src/cloud-rx/operators";
import { Shard as DynamoDBShard } from "@aws-sdk/client-dynamodb-streams";

const shards: DynamoDBShard[] = [
  {
    ShardId: "shardId-00000001748849499693-604b6900",
    SequenceNumberRange: {
      StartingSequenceNumber: "50467400003764017941281022",
      EndingSequenceNumber: "50467400003764017941281022",
    },
    ParentShardId: "shardId-00000001748834024200-d15e9c42",
  },
  {
    ShardId: "shardId-00000001748850662198-4c4e36e8",
    SequenceNumberRange: {
      StartingSequenceNumber: "50536900000919850762998647",
      EndingSequenceNumber: "50536900000919850762998647",
    },
    ParentShardId: "shardId-00000001748834724123-84a8070c",
  },
  {
    ShardId: "shardId-00000001748853979436-3eee3c9c",
    SequenceNumberRange: {
      StartingSequenceNumber: "50777300003138735951447676",
      EndingSequenceNumber: "50973100000281805341854991",
    },
    ParentShardId: "shardId-00000001748839394610-2641f42c",
  },
  {
    ShardId: "shardId-00000001748859430654-767c863b",
    SequenceNumberRange: {
      StartingSequenceNumber: "51004700000789746579073706",
      EndingSequenceNumber: "51004700000789746579073706",
    },
    ParentShardId: "shardId-00000001748844887560-0a4d0abf",
  },
  {
    ShardId: "shardId-00000001748865568790-465abcea",
    SequenceNumberRange: {
      StartingSequenceNumber: "51338100003930184812860886",
      EndingSequenceNumber: "51338100003930184812860886",
    },
    ParentShardId: "shardId-00000001748849499693-604b6900",
  },
  {
    ShardId: "shardId-00000001748866085756-4e892e29",
    SequenceNumberRange: {
      StartingSequenceNumber: "51372100000135855611307401",
      EndingSequenceNumber: "51372100000135855611307401",
    },
    ParentShardId: "shardId-00000001748850662198-4c4e36e8",
  },
  {
    ShardId: "shardId-00000001748870129537-8c3950e7",
    SequenceNumberRange: {
      StartingSequenceNumber: "51652200003468407398000083",
      EndingSequenceNumber: "51652200003468407398000083",
    },
    ParentShardId: "shardId-00000001748853979436-3eee3c9c",
  },
  {
    ShardId: "shardId-00000001748874746644-b393156e",
    SequenceNumberRange: {
      StartingSequenceNumber: "51832700000486288764094133",
      EndingSequenceNumber: "51832700000486288764094133",
    },
    ParentShardId: "shardId-00000001748859430654-767c863b",
  },
  {
    ShardId: "shardId-00000001748878732779-d1f78b16",
    SequenceNumberRange: {
      StartingSequenceNumber: "52050800001778973692192647",
      EndingSequenceNumber: "52050800001778973692192647",
    },
    ParentShardId: "shardId-00000001748865568790-465abcea",
  },
  {
    ShardId: "shardId-00000001748880411648-7323d918",
    SequenceNumberRange: {
      StartingSequenceNumber: "52146600002812678251995943",
      EndingSequenceNumber: "52146600002812678251995943",
    },
    ParentShardId: "shardId-00000001748866085756-4e892e29",
  },
  {
    ShardId: "shardId-00000001748885117158-d5f61276",
    SequenceNumberRange: {
      StartingSequenceNumber: "52463500001172141192025980",
      EndingSequenceNumber: "52463500001172141192025980",
    },
    ParentShardId: "shardId-00000001748870129537-8c3950e7",
  },
  {
    ShardId: "shardId-00000001748890848886-f09ebd00",
    SequenceNumberRange: {
      StartingSequenceNumber: "52704600000679631924787470",
      EndingSequenceNumber: "52704600000679631924787470",
    },
    ParentShardId: "shardId-00000001748874746644-b393156e",
  },
  {
    ShardId: "shardId-00000001748894717405-d5cc4fe7",
    SequenceNumberRange: {
      StartingSequenceNumber: "52915400003200796760861996",
      EndingSequenceNumber: "52915400003200796760861996",
    },
    ParentShardId: "shardId-00000001748878732779-d1f78b16",
  },
  {
    ShardId: "shardId-00000001748896497634-ab9e0aeb",
    SequenceNumberRange: {
      StartingSequenceNumber: "53018000004400111998196677",
      EndingSequenceNumber: "53018000004400111998196677",
    },
    ParentShardId: "shardId-00000001748880411648-7323d918",
  },
  {
    ShardId: "shardId-00000001748898300748-10a03d29",
    SequenceNumberRange: {
      StartingSequenceNumber: "53177700002345772457449070",
      EndingSequenceNumber: "54042500002115931320446067",
    },
    ParentShardId: "shardId-00000001748885117158-d5f61276",
  },
  {
    ShardId: "shardId-00000001748906145454-589476ab",
    SequenceNumberRange: {
      StartingSequenceNumber: "53532200000568714424390915",
      EndingSequenceNumber: "53532200000568714424390915",
    },
    ParentShardId: "shardId-00000001748890848886-f09ebd00",
  },
  {
    ShardId: "shardId-00000001748908425270-ae1d1010",
    SequenceNumberRange: {
      StartingSequenceNumber: "53657900001742886479593060",
      EndingSequenceNumber: "53657900001742886479593060",
    },
    ParentShardId: "shardId-00000001748894717405-d5cc4fe7",
  },
  {
    ShardId: "shardId-00000001748910422129-93c452cf",
    SequenceNumberRange: {
      StartingSequenceNumber: "53771800002048730199458490",
      EndingSequenceNumber: "53771800002048730199458490",
    },
    ParentShardId: "shardId-00000001748896497634-ab9e0aeb",
  },
  {
    ShardId: "shardId-00000001748913439282-ae6a2d11",
    SequenceNumberRange: {
      StartingSequenceNumber: "54066800002584304834436612",
      EndingSequenceNumber: "54297900000354227489310499",
    },
    ParentShardId: "shardId-00000001748898300748-10a03d29",
  },
  {
    ShardId: "shardId-00000001748921751442-9f2095f3",
    SequenceNumberRange: {
      StartingSequenceNumber: "54378900004069786422912901",
      EndingSequenceNumber: "54378900004069786422912901",
    },
    ParentShardId: "shardId-00000001748908425270-ae1d1010",
  },
  {
    ShardId: "shardId-00000001748922005760-495d386c",
    SequenceNumberRange: {
      StartingSequenceNumber: "54390700000510233827269054",
      EndingSequenceNumber: "54390700000510233827269054",
    },
    ParentShardId: "shardId-00000001748906145454-589476ab",
  },
  {
    ShardId: "shardId-00000001748923040868-5b47ffe9",
    SequenceNumberRange: {
      StartingSequenceNumber: "54453800000330355305370778",
      EndingSequenceNumber: "54453800000330355305370778",
    },
    ParentShardId: "shardId-00000001748910422129-93c452cf",
  },
  {
    ShardId: "shardId-00000001748928432926-177d2787",
    SequenceNumberRange: {
      StartingSequenceNumber: "54920300001497385598994309",
      EndingSequenceNumber: "54920300001497385598994309",
    },
    ParentShardId: "shardId-00000001748913439282-ae6a2d11",
  },
  {
    ShardId: "shardId-00000001748936013323-ce783b28",
    SequenceNumberRange: {
      StartingSequenceNumber: "55150200001885138811897326",
      EndingSequenceNumber: "55150200001885138811897326",
    },
    ParentShardId: "shardId-00000001748922005760-495d386c",
  },
  {
    ShardId: "shardId-00000001748937553563-39c51e55",
    SequenceNumberRange: {
      StartingSequenceNumber: "55234000002912023696750001",
      EndingSequenceNumber: "55234000002912023696750001",
    },
    ParentShardId: "shardId-00000001748921751442-9f2095f3",
  },
  {
    ShardId: "shardId-00000001748937899362-26a56e63",
    SequenceNumberRange: {
      StartingSequenceNumber: "55257900002518894283663209",
      EndingSequenceNumber: "55257900002518894283663209",
    },
    ParentShardId: "shardId-00000001748923040868-5b47ffe9",
  },
  {
    ShardId: "shardId-00000001748941907823-3f8d9e29",
    SequenceNumberRange: {
      StartingSequenceNumber: "55651800003278119417896918",
      EndingSequenceNumber: "55651800003278119417896918",
    },
    ParentShardId: "shardId-00000001748928432926-177d2787",
  },
  {
    ShardId: "shardId-00000001748951363094-396720e9",
    SequenceNumberRange: {
      StartingSequenceNumber: "55981100000558540999672476",
    },
    ParentShardId: "shardId-00000001748936013323-ce783b28",
  },
  {
    ShardId: "shardId-00000001748951643083-e80b4730",
    SequenceNumberRange: {
      StartingSequenceNumber: "56001400001855102835970445",
    },
    ParentShardId: "shardId-00000001748937899362-26a56e63",
  },
  {
    ShardId: "shardId-00000001748952952103-b5ac6def",
    SequenceNumberRange: {
      StartingSequenceNumber: "56067800002746169022449780",
    },
    ParentShardId: "shardId-00000001748937553563-39c51e55",
  },
  {
    ShardId: "shardId-00000001748955207897-64c43a61",
    SequenceNumberRange: {
      StartingSequenceNumber: "56371100002723073915495818",
    },
    ParentShardId: "shardId-00000001748941907823-3f8d9e29",
  },
];

console.log("=== Testing Concurrency Control Operators with All Shards ===");
console.log(`Total shards to process: ${shards.length}`);

// Test sequential processing with all shards
console.log("\n--- Sequential Processing (mutex behavior) ---");
const sequentialSubject = new Subject<DynamoDBShard>();
const startTime = Date.now();

sequentialSubject
  .pipe(
    mutex((shard, index) => {
      const shardStartTime = Date.now();
      console.log(`[${index + 1}/${shards.length}] Starting: ${shard.ShardId}`);

      return new Promise<DynamoDBShard>((resolve) => {
        // Simulate shard processing with varying durations
        const processingTime = 200 + Math.random() * 300; // 200-500ms
        setTimeout(() => {
          const duration = Date.now() - shardStartTime;
          console.log(
            `[${index + 1}/${shards.length}] Finished: ${
              shard.ShardId
            } (${duration}ms)`
          );
          resolve(shard);
        }, processingTime);
      });
    }) // Sequential processing (mutex)
  )
  .subscribe({
    next: (shard) => {
      // Count completed shards
      const completed =
        shards.findIndex((s) => s.ShardId === shard.ShardId) + 1;
      const elapsed = Date.now() - startTime;
      console.log(
        `✓ Processed ${completed}/${shards.length} shards (${elapsed}ms elapsed)`
      );
    },
    error: (err) => {
      console.error("❌ Error:", err);
    },
    complete: () => {
      const totalTime = Date.now() - startTime;
      console.log(
        `\n🎉 Sequential processing complete! Total time: ${totalTime}ms`
      );
      console.log(
        `Average time per shard: ${(totalTime / shards.length).toFixed(1)}ms`
      );

      // Start concurrent test after sequential completes
      startConcurrentTest();
    },
  });

// Emit all shards
shards.forEach((shard) => {
  sequentialSubject.next(shard);
});
sequentialSubject.complete();

function startConcurrentTest() {
  console.log("\n--- Concurrent Processing (concurrency=5) ---");
  const concurrentSubject = new Subject<DynamoDBShard>();
  const startTime = Date.now();
  let completedCount = 0;

  concurrentSubject
    .pipe(
      semaphore((shard, index) => {
        const shardStartTime = Date.now();
        console.log(
          `[${index + 1}/${shards.length}] Starting concurrent: ${
            shard.ShardId
          }`
        );

        return from(
          new Promise<DynamoDBShard>((resolve) => {
            // Simulate shard processing with varying durations
            const processingTime = 300 + Math.random() * 400; // 300-700ms
            setTimeout(() => {
              const duration = Date.now() - shardStartTime;
              console.log(
                `[${index + 1}/${shards.length}] Finished concurrent: ${
                  shard.ShardId
                } (${duration}ms)`
              );
              resolve(shard);
            }, processingTime);
          })
        );
      }, 5) // Allow 5 concurrent operations
    )
    .subscribe({
      next: () => {
        completedCount++;
        const elapsed = Date.now() - startTime;
        console.log(
          `✓ Concurrent processed ${completedCount}/${shards.length} shards (${elapsed}ms elapsed)`
        );
      },
      error: (err) => {
        console.error("❌ Concurrent Error:", err);
      },
      complete: () => {
        const totalTime = Date.now() - startTime;
        console.log(
          `\n🎉 Concurrent processing complete! Total time: ${totalTime}ms`
        );
        console.log(
          `Average time per shard: ${(totalTime / shards.length).toFixed(1)}ms`
        );
        console.log(
          `Speedup vs sequential: ~${(
            totalTime /
            shards.length /
            (totalTime / shards.length)
          ).toFixed(1)}x faster due to concurrency`
        );
      },
    });

  // Emit all shards for concurrent processing
  shards.forEach((shard) => {
    concurrentSubject.next(shard);
  });
  concurrentSubject.complete();
}
