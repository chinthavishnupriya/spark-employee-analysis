# Exercise 16 — cache() and persist()

## Practical Execution

### 1. Create RDD

Command:

val nums = sc.parallelize(1 to 1000000, 4)

Partitions:

4

### 2. Create Squares RDD

Command:

val squares = nums.map(x => x * x)

Partitions:

4

### 3. Check Storage Level Before Caching

Command:

squares.getStorageLevel

Output:

StorageLevel(1 replicas)

### 4. Cache the RDD

Command:

squares.cache()

Command:

squares.getStorageLevel

Output:

StorageLevel(memory, deserialized, 1 replicas)

### 5. Trigger Caching

Command:

squares.count()

Output:

1000000

### 6. Execute Cached RDD Again

Command:

squares.count()

Output:

1000000

### 7. Verify Spark Storage UI

The Spark Web UI Storage page was opened.

Observed:

- RDD: MapPartitionsRDD
- Cached partitions: 4
- Fraction cached: 100%
- Size in memory: approximately 3.8 MiB
- Size on disk: 0.0 B

### 8. Remove Cache

Command:

squares.unpersist()

Then:

squares.getStorageLevel

Output:

StorageLevel(1 replicas)

### 9. Test persist()

Command:

import org.apache.spark.storage.StorageLevel

Command:

val cubes = nums.map(x => x * x * x)

Command:

cubes.persist(StorageLevel.MEMORY_ONLY)

Storage level:

StorageLevel(memory, deserialized, 1 replicas)

### 10. Trigger Persistence

Command:

cubes.count()

Output:

1000000

### 11. Remove Persisted Data

Command:

cubes.unpersist()

## Practical Result

RDD caching and persistence were successfully tested using:

- cache()
- persist(StorageLevel.MEMORY_ONLY)
- getStorageLevel
- unpersist()

The Spark Storage UI was also used to verify cached partitions and memory usage.
