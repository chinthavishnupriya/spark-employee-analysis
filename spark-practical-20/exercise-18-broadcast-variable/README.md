# Exercise 18 — Broadcast Variable

## Practical Execution

### 1. Create Lookup Map

Command:

val lookup = Map(
  "E1" -> "Critical",
  "E2" -> "Warning",
  "E3" -> "Info"
)

Output:

Map(E1 -> Critical, E2 -> Warning, E3 -> Info)

### 2. Broadcast the Lookup Map

Command:

val lookupBroadcast = sc.broadcast(lookup)

Command:

lookupBroadcast.value

Output:

Map(E1 -> Critical, E2 -> Warning, E3 -> Info)

### 3. Create Accumulator

Command:

val unknownCount = sc.longAccumulator("unknownCount")

Initial value:

0

### 4. Create Error-Code RDD

Command:

val codes = sc.parallelize(List(
  "E1",
  "E2",
  "E1",
  "E3",
  "E9"
))

Output:

List(E1, E2, E1, E3, E9)

### 5. Apply Broadcast Lookup

Command:

val labeled = codes.map { code =>
  if (lookupBroadcast.value.contains(code)) {
    (code, lookupBroadcast.value(code))
  } else {
    unknownCount.add(1)
    (code, "Unknown")
  }
}

### 6. Display Labeled Results

Command:

labeled.collect().toList

Output:

List(
  (E1,Critical),
  (E2,Warning),
  (E1,Critical),
  (E3,Info),
  (E9,Unknown)
)

### 7. Check Unknown Count

Command:

println(unknownCount.value)

Output:

1

### 8. Check RDD Lineage

Command:

println(labeled.toDebugString)

Observed:

MapPartitionsRDD
 |
ParallelCollectionRDD

Partitions:

12

## Practical Result

A lookup Map was successfully broadcast to the Spark executors.

The broadcast variable was used to convert error codes into labels.

An accumulator counted codes that were not present in the lookup map.

Final unknown code count:

1
