# Exercise 13 — RDD Set Operations

## Practical Execution

### 1. Create RDDs

Command:

val rdd1 = sc.parallelize(List(1, 2, 3, 4, 5))

val rdd2 = sc.parallelize(List(4, 5, 6, 7, 8))

### 2. union()

Command:

val unionRDD = rdd1.union(rdd2)

Output:

List(1, 2, 3, 4, 5, 4, 5, 6, 7, 8)

Count:

10

### 3. distinct()

Command:

val distinctRDD = unionRDD.distinct()

Output:

List(1, 2, 3, 4, 5, 6, 7, 8)

Count:

8

### 4. intersection()

Command:

val intersectionRDD = rdd1.intersection(rdd2)

Output:

List(4, 5)

Count:

2

### 5. RDD Lineage

unionRDD.toDebugString:

UnionRDD
 |
 +-- ParallelCollectionRDD
 +-- ParallelCollectionRDD

distinctRDD.toDebugString showed:

UnionRDD
 |
 ShuffledRDD
 |
 MapPartitionsRDD

intersectionRDD.toDebugString showed:

CoGroupedRDD
 |
 MapPartitionsRDD

Partition observations:

unionRDD: 24 partitions
distinctRDD: 24 partitions
intersectionRDD: 12 partitions

## Practical Result

RDD set operations were successfully executed using:

- union()
- distinct()
- intersection()

The operations were verified using collect(), count(), and toDebugString.
