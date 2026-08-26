# Exercise 14 — mapPartitions()

## Practical Execution

### 1. Create RDD

Command:

val nums = sc.parallelize(1 to 12, 4)

### 2. Check Partitions

Command:

nums.getNumPartitions

Output:

4

### 3. Calculate Sum of Each Partition

Command:

val partitionSums = nums.mapPartitions { iter =>
  Iterator(iter.sum)
}

Output:

List(6, 15, 24, 33)

The partition sums were:

Partition 1: 1 + 2 + 3 = 6
Partition 2: 4 + 5 + 6 = 15
Partition 3: 7 + 8 + 9 = 24
Partition 4: 10 + 11 + 12 = 33

### 4. Count Partition Results

Command:

partitionSums.count()

Output:

4

### 5. Verify Original Data

Command:

nums.collect().toList

Output:

List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)

### 6. Check mapPartitions() Lineage

Command:

println(partitionSums.toDebugString)

Observed:

MapPartitionsRDD
 |
ParallelCollectionRDD

Partitions: 4

### 7. Compare with map()

Command:

val normalMap = nums.map(x => x * 2)

Output:

List(2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24)

Lineage:

MapPartitionsRDD
 |
ParallelCollectionRDD

## Practical Result

mapPartitions() was successfully executed on all four partitions.

Each partition produced one sum, resulting in:

List(6, 15, 24, 33)

A normal map() transformation was also executed for comparison.
