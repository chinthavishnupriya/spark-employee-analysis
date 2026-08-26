# Exercise 15 — repartition() and coalesce()

## Practical Execution

### 1. Create RDD

Command:

val nums = sc.parallelize(1 to 20, 4)

Partitions:

4

### 2. Increase Partitions Using repartition()

Command:

val repartitioned = nums.repartition(8)

Partitions:

8

Lineage showed:

MapPartitionsRDD
    |
CoalescedRDD
    |
ShuffledRDD
    |
ParallelCollectionRDD

### 3. Decrease Partitions Using coalesce()

Command:

val coalesced = repartitioned.coalesce(4)

Partitions:

4

Lineage showed:

CoalescedRDD
    |
MapPartitionsRDD
    |
CoalescedRDD
    |
ShuffledRDD
    |
ParallelCollectionRDD

### 4. Verify Data

Command:

coalesced.collect().toList

Output:

List(11, 18, 12, 19, 1, 6, 13, 20, 2, 7, 14, 3, 8, 15, 4, 9, 5, 10, 16, 17)

The data remained unchanged, but the order changed because of repartitioning.

### 5. Count Records

Command:

coalesced.count()

Output:

20

### 6. Compare Partitions

Original:

4

After repartition(8):

8

After coalesce(4):

4

## Practical Result

The RDD was successfully repartitioned from 4 to 8 partitions and then coalesced back to 4 partitions.

The lineage demonstrated that repartition() introduced a shuffle, while coalesce() produced a CoalescedRDD.

All 20 records were preserved.
