# Exercise 7 — Pair RDD

## Practical Execution

### 1. Create Pair Data

Command:

val data = List(
  ("A", 10),
  ("B", 20),
  ("A", 30),
  ("C", 40)
)

Output:

List((A,10), (B,20), (A,30), (C,40))

### 2. Create Pair RDD

Command:

val pairRDD = sc.parallelize(data)

RDD Type:

ParallelCollectionRDD

### 3. Display Pair RDD

Command:

pairRDD.collect().toList

Output:

List((A,10), (B,20), (A,30), (C,40))

### 4. Aggregate Values by Key

Command:

val totals = pairRDD.reduceByKey(_ + _)

### 5. Display Aggregated Result

Command:

totals.collect().sortBy(_._1).foreach(println)

Output:

(A,40)
(B,20)
(C,40)

### 6. Count Results

Command:

totals.count()

Output:

3

### 7. Check Lineage

Command:

println(totals.toDebugString)

Observed lineage:

ParallelCollectionRDD
        |
    reduceByKey
        |
    ShuffledRDD

The RDD contained 12 partitions.

## Practical Result

A Pair RDD was successfully created and `reduceByKey()` was used to aggregate values belonging to the same key.
