# Exercise 3 — RDD Creation

## Practical Execution

### 1. Create the Input Data

Command:

val data = List(10, 20, 30, 40, 50)

Output:

List(10, 20, 30, 40, 50)

### 2. Create an RDD

Command:

val rdd = sc.parallelize(data)

RDD Type:

ParallelCollectionRDD

### 3. Display RDD Data

Command:

rdd.collect().toList

Output:

List(10, 20, 30, 40, 50)

### 4. Check Number of Partitions

Command:

rdd.getNumPartitions

Output:

12

### 5. Count RDD Elements

Command:

rdd.count()

Output:

5

## Practical Result

The RDD was successfully created from a Scala collection using `sc.parallelize`.

The RDD contained 5 elements and was distributed across 12 partitions in the current `local[*]` Spark environment.
