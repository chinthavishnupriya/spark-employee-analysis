# Exercise 11 — RDD join()

## Practical Execution

### 1. Create Employee RDD

Command:

val employees = sc.parallelize(List(
  (1, "Alice"),
  (2, "Bob"),
  (3, "Charlie")
))

Output:

List((1,Alice), (2,Bob), (3,Charlie))

### 2. Create Salary RDD

Command:

val salaries = sc.parallelize(List(
  (1, 50000),
  (2, 60000),
  (3, 55000)
))

Output:

List((1,50000), (2,60000), (3,55000))

### 3. Join the RDDs

Command:

val joined = employees.join(salaries)

### 4. Display Joined Result

Command:

joined.collect().sortBy(_._1).foreach(println)

Output:

(1,(Alice,50000))
(2,(Bob,60000))
(3,(Charlie,55000))

### 5. Count Joined Records

Command:

joined.count()

Output:

3

### 6. Check RDD Lineage

Command:

println(joined.toDebugString)

Observed lineage:

ParallelCollectionRDD
        |
   CoGroupedRDD
        |
   MapPartitionsRDD

The join operation created a CoGroupedRDD as part of the execution lineage.

## Practical Result

Two Pair RDDs were successfully joined using join() based on their common keys.

The final RDD contained 3 joined records.
