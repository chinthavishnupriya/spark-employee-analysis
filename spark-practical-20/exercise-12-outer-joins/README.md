# Exercise 12 — leftOuterJoin() and rightOuterJoin()

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
  (4, 70000)
))

Output:

List((1,50000), (2,60000), (4,70000))

### 3. leftOuterJoin()

Command:

val leftJoined = employees.leftOuterJoin(salaries)

Output:

(1,(Alice,Some(50000)))
(2,(Bob,Some(60000)))
(3,(Charlie,None))

### 4. rightOuterJoin()

Command:

val rightJoined = employees.rightOuterJoin(salaries)

Output:

(1,(Some(Alice),50000))
(2,(Some(Bob),60000))
(4,(None,70000))

### 5. Count Results

Command:

leftJoined.count()

Output:

3

Command:

rightJoined.count()

Output:

3

### 6. Check RDD Lineage

Commands:

println(leftJoined.toDebugString)

println(rightJoined.toDebugString)

Observed lineage for both operations:

ParallelCollectionRDD
        |
    CoGroupedRDD
        |
   MapPartitionsRDD

Both joined RDDs contained 12 partitions.

## Practical Result

leftOuterJoin() preserved all keys from the employee RDD.

rightOuterJoin() preserved all keys from the salary RDD.

Missing matching values were represented using None.
