# Exercise 10 — Sorting Pair RDD

## Practical Execution

### 1. Create Pair RDD

Command:

val data = sc.parallelize(List(
  ("C", 30),
  ("A", 10),
  ("D", 40),
  ("B", 20),
  ("A", 50)
))

Output:

List((C,30), (A,10), (D,40), (B,20), (A,50))

### 2. Sort by Key

Command:

val sortedByKey = data.sortByKey()

Output:

List((A,10), (A,50), (B,20), (C,30), (D,40))

### 3. Sort by Value

Command:

val sortedByValue = data.sortBy(_._2)

Output:

List((A,10), (B,20), (C,30), (D,40), (A,50))

### 4. Sort by Value in Descending Order

Command:

val descending = data.sortBy(_._2, ascending = false)

Output:

List((A,50), (D,40), (C,30), (B,20), (A,10))

### 5. Check RDD Lineage

Commands:

println(sortedByKey.toDebugString)

println(sortedByValue.toDebugString)

Both sorting operations produced shuffle-based RDD lineage.

### 6. Count Records

Command:

sortedByKey.count()

Output:

5

## Practical Result

Pair RDD sorting was successfully performed using:

- sortByKey()
- sortBy()
- descending value sorting

The original 5 records were successfully sorted by key and by value.
