# Exercise 4 — RDD Transformations

## Practical Execution

### 1. Create RDD

Command:

val numbers = sc.parallelize(1 to 10)

### 2. Filter Even Numbers

Command:

val evens = numbers.filter(_ % 2 == 0)

Output:

List(2, 4, 6, 8, 10)

### 3. Multiply Values by 10

Command:

val multiplied = evens.map(_ * 10)

Output:

List(20, 40, 60, 80, 100)

### 4. Check RDD Lineage

Command:

println(multiplied.toDebugString)

The lineage showed the sequence of RDD transformations from the original ParallelCollectionRDD through filter and map.

### 5. Trigger Computation

Command:

multiplied.count()

Output:

5

## Practical Result

The RDD transformation pipeline was successfully executed using:

- filter()
- map()
- count()

The final transformed data was:

List(20, 40, 60, 80, 100)
