# Exercise 6 — RDD Actions

## Practical Execution

### 1. Create RDD

Command:

val nums = sc.parallelize(1 to 10)

### 2. collect()

Command:

nums.collect().toList

Output:

List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

### 3. count()

Command:

nums.count()

Output:

10

### 4. first()

Command:

nums.first()

Output:

1

### 5. take()

Command:

nums.take(5).toList

Output:

List(1, 2, 3, 4, 5)

### 6. reduce()

Command:

nums.reduce(_ + _)

Output:

55

### 7. foreach()

Command:

nums.foreach(println)

Output:

The RDD values were printed through Spark execution.

### 8. Spark UI Verification

Spark Web UI was opened at:

http://localhost:4040/jobs/

Completed Spark jobs were verified in the Jobs page.

## Practical Result

RDD actions were successfully executed using:

- collect()
- count()
- first()
- take()
- reduce()
- foreach()

The Spark UI was also used to verify the generated jobs.
