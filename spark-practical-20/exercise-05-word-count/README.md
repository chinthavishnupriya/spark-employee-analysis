# Exercise 5 — Word Count with RDDs

## Practical Execution

### 1. Create Input RDD

Command:

val lines = sc.parallelize(List(
  "Spark is fast",
  "Spark is distributed",
  "Hadoop is distributed",
  "Spark is powerful"
))

### 2. Split Lines into Words

Command:

val words = lines.flatMap(_.split("\\s+"))

### 3. Create Key-Value Pairs

Command:

val pairs = words.map(word => (word, 1))

### 4. Count Words

Command:

val wordCounts = pairs.reduceByKey(_ + _)

### 5. Display Word Counts

Command:

wordCounts.collect().sortBy(_._1).foreach(println)

Output:

(Hadoop,1)
(Spark,3)
(distributed,2)
(fast,1)
(is,4)
(powerful,1)

### 6. Check RDD Lineage

Command:

println(wordCounts.toDebugString)

The lineage showed the RDD transformations used to create the word-count result.

### 7. Count Result Records

Command:

wordCounts.count()

Output:

6

## Practical Result

Word count was successfully performed using Spark RDD transformations:

flatMap → map → reduceByKey
