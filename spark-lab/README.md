# Apache Spark — 20-Question Practical Lab

This folder records the completed Spark learning/practical lab carried out with Apache Spark 3.5.3, Scala 2.12.18, Java 17, and a local Spark master.

## Environment

- Apache Spark: 3.5.3
- Scala: 2.12.18
- Java: OpenJDK 17
- Spark master: `local[*]`
- Platform: Ubuntu 26.04 LTS / WSL2

## Completed Topics

### Section A — Scala Essentials

1. Scala collections and `for` comprehensions
2. `map()` with Scala collections
3. Scala traits and classes
4. Basic Spark/Scala shell usage
5. Lambda/function syntax
6. Scala syntax used in Spark transformations

### Section B — Spark and RDD Fundamentals

7. Spark context and local execution
8. Creating RDDs with `parallelize()`
9. RDD `map()` and `filter()` transformations
10. Partitions, `glom()`, lineage, and `toDebugString`

Example partition inspection:

```scala
val data = sc.parallelize(1 to 10, 3)
data.glom().collect().foreach(x => println(x.mkString("[", ", ", "]")))
```

Output:

```text
[1, 2, 3]
[4, 5, 6]
[7, 8, 9, 10]
```

## Section C — Word Count and PySpark

### 11. Word Count using Scala RDDs

Input file:

```text
hello spark
hello scala
spark scala
```

Pipeline:

```scala
val lines = sc.textFile("file:///home/vishnupriya/input.txt")
val words = lines.flatMap(line => line.split("\\s+"))
val pairs = words.map(word => (word.toLowerCase, 1))
val counts = pairs.reduceByKey(_ + _)
counts.collect().foreach(println)
```

Output:

```text
(scala,2)
(hello,2)
(spark,2)
```

### 12. PySpark

The same RDD concepts were tested in PySpark. The environment used Python 3.14.4 with Spark 3.5.3. Basic RDD creation and `collect()` worked, but Python RDD transformations using functions such as `lambda` caused a segmentation fault in this environment. This was recorded as an environment compatibility issue rather than treating the crashed execution as successful.

## Section D — Advanced Spark Concepts

### 13. Filtering application logs

Sample log data:

```text
2026-08-26 app E1 ERROR Database connection failed
2026-08-26 app E2 ERROR Invalid user request
2026-08-26 app E1 ERROR Database connection failed
2026-08-26 app E3 INFO Application started
2026-08-26 app E2 ERROR Invalid user request
```

Filtering errors:

```scala
val errorLines = logs.filter(line => line.contains("ERROR"))
```

Extracting error codes:

```scala
val severity = errorLines.map(line => (line.split(" ")(2), 1))
```

### 14. Broadcast Variables

A read-only severity lookup table was broadcast:

```scala
val severityMap = Map(
  "E1" -> "Critical",
  "E2" -> "Warning",
  "E3" -> "Info"
)

val broadcastMap = sc.broadcast(severityMap)
```

Practical use:

```scala
val errorCodes = sc.parallelize(List("E1", "E2", "E1", "E3"))
val descriptions = errorCodes.map(code => (code, broadcastMap.value(code)))
descriptions.collect().toList
```

Output:

```text
List((E1,Critical), (E2,Warning), (E1,Critical), (E3,Info))
```

### 15. Accumulators

An accumulator counted `ERROR` log lines:

```scala
val errorCount = sc.longAccumulator("errorCount")

logs.foreach { line =>
  if (line.contains("ERROR")) errorCount.add(1)
}

println(errorCount.value)
```

Output:

```text
4
```

Broadcast variables distribute read-only reference data to tasks, while accumulators allow tasks to contribute counters/metrics that the driver can read.

### 16. DAG, stages, tasks, and shuffle

Pipeline:

```scala
val nums = sc.parallelize(1 to 10, 3)
val evens = nums.filter(x => x % 2 == 0)
val pairs = evens.map(x => (x, x * 10))
val grouped = pairs.map { case (k, v) => ("all", v) }.reduceByKey(_ + _)
```

`reduceByKey()` created a `ShuffledRDD`, demonstrating a shuffle/wide dependency boundary.

Lineage:

```text
ParallelCollectionRDD
        |
      filter
        |
       map
        |
       map
        |
   reduceByKey
        |
     SHUFFLE
        |
   ShuffledRDD
```

Result:

```text
List((all,300))
```

### 17. Driver and Executors

The Spark application was checked with:

```scala
sc.appName
sc.master
```

Output included:

```text
Spark shell
local[*]
```

The driver coordinates the application, builds the execution plan, and schedules tasks. Executors perform tasks on partitions and can cache data.

A practical `take()` test showed:

```scala
nums.take(3)
```

Output:

```text
Array(1, 2, 3)
```

This was contrasted with `collect()`, which brings the complete result to the driver and therefore should be used carefully on large datasets.

## Section E — Execution and Performance

### 18. Transformations vs Actions

Transformation:

```scala
val testRDD = sc.parallelize(List(1, 2, 3, 4, 5))
val filteredTest = testRDD.filter(_ % 2 == 0)
```

Actions:

```scala
filteredTest.count()
filteredTest.collect().toList
filteredTest.saveAsTextFile("file:///home/vishnupriya/output/q18-result")
```

Results:

```text
count()       -> 2
collect()     -> List(2, 4)
saveAsTextFile -> output contains 2 and 4
```

### 19. Lazy Evaluation

A lazy transformation pipeline was created:

```scala
val data = sc.parallelize(1 to 10)
val result = data.filter(_ % 2 == 0).map(_ * 10)
```

Execution was triggered only by:

```scala
result.collect().toList
```

Result:

```text
List(20, 40, 60, 80, 100)
```

Lazy evaluation allows Spark to build the DAG before execution, pipeline compatible transformations, and avoid unnecessary intermediate work where possible.

### 20. Broadcast + Accumulator Together

Final integrated example:

```scala
val lookup = Map(
  "E1" -> "Critical",
  "E2" -> "Warning",
  "E3" -> "Info"
)

val lookupBroadcast = sc.broadcast(lookup)
val unknownCount = sc.longAccumulator("unknownCount")
val codes = sc.parallelize(List("E1", "E2", "E1", "E3", "E9"))

val labeled = codes.map { code =>
  if (lookupBroadcast.value.contains(code)) {
    (code, lookupBroadcast.value(code))
  } else {
    unknownCount.add(1)
    (code, "Unknown")
  }
}

labeled.collect().toList
println(unknownCount.value)
```

Output:

```text
List((E1,Critical), (E2,Warning), (E1,Critical), (E3,Info), (E9,Unknown))
1
```

This final example demonstrates both concepts together:

```text
Broadcast variable
    ↓
Read-only lookup available to tasks

Accumulator
    ↑
Tasks report the number of unknown codes
```

## Important Practical Notes

- `map`, `filter`, and `flatMap` are transformations.
- `collect`, `count`, `take`, and `saveAsTextFile` are actions.
- Transformations are evaluated lazily until an action is called.
- Narrow transformations can be pipelined within a stage.
- Wide dependencies such as `reduceByKey` introduce shuffle boundaries.
- `toDebugString` is useful for inspecting RDD lineage.
- `collect()` should not be used blindly on very large datasets because results are brought to the driver.
- Broadcast variables are read-only shared reference data.
- Accumulators are useful for counters and metrics.

## Environment Issue Recorded

During PySpark testing, Python 3.14.4 was being used with Spark 3.5.3. Basic PySpark RDD creation worked, but executing Python-side RDD transformations with `lambda` caused a segmentation fault. Scala Spark execution continued to work correctly. The issue was recorded rather than hiding it from the lab results.
