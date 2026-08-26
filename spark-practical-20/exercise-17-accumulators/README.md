# Exercise 17 — Spark Accumulators

## Practical Execution

### 1. Create Error Accumulator

Command:

val errorCount = sc.longAccumulator("Error Count")

Initial value:

0

### 2. Create Log Data

Command:

val logs = sc.parallelize(List(
  "INFO Application started",
  "ERROR File not found",
  "INFO Processing data",
  "ERROR Connection failed",
  "INFO Application completed"
))

### 3. Count Errors

Command:

logs.foreach { line =>
  if (line.startsWith("ERROR")) {
    errorCount.add(1)
  }
}

Accumulator value:

2

### 4. Create Warning Accumulator

Command:

val warningCount = sc.longAccumulator("Warning Count")

Initial value:

0

### 5. Create Message Data

Command:

val messages = sc.parallelize(List(
  "INFO Started",
  "WARNING Low memory",
  "ERROR Failed",
  "WARNING Disk space low",
  "INFO Completed"
))

### 6. Count Warnings

Command:

messages.foreach { line =>
  if (line.startsWith("WARNING")) {
    warningCount.add(1)
  }
}

Accumulator value:

2

### 7. Final Values

Command:

println("Errors = " + errorCount.value)

Output:

Errors = 2

Command:

println("Warnings = " + warningCount.value)

Output:

Warnings = 2

## Practical Result

Spark LongAccumulators were successfully used to count ERROR and WARNING messages during RDD execution.

Final values:

Errors = 2
Warnings = 2
