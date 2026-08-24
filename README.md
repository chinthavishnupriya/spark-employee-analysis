# Spark Employee Analysis

An Apache Spark project built with Scala and sbt for analyzing employee data using Spark DataFrames, RDDs, Spark SQL, aggregations, joins, partitioning, caching, and performance techniques.

## Technologies Used

- Apache Spark 3.5.3
- Scala 2.12
- Java 17
- sbt 2.0.7
- Apache Spark SQL
- Linux / WSL2

## Project Features

- Read employee data from CSV and JSON
- Create and manipulate Spark DataFrames
- Perform filtering and transformations
- Calculate employee bonuses
- Group employees by department
- Calculate average, maximum, and minimum salaries
- Perform DataFrame joins
- Perform RDD operations
- Demonstrate Word Count
- Demonstrate Spark SQL
- Demonstrate partitioning
- Demonstrate caching and persistence
- Demonstrate `groupByKey()` and `reduceByKey()`
- Demonstrate `repartition()` and `coalesce()`
- Inspect Spark Jobs, Stages, Tasks, DAG, and Storage using Spark Web UI
- Export analysis results to CSV

## Main Project

The main application is:

`src/main/scala/EmployeeAnalysis.scala`

It reads employee data from:

`data/employees.csv`

The application:

1. Loads the CSV into a Spark DataFrame.
2. Filters employees with salaries above 80,000.
3. Calculates a 10% bonus.
4. Groups employees by department.
5. Calculates average, maximum, and minimum salary.
6. Writes the final analysis to CSV.

## Sample Input

```text
name,dept,salary
Rakesh,Engineering,95000
Kavish,Sales,72000
Chetan,Engineering,88000
Arjun,Sales,85000
Rahul,HR,65000
