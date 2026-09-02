# Spark Employee Analysis

An Apache Spark project built with Scala and sbt for analyzing employee data using Spark DataFrames, RDDs, Spark SQL, aggregations, joins, partitioning, caching, and performance techniques.

## Technologies Used

- Apache Spark 3.5.6
- Scala 2.12.18
- Java 17
- sbt 2.x
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
- Perform simple aggregations using `agg()`
- Perform grouping aggregations using `groupBy()` + `agg()`
- Perform window aggregations and running totals
- Rank rows within customer partitions using `row_number()`
- Join customer and order DataFrames
- Aggregate customer spending after a join
- Inspect Spark physical execution plans
- Demonstrate the concepts behind shuffle and sort-merge joins
- Combine JOIN + aggregation + window functions in one practical analysis

## Existing Employee Analysis

The main employee application is:

`src/main/scala/EmployeeAnalysis.scala`

It reads employee data from:

`data/employees.csv`

The application loads the CSV into a Spark DataFrame, filters employees with salaries above 80,000, calculates a 10% bonus, groups employees by department, calculates average/max/min salary, and writes the analysis to `data/final_output`.

## New Project: Aggregations, Joins and Window Functions

The practical project from the supplied study material is implemented in:

`src/main/scala/AggregationsJoinsWindows.scala`

The program contains two examples and a combined exercise.

### Example 1 — Sales Analysis

Demonstrates:

1. Simple aggregation of the complete DataFrame.
2. Grouping aggregation by product.
3. Customer-level aggregation.
4. Window aggregation for customer running totals.
5. Window ranking of orders for each customer.

The sample sales data contains 8 orders for Chintan, Rahul, Priya, and Amit across Laptop and Mobile products.

### Example 2 — Customer + Orders Join

Demonstrates:

1. Customer DataFrame creation.
2. Orders DataFrame creation.
3. Inner join using `customer_id`.
4. Selecting the required joined columns.
5. Aggregating total sales and order count for each customer.
6. Inspecting the Spark execution plan with `explain(true)`.

### Combined Exercise

The final section combines JOIN + WINDOW operations to show, for every order:

- Customer ID
- Customer name
- City
- Order ID
- Order amount
- Total customer spending
- Order rank within that customer

This directly demonstrates the workflow:

`Customers -> JOIN Orders -> Window Total -> Window Rank`

## Important Spark Concepts Covered

| Operation | Purpose | Output |
|---|---|---|
| `agg()` | Aggregate entire DataFrame | Usually one row |
| `groupBy()` + `agg()` | Aggregate by key | One row per group |
| Window `sum()` | Calculate over related rows | Keeps original rows |
| `join()` | Combine DataFrames | Combined rows |
| Shuffle | Redistribute data | Expensive operation |
| Sort | Order data by join/group key | Prepared for merge |
| Sort-Merge Join | Join sorted partitions | Common large-data join strategy |

## How to Run

From the project directory:

```bash
sbt run
```

To package the project:

```bash
sbt package
```

If multiple main classes are detected, run the required application explicitly:

```bash
sbt "runMain AggregationsJoinsWindows"
```

## Expected Results for the New Sales Example

Simple aggregation:

- Total sales: 300000
- Average sales: 37500
- Minimum sale: 15000
- Maximum sale: 60000
- Number of orders: 8

Product aggregation:

- Laptop: total 210000, average 52500, 4 orders
- Mobile: total 90000, average 22500, 4 orders

Customer totals from the supplied sales data:

- Chintan: 75000
- Rahul: 75000
- Priya: 90000
- Amit: 60000

## Learning Outcome

This project demonstrates how Spark moves from basic DataFrame aggregation to grouped analysis, window calculations, relational joins, post-join aggregation, execution-plan inspection, and combined analytical workflows used in practical data engineering.
