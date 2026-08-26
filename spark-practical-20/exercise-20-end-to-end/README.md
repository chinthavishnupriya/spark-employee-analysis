# Exercise 20 — End-to-End Spark Transaction Processing

## Objective

This practical demonstrates an end-to-end Spark transaction-processing workflow using DataFrames, CSV, Parquet, aggregations, window functions, UDF, broadcast join, repartitioning, and execution-plan analysis.

## Input Data

Input file: data/transactions.csv

The dataset contains 10 transactions with transaction_id, customer_id, amount, transaction_type, and timestamp fields. Transaction T007 intentionally contains a null amount.

## 1. Read CSV Data

The CSV file was read using Spark DataFrameReader with header, schema inference, and timestamp format options.

## 2. Null Handling

One null amount was detected. The null was replaced with 0.0 using:

    val cleanedDF = transactionsDF.na.fill(0.0, Seq("amount"))

T007 was successfully converted to amount 0.0.

## 3. Transaction Value

A transaction_value column was created. Purchases were represented as positive values and refunds as negative values.

## 4. UDF Classification

A UDF classified transactions with amount greater than or equal to 1000.0 as HIGH_VALUE and the remaining transactions as NORMAL.

T002, with amount 1200.0, was classified as HIGH_VALUE.

## 5. Customer Aggregation

Transactions were grouped by customer_id using sum, count, and average.

Results:

C001  total=650.0   count=3
C002  total=1350.0  count=3
C003  total=750.0   count=2
C004  total=1300.0  count=2

The physical plan showed hash partitioning and shuffle during aggregation.

## 6. Window Function

A Window partitioned data by customer_id and ordered records by timestamp. A running_customer_value column was calculated using a cumulative sum.

Running totals included:

C001: 500 -> 400 -> 650
C002: 1200 -> 1500 -> 1350
C003: 750 -> 750
C004: 900 -> 1300

## 7. Broadcast Join

A small customer lookup DataFrame was joined using broadcast(customerLookup).

The physical plan showed BroadcastHashJoin and BroadcastExchange. All 10 transactions were successfully enriched with customer name and segment.

## 8. Repartitioning

The final DataFrame was repartitioned by customer_id into 4 partitions. RDD lineage showed a shuffle operation.

## 9. Parquet Write

The final data was written to:

output/parquet

Parquet output was successfully created.

## 10. Parquet Read

The Parquet data was read back successfully. The final DataFrame contained 10 records.

## 11. Final Segment Analysis

The final data was grouped by segment.

Premium: 2000.0 total, 6 transactions
Standard: 2050.0 total, 4 transactions

Overall net transaction value: 4050.0

## 12. Execution Plan and Lineage

The practical used explain() and toDebugString to inspect Spark execution plans and RDD lineage.

The plans demonstrated FileScan, HashAggregate, Exchange, Sort, Window processing, BroadcastHashJoin, and shuffle operations.

## Final Workflow

CSV Input -> DataFrame -> Null Handling -> Derived Column -> UDF Classification -> Customer Aggregation -> Window Aggregation -> Broadcast Join -> Repartition -> Parquet Write -> Parquet Read -> Segment Analysis -> DAG and Lineage Analysis

## Final Validation

Input transactions: 10
Customers: 4
Final records: 10
Segments: 2
Premium total: 2000.0
Standard total: 2050.0
Overall net value: 4050.0

The complete end-to-end Spark transaction-processing practical was successfully executed and validated.
