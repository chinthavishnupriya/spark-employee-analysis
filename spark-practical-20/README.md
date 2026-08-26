# Apache Spark — 20 Practical Questions Exercise

This document contains the completed practical exercises covering Scala, Apache Spark RDDs, transformations, actions, Word Count, DAG execution, broadcast variables, accumulators, lazy evaluation, stages, tasks, driver, and executors.

## Environment

- Apache Spark: 3.5.3
- Scala: 2.12.18
- Java: 17.0.19
- Spark Master: local[*]
- OS: Ubuntu 26.04 LTS

---

# Section A — Scala Essentials

## Q1 — Scala Basics

Basic Scala expressions and values were practiced using `spark-shell`.

## Q2 — Collections

Scala collections and collection operations were practiced.

Example:

```scala
List(1, 2, 3).map(x => x * x)
