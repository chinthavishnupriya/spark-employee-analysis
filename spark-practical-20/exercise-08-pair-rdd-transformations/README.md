# Exercise 8 — Pair RDD Transformations

## Practical Execution

### 1. Create Employee Pair RDD

Command:

val employees = sc.parallelize(List(
  ("IT", 50000),
  ("HR", 40000),
  ("IT", 60000),
  ("Sales", 45000),
  ("HR", 35000)
))

Output:

List((IT,50000), (HR,40000), (IT,60000), (Sales,45000), (HR,35000))

### 2. Filter High Salaries

Command:

val highSalary = employees.filter { case (_, salary) => salary >= 50000 }

Output:

List((IT,50000), (IT,60000))

### 3. Increase Salaries

Command:

val increasedSalary = employees.mapValues(_ + 5000)

Output:

List((IT,55000), (HR,45000), (IT,65000), (Sales,50000), (HR,40000))

### 4. Extract Departments

Command:

val departments = employees.keys

Output:

List(IT, HR, IT, Sales, HR)

### 5. Extract Salaries

Command:

val salaries = employees.values

Output:

List(50000, 40000, 60000, 45000, 35000)

### 6. Count Employees by Department

Command:

val departmentCount = employees.mapValues(_ => 1).reduceByKey(_ + _)

### 7. Display Department Counts

Command:

departmentCount.collect().sortBy(_._1).foreach(println)

Output:

(HR,2)
(IT,2)
(Sales,1)

### 8. Count Result Records

Command:

departmentCount.count()

Output:

3

### 9. Check RDD Lineage

Command:

println(departmentCount.toDebugString)

Observed lineage:

ParallelCollectionRDD
        |
    mapValues
        |
    ShuffledRDD

The final RDD contained 12 partitions.

## Practical Result

Pair RDD transformations were successfully executed using:

- filter()
- mapValues()
- keys
- values
- reduceByKey()

The employee data was filtered, transformed, and aggregated by department.
