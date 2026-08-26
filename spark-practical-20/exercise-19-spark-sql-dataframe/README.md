# Exercise 19 — Spark SQL and DataFrame

## Practical Execution

### 1. Create Employee Data

Command:

val employeeData = Seq(
  (1, "Alice", "IT", 50000),
  (2, "Bob", "HR", 40000),
  (3, "Charlie", "IT", 60000),
  (4, "David", "Sales", 45000),
  (5, "Eva", "HR", 55000)
)

### 2. Create DataFrame

Command:

val employeesDF = employeeData.toDF("id", "name", "department", "salary")

### 3. Display DataFrame

Command:

employeesDF.show()

Output:

Alice    IT       50000
Bob      HR       40000
Charlie  IT       60000
David    Sales    45000
Eva      HR       55000

### 4. Check Schema

Command:

employeesDF.printSchema()

Schema:

id          integer
name        string
department  string
salary      integer

### 5. Select Columns

Command:

employeesDF.select("name", "salary").show()

### 6. Filter Salary

Command:

employeesDF.filter("salary >= 50000").show()

Matching employees:

Alice
Charlie
Eva

### 7. Group by Department

Command:

employeesDF.groupBy("department").avg("salary").show()

Output:

IT     55000.0
HR     47500.0
Sales  45000.0

### 8. Create Temporary View

Command:

employeesDF.createOrReplaceTempView("employees")

Command:

spark.catalog.tableExists("employees")

Output:

true

### 9. Execute Spark SQL

Command:

val result = spark.sql("""
  SELECT department, COUNT(*) AS employee_count, AVG(salary) AS average_salary
  FROM employees
  GROUP BY department
  ORDER BY department
""")

Output:

HR       2    47500.0
IT       2    55000.0
Sales    1    45000.0

### 10. Collect SQL Result

Command:

result.collect().foreach(println)

Output:

[HR,2,47500.0]
[IT,2,55000.0]
[Sales,1,45000.0]

### 11. Check Execution Plan

Command:

result.explain()

The physical plan showed:

LocalTableScan
    |
HashAggregate
    |
Exchange / Shuffle
    |
HashAggregate
    |
Exchange / Shuffle
    |
Sort

Adaptive Spark execution was also shown in the physical plan.

### 12. Count SQL Results

Command:

result.count()

Output:

3

## Practical Result

A Spark DataFrame was successfully created and queried using both DataFrame operations and Spark SQL.

The practical execution verified:

- DataFrame creation
- Schema inspection
- Column selection
- Filtering
- Grouping and aggregation
- Temporary SQL view
- SQL aggregation
- Physical execution plan
- Result counting
