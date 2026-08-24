import org.apache.spark.sql.SparkSession

object SparkSQLExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkSQLExample")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/employees.csv")

    df.createOrReplaceTempView("employees")

    println("Employees with salary above 80000:")
    spark.sql("""
      SELECT name, dept, salary
      FROM employees
      WHERE salary > 80000
    """).show()

    println("Department-wise average salary:")
    spark.sql("""
      SELECT dept, AVG(salary) AS average_salary
      FROM employees
      GROUP BY dept
    """).show()

    println("Highest paid employee:")
    spark.sql("""
      SELECT name, dept, salary
      FROM employees
      ORDER BY salary DESC
      LIMIT 1
    """).show()

    spark.stop()
  }
}
