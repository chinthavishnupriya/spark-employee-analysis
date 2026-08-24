import org.apache.spark.sql.SparkSession

object SparkSQLPractice {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkSQLPractice")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/employees.csv")

    df.createOrReplaceTempView("employees")

    println("1. All employees:")
    spark.sql("SELECT * FROM employees").show()

    println("2. Engineering employees:")
    spark.sql("""
      SELECT name, salary
      FROM employees
      WHERE dept = 'Engineering'
    """).show()

    println("3. Employee count by department:")
    spark.sql("""
      SELECT dept, COUNT(*) AS employee_count
      FROM employees
      GROUP BY dept
    """).show()

    println("4. Highest salary in each department:")
    spark.sql("""
      SELECT dept, MAX(salary) AS highest_salary
      FROM employees
      GROUP BY dept
    """).show()

    println("5. Employees sorted by salary:")
    spark.sql("""
      SELECT name, dept, salary
      FROM employees
      ORDER BY salary DESC
    """).show()

    spark.stop()
  }
}
