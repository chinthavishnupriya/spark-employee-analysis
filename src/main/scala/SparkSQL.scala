import org.apache.spark.sql.SparkSession

object SparkSQL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val employees = Seq(
      ("Rakesh", "Engineering", 95000),
      ("Kavish", "Sales", 72000),
      ("Chetan", "Engineering", 88000),
      ("Arjun", "Sales", 85000),
      ("Rahul", "HR", 65000)
    ).toDF("name", "dept", "salary")

    employees.createOrReplaceTempView("employees")

    val result = spark.sql(
      """
        SELECT dept, AVG(salary) AS average_salary
        FROM employees
        GROUP BY dept
        ORDER BY average_salary DESC
      """
    )

    result.show()

    spark.stop()
  }
}
