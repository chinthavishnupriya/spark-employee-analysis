import org.apache.spark.sql.SparkSession

object JoinExample {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("JoinExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val employees = Seq(
      (1, "Rakesh", 101),
      (2, "Kavish", 102),
      (3, "Chetan", 101),
      (4, "Arjun", 102),
      (5, "Rahul", 103)
    ).toDF("emp_id", "name", "dept_id")

    val departments = Seq(
      (101, "Engineering"),
      (102, "Sales"),
      (103, "HR")
    ).toDF("dept_id", "dept_name")

    println("Employees:")
    employees.show()

    println("Departments:")
    departments.show()

    println("Inner Join:")
    employees.join(departments, "dept_id").show()

    println("Left Join:")
    employees.join(departments, Seq("dept_id"), "left").show()

    spark.stop()
  }
}
