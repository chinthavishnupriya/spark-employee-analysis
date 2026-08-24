import org.apache.spark.sql.SparkSession

object App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkDataFramePractice")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = Seq(
      ("Rakesh", "Engineering", 95000),
      ("Kavish", "Sales", 72000),
      ("Chetan", "Engineering", 88000),
      ("Arjun", "Sales", 85000),
      ("Rahul", "HR", 65000)
    ).toDF("name", "dept", "salary")

    println("Employees with salary greater than 80000:")

    df.filter($"salary" > 80000).show()

    println("Average salary by department:")

    df.groupBy("dept")
      .avg("salary")
      .show()

    spark.stop()
  }
}
