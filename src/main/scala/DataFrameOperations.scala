import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataFrameOperations {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrameOperations")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/employees.csv")

    println("Employees with salary above 80000:")
    df.filter(col("salary") > 80000).show()

    println("Employee names and salaries:")
    df.select("name", "salary").show()

    println("Employees sorted by salary:")
    df.orderBy(desc("salary")).show()

    println("Department statistics:")
    df.groupBy("dept")
      .agg(
        avg("salary").alias("average_salary"),
        max("salary").alias("highest_salary"),
        count("*").alias("employee_count")
      )
      .show()

    spark.stop()
  }
}
