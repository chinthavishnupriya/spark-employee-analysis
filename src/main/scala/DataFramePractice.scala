import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataFramePractice {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("DataFramePractice")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/employees.csv")

    println("1. Select name and salary:")
    df.select("name", "salary").show()

    println("2. Employees with salary above 80000:")
    df.filter(col("salary") > 80000).show()

    println("3. Salary increased by 10 percent:")
    df.withColumn("new_salary", col("salary") * 1.10).show()

    println("4. Employees sorted by salary:")
    df.orderBy(desc("salary")).show()

    println("5. Average salary by department:")
    df.groupBy("dept")
      .agg(avg("salary").alias("average_salary"))
      .show()

    spark.stop()
  }
}
