import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object EmployeeAnalysis {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("EmployeeAnalysis")
      .master("local[*]")
      .getOrCreate()

    val employees = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/employees.csv")

    println("Original Employee Data:")
    employees.show()

    println("Employees with salary above 80000:")
    employees
      .filter(col("salary") > 80000)
      .show()

    val updatedEmployees = employees.withColumn(
      "bonus",
      col("salary") * 0.10
    )

    println("Employees with Bonus:")
    updatedEmployees.show()

    println("Average Salary by Department:")

    val departmentSalary = employees
      .groupBy("dept")
      .agg(
        avg("salary").alias("average_salary"),
        max("salary").alias("maximum_salary"),
        min("salary").alias("minimum_salary")
      )

    departmentSalary.show()

    departmentSalary.write
      .mode("overwrite")
      .option("header", "true")
      .csv("data/final_output")

    println("Final analysis written to data/final_output")

    spark.stop()
  }
}
