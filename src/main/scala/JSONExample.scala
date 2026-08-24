import org.apache.spark.sql.SparkSession

object JSONExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("JSONExample")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read.json("data/employees.json")

    println("Employee data:")
    df.show()

    println("Engineering employees:")
    df.filter("dept = 'Engineering'").show()

    println("Average salary by department:")
    df.groupBy("dept")
      .avg("salary")
      .show()

    spark.stop()
  }
}
