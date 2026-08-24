import org.apache.spark.sql.SparkSession

object CSVExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CSVExample")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/employees.csv")

    df.show()

    df.groupBy("dept")
      .avg("salary")
      .show()

    df.write
      .mode("overwrite")
      .option("header", "true")
      .csv("data/output")

    spark.stop()
  }
}
