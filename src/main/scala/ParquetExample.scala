import org.apache.spark.sql.SparkSession

object ParquetExample {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("ParquetExample")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/employees.csv")

    df.write
      .mode("overwrite")
      .parquet("data/employees_parquet")

    println("Data written to Parquet successfully")

    spark.stop()
  }
}
