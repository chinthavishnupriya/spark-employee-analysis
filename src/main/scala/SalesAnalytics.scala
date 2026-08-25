import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SalesAnalytics {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SalesAnalytics")
      .master("local[*]")
      .getOrCreate()

    val sales = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/sales.csv")

    println("Sales Data:")
    sales.show()

    val salesWithTotal = sales.withColumn(
      "total_sales",
      col("quantity") * col("price")
    )

    println("Sales with Total:")
    salesWithTotal.show()

    println("Total Sales by Category:")
    salesWithTotal
      .groupBy("category")
      .agg(
        sum("total_sales").alias("total_sales"),
        sum("quantity").alias("total_quantity")
      )
      .orderBy(desc("total_sales"))
      .show()

    println("Total Sales by Product:")
    salesWithTotal
      .groupBy("product")
      .agg(
        sum("total_sales").alias("total_sales"),
        sum("quantity").alias("total_quantity")
      )
      .orderBy(desc("total_sales"))
      .show()

    println("Overall Sales:")
    salesWithTotal
      .agg(
        sum("total_sales").alias("total_sales"),
        sum("quantity").alias("total_quantity")
      )
      .show()

    salesWithTotal.write
      .mode("overwrite")
      .option("header", "true")
      .csv("data/sales_output")

    spark.stop()
  }
}
