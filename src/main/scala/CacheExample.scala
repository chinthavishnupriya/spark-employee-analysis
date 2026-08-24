import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object CacheExample {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("CacheExample")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/employees.csv")

    println("Before caching:")
    println(df.storageLevel)

    df.cache()

    println("After caching:")
    println(df.storageLevel)

    println("First action:")
    df.show()

    println("Second action:")
    df.groupBy("dept").avg("salary").show()

    df.unpersist()

    println("After unpersist:")
    println(df.storageLevel)

    spark.stop()
  }
}
