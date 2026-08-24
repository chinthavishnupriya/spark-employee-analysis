import org.apache.spark.sql.SparkSession

object PartitionExample {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("PartitionExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = Seq(
      ("Rakesh", 95000),
      ("Kavish", 72000),
      ("Chetan", 88000),
      ("Arjun", 85000),
      ("Rahul", 65000),
      ("Suresh", 78000),
      ("Vijay", 91000),
      ("Kiran", 69000)
    ).toDF("name", "salary")

    println("Initial partitions:")
    println(df.rdd.getNumPartitions)

    val repartitioned = df.repartition(4)

    println("After repartition:")
    println(repartitioned.rdd.getNumPartitions)

    val reduced = repartitioned.coalesce(2)

    println("After coalesce:")
    println(reduced.rdd.getNumPartitions)

    reduced.show()

    spark.stop()
  }
}
