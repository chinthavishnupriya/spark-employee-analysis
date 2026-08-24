import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("WordCount")
      .master("local[*]")
      .getOrCreate()

    val words = spark.sparkContext.parallelize(
      Seq("spark is fast", "spark is fun")
    )

    val counts = words
      .flatMap(_.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    counts.collect().foreach(println)

    spark.stop()
  }
}
