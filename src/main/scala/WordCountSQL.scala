import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WordCountSQL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("WordCountSQL")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = Seq(
      "spark is fast",
      "spark is fun"
    ).toDF("line")

    val wordCounts = df
      .select(explode(split($"line", " ")).alias("word"))
      .groupBy("word")
      .count()
      .orderBy("word")

    wordCounts.show()

    spark.stop()
  }
}
