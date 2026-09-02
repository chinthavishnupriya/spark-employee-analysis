import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object AggregationsJoinsWindows {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Aggregations Joins Windows")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    // =====================================================
    // EXAMPLE 1 — SALES ANALYSIS
    // Aggregations + Window Functions
    // =====================================================
    val sales = Seq(
      (1, "Chintan", "Laptop", 50000),
      (2, "Rahul", "Mobile", 20000),
      (3, "Priya", "Laptop", 60000),
      (4, "Amit", "Mobile", 15000),
      (5, "Chintan", "Mobile", 25000),
      (6, "Rahul", "Laptop", 55000),
      (7, "Priya", "Mobile", 30000),
      (8, "Amit", "Laptop", 45000)
    ).toDF("order_id", "customer", "product", "amount")

    println("===== ORIGINAL SALES DATA =====")
    sales.show()

    println("===== SIMPLE AGGREGATION =====")
    val totalSales = sales.agg(
      sum("amount").alias("total_sales"),
      avg("amount").alias("average_sales"),
      min("amount").alias("minimum_sales"),
      max("amount").alias("maximum_sales"),
      count("*").alias("number_of_orders")
    )
    totalSales.show()

    println("===== GROUPING AGGREGATION BY PRODUCT =====")
    val productSales = sales
      .groupBy("product")
      .agg(
        sum("amount").alias("total_sales"),
        avg("amount").alias("average_sales"),
        count("*").alias("number_of_orders")
      )
      .orderBy(desc("total_sales"))
    productSales.show()

    println("===== CUSTOMER AGGREGATION =====")
    val customerSales = sales
      .groupBy("customer")
      .agg(
        sum("amount").alias("total_spent"),
        count("*").alias("number_of_orders")
      )
      .orderBy(desc("total_spent"))
    customerSales.show()

    println("===== WINDOW AGGREGATION: RUNNING TOTAL =====")
    val customerWindow = Window
      .partitionBy("customer")
      .orderBy("order_id")

    val windowResult = sales.withColumn(
      "running_total",
      sum("amount").over(customerWindow)
    )
    windowResult.show()

    println("===== WINDOW RANKING =====")
    val rankingWindow = Window
      .partitionBy("customer")
      .orderBy(desc("amount"))

    val rankedSales = sales.withColumn(
      "rank",
      row_number().over(rankingWindow)
    )
    rankedSales.show()

    // =====================================================
    // EXAMPLE 2 — CUSTOMER + ORDERS JOIN
    // Join + Aggregation + Window Ranking
    // =====================================================
    val customers = Seq(
      (1, "Chintan", "Hyderabad"),
      (2, "Rahul", "Mumbai"),
      (3, "Priya", "Bangalore"),
      (4, "Amit", "Delhi")
    ).toDF("customer_id", "customer_name", "city")

    val orders = Seq(
      (101, 1, 50000),
      (102, 2, 20000),
      (103, 1, 25000),
      (104, 3, 30000),
      (105, 4, 15000),
      (106, 2, 40000)
    ).toDF("order_id", "customer_id", "amount")

    println("===== CUSTOMERS =====")
    customers.show()

    println("===== ORDERS =====")
    orders.show()

    println("===== INNER JOIN =====")
    val joinedData = customers.join(
      orders,
      customers("customer_id") === orders("customer_id"),
      "inner"
    )
    joinedData.show()

    println("===== SELECTED JOIN DATA =====")
    val result = joinedData.select(
      customers("customer_id"),
      customers("customer_name"),
      customers("city"),
      orders("order_id"),
      orders("amount")
    )
    result.show()

    println("===== CUSTOMER TOTAL SALES AFTER JOIN =====")
    val customerTotal = customers
      .join(
        orders,
        customers("customer_id") === orders("customer_id"),
        "inner"
      )
      .groupBy(
        customers("customer_id"),
        customers("customer_name"),
        customers("city")
      )
      .agg(
        sum(orders("amount")).alias("total_sales"),
        count(orders("order_id")).alias("order_count")
      )
      .orderBy(desc("total_sales"))
    customerTotal.show()

    println("===== EXECUTION PLAN =====")
    joinedData.explain(true)

    // =====================================================
    // ONE MORE EXERCISE
    // For every customer, show orders, total spending,
    // and rank orders by amount.
    // =====================================================
    println("===== JOIN + TOTAL SPENDING + ORDER RANK =====")
    val windowSpec = Window
      .partitionBy("customer_id")
      .orderBy(desc("amount"))

    val exerciseResult = joinedData
      .withColumn(
        "total_customer_spending",
        sum("amount").over(Window.partitionBy("customer_id"))
      )
      .withColumn(
        "order_rank",
        row_number().over(windowSpec)
      )
      .select(
        col("customer_id"),
        col("customer_name"),
        col("city"),
        col("order_id"),
        col("amount"),
        col("total_customer_spending"),
        col("order_rank")
      )
      .orderBy(col("customer_id"), col("order_rank"))

    exerciseResult.show()

    println("===== PROJECT COMPLETED =====")
    println("Implemented: simple aggregation, groupBy aggregation, window aggregation, window ranking, inner join, aggregation after join, execution plan, shuffle/sort-merge join inspection, and combined join + window analysis.")

    spark.stop()
  }
}
