package misc.frameworks.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Harness {

  def initializeDummyTPCDSTables(spark: SparkSession): Unit = {
    import spark.implicits._

    // Create dummy orders DataFrame
    val orders = Seq(
      (10, "2021-01-01"),
      (18, "2021-01-02"),
      (26, "2021-01-03"),
      (30, "2021-01-04")
    ).toDF("id", "order_date")

    // Create dummy users DataFrame
    val users = Seq(
      (10, "Alice"),
      (18, "Bob"),
      (26, "Charlie"),
      (30, "David")
    ).toDF("id", "name")

    orders.createOrReplaceTempView("orders")
    users.createOrReplaceTempView("users")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Fuzzer")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    initializeDummyTPCDSTables(spark)

    val auto5 = spark.table("orders").as("orders")
    val auto0 = spark.table("users").as("users")
    val auto6 = auto5.filter(col("orders.id") > 17)
    val auto10 = auto6.orderBy(col("orders.id"),col("orders.id"))
    val auto8 = auto10.join(auto5, col("orders.id") < 27, "inner")
    val auto9 = auto8.as("U8Grk")
    val auto1 = auto9.join(auto0, col("users.id") === col("U8Grk.id"), "outer")
    val auto2 = auto1.withColumn("gm0sC", col("users.id") >= -6)
    val auto3 = auto2.join(auto0, col("users.id") === col("U8Grk.id"), "inner")
    val auto4 = auto3.limit(43)
    val sink = auto6.join(auto4, col("users.id") === col("U8Grk.id"), "right")
    sink.explain(true)

  }
}
