package bugs

import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object MinimalG13291 {
  case class ComplexObject(field1: Int, field2: Int)

  def main(args: Array[String]): Unit = {
    // Initialize Spark
    val spark = SparkSession.builder()
      .appName("MinimalG13291")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // Create minimal test data for tpcds.promotion
    val promotionData = Seq(
      (1, "Channel1", "Demo1", "TV1"),
      (2, "Channel2", "Demo2", "TV2"),
      (3, "Channel3", "Demo3", "TV3"),
      (4, "Channel4", "Demo4", "TV4"),
      (5, "Channel5", "Demo5", "TV5")
    )

    val promotionDF = promotionData.toDF("p_promo_id", "p_channel_catalog", "p_channel_demo", "p_channel_tv")

    // Create minimal test data for tpcds.time_dim
    val timeDimData = Seq(
      (1, "AM", "breakfast", true),
      (2, "PM", "lunch", false),
      (3, "AM", "dinner", true),
      (4, "PM", "snack", false),
      (5, "AM", "breakfast", true),
      (1, "PM", "lunch", true),
      (2, "AM", "dinner", false),
      (3, "PM", "snack", true)
    )

    val timeDimDF = timeDimData.toDF("t_time_sk", "t_am_pm", "t_meal_time", "t_shift")

    // Register as temporary tables
    promotionDF.createOrReplaceTempView("promotion")
    timeDimDF.createOrReplaceTempView("time_dim")

    // Create database and tables
    spark.sql("CREATE DATABASE IF NOT EXISTS tpcds")
    promotionDF.write.mode("overwrite").saveAsTable("tpcds.promotion")
    timeDimDF.write.mode("overwrite").saveAsTable("tpcds.time_dim")

    // Define the UDF
    val preloadedUDF = udf((s: Any) => {
      val r = scala.util.Random.nextInt()
      ComplexObject(r, r)
    }).asNondeterministic()

    val promotionTable = spark.table("tpcds.promotion")
    val timeDimTable = spark.table("tpcds.time_dim")
    val promotionWithOffset = promotionTable.offset(13)
    val filteredTimeDim = timeDimTable.filter(col("time_dim.t_meal_time").cast("boolean") === lit(-43))
    val outerJoinResult = filteredTimeDim.join(promotionWithOffset, col("promotion.p_promo_id") === col("time_dim.t_am_pm"), "outer")
    val limitedResult = outerJoinResult.limit(61)
    val sink = limitedResult.select(col("promotion.p_channel_tv"), preloadedUDF(col("promotion.p_channel_demo")))

    sink.explain(true)

    spark.stop()
  }
}