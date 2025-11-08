import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

case class ComplexObject(field1: Int, field2: Int)

object MinimalG13291 {

  def main(args: Array[String]): Unit = {
    // Initialize Spark
    val spark = SparkSession.builder()
      .appName("MinimalG13291")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
      .getOrCreate()

    import spark.implicits._

    // Create minimal test data for tpcds.promotion
    val promotionData = Seq(
      (1, "Channel1", "Demo1", "TV1"),
      (2, "Channel2", "Demo2", "TV2"),
      (3, "Channel3", "Demo3", "TV3"),
      (4, "Channel4", "Demo4", "TV4"),
      (5, "Channel5", "Demo5", "TV5")
    )

    val promotionSchema = StructType(Array(
      StructField("p_promo_id", IntegerType, nullable = true),
      StructField("p_channel_catalog", StringType, nullable = true),
      StructField("p_channel_demo", StringType, nullable = true),
      StructField("p_channel_tv", StringType, nullable = true)
    ))

    val promotionDF = spark.createDataFrame(
      promotionData.map { case (id, catalog, demo, tv) =>
        Row(id, catalog, demo, tv)
      },
      promotionSchema
    )

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

    val timeDimSchema = StructType(Array(
      StructField("t_time_sk", IntegerType, nullable = true),
      StructField("t_am_pm", StringType, nullable = true),
      StructField("t_meal_time", StringType, nullable = true),
      StructField("t_shift", BooleanType, nullable = true)
    ))

    val timeDimDF = spark.createDataFrame(
      timeDimData.map { case (sk, ampm, meal, shift) =>
        Row(sk, ampm, meal, shift)
      },
      timeDimSchema
    )

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

    // Original program logic
    val autonode_9 = spark.table("tpcds.promotion").select(spark.table("tpcds.promotion").columns.map(colName => col(colName).alias(s"${colName}_node_9")): _*).as("promotion_node_9")
    val autonode_8 = spark.table("tpcds.time_dim").select(spark.table("tpcds.time_dim").columns.map(colName => col(colName).alias(s"${colName}_node_8")): _*).as("time_dim_node_8")
    val autonode_7 = autonode_9.distinct()
    val autonode_6 = autonode_8.limit(34)
    val autonode_5 = autonode_7.offset(13)
    val autonode_4 = autonode_6.filter(col("time_dim_node_8.t_meal_time_node_8").cast("boolean") === lit(-43))
    val autonode_3 = autonode_4.join(autonode_5, col("promotion_node_9.p_promo_id_node_9") === col("time_dim_node_8.t_am_pm_node_8"), "outer")
    val autonode_2 = autonode_3.as("hoyrN")
    val autonode_1 = autonode_2.limit(61)
    val sink = autonode_1.select(col("hoyrN.p_channel_tv_node_9"), preloadedUDF(col("hoyrN.p_channel_demo_node_9")))

    sink.explain(true)

    // Show results
    println("\n=== Results ===")
    sink.show(false)

    // Cleanup
    spark.stop()
  }
}