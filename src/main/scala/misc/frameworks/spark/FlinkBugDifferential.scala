package misc.frameworks.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

object FlinkBugDifferential {
  // Define custom aggregation function
  object ProductAggregator extends Aggregator[Long, Long, Long] {
    def zero: Long = 1L

    def reduce(buffer: Long, data: Long): Long = buffer * data

    def merge(b1: Long, b2: Long): Long = b1 * b2

    def finish(reduction: Long): Long = reduction

    def bufferEncoder: Encoder[Long] = Encoders.scalaLong

    def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

  def main(args: Array[String]): Unit = {
    // Create Spark session
    val spark = SparkSession.builder()
      .appName("FlinkToSparkExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Register the custom aggregation function
    val customUdfAgg = udaf(ProductAggregator)
    spark.udf.register("custom_udf_agg", customUdfAgg)

    // Create source data
    val data = Seq(
      (101),
      (102),
      (101),
      (103)
    )

    // Create DataFrame
    val sourceTable = data.toDF("A").alias("df")

    // Apply transformations
    var t = sourceTable
      .groupBy(col("df.A"))
      .agg(avg(col("df.A")).alias("A"))

    t = t.distinct()

    t = t
      .groupBy(col("df.A"))
      .agg(max(col("df.A")).alias("A"))

    t = t
      .groupBy(col("df.A"))
      .agg(customUdfAgg(col("df.A")).alias("A"))

    // Show result
    t.show()

    // Stop the session
    spark.stop()
  }

}

