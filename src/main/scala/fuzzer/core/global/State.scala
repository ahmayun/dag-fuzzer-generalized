package fuzzer.core.global

import fuzzer.data.tables.TableMetadata
import org.apache.spark.sql.{DataFrame, SparkSession}

object State {

  var config: Option[FuzzerConfig] = None
  var iteration: Long = 0
  var src2TableMap: Map[String, TableMetadata] = Map()
  var sparkOption: Option[SparkSession] = None // set at runtime

  var finalDF: Option[DataFrame] = None
  var optDF: Option[DataFrame] = None
  var unOptDF: Option[DataFrame] = None

  var unOptRunException: Option[Throwable] = None
  var optRunException: Option[Throwable] = None
}
