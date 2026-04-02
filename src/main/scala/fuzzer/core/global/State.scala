package fuzzer.core.global

import fuzzer.data.tables.TableMetadata
import org.apache.spark.sql.{DataFrame, SparkSession}

object State {

  var config: Option[FuzzerConfig] = None
  var iteration: Long = 0
  /** Raw catalog metadata per source node id (from `zip(tables)` before `prepTableMetadata`). */
  var src2TableMap: Map[String, TableMetadata] = Map()

  /** Adapter-aliased metadata per source node id (same keys as `src2TableMap`; values from `prepTableMetadata`). Used when stage 3 is off so picks stay aliased but not state-view-aware. */
  var aliasedSrc2TableMap: Map[String, TableMetadata] = Map()
  var sparkOption: Option[SparkSession] = None // set at runtime

  var finalDF: Option[DataFrame] = None
  var optDF: Option[DataFrame] = None
  var unOptDF: Option[DataFrame] = None

  var unOptRunException: Option[Throwable] = None
  var optRunException: Option[Throwable] = None
}
