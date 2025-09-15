package fuzzer.core.global

case class FuzzerConfig(
                         master: String,
                         targetAPI: String,
                         specPath: String,
                         exitAfterNSuccesses: Boolean,
                         N: Int,
                         d: Int,
                         p: Int,
                         outDir: String,
                         outExt: String,
                         timeLimitSec: Int,
                         dagGenDir: String,
                         localTpcdsPath: String,
                         seed: Int,
                         maxStringLength: Int,
                         updateLiveStatsAfter: Int,
                         intermediateVarPrefix: String,
                         finalVariableName: String,
                         probUDFInsert: Double,
                         maxListLength: Int,
                         randIntMin: Int,
                         randIntMax: Int,
                         randFloatMin: Double,
                         randFloatMax: Double,
                         logicalOperatorSet: Set[String]
                       )

object FuzzerConfig {

//  def fromJsonFile(path: String): FuzzerConfig = {
//    val fileContents = Source.fromFile(new File(path)).getLines().mkString
//    val conf = Json.parse(fileContents).as[FuzzerConfig]
//    conf
//  }


  def getSparkScalaConfig: FuzzerConfig = {
    FuzzerConfig(
      master = "local[*]",
      targetAPI = "spark-scala",
      specPath ="specs/spark-scala.json",
      exitAfterNSuccesses = true,
      N = 200,
      d = 200,
      p = 1,
      outDir = "./target/dagfuzz-out/spark-scala",
      outExt = ".scala",
      timeLimitSec = 10,
      dagGenDir = "dag-gen/DAGs/DAGs",
      localTpcdsPath = "tpcds-data",
      seed = "ahmad35".hashCode,
      maxStringLength = 5,
      updateLiveStatsAfter = 10,
      intermediateVarPrefix = "auto",
      finalVariableName = "sink",
      probUDFInsert = 0.1,
      maxListLength = 2,
      randIntMin = -50,
      randIntMax = 50,
      randFloatMin = -50.0,
      randFloatMax = 50.0,
      logicalOperatorSet = Set(">", "<", ">=", "<=")
    )
  }

  def getFlinkPythonConfig: FuzzerConfig = {
    FuzzerConfig(
      master = "local[*]",
      targetAPI = "flink-python",
      specPath ="specs/flink-python.json",
      exitAfterNSuccesses = true,
      N = 200,
      d = 200,
      p = 1,
      outDir = "./target/dagfuzz-out/flink-python",
      outExt = ".py",
      timeLimitSec = 10,
      dagGenDir = "dag-gen/DAGs/DAGs",
      localTpcdsPath = "tpcds-data",
      seed = "ahmad35".hashCode,
      maxStringLength = 5,
      updateLiveStatsAfter = 10,
      intermediateVarPrefix = "auto",
      finalVariableName = "sink",
      probUDFInsert = 0.1,
      maxListLength = 1,
      randIntMin = -50,
      randIntMax = 50,
      randFloatMin = -50.0,
      randFloatMax = 50.0,
      logicalOperatorSet = Set(">", "<", ">=", "<=")
    )
  }


}