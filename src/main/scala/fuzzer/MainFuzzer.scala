package fuzzer

import fuzzer.code.SourceCode
import fuzzer.core.engine.FuzzerEngine
import fuzzer.core.global.FuzzerConfig
import fuzzer.core.graph.{DFOperator, Graph}
import fuzzer.factory.AdapterFactory
import fuzzer.framework.{UserImplDaskPython, UserImplFlinkPython, UserImplPolarsPython, UserImplSparkScala, UserImplTFPython}
import fuzzer.utils.io.ReadWriteUtils._
import fuzzer.utils.json.JsonReader
import fuzzer.utils.random.Random
import play.api.libs.json.JsValue

object MainFuzzer {

  def createEngineFromConfig(config: FuzzerConfig, spec: JsValue, dag2CodeFunc: Graph[DFOperator] => SourceCode): FuzzerEngine = {

    val (dataAdapter, codeGenerator, codeExecutor) = AdapterFactory.createComponents(config, dag2CodeFunc)

    val engine = new FuzzerEngine(
      config = config,
      spec = spec,
      dataAdapter = dataAdapter,
      codeGenerator = codeGenerator,
      codeExecutor = codeExecutor
    )

    engine
  }

  private def parseArgs(args: Array[String], baseConfig: FuzzerConfig): FuzzerConfig = {
    def parseRec(remaining: List[String], config: FuzzerConfig): FuzzerConfig = {
      remaining match {
        case Nil => config
        case flag :: value :: tail if flag.startsWith("--") =>
          val updated = flag match {
            case "--master" => config.copy(master = value)
            case "--replay" => config.copy(replay = value.toBoolean)
            case "--artifacts-dir" => config.copy(artifactsDir = value)
            case "--target-api" => config.copy(targetAPI = value)
            case "--spec-path" => config.copy(specPath = value)
            case "--exit-after-n-successes" => config.copy(exitAfterNSuccesses = value.toBoolean)
            case "--n" => config.copy(N = value.toInt)
            case "--d" => config.copy(d = value.toInt)
            case "--p" => config.copy(p = value.toInt)
            case "--out-dir" => config.copy(outDir = value)
            case "--out-ext" => config.copy(outExt = value)
            case "--time-limit-sec" => config.copy(timeLimitSec = value.toInt)
            case "--dag-gen-dir" => config.copy(dagGenDir = value)
            case "--local-tpcds-path" => config.copy(localTpcdsPath = value)
            case "--seed" => config.copy(seed = value.hashCode)
            case "--max-string-length" => config.copy(maxStringLength = value.toInt)
            case "--update-live-stats-after" => config.copy(updateLiveStatsAfter = value.toInt)
            case "--intermediate-var-prefix" => config.copy(intermediateVarPrefix = value)
            case "--final-variable-name" => config.copy(finalVariableName = value)
            case "--prob-udf-insert" => config.copy(probUDFInsert = value.toDouble)
            case "--max-list-length" => config.copy(maxListLength = value.toInt)
            case "--rand-int-min" => config.copy(randIntMin = value.toInt)
            case "--rand-int-max" => config.copy(randIntMax = value.toInt)
            case "--rand-float-min" => config.copy(randFloatMin = value.toDouble)
            case "--rand-float-max" => config.copy(randFloatMax = value.toDouble)
            case "--logical-operator-set" => config.copy(logicalOperatorSet = value.split(",").map(_.trim).toSet)
            case "--debug-mode" => config.copy(debugMode = value.toBoolean)
            case unknown => throw new IllegalArgumentException(s"Unknown argument: $unknown")
          }
          parseRec(tail, updated)
        case flag :: Nil if flag.startsWith("--") =>
          throw new IllegalArgumentException(s"Flag $flag provided without value")
        case other :: _ =>
          throw new IllegalArgumentException(s"Invalid argument format: $other")
      }
    }

    try {
      parseRec(args.toList, baseConfig)
    } catch {
      case e: NumberFormatException =>
        throw new IllegalArgumentException(s"Invalid numeric value: ${e.getMessage}")
      case e: IllegalArgumentException => throw e
    }
  }

  def main(args: Array[String]): Unit = {

    if (args.isEmpty) {
      throw new IllegalArgumentException("Domain argument required: spark-scala, flink-python, or dask-python")
    }

    val domain = args.head
    val baseConfig = domain match {
      case "spark-scala" => FuzzerConfig.getSparkScalaConfig
      case "flink-python" => FuzzerConfig.getFlinkPythonConfig
      case "dask-python" => FuzzerConfig.getDaskPythonConfig
      case "tensorflow-python" => FuzzerConfig.getTensorflowPythonConfig
      case "polars-python" => FuzzerConfig.getPolarsPythonConfig
      case _ => throw new IllegalArgumentException(s"Unknown domain: $domain. Expected: spark-scala, flink-python, or dask-python")
    }

    val config = parseArgs(args.tail, baseConfig)

    val spec = JsonReader.readJsonFile(config.specPath)

    val dag2CodeFunc = domain match {
      case "spark-scala" => UserImplSparkScala.dag2SparkScala(spec) _
      case "flink-python" => UserImplFlinkPython.dag2FlinkPython(spec) _
      case "dask-python" => UserImplDaskPython.dag2DaskPython(spec) _
      case "tensorflow-python" => UserImplTFPython.dag2tensorflowPython(spec) _
      case "polars-python" => UserImplPolarsPython.dag2polarsPython(spec) _
      case _ => throw new IllegalArgumentException("Required args not provided")
    }
    val engine = createEngineFromConfig(config, spec, dag2CodeFunc)

    fuzzer.core.global.State.config = Some(config)
    Random.setSeed(config.seed)

    deleteDir(config.dagGenDir)
    deleteDir(config.outDir)
    createDir(config.outDir)

    println(s"Starting fuzzing campaign with seed: ${config.seed}")
    println(s"Target API: ${config.targetAPI}")

    val startTime = System.currentTimeMillis()

    val results = if(config.replay) engine.replay() else engine.run()

    val elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000

    // Print final results
    println(s"Fuzzing campaign completed in $elapsedSeconds seconds")
    println(prettyPrintStats(results.stats))

    // Save detailed results to file
    saveResultsToFile(config, results, elapsedSeconds)
  }

}