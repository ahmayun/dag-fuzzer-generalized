package fuzzer

import fuzzer.code.SourceCode
import fuzzer.core.engine.FuzzerEngine
import fuzzer.core.global.FuzzerConfig
import fuzzer.core.graph.{DFOperator, Graph}
import fuzzer.core.interfaces.{CodeExecutor, CodeGenerator, DataAdapter}
import fuzzer.factory.AdapterFactory
import fuzzer.framework.UserImplSparkScala
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

  def main(args: Array[String]): Unit = {

    val config = args.head match {
      case "spark-scala" => FuzzerConfig.getSparkScalaConfig.copy(seed = 1234)
      case "flink-python" => FuzzerConfig.getFlinkPythonConfig.copy(seed = 1234)
      case _ => throw new IllegalArgumentException("Required args not provided")
    }

    val spec = JsonReader.readJsonFile(config.specPath)

    val dag2SparkScalaFunc = UserImplSparkScala.dag2SparkScala(spec) _
    val engine = createEngineFromConfig(config, spec, dag2SparkScalaFunc)

    fuzzer.core.global.State.config = Some(config)
    Random.setSeed(config.seed)

    deleteDir(config.dagGenDir)
    deleteDir(config.outDir)
    createDir(config.outDir)

    println(s"Starting fuzzing campaign with seed: ${config.seed}")
    println(s"Target API: ${config.targetAPI}")

    val startTime = System.currentTimeMillis()

    val results = engine.run()

    val elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000

    // Print final results
    println(s"Fuzzing campaign completed in $elapsedSeconds seconds")
    println(prettyPrintStats(results.stats))

    // Save detailed results to file
    saveResultsToFile(config, results, elapsedSeconds)
  }

}