package fuzzer

import fuzzer.core.engine.{FuzzerEngine, FuzzerResults}
import fuzzer.core.global.FuzzerConfig
import fuzzer.factory.AdapterFactory
import fuzzer.core.interfaces.{CodeExecutor, CodeGenerator, DataAdapter}
import fuzzer.utils.random.Random
import fuzzer.utils.io.ReadWriteUtils._
import fuzzer.utils.json.JsonReader
import play.api.libs.json.JsValue


/**
 * Entry point for the DAG Fuzzer application.
 * This class is responsible for configuring and running the fuzzer engine
 * but delegates all fuzzing logic to the engine and API-specific implementations.
 */
object MainFuzzer {
  /**
   * Main method that processes command line arguments, initializes the fuzzer,
   * and runs the fuzzing campaign.
   *
   * @param args Command line arguments, expecting a path to a config file as the first argument
   */
  def main(args: Array[String]): Unit = {
    // Parse config from file or use default
    val config = FuzzerConfig.getSparkScalaConfig
    fuzzer.core.global.State.config = Some(config)
    // Set random seed for reproducibility
    Random.setSeed(config.seed)

    // Clean output directories
    deleteDir(config.dagGenDir)
    deleteDir(config.outDir)
    createDir(config.outDir)

    // Log basic information
    println(s"Starting fuzzing campaign with seed: ${config.seed}")
    println(s"Target API: ${config.targetAPI}")

    // Create API-specific components using factory
    val (dataAdapter, codeGenerator, codeExecutor) = AdapterFactory.createComponents(config)

    // Create and run the fuzzer engine
    val startTime = System.currentTimeMillis()
    val spec = JsonReader.readJsonFile(config.specPath)

    val engine = createFuzzerEngine(config, spec, dataAdapter, codeGenerator, codeExecutor)
    val results = engine.run()
    val elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000

    // Print final results
    println(s"Fuzzing campaign completed in $elapsedSeconds seconds")
    println(prettyPrintStats(results.stats))

    // Save detailed results to file
    saveResultsToFile(config, results, elapsedSeconds)
  }

  /**
   * Creates a fuzzer engine with the specified components.
   */
  private def createFuzzerEngine(
                                  config: FuzzerConfig,
                                  spec: JsValue,
                                  dataAdapter: DataAdapter,
                                  codeGenerator: CodeGenerator,
                                  codeExecutor: CodeExecutor
                                ): FuzzerEngine = {
    new FuzzerEngine(
      config = config,
      spec = spec,
      dataAdapter = dataAdapter,
      codeGenerator = codeGenerator,
      codeExecutor = codeExecutor
    )
  }

}