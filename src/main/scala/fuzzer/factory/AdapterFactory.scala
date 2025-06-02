package fuzzer.factory

import fuzzer.adapters.spark.{SparkCodeExecutor, SparkCodeGenerator, SparkDataAdapter}
import fuzzer.core.global.FuzzerConfig
import fuzzer.core.interfaces.{CodeExecutor, CodeGenerator, DataAdapter}
import fuzzer.utils.json.JsonReader

object AdapterFactory {
  def createComponents(config: FuzzerConfig): (DataAdapter, CodeGenerator, CodeExecutor) = {
    val spec = JsonReader.readJsonFile(config.specPath)

    config.targetAPI match {
      case "spark-scala" =>
        val dataAdapter = new SparkDataAdapter(config)
        val codeGenerator = new SparkCodeGenerator(config, spec)
        val codeExecutor = new SparkCodeExecutor(config, spec)
        (dataAdapter, codeGenerator, codeExecutor)

      case "beam" =>  ???
      case _ => throw new IllegalArgumentException(s"Unsupported API: ${config.targetAPI}")
    }
  }
}