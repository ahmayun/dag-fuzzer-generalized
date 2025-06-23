package fuzzer.factory

import fuzzer.adapters.spark.{SparkCodeExecutor, SparkCodeGenerator, SparkDataAdapter}
import fuzzer.adapters.flink.{FlinkCodeExecutor, FlinkCodeGenerator, FlinkDataAdapter}
import fuzzer.code.SourceCode
import fuzzer.core.global.FuzzerConfig
import fuzzer.core.graph.{DFOperator, Graph}
import fuzzer.core.interfaces.{CodeExecutor, CodeGenerator, DataAdapter}
import fuzzer.utils.json.JsonReader

object AdapterFactory {
  def createComponents(config: FuzzerConfig, dag2CodeFunc: Graph[DFOperator] => SourceCode): (DataAdapter, CodeGenerator, CodeExecutor) = {
    val spec = JsonReader.readJsonFile(config.specPath)

    config.targetAPI match {
      case "spark-scala" =>
        val dataAdapter = new SparkDataAdapter(config)
        val codeGenerator = new SparkCodeGenerator(config, spec, dag2CodeFunc)
        val codeExecutor = new SparkCodeExecutor(config, spec)
        (dataAdapter, codeGenerator, codeExecutor)

      case "flink-python" =>
        val dataAdapter = new FlinkDataAdapter(config)
        val codeGenerator = new FlinkCodeGenerator(config, spec, dag2CodeFunc)
        val codeExecutor = new FlinkCodeExecutor(config, spec)
        (dataAdapter, codeGenerator, codeExecutor)

      case _ =>
        throw new IllegalArgumentException(s"Unsupported API: ${config.targetAPI}")
    }
  }
}