package fuzzer.adapters.flink

import fuzzer.code.SourceCode
import fuzzer.core.global.FuzzerConfig
import fuzzer.core.graph.{DFOperator, Graph, Node}
import fuzzer.core.interfaces.{CodeExecutor, CodeGenerator, DataAdapter, ExecutionResult}
import fuzzer.data.tables.TableMetadata
import play.api.libs.json.JsValue

class FlinkCodeGenerator(config: FuzzerConfig, spec: JsValue, dag2CodeFunc: Graph[DFOperator] => SourceCode) extends CodeGenerator {

  override def getDag2CodeFunc: Graph[DFOperator] => SourceCode = dag2CodeFunc
}

class FlinkDataAdapter(config: FuzzerConfig) extends DataAdapter {

  override def getTables(): Seq[TableMetadata] = ???

  override def getTableByName(name: String): Option[TableMetadata] = ???

  override def loadData(executor: CodeExecutor): Unit = {
    ???
  }

}

class FlinkCodeExecutor(config: FuzzerConfig, spec: JsValue) extends CodeExecutor {

  override def execute(code: SourceCode): ExecutionResult = {

    ???
  }

  override def setupEnvironment(): Unit = {
    ???
  }

  override def teardownEnvironment(): Unit = {
    // Stop SparkSession
    // ...
    ???
  }

}
