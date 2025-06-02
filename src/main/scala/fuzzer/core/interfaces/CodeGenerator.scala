package fuzzer.core.interfaces

import fuzzer.code.SourceCode
import fuzzer.core.graph.{DFOperator, Graph, Node}

trait CodeGenerator {
  def generateCode(graph: Graph[DFOperator]): SourceCode
  def constructOperatorCall(node: Node[DFOperator], inputs: Seq[String]): String
  def supportedOperators: Set[String]
}
