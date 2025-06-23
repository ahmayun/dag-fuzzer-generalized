package fuzzer.core.interfaces

import fuzzer.code.SourceCode
import fuzzer.core.graph.{DFOperator, Graph}


trait CodeGenerator {
  def getDag2CodeFunc: Graph[DFOperator] => SourceCode
}
