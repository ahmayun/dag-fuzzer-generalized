package fuzzer.core.interfaces

import fuzzer.core.graph.{DFOperator, Node}
import fuzzer.data.tables.TableMetadata

trait DataAdapter {
  def getTables: Seq[TableMetadata]
  def getTableByName(name: String): Option[TableMetadata]
  def loadData(executor: CodeExecutor): Unit

  // This function is called right before a program is generated
  // The user may use this if they want to, say, alias table columns before the program is generated
  // Note: This only allows the user to change the metadata for the fuzzer and not alias actual table columns in the program. That must be done manually in codegen.
  def prepTableMetadata(sources: List[(Node[DFOperator], TableMetadata)]): List[(Node[DFOperator], TableMetadata)]
}