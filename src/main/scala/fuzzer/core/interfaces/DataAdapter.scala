package fuzzer.core.interfaces

import fuzzer.data.tables.TableMetadata

trait DataAdapter {
  def getTables(): Seq[TableMetadata]
  def getTableByName(name: String): Option[TableMetadata]
  def loadData(executor: CodeExecutor): Unit
}