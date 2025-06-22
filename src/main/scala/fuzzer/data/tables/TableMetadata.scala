package fuzzer.data.tables

case class TableMetadata(
                          private val _identifier: String,                    // Table name or ID
                          private val _columns: Seq[ColumnMetadata],
                          private val _metadata: Map[String, String] = Map.empty // Extra info (e.g. source, owner)
                        ) {
  private var tableName: String = _identifier
  private var _originalIdentifier: String = _identifier

  def columnNames: Seq[String] = _columns.map(_.name)

  def keyColumns: Seq[ColumnMetadata] = _columns.filter(_.isKey)
  def columns: Seq[ColumnMetadata] = _columns
  def metadata: Map[String, String] = _metadata

  def nonKeyColumns: Seq[ColumnMetadata] = _columns.filterNot(_.isKey)
  def identifier: String = tableName
  def originalIdentifier: String = _originalIdentifier
  def setOriginalIdentifier(v: String): Unit = {
    _originalIdentifier = v
  }
  def setIdentifier(v: String): Unit = {
    tableName = v
  }

  def copy(): TableMetadata = {
    val n = TableMetadata(identifier, columns, metadata)
    n.setOriginalIdentifier(this.originalIdentifier)
    n
  }

  override def toString: String = {
    s"""Table[id="$identifier,cols=[${columns.mkString(",")}]"]"""
  }

  def filterColumns(cols: Array[String]): TableMetadata = {
    val n = TableMetadata(identifier, columns.filter {
      colMd =>
//        println(s"Checking if ${colMd.name} in ${cols.mkString(",")}")
        cols.contains(colMd.name)
    }, metadata)

//    println(s"FINAL COLS AFTER FILTER ${n.columns.mkString(",")}")
    n.setOriginalIdentifier(this.originalIdentifier)
    n
  }

}