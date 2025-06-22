package fuzzer.data.tables

import fuzzer.data.types.DataType

case class ColumnMetadata(
                           name: String,
                           dataType: DataType,
                           isNullable: Boolean = true,
                           isKey: Boolean = false,
                           defaultValue: Option[Any] = None,
                           metadata: Map[String, String] = Map.empty // e.g. comments, tags, etc.
                         ) {
  override def toString: String = {
    name
  }
}