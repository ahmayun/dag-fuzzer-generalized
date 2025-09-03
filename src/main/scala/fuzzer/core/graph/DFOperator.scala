package fuzzer.core.graph


import fuzzer.data.tables.TableMetadata

class DFOperator(val name: String, val id: Int) {

  var state: TableMetadata = null
  var varName: String = ""
  var stateView: Map[String, TableMetadata] = Map()

  def this(id: Int) {
    this(null, id)
  }

  override def toString: String = {
    s"DFOperator($name, $id)"
  }

}

object DFOperator {
  def fromMap(map: Map[String, Any]): DFOperator = {
    new DFOperator(
//      name=map("op").asInstanceOf[String],
      id=map("id").asInstanceOf[Int]
    )
  }

  def fromInt(id: Int): DFOperator = {
    new DFOperator(id=id)
  }

}
