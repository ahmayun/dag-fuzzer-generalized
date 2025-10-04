package fuzzer.framework

import fuzzer.code.SourceCode
import fuzzer.core.graph.{DFOperator, Graph, Node}
import fuzzer.data.tables.{ColumnMetadata, TableMetadata}
import fuzzer.data.types.{BooleanType, DataType}
import fuzzer.utils.random.Random
import play.api.libs.json._

import scala.collection.mutable

object UserImplDaskPython {

  def constructDFOCall(spec: JsValue, node: Node[DFOperator], in1: String, in2: String): String = {
    val opName = node.value.name
    val opSpec = spec \ opName

    if (opSpec.isInstanceOf[JsUndefined]) {
      return opName
    }

    // Get operation type
    val opType = (opSpec \ "type").as[String]

    // Get parameters
    val parameters = (opSpec \ "parameters").as[JsObject]

    // Generate arguments based on parameters
    val args = generateArguments(node, parameters, opType, in2)

    // Construct the function call based on operation type
    opType match {
      case "source" => constructSourceCall(node, spec, opName, opType, parameters, args)
      case "unary" if opName == "groupby" => s"$in1.$opName(${args.mkString(", ")}).${constructAggFollowup(node, spec, opName, opType, parameters, args)}"
      case _ => s"$in1.$opName(${args.mkString(", ")})"
    }
  }

  private def constructAggFollowup(node: Node[DFOperator], spec: JsValue, opName: String, opType: String, parameters: JsObject, args: List[String]): String = {
    val (table, col) = pickRandomColumnFromReachableSources(node)
    // Choose between sum, mean, count, etc.
    val aggFuncs = Seq("sum", "mean", "count", "min", "max", "std", "var", "size")
    val chosenAggFunc = aggFuncs(scala.util.Random.nextInt(aggFuncs.length))

    val fullColName = constructFullColumnName(table, col)

    filterColumns(fullColName, node)
    propagateState(node)

    // Dask uses pandas-like aggregation syntax
    s"agg({'$fullColName': '$chosenAggFunc'}).reset_index()"
  }

  private def constructSourceCall(node: Node[DFOperator], spec: JsValue, opName: String, opType: String, parameters: JsObject, args: List[String]): String = {
    val tableName = fuzzer.core.global.State.src2TableMap(node.id).identifier
    opName match {
//      case "dd.read_csv" => s"""dd.read_csv("$tableName")"""
//      case "dd.read_parquet" => s"""dd.read_parquet("$tableName")"""
//      case _ => s"$opName(${args.mkString(", ")})"
      case _ => s"$tableName" // we are already loading the table at the oracle server
    }
  }

  def generateArguments(node: Node[DFOperator], parameters: JsObject, opType: String, in2: String): List[String] = {
    val paramNames = parameters.keys.toList

    paramNames.flatMap { paramName =>
      val param = parameters \ paramName
      val paramType = getParamType((param \ "type").toOption.getOrElse(JsString("str")))
      val required = (param \ "required").as[Boolean]
      val hasDefault = (param \ "default").isDefined

      // For binary operations with DataFrame parameter, use in2
      if (paramName == "other" && paramType == "DataFrame" && opType == "binary") {
        Some(in2)
      } else if (required || (!hasDefault && Random.nextBoolean())) {
        // Generate a value for required parameters or randomly for optional ones
        Some(s"$paramName=${generateRandomValue(node, param, paramType, paramName)}")
      } else {
        None // Skip optional parameter
      }
    }
  }

  def getParamType(typeJson: JsValue): String = {
    typeJson match {
      case JsString(t) => t
      case JsArray(types) => types(Random.nextInt(types.length)).as[String]
      case _ => "str" // Default to string
    }
  }

  def pickRandomReachableSource(node: Node[DFOperator]): Node[DFOperator] = {
    val sources = node.getReachableSources.toSeq
    assert(sources.nonEmpty, "Expected DAG sources to be non-empty")
    sources(Random.nextInt(sources.length))
  }

  def getAllColumns(node: Node[DFOperator], preferUnique: Boolean = true): Seq[(TableMetadata, ColumnMetadata)] = {
    val tablesColPairs = node.value.stateView.values.toSeq.flatMap { t =>
      t.columns.map(c => (t, c))
    }
//    println(s"[getAllColumns] tableColPairs.length = ${tablesColPairs.length} | stateView.size = ${node.value.stateView.size}")
    tablesColPairs
  }

  def pickRandomColumnFromReachableSources(node: Node[DFOperator], preferUnique: Boolean = true): (TableMetadata, ColumnMetadata) = {
    val tablesColPairs = getAllColumns(node, preferUnique).filter {
      case (_, col) =>
        col.metadata.get("gen-iteration") match {
          case None => true
          case Some(i) =>
            fuzzer.core.global.State.iteration.toString != i
        }
    }
    assert(tablesColPairs.nonEmpty, s"Expected columnNames to be non-empty: stateViewMap = ${node.value.stateView}")
    val pick = tablesColPairs(Random.nextInt(tablesColPairs.length))
    pick
  }

  def renameTables(newValue: String, node: Node[DFOperator]): Unit = {
    val dfOp = node.value

    // Rename each table metadata entry in the stateView
    val renamedStateView: Map[String, TableMetadata] = dfOp.stateView.map {
      case (id, tableMeta) =>
        val renamed = tableMeta.copy() // get a deep copy
        renamed.setIdentifier(newValue) // modify as needed
        id -> renamed
    }

    // Update this node's stateView with renamed copies
    dfOp.stateView = renamedStateView
  }

  def addColumn(value: String, node: Node[DFOperator]): Unit = {
    node.value.stateView = node.value.stateView + ("added" -> TableMetadata(
      _identifier = "",
      _columns = Seq(ColumnMetadata(name = value, dataType = DataType.generateRandom, metadata = Map("source" -> "runtime", "gen-iteration" -> fuzzer.core.global.State.iteration.toString))),
      _metadata = Map("source" -> "runtime", "gen-iteration" -> fuzzer.core.global.State.iteration.toString)
    ))
  }

  def filterColumns(paramVal: String, node: Node[DFOperator]): Unit = {
    node.value.stateView = node.value.stateView.map {
      case (tname, tmd) =>
        (tname -> tmd.filterColumns(Array(paramVal)))
    }
  }

  def updateSourceState(
                         node: Node[DFOperator],
                         param: JsLookupResult,
                         paramName: String,
                         paramType: String,
                         paramVal: String
                       ): Unit = {
    val effect = (param \ "state-effect").asOpt[String].get
    effect match {
      case "table-rename" => renameTables(paramVal, node)
      case "column-add" => addColumn(paramVal, node)
      case "column-filter" => filterColumns(paramVal, node)
      case "column-remove" => filterColumns(paramVal, node)
      case "column-rename" => renameTables(paramVal, node)
      case "index-change" => // Dask specific - handled in place
    }
  }

  def propagateState(startNode: Node[DFOperator]): Unit = {
    val visited = mutable.Set[String]()
    val queue = mutable.Queue[Node[DFOperator]]()
    queue.enqueueAll(startNode.children)

    while (queue.nonEmpty) {
      val current = queue.dequeue()
      if (!visited.contains(current.id)) {
        visited += current.id

        val currentDFOp = current.value
        val startStateView = startNode.value.stateView

        // Only update keys that are also present in startNode.stateView
        val updatedView = currentDFOp.stateView.map {
          case (key, _) if startStateView.contains(key) =>
            key -> startStateView(key).copy()
          case other =>
            other
        }

        currentDFOp.stateView = updatedView

        // Enqueue children
        queue.enqueueAll(current.children)
      }
    }
  }

  def generateOrPickString(
                            node: Node[DFOperator],
                            param: JsLookupResult,
                            paramName: String,
                            paramType: String
                          ): String = {

    val isStateAltering: Boolean = (param \ "state-altering").asOpt[Boolean].getOrElse(false)

    if (isStateAltering) {
      // Generate random string (e.g. for creating a new column)
      val gen = s"${Random.alphanumeric.take(fuzzer.core.global.State.config.get.maxStringLength).mkString}"
      updateSourceState(node, param, paramName, paramType, gen)
      propagateState(node)
      gen
    } else {
      // Pick a column name from reachable source nodes
      val (table, col) = pickRandomColumnFromReachableSources(node)
      constructFullColumnName(table, col)
    }
  }

  def constructFullColumnName(table: TableMetadata, col: ColumnMetadata): String = {
    // Dask uses simple column names like pandas
    s"${col.name}"
  }

  def pickTwoColumns(stateView: Map[String, TableMetadata]): Option[((TableMetadata, ColumnMetadata), (TableMetadata, ColumnMetadata))] = {

    // Step 1: Flatten all columns and group by DataType -> Map[DataType, List[(TableMetadata, ColumnMetadata)]]
    val columnsByType: Map[DataType, Seq[(TableMetadata, ColumnMetadata)]] = stateView
      .values
      .toSeq
      .flatMap { table =>
        table.columns.map(c => (c.dataType, (table, c)))
      }
      .groupBy(_._1)
      .view
      .mapValues(_.map(_._2))
      .toMap

    // Step 2: Filter to only those datatypes which have columns from at least 2 distinct tables
    val viableTypes: Seq[(DataType, Seq[(TableMetadata, ColumnMetadata)])] = columnsByType.toSeq
      .map { case (dt, cols) =>
        val tableGroups = cols.groupBy(_._1) // group by TableMetadata
        (dt, tableGroups)
      }
      .filter { case (_, tableGroups) => tableGroups.size >= 2 }
      .map { case (dt, tableGroups) =>
        (dt, tableGroups.values.flatten.toSeq)
      }

    // Step 3: Randomly pick a datatype from viable options
    if (viableTypes.isEmpty) {
      return None
    }

    val (_, candidates) = Random.shuffle(viableTypes).head

    // Step 4: Select two columns from different tables
    val byTable = candidates.groupBy(_._1).toSeq
    val shuffledPairs = Random.shuffle(byTable.combinations(2).toSeq)
    val chosenPair = shuffledPairs.head

    val col1 = Random.shuffle(chosenPair(0)._2).head
    val col2 = Random.shuffle(chosenPair(1)._2).head

    Some((col1, col2))
  }

  def pickMultiColumnsFromReachableSources(node: Node[DFOperator]): Option[((TableMetadata, ColumnMetadata), (TableMetadata, ColumnMetadata))] = {
    pickTwoColumns(node.value.stateView)
  }

  def generateMultiColumnExpression(
                                     node: Node[DFOperator],
                                     param: JsLookupResult,
                                     paramName: String,
                                     paramType: String
                                   ): Option[String] = {
    pickMultiColumnsFromReachableSources(node) match {
      case Some(pair) =>
        val ((table1, col1), (table2, col2)) = pair
        val fullColName1 = constructFullColumnName(table1, col1)
        val fullColName2 = constructFullColumnName(table2, col2)
        // Dask uses pandas-like column access
        val crossTableExpr = s"df['$fullColName1'] == df['$fullColName2']"
        Some(crossTableExpr)
      case None => None
    }
  }

  def pickColumnName(
                      node: Node[DFOperator],
                      param: JsLookupResult,
                      paramName: String,
                      paramType: String
                    ): String = {

    val isStateAltering: Boolean = (param \ "state-altering").asOpt[Boolean].getOrElse(false)

    val (table, col) = pickRandomColumnFromReachableSources(node)
    val fullColName = constructFullColumnName(table, col)

    if (isStateAltering) {
      // Generate random string (e.g. for creating a new column)
      updateSourceState(node, param, paramName, paramType, fullColName)
      propagateState(node)
    }
    fullColName
  }

  def generateFilterUDFCall(node: Node[DFOperator], args: List[(TableMetadata, ColumnMetadata)]): String = {
    val col = args.head._2
    val fullColName = constructFullColumnName(args.head._1, col)
    val chosenType = col.dataType.name.toLowerCase
    s"x['$fullColName'].apply(filter_udf_$chosenType)"
  }

  def generateComplexUDFCall(node: Node[DFOperator], args: List[(TableMetadata, ColumnMetadata)]): String = {
    val colNames = args.map { case (table, col) => s"'${constructFullColumnName(table, col)}'" }.mkString(", ")
    s"x[[$colNames]].apply(lambda row: preloaded_udf_complex(*row), axis=1)"
  }

  def generateUDFCall(node: Node[DFOperator], args: List[(TableMetadata, ColumnMetadata)]): String = {
    if (node.value.name.contains("filter") || node.value.name.contains("query"))
      generateFilterUDFCall(node, args)
    else
      generateComplexUDFCall(node, args)
  }

  def generateSingleColumnExpression(
                                      node: Node[DFOperator],
                                      param: JsLookupResult,
                                      paramName: String,
                                      paramType: String
                                    ): String = {

    val config = fuzzer.core.global.State.config.get
    val prob = config.probUDFInsert
    val chosenTuple@(table, col) = pickRandomColumnFromReachableSources(node)
    val fullColName = constructFullColumnName(table, col)

    val op = config.logicalOperatorSet.toVector(Random.nextInt(config.logicalOperatorSet.size))

    val intValue = config.randIntMin + Random.nextInt(config.randIntMax - config.randIntMin + 1)
    val floatValue = config.randFloatMin + Random.nextFloat() * (config.randFloatMax - config.randFloatMin)

    // Dask uses pandas operator syntax
    val daskOp = op match {
      case "==" => "=="
      case "!=" => "!="
      case ">" => ">"
      case "<" => "<"
      case ">=" => ">="
      case "<=" => "<="
      case _ => "=="
    }

    val intExpr = s"$fullColName $daskOp $intValue"
    val floatExpr = s"$fullColName $daskOp $floatValue"
    val stringExpr = s"$fullColName.str.len() $daskOp 5"
    val boolExpr = s"~$fullColName"
    val udfExpr = generateUDFCall(node, List(chosenTuple))

    if (Random.nextFloat() < prob) {
      udfExpr
    } else {
      col.dataType match {
        case fuzzer.data.types.IntegerType => intExpr
        case fuzzer.data.types.FloatType => floatExpr
        case fuzzer.data.types.StringType => stringExpr
        case fuzzer.data.types.BooleanType => boolExpr
        case fuzzer.data.types.LongType => intExpr
        case fuzzer.data.types.DecimalType => floatExpr
        case fuzzer.data.types.DateType => stringExpr
      }
    }
  }

  def generateColumnExpression(
                                node: Node[DFOperator],
                                param: JsLookupResult,
                                paramName: String,
                                paramType: String
                              ): String = {

    lazy val singleColExpr = generateSingleColumnExpression(node, param, paramName, paramType)
    if (node.isBinary) {
      generateMultiColumnExpression(node, param, paramName, paramType) match {
        case Some(expr) => expr
        case None => singleColExpr
      }
    } else {
      singleColExpr
    }
  }

  def generateRandomValue(node: Node[DFOperator], param: JsLookupResult, paramType: String, paramName: String): String = {

    val maxListLength = fuzzer.core.global.State.config.get.maxListLength
    // Try to get allowed values from the param JSON
    val allowedValues: Option[Seq[JsValue]] = (param \ "values").asOpt[Seq[JsValue]]

    // If allowed values are provided, pick one randomly
    allowedValues match {
      case Some(values) if values.nonEmpty =>
        val randomValue = values(Random.nextInt(values.length))
        randomValue match {
          case JsString(str) => s"'$str'"
          case other => other.toString
        }

      // Generate values if spec doesn't provide fixed options
      case _ =>
        paramType match {
          case "int" => Random.nextInt(100).toString
          case "bool" => if (Random.nextBoolean()) "True" else "False"
          case "str" | "string" => s"'${generateOrPickString(node, param, paramName, paramType)}'"
          case "str|list[str]" =>
            if (Random.nextBoolean()) s"'${generateOrPickString(node, param, paramName, paramType)}'"
            else s"[${(0 until Random.nextInt(maxListLength) + 1).map(_ => s"'${generateOrPickString(node, param, paramName, paramType)}'").mkString(", ")}]"
          case "list[str]" =>
            s"[${(0 until Random.nextInt(maxListLength) + 1).map(_ => s"'${generateOrPickString(node, param, paramName, paramType)}'").mkString(", ")}]"
          case "dict[str,str]" =>
            val numEntries = Random.nextInt(maxListLength) + 1
            val entries = (0 until numEntries).map { _ =>
              val key = generateOrPickString(node, param, paramName, "str")
              val value = generateOrPickString(node, param, paramName, "str")
              s"'$key': '$value'"
            }.mkString(", ")
            s"{$entries}"
          case "dict[str,Expression]" =>
            val numEntries = Random.nextInt(maxListLength) + 1
            val entries = (0 until numEntries).map { _ =>
              val key = Random.alphanumeric.take(8).mkString
              val (table, col) = pickRandomColumnFromReachableSources(node)
              val fullColName = constructFullColumnName(table, col)
              addColumn(key, node)
              propagateState(node)
              s"'$key': '$fullColName'"
            }.mkString(", ")
            s"{$entries}"
          case "Expression" =>
            s"'${generateColumnExpression(node, param, paramName, paramType)}'"
          case "callable" => "lambda x: x"
          case "scalar|dict" =>
            if (Random.nextBoolean()) Random.nextInt(100).toString
            else s"{'${pickColumnName(node, param, paramName, "str")}': ${Random.nextInt(100)}}"
          case _ => s"'${Random.alphanumeric.take(8).mkString}'"
        }
    }
  }

  def generatePreloadedUDF(): String = {
    s"""
       |import pandas as pd
       |import numpy as np
       |
       |# String Filter UDFs
       |def filter_udf_string(arg):
       |    return len(str(arg)) > 5
       |
       |# Integer Filter UDFs
       |def filter_udf_integer(arg):
       |    return arg > 0 and arg % 2 == 0
       |
       |# Decimal Filter UDFs
       |def filter_udf_decimal(arg):
       |    return arg > 0.0 and arg < 1000.0
       |
       |# Complex UDF
       |def preloaded_udf_complex(*args):
       |    return hash(args[0]) if len(args) > 0 else 0
       |""".stripMargin
  }

  def dag2DaskPython(spec: JsValue)(graph: Graph[DFOperator]): SourceCode = {
    val l = mutable.ListBuffer[String]()
    val variablePrefix = "auto"
    val finalVariableName = "sink"

    // Add Dask imports
    l += "import dask.dataframe as dd"
    l += "import pandas as pd"
    l += "import numpy as np"
    l += generatePreloadedUDF()

    graph.traverseTopological { node =>
      node.value.varName = s"$variablePrefix${node.id}"

      val call = node.getInDegree match {
        case 0 =>
          val loadCall = constructDFOCall(spec, node, null, null)
          // Add suffix to column names to avoid conflicts in joins
          s"$loadCall.rename(columns=lambda col: f'{col}_${node.id}')"
        case 1 => constructDFOCall(spec, node, node.parents.head.value.varName, null)
        case 2 => constructDFOCall(spec, node, node.parents.head.value.varName, node.parents.last.value.varName)
      }

      val lhs = if (node.isSink) s"$finalVariableName = " else s"${node.value.varName} = "
      val line = s"$lhs$call"

      l += line
    }

    // Dask uses compute() to trigger execution
    l += s"print($finalVariableName.head(10))"
    l += s"# To compute the full result: result = $finalVariableName.compute()"

    val code = l.mkString("\n")

    SourceCode(src = code, ast = null)
  }

}
