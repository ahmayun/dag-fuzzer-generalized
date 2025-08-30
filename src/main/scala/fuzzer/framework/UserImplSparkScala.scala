package fuzzer.framework

import fuzzer.code.SourceCode
import fuzzer.core.graph.{DFOperator, Graph, Node}
import fuzzer.data.tables.{ColumnMetadata, TableMetadata}
import fuzzer.data.types.DataType
import fuzzer.utils.random.Random
import play.api.libs.json._

import scala.collection.mutable

object UserImplSparkScala {

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
      case "unary" if Array("groupBy").contains(opName) => s"$in1.$opName(${args.mkString(", ")}).${constructAggFollowup(node, spec, opName, opType, parameters, args)}"
//      case "binary" => s"$in1.$opName(${args.mkString(", ")})"
//      case "action" => s"$in1.$opName(${args.mkString(", ")})"
      case _ => s"$in1.$opName(${args.mkString(", ")})"
    }
  }

  private def constructAggFollowup(node: Node[DFOperator], spec: JsValue, opName: String, opType: String, parameters: JsObject, args: List[String]): String = {
    val (table, col) =  pickRandomColumnFromReachableSources(node)
    // Choose between sum, avg, count, etc.
    val aggFuncs = Seq("sum", "avg", "count", "min", "max")
    val chosenAggFunc = aggFuncs(scala.util.Random.nextInt(aggFuncs.length))

    // Decide whether to do a shortcut (e.g., .sum("col")) or full agg (e.g., .agg(sum("col")))
    val useShortcut = chosenAggFunc == "sum" || chosenAggFunc == "avg"

    val fullColName = s"${table.identifier}.${col.name}"
    val aggCol = s"""$chosenAggFunc("$fullColName")"""

    if (useShortcut) {
      // .groupBy("a").sum("b")
      s"""$chosenAggFunc("$fullColName")"""
    } else {
      // .agg(count("b"))
      s"""agg($aggCol)"""
    }
  }

  private def constructSourceCall(node: Node[DFOperator], spec: JsValue, opName: String, opType: String, parameters: JsObject, args: List[String]): String = {
    val tpcdsTablesPath = fuzzer.core.global.State.config.get.localTpcdsPath
    val tableName = fuzzer.core.global.State.src2TableMap(node.id).identifier
    opName match {
      case "spark.table" => s"""$opName("tpcds.$tableName").as("$tableName")"""
      case "spark.read.parquet" => s"""$opName("$tpcdsTablesPath/$tableName").as("$tableName")"""
      case _ => s"$opName(${args.mkString(", ")})"
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
        Some(generateRandomValue(node, param, paramType, paramName))
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
    tablesColPairs
    //    if (preferUnique) {
    //      val sourcesToPickFrom = node.getReachableFromOnce
    ////      println("Preferring Unique")
    ////      println(s"${sourcesToPickFrom.size}/${node.getReachableSources.size}")
    //      if (sourcesToPickFrom.isEmpty) {
    //        tablesColPairs
    //      } else {
    //        val sourceTableNames = sourcesToPickFrom.map(_.value.state.originalIdentifier)
    ////        println(s"Tables to pick from ${sourceTableNames}")
    //        val fil = tablesColPairs.filter { case (t, _) =>
    //          val origName = t.originalIdentifier
    ////          println(s"Checking if $origName in $sourceTableNames")
    //          sourceTableNames.contains(origName)
    //        }
    //        println(s"filtered from ${tablesColPairs.size} => ${fil.size} for node ${node.value.name}")
    //        if(fil.nonEmpty) fil else tablesColPairs
    //      }
    //    } else {
    //      tablesColPairs
    //    }
  }

  def pickRandomColumnFromReachableSources(node: Node[DFOperator], preferUnique: Boolean=true): (TableMetadata, ColumnMetadata) = {
    val tablesColPairs = getAllColumns(node, preferUnique).filter {
      case (_, col) =>
        col.metadata.get("gen-iteration") match {
          case None => true
          case Some(i) =>
            fuzzer.core.global.State.iteration.toString != i
        }
    }
    assert(tablesColPairs.nonEmpty, "Expected columnNames to be non-empty")
    val pick = tablesColPairs(Random.nextInt(tablesColPairs.length))
    pick
  }

  def renameTables(newValue: String, node: Node[DFOperator]): Unit = {
    val dfOp = node.value
    //    println(s"====== RENAMING TABLE VIEW FOR ${node.id} (${node.value.name}) ===========")

    // Rename each table metadata entry in the stateView
    val renamedStateView: Map[String, TableMetadata] = dfOp.stateView.map {
      case (id, tableMeta) =>
        val renamed = tableMeta.copy() // get a deep copy
        //        println(s"\t => At $id (${tableMeta.identifier} => $newValue)")
        renamed.setIdentifier(newValue) // modify as needed
        id -> renamed
    }

    // Update this node’s stateView with renamed copies
    dfOp.stateView = renamedStateView
  }

  def addColumn(value: String, node: Node[DFOperator]): Unit = {
    node.value.stateView = node.value.stateView + ("added" -> TableMetadata(
      _identifier = "",
      _columns = Seq(ColumnMetadata(name=value, dataType = DataType.generateRandom, metadata = Map("source" -> "runtime", "gen-iteration" -> fuzzer.core.global.State.iteration.toString))),
      _metadata = Map("source" -> "runtime", "gen-iteration" -> fuzzer.core.global.State.iteration.toString)
    ))
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
    val prefix = if(table.identifier != null && table.identifier.nonEmpty) s"${table.identifier}." else ""
    s"$prefix${col.name}"
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
        val tableGroups = cols.groupBy(_._1)  // group by TableMetadata
        (dt, tableGroups)
      }
      .filter { case (_, tableGroups) => tableGroups.size >= 2 }
      .map { case (dt, tableGroups) =>
        (dt, tableGroups.values.flatten.toSeq)
      }

    // Step 3: Randomly pick a datatype from viable options
    if (viableTypes.isEmpty) {
      //      throw new RuntimeException("No two columns of the same type from different tables found")
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
        val colExpr1 = s"""col("$fullColName1")"""
        val colExpr2 = s"""col("$fullColName2")"""
        val crossTableExpr = s"$colExpr1 === $colExpr2"
        Some(crossTableExpr)
      case None => None
    }
  }

  def pickColumnExpr(
                      node: Node[DFOperator],
                      param: JsLookupResult,
                      paramName: String,
                      paramType: String
                    ): String = {
    val (table, col) = pickRandomColumnFromReachableSources(node)
    val fullColName = constructFullColumnName(table, col)
    if (Random.nextFloat() < fuzzer.core.global.State.config.get.probUDFInsert) {
      s"""preloadedUDF(col("$fullColName"))"""
    } else {
      s"""col("$fullColName")"""
    }
  }

  def generateSingleColumnExpression(
                                      node: Node[DFOperator],
                                      param: JsLookupResult,
                                      paramName: String,
                                      paramType: String
                                    ): String = {

    val config = fuzzer.core.global.State.config.get
    val prob = config.probUDFInsert
    val (table, col) = pickRandomColumnFromReachableSources(node)
    val fullColName = constructFullColumnName(table, col)
    val colExpr = s"""col("$fullColName")"""

    val op = config.logicalOperatorSet.toVector(Random.nextInt(config.logicalOperatorSet.size))

    val intValue = config.randIntMin + Random.nextInt(config.randIntMax - config.randIntMin + 1)
    val floatValue = config.randFloatMin + Random.nextFloat() * (config.randFloatMax - config.randFloatMin)

    val intExpr = s"$colExpr $op $intValue"
    val floatExpr = s"$colExpr $op $floatValue"
    val stringExpr = s"length($colExpr) $op 5"
    val boolExpr = s"!$colExpr"
    val udfExpr = s"preloadedUDF($colExpr)"

    if (Random.nextFloat() < prob && !node.value.name.contains("filter")) {
      udfExpr
    } else {
      col.dataType match {
        case fuzzer.data.types.IntegerType => intExpr
        case fuzzer.data.types.FloatType => floatExpr
        case fuzzer.data.types.StringType => stringExpr
        case fuzzer.data.types.BooleanType => boolExpr
        case fuzzer.data.types.LongType => intExpr
        case fuzzer.data.types.DecimalType => floatExpr
        case fuzzer.data.types.DateType => udfExpr
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
          case JsString(str) => s""""$str"""" // Add quotes for strings
          case other => other.toString       // Leave numbers, bools, etc. as-is
        }

      // Generate values if spec doesn't provide fixed options
      case _ =>
        paramType match {
          case "int" => Random.nextInt(100).toString
          case "bool" => Random.nextBoolean().toString
          case "str" => s""""${generateOrPickString(node, param, paramName, paramType)}""""
          case "Column" => generateColumnExpression(node, param, paramName, paramType)
          case "List[str]" =>
            s"List(${(0 until maxListLength).map(_ => generateOrPickString(node, param, paramName, paramType)).mkString(",")})"
          case "Column*" =>
            s"${(0 until maxListLength).map(_ => pickColumnExpr(node, param, paramName, paramType)).mkString(",")}"
          case "list" => s"""List("${Random.alphanumeric.take(5).mkString}")"""
          case _ => s""""${Random.alphanumeric.take(8).mkString}""""
        }
    }
  }

  def dag2SparkScala(spec: JsValue)(graph: Graph[DFOperator]): SourceCode = {
    val l = mutable.ListBuffer[String]()
    val variablePrefix = "auto"
    val finalVariableName = "sink"


    graph.traverseTopological { node =>
      node.value.varName = s"$variablePrefix${node.id}"

      val call = node.getInDegree match {
        case 0 => constructDFOCall(spec, node, null, null)
        case 1 => constructDFOCall(spec, node, node.parents.head.value.varName, null)
        case 2 => constructDFOCall(spec, node, node.parents.head.value.varName, node.parents.last.value.varName)
      }
      val lhs = if(node.isSink) s"val $finalVariableName = " else s"val ${node.value.varName} = "
      l += s"$lhs$call"
    }
    l += s"$finalVariableName.explain(true)"

//     Post-program state updates
//        l += s"fuzzer.global.State.finalDF = Some(${fuzzer.global.FuzzerConfig.config.finalVariableName})"

    SourceCode(src=l.mkString("\n"), ast=null)
  }

}
