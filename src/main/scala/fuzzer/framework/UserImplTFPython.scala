package fuzzer.framework

import fuzzer.code.SourceCode
import fuzzer.core.graph.{DFOperator, Graph, Node}
import fuzzer.data.tables.{ColumnMetadata, TableMetadata}
import fuzzer.data.types.DataType
import fuzzer.utils.random.Random
import play.api.libs.json._

import scala.collection.mutable

object UserImplTFPython {

  // ============================================================================
  // MAIN ENTRY POINTS
  // ============================================================================

  def constructDFOCall(spec: JsValue, node: Node[DFOperator], in1: String, in2: String): String = {
    val opName = node.value.name
    val opSpec = spec \ opName

    if (opSpec.isInstanceOf[JsUndefined]) {
      return opName
    }

    val opType = (opSpec \ "type").as[String]
    val parameters = (opSpec \ "parameters").as[JsObject]

    opType match {
      case "source" => generateSourceOperation(node, spec, opName, parameters)
      case "unary" => generateUnaryOperation(node, spec, opName, parameters, in1)
      case "binary" => generateBinaryOperation(node, spec, opName, parameters, in1, in2)
      case _ => s"$in1.$opName(${generateArguments(node, parameters, opType, in2).mkString(", ")})"
    }
  }

  def generateCSVLoader(): String = {
    """def load_tf_csv(path, batch_size=32, skip_header=True):
      |    with tf.io.gfile.GFile(path, "r") as f:
      |        num_columns = len(f.readline().strip().split(","))
      |    record_defaults = [tf.constant("", tf.string)] * num_columns
      |    ds = tf.data.TextLineDataset(path)
      |    if skip_header: ds = ds.skip(1)
      |    ds = ds.map(lambda line: tuple(tf.io.decode_csv(line, record_defaults)))
      |    return ds.batch(batch_size).prefetch(tf.data.AUTOTUNE)
      |""".stripMargin
  }

  def dag2tensorflowPython(spec: JsValue)(graph: Graph[DFOperator]): SourceCode = {
    val l = mutable.ListBuffer[String]()
    val variablePrefix = "auto"
    val finalVariableName = "sink"

    // Add tensorflow imports
    l += "import tensorflow as tf"
    l += "import numpy as np"
    l += generatePreloadedUDF()
    l += generateCSVLoader()

    graph.traverseTopological { node =>
      node.value.varName = s"$variablePrefix${node.id}"

      val call = node.getInDegree match {
        case 0 =>
          val loadCall = constructDFOCall(spec, node, null, null)
          loadCall
        case 1 => constructDFOCall(spec, node, node.parents.head.value.varName, null)
        case 2 => constructDFOCall(spec, node, node.parents.head.value.varName, node.parents.last.value.varName)
      }

      val lhs = if (node.isSink) s"$finalVariableName = " else s"${node.value.varName} = "
      val line = s"$lhs$call"

      l += line
    }

    l += s"# Iterate through the dataset"
    l += s"for element in $finalVariableName.take(10):"
    l += s"    print(element)"

    val code = l.mkString("\n")
    SourceCode(src = code, ast = null)
  }

  // ============================================================================
  // OPERATION TYPE GENERATORS
  // ============================================================================

  private def generateSourceOperation(
                                       node: Node[DFOperator],
                                       spec: JsValue,
                                       opName: String,
                                       parameters: JsObject
                                     ): String = {
    val tableName = fuzzer.core.global.State.src2TableMap(node.id).identifier
    opName match {
//      case _ => s"""load_tf_csv("tpcds-csv-5pc/$tableName")"""
//      case "from_tensor_slices" => s"tf.data.Dataset.from_tensor_slices($tableName)"
//      case "from_tensors" => s"tf.data.Dataset.from_tensors($tableName)"
      case _ =>
        val start = Random.nextInt(10)
        val stop = start + Random.nextInt(90) + 10
        s"tf.data.Dataset.range($start, $stop)"
//      case _ => s"tf.data.Dataset.from_tensor_slices($tableName)" // Default to from_tensor_slices
    }
  }

  private def generateUnaryOperation(
                                      node: Node[DFOperator],
                                      spec: JsValue,
                                      opName: String,
                                      parameters: JsObject,
                                      in1: String
                                    ): String = {
    opName match {
      case "map" => generateMapOperation(node, parameters, in1)
      case "filter" => generateFilterOperation(node, parameters, in1)
      case "shuffle" => generateShuffleOperation(node, parameters, in1)
      case "take" => generateTakeOperation(node, parameters, in1)
      case "flat_map" => generateFlatMapOperation(node, parameters, in1)
      case "interleave" => generateInterleaveOperation(node, parameters, in1)
      case "reduce" => generateReduceOperation(node, parameters, in1)
      case "scan" => generateScanOperation(node, parameters, in1)
      case "apply" => generateApplyOperation(node, parameters, in1)
      case "batch" => generateBatchOperation(node, parameters, in1)
      case "prefetch" => generatePrefetchOperation(node, parameters, in1)
      case "cache" => generateCacheOperation(node, parameters, in1)
      case "repeat" => generateRepeatOperation(node, parameters, in1)
      case "skip" => generateSkipOperation(node, parameters, in1)
      case "window" => generateWindowOperation(node, parameters, in1)
      case "unbatch" => generateUnbatchOperation(node, parameters, in1)
      case "enumerate" => generateEnumerateOperation(node, parameters, in1)
      case "shard" => generateShardOperation(node, parameters, in1)
      case _ => generateGenericUnaryOperation(node, opName, parameters, in1)
    }
  }

  private def generateBinaryOperation(
                                       node: Node[DFOperator],
                                       spec: JsValue,
                                       opName: String,
                                       parameters: JsObject,
                                       in1: String,
                                       in2: String
                                     ): String = {
    opName match {
      case "concatenate" => generateConcatenateOperation(node, parameters, in1, in2)
      case "zip" => generateZipOperation(node, parameters, in1, in2)
      case _ => generateGenericBinaryOperation(node, opName, parameters, in1, in2)
    }
  }

  // ============================================================================
  // SPECIFIC OPERATION GENERATORS
  // ============================================================================

  private def generateMapOperation(
                                    node: Node[DFOperator],
                                    parameters: JsObject,
                                    in1: String
                                  ): String = {
    val args = generateArguments(node, parameters, "unary", null)
    s"$in1.map(${args.mkString(", ")})"
  }

  private def generateFilterOperation(
                                       node: Node[DFOperator],
                                       parameters: JsObject,
                                       in1: String
                                     ): String = {
    val args = generateArguments(node, parameters, "unary", null)
    s"$in1.filter(${args.mkString(", ")})"
  }

  private def generateShuffleOperation(
                                        node: Node[DFOperator],
                                        parameters: JsObject,
                                        in1: String
                                      ): String = {
    val args = generateArguments(node, parameters, "unary", null)
    s"$in1.shuffle(${args.mkString(", ")})"
  }

  private def generateTakeOperation(
                                     node: Node[DFOperator],
                                     parameters: JsObject,
                                     in1: String
                                   ): String = {
    val args = generateArguments(node, parameters, "unary", null)
    s"$in1.take(${args.mkString(", ")})"
  }

  private def generateFlatMapOperation(
                                        node: Node[DFOperator],
                                        parameters: JsObject,
                                        in1: String
                                      ): String = {
    val args = generateArguments(node, parameters, "unary", null)
    s"$in1.flat_map(${args.mkString(", ")})"
  }

  private def generateInterleaveOperation(
                                           node: Node[DFOperator],
                                           parameters: JsObject,
                                           in1: String
                                         ): String = {
    val args = generateArguments(node, parameters, "unary", null)
    s"$in1.interleave(${args.mkString(", ")})"
  }

  private def generateReduceOperation(
                                       node: Node[DFOperator],
                                       parameters: JsObject,
                                       in1: String
                                     ): String = {
    val args = generateArguments(node, parameters, "unary", null)
    s"$in1.reduce(${args.mkString(", ")})"
  }

  private def generateScanOperation(
                                     node: Node[DFOperator],
                                     parameters: JsObject,
                                     in1: String
                                   ): String = {
    val args = generateArguments(node, parameters, "unary", null)
    s"$in1.scan(${args.mkString(", ")})"
  }

  private def generateApplyOperation(
                                      node: Node[DFOperator],
                                      parameters: JsObject,
                                      in1: String
                                    ): String = {
    val args = generateArguments(node, parameters, "unary", null)
    s"$in1.apply(${args.mkString(", ")})"
  }

  private def generateBatchOperation(
                                      node: Node[DFOperator],
                                      parameters: JsObject,
                                      in1: String
                                    ): String = {
    val args = generateArguments(node, parameters, "unary", null)
    s"$in1.batch(${args.mkString(", ")})"
  }

  private def generatePrefetchOperation(
                                         node: Node[DFOperator],
                                         parameters: JsObject,
                                         in1: String
                                       ): String = {
    val args = generateArguments(node, parameters, "unary", null)
    s"$in1.prefetch(${args.mkString(", ")})"
  }

  private def generateCacheOperation(
                                      node: Node[DFOperator],
                                      parameters: JsObject,
                                      in1: String
                                    ): String = {
    val args = generateArguments(node, parameters, "unary", null)
    if (args.isEmpty) {
      s"$in1.cache()"
    } else {
      s"$in1.cache(${args.mkString(", ")})"
    }
  }

  private def generateRepeatOperation(
                                       node: Node[DFOperator],
                                       parameters: JsObject,
                                       in1: String
                                     ): String = {
    val args = generateArguments(node, parameters, "unary", null)
    if (args.isEmpty) {
      s"$in1.repeat()"
    } else {
      s"$in1.repeat(${args.mkString(", ")})"
    }
  }

  private def generateSkipOperation(
                                     node: Node[DFOperator],
                                     parameters: JsObject,
                                     in1: String
                                   ): String = {
    val args = generateArguments(node, parameters, "unary", null)
    s"$in1.skip(${args.mkString(", ")})"
  }

  private def generateWindowOperation(
                                       node: Node[DFOperator],
                                       parameters: JsObject,
                                       in1: String
                                     ): String = {
    val args = generateArguments(node, parameters, "unary", null)
    s"$in1.window(${args.mkString(", ")})"
  }

  private def generateUnbatchOperation(
                                        node: Node[DFOperator],
                                        parameters: JsObject,
                                        in1: String
                                      ): String = {
    s"$in1.unbatch()"
  }

  private def generateEnumerateOperation(
                                          node: Node[DFOperator],
                                          parameters: JsObject,
                                          in1: String
                                        ): String = {
    val args = generateArguments(node, parameters, "unary", null)
    if (args.isEmpty) {
      s"$in1.enumerate()"
    } else {
      s"$in1.enumerate(${args.mkString(", ")})"
    }
  }

  private def generateShardOperation(
                                      node: Node[DFOperator],
                                      parameters: JsObject,
                                      in1: String
                                    ): String = {
    val args = generateArguments(node, parameters, "unary", null)
    s"$in1.shard(${args.mkString(", ")})"
  }

  private def generateConcatenateOperation(
                                            node: Node[DFOperator],
                                            parameters: JsObject,
                                            in1: String,
                                            in2: String
                                          ): String = {
    val args = generateArguments(node, parameters, "binary", in2)
    s"$in1.concatenate($in2)"
  }

  private def generateZipOperation(
                                    node: Node[DFOperator],
                                    parameters: JsObject,
                                    in1: String,
                                    in2: String
                                  ): String = {
    s"tf.data.Dataset.zip(($in1, $in2))"
  }

  private def generateGenericUnaryOperation(
                                             node: Node[DFOperator],
                                             opName: String,
                                             parameters: JsObject,
                                             in1: String
                                           ): String = {
    val args = generateArguments(node, parameters, "unary", null)
    s"$in1.$opName(${args.mkString(", ")})"
  }

  private def generateGenericBinaryOperation(
                                              node: Node[DFOperator],
                                              opName: String,
                                              parameters: JsObject,
                                              in1: String,
                                              in2: String
                                            ): String = {
    val args = generateArguments(node, parameters, "binary", in2)
    s"$in1.$opName(${args.mkString(", ")})"
  }

  // ============================================================================
  // PARAMETER AND ARGUMENT GENERATION
  // ============================================================================

  def generateArguments(
                         node: Node[DFOperator],
                         parameters: JsObject,
                         opType: String,
                         in2: String
                       ): List[String] = {
    val paramNames = parameters.keys.toList

    paramNames.flatMap { paramName =>
      val param = parameters \ paramName
      val paramType = getParamType((param \ "type").toOption.getOrElse(JsString("str")))
      val required = (param \ "required").as[Boolean]
      val hasDefault = (param \ "default").isDefined

      if (paramName == "other" && paramType == "DataFrame" && opType == "binary") {
        Some(in2)
      } else if (required || (!hasDefault && Random.nextBoolean())) {
        Some(s"$paramName=${generateRandomValue(node, param, paramType, paramName)}")
      } else {
        None
      }
    }
  }

  def generateRandomValue(
                           node: Node[DFOperator],
                           param: JsLookupResult,
                           paramType: String,
                           paramName: String
                         ): String = {
    val maxListLength = fuzzer.core.global.State.config.get.maxListLength
    val allowedValues: Option[Seq[JsValue]] = (param \ "values").asOpt[Seq[JsValue]]

    allowedValues match {
      case Some(values) if values.nonEmpty =>
        val randomValue = values(Random.nextInt(values.length))
        randomValue match {
          case JsString(str) =>
            // Handle special TensorFlow constants
            if (str == "AUTOTUNE") "tf.data.AUTOTUNE"
            else s"'$str'"
          case other => other.toString
        }

      case _ =>
        paramType match {
          case "int" => Random.nextInt(100).toString
          case "bool" => if (Random.nextBoolean()) "True" else "False"
          case "callable" =>
            // Generate appropriate lambda or UDF reference based on parameter name
            paramName match {
              case "map_func" | "func" => "map_udf"
              case "predicate" => "filter_udf"
              case "reduce_func" => "reduce_udf"
              case "scan_func" => "scan_udf"
              case "transformation_func" => "lambda ds: ds"
              case _ => "lambda x: x"
            }
          case "tensor" | "scalar" =>
            // Generate initial state for reduce/scan
            paramName match {
              case "initial_state" => "tf.constant(0)"
              case _ => Random.nextInt(100).toString
            }
          case "str" | "string" => s"'${Random.alphanumeric.take(8).mkString}'"
          case "buffer_size" =>
            // For shuffle buffer_size, generate reasonable values
            Random.nextInt(1000).max(10).toString
          case _ => Random.nextInt(100).toString
        }
    }
  }

  def getParamType(typeJson: JsValue): String = {
    typeJson match {
      case JsString(t) => t
      case JsArray(types) => types(Random.nextInt(types.length)).as[String]
      case _ => "str"
    }
  }

  // ============================================================================
  // EXPRESSION GENERATORS
  // ============================================================================

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

    val tensorflowOp = op match {
      case "==" => "=="
      case "!=" => "!="
      case ">" => ">"
      case "<" => "<"
      case ">=" => ">="
      case "<=" => "<="
      case _ => "=="
    }

    val intExpr = s"$fullColName $tensorflowOp $intValue"
    val floatExpr = s"$fullColName $tensorflowOp $floatValue"
    val stringExpr = s"$fullColName.str.len() $tensorflowOp 5"
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
        val crossTableExpr = s"df['$fullColName1'] == df['$fullColName2']"
        Some(crossTableExpr)
      case None => None
    }
  }

  // ============================================================================
  // UDF GENERATORS
  // ============================================================================

  def generateUDFCall(node: Node[DFOperator], args: List[(TableMetadata, ColumnMetadata)]): String = {
    if (node.value.name.contains("filter") || node.value.name.contains("query"))
      generateFilterUDFCall(node, args)
    else
      generateComplexUDFCall(node, args)
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

  def generatePreloadedUDF(): String = {
    s"""
       |# Predefined UDFs for TensorFlow Dataset transformations
       |def filter_udf(*x):
       |    # Example filter: keep elements greater than 0
       |    return x[0] > 0
       |
       |def map_udf(*x):
       |    # Example map: double the value
       |    return x[0] * 2
       |
       |def flat_map_udf(*x):
       |    # Example flat_map: return dataset of repeated values
       |    return tf.data.Dataset.from_tensors(x[0]).repeat(2)
       |
       |def reduce_udf(state, *x):
       |    # Example reduce: sum accumulator
       |    return state + x[0]
       |
       |def scan_udf(state, *x):
       |    # Example scan: cumulative sum
       |    new_state = state + x[0]
       |    return new_state, new_state
       |""".stripMargin
  }

  // ============================================================================
  // STRING AND COLUMN UTILITIES
  // ============================================================================

  def generateOrPickString(
                            node: Node[DFOperator],
                            param: JsLookupResult,
                            paramName: String,
                            paramType: String
                          ): String = {
    val isStateAltering: Boolean = (param \ "state-altering").asOpt[Boolean].getOrElse(false)

    if (isStateAltering) {
      val gen = s"${Random.alphanumeric.take(fuzzer.core.global.State.config.get.maxStringLength).mkString}"
      updateSourceState(node, param, paramName, paramType, gen)
      propagateState(node)
      gen
    } else {
      val (table, col) = pickRandomColumnFromReachableSources(node)
      constructFullColumnName(table, col)
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
      updateSourceState(node, param, paramName, paramType, fullColName)
      propagateState(node)
    }
    fullColName
  }

  def constructFullColumnName(table: TableMetadata, col: ColumnMetadata): String = {
    s"${col.name}"
  }

  // ============================================================================
  // COLUMN SELECTION UTILITIES
  // ============================================================================

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
  }

  def pickRandomColumnFromReachableSources(
                                            node: Node[DFOperator],
                                            preferUnique: Boolean = true
                                          ): (TableMetadata, ColumnMetadata) = {
    val tablesColPairs = getAllColumns(node, preferUnique).filter {
      case (_, col) =>
        col.metadata.get("gen-iteration") match {
          case None => true
          case Some(i) => fuzzer.core.global.State.iteration.toString != i
        }
    }
    assert(tablesColPairs.nonEmpty, s"Expected columnNames to be non-empty: stateViewMap = ${node.value.stateView}")
    val pick = tablesColPairs(Random.nextInt(tablesColPairs.length))
    pick
  }

  def pickTwoColumns(
                      stateView: Map[String, TableMetadata]
                    ): Option[((TableMetadata, ColumnMetadata), (TableMetadata, ColumnMetadata))] = {
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

    val viableTypes: Seq[(DataType, Seq[(TableMetadata, ColumnMetadata)])] = columnsByType.toSeq
      .map { case (dt, cols) =>
        val tableGroups = cols.groupBy(_._1)
        (dt, tableGroups)
      }
      .filter { case (_, tableGroups) => tableGroups.size >= 2 }
      .map { case (dt, tableGroups) =>
        (dt, tableGroups.values.flatten.toSeq)
      }

    if (viableTypes.isEmpty) {
      return None
    }

    val (_, candidates) = Random.shuffle(viableTypes).head
    val byTable = candidates.groupBy(_._1).toSeq
    val shuffledPairs = Random.shuffle(byTable.combinations(2).toSeq)
    val chosenPair = shuffledPairs.head

    val col1 = Random.shuffle(chosenPair(0)._2).head
    val col2 = Random.shuffle(chosenPair(1)._2).head

    Some((col1, col2))
  }

  def pickMultiColumnsFromReachableSources(
                                            node: Node[DFOperator]
                                          ): Option[((TableMetadata, ColumnMetadata), (TableMetadata, ColumnMetadata))] = {
    pickTwoColumns(node.value.stateView)
  }

  // ============================================================================
  // STATE MANAGEMENT
  // ============================================================================

  def renameTables(newValue: String, node: Node[DFOperator]): Unit = {
    val dfOp = node.value
    val renamedStateView: Map[String, TableMetadata] = dfOp.stateView.map {
      case (id, tableMeta) =>
        val renamed = tableMeta.copy()
        renamed.setIdentifier(newValue)
        id -> renamed
    }
    dfOp.stateView = renamedStateView
  }

  def addColumn(value: String, node: Node[DFOperator]): Unit = {
    node.value.stateView = node.value.stateView + ("added" -> TableMetadata(
      _identifier = "",
      _columns = Seq(ColumnMetadata(
        name = value,
        dataType = DataType.generateRandom,
        metadata = Map("source" -> "runtime", "gen-iteration" -> fuzzer.core.global.State.iteration.toString)
      )),
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
      case "index-change" => // tensorflow specific - handled in place
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

        val updatedView = currentDFOp.stateView.map {
          case (key, _) if startStateView.contains(key) =>
            key -> startStateView(key).copy()
          case other =>
            other
        }

        currentDFOp.stateView = updatedView
        queue.enqueueAll(current.children)
      }
    }
  }

}