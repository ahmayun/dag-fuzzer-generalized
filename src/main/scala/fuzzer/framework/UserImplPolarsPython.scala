package fuzzer.framework

import fuzzer.code.SourceCode
import fuzzer.core.exceptions.DAGFuzzerException
import fuzzer.core.graph.{DFOperator, Graph, Node}
import fuzzer.data.tables.{ColumnMetadata, TableMetadata}
import fuzzer.data.types.DataType
import fuzzer.utils.random.Random
import play.api.libs.json._

import scala.sys.process._
import java.nio.file.{Files, Paths}
import scala.collection.mutable
import scala.io.Source

object UserImplPolarsPython {

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

  def dag2polarsPython(spec: JsValue)(graph: Graph[DFOperator]): SourceCode = {
    val preamble = generatePreamble()
    val l = mutable.ListBuffer[String]()
    val variablePrefix = "df"
    val finalVariableName = "result"

    // Add polars imports
    l += "import polars as pl"
    l += "import numpy as np"
    l += preamble

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

    l += s"final_plan = $finalVariableName.explain()"
//    l += s"final_df = $finalVariableName.collect()"

//    val withDebugLines = l.zip(l.indices.map(i => s"print($i)")).flatMap(e => Array(e._1, e._2))
//    val code = withDebugLines.mkString("\n")
    val code = l.mkString("\n")
    SourceCode(src = code, ast = null, preamble = preamble)
  }

  def generatePreamble(): String = {
    s"""
       |${generatePreloadedUDF()}
       |""".stripMargin
  }

  // ============================================================================
  // UDF GENERATORS
  // ============================================================================

  def generatePreloadedUDF(): String = {
    val config = fuzzer.core.global.State.config.get

    val pythonScriptPath: String = "llm-caller/generator.py"
    val numTries: Int = 3
    val langName: String = "Python"
    val batch = (fuzzer.core.global.State.iteration / config.refreshUdfsAfter).toInt
    val outDir = s"generated/$langName"
    val outPath = s"$outDir/udfs_$batch.json"
    val prompt = s"""
Generate a json file of the following format
```
{ "functions": ["def selectUdfSingleCol(x): ..."] }
```
The functions array should contain ${config.numUdfsPerLLMCall} Python functions. Each function should be short and simple that does something arbitrary.
Each function should be:
- Named "selectUdfSingleCol"
- Complete and runnable
- Contain only code (no comments or docstrings)
- Take a single argument that can be any primitive type
- Should be between 1-10 lines of code
""".trim

    val udfList =
      if (!Files.exists(Paths.get(outPath)) ||
        (fuzzer.core.global.State.iteration % config.refreshUdfsAfter) == 0) {

        var lastException: Throwable = null
        var attempt = 0
        var success = false

        while (attempt < numTries && !success) {
          try {
            generatePreloadedUDF(
              pythonScriptPath,
              prompt,
              outDir,
              outPath
            )
            success = true
          } catch {
            case e: Throwable =>
              lastException = e
              attempt += 1
          }
        }

        if (!success) {
          val outPathObj = Paths.get(outPath)
          if (Files.exists(outPathObj)) {
            Files.delete(outPathObj)
          }

          val previousBatchOpt =
            (0 until batch).reverse
              .map(b => s"$outDir/udfs_$b.json")
              .find(p => Files.exists(Paths.get(p)))

          previousBatchOpt match {
            case Some(prevPath) =>
              readFunctionsFromJson(prevPath)

            case None =>
              throw new DAGFuzzerException(
                s"UDF Generation failed after $numTries tries and no previous batch exists",
                lastException
              )
          }
        } else {
          readFunctionsFromJson(outPath)
        }

      } else {
        readFunctionsFromJson(outPath)
      }

    Random.choice(udfList)
  }


  def generatePreloadedUDF(pythonScriptPath: String, prompt: String, outDir: String, outPath: String): List[String] = {
    Files.createDirectories(Paths.get(outDir))

    val cmd = Seq(
      "oracle-servers/venv/bin/python",
      pythonScriptPath,
      "--prompt", prompt,
      "--out", outPath
    )

    cmd.!

    readFunctionsFromJson(outPath)
  }

  def readFunctionsFromJson(path: String): List[String] = {
    val source = Source.fromFile(path)
    try {
      val jsonStr = source.mkString
      val json = Json.parse(jsonStr)
      (json \ "functions").as[List[String]]
    } finally {
      source.close()
    }
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
      case "pl.read_parquet" =>
        s"""pl.read_parquet("tpcds-data-5pc/$tableName/*.parquet")"""
      case "pl.scan_parquet" =>
        s"""pl.scan_parquet("tpcds-data-5pc/$tableName/*.parquet")"""
//        .select(spark.table("tpcds.$tableName").columns.map(colName => col(colName).alias(s"$${colName}_${node.id}")): _*).as("${tableName}_${node.id}")
      case _ =>
        // Generate synthetic data as DataFrame
        val rows = Random.nextInt(90) + 10
        val cols = Random.nextInt(5) + 3
        s"""pl.DataFrame({
           |    ${(0 until cols).map(i => s"'col_$i': np.random.rand($rows)").mkString(",\n    ")}
           |})""".stripMargin
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
      case "rename" => generateRenameOperation(node, parameters, in1)
      case "group_by" => generateGroupByOperation(node, parameters, in1)
      case "with_columns" => generateWithColumnsOperation(node, parameters, in1)
      case "filter" => generateFilterOperation(node, parameters, in1)
      case "select" => generateSelectOperation(node, parameters, in1)
      case "sort" => generateSortOperation(node, parameters, in1)
      case "unique" => generateUniqueOperation(node, parameters, in1)
      case "limit" => generateLimitOperation(node, parameters, in1)
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
      case "join" => generateJoinOperation(node, parameters, in1, in2)
      case _ => generateGenericBinaryOperation(node, opName, parameters, in1, in2)
    }
  }

  // ============================================================================
  // SPECIFIC OPERATION GENERATORS
  // ============================================================================

  private def generateRenameOperation(
                                       node: Node[DFOperator],
                                       parameters: JsObject,
                                       in1: String
                                     ): String = {
    val oldCol = pickRandomColumnFromReachableSources(node)._2.name
    val newName = Random.alphanumeric.take(8).mkString
    updateSourceState(node, parameters \ "mapping", "mapping", "dict", s"{'$oldCol': '$newName'}")
    propagateState(node)
    s"$in1.rename({'$oldCol': '$newName'})"
  }

  private def constructAggFollowup(node: Node[DFOperator]): String = {
    val (table, col) = pickRandomColumnFromReachableSources(node)
    // Choose between sum, avg, count, etc.
    val aggFuncs = Seq("sum", "avg", "count", "min", "max")
    val chosenAggFunc = aggFuncs(scala.util.Random.nextInt(aggFuncs.length))

    val fullColName = col.name // UserImplFlinkPython.constructFullColumnName(table, col)

    filterColumns(fullColName, node)
    propagateState(node)

    chosenAggFunc match {
//      agg(pl.col('$aggCol').mean())
      case "sum" => s"agg(pl.col('$fullColName').sum().alias('$fullColName'))"
      case "avg" => s"agg(pl.col('$fullColName').mean().alias('$fullColName'))"
      case "count" => s"agg(pl.col('$fullColName').count().alias('$fullColName'))"
      case "min" => s"agg(pl.col('$fullColName').min().alias('$fullColName'))"
      case "max" => s"agg(pl.col('$fullColName').max().alias('$fullColName'))"
      case "udf" => s"agg(call('preloaded_udf_agg', col('$fullColName')).alias('$fullColName'))"
    }

  }

  private def generateGroupByOperation(
                                        node: Node[DFOperator],
                                        parameters: JsObject,
                                        in1: String
                                      ): String = {
    val groupCol = pickRandomColumnFromReachableSources(node)._2.name
    s"$in1.group_by('$groupCol').${constructAggFollowup(node)}"
  }

  private def generateWithColumnsOperation(
                                            node: Node[DFOperator],
                                            parameters: JsObject,
                                            in1: String
                                          ): String = {
    val newColName = s"new_col_${Random.alphanumeric.take(5).mkString}"
    val (_, baseCol) = pickRandomColumnFromReachableSources(node)
    val expr = generatePolarsExpression(node, baseCol)
    addColumn(newColName, node)
    propagateState(node)
    s"$in1.with_columns(($expr).alias('$newColName'))"
  }

  private def generateFilterOperation(
                                       node: Node[DFOperator],
                                       parameters: JsObject,
                                       in1: String
                                     ): String = {
    val predicate = generateFilterPredicate(node)
    s"$in1.filter($predicate)"
  }

  private def generateSelectOperation(
                                       node: Node[DFOperator],
                                       parameters: JsObject,
                                       in1: String
                                     ): String = {

    val columns = getAllColumns(node).map(_._2).take(Random.nextInt(3) + 1)
    filterColumns(columns.map(_.name).mkString(","), node)
    propagateState(node)

    // Randomly (with some probability) insert a map_elements UDF on ONE selected column
    val udfProb = 0.1
    val colExprs = columns.map(c => s"'${c.name}'").mkString(", ")

    if (columns.nonEmpty && Random.nextDouble() < udfProb) {
      val targetCol = columns(Random.nextInt(columns.length))
      val colType = targetCol.dataType // could use this but keeping things simple for now

      s"$in1.select([$colExprs]).with_columns(pl.col('${targetCol.name}').map_elements(selectUdfSingleCol).alias('${targetCol.name}'))"
    } else {
      s"$in1.select([$colExprs])"
    }
  }

  private def generateSortOperation(
                                     node: Node[DFOperator],
                                     parameters: JsObject,
                                     in1: String
                                   ): String = {
    val sortCol = pickRandomColumnFromReachableSources(node)._2.name
    val descending = Random.choice(List("True", "False"))
    s"$in1.sort('$sortCol', descending=$descending)"
  }

  private def generateUniqueOperation(
                                       node: Node[DFOperator],
                                       parameters: JsObject,
                                       in1: String
                                     ): String = {
    if (Random.nextBoolean()) {
      val col = pickRandomColumnFromReachableSources(node)._2.name
      s"$in1.unique(subset=['$col'])"
    } else {
      s"$in1.unique()"
    }
  }

  private def generateLimitOperation(
                                      node: Node[DFOperator],
                                      parameters: JsObject,
                                      in1: String
                                    ): String = {
    val n = Random.nextInt(100) + 1
    s"$in1.limit($n)"
  }

  private def generateJoinOperation(
                                     node: Node[DFOperator],
                                     parameters: JsObject,
                                     in1: String,
                                     in2: String
                                   ): String = {
    val joinTypes = List("inner", "left", "right", "outer")
    val how = joinTypes(Random.nextInt(joinTypes.length))

    // Try to find columns with matching names
    val leftCols = node.parents.head.value.stateView.values.flatMap(_.columns.map(_.name)).toSet
    val rightCols = node.parents.last.value.stateView.values.flatMap(_.columns.map(_.name)).toSet
    val commonCols = leftCols.intersect(rightCols)

    if (commonCols.nonEmpty) {
      val joinCol = commonCols.toList(Random.nextInt(commonCols.size))
      s"$in1.join($in2, on='$joinCol', how='$how')"
    } else {
      // Use different columns for left_on and right_on
      val leftCol = pickRandomColumnFromNode(node.parents.head)._2.name
      val rightCol = pickRandomColumnFromNode(node.parents.last)._2.name
      s"$in1.join($in2, left_on='$leftCol', right_on='$rightCol', how='$how')"
    }
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
    val allowedValues: Option[Seq[JsValue]] = (param \ "values").asOpt[Seq[JsValue]]

    allowedValues match {
      case Some(values) if values.nonEmpty =>
        val randomValue = values(Random.nextInt(values.length))
        randomValue match {
          case JsString(str) => s"'$str'"
          case other => other.toString
        }

      case _ =>
        paramType match {
          case "int" => Random.nextInt(100).toString
          case "bool" => if (Random.nextBoolean()) "True" else "False"
          case "str" | "string" => s"'${Random.alphanumeric.take(8).mkString}'"
          case "dict" => "{}"  // For rename operation
          case "Expr" => generateFilterPredicate(node)
          case "IntoExpr | Iterable[IntoExpr]" => generatePolarsExpression(node, pickRandomColumnFromReachableSources(node)._2).toString
          case _ => s"'${Random.alphanumeric.take(8).mkString}'"
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

  def generatePolarsExpression(node: Node[DFOperator], col: ColumnMetadata): String = {
    val colName = col.name

    col.dataType match {
      case fuzzer.data.types.IntegerType | fuzzer.data.types.LongType =>
        val op = Random.nextInt(4)
        op match {
          case 0 => s"pl.col('$colName') * 2"
          case 1 => s"pl.col('$colName') + ${Random.nextInt(100)}"
          case 2 => s"pl.col('$colName').abs()"
          case _ => s"pl.col('$colName')"
        }
      case fuzzer.data.types.FloatType | fuzzer.data.types.DecimalType =>
        val op = Random.nextInt(3)
        op match {
          case 0 => s"pl.col('$colName').round(2)"
          case 1 => s"pl.col('$colName') * ${Random.nextFloat()}"
          case _ => s"pl.col('$colName')"
        }
      case fuzzer.data.types.StringType =>
        val op = Random.nextInt(3)
        op match {
          case 0 => s"pl.col('$colName').str.len_chars()"
          case 1 => s"pl.col('$colName').str.to_uppercase()"
          case _ => s"pl.col('$colName')"
        }
      case fuzzer.data.types.BooleanType =>
        s"pl.col('$colName').not_()"
      case _ =>
        s"pl.col('$colName')"
    }
  }

  def generateFilterPredicate(node: Node[DFOperator]): String = {
    val (_, col) = pickRandomColumnFromReachableSources(node)
    val colName = col.name
    val config = fuzzer.core.global.State.config.get

    col.dataType match {
      case fuzzer.data.types.IntegerType | fuzzer.data.types.LongType =>
        val value = Random.nextInt(100)
        val ops = List(">", "<", ">=", "<=", "==", "!=")
        val op = ops(Random.nextInt(ops.length))
        s"pl.col('$colName') $op $value"
      case fuzzer.data.types.FloatType | fuzzer.data.types.DecimalType =>
        val value = Random.nextFloat() * 100
        s"pl.col('$colName') > $value"
      case fuzzer.data.types.StringType =>
        s"pl.col('$colName').str.len_chars() > 5"
      case fuzzer.data.types.BooleanType =>
        if (Random.nextBoolean()) s"pl.col('$colName')" else s"pl.col('$colName').not_()"
      case _ =>
        s"pl.col('$colName').is_not_null()"
    }
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

  def pickRandomColumnFromNode(node: Node[DFOperator]): (TableMetadata, ColumnMetadata) = {
    val tablesColPairs = node.value.stateView.values.toSeq.flatMap { t =>
      t.columns.map(c => (t, c))
    }
    assert(tablesColPairs.nonEmpty, "Expected columns to be non-empty")
    tablesColPairs(Random.nextInt(tablesColPairs.length))
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
      _columns = Seq(ColumnMetadata(name = value, dataType = DataType.generateRandom, metadata = Map("source" -> "runtime", "gen-iteration" -> fuzzer.core.global.State.iteration.toString))),
      _metadata = Map("source" -> "runtime", "gen-iteration" -> fuzzer.core.global.State.iteration.toString)
    ))
  }

  def filterColumns(columns: String, node: Node[DFOperator]): Unit = {
    val colNames = columns.split(",").map(_.trim)
    node.value.stateView = node.value.stateView.map {
      case (tname, tmd) =>
        (tname -> tmd.filterColumns(colNames))
    }
  }

  def updateSourceState(
                         node: Node[DFOperator],
                         param: JsLookupResult,
                         paramName: String,
                         paramType: String,
                         paramVal: String
                       ): Unit = {
    val effect = (param \ "state-effect").asOpt[String].getOrElse("")
    effect match {
      case "table-rename" => renameTables(paramVal, node)
      case "column-add" => addColumn(paramVal, node)
      case "column-filter" => filterColumns(paramVal, node)
      case "column-rename" => // Handle column renaming
      case _ => // No state change
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