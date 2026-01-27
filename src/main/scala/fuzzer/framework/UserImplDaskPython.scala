package fuzzer.framework

import fuzzer.code.SourceCode
import fuzzer.core.exceptions.DAGFuzzerException
import fuzzer.core.graph.{DFOperator, Graph, Node}
import fuzzer.data.tables.{ColumnMetadata, TableMetadata}
import fuzzer.data.types.{BooleanType, DataType, DateType, DecimalType, FloatType, IntegerType, LongType, StringType}
import fuzzer.utils.random.Random
import play.api.libs.json._

import scala.sys.process._
import java.nio.file.{Files, Paths}
import scala.collection.mutable
import scala.io.Source

object UserImplDaskPython {

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

  def dag2DaskPython(spec: JsValue)(graph: Graph[DFOperator]): SourceCode = {
    val preamble = generatePreamble()
    val l = mutable.ListBuffer[String]()
    val variablePrefix = "df"
    val finalVariableName = "result"

    // Add dask imports
    l += "import dask.dataframe as dd"
    l += "import pandas as pd"
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

    // Dask uses task graph visualization or compute() for execution
//    l += s"final_graph = $finalVariableName.explain()"
//     l += s"final_df = $finalVariableName.compute()"
    l += s"""$finalVariableName.expr.optimize().pprint()"""

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
    val batch = (fuzzer.core.global.State.iteration / config.refreshUdfsAfter).toInt
    val outDir = s"generated/Dask"
    val outPath = s"$outDir/udfs_$batch.json"
    val prompt = s"""
Generate a json file of the following format
```
{ "functions": ["def mapPartitionsUdf(df): ..."] }
```
The functions array should contain ${config.numUdfsPerLLMCall} Python functions. Each function should be short and simple that does something arbitrary.
Each function should be:
- Named "mapPartitionsUdf"
- Complete and runnable
- Contain only code (no comments or docstrings)
- Take a single pandas DataFrame argument and return a pandas DataFrame
- Should be between 1-10 lines of code
- Must preserve the DataFrame structure (same columns)
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
      case "dd.read_parquet" =>
        s"""dd.read_parquet("tpcds-data-5pc/$tableName").rename(columns={col: f"{col}_${node.id}" for col in dd.read_parquet("tpcds-data-5pc/$tableName").columns})"""
      case _ =>
        // Generate synthetic data as Dask DataFrame from pandas
        val rows = Random.nextInt(90) + 10
        val cols = Random.nextInt(5) + 3
        val npartitions = Random.nextInt(4) + 1
        s"""dd.from_pandas(pd.DataFrame({
           |    ${(0 until cols).map(i => s"'col_$i': np.random.rand($rows)").mkString(",\n    ")}
           |}), npartitions=$npartitions)""".stripMargin
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
      case "groupby" => generateGroupByOperation(node, parameters, in1)
      case "map_partitions" => generateMapPartitionsOperation(node, parameters, in1)
      case "query" => generateQueryOperation(node, parameters, in1)
      case "loc" => generateLocOperation(node, parameters, in1)
      case "sort_values" => generateSortValuesOperation(node, parameters, in1)
      case "drop_duplicates" => generateDropDuplicatesOperation(node, parameters, in1)
      case "head" => generateHeadOperation(node, parameters, in1)
      case "fillna" => generateFillnaOperation(node, parameters, in1)
      case "dropna" => generateDropnaOperation(node, parameters, in1)
      case "repartition" => generateRepartitionOperation(node, parameters, in1)
      case "persist" => generatePersistOperation(node, parameters, in1)
      case "compute" => generateComputeOperation(node, parameters, in1)
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
      case "merge" => generateMergeOperation(node, parameters, in1, in2)
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
    updateSourceState(node, parameters \ "columns", "columns", "dict", s"{'$oldCol': '$newName'}")
    propagateState(node)
    s"$in1.rename(columns={'$oldCol': '$newName'})"
  }

  private def constructAggFollowup(node: Node[DFOperator]): String = {
    val (table, col) = pickRandomColumnFromReachableSources(node)
    // Choose between sum, mean, count, min, max
    val aggFuncs = Seq("sum(numeric_only=True)", "mean(numeric_only=True)", "count()", "min(numeric_only=True)", "max(numeric_only=True)")
    val chosenAggFunc = aggFuncs(scala.util.Random.nextInt(aggFuncs.length))

    val fullColName = col.name

    filterColumns(fullColName, node)
    propagateState(node)

    // Dask groupby aggregation syntax
    s"$chosenAggFunc.reset_index()[['$fullColName']]"
  }

  private def generateGroupByOperation(
                                        node: Node[DFOperator],
                                        parameters: JsObject,
                                        in1: String
                                      ): String = {
    val groupCol = pickRandomColumnFromReachableSources(node)._2.name
    s"$in1.groupby('$groupCol').${constructAggFollowup(node)}"
  }

  private def generateMapPartitionsOperation(
                                              node: Node[DFOperator],
                                              parameters: JsObject,
                                              in1: String
                                            ): String = {
    // map_partitions applies a function to each partition (pandas DataFrame)
    // Generate a simple transformation function
    val (_, col) = pickRandomColumnFromReachableSources(node)
    val colName = col.name

    val udfProb = 0.3
    if (Random.nextDouble() < udfProb) {
      // Use the preloaded UDF
      s"$in1.map_partitions(mapPartitionsUdf, meta=$in1._meta)"
    } else {
      // Generate inline lambda transformation
      col.dataType match {
        case fuzzer.data.types.IntegerType | fuzzer.data.types.LongType =>
          s"$in1.map_partitions(lambda df: df.assign($colName=df['$colName'] * 2), meta=$in1._meta)"
        case fuzzer.data.types.FloatType | fuzzer.data.types.DecimalType =>
          s"$in1.map_partitions(lambda df: df.assign($colName=df['$colName'].round(2)), meta=$in1._meta)"
        case fuzzer.data.types.StringType =>
          s"$in1.map_partitions(lambda df: df.assign($colName=df['$colName'].str.upper()), meta=$in1._meta)"
        case _ =>
          s"$in1.map_partitions(lambda df: df, meta=$in1._meta)"
      }
    }
  }

  private def generateQueryOperation(
                                      node: Node[DFOperator],
                                      parameters: JsObject,
                                      in1: String
                                    ): String = {
    val predicate = generateQueryPredicate(node)
    s"""$in1.query("$predicate")"""
  }

  private def generateLocOperation(
                                    node: Node[DFOperator],
                                    parameters: JsObject,
                                    in1: String
                                  ): String = {
    val predicate = generateLocPredicate(node, in1)
    s"$in1.loc[$predicate]"
  }

  private def generateSortValuesOperation(
                                           node: Node[DFOperator],
                                           parameters: JsObject,
                                           in1: String
                                         ): String = {
    val sortCol = pickRandomColumnFromReachableSources(node)._2.name
    val ascending = Random.choice(List("True", "False"))
    s"$in1.sort_values(by='$sortCol', ascending=$ascending)"
  }

  private def generateDropDuplicatesOperation(
                                               node: Node[DFOperator],
                                               parameters: JsObject,
                                               in1: String
                                             ): String = {
    if (Random.nextBoolean()) {
      val col = pickRandomColumnFromReachableSources(node)._2.name
      s"$in1.drop_duplicates(subset=['$col'])"
    } else {
      s"$in1.drop_duplicates()"
    }
  }

  private def generateHeadOperation(
                                     node: Node[DFOperator],
                                     parameters: JsObject,
                                     in1: String
                                   ): String = {
    val n = Random.nextInt(100) + 1
    val compute = Random.choice(List("True", "False"))
    s"$in1.head(n=$n, compute=$compute)"
  }

  private def generateFillnaOperation(
                                       node: Node[DFOperator],
                                       parameters: JsObject,
                                       in1: String
                                     ): String = {
    val (_, col) = pickRandomColumnFromReachableSources(node)
    col.dataType match {
      case fuzzer.data.types.IntegerType | fuzzer.data.types.LongType =>
        s"$in1.fillna(value=0)"
      case fuzzer.data.types.FloatType | fuzzer.data.types.DecimalType =>
        s"$in1.fillna(value=0.0)"
      case fuzzer.data.types.StringType =>
        s"$in1.fillna(value='')"
      case fuzzer.data.types.BooleanType =>
        s"$in1.fillna(value=False)"
      case _ =>
        s"$in1.fillna(value=0)"
    }
  }

  private def generateDropnaOperation(
                                       node: Node[DFOperator],
                                       parameters: JsObject,
                                       in1: String
                                     ): String = {
    val columns = getAllColumns(node).map(_._2).take(Random.nextInt(3) + 1)
    val colList = columns.map(c => s"'${c.name}'").mkString(", ")
    val how = Random.choice(List("'any'", "'all'"))
    s"$in1.dropna(subset=[$colList], how=$how)"
  }

  private def generateRepartitionOperation(
                                            node: Node[DFOperator],
                                            parameters: JsObject,
                                            in1: String
                                          ): String = {
    val npartitions = Random.nextInt(8) + 1
    s"$in1.repartition(npartitions=$npartitions)"
  }

  private def generatePersistOperation(
                                        node: Node[DFOperator],
                                        parameters: JsObject,
                                        in1: String
                                      ): String = {
    s"$in1.persist()"
  }

  private def generateComputeOperation(
                                        node: Node[DFOperator],
                                        parameters: JsObject,
                                        in1: String
                                      ): String = {
    // Note: compute() returns a pandas DataFrame, wrapping it back to dask
    s"dd.from_pandas($in1.compute(), npartitions=1)"
  }

  private def generateMergeOperation(
                                      node: Node[DFOperator],
                                      parameters: JsObject,
                                      in1: String,
                                      in2: String
                                    ): String = {
    val joinTypes = List("inner", "left", "right", "outer")
    val how = joinTypes(Random.nextInt(joinTypes.length))

    UserImplFlinkPython.pickTwoColumns(node.value.stateView) match {
      case Some(((_, leftCol), (_, rightCol))) => s"$in1.merge($in2, left_on='${leftCol.name}', right_on='${rightCol.name}', how='$how')"
      case None =>
        val col = pickRandomColumnFromReachableSources(node)
        s"$in1.merge($in2, on='${col._2.name}', how='$how')"
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
          case "dict" | "dict[str,str]" => "{}"
          case "callable" => "lambda x: x"
          case "dict|DataFrame" => "{}"
          case "scalar|dict" => Random.nextInt(100).toString
          case "list[str]" =>
            val cols = getAllColumns(node).map(_._2.name).take(Random.nextInt(2) + 1)
            s"[${cols.map(c => s"'$c'").mkString(", ")}]"
          case "str|list[str]" =>
            if (Random.nextBoolean()) {
              val col = pickRandomColumnFromReachableSources(node)._2.name
              s"'$col'"
            } else {
              val cols = getAllColumns(node).map(_._2.name).take(Random.nextInt(2) + 1)
              s"[${cols.map(c => s"'$c'").mkString(", ")}]"
            }
          case "bool|list[bool]" =>
            if (Random.nextBoolean()) "True" else "False"
          case "Expression" => generateLocPredicate(node, "df")
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

  def generateDaskExpression(node: Node[DFOperator], col: ColumnMetadata, dfVar: String): String = {
    val colName = col.name

    col.dataType match {
      case fuzzer.data.types.IntegerType | fuzzer.data.types.LongType =>
        val op = Random.nextInt(4)
        op match {
          case 0 => s"$dfVar['$colName'] * 2"
          case 1 => s"$dfVar['$colName'] + ${Random.nextInt(100)}"
          case 2 => s"$dfVar['$colName'].abs()"
          case _ => s"$dfVar['$colName']"
        }
      case fuzzer.data.types.FloatType | fuzzer.data.types.DecimalType =>
        val op = Random.nextInt(3)
        op match {
          case 0 => s"$dfVar['$colName'].round(2)"
          case 1 => s"$dfVar['$colName'] * ${Random.nextFloat()}"
          case _ => s"$dfVar['$colName']"
        }
      case fuzzer.data.types.StringType =>
        val op = Random.nextInt(3)
        op match {
          case 0 => s"$dfVar['$colName'].str.len()"
          case 1 => s"$dfVar['$colName'].str.upper()"
          case _ => s"$dfVar['$colName']"
        }
      case fuzzer.data.types.BooleanType =>
        s"~$dfVar['$colName']"
      case _ =>
        s"$dfVar['$colName']"
    }
  }

  def generateQueryPredicate(node: Node[DFOperator]): String = {
    val (_, col) = pickRandomColumnFromReachableSources(node)
    val colName = col.name

    col.dataType match {
      case fuzzer.data.types.IntegerType | fuzzer.data.types.LongType =>
        val value = Random.nextInt(100)
        val ops = List(">", "<", ">=", "<=", "==", "!=")
        val op = ops(Random.nextInt(ops.length))
        s"$colName $op $value"
      case fuzzer.data.types.FloatType | fuzzer.data.types.DecimalType =>
        val value = Random.nextFloat() * 100
        s"$colName > $value"
      case fuzzer.data.types.StringType =>
        // Query syntax for string length comparison
        s"$colName.str.len() > 5"
      case fuzzer.data.types.BooleanType =>
        if (Random.nextBoolean()) s"$colName == True" else s"$colName == False"
      case _ =>
        s"$colName == $colName"  // Always true, fallback
    }
  }

  def generateLocPredicate(node: Node[DFOperator], dfVar: String): String = {
    val (_, col) = pickRandomColumnFromReachableSources(node)
    val colName = col.name

    col.dataType match {
      case fuzzer.data.types.IntegerType | fuzzer.data.types.LongType =>
        val value = Random.nextInt(100)
        val ops = List(">", "<", ">=", "<=", "==", "!=")
        val op = ops(Random.nextInt(ops.length))
        s"$dfVar['$colName'] $op $value"
      case fuzzer.data.types.FloatType | fuzzer.data.types.DecimalType =>
        val value = Random.nextFloat() * 100
        s"$dfVar['$colName'] > $value"
      case fuzzer.data.types.StringType =>
        s"$dfVar['$colName'].str.len() > 5"
      case fuzzer.data.types.BooleanType =>
        if (Random.nextBoolean()) s"$dfVar['$colName']" else s"~$dfVar['$colName']"
      case _ =>
        s"$dfVar['$colName'].notna()"
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

  def getAllColumns(node: Node[DFOperator], preferUnique: Boolean = false ): Seq[(TableMetadata, ColumnMetadata)] = {
    val tablesColPairs = node.value.stateView.values.toSeq.flatMap { t =>
      t.columns.map(c => (t, c))
    }
    tablesColPairs
  }

  def pickRandomColumnFromReachableSources(
                                            node: Node[DFOperator],
                                            preferUnique: Boolean = false
                                          ): (TableMetadata, ColumnMetadata) = {
    val tablesColPairs = getAllColumns(node, preferUnique).filter {
      case (_, col) =>
        col.metadata.get("gen-iteration") match {
          case None => true
          case Some(i) => fuzzer.core.global.State.iteration.toString != i
        }
    }

    val numericOnly = tablesColPairs.filter{
      case (_, col) if col.dataType == StringType => false
      case _ => true
    }

    val finalList = tablesColPairs // if(numericOnly.nonEmpty) numericOnly else tablesColPairs


    assert(finalList.nonEmpty, s"Expected columnNames to be non-empty: stateViewMap = ${node.value.stateView}")
    val pick = finalList(Random.nextInt(finalList.length))
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