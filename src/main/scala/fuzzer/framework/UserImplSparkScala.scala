package fuzzer.framework

import fuzzer.code.SourceCode
import fuzzer.core.graph.{DFOperator, Graph, Node}
import fuzzer.data.tables.{ColumnMetadata, TableMetadata}
import fuzzer.data.types.DataType
import fuzzer.utils.random.Random
import play.api.libs.json._

import scala.collection.mutable

object UserImplSparkScala {


  def generateStringFilterUDF(): String = {
    // String Filter UDFs
    val stringFilters = List(
      """
        |val filterUdfString = udf((arg: String) => {
        |  arg.length > 5
        |}).asNondeterministic()
        |""".stripMargin,

      """
        |val filterUdfString = udf((arg: String) => {
        |  arg.startsWith("A") || arg.startsWith("a")
        |}).asNondeterministic()
        |""".stripMargin,

      """
        |val filterUdfString = udf((arg: String) => {
        |  arg.toLowerCase.contains("test")
        |}).asNondeterministic()
        |""".stripMargin,

      """
        |val filterUdfString = udf((arg: String) => {
        |  arg.forall(_.isLetter) && arg.length < 10
        |}).asNondeterministic()
        |""".stripMargin,

      """
        |val filterUdfString = udf((arg: String) => {
        |  arg.count(_ == 'e') >= 2
        |}).asNondeterministic()
        |""".stripMargin
    )
    Random.choice(stringFilters)
  }

  def generateIntFilterUDF(): String = {
    // Integer Filter UDFs
    val intFilters = List(
      """
        |val filterUdfInteger = udf((arg: Int) => {
        |  arg > 0 && arg % 2 == 0
        |}).asNondeterministic()
        |""".stripMargin,

      """
        |val filterUdfInteger = udf((arg: Int) => {
        |  arg >= 10 && arg <= 100
        |}).asNondeterministic()
        |""".stripMargin,

      """
        |val filterUdfInteger = udf((arg: Int) => {
        |  arg % 3 == 0 || arg % 5 == 0
        |}).asNondeterministic()
        |""".stripMargin,

      """
        |val filterUdfInteger = udf((arg: Int) => {
        |  arg.toString == arg.toString.reverse
        |}).asNondeterministic()
        |""".stripMargin,

      """
        |val filterUdfInteger = udf((arg: Int) => {
        |  arg > 0 && (arg & (arg - 1)) == 0
        |}).asNondeterministic()
        |""".stripMargin
    )

    Random.choice(intFilters)
  }

  def generateDecimalFilterUDF(): String = {
    // Decimal Filter UDFs
    val decimalFilters = List(
      """
        |val filterUdfDecimal = udf((arg: Double) => {
        |  arg > 0.0 && arg < 1000.0
        |}).asNondeterministic()
        |""".stripMargin,

      """
        |val filterUdfDecimal = udf((arg: Double) => {
        |  (scala.math.round(arg * 100).toDouble / 100.0) == arg
        |}).asNondeterministic()
        |""".stripMargin,

      """
        |val filterUdfDecimal = udf((arg: Double) => {
        |  arg >= 50.5 && arg.toInt % 10 == 5
        |}).asNondeterministic()
        |""".stripMargin,

      """
        |val filterUdfDecimal = udf((arg: Double) => {
        |  (arg * 100) % 25 == 0
        |}).asNondeterministic()
        |""".stripMargin,

      """
        |val filterUdfDecimal = udf((arg: Double) => {
        |  scala.math.abs(arg - scala.math.round(arg).toDouble) > 0.1
        |}).asNondeterministic()
        |""".stripMargin
    )

    Random.choice(decimalFilters)
  }

  def constructDFOCall(spec: JsValue, node: Node[DFOperator], in1: String, in2: String): String = {
    val opName = node.value.name
    val opSpec = spec \ opName

    if (opSpec.isInstanceOf[JsUndefined]) {
      return opName
    }
    val opType = (opSpec \ "type").as[String]
    val parameters = (opSpec \ "parameters").as[JsObject]
    val args = generateArguments(node, parameters, opType, in2)

    // Construct the function call based on operation type
    opType match {
      case "source" => constructSourceCall(node, spec, opName, opType, parameters, args)
      case "unary" if Array("groupBy").contains(opName) => s"$in1.$opName(${args.mkString(", ")}).${constructAggFollowup(node, spec, opName, opType, parameters, args)}"
      case _ => s"$in1.$opName(${args.mkString(", ")})"
    }
  }

  private def constructAggFollowup(node: Node[DFOperator], spec: JsValue, opName: String, opType: String, parameters: JsObject, args: List[String]): String = {
    val (table, col) =  pickRandomColumnFromReachableSources(node)
    val aggFuncs = Seq("sum", "avg", "count", "min", "max")
    val chosenAggFunc = aggFuncs(scala.util.Random.nextInt(aggFuncs.length))

    val useShortcut = chosenAggFunc == "sum" || chosenAggFunc == "avg"

    val fullColName = s"${table.identifier}.${col.name}"
    val aggCol = s"""$chosenAggFunc("$fullColName")"""

    if (useShortcut) {
      s"""$chosenAggFunc("$fullColName")"""
    } else {
      s"""agg($aggCol)"""
    }
  }

  private def constructSourceCall(node: Node[DFOperator], spec: JsValue, opName: String, opType: String, parameters: JsObject, args: List[String]): String = {
    val tpcdsTablesPath = fuzzer.core.global.State.config.get.localTpcdsPath
    val tableName = fuzzer.core.global.State.src2TableMap(node.id).identifier
    opName match {
      case "spark.table" => s"""$opName("tpcds.$tableName").select(spark.table("tpcds.$tableName").columns.map(colName => col(colName).alias(s"$${colName}_${node.id}")): _*).as("${tableName}_${node.id}")"""
      case "spark.read.parquet" => s"""$opName("$tpcdsTablesPath/$tableName").as("${tableName}_${node.id}")"""
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

    val renamedStateView: Map[String, TableMetadata] = dfOp.stateView.map {
      case (id, tableMeta) =>
        val renamed = tableMeta.copy() // get a deep copy
        renamed.setIdentifier(newValue) // modify as needed
        id -> renamed
    }

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
      val gen = s"${Random.alphanumeric.take(fuzzer.core.global.State.config.get.maxStringLength).mkString}"
      updateSourceState(node, param, paramName, paramType, gen)
      propagateState(node)
      gen
    } else {
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

  def chooseUdfSuffixBasedOnType(col: ColumnMetadata): String = {
    col.dataType.name match {
      case "Date" => "String"
      case "Long" | "Boolean" => "Integer"
      case "Float" => "Decimal"
      case t => t
    }
  }

  def generateFilterUDFCall(node: Node[DFOperator], args: List[(TableMetadata, ColumnMetadata)]): String = {
    val fullColExpressions = args.map{case (table, col) => s"""col("${constructFullColumnName(table, col)}")"""}
    val col = args.head._2 // for now we only have single arg udfs so we can assume the list has one element
    val chosenType = chooseUdfSuffixBasedOnType(col)
    s"filterUdf$chosenType(${fullColExpressions.mkString(",")})"
  }

  def generateComplexUDFCall(node: Node[DFOperator], args: List[(TableMetadata, ColumnMetadata)]): String = {
    val fullColExpressions = args.map{case (table, col) => s"""col("${constructFullColumnName(table, col)}")"""}
    s"preloadedUDF(${fullColExpressions.mkString(",")})"
  }

  def generateUDFCall(node: Node[DFOperator], args: List[(TableMetadata, ColumnMetadata)]): String = {
    if(node.value.name.contains("filter"))
      generateFilterUDFCall(node, args)
    else
      generateComplexUDFCall(node, args)
  }

  // NEW: Generate a lit() expression with random value based on data type
  def generateLitExpression(): String = {
    val config = fuzzer.core.global.State.config.get
    val dataTypes = Seq("int", "float", "string", "bool")
    val chosenType = dataTypes(Random.nextInt(dataTypes.length))

    chosenType match {
      case "int" =>
        val value = config.randIntMin + Random.nextInt(config.randIntMax - config.randIntMin + 1)
        s"lit($value)"
      case "float" =>
        val value = config.randFloatMin + Random.nextFloat() * (config.randFloatMax - config.randFloatMin)
        s"lit($value)"
      case "string" =>
        val value = Random.alphanumeric.take(config.maxStringLength).mkString
        s"""lit("$value")"""
      case "bool" =>
        s"lit(${Random.nextBoolean()})"
    }
  }

  // NEW: Generate a when().otherwise() chain
  def generateWhenOtherwiseExpression(node: Node[DFOperator]): String = {
    val config = fuzzer.core.global.State.config.get
    val numConditions = 1 + Random.nextInt(3) // 1-3 when clauses

    val conditions = (0 until numConditions).map { _ =>
      val (table, col) = pickRandomColumnFromReachableSources(node)
      val fullColName = constructFullColumnName(table, col)
      val colExpr = s"""col("$fullColName")"""

      val op = config.logicalOperatorSet.toVector(Random.nextInt(config.logicalOperatorSet.size))
      val value = col.dataType match {
        case fuzzer.data.types.IntegerType | fuzzer.data.types.LongType =>
          config.randIntMin + Random.nextInt(config.randIntMax - config.randIntMin + 1)
        case fuzzer.data.types.FloatType | fuzzer.data.types.DecimalType =>
          config.randFloatMin + Random.nextFloat() * (config.randFloatMax - config.randFloatMin)
        case _ => 0
      }

      val condition = s"$colExpr $op $value"
      val thenValue = generateLitExpression()
      (condition, thenValue)
    }

    val otherwiseValue = generateLitExpression()

    val whenClauses = conditions.map { case (cond, thenVal) =>
      s"when($cond, $thenVal)"
    }.mkString(".")

    s"$whenClauses.otherwise($otherwiseValue)"
  }

  // NEW: Generate a cast() expression
  def generateCastExpression(node: Node[DFOperator]): String = {
    val (table, col) = pickRandomColumnFromReachableSources(node)
    val fullColName = constructFullColumnName(table, col)
    val colExpr = s"""col("$fullColName")"""

    // Target cast types
    val castTypes = Seq("string", "int", "long", "float", "double", "boolean", "date")
    val targetType = castTypes(Random.nextInt(castTypes.length))

    s"""$colExpr.cast("$targetType")"""
  }

  // NEW: Generate window function with partitionBy and orderBy
  def generateWindowFunction(node: Node[DFOperator]): String = {
    val (table, col) = pickRandomColumnFromReachableSources(node)
    val fullColName = constructFullColumnName(table, col)

    // Pick partition columns (1-2 columns)
    val numPartitionCols = 1 + Random.nextInt(2)
    val partitionCols = (0 until numPartitionCols).map { _ =>
      val (t, c) = pickRandomColumnFromReachableSources(node)
      s"""col("${constructFullColumnName(t, c)}")"""
    }.mkString(", ")

    // Pick order column
    val (orderTable, orderCol) = pickRandomColumnFromReachableSources(node)
    val orderColExpr = s"""col("${constructFullColumnName(orderTable, orderCol)}")"""
    val orderDir = if (Random.nextBoolean()) ".asc" else ".desc"

    // Choose window function type
    val windowFuncs = Seq("row_number", "rank", "dense_rank", "percent_rank", "ntile")
    val chosenFunc = windowFuncs(Random.nextInt(windowFuncs.length))

    val funcCall = if (chosenFunc == "ntile") {
      val n = 2 + Random.nextInt(9) // 2-10 tiles
      s"$chosenFunc($n)"
    } else {
      s"$chosenFunc()"
    }

    s"""$funcCall.over(Window.partitionBy($partitionCols).orderBy($orderColExpr$orderDir))"""
  }

  // NEW: Generate array/collection operations
  def generateArrayExpression(node: Node[DFOperator]): String = {
    val arrayOps = Seq("array_contains", "array_distinct", "array_join", "array_max", "array_min", "size", "sort_array", "reverse", "slice")
    val chosenOp = arrayOps(Random.nextInt(arrayOps.length))

    val (table, col) = pickRandomColumnFromReachableSources(node)
    val fullColName = constructFullColumnName(table, col)
    val colExpr = s"""col("$fullColName")"""

    chosenOp match {
      case "array_contains" =>
        val value = generateLitExpression()
        s"array_contains($colExpr, $value)"
      case "array_join" =>
        val delimiter = Random.alphanumeric.take(1).mkString
        s"""array_join($colExpr, "$delimiter")"""
      case "slice" =>
        val start = 1 + Random.nextInt(5)
        val length = 1 + Random.nextInt(5)
        s"slice($colExpr, $start, $length)"
      case _ => s"$chosenOp($colExpr)"
    }
  }

  // NEW: Generate string operations
  def generateStringFunction(node: Node[DFOperator]): String = {
    val (table, col) = pickRandomColumnFromReachableSources(node)
    val fullColName = constructFullColumnName(table, col)
    val colExpr = s"""col("$fullColName")"""

    val stringFuncs = Seq("upper", "lower", "trim", "ltrim", "rtrim", "substring", "concat", "split", "regexp_replace", "length")
    val chosenFunc = stringFuncs(Random.nextInt(stringFuncs.length))

    chosenFunc match {
      case "substring" =>
        val pos = 1 + Random.nextInt(5)
        val len = 1 + Random.nextInt(10)
        s"substring($colExpr, $pos, $len)"
      case "concat" =>
        val (table2, col2) = pickRandomColumnFromReachableSources(node)
        val col2Expr = s"""col("${constructFullColumnName(table2, col2)}")"""
        s"concat($colExpr, $col2Expr)"
      case "split" =>
        val delimiter = Seq(",", " ", "|", "-")(Random.nextInt(4))
        s"""split($colExpr, "$delimiter")"""
      case "regexp_replace" =>
        val pattern = Seq("[0-9]", "[a-z]", "\\\\s+")(Random.nextInt(3))
        val replacement = Random.alphanumeric.take(3).mkString
        s"""regexp_replace($colExpr, "$pattern", "$replacement")"""
      case _ => s"$chosenFunc($colExpr)"
    }
  }

  // NEW: Generate date/timestamp operations
  def generateDateFunction(node: Node[DFOperator]): String = {
    val (table, col) = pickRandomColumnFromReachableSources(node)
    val fullColName = constructFullColumnName(table, col)
    val colExpr = s"""col("$fullColName")"""

    val dateFuncs = Seq("year", "month", "dayofmonth", "dayofweek", "hour", "minute", "second", "date_add", "date_sub", "datediff", "months_between", "to_date", "date_format")
    val chosenFunc = dateFuncs(Random.nextInt(dateFuncs.length))

    chosenFunc match {
      case "date_add" | "date_sub" =>
        val days = Random.nextInt(365)
        s"$chosenFunc($colExpr, $days)"
      case "datediff" =>
        val (table2, col2) = pickRandomColumnFromReachableSources(node)
        val col2Expr = s"""col("${constructFullColumnName(table2, col2)}")"""
        s"datediff($colExpr, $col2Expr)"
      case "months_between" =>
        val (table2, col2) = pickRandomColumnFromReachableSources(node)
        val col2Expr = s"""col("${constructFullColumnName(table2, col2)}")"""
        s"months_between($colExpr, $col2Expr)"
      case "date_format" =>
        val formats = Seq("yyyy-MM-dd", "MM/dd/yyyy", "dd-MM-yyyy", "yyyy-MM-dd HH:mm:ss")
        val format = formats(Random.nextInt(formats.length))
        s"""date_format($colExpr, "$format")"""
      case "to_date" =>
        val formats = Seq("yyyy-MM-dd", "MM/dd/yyyy", "dd-MM-yyyy")
        val format = formats(Random.nextInt(formats.length))
        s"""to_date($colExpr, "$format")"""
      case _ => s"$chosenFunc($colExpr)"
    }
  }

  // NEW: Generate mathematical/numeric operations
  def generateMathFunction(node: Node[DFOperator]): String = {
    val (table, col) = pickRandomColumnFromReachableSources(node)
    val fullColName = constructFullColumnName(table, col)
    val colExpr = s"""col("$fullColName")"""

    val mathFuncs = Seq("abs", "ceil", "floor", "round", "sqrt", "pow", "exp", "log", "sin", "cos", "tan")
    val chosenFunc = mathFuncs(Random.nextInt(mathFuncs.length))

    chosenFunc match {
      case "pow" =>
        val exp = 2 + Random.nextInt(4) // power of 2-5
        s"pow($colExpr, $exp)"
      case "round" =>
        val scale = Random.nextInt(5)
        s"round($colExpr, $scale)"
      case _ => s"$chosenFunc($colExpr)"
    }
  }

  // NEW: Generate null handling operations
  def generateNullHandlingExpression(node: Node[DFOperator]): String = {
    val (table, col) = pickRandomColumnFromReachableSources(node)
    val fullColName = constructFullColumnName(table, col)
    val colExpr = s"""col("$fullColName")"""

    val nullOps = Seq("isNull", "isNotNull", "coalesce", "ifnull", "nullif", "nvl", "nvl2")
    val chosenOp = nullOps(Random.nextInt(nullOps.length))

    chosenOp match {
      case "isNull" | "isNotNull" =>
        s"$colExpr.$chosenOp()"
      case "coalesce" =>
        val defaultValue = generateLitExpression()
        s"coalesce($colExpr, $defaultValue)"
      case "ifnull" | "nvl" =>
        val replacement = generateLitExpression()
        s"$chosenOp($colExpr, $replacement)"
      case "nvl2" =>
        val valueIfNotNull = generateLitExpression()
        val valueIfNull = generateLitExpression()
        s"nvl2($colExpr, $valueIfNotNull, $valueIfNull)"
      case "nullif" =>
        val compareValue = generateLitExpression()
        s"nullif($colExpr, $compareValue)"
      case _ => s"$chosenOp($colExpr)"
    }
  }

  // NEW: Generate aggregation with multiple columns
  def generateComplexAggregation(node: Node[DFOperator]): String = {
    val aggFuncs = Seq("sum", "avg", "count", "min", "max", "stddev", "variance", "collect_list", "collect_set", "approx_count_distinct")
    val chosenFunc = aggFuncs(Random.nextInt(aggFuncs.length))

    val (table, col) = pickRandomColumnFromReachableSources(node)
    val fullColName = constructFullColumnName(table, col)

    chosenFunc match {
      case "approx_count_distinct" =>
        val rsd = 0.01 + Random.nextFloat() * 0.04 // 0.01 to 0.05
        s"""approx_count_distinct("$fullColName", $rsd)"""
      case _ =>
        s"""$chosenFunc("$fullColName")"""
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
    val colExpr = s"""col("$fullColName")"""

    val op = config.logicalOperatorSet.toVector(Random.nextInt(config.logicalOperatorSet.size))

    val intValue = config.randIntMin + Random.nextInt(config.randIntMax - config.randIntMin + 1)
    val floatValue = config.randFloatMin + Random.nextFloat() * (config.randFloatMax - config.randFloatMin)

    val intExpr = s"$colExpr $op $intValue"
    val floatExpr = s"$colExpr $op $floatValue"
    val stringExpr = s"length($colExpr) $op 5"
    val boolExpr = s"!$colExpr"
    val udfExpr = generateUDFCall(node, List(chosenTuple))

    // NEW: Add probability to generate lit(), when/otherwise, cast, or offset expressions
    val advancedExprProb = 0.35f // 15% chance to use advanced expressions

    if (Random.nextFloat() < advancedExprProb) {
      val exprTypes = Seq("lit", "when", "cast", "window", "array", "string", "date", "math", "null")
      val chosenExpr = exprTypes(Random.nextInt(exprTypes.length))

      chosenExpr match {
        case "lit" => s"$colExpr === ${generateLitExpression()}"
        case "when" => generateWhenOtherwiseExpression(node)
        case "cast" => s"${generateCastExpression(node)} === ${generateLitExpression()}"
        case "window" => generateWindowFunction(node)
        case "array" => generateArrayExpression(node)
        case "string" => generateStringFunction(node)
        case "date" => generateDateFunction(node)
        case "math" => generateMathFunction(node)
        case "null" => generateNullHandlingExpression(node)
      }
    } else if (Random.nextFloat() < prob) {
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

  def generatePreamble(): String = {
    s"""
       |${generateStringFilterUDF()}
       |
       |${generateIntFilterUDF()}
       |
       |${generateDecimalFilterUDF()}
       |""".stripMargin
  }

  def dag2SparkScala(spec: JsValue)(graph: Graph[DFOperator]): SourceCode = {
    val l = mutable.ListBuffer[String]()
    val variablePrefix = "auto"
    val finalVariableName = "sink"

    l += generatePreamble()

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

    SourceCode(src=l.mkString("\n"), ast=null)
  }

}
