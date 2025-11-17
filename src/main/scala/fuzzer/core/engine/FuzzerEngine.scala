package fuzzer.core.engine

import fuzzer.code.SourceCode
import fuzzer.core.exceptions.{DAGFuzzerException, ImpossibleDFGException, MismatchException, Success}
import fuzzer.core.global.FuzzerConfig
import fuzzer.core.graph.{DAGParser, DFOperator, Graph, Node}
import fuzzer.core.interfaces.{CodeExecutor, CodeGenerator, DataAdapter, ExecutionResult}
import fuzzer.data.tables.TableMetadata
import fuzzer.utils.generation.dag.DAGGenUtils.generateRandomInvertedBinaryTreeDAG
import fuzzer.utils.io.ReadWriteUtils
import fuzzer.utils.io.ReadWriteUtils.{prettyPrintStats, writeLiveStats}
import fuzzer.utils.random.Random
import org.yaml.snakeyaml.Yaml
import play.api.libs.json.{JsObject, JsValue}

import scala.io.StdIn
import scala.collection.mutable
import java.io.{File, FileWriter}
import scala.util.Using
import scala.util.control.Breaks.{break, breakable}

class FuzzerEngine(
                    val config: FuzzerConfig,
                    val spec: JsValue,
                    val dataAdapter: DataAdapter,
                    val codeGenerator: CodeGenerator,
                    val codeExecutor: CodeExecutor
                  ) {


  def pickRandomSource(opMap: Map[String, Seq[String]]): Option[String] = {
    opMap.get("source") match {
      case Some(ops) =>
        val idx = Random.nextInt(ops.size)
        ops.lift(idx)
      case None => None
    }
  }

  def pickRandomUnaryOp(opMap: Map[String, Seq[String]], node: Node[DFOperator]): Option[String] = {
    opMap.get("unary") match {
      case Some(ops) =>
        val idx = Random.nextInt(ops.size)
        ops.lift(idx)
      case None =>
        throw new RuntimeException("Abstract DFG construction failed: No unary operators in spec to choose from!")
    }
  }

  def pickRandomBinaryOp(opMap: Map[String, Seq[String]]): Option[String] = {
    opMap.get("binary") match {
      case Some(ops) =>
        val idx = Random.nextInt(ops.size)
        ops.lift(idx)
      case None => None
    }
  }

  def buildOpMap(spec: JsValue): Map[String, Seq[String]] = {
    spec.as[JsObject].fields.foldLeft(Map.empty[String, Seq[String]]) {
      case (acc, (name, definition)) =>
        val opType = (definition \ "type").as[String]
        acc.updatedWith(opType) {
          case Some(seq) => Some(seq :+ name)
          case None => Some(Seq(name))
        }
    }
  }

  private def fillOperators(graph: Graph[DFOperator], spec: JsValue): Graph[DFOperator] = {
    val opMap = buildOpMap(spec)

    graph.transformNodes { node =>
      val opOpt = (node.getInDegree, node.getOutDegree) match {
        //        case (_,0) => pickRandomAction(opMap) // Better to delegate action choice to DFG2Source converter
        case (0,_) => pickRandomSource(opMap)
        case (1,_) => pickRandomUnaryOp(opMap, node)
        case (2,_) => pickRandomBinaryOp(opMap)
        case (in, _) => throw new ImpossibleDFGException(s"Impossible DFG provided, a node has in-degree=$in")
      }
      assert(opOpt.isDefined, s"Couldn't find an operator in the provided spec that fits the node: $node")
      val Some(op) = opOpt
      new DFOperator(op, node.value.id)
    }
  }

  def initializeStateViews(graph: Graph[DFOperator]): Unit = {
    for (node <- graph.nodes) {
      val dfOp = node.value

      val stateCopies: Map[String, TableMetadata] = node.getReachableSources
        .map(source => source.id -> source.value.state.copy())
        .toMap

      dfOp.stateView = stateCopies
    }
  }

  def constructDFG(dag: Graph[DFOperator], apiSpec: JsValue, tables: Seq[TableMetadata]): Graph[DFOperator] = {

    val dfg = fillOperators(dag, apiSpec)
    dfg.computeReachabilityFromSources()

    val zipped = dfg.getSourceNodes.sortBy(_.value.id).zip(tables)
    val preprocessed = dataAdapter.prepTableMetadata(zipped)
    preprocessed.foreach {
      case (node, table) =>
        node.value.state = table
    }

    fuzzer.core.global.State.src2TableMap = zipped.map {
      case (node, table) =>
        node.id -> table
    }.toMap

    initializeStateViews(dfg)
    dfg
  }

  def generateSingleProgram(
                             dag: Graph[DFOperator],
                             spec: JsValue,
                             dag2SourceFunc: Graph[DFOperator] => SourceCode,
                             tables: Seq[TableMetadata]
                           ): SourceCode = {
    val (isInvalid, message) = isInvalidDFG(dag)
    if (isInvalid) {
      throw new ImpossibleDFGException(s"Impossible to convert DAG to DFG. $message")
    }


    val dfg = constructDFG(dag, spec, tables)
    val generatedSource = dfg.generateCode(dag2SourceFunc)
    generatedSource
  }

  private def createDAGIteratorInternal(config: FuzzerConfig): Iterator[(Graph[DFOperator], String)] = {
    new Iterator[(Graph[DFOperator], String)] {
      var counter: Int = -1
      override def hasNext: Boolean = true

      override def next(): (Graph[DFOperator], String) = {
        counter += 1
        (
          generateRandomInvertedBinaryTreeDAG(
            valueGenerator = DFOperator.fromInt,
            maxDepth = Random.nextIntInclusiveRange(3, 7)
          ), s"dag_$counter")
      }
    }
  }

  private def createDAGGenerator(config: FuzzerConfig): Iterator[(Graph[DFOperator], String)] = {
    // Uncomment this to use external dag generator
    // generateYamlFilesInfinite(config.d)
    createDAGIteratorInternal(config)
  }

  def run(): FuzzerResults = {
    val stats = new CampaignStats()
    stats.setSeed(config.seed)

    // Setup environment
    val terminateF = codeExecutor.setupEnvironment()
    dataAdapter.loadData(codeExecutor)
    val allTables = dataAdapter.getTables

    // Track time for timeout
    val startTime = System.currentTimeMillis()

    def shouldStop: Boolean = {
      val elapsed = (System.currentTimeMillis() - startTime) / 1000
      if (config.exitAfterNSuccesses) {
        stats.getGenerated >= config.N
      } else {
        elapsed >= config.timeLimitSec
      }
    }

    try {
      // Main fuzzing loop
      val dagIterator = createDAGGenerator(config)
      while (!shouldStop) {
        val (dag, dagName) = dagIterator.next()
        processSingleDAG(dag, dagName, stats, shouldStop, startTime, allTables)
      }

      // Return final results
      codeExecutor.tearDownEnvironment(terminateF)
      FuzzerResults(stats)
    } catch {
      case ex: DAGFuzzerException =>
        println(s"ERROR MSG: ${ex.inner.getMessage}")
        codeExecutor.tearDownEnvironment(terminateF)
        throw ex.inner
    }
  }

  def scalaFilesLazy(replayPath: String): LazyList[File] = {
    val replayDir = new File(replayPath)

    def filesInDir(dir: File, inSuccessDir: Boolean): LazyList[File] = {
      if (!dir.exists() || !dir.isDirectory) {
        LazyList.empty
      } else {
        // Check if THIS directory is named "Success"
        val isSuccess = dir.getName == "Success"
        val shouldCollect = inSuccessDir || isSuccess

        val (dirs, files) = dir.listFiles().partition(_.isDirectory)

        // Only collect scala files if we're inside a "Success" directory
        val scalaFiles = if (shouldCollect) {
          LazyList.from(files.filter(_.getName.endsWith(".scala")))
        } else {
          LazyList.empty
        }

        // Recursively process subdirectories
        val subDirFiles = LazyList.from(dirs).flatMap(d =>
          filesInDir(d, shouldCollect)
        )

        scalaFiles #::: subDirFiles
      }
    }

    filesInDir(replayDir, inSuccessDir = false)
  }

  def replaceExplainWithShow(code: String): String = {
    code.replace("sink.explain(true)", "println(sink.show())")
  }

  def movePreloadedUDFOutOfObject(code: String): String = {
    // Find the start: line containing "val preloadedUDF = udf((s: Any) => {"
    val startPattern = """val preloadedUDF = udf\(\(s: Any\) => \{""".r

    var modifiedCode = code
    var udfBlock: Option[String] = None

    // Find and remove all occurrences of preloadedUDF
    var continue = true
    while (continue) {
      startPattern.findFirstMatchIn(modifiedCode) match {
        case Some(startMatch) =>
          val startIndex = startMatch.start

          // Find the closing "}).asNondeterministic()" after preloadedUDF
          val closingPattern = """\}\s*\)\s*\.asNondeterministic\s*\(\s*\)""".r

          closingPattern.findFirstMatchIn(modifiedCode.substring(startIndex)) match {
            case Some(closingMatch) =>
              val endIndex = startIndex + closingMatch.end

              // Extract the preloadedUDF block (save it only once)
              if (udfBlock.isEmpty) {
                udfBlock = Some(modifiedCode.substring(startIndex, endIndex))
              }

              // Remove this occurrence
              modifiedCode = modifiedCode.substring(0, startIndex) + modifiedCode.substring(endIndex)

            case None =>
              continue = false
          }

        case None =>
          continue = false
      }
    }

    // If we found at least one UDF, insert it before the first object
    udfBlock match {
      case Some(block) =>
        val objectPattern = """(?m)^(\s*)object\s+\w+""".r

        objectPattern.findFirstMatchIn(modifiedCode) match {
          case Some(objMatch) =>
            val insertIndex = objMatch.start

            // Insert UDF block before object
            modifiedCode.substring(0, insertIndex) +
              block + "\n\n" +
              modifiedCode.substring(insertIndex)

          case None =>
            // No object found, return modified code
            modifiedCode
        }

      case None =>
        // No UDF found, return original
        code
    }
  }

  def moveFilterUDFsOutOfObject(code: String): String = {
    // Find the start: line containing "val filterUdfString = udf((arg: String) => {"
    val startPattern = """val filterUdfString = udf\(\(arg: String\) => \{""".r

    // Find the end: the closing of "val filterUdfDecimal = udf((arg: Double) => {"
    val filterUdfDecimalStart = """val filterUdfDecimal = udf\(\(arg: Double\) => \{""".r

    var modifiedCode = code
    var udfBlock: Option[String] = None

    // Find and remove all occurrences of the filter UDFs block
    var continue = true
    while (continue) {
      startPattern.findFirstMatchIn(modifiedCode) match {
        case Some(startMatch) =>
          val startIndex = startMatch.start

          // Find where filterUdfDecimal starts
          filterUdfDecimalStart.findFirstMatchIn(modifiedCode.substring(startIndex)) match {
            case Some(endDeclMatch) =>
              val endDeclIndex = startIndex + endDeclMatch.start

              // Now find the closing "}).asNondeterministic()" after filterUdfDecimal
              val closingPattern = """\}\s*\)\s*\.asNondeterministic\s*\(\s*\)""".r

              // Search for the first closing pattern after filterUdfDecimal declaration
              closingPattern.findFirstMatchIn(modifiedCode.substring(endDeclIndex)) match {
                case Some(closingMatch) =>
                  val endIndex = endDeclIndex + closingMatch.end

                  // Extract the filter UDFs block (save it only once)
                  if (udfBlock.isEmpty) {
                    udfBlock = Some(modifiedCode.substring(startIndex, endIndex))
                  }

                  // Remove this occurrence
                  modifiedCode = modifiedCode.substring(0, startIndex) + modifiedCode.substring(endIndex)

                case None =>
                  continue = false
              }

            case None =>
              continue = false
          }

        case None =>
          continue = false
      }
    }

    // If we found at least one UDF block, insert it before the first object
    udfBlock match {
      case Some(block) =>
        val objectPattern = """(?m)^(\s*)object\s+\w+""".r

        objectPattern.findFirstMatchIn(modifiedCode) match {
          case Some(objMatch) =>
            val insertIndex = objMatch.start

            // Insert UDF block before object
            modifiedCode.substring(0, insertIndex) +
              block + "\n\n" +
              modifiedCode.substring(insertIndex)

          case None =>
            // No object found, return modified code
            modifiedCode
        }

      case None =>
        // No UDF found, return original
        code
    }
  }

  def moveUDFsOutOfObject(code: String): String = {
    // First pass: move preloadedUDF
    val afterFirstPass = movePreloadedUDFOutOfObject(code)

    // Second pass: move the three filter UDFs
    moveFilterUDFsOutOfObject(afterFirstPass)
  }

  def replaceExcludedRulesLine(code: String): String = {
    val oldLine = """val excludedRules = excludableRules.mkString\(","\)"""

    val newCode = """val criticalRules = Set(
  "org.apache.spark.sql.catalyst.optimizer.PruneFilters",
  "org.apache.spark.sql.catalyst.optimizer.ColumnPruning",
  "org.apache.spark.sql.catalyst.optimizer.RemoveNoopOperators",
  "org.apache.spark.sql.execution.datasources.PruneFileSourcePartitions",
  "org.apache.spark.sql.catalyst.optimizer.LimitPushDown",
  "org.apache.spark.sql.catalyst.optimizer.UnwrapCastInBinaryComparison",
  "org.apache.spark.sql.catalyst.optimizer.PropagateEmptyRelation",
  "org.apache.spark.sql.catalyst.optimizer.BooleanSimplification",
  "org.apache.spark.sql.catalyst.optimizer.ReplaceNullWithFalseInPredicate",
)
val excludedRules = (excludableRules -- criticalRules).mkString(",")
"""

    val pattern = oldLine.r

    pattern.replaceAllIn(code, newCode)
  }

  def preprocessSparkCode(code: String): String = {
    val withCommentFix = code + "*/"
    val withShow = replaceExplainWithShow(withCommentFix)
    val withUDFsMoved = moveUDFsOutOfObject(withShow)
    val finalCode = replaceExcludedRulesLine(withUDFsMoved)

    finalCode
  }

  def replay(): FuzzerResults = {
    val stats = new CampaignStats()

    val terminateF = codeExecutor.setupEnvironment()
    dataAdapter.loadData(codeExecutor, s => Array("time_dim", "promotion").contains(s))

    // here the spec path is actually the path to artifacts dir
    val lazyFileIter = scalaFilesLazy(config.artifactsDir)

    try {
      lazyFileIter.foreach { f =>
        println(s"===== START: $f =====")
        val contents = Using(scala.io.Source.fromFile(f))(_.mkString).get
        val executable = preprocessSparkCode(contents)
        codeExecutor.executeRaw(executable)
        println(s"===== END: $f =====")
      }

      // Return final results
      codeExecutor.tearDownEnvironment(terminateF)
      FuzzerResults(stats)
    } catch {
      case ex: DAGFuzzerException =>
        println(s"ERROR MSG: ${ex.inner.getMessage}")
        codeExecutor.tearDownEnvironment(terminateF)
        throw ex.inner
    }
  }

  def isInvalidDFG(dag: Graph[DFOperator]): (Boolean, String) = {
    dag match {
      case _ if dag.nodes.exists(_.getInDegree > 2) =>
        (true, "Has a node with in-degree > 2.")
      case _ if dag.getSinkNodes.length > 1 =>
        (true, "DAG has more than one sink.")
      case _ if dag.getSinkNodes.head.parents.length > 1 =>
        (false, "Sink has more than one parent.") // Not ideal, but can let this slide.
      case _ =>
        (false, "")
    }

  }

  private def processSingleDAG(dag: Graph[DFOperator], dagName: String, stats: CampaignStats, shouldStop: => Boolean, startTime: Long, allTables: Seq[TableMetadata]): Unit = {
    try {
      val (isInvalid, message) = isInvalidDFG(dag)
      if (isInvalid) {
        throw new ImpossibleDFGException(s"Impossible to convert DAG to DFG. $message")
      }

      breakable {
        for (i <- 1 to config.p) {
          if (shouldStop)
            break

          fuzzer.core.global.State.iteration += 1
          stats.setIteration(fuzzer.core.global.State.iteration)

          try {
            // --- Sampling tables without replacement ---
            // val selectedTables = Random.shuffle(allTables).take(dag.getSourceNodes.length).toList
            // --- Sampling tables with replacement ---
            val selectedTables = (1 to dag.getSourceNodes.length).map(_ => allTables(Random.nextInt(allTables.length))).toList
            val sourceCode = generateSingleProgram(dag, spec, codeGenerator.getDag2CodeFunc, selectedTables)
            println(s"-> EXECUTING g_${stats.getGenerated}-a_${stats.getAttempts}")

            val results = if (true || stats.getGenerated == 10) {
              codeExecutor.execute(sourceCode)
            } else {
              ExecutionResult(success = false, new Success(""))
            }

            stats.setCumulativeCoverageIfChanged(
              results.coverage,
              fuzzer.core.global.State.iteration,
              (System.currentTimeMillis()-startTime)/1000)

            val result = results.exception
            val resultType = result.getClass.toString.split('.').last
            val ruleBranchesCovered = results.coverage.toSet.size

            result match {
              case ex: DAGFuzzerException => throw ex
              case _: Success =>
                println(s"==== FUZZER ITERATION ${fuzzer.core.global.State.iteration} GENERATED: ${stats.getGenerated}====")
                println(s"RESULT: $result")
                println(s"$ruleBranchesCovered")
              case _: MismatchException =>
                println(s"==== FUZZER ITERATION ${fuzzer.core.global.State.iteration}====")
                println(s"RESULT: $result")
                println(s"$ruleBranchesCovered")
              case _ =>
                println(s"==== FUZZER ITERATION ${fuzzer.core.global.State.iteration}====")
                println(s"RESULT: $resultType")
            }
            stats.updateWith(resultType) {
              case Some(existing) => Some((existing.toInt + 1).toString)
              case None => Some("1")
            }

            // Create subdirectory inside outDir using the result value
            val resultSubDir = new File(config.outDir, resultType)
            resultSubDir.mkdirs() // Creates the directory if it doesn't exist

            // Prepare output file in the result-named subdirectory
            val outFileName = s"g_${stats.getGenerated}-a_${stats.getAttempts}-${dagName.stripSuffix(".yaml")}-dfg$i${config.outExt}"
            val outFile = new File(resultSubDir, outFileName)

            // Write the fullSource to the file
            val writer = new FileWriter(outFile)
            writer.write(results.combinedSourceWithResults+s"\n\n//Optimizer Branch Coverage: $ruleBranchesCovered")
            writer.close()

            if (stats.getGenerated % config.updateLiveStatsAfter == 0) {
              writeLiveStats(config, stats, startTime)
            }

            stats.setGenerated(stats.getGenerated+1)
          } catch {
            case ex: DAGFuzzerException => throw ex
            case ex: Exception =>
              println("==========")
              println(s"DFG construction or codegen failed, attempt #$i. Reason: $ex")
              println(ex)
              println(ex.getStackTrace.mkString("\t", "\n\t", ""))
              println("==========")
          } finally {
            stats.setAttempts(stats.getAttempts+1)
          }
        }
      }
    } catch {
      case ex: ImpossibleDFGException =>
        stats.setAttempts(stats.getAttempts+1)
        println(s"DFG construction or codegen failed for ??? . Reason: ${ex.getMessage}")
      case ex: DAGFuzzerException => throw ex
      case ex: Exception =>
        stats.setAttempts(stats.getAttempts+1)
        println(s"Failed to parse DAG file: ???. Reason: ${ex.getMessage}")
    }
  }
}

case class FuzzerResults(stats: CampaignStats)