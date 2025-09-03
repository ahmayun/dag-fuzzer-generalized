package fuzzer.core.engine

import fuzzer.code.SourceCode
import fuzzer.core.exceptions.{DAGFuzzerException, ImpossibleDFGException, MismatchException, Success}
import fuzzer.core.global.FuzzerConfig
import fuzzer.core.graph.{DAGParser, DFOperator, Graph, Node}
import fuzzer.core.interfaces.{CodeExecutor, CodeGenerator, DataAdapter}
import fuzzer.data.tables.TableMetadata
import fuzzer.utils.generation.dag.DAGGenUtils.generateRandomInvertedBinaryTreeDAG
import fuzzer.utils.io.ReadWriteUtils.{prettyPrintStats, writeLiveStats}
import fuzzer.utils.random.Random
import org.yaml.snakeyaml.Yaml
import play.api.libs.json.{JsObject, JsValue}

import scala.collection.mutable
import java.io.{File, FileWriter}
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
    zipped.foreach {
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
        processSingleDAG(dag, dagName, stats, shouldStop, startTime)
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

  private def processSingleDAG(dag: Graph[DFOperator], dagName: String, stats: CampaignStats, shouldStop: => Boolean, startTime: Long): Unit = {
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
            val selectedTables = Random.shuffle(dataAdapter.getTables).take(dag.getSourceNodes.length).toList
            val sourceCode = generateSingleProgram(dag, spec, codeGenerator.getDag2CodeFunc, selectedTables)
            val results = codeExecutor.execute(sourceCode)

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