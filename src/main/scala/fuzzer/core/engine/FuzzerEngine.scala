package fuzzer.core.engine

import fuzzer.code.SourceCode
import fuzzer.core.exceptions.ImpossibleDFGException
import fuzzer.core.global.FuzzerConfig
import fuzzer.core.graph.{DAGParser, DFOperator, Graph, Node}
import fuzzer.core.interfaces.{CodeExecutor, CodeGenerator, DataAdapter}
import fuzzer.data.tables.Examples.tpcdsTables
import fuzzer.data.tables.TableMetadata
import fuzzer.utils.random.Random
import org.yaml.snakeyaml.Yaml
import play.api.libs.json.{JsObject, JsValue}

import java.io.{File, FileWriter}
import scala.jdk.CollectionConverters._
import scala.sys.process._
import scala.util.control.Breaks.{break, breakable}

class FuzzerEngine(
                    val config: FuzzerConfig,
                    val spec: JsValue,
                    val dataAdapter: DataAdapter,
                    val codeGenerator: CodeGenerator,
                    val codeExecutor: CodeExecutor
                  ) {


  def generateSingleDAG(): Graph[DFOperator] = {
    val dagYamlFile = generateYamlFiles(1).head
    DAGParser.parseYamlFile(dagYamlFile.getAbsolutePath, map => DFOperator.fromMap(map))
  }



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

  def constructDFG(dag: Graph[DFOperator], apiSpec: JsValue, tables: List[TableMetadata]): Graph[DFOperator] = {

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
                             tables: List[TableMetadata]
                           ): SourceCode = {
    val (isInvalid, message) = isInvalidDFG(dag)
    if (isInvalid) {
      throw new ImpossibleDFGException(s"Impossible to convert DAG to DFG. $message")
    }


    val dfg = constructDFG(dag, spec, tables)
    val generatedSource = dfg.generateCode(dag2SourceFunc)
    generatedSource
  }

  def run(): FuzzerResults = {
    val stats = new CampaignStats()
    stats.setSeed(config.seed)

    // Setup environment
    codeExecutor.setupEnvironment()
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
      while (!shouldStop) {
        val dagYamlFiles = generateYamlFiles(config.d)

        for (dagYamlFile <- dagYamlFiles if !shouldStop) {
          val dag = DAGParser.parseYamlFile(dagYamlFile.getAbsolutePath, map => DFOperator.fromMap(map))

          println(s"Processing ${dagYamlFile.getName}")
//          processSingleDAG(dag, stats, shouldStop)
        }
      }

      // Return final results
      FuzzerResults(stats)
    } finally {
      // Clean up
      codeExecutor.teardownEnvironment()
    }
  }

  def genTempConfigWithNewSeed(yamlFile: String): String = {
    val yaml = new Yaml()
    val newSeed = Random.nextInt(Int.MaxValue)
    val inputStream = new java.io.FileInputStream(new File(yamlFile))
    val data = yaml.load[java.util.Map[String, Object]](inputStream).asScala
    data.update("Seed", Integer.valueOf(newSeed))
    val outputPath = "/tmp/runtime-config.yaml"
    val writer = new FileWriter(outputPath)
    yaml.dump(data.asJava, writer)
    writer.close()
    outputPath
  }

  def updateConfigProperty(configFile: String, propName: String, propValue: Int): Unit = {
    val yaml = new Yaml()
    val inputStream = new java.io.FileInputStream(new File(configFile))
    val data = yaml.load[java.util.Map[String, Object]](inputStream).asScala
    data.update(propName, Integer.valueOf(propValue))
    val outputPath = "/tmp/runtime-config.yaml"
    val writer = new FileWriter(outputPath)
    yaml.dump(data.asJava, writer)
    writer.close()
  }

  def generateDAGFolder(n: Int): File = {
    val newConfig = genTempConfigWithNewSeed("dag-gen/sample_config/dfg-config.yaml")
    updateConfigProperty(newConfig, "Number of DAGs", n)
    val generateCmd = s"./dag-gen/venv/bin/python dag-gen/run_generator.py -c ${newConfig}" // dag-gen/sample_config/dfg-config.yaml
    val exitCode = generateCmd.!
    if (exitCode != 0) {
      println(s"Warning: DAG generation command failed with exit code $exitCode")
      sys.exit(-1)
    }

    val dagFolder = new File(config.dagGenDir)
    if (!dagFolder.exists() || !dagFolder.isDirectory) {
      println("Warning: 'DAGs' folder not found or not a directory. Exiting.")
      sys.exit(-1)
    }
    dagFolder
  }

  def generateYamlFiles(n: Int): Array[File] = {

    val dagFolder = generateDAGFolder(n)

    val yamlFiles = dagFolder
      .listFiles()
      .filter(f => f.isFile && f.getName.startsWith("dag") && f.getName.endsWith(".yaml"))
      .take(config.d)

    yamlFiles
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

  private def processSingleDAG(dag: Graph[DFOperator], stats: CampaignStats, shouldStop: => Boolean): Unit = {
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
            val selectedTables = Random.shuffle(tpcdsTables).take(dag.getSourceNodes.length).toList

            stats.setGenerated(stats.getGenerated+1)
          } catch {
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
      case ex: Exception =>
        stats.setAttempts(stats.getAttempts+1)
        println(s"Failed to parse DAG file: ???. Reason: ${ex.getMessage}")
    }
  }

}

case class FuzzerResults(stats: CampaignStats)