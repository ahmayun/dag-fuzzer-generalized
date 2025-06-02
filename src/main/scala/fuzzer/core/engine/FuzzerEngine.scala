package fuzzer.core.engine

import fuzzer.core.engine.CampaignStats
import fuzzer.core.exceptions.ImpossibleDFGException
import fuzzer.core.global.FuzzerConfig
import fuzzer.core.interfaces.{CodeExecutor, CodeGenerator, DataAdapter}
import fuzzer.core.graph.{DAGParser, DFOperator, Graph}
import fuzzer.data.tables.Examples.tpcdsTables
import fuzzer.data.tables.TableMetadata
import fuzzer.utils.random.Random
import org.apache.spark.sql.catalyst.rules.Rule.coverage
import org.yaml.snakeyaml.Yaml
import play.api.libs.json.JsValue

import scala.sys.process._
import java.io.{File, FileWriter}
import scala.jdk.CollectionConverters._
import scala.util.control.Breaks.{break, breakable}

class FuzzerEngine(
                    config: FuzzerConfig,
                    spec: JsValue,
                    dataAdapter: DataAdapter,
                    codeGenerator: CodeGenerator,
                    codeExecutor: CodeExecutor
                  ) {

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
        val dagYamlFiles = generateYamlFiles

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

  def generateDAGFolder: File = {
    val generateCmd = s"./dag-gen/venv/bin/python dag-gen/run_generator.py -c ${genTempConfigWithNewSeed("dag-gen/sample_config/dfg-config.yaml")}" // dag-gen/sample_config/dfg-config.yaml
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

  def generateYamlFiles: Array[File] = {

    val dagFolder = generateDAGFolder

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