package fuzzer.adapters.spark

import fuzzer.code.SourceCode
import fuzzer.core.exceptions.{MismatchException, Success}
import fuzzer.core.global.FuzzerConfig
import fuzzer.core.graph.{DFOperator, Graph, Node}
import fuzzer.core.interfaces.{CodeExecutor, CodeGenerator, DataAdapter, ExecutionResult}
import fuzzer.data.tables.TableMetadata
import fuzzer.templates.Harness
import fuzzer.utils.spark.tpcds.TPCDSTablesLoader
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.catalyst.rules.Rule.coverage
import org.apache.spark.sql.functions.{lit, row_number}
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.JsValue
import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.tools.reflect.ToolBox
import java.lang.reflect.InvocationTargetException
import scala.reflect.runtime.{currentMirror, universe}
import scala.util.matching.Regex

class SparkCodeGenerator(config: FuzzerConfig, spec: JsValue, dag2CodeFunc: Graph[DFOperator] => SourceCode) extends CodeGenerator {

  override def getDag2CodeFunc: Graph[DFOperator] => SourceCode = dag2CodeFunc
}

class SparkDataAdapter(config: FuzzerConfig) extends DataAdapter {

  override def getTables: Seq[TableMetadata] = fuzzer.data.tables.Examples.tpcdsTables

  override def getTableByName(name: String): Option[TableMetadata] = ???

  override def loadData(executor: CodeExecutor, filterF: String => Boolean = _ => true): Unit = {
    TPCDSTablesLoader.loadAll(executor.asInstanceOf[SparkCodeExecutor].session.get, config.localTpcdsPath, dbName = "tpcds", filterF)
    println("Loaded tpcds datasets successfully!")
  }

  override def prepTableMetadata(sources: List[(Node[DFOperator], TableMetadata)]): List[(Node[DFOperator], TableMetadata)] = {
    sources.map { case (node, table) =>
      val columns = table.columns.map(c => c.copy(name = s"${c.name}_${node.id}"))
      (node, TableMetadata(
        s"${table.identifier}_${node.id}",
        columns,
        table.metadata
      ))
    }
  }

}

class SparkCodeExecutor(config: FuzzerConfig, spec: JsValue) extends CodeExecutor {
  private def createFullSourcesFromHarness(source: SourceCode): (String, String) = {
    val fullSourceOpt = Harness.embedCode(
      Harness.sparkProgramOptimizationsOn,
      source,
      Harness.insertionMark,
      "    " // indent 4 spaces
    )
    val fullSourceUnOpt = Harness.embedCode(
      Harness.sparkProgramOptimizationsOff,
      source,
      Harness.insertionMark,
      "    " // indent 4 spaces
    )

    (fullSourceOpt, fullSourceUnOpt)
  }

  private def runRaw(source: String): (Throwable, String, String) = {
    val toolbox = currentMirror.mkToolBox()

    val throwable = try {
      val evalFuture = Future {
        toolbox.eval(toolbox.parse(source))
      }

      Await.result(evalFuture, 1.seconds)
      new Success("Success")
    } catch {
      case e: TimeoutException =>
        new TimeoutException("Execution timed out after 10 seconds")
      case e: InvocationTargetException =>
        e.getCause
      case e: Exception =>
        new RuntimeException("Dynamic program invocation failed in OracleSystem.runWithSuppressOutput()", e)
    }

    (throwable, "", "")
  }

  override def executeRaw(source: String): Any = {
    val toolbox = currentMirror.mkToolBox()
    toolbox.eval(toolbox.parse(source))
  }

  private def oracleUDFDuplication(optDF: DataFrame, unOptDF: DataFrame): Throwable = {
    def countUDFs(plan: LogicalPlan): Int = {
      val planStr = plan.toString()
      val udfRegex: Regex = """UDF""".r  // Adjust this pattern to match your actual UDF name
      udfRegex.findAllIn(planStr).length
    }

    val optPlan = optDF.queryExecution.optimizedPlan
    val unOptPlan = unOptDF.queryExecution.optimizedPlan
    val optCount = countUDFs(optPlan)
    val unOptCount = countUDFs(unOptPlan)

    if (optCount != unOptCount)
      new MismatchException(
        s"""
           |UDF counts don't match Opt: $optCount, UnOpt: $unOptCount.
           |=== UnOptimized Plan ===
           |$unOptPlan
           |
           |=== Optimized Plan ===
           |$optPlan
           |""".stripMargin)
    else
      new Success(s"Success: Opt: $optCount - $unOptCount : UnOpt.")
  }

  def oracleDFComparison(optDF: DataFrame, unOptDF: DataFrame): Throwable = {
    // Add a deterministic row id so order is considered in equality.
    // We use row_number over a constant sort key to create a sequence per DF.
    val w = Window.orderBy(lit(1))
    val lhs = optDF.withColumn("__row_id__", row_number().over(w))
    val rhs = unOptDF.withColumn("__row_id__", row_number().over(w))

    // Compare schemas first (including nullability & types)
    if (lhs.schema != rhs.schema) {
      return new MismatchException(
        s"""Schemas don't match
           |optDF schema: ${lhs.schema.treeString}
           |unOptDF schema: ${rhs.schema.treeString}
           |""".stripMargin)
    }

    // Set-minus both ways with counts (use exceptAll to respect duplicates)
    val diffL = lhs.exceptAll(rhs)
    val diffR = rhs.exceptAll(lhs)

    val leftCount  = diffL.limit(1).count()  // cheap emptiness check
    val rightCount = diffR.limit(1).count()

    if (leftCount == 0 && rightCount == 0) {
      new Success("DFs match")
    } else {
      // Collect a small, readable sample of diffs from both sides
      val sampleLeft  = diffL.drop("__row_id__").limit(20).toJSON.collect().mkString("\n")
      val sampleRight = diffR.drop("__row_id__").limit(20).toJSON.collect().mkString("\n")

      new MismatchException(
        s"""
           |Outputs don't match (order-sensitive)
           |
           |Rows in optDF but not in unOptDF (up to 20):
           |$sampleLeft
           |
           |Rows in unOptDF but not in optDF (up to 20):
           |$sampleRight
           |""".stripMargin)
    }
  }

  def compareRuns(optDF: DataFrame, unOptDF: DataFrame): Throwable = {
    val udfCompare = oracleUDFDuplication(optDF, unOptDF)   // assumed: Try[String]
    val dfCompare  = oracleDFComparison(optDF, unOptDF)     // Try[String]

    dfCompare match {
      case _: Success =>
        udfCompare match {
          case ex: MismatchException => ex
          case _: Success => new Success("Both Plans and Final DFs match")
        }
      case ex: MismatchException => ex

    }
  }

  def checkOneGo(source: SourceCode): (Throwable, (Throwable, String), (Throwable, String)) = {
    val (fullSourceOpt, fullSourceUnOpt) = createFullSourcesFromHarness(source)

    val combined = fullSourceOpt + fullSourceUnOpt
    val (result, stdOutOpt, stdErrOpt) = runRaw(combined)

    val compare = result match {
      case _: Success =>
        (fuzzer.core.global.State.optRunException, fuzzer.core.global.State.unOptRunException) match {
          case (Some(_: Success), Some(_: Success)) =>
            compareRuns(fuzzer.core.global.State.optDF.get, fuzzer.core.global.State.unOptDF.get)
          case (Some(a), Some(b)) if a.getClass == b.getClass =>
            a
          case _ =>
            new MismatchException("Execution result mismatch between optimized and unoptimized versions.")
        }
      case e: Throwable =>
        e
    }

    val optEx = fuzzer.core.global.State.optRunException.get
    val unOptEx = fuzzer.core.global.State.unOptRunException.get

    fuzzer.core.global.State.optDF = None
    fuzzer.core.global.State.unOptDF = None
    fuzzer.core.global.State.optRunException = None
    fuzzer.core.global.State.unOptRunException = None
    fuzzer.core.global.State.finalDF = None

    (compare, (optEx, fullSourceOpt), (unOptEx, fullSourceUnOpt))
  }

  private def constructFileContents(result: Throwable, fullSourceOpt: String): String = {

    val stackTrace = decideStackTrace(result)

    val fullResult = s"$result\n$stackTrace"
    Harness.embedCode(fullSourceOpt, fullResult, Harness.resultMark)
  }

  private def decideStackTrace(result: Throwable): String = {
    result match {
      case _ : fuzzer.core.exceptions.Success => ""
      case _ => s"${result.getStackTrace.mkString("\n")}"
    }
  }

  private def constructCombinedFileContents(result: Throwable, optResult: Throwable, unOptResult: Throwable, fullSourceOpt: String, fullSourceUnOpt: String): String = {
    val optFileContents = constructFileContents(optResult, fullSourceOpt)
    val unOptFileContents = constructFileContents(unOptResult, fullSourceUnOpt)
    s"""
       |$optFileContents
       |
       |$unOptFileContents
       |
       |/* ========== ORACLE RESULT ===================
       |$result
       |${decideStackTrace(result)}
       |""".stripMargin
  }

  var session: Option[SparkSession] = None
  override def execute(code: SourceCode): ExecutionResult = {
    val (result, (optResult, fullSourceOpt), (unOptResult, fullSourceUnOpt)) = checkOneGo(code)
    val combinedSourceWithResults = constructCombinedFileContents(result, optResult, unOptResult, fullSourceOpt, fullSourceUnOpt)

    val success = if(result.isInstanceOf[Success]) true else false
    ExecutionResult(
      success = success,
      exception = result,
      combinedSourceWithResults = combinedSourceWithResults,
      coverage = coverage.clone(),
    )
  }

  override def setupEnvironment(): () => Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Fuzzer")
      .master(config.master)
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.ui.enabled", "false")
      .config("spark.task.maxFailures", "1")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")

    session = Some(sparkSession)
    fuzzer.core.global.State.sparkOption = session

    () => sparkSession.stop()
  }

  override def tearDownEnvironment(terminateF: () => Unit): Unit = {
    terminateF()
  }

}
