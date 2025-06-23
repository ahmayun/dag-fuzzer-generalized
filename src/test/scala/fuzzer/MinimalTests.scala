package fuzzer

import fuzzer.MainFuzzer.createFuzzerEngine
import fuzzer.adapters.spark.{SparkCodeExecutor, SparkCodeGenerator, SparkDataAdapter}
import fuzzer.code.SourceCode
import fuzzer.core.engine.FuzzerEngine
import fuzzer.core.global.FuzzerConfig
import fuzzer.core.graph.{DFOperator, Graph}
import fuzzer.data.tables.{ColumnMetadata, TableMetadata}
import fuzzer.data.types.IntegerType
import fuzzer.factory.AdapterFactory
import fuzzer.utils.io.ReadWriteUtils.{createDir, deleteDir, prettyPrintStats, saveResultsToFile}
import fuzzer.utils.json.JsonReader
import fuzzer.utils.random.Random
import org.junit.Test
import play.api.libs.json.JsValue

import java.io.{BufferedWriter, OutputStreamWriter}
import java.net.Socket
import scala.util.{Failure, Success, Try}

class MinimalTests {

  val hardcodedTables: List[TableMetadata] = List(
    TableMetadata(
      _identifier = "users",
      _columns = Seq(
        ColumnMetadata("id", IntegerType, isNullable = false, isKey = true),
      ),
      _metadata = Map("source" -> "auth_system")
    ),
    TableMetadata(
      _identifier = "orders",
      _columns = Seq(
        ColumnMetadata("id", IntegerType, isNullable = false, isKey = true),
      ),
      _metadata = Map("source" -> "ecommerce")
    ),
    TableMetadata(
      _identifier = "products",
      _columns = Seq(
        ColumnMetadata("id", IntegerType, isNullable = false, isKey = true),
      ),
      _metadata = Map("source" -> "inventory")
    )
  )

  val tpcdsTables: List[TableMetadata] = List (

  )

  def createEngineFromConfig(config: FuzzerConfig, spec: JsValue, dag2CodeFunc: Graph[DFOperator] => SourceCode): FuzzerEngine = {

    // Create API-specific components using factory
//    val dataAdapter = new SparkDataAdapter(config)
//    val codeGenerator = new SparkCodeGenerator(config, spec)
//    val codeExecutor = new SparkCodeExecutor(config, spec)
    val (dataAdapter, codeGenerator, codeExecutor) = AdapterFactory.createComponents(config, dag2CodeFunc)

    val engine = new FuzzerEngine(
      config = config,
      spec = spec,
      dataAdapter = dataAdapter,
      codeGenerator = codeGenerator,
      codeExecutor = codeExecutor
    )

    engine
  }

  @Test
  def testSparkScalaGen(): Unit = {
    val config = FuzzerConfig.getSparkScalaConfig
      .copy(seed = 1234)
    val spec = JsonReader.readJsonFile(config.specPath)
    val dag2SparkScalaFunc = UserImplSparkScala.dag2SparkScala(spec) _
    val engine = createEngineFromConfig(config, spec, dag2SparkScalaFunc)

    // This may generate an invalid DAG (that's normal
    // since I have no control over what the DAG module
    // will generate), change seed if the DAG is invalid.
    val dag = engine.generateSingleDAG()
    println(dag)

    val sourceCode = engine.generateSingleProgram(dag, spec, dag2SparkScalaFunc, hardcodedTables)
    println(sourceCode)

    assert(true)
  }

  def generateFlinkPythonSourceCode(): SourceCode = {
    val config = FuzzerConfig.getFlinkPythonConfig
      .copy(seed = 1234)
    val spec = JsonReader.readJsonFile(config.specPath)
    val dag2FlinkPythonFunc = UserImplFlinkPython.dag2FlinkPython(spec) _
    val engine = createEngineFromConfig(config, spec, dag2FlinkPythonFunc)

    // This may generate an invalid DAG (that's normal
    // since I have no control over what the DAG module
    // will generate), change seed if the DAG is invalid.
    val dag = engine.generateSingleDAG()
    println(dag)

    val sourceCode = engine.generateSingleProgram(dag, spec, dag2FlinkPythonFunc, fuzzer.data.tables.Examples.tpcdsTables)
    sourceCode
  }

  @Test
  def testFlinkPython(): Unit = {
    val sourceCode = generateFlinkPythonSourceCode();
    println(sourceCode)
    assert(true)
  }

  @Test
  def testFlinkPythonServer(): Unit = {
    val sourceCode = generateFlinkPythonSourceCode()
    println(sourceCode)

    // Server configuration
    val serverHost = "localhost"
    val serverPort = 8888

    // Send source code to Python server
    val result = Try {
      val socket = new Socket(serverHost, serverPort)
      val writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream))

      try {
//        val codeString = List(
//          """auto0 = table_env.from_path("call_center")""",
//          """result = auto0.group_by(lit(1)).select(col("cc_call_center_sk").count.alias("total"))""",
//          """for row in result.execute().collect():""",
//          """    print(f"TOTAL ROW COUNT: {row[0]}")"""
//        ).mkString("\n")

        val sourceLines = sourceCode.toString.split("\n")
        val (first5Lines, rest) = sourceLines.splitAt(6)
        val addedCode = List(
//          """print(f"COLS: {auto0.get_schema().get_field_names()}")""",
//          """print(f"COLS: {auto6.get_schema().get_field_names()}")""",
//          """
//            |print("COLUMNS:", auto11.get_schema().get_field_names())
//            |
//            |result = auto11.execute()
//            |for i, row in enumerate(result.collect()):
//            |    if i >= 10:  # limit to first 10 rows
//            |        break
//            |    print(row)""".stripMargin
        )
        val codeString = (first5Lines ++ addedCode ++ rest).mkString("\n")

        // Send length first, then the code
        writer.write(s"${codeString.length}\n")
        writer.write(codeString)
        writer.newLine()
        writer.flush()

        println(s"Successfully sent source code (${codeString.length} chars) to server at $serverHost:$serverPort")
        true
      } finally {
        writer.close()
        socket.close()
      }
    }

    result match {
      case Success(_) => assert(true)
      case Failure(exception) =>
        println(s"Failed to send source code: ${exception.getMessage}")
        assert(false, s"Connection failed: ${exception.getMessage}")
    }
  }

  @Test
  def testSparkScalaFuzzing(): Unit = {

    val config = FuzzerConfig.getSparkScalaConfig
      .copy(seed = 1234)
    val spec = JsonReader.readJsonFile(config.specPath)

    val dag2SparkScalaFunc = UserImplSparkScala.dag2SparkScala(spec) _
    val engine = createEngineFromConfig(config, spec, dag2SparkScalaFunc)

    fuzzer.core.global.State.config = Some(config)
    Random.setSeed(config.seed)

    deleteDir(config.dagGenDir)
    deleteDir(config.outDir)
    createDir(config.outDir)

    println(s"Starting fuzzing campaign with seed: ${config.seed}")
    println(s"Target API: ${config.targetAPI}")

    val startTime = System.currentTimeMillis()

    val results = engine.run()

    val elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000

    // Print final results
    println(s"Fuzzing campaign completed in $elapsedSeconds seconds")
    println(prettyPrintStats(results.stats))

    // Save detailed results to file
    saveResultsToFile(config, results, elapsedSeconds)
  }
}
