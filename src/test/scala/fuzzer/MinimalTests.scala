package fuzzer

import fuzzer.MainFuzzer.{createEngineFromConfig, createFuzzerEngine}
import fuzzer.adapters.spark.{SparkCodeExecutor, SparkCodeGenerator, SparkDataAdapter}
import fuzzer.code.SourceCode
import fuzzer.core.engine.FuzzerEngine
import fuzzer.core.global.FuzzerConfig
import fuzzer.core.graph.{DFOperator, Graph}
import fuzzer.data.tables.{ColumnMetadata, TableMetadata}
import fuzzer.data.types.IntegerType
import fuzzer.factory.AdapterFactory
import fuzzer.framework.{UserImplFlinkPython, UserImplSparkScala}
import fuzzer.utils.io.ReadWriteUtils.{createDir, deleteDir, prettyPrintStats, saveResultsToFile}
import fuzzer.utils.json.JsonReader
import fuzzer.utils.network.HttpUtils
import fuzzer.utils.random.Random
import org.junit.Test
import play.api.libs.json.{JsValue, Json}

import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.net.URI
import java.time.Duration
import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
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

    fuzzer.core.global.State.config = Some(config)
    Random.setSeed(config.seed)

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
  def testJsonServer(): Unit = {
    val config = FuzzerConfig.getFlinkPythonConfig
      .copy(seed = 1234)
    val spec = JsonReader.readJsonFile(config.specPath)
    val dag2FlinkPythonFunc = UserImplFlinkPython.dag2FlinkPython(spec) _
    val (dataAdapter, _, _) = AdapterFactory.createComponents(config, dag2FlinkPythonFunc)
    print(dataAdapter.getTables)
    assert(true)
  }

  @Test
  def testFlinkPythonServer(): Unit = {
    val sourceCode = generateFlinkPythonSourceCode()
    println(sourceCode)

    // Server configuration
    val serverHost = "localhost"
    val serverPort = 8888

    // Send source code to Python server as JSON HTTP request
      val sourceLines = sourceCode.toString.split("\n")
      val (first5Lines, rest) = sourceLines.splitAt(6)
      val addedCode = List(
        // """print(f"COLS: {auto0.get_schema().get_field_names()}")""",
        // """print(f"COLS: {auto6.get_schema().get_field_names()}")""",
        // """
        // |print("COLUMNS:", auto11.get_schema().get_field_names())
        // |
        // |result = auto11.execute()
        // |for i, row in enumerate(result.collect()):
        // |    if i >= 10:  # limit to first 10 rows
        // |        break
        // |    print(row)""".stripMargin
      )
      val codeString = (first5Lines ++ addedCode ++ rest).mkString("\n")

      val loadRequest = Json.obj(
        "message_type" -> "load_data"
      )

      HttpUtils.postJson(loadRequest, "localhost", 8888, timeoutSeconds = 120)

      // Create JSON request
      val jsonRequest = Json.obj(
        "message_type" -> "execute_code",
        "code" -> codeString
      )

      val jsonString = jsonRequest.toString()

      // Create HTTP client and request
      val client = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .build()

      val request = HttpRequest.newBuilder()
        .uri(URI.create(s"http://$serverHost:$serverPort"))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(jsonString))
        .build()

      println(s"Sending execute_code JSON request to http://$serverHost:$serverPort")
      println(s"Request body: $jsonString")

      // Make HTTP request
      val response = client.send(request, HttpResponse.BodyHandlers.ofString())

      println(s"HTTP Response Status: ${response.statusCode()}")
      val responseBody = response.body()

      if (responseBody.nonEmpty) {
        try {
          val responseJson = Json.parse(responseBody)
          val success = (responseJson \ "success").asOpt[Boolean].getOrElse(false)
          val message = (responseJson \ "message").asOpt[String]
          val error = (responseJson \ "error").asOpt[String]
          val programNumber = (responseJson \ "program_number").asOpt[Int]
          val returnCode = (responseJson \ "return_code").asOpt[Int]
          val stdout = (responseJson \ "stdout").asOpt[String]
          val stderr = (responseJson \ "stderr").asOpt[String]

          println(s"Server response - Success: $success")
          if (programNumber.isDefined) println(s"Program Number: ${programNumber.get}")
          if (returnCode.isDefined) println(s"Return Code: ${returnCode.get}")
          if (message.isDefined) println(s"Message: ${message.get}")
          if (error.isDefined) println(s"Error: ${error.get}")
          if (stdout.isDefined && stdout.get.nonEmpty) println(s"STDOUT:\n${stdout.get}")
          if (stderr.isDefined && stderr.get.nonEmpty) println(s"STDERR:\n${stderr.get}")

        } catch {
          case _: Exception =>
            println(s"Raw server response: $responseBody")
        }
      } else {
        println("Empty response from server")
      }

      assert(response.statusCode() == 200)


//    result match {
//      case Success(true) => assert(true)
//      case Success(false) =>
//        println("Server returned non-200 status code")
//        assert(false, "HTTP request failed")
//      case Failure(exception) =>
//        println(s"Failed to send execute_code request: ${exception.getMessage}")
//        assert(false, s"Connection failed: ${exception.getMessage}")
//    }
  }

  @Test
  def testFlinkPythonFuzzing(): Unit = {
    val config = FuzzerConfig.getFlinkPythonConfig
      .copy(seed = 1234)
    val spec = JsonReader.readJsonFile(config.specPath)

    val dag2FlinkPythonFunc = UserImplFlinkPython.dag2FlinkPython(spec) _
    val engine = createEngineFromConfig(config, spec, dag2FlinkPythonFunc)

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
