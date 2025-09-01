package fuzzer.adapters.flink

import fuzzer.code.SourceCode
import fuzzer.core.exceptions
import fuzzer.core.exceptions.{DAGFuzzerException, ValidationException}
import fuzzer.core.global.FuzzerConfig
import fuzzer.core.graph.{DFOperator, Graph, Node}
import fuzzer.core.interfaces.{CodeExecutor, CodeGenerator, DataAdapter, ExecutionResult}
import fuzzer.data.tables.{ColumnMetadata, TableMetadata}
import fuzzer.data.types._
import fuzzer.utils.network.HttpUtils
import play.api.libs.json._

import java.io.{BufferedWriter, File, IOException, OutputStreamWriter}
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.net.URI
import java.time.Duration
import java.net.{ConnectException, Socket}
import scala.sys.process._
import scala.util.{Failure, Success, Try}


class FlinkCodeGenerator(config: FuzzerConfig, spec: JsValue, dag2CodeFunc: Graph[DFOperator] => SourceCode) extends CodeGenerator {

  override def getDag2CodeFunc: Graph[DFOperator] => SourceCode = dag2CodeFunc
}

class FlinkDataAdapter(config: FuzzerConfig) extends DataAdapter {

  // Create HTTP client and request
  val client: HttpClient = HttpClient.newBuilder()
    .connectTimeout(Duration.ofSeconds(120))
    .build()

  override def getTables: Seq[TableMetadata] = {
    try {
      // Create request JSON
      val requestJson = Json.obj("message_type" -> "get_tables")

      val response = HttpUtils.postJson(client, requestJson, host="localhost", port=8888)
      val responseJson = Json.parse(response.body())

      // Parse response
      val tables = (responseJson \ "tables").as[JsArray]

      tables.value.map { tableJson =>
        val identifier = (tableJson \ "identifier").as[String]
        val metadata = (tableJson \ "metadata").asOpt[Map[String, String]].getOrElse(Map.empty)

        val columns = (tableJson \ "columns").as[JsArray].value.map { columnJson =>
          val name = (columnJson \ "name").as[String]
          val dataTypeStr = (columnJson \ "dataType").as[String]
          val isNullable = (columnJson \ "isNullable").asOpt[Boolean].getOrElse(true)
          val isKey = (columnJson \ "isKey").asOpt[Boolean].getOrElse(false)
          val defaultValue = (columnJson \ "defaultValue").asOpt[String].map(parseDefaultValue(_, dataTypeStr))
          val colMetadata = (columnJson \ "metadata").asOpt[Map[String, String]].getOrElse(Map.empty)

          ColumnMetadata(
            name = name,
            dataType = parseDataType(dataTypeStr),
            isNullable = isNullable,
            isKey = isKey,
            defaultValue = defaultValue,
            metadata = colMetadata
          )
        }

        TableMetadata(identifier, columns.toSeq, metadata)
      }.toSeq

    } catch {
      case _: Exception => Seq.empty[TableMetadata]
    }
  }

  private def parseDataType(dataTypeStr: String): DataType = {
    dataTypeStr.toLowerCase match {
      case "boolean" => BooleanType
      case "date" => DateType
      case "decimal" => DecimalType
      case "float" => FloatType
      case "integer" | "int" => IntegerType
      case "long" => LongType
      case _ => StringType
    }
  }

  private def parseDefaultValue(valueStr: String, dataTypeStr: String): Any = {
    Try {
      dataTypeStr.toLowerCase match {
        case "boolean" => valueStr.toBoolean
        case "decimal" => BigDecimal(valueStr)
        case "float" => valueStr.toFloat
        case "integer" | "int" => valueStr.toInt
        case "long" => valueStr.toLong
        case _ => valueStr
      }
    }.getOrElse(valueStr)
  }

  override def getTableByName(name: String): Option[TableMetadata] = ???

  override def loadData(executor: CodeExecutor): Unit = {
    // Create JSON request
    val request = Json.obj(
      "message_type" -> "load_data"
    )

    val response = HttpUtils.postJson(client, request, "localhost", 8888, timeoutSeconds = 120)

    val body = Json.parse(response.body())
    val success = (body \ "success").asOpt[Boolean]
    success match {
      case Some(true) =>
      case _ => throw new Exception("PyFlink server failed to load data!")
    }
  }

}

class FlinkCodeExecutor(config: FuzzerConfig, spec: JsValue) extends CodeExecutor {

  // Create HTTP client and request
  val client: HttpClient = HttpClient.newBuilder()
    .connectTimeout(Duration.ofSeconds(120))
    .build()

  private def parseResponse(responseBody: String): Option[JsValue] = {
    if (responseBody.nonEmpty) {
      Some(Json.parse(responseBody))
    } else {
      None
    }
  }

  private def jsonToMap(responseJson: JsValue): Map[String, Any] = {

    println("RAW RESPONSE")
    println(responseJson.toString())
    try {
      Map(
        "success" -> (responseJson \ "success").as[Boolean],
        "error_message" -> (responseJson \ "error_message").as[String],
        "error_name" -> (responseJson \ "error_name").as[String],
        "return_code" -> (responseJson \ "return_code").as[Int],
        "final_program" -> (responseJson \ "final_program").as[String],
        "stdout" -> (responseJson \ "stdout").as[String],
        "stderr" -> (responseJson \ "stderr").as[String],
      )
    } catch {
      case ex: Exception => throw new DAGFuzzerException(ex.getMessage, ex)
    }

  }

  private def printResponse(responseMap: Map[String, Any]): Unit = {
      println(s"Server response - Success: ${responseMap("success")}")
      println(s"Return Code: ${responseMap("return_code")}")
      println(s"STDOUT:\n${responseMap("stdout")}")
      println(s"STDERR:\n${responseMap("stderr")}")
  }

  override def execute(sourceCode: SourceCode): ExecutionResult = {

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

    // Create JSON request
    val jsonRequest = Json.obj(
      "message_type" -> "execute_code",
      "code" -> codeString
    )

    val response = HttpUtils.postJson(client, jsonRequest, serverHost, serverPort)

    println(s"HTTP Response Status: ${response.statusCode()}")
    val responseBody = response.body()

    val responseJsonOpt = parseResponse(responseBody)

    val responseJson = responseJsonOpt match {
      case None => throw new Exception("response could not be parsed")
      case Some(responseJson) => responseJson
    }

    val responseMap = jsonToMap(responseJson)
    printResponse(responseMap)

    mapToExecutionResult(responseMap)
  }

  private def createException(errorName: String, errorMessage: String): Exception = {

    errorName match {
      case "ValidationException" => new ValidationException(errorMessage)
      case _ => new Exception(errorMessage)
    }
  }

  private def mapToExecutionResult(responseMap: Map[String, Any]): ExecutionResult = {
    val success = responseMap("success").asInstanceOf[Boolean]
    val errorName = responseMap("error_name").asInstanceOf[String]
    val errorMessage = responseMap("error_message").asInstanceOf[String]
    val (stdout, stderr) = (responseMap("stdout").asInstanceOf[String], responseMap("stderr").asInstanceOf[String])

    val finalProgram = responseMap.getOrElse("final_program", "").asInstanceOf[String]
    val sourceWithResults = s"$finalProgram\n\nSTDOUT:\n$stdout\n\nSTDERR:\n$stderr"

    ExecutionResult(
      success = success,
      exception = if (success) new exceptions.Success("dummy") else createException(errorName, errorMessage),
      combinedSourceWithResults = sourceWithResults)
  }

  override def setupEnvironment(): () => Unit = {
    val processBuilder = Process("pyflink-oracle-server/venv/bin/python pyflink-oracle-server/basic-json-server.py") #> new File(".server.log")
    val process = processBuilder.run()

    () => {
      process.destroy()
    }
  }

  override def tearDownEnvironment(terminateF: () => Unit): Unit = {
    terminateF()
  }

}
