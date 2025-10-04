package fuzzer.adapters.dask

import fuzzer.code.SourceCode
import fuzzer.core.exceptions
import fuzzer.core.exceptions.{DAGFuzzerException, MismatchException, TableException, ValidationException}
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


class DaskCodeGenerator(config: FuzzerConfig, spec: JsValue, dag2CodeFunc: Graph[DFOperator] => SourceCode) extends CodeGenerator {

  override def getDag2CodeFunc: Graph[DFOperator] => SourceCode = dag2CodeFunc
}

class DaskDataAdapter(config: FuzzerConfig) extends DataAdapter {

  // Create HTTP client and request
  val client: HttpClient = HttpClient.newBuilder()
    .connectTimeout(Duration.ofSeconds(120))
    .build()

  override def getTables: Seq[TableMetadata] = {
    try {
      // Create request JSON
      val requestJson = Json.obj("message_type" -> "get_tables")

      val response = HttpUtils.postJson(client, requestJson, host="localhost", port=8889)
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
      case "boolean" | "bool" => BooleanType
      case "date" | "datetime" => DateType
      case "decimal" => DecimalType
      case "float" | "float64" => FloatType
      case "integer" | "int" | "int64" => IntegerType
      case "long" => LongType
      case "string" | "object" => StringType
      case _ => StringType
    }
  }

  private def parseDefaultValue(valueStr: String, dataTypeStr: String): Any = {
    Try {
      dataTypeStr.toLowerCase match {
        case "boolean" | "bool" => valueStr.toBoolean
        case "decimal" => BigDecimal(valueStr)
        case "float" | "float64" => valueStr.toDouble
        case "integer" | "int" | "int64" => valueStr.toLong
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

    val response = HttpUtils.postJson(client, request, "localhost", 8889, timeoutSeconds = 120)

    val body = Json.parse(response.body())
    val success = (body \ "success").asOpt[Boolean]
    success match {
      case Some(true) =>
      case _ => throw new Exception("Dask server failed to load data!")
    }
  }

  override def prepTableMetadata(sources: List[(Node[DFOperator], TableMetadata)]): List[(Node[DFOperator], TableMetadata)] = {
    sources.map { case (node, table) =>
      val columns = table.columns.map(c => c.copy(name = s"${c.name}_${node.id}"))
      (node, TableMetadata(
        table.identifier,
        columns,
        table.metadata
      ))
    }
  }

}

class DaskCodeExecutor(config: FuzzerConfig, spec: JsValue) extends CodeExecutor {

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

  private def jsonToMap(json: JsValue): Map[String, Any] = {
    json.as[Map[String, JsValue]].map { case (key, value) =>
      key -> jsValueToScala(value)
    }
  }

  private def jsValueToScala(jsValue: JsValue): Any = {
    jsValue match {
      case JsNull => null
      case JsBoolean(b) => b
      case JsNumber(n) => if (n.isValidInt) n.toInt else n.toDouble
      case JsString(s) => s
      case arr: JsArray => arr.value.map(jsValueToScala).toList
      case obj: JsObject => obj.value.map { case (k, v) => k -> jsValueToScala(v) }.toMap
    }
  }

  private def truncate(str: String, maxLength: Int): String = {
    if (str == null) return ""
    if (maxLength <= 0) return ""
    if (str.length <= maxLength) return str
    if (maxLength <= 3) return "." * maxLength

    str.take(maxLength - 3) + "..."
  }

  private def prettyPrintMap(map: Map[String, Any], indent: Int = 0): String = {
    val indentStr = "  " * indent
    val entries = map.map { case (key, value) =>
      val valueStr = value match {
        case m: Map[String, Any] @unchecked =>
          s"\n${prettyPrintMap(m, indent + 1)}"
        case list: List[Any] @unchecked =>
          prettyPrintList(list, indent + 1)
        case arr: Vector[Any] @unchecked =>
          prettyPrintList(arr.toList, indent + 1)
        case null => "null"
        case str: String => s""""$str""""
        case other => other.toString
      }
      s"$indentStr$key -> ${truncate(valueStr, 30)}"
    }

    if (indent == 0) {
      "Map(\n" + entries.mkString(",\n") + "\n)"
    } else {
      "Map(\n" + entries.mkString(",\n") + s"\n${"  " * (indent - 1)})"
    }
  }

  private def prettyPrintList(list: List[Any], indent: Int): String = {
    val indentStr = "  " * indent
    val items = list.map {
      case m: Map[String, Any] @unchecked =>
        s"\n$indentStr${prettyPrintMap(m, indent + 1)}"
      case null => "null"
      case str: String => s""""$str""""
      case other => other.toString
    }

    if (items.forall(!_.startsWith("\n"))) {
      s"List(${items.mkString(", ")})"
    } else {
      s"List(\n${items.mkString(",\n")}\n${"  " * (indent - 1)})"
    }
  }

  override def execute(sourceCode: SourceCode): ExecutionResult = {

    // Server configuration
    val serverHost = "localhost"
    val serverPort = 8889

    // Send source code to Python server as JSON HTTP request
    val codeString = sourceCode.toString

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

    mapToExecutionResult(responseMap)
  }

  private def createException(errorName: String, errorMessage: String): Exception = {

    errorName.trim() match {
      case "ValidationException" => new ValidationException(errorMessage)
      case "RuntimeException" => new RuntimeException(errorMessage)
      case "TableException" => new TableException(errorMessage)
      case "MismatchException" => new MismatchException(errorMessage)
      case "Success" | "" => new exceptions.Success("Generated query hit the optimizer")
//      case "ValueError" => new ValidationException(errorMessage)
//      case "KeyError" => new TableException(errorMessage)
//      case "TypeError" => new ValidationException(errorMessage)
//      case "AttributeError" => new RuntimeException(errorMessage)
//      case "Success" | "" => new exceptions.Success("Generated query executed successfully")
      case _ => new Exception(errorMessage)
    }
  }

  private def mapToExecutionResult(responseMap: Map[String, Any]): ExecutionResult = {
    val same_output = responseMap("success").asInstanceOf[Boolean]
    val errorName = responseMap("error_name").asInstanceOf[String]
    val errorMessage = responseMap("error_message").asInstanceOf[String]
    val finalProgram = responseMap.getOrElse("final_program", "").asInstanceOf[String]
    val sourceWithResults = s"$finalProgram"


    ExecutionResult(
      success = same_output,
      exception = createException(errorName, errorMessage),
      combinedSourceWithResults = sourceWithResults)
  }

  override def setupEnvironment(): () => Unit = {
    val processBuilder = Process(
      "pyflink-oracle-server/venv/bin/python dask-oracle-server/basic-json-server.py",
      None
    ) #> new File(".dask-server.log")

    val process = processBuilder.run()
    Thread.sleep(500)

    () => {
      process.destroy()
    }
  }

  override def tearDownEnvironment(terminateF: () => Unit): Unit = {
    terminateF()
  }

}