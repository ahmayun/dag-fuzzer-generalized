package fuzzer.utils.json

import play.api.libs.json._

import scala.io.Source

object JsonReader {
  def readJsonFile(path: String): JsValue = {
    val source = Source.fromFile(path)
    val jsonStr = try source.mkString finally source.close()
    val json: JsValue = Json.parse(jsonStr)
    json
  }
}
