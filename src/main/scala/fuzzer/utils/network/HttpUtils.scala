package fuzzer.utils.network

import play.api.libs.json.JsObject

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration

object HttpUtils {

  def postJson(requestJson: JsObject, host: String, port: Int, timeoutSeconds: Int = 10): HttpResponse[String] = {
    // Create HTTP client and request
    val client = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(timeoutSeconds))
      .build()

    val request = HttpRequest.newBuilder()
      .uri(URI.create(s"http://$host:$port"))
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(requestJson.toString))
      .build()

    // Make request
    val response = client.send(request, HttpResponse.BodyHandlers.ofString())
    response
  }

}
