package fuzzer.utils.generation.udf

import scala.sys.process._
import scala.util.{Try, Success, Failure}

class LLMUtils {


  def callLLMViaPython(prompt: String): Try[String] = {
    Try {
      val escapedPrompt = prompt.replace("\"", "\\\"")
      val command = Seq("python3", "udf-gen.py", escapedPrompt)
      val output = command.!!

      output.trim
    }
  }

}
