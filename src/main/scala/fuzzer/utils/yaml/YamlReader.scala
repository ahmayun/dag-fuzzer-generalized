package fuzzer.utils.yaml

import org.yaml.snakeyaml.Yaml
import java.io.FileInputStream
import scala.jdk.CollectionConverters._

object YamlReader {
  def readYamlFile(path: String): Map[String, Any] = {
    val yaml = new Yaml()
    val input = new FileInputStream(path)
    val data = yaml.load[java.util.Map[String, Object]](input)
    input.close()
    data.asScala.toMap
  }
}
