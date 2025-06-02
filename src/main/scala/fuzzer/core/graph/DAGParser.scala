package fuzzer.core.graph

import fuzzer.utils.yaml.YamlReader

import scala.jdk.CollectionConverters._

object DAGParser {
  def parseYamlFile[T](filePath: String, nodeBuilder: Map[String, Any] => T): Graph[T] = {
    val data = YamlReader.readYamlFile(filePath)

    val nodesRaw = data("nodes").asInstanceOf[java.util.List[java.util.Map[String, Any]]].asScala
    val linksRaw = data("links").asInstanceOf[java.util.List[java.util.Map[String, Any]]].asScala

    // Step 1: Create all Node[T] instances with no edges

    def readAsString(value: Any): String = {
      try {
        value.asInstanceOf[String]
      } catch {
        case _: ClassCastException => value.asInstanceOf[Int].toString
        case e: Throwable => throw e
      }
    }

    val nodeMap: Map[String, Node[T]] = nodesRaw.map(_.asScala.toMap).map { m =>
      val id = readAsString(m("id"))
      val node = Node(id, nodeBuilder(m))
      id -> node
    }.toMap

    // Step 2: Build children and parents maps
    val emptyMap: Map[String, List[String]] = nodeMap.keys.map(_ -> List.empty[String]).toMap

    val (childrenMap, parentsMap) = linksRaw.map(_.asScala.toMap).foldLeft((emptyMap, emptyMap)) {
      case ((childrenAcc, parentsAcc), link) =>
        val src = readAsString(link("source"))
        val tgt = readAsString(link("target"))
        val updatedChildren = childrenAcc.updated(src, tgt :: childrenAcc(src))
        val updatedParents = parentsAcc.updated(tgt, src :: parentsAcc(tgt))
        (updatedChildren, updatedParents)
    }

    val graph = Graph(nodeMap, childrenMap, parentsMap)
    graph.nodes.foreach { n =>
      n.graph = graph
    }
    graph
  }
//  def parseYamlFile[T](filePath: String, nodeBuilder: Map[String, Any] => T): Graph[T] = {
//    val data = YamlReader.readYamlFile(filePath)
//
//    val nodesRaw = data("nodes").asInstanceOf[java.util.List[java.util.Map[String, Any]]].asScala
//    val linksRaw = data("links").asInstanceOf[java.util.List[java.util.Map[String, Any]]].asScala
//
//    // Step 1: Create all Node[T] instances with no edges
//    val nodeMap: Map[Any, Node[T]] = nodesRaw.map(_.asScala.toMap).map { m =>
//      val id = m("id")
//      val node = Node(nodeBuilder(m))
//      id -> node
//    }.toMap
//
//    // Step 2: Add child and parent references
//    linksRaw.map(_.asScala.toMap).foreach { link =>
//      val src = link("source")
//      val tgt = link("target")
//      val parent = nodeMap(src)
//      val child = nodeMap(tgt)
//      parent.children ::= child
//      child.parents ::= parent
//    }
//
//    Graph(nodeMap.values.toList)
//  }
}
