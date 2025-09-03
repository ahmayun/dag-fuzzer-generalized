package fuzzer.core.graph


import fuzzer.code.SourceCode

import scala.collection.mutable

case class Graph[T](
                     nodesMap: Map[String, Node[T]],
                     children: Map[String, List[String]],
                     parents: Map[String, List[String]]
                   ) {

  val nodes: List[Node[T]] = nodesMap.values.toList
  private lazy val topoSortedNodes: List[Node[T]] = this.kahnTopoSort

  def getSourceNodes: List[Node[T]] = {
    this.nodes.filter(_.parents.isEmpty)
  }

  def getSinkNodes: List[Node[T]] = {
    this.nodes.filter(_.children.isEmpty)
  }

  private def kahnTopoSort: List[Node[T]] = {
    val inDegree = mutable.Map[Node[T], Int]()
    val queue = mutable.Queue[Node[T]]()
    val sorted = mutable.ListBuffer[Node[T]]()

    // Initialize in-degrees
    for (node <- nodes) {
      inDegree(node) = node.parents.size
      if (node.parents.isEmpty) {
        queue.enqueue(node)
      }
    }

    val visited = mutable.Set[Node[T]]()

    while (queue.nonEmpty) {
      val current = queue.dequeue()
      if (!visited.contains(current)) {
        sorted += current
        visited += current

        for (child <- current.children) {
          inDegree(child) -= 1
          if (inDegree(child) == 0) {
            queue.enqueue(child)
          }
        }
      }
    }

    sorted.toList
  }

  def generateCode(generator: Graph[T] => SourceCode): SourceCode = {
    generator(this)
  }

  def computeReachabilityFromSources(): Unit = {
    val graph = this

    val sources = graph.getSourceNodes
    sources.foreach(source => source.reachableFromSources += source)

    for (node <- this.topoSortedNodes) {
      for (child <- node.children) {
        child.reachableFromSources ++= node.reachableFromSources
      }
    }
  }

  def computeReachableCounts(): Unit = {
    val graph = this
    val sources = graph.getSourceNodes

    // Initialize: each source has 1 path to itself
    for (source <- sources) {
      source.reachablePathCount(source) = 1
    }

    // Traverse in topological order and propagate counts
    for (node <- this.topoSortedNodes) {
      for (child <- node.children) {
        for ((source, count) <- node.reachablePathCount) {
          child.reachablePathCount(source) =
            child.reachablePathCount.getOrElse(source, 0) + count
        }
      }
    }
  }

  def traverseTopological(visit: Node[T] => Unit): Unit = this.topoSortedNodes.foreach(visit)
  def transformNodes(visit: Node[T] => T): Graph[T] = {
    val newNodesMap = nodesMap.map { case (id, node) =>
      id -> Node(id, visit(node))
    }
    val newGraph = Graph(newNodesMap, children, parents)
    newGraph.nodes.foreach { n =>
      n.graph = newGraph
    }

    newGraph
  }

  def toDebugString: String = {
    val sb = new StringBuilder()

    sb.append("=" * 60).append("\n")
    sb.append(s"GRAPH DEBUG - ${nodes.length} nodes total\n")
    sb.append("=" * 60).append("\n")

    // Print basic stats
    sb.append(s"Sources: ${getSourceNodes.length}\n")
    sb.append(s"Sinks: ${getSinkNodes.length}\n")
    sb.append("\n")

    // Print nodesMap
    sb.append("NODES MAP:\n")
    sb.append("-" * 30).append("\n")
    nodesMap.toSeq.sortBy(_._1).foreach { case (id, node) =>
      sb.append(s"  $id -> Node(id='${node.id}', value=${node.value})\n")
    }
    sb.append("\n")

    // Print children map
    sb.append("CHILDREN MAP:\n")
    sb.append("-" * 30).append("\n")
    children.toSeq.sortBy(_._1).foreach { case (nodeId, childIds) =>
      val childrenStr = if (childIds.isEmpty) "[]" else childIds.mkString("[", ", ", "]")
      sb.append(s"  $nodeId -> $childrenStr\n")
    }
    sb.append("\n")

    // Print parents map
    sb.append("PARENTS MAP:\n")
    sb.append("-" * 30).append("\n")
    parents.toSeq.sortBy(_._1).foreach { case (nodeId, parentIds) =>
      val parentsStr = if (parentIds.isEmpty) "[]" else parentIds.mkString("[", ", ", "]")
      sb.append(s"  $nodeId -> $parentsStr\n")
    }
    sb.append("\n")

    // Print node details with relationships
    sb.append("NODE DETAILS:\n")
    sb.append("-" * 30).append("\n")
    nodes.sortBy(_.id).foreach { node =>
      val parentIds = node.parents.map(_.id).mkString("[", ", ", "]")
      val childIds = node.children.map(_.id).mkString("[", ", ", "]")
      val nodeType = if (node.parents.isEmpty) "SOURCE"
      else if (node.children.isEmpty) "SINK"
      else "INTERNAL"

      sb.append(s"  ${node.id} (${node.value}) - $nodeType\n")
      sb.append(s"    Parents: $parentIds (in-degree: ${node.getInDegree})\n")
      sb.append(s"    Children: $childIds (out-degree: ${node.getOutDegree})\n")
    }
    sb.append("\n")

    // Print tree structure visualization
    sb.append("TREE STRUCTURE (from sinks up to sources):\n")
    sb.append("-" * 30).append("\n")

    def appendTreeFromNode(node: Node[T], indent: String, visited: mutable.Set[String]): Unit = {
      if (!visited.contains(node.id)) {
        visited.add(node.id)
        val nodeType = if (node.parents.isEmpty) "[SOURCE]"
        else if (node.children.isEmpty) "[SINK]"
        else ""
        sb.append(s"$indent${node.id} (${node.value}) $nodeType\n")

        // Print parents with increased indent
        node.parents.foreach { parent =>
          appendTreeFromNode(parent, indent + "  ↑ ", visited)
        }
      } else {
        // Already visited - just show reference
        sb.append(s"$indent${node.id} (${node.value}) [ALREADY SHOWN]\n")
      }
    }

    val visited = mutable.Set[String]()
    getSinkNodes.foreach { sink =>
      appendTreeFromNode(sink, "", visited)
      sb.append("\n")
    }

    sb.append("=" * 60)
    sb.toString()
  }

}