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

}