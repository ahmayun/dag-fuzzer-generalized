package fuzzer.core.graph

import scala.collection.mutable

case class Node[T](id: String, value: T) {

  var graph: Graph[T] = null // initialized by user
  var reachableFromSources: mutable.Set[Node[T]] = mutable.Set()
  var reachablePathCount: mutable.Map[Node[T], Int] = mutable.Map()

  def children: List[Node[T]] = graph.children(id).map(i => graph.nodesMap(i))
  def parents: List[Node[T]] = graph.parents(id).map(i => graph.nodesMap(i))
  def traverseDescendants: List[T] = value :: children.flatMap(_.traverseDescendants)
  def traverseAncestors: List[T] = value :: parents.flatMap(_.traverseAncestors)
  def hasAncestors: Boolean = parents.nonEmpty
  def isUnary: Boolean = parents.length == 1
  def isBinary: Boolean = parents.length == 2
  def isSink: Boolean = getOutDegree == 0
  def getInDegree: Int = parents.length
  def getOutDegree: Int = children.length
  def getReachableSources: mutable.Set[Node[T]] = {
    reachableFromSources
  }
  def getReachableFromOnce: Seq[Node[T]] = {
    reachablePathCount.collect {
      case (source, count) if count == 1 => source
    }.toSeq
  }

  override def toString: String = value.toString
}