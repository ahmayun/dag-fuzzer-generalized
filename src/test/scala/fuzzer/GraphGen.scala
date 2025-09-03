package fuzzer

import fuzzer.core.graph.DFOperator
import fuzzer.utils.generation.dag.DAGGenUtils.generateRandomInvertedBinaryTreeDAG
import org.junit.Test

class GraphGen {

  @Test
  def testGraphGen(): Unit = {
    val graph = generateRandomInvertedBinaryTreeDAG(
      valueGenerator = DFOperator.fromInt,
      maxDepth = 6,
      splittingProbability = 1.0,
      twoChildProbability = 0.3
    )

    println(graph.toDebugString)
  }
}
