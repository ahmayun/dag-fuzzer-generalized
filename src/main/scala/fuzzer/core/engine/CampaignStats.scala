package fuzzer.core.engine

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class CampaignStats {
  val stats: mutable.Map[String, String] = mutable.Map[String, String](
    "attempts" -> "0",
    "generated" -> "0",
    "dag-batch" -> "0",
    "elapsed-seconds" -> "0",
    "covered-rules" -> "",
    "cumulative-coverage" -> "0",
    "seed" -> ""
  )

  def getMap: mutable.Map[String, String] = stats
  def getAttempts: Int = stats("attempts").toInt
  def getGenerated: Int = stats("generated").toInt
  def getDagBatch: Int = stats("dag-batch").toInt
  def getElapsedSeconds: Long = stats("elapsed-seconds").toLong
  def getCoveredRules: Array[String] = stats("covered-rules").split(',')

  def setAttempts(v: Int): Unit = {
    stats("attempts") = v.toString
  }
  def setGenerated(v: Int): Unit = {
    stats("generated") = v.toString
  }
  def setDagBatch(v: Int): Unit = {
    stats("dag-batch") = v.toString
  }

  def setSeed(v: Int): Unit = {
    stats("seed") = v.toString
  }

  def setElapsedSeconds(v: Long): Unit = {
    stats("elapsed-seconds") = v.toString
  }

  def setCoveredRules(v: Set[String]): Unit = {
    stats("covered-rules") = v.mkString(",")
  }

  def updateWith(key: String)(remappingFunc: Option[String] => Option[String]): Option[String] = {
    stats.updateWith(key)(remappingFunc)
  }

  def setCumulativeCoverageIfChanged(coverage: ListBuffer[String], iter: Long, elapsedSeconds: Long): Unit = {
    val cumuCoverage = this.getCoveredRules.toSet
    val cov = cumuCoverage.union(coverage.toSet)
    val size = cov.size
    val existing = stats.get("cumulative-coverage")
    if (existing.isEmpty || existing.get != size.toString) {
      stats("cumulative-coverage") = size.toString
      stats("last-coverage-update-iter") = iter.toString
      stats("last-coverage-update-elapsed") = elapsedSeconds.toString
      setCoveredRules(cov)
    }
  }

  def setIteration(iteration: Long): Unit = {
    stats("iter") = iteration.toString
  }
}
