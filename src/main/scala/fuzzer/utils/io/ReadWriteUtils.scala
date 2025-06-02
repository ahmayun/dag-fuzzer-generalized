package fuzzer.utils.io

import fuzzer.core.engine.{CampaignStats, FuzzerResults}
import fuzzer.core.global.FuzzerConfig
import java.io.{File, FileWriter}
import scala.sys.process._

object ReadWriteUtils {

  def deleteDir(path: String): Unit = {
    val dir = new File(path)
    if (dir.exists()) {
      val cmd = s"rm -rf ${path}"
      val exitCode = cmd.!
      if (exitCode != 0) {
        println(s"Failed to delete $path")
      }
    }
  }


  def createDir(path: String): Unit = {
    val dir = new File(path)
    if (!dir.exists()) {
      dir.mkdirs()
    }
  }

  def prettyPrintStats(stats: CampaignStats): String = {
    val statsMap = stats.getMap
    val generated = stats.getGenerated
    val attempts = stats.getAttempts
    // This is the number of inputs that actually reached the optimizer
    val successful = statsMap.getOrElse("Success", "0").toInt + statsMap.getOrElse("MismatchException", "0").toInt

    val builder = new StringBuilder
    builder.append("=== STATS ===\n")
    builder.append("----- Details -----\n")
    statsMap.foreach { case (k, v) => builder.append(s"$k = $v\n") }
    builder.append("------ Summary -----\n")
    builder.append(f"Exiting after DFGs generated == $generated\n")
    val tpDag2Dfg = (generated.toFloat / attempts.toFloat) * 100
    val tpDag2Valid = (successful.toFloat / attempts.toFloat) * 100
    val tpDfg2Valid = (successful.toFloat / generated.toFloat) * 100
    builder.append(f"Throughput DAG -> DFG: $generated/$attempts ($tpDag2Dfg%.2f%%)\n")
    builder.append(f"Throughput DAG -> Valid: $successful/$attempts ($tpDag2Valid%.2f%%)\n")
    builder.append(f"Throughput DFG -> Valid: $successful/$generated ($tpDfg2Valid%.2f%%)\n")
    builder.append("=============\n")

    builder.toString()
  }


  def saveResultsToFile(config: FuzzerConfig, results: FuzzerResults, elapsedSeconds: Long): Unit = {
    val summaryFile = new File(s"${config.outDir}/summary.txt")
    val writer = new FileWriter(summaryFile)
    try {
      writer.write(s"Fuzzing campaign for ${config.targetAPI}\n")
      writer.write(s"Seed: ${config.seed}\n")
      writer.write(s"Elapsed time: $elapsedSeconds seconds\n")
      writer.write(prettyPrintStats(results.stats))
    } finally {
      writer.close()
    }
  }
}
