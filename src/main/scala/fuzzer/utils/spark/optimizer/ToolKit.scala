package fuzzer.utils.spark.optimizer

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.internal.SQLConf

import scala.collection.mutable

object ToolKit {

  def countTotalNodes(plan: LogicalPlan, nodes: mutable.Map[String, mutable.Map[Int, Int]]): Int = {
    var count = 1

    nodes.update(plan.nodeName, {
      val inner = nodes.getOrElseUpdate(plan.nodeName, mutable.Map(plan.hashCode() -> 0))
      inner.update(plan.hashCode(), inner.getOrElseUpdate(plan.hashCode(), 0) + 1)
      inner
    })
    //    println(s"${plan.nodeName} (${plan.hashCode()}): ${plan.expressions.map(_.toString()).mkString(",")}")
    count += plan.children.map(n => countTotalNodes(n, nodes)).sum
    val subqueryNodes = plan.collect {
      case p if p.subqueries.nonEmpty => p.subqueries.map(n => countTotalNodes(n, nodes)).sum
    }.sum
    count + subqueryNodes
  }

  private def withSQLConf[T](pairs: (String, String)*)(f: => T): T = {
    val conf = SQLConf.get
    val (keys, values) = pairs.unzip
    val currentValues = keys.map { key =>
      if (conf.contains(key)) {
        Some(conf.getConfString(key))
      } else {
        None
      }
    }
    keys.lazyZip(values).foreach { (k, v) =>
      //      assert(!SQLConf.staticConfKeys.contains(k))
      conf.setConfString(k, v)
    }
    try f finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => conf.setConfString(key, value)
        case (key, None) => conf.unsetConf(key)
      }
    }
  }

  def withOptimized[T](f: => T): T = {
    // Sets up all the configurations for the Catalyst optimizer
    val optConfigs = Seq(
      (SQLConf.CBO_ENABLED.key, "true"),
      (SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key, "true")
    )
    withSQLConf(optConfigs: _*) {
      f
    }
  }
  def withoutOptimized[T](excludedRules: String)(f: => T): T = {
    val nonOptConfigs = Seq((SQLConf.OPTIMIZER_EXCLUDED_RULES.key, excludedRules))
    withSQLConf(nonOptConfigs: _*) {
      f
    }
  }
}
