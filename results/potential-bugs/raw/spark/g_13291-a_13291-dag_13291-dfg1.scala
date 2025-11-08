


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import fuzzer.core.global.State.sparkOption
import fuzzer.utils.spark.optimizer.ToolKit.withoutOptimized
import fuzzer.templates.ComplexObject
import fuzzer.core.exceptions._
import org.apache.spark.sql.expressions.Window


object Optimized {

  def main(args: Array[String]): Unit = {
    val spark = sparkOption.get

    val preloadedUDF = udf((s: Any) => {
      val r = scala.util.Random.nextInt()
      ComplexObject(r,r)
    }).asNondeterministic()


    
    
    val filterUdfString = udf((arg: String) => {
      arg.startsWith("A") || arg.startsWith("a")
    }).asNondeterministic()
    
    
    
    val filterUdfInteger = udf((arg: Int) => {
      arg.toString == arg.toString.reverse
    }).asNondeterministic()
    
    
    
    val filterUdfDecimal = udf((arg: Double) => {
      (scala.math.round(arg * 100).toDouble / 100.0) == arg
    }).asNondeterministic()
    
    
    val autonode_9 = spark.table("tpcds.promotion").select(spark.table("tpcds.promotion").columns.map(colName => col(colName).alias(s"${colName}_node_9")): _*).as("promotion_node_9")
    val autonode_8 = spark.table("tpcds.time_dim").select(spark.table("tpcds.time_dim").columns.map(colName => col(colName).alias(s"${colName}_node_8")): _*).as("time_dim_node_8")
    val autonode_7 = autonode_9.distinct()
    val autonode_6 = autonode_8.limit(34)
    val autonode_5 = autonode_7.offset(13)
    val autonode_4 = autonode_6.filter(col("time_dim_node_8.t_meal_time_node_8").cast("boolean") === lit(-43))
    val autonode_3 = autonode_4.join(autonode_5, col("promotion_node_9.p_promo_id_node_9") === col("time_dim_node_8.t_am_pm_node_8"), "outer")
    val autonode_2 = autonode_3.as("hoyrN")
    val autonode_1 = autonode_2.limit(61)
    val sink = autonode_1.select(col("hoyrN.p_channel_tv_node_9"),preloadedUDF(col("hoyrN.p_channel_demo_node_9")))
    sink.explain(true)

    fuzzer.core.global.State.optDF = Some(sink)
  }
}

try {
   Optimized.main(Array())
} catch {
 case e =>
    fuzzer.core.global.State.optRunException = Some(e)
}

if (fuzzer.core.global.State.optRunException.isEmpty)
   fuzzer.core.global.State.optRunException = Some(new Success("Success"))
/*
org.apache.spark.SparkException: [INTERNAL_ERROR] The Spark SQL phase planning failed with an internal error. You hit a bug in Spark or the Spark plugins you use. Please, report this bug to the corresponding communities or vendors, and provide the full stack trace.
org.apache.spark.SparkException$.internalError(SparkException.scala:107)
org.apache.spark.sql.execution.QueryExecution$.toInternalError(QueryExecution.scala:536)
org.apache.spark.sql.execution.QueryExecution$.withInternalError(QueryExecution.scala:548)
org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$1(QueryExecution.scala:219)
org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:900)
org.apache.spark.sql.execution.QueryExecution.executePhase(QueryExecution.scala:218)
org.apache.spark.sql.execution.QueryExecution.sparkPlan$lzycompute(QueryExecution.scala:171)
org.apache.spark.sql.execution.QueryExecution.sparkPlan(QueryExecution.scala:164)
org.apache.spark.sql.execution.QueryExecution.$anonfun$executedPlan$1(QueryExecution.scala:186)
org.apache.spark.sql.catalyst.QueryPlanningTracker.measurePhase(QueryPlanningTracker.scala:138)
org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$2(QueryExecution.scala:219)
org.apache.spark.sql.execution.QueryExecution$.withInternalError(QueryExecution.scala:546)
org.apache.spark.sql.execution.QueryExecution.$anonfun$executePhase$1(QueryExecution.scala:219)
org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:900)
org.apache.spark.sql.execution.QueryExecution.executePhase(QueryExecution.scala:218)
org.apache.spark.sql.execution.QueryExecution.executedPlan$lzycompute(QueryExecution.scala:186)
org.apache.spark.sql.execution.QueryExecution.executedPlan(QueryExecution.scala:179)
org.apache.spark.sql.execution.QueryExecution.$anonfun$writePlans$5(QueryExecution.scala:305)
org.apache.spark.sql.catalyst.plans.QueryPlan$.append(QueryPlan.scala:682)
org.apache.spark.sql.execution.QueryExecution.writePlans(QueryExecution.scala:305)
org.apache.spark.sql.execution.QueryExecution.toString(QueryExecution.scala:320)
org.apache.spark.sql.execution.QueryExecution.org$apache$spark$sql$execution$QueryExecution$$explainString(QueryExecution.scala:274)
org.apache.spark.sql.execution.QueryExecution.explainString(QueryExecution.scala:252)
org.apache.spark.sql.Dataset.$anonfun$explain$1(Dataset.scala:593)
scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)
org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:900)
org.apache.spark.sql.Dataset.explain(Dataset.scala:593)
org.apache.spark.sql.Dataset.explain(Dataset.scala:606)
__wrapper$1$9f3862ffbb19478984b3d1c19a435b70.__wrapper$1$9f3862ffbb19478984b3d1c19a435b70$Optimized$1$.main(<no source file>:52)
__wrapper$1$9f3862ffbb19478984b3d1c19a435b70.__wrapper$1$9f3862ffbb19478984b3d1c19a435b70$.wrapper(<no source file>:59)
java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
java.base/java.lang.reflect.Method.invoke(Method.java:566)
scala.tools.reflect.ToolBoxFactory$ToolBoxImpl$ToolBoxGlobal.$anonfun$compile$11(ToolBoxFactory.scala:291)
scala.tools.reflect.ToolBoxFactory$ToolBoxImpl.eval(ToolBoxFactory.scala:460)
fuzzer.adapters.spark.SparkCodeExecutor.runRaw(SparkCodeGenerator.scala:72)
fuzzer.adapters.spark.SparkCodeExecutor.checkOneGo(SparkCodeGenerator.scala:120)
fuzzer.adapters.spark.SparkCodeExecutor.execute(SparkCodeGenerator.scala:179)
fuzzer.core.engine.FuzzerEngine.$anonfun$processSingleDAG$2(FuzzerEngine.scala:232)
scala.collection.immutable.Range.foreach$mVc$sp(Range.scala:190)
fuzzer.core.engine.FuzzerEngine.$anonfun$processSingleDAG$1(FuzzerEngine.scala:219)
scala.util.control.Breaks.breakable(Breaks.scala:77)
fuzzer.core.engine.FuzzerEngine.processSingleDAG(FuzzerEngine.scala:219)
fuzzer.core.engine.FuzzerEngine.run(FuzzerEngine.scala:182)
fuzzer.MainFuzzer$.main(MainFuzzer.scala:118)
fuzzer.MainFuzzer.main(MainFuzzer.scala)
*/




import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import fuzzer.core.global.State.sparkOption
import fuzzer.utils.spark.optimizer.ToolKit.withoutOptimized
import fuzzer.templates.ComplexObject
import fuzzer.core.exceptions._
import org.apache.spark.sql.expressions.Window


object UnOptimized {

  def main(args: Array[String]): Unit = {
    val spark = sparkOption.get


    val preloadedUDF = udf((s: Any) => {
      val r = scala.util.Random.nextInt()
      ComplexObject(r,r)
    }).asNondeterministic()


    val sparkOpt = spark.sessionState.optimizer
    val excludableRules = {
      val defaultRules = sparkOpt.defaultBatches.flatMap(_.rules.map(_.ruleName)).toSet
      val rules = defaultRules -- sparkOpt.nonExcludableRules.toSet
      rules
    }
    val excludedRules = excludableRules.mkString(",")
    withoutOptimized(excludedRules) {
    
    
    val filterUdfString = udf((arg: String) => {
      arg.startsWith("A") || arg.startsWith("a")
    }).asNondeterministic()
    
    
    
    val filterUdfInteger = udf((arg: Int) => {
      arg.toString == arg.toString.reverse
    }).asNondeterministic()
    
    
    
    val filterUdfDecimal = udf((arg: Double) => {
      (scala.math.round(arg * 100).toDouble / 100.0) == arg
    }).asNondeterministic()
    
    
    val autonode_9 = spark.table("tpcds.promotion").select(spark.table("tpcds.promotion").columns.map(colName => col(colName).alias(s"${colName}_node_9")): _*).as("promotion_node_9")
    val autonode_8 = spark.table("tpcds.time_dim").select(spark.table("tpcds.time_dim").columns.map(colName => col(colName).alias(s"${colName}_node_8")): _*).as("time_dim_node_8")
    val autonode_7 = autonode_9.distinct()
    val autonode_6 = autonode_8.limit(34)
    val autonode_5 = autonode_7.offset(13)
    val autonode_4 = autonode_6.filter(col("time_dim_node_8.t_meal_time_node_8").cast("boolean") === lit(-43))
    val autonode_3 = autonode_4.join(autonode_5, col("promotion_node_9.p_promo_id_node_9") === col("time_dim_node_8.t_am_pm_node_8"), "outer")
    val autonode_2 = autonode_3.as("hoyrN")
    val autonode_1 = autonode_2.limit(61)
    val sink = autonode_1.select(col("hoyrN.p_channel_tv_node_9"),preloadedUDF(col("hoyrN.p_channel_demo_node_9")))
    sink.explain(true)

    fuzzer.core.global.State.unOptDF = Some(sink)
    }
  }
}


try {
   UnOptimized.main(Array())
} catch {
 case e =>
    fuzzer.core.global.State.unOptRunException = Some(e)
}

if (fuzzer.core.global.State.unOptRunException.isEmpty)
   fuzzer.core.global.State.unOptRunException = Some(new Success("Success"))

/*
fuzzer.core.exceptions.Success: Success
*/


/* ========== ORACLE RESULT ===================
fuzzer.core.exceptions.MismatchException: Execution result mismatch between optimized and unoptimized versions.
fuzzer.adapters.spark.SparkCodeExecutor.checkOneGo(SparkCodeGenerator.scala:130)
fuzzer.adapters.spark.SparkCodeExecutor.execute(SparkCodeGenerator.scala:179)
fuzzer.core.engine.FuzzerEngine.$anonfun$processSingleDAG$2(FuzzerEngine.scala:232)
scala.collection.immutable.Range.foreach$mVc$sp(Range.scala:190)
fuzzer.core.engine.FuzzerEngine.$anonfun$processSingleDAG$1(FuzzerEngine.scala:219)
scala.util.control.Breaks.breakable(Breaks.scala:77)
fuzzer.core.engine.FuzzerEngine.processSingleDAG(FuzzerEngine.scala:219)
fuzzer.core.engine.FuzzerEngine.run(FuzzerEngine.scala:182)
fuzzer.MainFuzzer$.main(MainFuzzer.scala:118)
fuzzer.MainFuzzer.main(MainFuzzer.scala)


//Optimizer Branch Coverage: 163