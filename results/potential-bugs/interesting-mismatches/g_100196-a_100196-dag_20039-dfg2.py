# ======== Program ========
from pyflink.table import *
from pyflink.table.expressions import *
from pyflink.table.udf import udf
from pyflink.table.types import DataTypes

from pyflink.table.udf import AggregateFunction, udaf
from pyflink.table import DataTypes
import pandas as pd

class MyObject:
    def __init__(self, name, value):
        self.name = name
        self.value = value

# UDF that returns the custom object
@udf(result_type=DataTypes.ROW([
    DataTypes.FIELD("name", DataTypes.STRING()),
    DataTypes.FIELD("value", DataTypes.INT())
]))
def preloaded_udf_complex(*input_val):
    obj = MyObject("test", hash(input_val[0]))
    return (obj.name, obj.value)  # Return as tuple

@udf(result_type=DataTypes.BOOLEAN())
def preloaded_udf_boolean(input_val):
    return True


def preloaded_aggregation(values: pd.Series) -> float:
    return len(values) / (1.0 / values).sum() if (values != 0).all() else None


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_4 = table_env.from_path("web_page").select(*[col(column_name).alias(f"{column_name}_node_4") for column_name in table_env.from_path("web_page").get_schema().get_field_names()])
autonode_5 = table_env.from_path("promotion").select(*[col(column_name).alias(f"{column_name}_node_5") for column_name in table_env.from_path("promotion").get_schema().get_field_names()])
autonode_2 = autonode_4.order_by(col('wp_max_ad_count_node_4'))
autonode_3 = autonode_5.select(col('p_channel_email_node_5'))
autonode_1 = autonode_2.join(autonode_3, col('p_channel_email_node_5') == col('wp_rec_end_date_node_4'))
sink = autonode_1.group_by(col('wp_creation_date_sk_node_4')).select(col('wp_access_date_sk_node_4').max.alias('wp_access_date_sk_node_4'))
print(sink.explain())

# ======== Details ========
"""
{
  "is_same": false,
  "result_name": "MismatchException",
  "result_details": {
    "opt": {
      "success": true,
      "error_name": "",
      "error_message": "",
      "stdout": "== Abstract Syntax Tree ==
LogicalProject(wp_access_date_sk_node_4=[$1])
+- LogicalAggregate(group=[{4}], EXPR$0=[MAX($5)])
   +- LogicalJoin(condition=[=($14, $3)], joinType=[inner])
      :- LogicalSort(sort0=[$13], dir0=[ASC])
      :  +- LogicalProject(wp_web_page_sk_node_4=[$0], wp_web_page_id_node_4=[$1], wp_rec_start_date_node_4=[$2], wp_rec_end_date_node_4=[$3], wp_creation_date_sk_node_4=[$4], wp_access_date_sk_node_4=[$5], wp_autogen_flag_node_4=[$6], wp_customer_sk_node_4=[$7], wp_url_node_4=[$8], wp_type_node_4=[$9], wp_char_count_node_4=[$10], wp_link_count_node_4=[$11], wp_image_count_node_4=[$12], wp_max_ad_count_node_4=[$13])
      :     +- LogicalTableScan(table=[[default_catalog, default_database, web_page]])
      +- LogicalProject(p_channel_email_node_5=[$9])
         +- LogicalTableScan(table=[[default_catalog, default_database, promotion]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS wp_access_date_sk_node_4])
+- HashAggregate(isMerge=[true], groupBy=[wp_creation_date_sk], select=[wp_creation_date_sk, Final_MAX(max$0) AS EXPR$0])
   +- Exchange(distribution=[hash[wp_creation_date_sk]])
      +- LocalHashAggregate(groupBy=[wp_creation_date_sk], select=[wp_creation_date_sk, Partial_MAX(wp_access_date_sk) AS max$0])
         +- HashJoin(joinType=[InnerJoin], where=[=(p_channel_email, wp_rec_end_date)], select=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count, p_channel_email], isBroadcast=[true], build=[right])
            :- Sort(orderBy=[wp_max_ad_count ASC])
            :  +- Exchange(distribution=[single])
            :     +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
            +- Exchange(distribution=[broadcast])
               +- TableSourceScan(table=[[default_catalog, default_database, promotion, project=[p_channel_email], metadata=[]]], fields=[p_channel_email])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS wp_access_date_sk_node_4])
+- HashAggregate(isMerge=[true], groupBy=[wp_creation_date_sk], select=[wp_creation_date_sk, Final_MAX(max$0) AS EXPR$0])
   +- Exchange(distribution=[hash[wp_creation_date_sk]])
      +- LocalHashAggregate(groupBy=[wp_creation_date_sk], select=[wp_creation_date_sk, Partial_MAX(wp_access_date_sk) AS max$0])
         +- HashJoin(joinType=[InnerJoin], where=[(p_channel_email = wp_rec_end_date)], select=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count, p_channel_email], isBroadcast=[true], build=[right])
            :- Sort(orderBy=[wp_max_ad_count ASC])
            :  +- Exchange(distribution=[single])
            :     +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
            +- Exchange(distribution=[broadcast])
               +- TableSourceScan(table=[[default_catalog, default_database, promotion, project=[p_channel_email], metadata=[]]], fields=[p_channel_email])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o54702127.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#109545651:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[13](input=RelSubset#109545649,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[13]), rel#109545648:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[13](input=RelSubset#109545647,groupBy=wp_rec_end_date, wp_creation_date_sk,select=wp_rec_end_date, wp_creation_date_sk, Partial_MAX(wp_access_date_sk) AS max$0)]
\tat org.apache.calcite.plan.volcano.VolcanoRuleCall.onMatch(VolcanoRuleCall.java:250)
\tat org.apache.calcite.plan.volcano.IterativeRuleDriver.drive(IterativeRuleDriver.java:59)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.findBestExp(VolcanoPlanner.java:523)
\tat org.apache.calcite.tools.Programs$RuleSetProgram.run(Programs.java:317)
\tat org.apache.flink.table.planner.plan.optimize.program.FlinkVolcanoProgram.optimize(FlinkVolcanoProgram.scala:62)
\tat org.apache.flink.table.planner.plan.optimize.program.FlinkChainedProgram.$anonfun$optimize$1(FlinkChainedProgram.scala:59)
\tat scala.collection.TraversableOnce$folder$1.apply(TraversableOnce.scala:196)
\tat scala.collection.TraversableOnce$folder$1.apply(TraversableOnce.scala:194)
\tat scala.collection.Iterator.foreach(Iterator.scala:943)
\tat scala.collection.Iterator.foreach$(Iterator.scala:943)
\tat scala.collection.AbstractIterator.foreach(Iterator.scala:1431)
\tat scala.collection.IterableLike.foreach(IterableLike.scala:74)
\tat scala.collection.IterableLike.foreach$(IterableLike.scala:73)
\tat scala.collection.AbstractIterable.foreach(Iterable.scala:56)
\tat scala.collection.TraversableOnce.foldLeft(TraversableOnce.scala:199)
\tat scala.collection.TraversableOnce.foldLeft$(TraversableOnce.scala:192)
\tat scala.collection.AbstractTraversable.foldLeft(Traversable.scala:108)
\tat org.apache.flink.table.planner.plan.optimize.program.FlinkChainedProgram.optimize(FlinkChainedProgram.scala:55)
\tat org.apache.flink.table.planner.plan.optimize.BatchCommonSubGraphBasedOptimizer.optimizeTree(BatchCommonSubGraphBasedOptimizer.scala:93)
\tat org.apache.flink.table.planner.plan.optimize.BatchCommonSubGraphBasedOptimizer.optimizeBlock(BatchCommonSubGraphBasedOptimizer.scala:58)
\tat org.apache.flink.table.planner.plan.optimize.BatchCommonSubGraphBasedOptimizer.$anonfun$doOptimize$1(BatchCommonSubGraphBasedOptimizer.scala:45)
\tat org.apache.flink.table.planner.plan.optimize.BatchCommonSubGraphBasedOptimizer.$anonfun$doOptimize$1$adapted(BatchCommonSubGraphBasedOptimizer.scala:45)
\tat scala.collection.immutable.List.foreach(List.scala:431)
\tat org.apache.flink.table.planner.plan.optimize.BatchCommonSubGraphBasedOptimizer.doOptimize(BatchCommonSubGraphBasedOptimizer.scala:45)
\tat org.apache.flink.table.planner.plan.optimize.CommonSubGraphBasedOptimizer.optimize(CommonSubGraphBasedOptimizer.scala:87)
\tat org.apache.flink.table.planner.delegation.PlannerBase.optimize(PlannerBase.scala:390)
\tat org.apache.flink.table.planner.delegation.PlannerBase.getExplainGraphs(PlannerBase.scala:625)
\tat org.apache.flink.table.planner.delegation.BatchPlanner.explain(BatchPlanner.scala:149)
\tat org.apache.flink.table.planner.delegation.BatchPlanner.explain(BatchPlanner.scala:49)
\tat org.apache.flink.table.api.internal.TableEnvironmentImpl.explainInternal(TableEnvironmentImpl.java:753)
\tat org.apache.flink.table.api.internal.TableImpl.explain(TableImpl.java:482)
\tat jdk.internal.reflect.GeneratedMethodAccessor32.invoke(Unknown Source)
\tat java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
\tat java.base/java.lang.reflect.Method.invoke(Method.java:566)
\tat org.apache.flink.api.python.shaded.py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)
\tat org.apache.flink.api.python.shaded.py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:374)
\tat org.apache.flink.api.python.shaded.py4j.Gateway.invoke(Gateway.java:282)
\tat org.apache.flink.api.python.shaded.py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
\tat org.apache.flink.api.python.shaded.py4j.commands.CallCommand.execute(CallCommand.java:79)
\tat org.apache.flink.api.python.shaded.py4j.GatewayConnection.run(GatewayConnection.java:238)
\tat java.base/java.lang.Thread.run(Thread.java:829)
Caused by: java.lang.RuntimeException: Error occurred while applying rule FlinkExpandConversionRule
\tat org.apache.calcite.plan.volcano.VolcanoRuleCall.transformTo(VolcanoRuleCall.java:157)
\tat org.apache.calcite.plan.RelOptRuleCall.transformTo(RelOptRuleCall.java:273)
\tat org.apache.calcite.plan.RelOptRuleCall.transformTo(RelOptRuleCall.java:288)
\tat org.apache.flink.table.planner.plan.rules.physical.FlinkExpandConversionRule.satisfyTraitsBySelf(FlinkExpandConversionRule.scala:72)
\tat org.apache.flink.table.planner.plan.rules.physical.FlinkExpandConversionRule.onMatch(FlinkExpandConversionRule.scala:52)
\tat org.apache.calcite.plan.volcano.VolcanoRuleCall.onMatch(VolcanoRuleCall.java:223)
\t... 40 more
Caused by: java.lang.ArrayIndexOutOfBoundsException
",
      "stdout": "",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0