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
    return values.max() - values.min()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_6 = table_env.from_path("web_page").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("web_page").get_schema().get_field_names()])
autonode_8 = table_env.from_path("time_dim").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("time_dim").get_schema().get_field_names()])
autonode_7 = table_env.from_path("reason").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("reason").get_schema().get_field_names()])
autonode_4 = autonode_6.alias('gmJdg')
autonode_5 = autonode_7.join(autonode_8, col('t_sub_shift_node_8') == col('r_reason_desc_node_7'))
autonode_2 = autonode_4.add_columns(lit("hello"))
autonode_3 = autonode_5.order_by(col('t_time_id_node_8'))
autonode_1 = autonode_2.join(autonode_3, col('t_shift_node_8') == col('wp_url_node_6'))
sink = autonode_1.group_by(col('r_reason_sk_node_7')).select(col('r_reason_sk_node_7').min.alias('r_reason_sk_node_7'))
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
LogicalProject(r_reason_sk_node_7=[$1])
+- LogicalAggregate(group=[{15}], EXPR$0=[MIN($15)])
   +- LogicalJoin(condition=[=($25, $8)], joinType=[inner])
      :- LogicalProject(gmJdg=[AS($0, _UTF-16LE'gmJdg')], wp_web_page_id_node_6=[$1], wp_rec_start_date_node_6=[$2], wp_rec_end_date_node_6=[$3], wp_creation_date_sk_node_6=[$4], wp_access_date_sk_node_6=[$5], wp_autogen_flag_node_6=[$6], wp_customer_sk_node_6=[$7], wp_url_node_6=[$8], wp_type_node_6=[$9], wp_char_count_node_6=[$10], wp_link_count_node_6=[$11], wp_image_count_node_6=[$12], wp_max_ad_count_node_6=[$13], _c14=[_UTF-16LE'hello'])
      :  +- LogicalTableScan(table=[[default_catalog, default_database, web_page]])
      +- LogicalSort(sort0=[$4], dir0=[ASC])
         +- LogicalJoin(condition=[=($11, $2)], joinType=[inner])
            :- LogicalProject(r_reason_sk_node_7=[$0], r_reason_id_node_7=[$1], r_reason_desc_node_7=[$2])
            :  +- LogicalTableScan(table=[[default_catalog, default_database, reason]])
            +- LogicalProject(t_time_sk_node_8=[$0], t_time_id_node_8=[$1], t_time_node_8=[$2], t_hour_node_8=[$3], t_minute_node_8=[$4], t_second_node_8=[$5], t_am_pm_node_8=[$6], t_shift_node_8=[$7], t_sub_shift_node_8=[$8], t_meal_time_node_8=[$9])
               +- LogicalTableScan(table=[[default_catalog, default_database, time_dim]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS r_reason_sk_node_7])
+- HashAggregate(isMerge=[true], groupBy=[r_reason_sk], select=[r_reason_sk, Final_MIN(min$0) AS EXPR$0])
   +- Exchange(distribution=[hash[r_reason_sk]])
      +- LocalHashAggregate(groupBy=[r_reason_sk], select=[r_reason_sk, Partial_MIN(r_reason_sk) AS min$0])
         +- HashJoin(joinType=[InnerJoin], where=[=(t_shift, wp_url_node_6)], select=[gmJdg, wp_web_page_id_node_6, wp_rec_start_date_node_6, wp_rec_end_date_node_6, wp_creation_date_sk_node_6, wp_access_date_sk_node_6, wp_autogen_flag_node_6, wp_customer_sk_node_6, wp_url_node_6, wp_type_node_6, wp_char_count_node_6, wp_link_count_node_6, wp_image_count_node_6, wp_max_ad_count_node_6, _c14, r_reason_sk, r_reason_id, r_reason_desc, t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time], isBroadcast=[true], build=[left])
            :- Exchange(distribution=[broadcast])
            :  +- Calc(select=[wp_web_page_sk AS gmJdg, wp_web_page_id AS wp_web_page_id_node_6, wp_rec_start_date AS wp_rec_start_date_node_6, wp_rec_end_date AS wp_rec_end_date_node_6, wp_creation_date_sk AS wp_creation_date_sk_node_6, wp_access_date_sk AS wp_access_date_sk_node_6, wp_autogen_flag AS wp_autogen_flag_node_6, wp_customer_sk AS wp_customer_sk_node_6, wp_url AS wp_url_node_6, wp_type AS wp_type_node_6, wp_char_count AS wp_char_count_node_6, wp_link_count AS wp_link_count_node_6, wp_image_count AS wp_image_count_node_6, wp_max_ad_count AS wp_max_ad_count_node_6, 'hello' AS _c14])
            :     +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
            +- Sort(orderBy=[t_time_id ASC])
               +- Exchange(distribution=[single])
                  +- HashJoin(joinType=[InnerJoin], where=[=(t_sub_shift, r_reason_desc)], select=[r_reason_sk, r_reason_id, r_reason_desc, t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time], isBroadcast=[true], build=[left])
                     :- Exchange(distribution=[broadcast])
                     :  +- TableSourceScan(table=[[default_catalog, default_database, reason]], fields=[r_reason_sk, r_reason_id, r_reason_desc])
                     +- TableSourceScan(table=[[default_catalog, default_database, time_dim]], fields=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS r_reason_sk_node_7])
+- HashAggregate(isMerge=[true], groupBy=[r_reason_sk], select=[r_reason_sk, Final_MIN(min$0) AS EXPR$0])
   +- Exchange(distribution=[hash[r_reason_sk]])
      +- LocalHashAggregate(groupBy=[r_reason_sk], select=[r_reason_sk, Partial_MIN(r_reason_sk) AS min$0])
         +- HashJoin(joinType=[InnerJoin], where=[(t_shift = wp_url_node_6)], select=[gmJdg, wp_web_page_id_node_6, wp_rec_start_date_node_6, wp_rec_end_date_node_6, wp_creation_date_sk_node_6, wp_access_date_sk_node_6, wp_autogen_flag_node_6, wp_customer_sk_node_6, wp_url_node_6, wp_type_node_6, wp_char_count_node_6, wp_link_count_node_6, wp_image_count_node_6, wp_max_ad_count_node_6, _c14, r_reason_sk, r_reason_id, r_reason_desc, t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time], isBroadcast=[true], build=[left])
            :- Exchange(distribution=[broadcast])
            :  +- Calc(select=[wp_web_page_sk AS gmJdg, wp_web_page_id AS wp_web_page_id_node_6, wp_rec_start_date AS wp_rec_start_date_node_6, wp_rec_end_date AS wp_rec_end_date_node_6, wp_creation_date_sk AS wp_creation_date_sk_node_6, wp_access_date_sk AS wp_access_date_sk_node_6, wp_autogen_flag AS wp_autogen_flag_node_6, wp_customer_sk AS wp_customer_sk_node_6, wp_url AS wp_url_node_6, wp_type AS wp_type_node_6, wp_char_count AS wp_char_count_node_6, wp_link_count AS wp_link_count_node_6, wp_image_count AS wp_image_count_node_6, wp_max_ad_count AS wp_max_ad_count_node_6, 'hello' AS _c14])
            :     +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
            +- Sort(orderBy=[t_time_id ASC])
               +- Exchange(distribution=[single])
                  +- MultipleInput(readOrder=[0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(t_sub_shift = r_reason_desc)], select=[r_reason_sk, r_reason_id, r_reason_desc, t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time], isBroadcast=[true], build=[left])\
:- [#1] Exchange(distribution=[broadcast])\
+- [#2] TableSourceScan(table=[[default_catalog, default_database, time_dim]], fields=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time])\
])
                     :- Exchange(distribution=[broadcast])
                     :  +- TableSourceScan(table=[[default_catalog, default_database, reason]], fields=[r_reason_sk, r_reason_id, r_reason_desc])
                     +- TableSourceScan(table=[[default_catalog, default_database, time_dim]], fields=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o117677009.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#237216094:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[4](input=RelSubset#237216092,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[4]), rel#237216091:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[4](input=RelSubset#237216090,groupBy=r_reason_sk, t_shift,select=r_reason_sk, t_shift, Partial_MIN(r_reason_sk) AS min$0)]
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