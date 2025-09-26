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
    return values.max()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_13 = table_env.from_path("inventory").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("inventory").get_schema().get_field_names()])
autonode_12 = table_env.from_path("web_page").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("web_page").get_schema().get_field_names()])
autonode_11 = table_env.from_path("time_dim").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("time_dim").get_schema().get_field_names()])
autonode_10 = autonode_13.order_by(col('inv_warehouse_sk_node_13'))
autonode_9 = autonode_12.alias('COymK')
autonode_8 = autonode_11.filter(col('t_meal_time_node_11').char_length > 5)
autonode_7 = autonode_10.alias('RkUoR')
autonode_6 = autonode_9.limit(90)
autonode_5 = autonode_8.filter(col('t_time_node_11') >= 29)
autonode_4 = autonode_7.filter(col('inv_item_sk_node_13') < -50)
autonode_3 = autonode_5.join(autonode_6, col('wp_customer_sk_node_12') == col('t_minute_node_11'))
autonode_2 = autonode_3.join(autonode_4, col('inv_quantity_on_hand_node_13') == col('t_hour_node_11'))
autonode_1 = autonode_2.group_by(col('wp_link_count_node_12')).select(col('t_second_node_11').avg.alias('t_second_node_11'))
sink = autonode_1.distinct()
print(sink.explain())

# ======== Details ========
"""
{
  "is_same": false,
  "result_name": "MismatchException",
  "result_details": {
    "opt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o248489959.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#502658193:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[2](input=RelSubset#502658191,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[2]), rel#502658190:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[2](input=RelSubset#502658189,groupBy=inv_quantity_on_hand,select=inv_quantity_on_hand, Partial_COUNT(*) AS count1$0)]
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
    },
    "unopt": {
      "success": true,
      "error_name": "",
      "error_message": "",
      "stdout": "== Abstract Syntax Tree ==
LogicalAggregate(group=[{0}])
+- LogicalProject(t_second_node_11=[$1])
   +- LogicalAggregate(group=[{21}], EXPR$0=[AVG($5)])
      +- LogicalJoin(condition=[=($27, $3)], joinType=[inner])
         :- LogicalJoin(condition=[=($17, $4)], joinType=[inner])
         :  :- LogicalFilter(condition=[>=($2, 29)])
         :  :  +- LogicalFilter(condition=[>(CHAR_LENGTH($9), 5)])
         :  :     +- LogicalProject(t_time_sk_node_11=[$0], t_time_id_node_11=[$1], t_time_node_11=[$2], t_hour_node_11=[$3], t_minute_node_11=[$4], t_second_node_11=[$5], t_am_pm_node_11=[$6], t_shift_node_11=[$7], t_sub_shift_node_11=[$8], t_meal_time_node_11=[$9])
         :  :        +- LogicalTableScan(table=[[default_catalog, default_database, time_dim]])
         :  +- LogicalSort(fetch=[90])
         :     +- LogicalProject(COymK=[AS($0, _UTF-16LE'COymK')], wp_web_page_id_node_12=[$1], wp_rec_start_date_node_12=[$2], wp_rec_end_date_node_12=[$3], wp_creation_date_sk_node_12=[$4], wp_access_date_sk_node_12=[$5], wp_autogen_flag_node_12=[$6], wp_customer_sk_node_12=[$7], wp_url_node_12=[$8], wp_type_node_12=[$9], wp_char_count_node_12=[$10], wp_link_count_node_12=[$11], wp_image_count_node_12=[$12], wp_max_ad_count_node_12=[$13])
         :        +- LogicalTableScan(table=[[default_catalog, default_database, web_page]])
         +- LogicalFilter(condition=[<($1, -50)])
            +- LogicalProject(RkUoR=[AS($0, _UTF-16LE'RkUoR')], inv_item_sk_node_13=[$1], inv_warehouse_sk_node_13=[$2], inv_quantity_on_hand_node_13=[$3])
               +- LogicalSort(sort0=[$2], dir0=[ASC])
                  +- LogicalProject(inv_date_sk_node_13=[$0], inv_item_sk_node_13=[$1], inv_warehouse_sk_node_13=[$2], inv_quantity_on_hand_node_13=[$3])
                     +- LogicalTableScan(table=[[default_catalog, default_database, inventory]])

== Optimized Physical Plan ==
HashAggregate(isMerge=[true], groupBy=[t_second_node_11], select=[t_second_node_11])
+- Exchange(distribution=[hash[t_second_node_11]])
   +- LocalHashAggregate(groupBy=[t_second_node_11], select=[t_second_node_11])
      +- Calc(select=[EXPR$0 AS t_second_node_11])
         +- HashAggregate(isMerge=[true], groupBy=[wp_link_count_node_12], select=[wp_link_count_node_12, Final_AVG(sum$0, count$1) AS EXPR$0])
            +- Exchange(distribution=[hash[wp_link_count_node_12]])
               +- LocalHashAggregate(groupBy=[wp_link_count_node_12], select=[wp_link_count_node_12, Partial_AVG(t_second) AS (sum$0, count$1)])
                  +- NestedLoopJoin(joinType=[InnerJoin], where=[=(inv_quantity_on_hand_node_13, t_hour)], select=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time, COymK, wp_web_page_id_node_12, wp_rec_start_date_node_12, wp_rec_end_date_node_12, wp_creation_date_sk_node_12, wp_access_date_sk_node_12, wp_autogen_flag_node_12, wp_customer_sk_node_12, wp_url_node_12, wp_type_node_12, wp_char_count_node_12, wp_link_count_node_12, wp_image_count_node_12, wp_max_ad_count_node_12, RkUoR, inv_item_sk_node_13, inv_warehouse_sk_node_13, inv_quantity_on_hand_node_13], build=[right])
                     :- HashJoin(joinType=[InnerJoin], where=[=(wp_customer_sk_node_12, t_minute)], select=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time, COymK, wp_web_page_id_node_12, wp_rec_start_date_node_12, wp_rec_end_date_node_12, wp_creation_date_sk_node_12, wp_access_date_sk_node_12, wp_autogen_flag_node_12, wp_customer_sk_node_12, wp_url_node_12, wp_type_node_12, wp_char_count_node_12, wp_link_count_node_12, wp_image_count_node_12, wp_max_ad_count_node_12], isBroadcast=[true], build=[right])
                     :  :- Calc(select=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time], where=[AND(>(CHAR_LENGTH(t_meal_time), 5), >=(t_time, 29))])
                     :  :  +- TableSourceScan(table=[[default_catalog, default_database, time_dim, filter=[and(>(CHAR_LENGTH(t_meal_time), 5), >=(t_time, 29))]]], fields=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time])
                     :  +- Exchange(distribution=[broadcast])
                     :     +- Limit(offset=[0], fetch=[90], global=[true])
                     :        +- Exchange(distribution=[single])
                     :           +- Limit(offset=[0], fetch=[90], global=[false])
                     :              +- Calc(select=[wp_web_page_sk AS COymK, wp_web_page_id AS wp_web_page_id_node_12, wp_rec_start_date AS wp_rec_start_date_node_12, wp_rec_end_date AS wp_rec_end_date_node_12, wp_creation_date_sk AS wp_creation_date_sk_node_12, wp_access_date_sk AS wp_access_date_sk_node_12, wp_autogen_flag AS wp_autogen_flag_node_12, wp_customer_sk AS wp_customer_sk_node_12, wp_url AS wp_url_node_12, wp_type AS wp_type_node_12, wp_char_count AS wp_char_count_node_12, wp_link_count AS wp_link_count_node_12, wp_image_count AS wp_image_count_node_12, wp_max_ad_count AS wp_max_ad_count_node_12])
                     :                 +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
                     +- Exchange(distribution=[broadcast])
                        +- Calc(select=[inv_date_sk AS RkUoR, inv_item_sk AS inv_item_sk_node_13, inv_warehouse_sk AS inv_warehouse_sk_node_13, inv_quantity_on_hand AS inv_quantity_on_hand_node_13], where=[<(inv_item_sk, -50)])
                           +- SortLimit(orderBy=[inv_warehouse_sk ASC], offset=[0], fetch=[1], global=[true])
                              +- Exchange(distribution=[single])
                                 +- SortLimit(orderBy=[inv_warehouse_sk ASC], offset=[0], fetch=[1], global=[false])
                                    +- TableSourceScan(table=[[default_catalog, default_database, inventory]], fields=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand])

== Optimized Execution Plan ==
HashAggregate(isMerge=[true], groupBy=[t_second_node_11], select=[t_second_node_11])
+- Exchange(distribution=[hash[t_second_node_11]])
   +- LocalHashAggregate(groupBy=[t_second_node_11], select=[t_second_node_11])
      +- Calc(select=[EXPR$0 AS t_second_node_11])
         +- HashAggregate(isMerge=[true], groupBy=[wp_link_count_node_12], select=[wp_link_count_node_12, Final_AVG(sum$0, count$1) AS EXPR$0])
            +- Exchange(distribution=[hash[wp_link_count_node_12]])
               +- LocalHashAggregate(groupBy=[wp_link_count_node_12], select=[wp_link_count_node_12, Partial_AVG(t_second) AS (sum$0, count$1)])
                  +- NestedLoopJoin(joinType=[InnerJoin], where=[(inv_quantity_on_hand_node_13 = t_hour)], select=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time, COymK, wp_web_page_id_node_12, wp_rec_start_date_node_12, wp_rec_end_date_node_12, wp_creation_date_sk_node_12, wp_access_date_sk_node_12, wp_autogen_flag_node_12, wp_customer_sk_node_12, wp_url_node_12, wp_type_node_12, wp_char_count_node_12, wp_link_count_node_12, wp_image_count_node_12, wp_max_ad_count_node_12, RkUoR, inv_item_sk_node_13, inv_warehouse_sk_node_13, inv_quantity_on_hand_node_13], build=[right])
                     :- HashJoin(joinType=[InnerJoin], where=[(wp_customer_sk_node_12 = t_minute)], select=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time, COymK, wp_web_page_id_node_12, wp_rec_start_date_node_12, wp_rec_end_date_node_12, wp_creation_date_sk_node_12, wp_access_date_sk_node_12, wp_autogen_flag_node_12, wp_customer_sk_node_12, wp_url_node_12, wp_type_node_12, wp_char_count_node_12, wp_link_count_node_12, wp_image_count_node_12, wp_max_ad_count_node_12], isBroadcast=[true], build=[right])
                     :  :- Calc(select=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time], where=[((CHAR_LENGTH(t_meal_time) > 5) AND (t_time >= 29))])
                     :  :  +- TableSourceScan(table=[[default_catalog, default_database, time_dim, filter=[and(>(CHAR_LENGTH(t_meal_time), 5), >=(t_time, 29))]]], fields=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time])
                     :  +- Exchange(distribution=[broadcast])
                     :     +- Limit(offset=[0], fetch=[90], global=[true])
                     :        +- Exchange(distribution=[single])
                     :           +- Limit(offset=[0], fetch=[90], global=[false])
                     :              +- Calc(select=[wp_web_page_sk AS COymK, wp_web_page_id AS wp_web_page_id_node_12, wp_rec_start_date AS wp_rec_start_date_node_12, wp_rec_end_date AS wp_rec_end_date_node_12, wp_creation_date_sk AS wp_creation_date_sk_node_12, wp_access_date_sk AS wp_access_date_sk_node_12, wp_autogen_flag AS wp_autogen_flag_node_12, wp_customer_sk AS wp_customer_sk_node_12, wp_url AS wp_url_node_12, wp_type AS wp_type_node_12, wp_char_count AS wp_char_count_node_12, wp_link_count AS wp_link_count_node_12, wp_image_count AS wp_image_count_node_12, wp_max_ad_count AS wp_max_ad_count_node_12])
                     :                 +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
                     +- Exchange(distribution=[broadcast])
                        +- Calc(select=[inv_date_sk AS RkUoR, inv_item_sk AS inv_item_sk_node_13, inv_warehouse_sk AS inv_warehouse_sk_node_13, inv_quantity_on_hand AS inv_quantity_on_hand_node_13], where=[(inv_item_sk < -50)])
                           +- SortLimit(orderBy=[inv_warehouse_sk ASC], offset=[0], fetch=[1], global=[true])
                              +- Exchange(distribution=[single])
                                 +- SortLimit(orderBy=[inv_warehouse_sk ASC], offset=[0], fetch=[1], global=[false])
                                    +- TableSourceScan(table=[[default_catalog, default_database, inventory]], fields=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0