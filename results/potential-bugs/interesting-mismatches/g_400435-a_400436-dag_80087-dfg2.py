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
    return values.kurtosis()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_13 = table_env.from_path("promotion").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("promotion").get_schema().get_field_names()])
autonode_12 = table_env.from_path("time_dim").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("time_dim").get_schema().get_field_names()])
autonode_11 = table_env.from_path("catalog_returns").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("catalog_returns").get_schema().get_field_names()])
autonode_10 = autonode_12.join(autonode_13, col('t_shift_node_12') == col('p_channel_details_node_13'))
autonode_9 = autonode_11.select(col('cr_reason_sk_node_11'))
autonode_8 = autonode_10.add_columns(lit("hello"))
autonode_7 = autonode_9.select(col('cr_reason_sk_node_11'))
autonode_6 = autonode_8.limit(42)
autonode_5 = autonode_7.limit(70)
autonode_4 = autonode_6.order_by(col('p_channel_catalog_node_13'))
autonode_3 = autonode_5.select(col('cr_reason_sk_node_11'))
autonode_2 = autonode_3.join(autonode_4, col('t_hour_node_12') == col('cr_reason_sk_node_11'))
autonode_1 = autonode_2.filter(col('t_meal_time_node_12').char_length < 5)
sink = autonode_1.group_by(col('t_hour_node_12')).select(col('t_hour_node_12').min.alias('t_hour_node_12'))
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
LogicalProject(t_hour_node_12=[$1])
+- LogicalAggregate(group=[{4}], EXPR$0=[MIN($4)])
   +- LogicalFilter(condition=[<(CHAR_LENGTH($10), 5)])
      +- LogicalJoin(condition=[=($4, $0)], joinType=[inner])
         :- LogicalProject(cr_reason_sk_node_11=[$0])
         :  +- LogicalSort(fetch=[70])
         :     +- LogicalProject(cr_reason_sk_node_11=[$15])
         :        +- LogicalTableScan(table=[[default_catalog, default_database, catalog_returns]])
         +- LogicalSort(sort0=[$20], dir0=[ASC])
            +- LogicalSort(fetch=[42])
               +- LogicalProject(t_time_sk_node_12=[$0], t_time_id_node_12=[$1], t_time_node_12=[$2], t_hour_node_12=[$3], t_minute_node_12=[$4], t_second_node_12=[$5], t_am_pm_node_12=[$6], t_shift_node_12=[$7], t_sub_shift_node_12=[$8], t_meal_time_node_12=[$9], p_promo_sk_node_13=[$10], p_promo_id_node_13=[$11], p_start_date_sk_node_13=[$12], p_end_date_sk_node_13=[$13], p_item_sk_node_13=[$14], p_cost_node_13=[$15], p_response_target_node_13=[$16], p_promo_name_node_13=[$17], p_channel_dmail_node_13=[$18], p_channel_email_node_13=[$19], p_channel_catalog_node_13=[$20], p_channel_tv_node_13=[$21], p_channel_radio_node_13=[$22], p_channel_press_node_13=[$23], p_channel_event_node_13=[$24], p_channel_demo_node_13=[$25], p_channel_details_node_13=[$26], p_purpose_node_13=[$27], p_discount_active_node_13=[$28], _c29=[_UTF-16LE'hello'])
                  +- LogicalJoin(condition=[=($7, $26)], joinType=[inner])
                     :- LogicalProject(t_time_sk_node_12=[$0], t_time_id_node_12=[$1], t_time_node_12=[$2], t_hour_node_12=[$3], t_minute_node_12=[$4], t_second_node_12=[$5], t_am_pm_node_12=[$6], t_shift_node_12=[$7], t_sub_shift_node_12=[$8], t_meal_time_node_12=[$9])
                     :  +- LogicalTableScan(table=[[default_catalog, default_database, time_dim]])
                     +- LogicalProject(p_promo_sk_node_13=[$0], p_promo_id_node_13=[$1], p_start_date_sk_node_13=[$2], p_end_date_sk_node_13=[$3], p_item_sk_node_13=[$4], p_cost_node_13=[$5], p_response_target_node_13=[$6], p_promo_name_node_13=[$7], p_channel_dmail_node_13=[$8], p_channel_email_node_13=[$9], p_channel_catalog_node_13=[$10], p_channel_tv_node_13=[$11], p_channel_radio_node_13=[$12], p_channel_press_node_13=[$13], p_channel_event_node_13=[$14], p_channel_demo_node_13=[$15], p_channel_details_node_13=[$16], p_purpose_node_13=[$17], p_discount_active_node_13=[$18])
                        +- LogicalTableScan(table=[[default_catalog, default_database, promotion]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS t_hour_node_12])
+- HashAggregate(isMerge=[true], groupBy=[t_hour_node_12], select=[t_hour_node_12, Final_MIN(min$0) AS EXPR$0])
   +- Exchange(distribution=[hash[t_hour_node_12]])
      +- LocalHashAggregate(groupBy=[t_hour_node_12], select=[t_hour_node_12, Partial_MIN(t_hour_node_12) AS min$0])
         +- HashJoin(joinType=[InnerJoin], where=[=(t_hour_node_12, cr_reason_sk)], select=[cr_reason_sk, t_time_sk_node_12, t_time_id_node_12, t_time_node_12, t_hour_node_12, t_minute_node_12, t_second_node_12, t_am_pm_node_12, t_shift_node_12, t_sub_shift_node_12, t_meal_time_node_12, p_promo_sk_node_13, p_promo_id_node_13, p_start_date_sk_node_13, p_end_date_sk_node_13, p_item_sk_node_13, p_cost_node_13, p_response_target_node_13, p_promo_name_node_13, p_channel_dmail_node_13, p_channel_email_node_13, p_channel_catalog_node_13, p_channel_tv_node_13, p_channel_radio_node_13, p_channel_press_node_13, p_channel_event_node_13, p_channel_demo_node_13, p_channel_details_node_13, p_purpose_node_13, p_discount_active_node_13, _c29], isBroadcast=[true], build=[left])
            :- Exchange(distribution=[broadcast])
            :  +- Limit(offset=[0], fetch=[70], global=[true])
            :     +- Exchange(distribution=[single])
            :        +- Limit(offset=[0], fetch=[70], global=[false])
            :           +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns, project=[cr_reason_sk], metadata=[], limit=[70]]], fields=[cr_reason_sk])
            +- Calc(select=[t_time_sk AS t_time_sk_node_12, t_time_id AS t_time_id_node_12, t_time AS t_time_node_12, t_hour AS t_hour_node_12, t_minute AS t_minute_node_12, t_second AS t_second_node_12, t_am_pm AS t_am_pm_node_12, t_shift AS t_shift_node_12, t_sub_shift AS t_sub_shift_node_12, t_meal_time AS t_meal_time_node_12, p_promo_sk AS p_promo_sk_node_13, p_promo_id AS p_promo_id_node_13, p_start_date_sk AS p_start_date_sk_node_13, p_end_date_sk AS p_end_date_sk_node_13, p_item_sk AS p_item_sk_node_13, p_cost AS p_cost_node_13, p_response_target AS p_response_target_node_13, p_promo_name AS p_promo_name_node_13, p_channel_dmail AS p_channel_dmail_node_13, p_channel_email AS p_channel_email_node_13, p_channel_catalog AS p_channel_catalog_node_13, p_channel_tv AS p_channel_tv_node_13, p_channel_radio AS p_channel_radio_node_13, p_channel_press AS p_channel_press_node_13, p_channel_event AS p_channel_event_node_13, p_channel_demo AS p_channel_demo_node_13, p_channel_details AS p_channel_details_node_13, p_purpose AS p_purpose_node_13, p_discount_active AS p_discount_active_node_13, 'hello' AS _c29], where=[<(CHAR_LENGTH(t_meal_time), 5)])
               +- Sort(orderBy=[p_channel_catalog ASC])
                  +- Limit(offset=[0], fetch=[42], global=[true])
                     +- Exchange(distribution=[single])
                        +- Limit(offset=[0], fetch=[42], global=[false])
                           +- HashJoin(joinType=[InnerJoin], where=[=(t_shift, p_channel_details)], select=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time, p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active], isBroadcast=[true], build=[right])
                              :- TableSourceScan(table=[[default_catalog, default_database, time_dim]], fields=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time])
                              +- Exchange(distribution=[broadcast])
                                 +- TableSourceScan(table=[[default_catalog, default_database, promotion]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS t_hour_node_12])
+- HashAggregate(isMerge=[true], groupBy=[t_hour_node_12], select=[t_hour_node_12, Final_MIN(min$0) AS EXPR$0])
   +- Exchange(distribution=[hash[t_hour_node_12]])
      +- LocalHashAggregate(groupBy=[t_hour_node_12], select=[t_hour_node_12, Partial_MIN(t_hour_node_12) AS min$0])
         +- HashJoin(joinType=[InnerJoin], where=[(t_hour_node_12 = cr_reason_sk)], select=[cr_reason_sk, t_time_sk_node_12, t_time_id_node_12, t_time_node_12, t_hour_node_12, t_minute_node_12, t_second_node_12, t_am_pm_node_12, t_shift_node_12, t_sub_shift_node_12, t_meal_time_node_12, p_promo_sk_node_13, p_promo_id_node_13, p_start_date_sk_node_13, p_end_date_sk_node_13, p_item_sk_node_13, p_cost_node_13, p_response_target_node_13, p_promo_name_node_13, p_channel_dmail_node_13, p_channel_email_node_13, p_channel_catalog_node_13, p_channel_tv_node_13, p_channel_radio_node_13, p_channel_press_node_13, p_channel_event_node_13, p_channel_demo_node_13, p_channel_details_node_13, p_purpose_node_13, p_discount_active_node_13, _c29], isBroadcast=[true], build=[left])
            :- Exchange(distribution=[broadcast])
            :  +- Limit(offset=[0], fetch=[70], global=[true])
            :     +- Exchange(distribution=[single])
            :        +- Limit(offset=[0], fetch=[70], global=[false])
            :           +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns, project=[cr_reason_sk], metadata=[], limit=[70]]], fields=[cr_reason_sk])
            +- Calc(select=[t_time_sk AS t_time_sk_node_12, t_time_id AS t_time_id_node_12, t_time AS t_time_node_12, t_hour AS t_hour_node_12, t_minute AS t_minute_node_12, t_second AS t_second_node_12, t_am_pm AS t_am_pm_node_12, t_shift AS t_shift_node_12, t_sub_shift AS t_sub_shift_node_12, t_meal_time AS t_meal_time_node_12, p_promo_sk AS p_promo_sk_node_13, p_promo_id AS p_promo_id_node_13, p_start_date_sk AS p_start_date_sk_node_13, p_end_date_sk AS p_end_date_sk_node_13, p_item_sk AS p_item_sk_node_13, p_cost AS p_cost_node_13, p_response_target AS p_response_target_node_13, p_promo_name AS p_promo_name_node_13, p_channel_dmail AS p_channel_dmail_node_13, p_channel_email AS p_channel_email_node_13, p_channel_catalog AS p_channel_catalog_node_13, p_channel_tv AS p_channel_tv_node_13, p_channel_radio AS p_channel_radio_node_13, p_channel_press AS p_channel_press_node_13, p_channel_event AS p_channel_event_node_13, p_channel_demo AS p_channel_demo_node_13, p_channel_details AS p_channel_details_node_13, p_purpose AS p_purpose_node_13, p_discount_active AS p_discount_active_node_13, 'hello' AS _c29], where=[(CHAR_LENGTH(t_meal_time) < 5)])
               +- Sort(orderBy=[p_channel_catalog ASC])
                  +- Limit(offset=[0], fetch=[42], global=[true])
                     +- Exchange(distribution=[single])
                        +- Limit(offset=[0], fetch=[42], global=[false])
                           +- MultipleInput(readOrder=[1,0], members=[\
HashJoin(joinType=[InnerJoin], where=[(t_shift = p_channel_details)], select=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time, p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active], isBroadcast=[true], build=[right])\
:- [#1] TableSourceScan(table=[[default_catalog, default_database, time_dim]], fields=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time])\
+- [#2] Exchange(distribution=[broadcast])\
])
                              :- TableSourceScan(table=[[default_catalog, default_database, time_dim]], fields=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time])
                              +- Exchange(distribution=[broadcast])
                                 +- TableSourceScan(table=[[default_catalog, default_database, promotion]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o217885783.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#440871032:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[20](input=RelSubset#440871030,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[20]), rel#440871029:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[20](input=RelSubset#440871028,groupBy=t_hour,select=t_hour, Partial_MIN(t_hour) AS min$0)]
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