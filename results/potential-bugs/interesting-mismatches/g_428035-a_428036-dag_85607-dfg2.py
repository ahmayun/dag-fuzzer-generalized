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
    return values.iloc[-1] if len(values) > 0 else None


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_6 = table_env.from_path("income_band").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("income_band").get_schema().get_field_names()])
autonode_5 = table_env.from_path("promotion").select(*[col(column_name).alias(f"{column_name}_node_5") for column_name in table_env.from_path("promotion").get_schema().get_field_names()])
autonode_4 = autonode_6.alias('c79Z2')
autonode_3 = autonode_5.order_by(col('p_channel_catalog_node_5'))
autonode_2 = autonode_3.join(autonode_4, col('ib_upper_bound_node_6') == col('p_end_date_sk_node_5'))
autonode_1 = autonode_2.group_by(col('p_channel_press_node_5')).select(col('p_item_sk_node_5').min.alias('p_item_sk_node_5'))
sink = autonode_1.group_by(col('p_item_sk_node_5')).select(col('p_item_sk_node_5').min.alias('p_item_sk_node_5'))
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
LogicalProject(p_item_sk_node_5=[$1])
+- LogicalAggregate(group=[{0}], EXPR$0=[MIN($0)])
   +- LogicalProject(p_item_sk_node_5=[$1])
      +- LogicalAggregate(group=[{13}], EXPR$0=[MIN($4)])
         +- LogicalJoin(condition=[=($21, $3)], joinType=[inner])
            :- LogicalSort(sort0=[$10], dir0=[ASC])
            :  +- LogicalProject(p_promo_sk_node_5=[$0], p_promo_id_node_5=[$1], p_start_date_sk_node_5=[$2], p_end_date_sk_node_5=[$3], p_item_sk_node_5=[$4], p_cost_node_5=[$5], p_response_target_node_5=[$6], p_promo_name_node_5=[$7], p_channel_dmail_node_5=[$8], p_channel_email_node_5=[$9], p_channel_catalog_node_5=[$10], p_channel_tv_node_5=[$11], p_channel_radio_node_5=[$12], p_channel_press_node_5=[$13], p_channel_event_node_5=[$14], p_channel_demo_node_5=[$15], p_channel_details_node_5=[$16], p_purpose_node_5=[$17], p_discount_active_node_5=[$18])
            :     +- LogicalTableScan(table=[[default_catalog, default_database, promotion]])
            +- LogicalProject(c79Z2=[AS($0, _UTF-16LE'c79Z2')], ib_lower_bound_node_6=[$1], ib_upper_bound_node_6=[$2])
               +- LogicalTableScan(table=[[default_catalog, default_database, income_band]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS p_item_sk_node_5])
+- HashAggregate(isMerge=[true], groupBy=[p_item_sk_node_5], select=[p_item_sk_node_5, Final_MIN(min$0) AS EXPR$0])
   +- Exchange(distribution=[hash[p_item_sk_node_5]])
      +- LocalHashAggregate(groupBy=[p_item_sk_node_5], select=[p_item_sk_node_5, Partial_MIN(p_item_sk_node_5) AS min$0])
         +- Calc(select=[EXPR$0 AS p_item_sk_node_5])
            +- HashAggregate(isMerge=[true], groupBy=[p_channel_press], select=[p_channel_press, Final_MIN(min$0) AS EXPR$0])
               +- Exchange(distribution=[hash[p_channel_press]])
                  +- LocalHashAggregate(groupBy=[p_channel_press], select=[p_channel_press, Partial_MIN(p_item_sk) AS min$0])
                     +- HashJoin(joinType=[InnerJoin], where=[=(ib_upper_bound_node_6, p_end_date_sk)], select=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active, c79Z2, ib_lower_bound_node_6, ib_upper_bound_node_6], isBroadcast=[true], build=[right])
                        :- Sort(orderBy=[p_channel_catalog ASC])
                        :  +- Exchange(distribution=[single])
                        :     +- TableSourceScan(table=[[default_catalog, default_database, promotion]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])
                        +- Exchange(distribution=[broadcast])
                           +- Calc(select=[ib_income_band_sk AS c79Z2, ib_lower_bound AS ib_lower_bound_node_6, ib_upper_bound AS ib_upper_bound_node_6])
                              +- TableSourceScan(table=[[default_catalog, default_database, income_band]], fields=[ib_income_band_sk, ib_lower_bound, ib_upper_bound])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS p_item_sk_node_5])
+- HashAggregate(isMerge=[true], groupBy=[p_item_sk_node_5], select=[p_item_sk_node_5, Final_MIN(min$0) AS EXPR$0])
   +- Exchange(distribution=[hash[p_item_sk_node_5]])
      +- LocalHashAggregate(groupBy=[p_item_sk_node_5], select=[p_item_sk_node_5, Partial_MIN(p_item_sk_node_5) AS min$0])
         +- Calc(select=[EXPR$0 AS p_item_sk_node_5])
            +- HashAggregate(isMerge=[true], groupBy=[p_channel_press], select=[p_channel_press, Final_MIN(min$0) AS EXPR$0])
               +- Exchange(distribution=[hash[p_channel_press]])
                  +- LocalHashAggregate(groupBy=[p_channel_press], select=[p_channel_press, Partial_MIN(p_item_sk) AS min$0])
                     +- HashJoin(joinType=[InnerJoin], where=[(ib_upper_bound_node_6 = p_end_date_sk)], select=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active, c79Z2, ib_lower_bound_node_6, ib_upper_bound_node_6], isBroadcast=[true], build=[right])
                        :- Sort(orderBy=[p_channel_catalog ASC])
                        :  +- Exchange(distribution=[single])
                        :     +- TableSourceScan(table=[[default_catalog, default_database, promotion]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])
                        +- Exchange(distribution=[broadcast])
                           +- Calc(select=[ib_income_band_sk AS c79Z2, ib_lower_bound AS ib_lower_bound_node_6, ib_upper_bound AS ib_upper_bound_node_6])
                              +- TableSourceScan(table=[[default_catalog, default_database, income_band]], fields=[ib_income_band_sk, ib_lower_bound, ib_upper_bound])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o233106122.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#471312740:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[10](input=RelSubset#471312738,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[10]), rel#471312737:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[10](input=RelSubset#471312736,groupBy=p_end_date_sk, p_channel_press,select=p_end_date_sk, p_channel_press, Partial_MIN(p_item_sk) AS min$0)]
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