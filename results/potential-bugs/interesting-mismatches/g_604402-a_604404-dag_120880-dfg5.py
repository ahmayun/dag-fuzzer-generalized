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
    return values.iloc[0] if len(values) > 0 else None


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_6 = table_env.from_path("customer_address").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("customer_address").get_schema().get_field_names()])
autonode_5 = table_env.from_path("time_dim").select(*[col(column_name).alias(f"{column_name}_node_5") for column_name in table_env.from_path("time_dim").get_schema().get_field_names()])
autonode_4 = autonode_6.distinct()
autonode_3 = autonode_5.order_by(col('t_sub_shift_node_5'))
autonode_2 = autonode_3.join(autonode_4, col('t_am_pm_node_5') == col('ca_street_name_node_6'))
autonode_1 = autonode_2.filter(col('t_shift_node_5').char_length > 5)
sink = autonode_1.group_by(col('t_time_node_5')).select(col('t_time_node_5').max.alias('t_time_node_5'))
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
LogicalProject(t_time_node_5=[$1])
+- LogicalAggregate(group=[{2}], EXPR$0=[MAX($2)])
   +- LogicalFilter(condition=[>(CHAR_LENGTH($7), 5)])
      +- LogicalJoin(condition=[=($6, $13)], joinType=[inner])
         :- LogicalSort(sort0=[$8], dir0=[ASC])
         :  +- LogicalProject(t_time_sk_node_5=[$0], t_time_id_node_5=[$1], t_time_node_5=[$2], t_hour_node_5=[$3], t_minute_node_5=[$4], t_second_node_5=[$5], t_am_pm_node_5=[$6], t_shift_node_5=[$7], t_sub_shift_node_5=[$8], t_meal_time_node_5=[$9])
         :     +- LogicalTableScan(table=[[default_catalog, default_database, time_dim]])
         +- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}])
            +- LogicalProject(ca_address_sk_node_6=[$0], ca_address_id_node_6=[$1], ca_street_number_node_6=[$2], ca_street_name_node_6=[$3], ca_street_type_node_6=[$4], ca_suite_number_node_6=[$5], ca_city_node_6=[$6], ca_county_node_6=[$7], ca_state_node_6=[$8], ca_zip_node_6=[$9], ca_country_node_6=[$10], ca_gmt_offset_node_6=[$11], ca_location_type_node_6=[$12])
               +- LogicalTableScan(table=[[default_catalog, default_database, customer_address]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS t_time_node_5])
+- HashAggregate(isMerge=[true], groupBy=[t_time], select=[t_time, Final_MAX(max$0) AS EXPR$0])
   +- Exchange(distribution=[hash[t_time]])
      +- LocalHashAggregate(groupBy=[t_time], select=[t_time, Partial_MAX(t_time) AS max$0])
         +- HashJoin(joinType=[InnerJoin], where=[=(t_am_pm, ca_street_name)], select=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time, ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type], build=[left])
            :- Exchange(distribution=[hash[t_am_pm]])
            :  +- Calc(select=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time], where=[>(CHAR_LENGTH(t_shift), 5)])
            :     +- Sort(orderBy=[t_sub_shift ASC])
            :        +- Exchange(distribution=[single])
            :           +- TableSourceScan(table=[[default_catalog, default_database, time_dim]], fields=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time])
            +- Exchange(distribution=[hash[ca_street_name]])
               +- HashAggregate(isMerge=[false], groupBy=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type], select=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])
                  +- Exchange(distribution=[hash[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type]])
                     +- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS t_time_node_5])
+- HashAggregate(isMerge=[true], groupBy=[t_time], select=[t_time, Final_MAX(max$0) AS EXPR$0])
   +- Exchange(distribution=[hash[t_time]])
      +- LocalHashAggregate(groupBy=[t_time], select=[t_time, Partial_MAX(t_time) AS max$0])
         +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(t_am_pm = ca_street_name)], select=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time, ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type], build=[left])
            :- Exchange(distribution=[hash[t_am_pm]])
            :  +- Calc(select=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time], where=[(CHAR_LENGTH(t_shift) > 5)])
            :     +- Sort(orderBy=[t_sub_shift ASC])
            :        +- Exchange(distribution=[single])
            :           +- TableSourceScan(table=[[default_catalog, default_database, time_dim]], fields=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time])
            +- Exchange(distribution=[hash[ca_street_name]])
               +- HashAggregate(isMerge=[false], groupBy=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type], select=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])
                  +- Exchange(distribution=[hash[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type]])
                     +- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o329538221.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#664844566:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[8](input=RelSubset#664844564,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[8]), rel#664844563:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[8](input=RelSubset#664844562,groupBy=t_time, t_am_pm,select=t_time, t_am_pm, Partial_MAX(t_time) AS max$0)]
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