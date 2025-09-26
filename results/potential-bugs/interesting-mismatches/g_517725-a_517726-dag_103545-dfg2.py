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


def preloaded_aggregation(values: pd.Series) -> int:
    return len(values)


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_10 = table_env.from_path("time_dim").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("time_dim").get_schema().get_field_names()])
autonode_12 = table_env.from_path("customer_demographics").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("customer_demographics").get_schema().get_field_names()])
autonode_11 = table_env.from_path("inventory").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("inventory").get_schema().get_field_names()])
autonode_7 = autonode_10.distinct()
autonode_9 = autonode_12.select(col('cd_dep_employed_count_node_12'))
autonode_8 = autonode_11.order_by(col('inv_item_sk_node_11'))
autonode_6 = autonode_9.limit(98)
autonode_5 = autonode_7.join(autonode_8, col('t_time_node_10') == col('inv_warehouse_sk_node_11'))
autonode_4 = autonode_6.filter(preloaded_udf_boolean(col('cd_dep_employed_count_node_12')))
autonode_3 = autonode_5.group_by(col('t_hour_node_10')).select(col('t_hour_node_10').max.alias('t_hour_node_10'))
autonode_2 = autonode_3.join(autonode_4, col('t_hour_node_10') == col('cd_dep_employed_count_node_12'))
autonode_1 = autonode_2.alias('8PIud')
sink = autonode_1.alias('sdc7L')
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
      "error_message": "An error occurred while calling o282025000.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#570304633:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[1](input=RelSubset#570304631,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[1]), rel#570304630:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[1](input=RelSubset#570304629,groupBy=inv_warehouse_sk,select=inv_warehouse_sk)]
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
Caused by: java.lang.IndexOutOfBoundsException: index (1) must be less than size (1)
\tat org.apache.flink.calcite.shaded.com.google.common.base.Preconditions.checkElementIndex(Preconditions.java:1371)
\tat org.apache.flink.calcite.shaded.com.google.common.base.Preconditions.checkElementIndex(Preconditions.java:1353)
\tat org.apache.flink.calcite.shaded.com.google.common.collect.SingletonImmutableList.get(SingletonImmutableList.java:44)
\tat org.apache.calcite.util.Util$TransformingList.get(Util.java:2794)
\tat scala.collection.convert.Wrappers$JListWrapper.apply(Wrappers.scala:100)
\tat org.apache.flink.table.planner.plan.utils.RelExplainUtil$.$anonfun$collationToString$1(RelExplainUtil.scala:83)
\tat scala.collection.TraversableLike.$anonfun$map$1(TraversableLike.scala:286)
\tat scala.collection.Iterator.foreach(Iterator.scala:943)
\tat scala.collection.Iterator.foreach$(Iterator.scala:943)
\tat scala.collection.AbstractIterator.foreach(Iterator.scala:1431)
\tat scala.collection.IterableLike.foreach(IterableLike.scala:74)
\tat scala.collection.IterableLike.foreach$(IterableLike.scala:73)
\tat scala.collection.AbstractIterable.foreach(Iterable.scala:56)
\tat scala.collection.TraversableLike.map(TraversableLike.scala:286)
\tat scala.collection.TraversableLike.map$(TraversableLike.scala:279)
\tat scala.collection.AbstractTraversable.map(Traversable.scala:108)
\tat org.apache.flink.table.planner.plan.utils.RelExplainUtil$.collationToString(RelExplainUtil.scala:83)
\tat org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalSort.explainTerms(BatchPhysicalSort.scala:61)
\tat org.apache.calcite.rel.AbstractRelNode.getDigestItems(AbstractRelNode.java:414)
\tat org.apache.calcite.rel.AbstractRelNode.deepHashCode(AbstractRelNode.java:396)
\tat org.apache.calcite.rel.AbstractRelNode$InnerRelDigest.hashCode(AbstractRelNode.java:448)
\tat java.base/java.util.HashMap.hash(HashMap.java:340)
\tat java.base/java.util.HashMap.get(HashMap.java:553)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.registerImpl(VolcanoPlanner.java:1289)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.register(VolcanoPlanner.java:598)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.ensureRegistered(VolcanoPlanner.java:613)
\tat org.apache.calcite.plan.volcano.VolcanoRuleCall.transformTo(VolcanoRuleCall.java:144)
\t... 45 more
",
      "stdout": "",
      "stderr": ""
    },
    "unopt": {
      "success": true,
      "error_name": "",
      "error_message": "",
      "stdout": "== Abstract Syntax Tree ==
LogicalProject(sdc7L=[AS(AS($0, _UTF-16LE'8PIud'), _UTF-16LE'sdc7L')], cd_dep_employed_count_node_12=[$1])
+- LogicalJoin(condition=[=($0, $1)], joinType=[inner])
   :- LogicalProject(t_hour_node_10=[$1])
   :  +- LogicalAggregate(group=[{3}], EXPR$0=[MAX($3)])
   :     +- LogicalJoin(condition=[=($2, $12)], joinType=[inner])
   :        :- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}])
   :        :  +- LogicalProject(t_time_sk_node_10=[$0], t_time_id_node_10=[$1], t_time_node_10=[$2], t_hour_node_10=[$3], t_minute_node_10=[$4], t_second_node_10=[$5], t_am_pm_node_10=[$6], t_shift_node_10=[$7], t_sub_shift_node_10=[$8], t_meal_time_node_10=[$9])
   :        :     +- LogicalTableScan(table=[[default_catalog, default_database, time_dim]])
   :        +- LogicalSort(sort0=[$1], dir0=[ASC])
   :           +- LogicalProject(inv_date_sk_node_11=[$0], inv_item_sk_node_11=[$1], inv_warehouse_sk_node_11=[$2], inv_quantity_on_hand_node_11=[$3])
   :              +- LogicalTableScan(table=[[default_catalog, default_database, inventory]])
   +- LogicalFilter(condition=[*org.apache.flink.table.functions.python.PythonScalarFunction$2719bb62ba73d6c77acabda93ef50d3e*($0)])
      +- LogicalSort(fetch=[98])
         +- LogicalProject(cd_dep_employed_count_node_12=[$7])
            +- LogicalTableScan(table=[[default_catalog, default_database, customer_demographics]])

== Optimized Physical Plan ==
Calc(select=[t_hour_node_10 AS sdc7L, cd_dep_employed_count_node_12])
+- HashJoin(joinType=[InnerJoin], where=[=(t_hour_node_10, cd_dep_employed_count_node_12)], select=[t_hour_node_10, cd_dep_employed_count_node_12], isBroadcast=[true], build=[right])
   :- Calc(select=[EXPR$0 AS t_hour_node_10])
   :  +- HashAggregate(isMerge=[true], groupBy=[t_hour], select=[t_hour, Final_MAX(max$0) AS EXPR$0])
   :     +- Exchange(distribution=[hash[t_hour]])
   :        +- LocalHashAggregate(groupBy=[t_hour], select=[t_hour, Partial_MAX(t_hour) AS max$0])
   :           +- NestedLoopJoin(joinType=[InnerJoin], where=[=(t_time, inv_warehouse_sk)], select=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time, inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand], build=[right])
   :              :- HashAggregate(isMerge=[true], groupBy=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time], select=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time])
   :              :  +- Exchange(distribution=[hash[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time]])
   :              :     +- LocalHashAggregate(groupBy=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time], select=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time])
   :              :        +- TableSourceScan(table=[[default_catalog, default_database, time_dim]], fields=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time])
   :              +- Exchange(distribution=[broadcast])
   :                 +- SortLimit(orderBy=[inv_item_sk ASC], offset=[0], fetch=[1], global=[true])
   :                    +- Exchange(distribution=[single])
   :                       +- SortLimit(orderBy=[inv_item_sk ASC], offset=[0], fetch=[1], global=[false])
   :                          +- TableSourceScan(table=[[default_catalog, default_database, inventory]], fields=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand])
   +- Exchange(distribution=[broadcast])
      +- Calc(select=[cd_dep_employed_count AS cd_dep_employed_count_node_12], where=[f0])
         +- PythonCalc(select=[cd_dep_employed_count, *org.apache.flink.table.functions.python.PythonScalarFunction$2719bb62ba73d6c77acabda93ef50d3e*(cd_dep_employed_count) AS f0])
            +- Limit(offset=[0], fetch=[98], global=[true])
               +- Exchange(distribution=[single])
                  +- Limit(offset=[0], fetch=[98], global=[false])
                     +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics, project=[cd_dep_employed_count], metadata=[], limit=[98]]], fields=[cd_dep_employed_count])

== Optimized Execution Plan ==
Calc(select=[t_hour_node_10 AS sdc7L, cd_dep_employed_count_node_12])
+- HashJoin(joinType=[InnerJoin], where=[(t_hour_node_10 = cd_dep_employed_count_node_12)], select=[t_hour_node_10, cd_dep_employed_count_node_12], isBroadcast=[true], build=[right])
   :- Calc(select=[EXPR$0 AS t_hour_node_10])
   :  +- HashAggregate(isMerge=[true], groupBy=[t_hour], select=[t_hour, Final_MAX(max$0) AS EXPR$0])
   :     +- Exchange(distribution=[hash[t_hour]])
   :        +- LocalHashAggregate(groupBy=[t_hour], select=[t_hour, Partial_MAX(t_hour) AS max$0])
   :           +- NestedLoopJoin(joinType=[InnerJoin], where=[(t_time = inv_warehouse_sk)], select=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time, inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand], build=[right])
   :              :- HashAggregate(isMerge=[true], groupBy=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time], select=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time])
   :              :  +- Exchange(distribution=[hash[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time]])
   :              :     +- LocalHashAggregate(groupBy=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time], select=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time])
   :              :        +- TableSourceScan(table=[[default_catalog, default_database, time_dim]], fields=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time])
   :              +- Exchange(distribution=[broadcast])
   :                 +- SortLimit(orderBy=[inv_item_sk ASC], offset=[0], fetch=[1], global=[true])
   :                    +- Exchange(distribution=[single])
   :                       +- SortLimit(orderBy=[inv_item_sk ASC], offset=[0], fetch=[1], global=[false])
   :                          +- TableSourceScan(table=[[default_catalog, default_database, inventory]], fields=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand])
   +- Exchange(distribution=[broadcast])
      +- Calc(select=[cd_dep_employed_count AS cd_dep_employed_count_node_12], where=[f0])
         +- PythonCalc(select=[cd_dep_employed_count, *org.apache.flink.table.functions.python.PythonScalarFunction$2719bb62ba73d6c77acabda93ef50d3e*(cd_dep_employed_count) AS f0])
            +- Limit(offset=[0], fetch=[98], global=[true])
               +- Exchange(distribution=[single])
                  +- Limit(offset=[0], fetch=[98], global=[false])
                     +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics, project=[cd_dep_employed_count], metadata=[], limit=[98]]], fields=[cd_dep_employed_count])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0