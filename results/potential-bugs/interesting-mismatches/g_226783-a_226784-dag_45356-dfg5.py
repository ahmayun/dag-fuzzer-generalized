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
    import numpy as np
    return np.exp(np.log(values[values > 0]).mean()) if (values > 0).all() else None


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_6 = table_env.from_path("catalog_returns").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("catalog_returns").get_schema().get_field_names()])
autonode_7 = table_env.from_path("inventory").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("inventory").get_schema().get_field_names()])
autonode_4 = autonode_6.group_by(col('cr_reversed_charge_node_6')).select(col('cr_returning_cdemo_sk_node_6').avg.alias('cr_returning_cdemo_sk_node_6'))
autonode_5 = autonode_7.add_columns(lit("hello"))
autonode_2 = autonode_4.add_columns(lit("hello"))
autonode_3 = autonode_5.order_by(col('inv_quantity_on_hand_node_7'))
autonode_1 = autonode_2.join(autonode_3, col('cr_returning_cdemo_sk_node_6') == col('inv_warehouse_sk_node_7'))
sink = autonode_1.group_by(col('inv_quantity_on_hand_node_7')).select(col('inv_warehouse_sk_node_7').max.alias('inv_warehouse_sk_node_7'))
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
LogicalProject(inv_warehouse_sk_node_7=[$1])
+- LogicalAggregate(group=[{5}], EXPR$0=[MAX($4)])
   +- LogicalJoin(condition=[=($0, $4)], joinType=[inner])
      :- LogicalProject(cr_returning_cdemo_sk_node_6=[$1], _c1=[_UTF-16LE'hello'])
      :  +- LogicalAggregate(group=[{1}], EXPR$0=[AVG($0)])
      :     +- LogicalProject(cr_returning_cdemo_sk_node_6=[$8], cr_reversed_charge_node_6=[$24])
      :        +- LogicalTableScan(table=[[default_catalog, default_database, catalog_returns]])
      +- LogicalSort(sort0=[$3], dir0=[ASC])
         +- LogicalProject(inv_date_sk_node_7=[$0], inv_item_sk_node_7=[$1], inv_warehouse_sk_node_7=[$2], inv_quantity_on_hand_node_7=[$3], _c4=[_UTF-16LE'hello'])
            +- LogicalTableScan(table=[[default_catalog, default_database, inventory]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS inv_warehouse_sk_node_7])
+- HashAggregate(isMerge=[true], groupBy=[inv_quantity_on_hand_node_7], select=[inv_quantity_on_hand_node_7, Final_MAX(max$0) AS EXPR$0])
   +- Exchange(distribution=[hash[inv_quantity_on_hand_node_7]])
      +- LocalHashAggregate(groupBy=[inv_quantity_on_hand_node_7], select=[inv_quantity_on_hand_node_7, Partial_MAX(inv_warehouse_sk_node_7) AS max$0])
         +- HashJoin(joinType=[InnerJoin], where=[=(cr_returning_cdemo_sk_node_6, inv_warehouse_sk_node_7)], select=[cr_returning_cdemo_sk_node_6, _c1, inv_date_sk_node_7, inv_item_sk_node_7, inv_warehouse_sk_node_7, inv_quantity_on_hand_node_7, _c4], isBroadcast=[true], build=[left])
            :- Exchange(distribution=[broadcast])
            :  +- Calc(select=[EXPR$0 AS cr_returning_cdemo_sk_node_6, 'hello' AS _c1])
            :     +- HashAggregate(isMerge=[true], groupBy=[cr_reversed_charge], select=[cr_reversed_charge, Final_AVG(sum$0, count$1) AS EXPR$0])
            :        +- Exchange(distribution=[hash[cr_reversed_charge]])
            :           +- LocalHashAggregate(groupBy=[cr_reversed_charge], select=[cr_reversed_charge, Partial_AVG(cr_returning_cdemo_sk) AS (sum$0, count$1)])
            :              +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns, project=[cr_returning_cdemo_sk, cr_reversed_charge], metadata=[]]], fields=[cr_returning_cdemo_sk, cr_reversed_charge])
            +- Sort(orderBy=[inv_quantity_on_hand_node_7 ASC])
               +- Calc(select=[inv_date_sk AS inv_date_sk_node_7, inv_item_sk AS inv_item_sk_node_7, inv_warehouse_sk AS inv_warehouse_sk_node_7, inv_quantity_on_hand AS inv_quantity_on_hand_node_7, 'hello' AS _c4])
                  +- Exchange(distribution=[single])
                     +- TableSourceScan(table=[[default_catalog, default_database, inventory]], fields=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS inv_warehouse_sk_node_7])
+- HashAggregate(isMerge=[true], groupBy=[inv_quantity_on_hand_node_7], select=[inv_quantity_on_hand_node_7, Final_MAX(max$0) AS EXPR$0])
   +- Exchange(distribution=[hash[inv_quantity_on_hand_node_7]])
      +- LocalHashAggregate(groupBy=[inv_quantity_on_hand_node_7], select=[inv_quantity_on_hand_node_7, Partial_MAX(inv_warehouse_sk_node_7) AS max$0])
         +- HashJoin(joinType=[InnerJoin], where=[(cr_returning_cdemo_sk_node_6 = inv_warehouse_sk_node_7)], select=[cr_returning_cdemo_sk_node_6, _c1, inv_date_sk_node_7, inv_item_sk_node_7, inv_warehouse_sk_node_7, inv_quantity_on_hand_node_7, _c4], isBroadcast=[true], build=[left])
            :- Exchange(distribution=[broadcast])
            :  +- Calc(select=[EXPR$0 AS cr_returning_cdemo_sk_node_6, 'hello' AS _c1])
            :     +- HashAggregate(isMerge=[true], groupBy=[cr_reversed_charge], select=[cr_reversed_charge, Final_AVG(sum$0, count$1) AS EXPR$0])
            :        +- Exchange(distribution=[hash[cr_reversed_charge]])
            :           +- LocalHashAggregate(groupBy=[cr_reversed_charge], select=[cr_reversed_charge, Partial_AVG(cr_returning_cdemo_sk) AS (sum$0, count$1)])
            :              +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns, project=[cr_returning_cdemo_sk, cr_reversed_charge], metadata=[]]], fields=[cr_returning_cdemo_sk, cr_reversed_charge])
            +- Sort(orderBy=[inv_quantity_on_hand_node_7 ASC])
               +- Calc(select=[inv_date_sk AS inv_date_sk_node_7, inv_item_sk AS inv_item_sk_node_7, inv_warehouse_sk AS inv_warehouse_sk_node_7, inv_quantity_on_hand AS inv_quantity_on_hand_node_7, 'hello' AS _c4])
                  +- Exchange(distribution=[single])
                     +- TableSourceScan(table=[[default_catalog, default_database, inventory]], fields=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o123599111.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#249355363:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[3](input=RelSubset#249355361,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[3]), rel#249355360:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[3](input=RelSubset#249355359,groupBy=inv_warehouse_sk, inv_quantity_on_hand,select=inv_warehouse_sk, inv_quantity_on_hand, Partial_MAX(inv_warehouse_sk) AS max$0)]
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