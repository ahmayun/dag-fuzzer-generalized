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

autonode_9 = table_env.from_path("inventory").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("inventory").get_schema().get_field_names()])
autonode_8 = table_env.from_path("catalog_returns").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("catalog_returns").get_schema().get_field_names()])
autonode_7 = autonode_9.limit(73)
autonode_6 = autonode_8.limit(45)
autonode_5 = autonode_7.alias('YAXrD')
autonode_4 = autonode_6.order_by(col('cr_warehouse_sk_node_8'))
autonode_3 = autonode_4.join(autonode_5, col('inv_warehouse_sk_node_9') == col('cr_warehouse_sk_node_8'))
autonode_2 = autonode_3.group_by(col('cr_reversed_charge_node_8')).select(col('cr_return_quantity_node_8').avg.alias('cr_return_quantity_node_8'))
autonode_1 = autonode_2.add_columns(lit("hello"))
sink = autonode_1.select(col('cr_return_quantity_node_8'))
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
LogicalProject(cr_return_quantity_node_8=[$1])
+- LogicalAggregate(group=[{24}], EXPR$0=[AVG($17)])
   +- LogicalJoin(condition=[=($29, $14)], joinType=[inner])
      :- LogicalSort(sort0=[$14], dir0=[ASC])
      :  +- LogicalSort(fetch=[45])
      :     +- LogicalProject(cr_returned_date_sk_node_8=[$0], cr_returned_time_sk_node_8=[$1], cr_item_sk_node_8=[$2], cr_refunded_customer_sk_node_8=[$3], cr_refunded_cdemo_sk_node_8=[$4], cr_refunded_hdemo_sk_node_8=[$5], cr_refunded_addr_sk_node_8=[$6], cr_returning_customer_sk_node_8=[$7], cr_returning_cdemo_sk_node_8=[$8], cr_returning_hdemo_sk_node_8=[$9], cr_returning_addr_sk_node_8=[$10], cr_call_center_sk_node_8=[$11], cr_catalog_page_sk_node_8=[$12], cr_ship_mode_sk_node_8=[$13], cr_warehouse_sk_node_8=[$14], cr_reason_sk_node_8=[$15], cr_order_number_node_8=[$16], cr_return_quantity_node_8=[$17], cr_return_amount_node_8=[$18], cr_return_tax_node_8=[$19], cr_return_amt_inc_tax_node_8=[$20], cr_fee_node_8=[$21], cr_return_ship_cost_node_8=[$22], cr_refunded_cash_node_8=[$23], cr_reversed_charge_node_8=[$24], cr_store_credit_node_8=[$25], cr_net_loss_node_8=[$26])
      :        +- LogicalTableScan(table=[[default_catalog, default_database, catalog_returns]])
      +- LogicalProject(YAXrD=[AS($0, _UTF-16LE'YAXrD')], inv_item_sk_node_9=[$1], inv_warehouse_sk_node_9=[$2], inv_quantity_on_hand_node_9=[$3])
         +- LogicalSort(fetch=[73])
            +- LogicalProject(inv_date_sk_node_9=[$0], inv_item_sk_node_9=[$1], inv_warehouse_sk_node_9=[$2], inv_quantity_on_hand_node_9=[$3])
               +- LogicalTableScan(table=[[default_catalog, default_database, inventory]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS cr_return_quantity_node_8])
+- HashAggregate(isMerge=[false], groupBy=[cr_reversed_charge], select=[cr_reversed_charge, AVG(cr_return_quantity) AS EXPR$0])
   +- Exchange(distribution=[hash[cr_reversed_charge]])
      +- HashJoin(joinType=[InnerJoin], where=[=(inv_warehouse_sk_node_9, cr_warehouse_sk)], select=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss, YAXrD, inv_item_sk_node_9, inv_warehouse_sk_node_9, inv_quantity_on_hand_node_9], isBroadcast=[true], build=[right])
         :- Sort(orderBy=[cr_warehouse_sk ASC])
         :  +- Limit(offset=[0], fetch=[45], global=[true])
         :     +- Exchange(distribution=[single])
         :        +- Limit(offset=[0], fetch=[45], global=[false])
         :           +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns, limit=[45]]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])
         +- Exchange(distribution=[broadcast])
            +- Calc(select=[inv_date_sk AS YAXrD, inv_item_sk AS inv_item_sk_node_9, inv_warehouse_sk AS inv_warehouse_sk_node_9, inv_quantity_on_hand AS inv_quantity_on_hand_node_9])
               +- Limit(offset=[0], fetch=[73], global=[true])
                  +- Exchange(distribution=[single])
                     +- Limit(offset=[0], fetch=[73], global=[false])
                        +- TableSourceScan(table=[[default_catalog, default_database, inventory, limit=[73]]], fields=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS cr_return_quantity_node_8])
+- HashAggregate(isMerge=[false], groupBy=[cr_reversed_charge], select=[cr_reversed_charge, AVG(cr_return_quantity) AS EXPR$0])
   +- Exchange(distribution=[hash[cr_reversed_charge]])
      +- HashJoin(joinType=[InnerJoin], where=[(inv_warehouse_sk_node_9 = cr_warehouse_sk)], select=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss, YAXrD, inv_item_sk_node_9, inv_warehouse_sk_node_9, inv_quantity_on_hand_node_9], isBroadcast=[true], build=[right])
         :- Sort(orderBy=[cr_warehouse_sk ASC])
         :  +- Limit(offset=[0], fetch=[45], global=[true])
         :     +- Exchange(distribution=[single])
         :        +- Limit(offset=[0], fetch=[45], global=[false])
         :           +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns, limit=[45]]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])
         +- Exchange(distribution=[broadcast])
            +- Calc(select=[inv_date_sk AS YAXrD, inv_item_sk AS inv_item_sk_node_9, inv_warehouse_sk AS inv_warehouse_sk_node_9, inv_quantity_on_hand AS inv_quantity_on_hand_node_9])
               +- Limit(offset=[0], fetch=[73], global=[true])
                  +- Exchange(distribution=[single])
                     +- Limit(offset=[0], fetch=[73], global=[false])
                        +- TableSourceScan(table=[[default_catalog, default_database, inventory, limit=[73]]], fields=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o131176857.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#265187938:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[14](input=RelSubset#265187936,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[14]), rel#265187935:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[14](input=RelSubset#265187934,groupBy=cr_warehouse_sk, cr_reversed_charge,select=cr_warehouse_sk, cr_reversed_charge, Partial_SUM(cr_return_quantity) AS sum$0, Partial_COUNT(cr_return_quantity) AS count$1)]
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