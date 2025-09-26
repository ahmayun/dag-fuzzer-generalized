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

autonode_6 = table_env.from_path("store_returns").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("store_returns").get_schema().get_field_names()])
autonode_5 = table_env.from_path("catalog_returns").select(*[col(column_name).alias(f"{column_name}_node_5") for column_name in table_env.from_path("catalog_returns").get_schema().get_field_names()])
autonode_4 = autonode_6.select(col('sr_fee_node_6'))
autonode_3 = autonode_5.order_by(col('cr_warehouse_sk_node_5'))
autonode_2 = autonode_3.join(autonode_4, col('cr_return_amt_inc_tax_node_5') == col('sr_fee_node_6'))
autonode_1 = autonode_2.group_by(col('cr_fee_node_5')).select(col('cr_refunded_hdemo_sk_node_5').max.alias('cr_refunded_hdemo_sk_node_5'))
sink = autonode_1.order_by(col('cr_refunded_hdemo_sk_node_5'))
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
LogicalSort(sort0=[$0], dir0=[ASC])
+- LogicalProject(cr_refunded_hdemo_sk_node_5=[$1])
   +- LogicalAggregate(group=[{21}], EXPR$0=[MAX($5)])
      +- LogicalJoin(condition=[=($20, $27)], joinType=[inner])
         :- LogicalSort(sort0=[$14], dir0=[ASC])
         :  +- LogicalProject(cr_returned_date_sk_node_5=[$0], cr_returned_time_sk_node_5=[$1], cr_item_sk_node_5=[$2], cr_refunded_customer_sk_node_5=[$3], cr_refunded_cdemo_sk_node_5=[$4], cr_refunded_hdemo_sk_node_5=[$5], cr_refunded_addr_sk_node_5=[$6], cr_returning_customer_sk_node_5=[$7], cr_returning_cdemo_sk_node_5=[$8], cr_returning_hdemo_sk_node_5=[$9], cr_returning_addr_sk_node_5=[$10], cr_call_center_sk_node_5=[$11], cr_catalog_page_sk_node_5=[$12], cr_ship_mode_sk_node_5=[$13], cr_warehouse_sk_node_5=[$14], cr_reason_sk_node_5=[$15], cr_order_number_node_5=[$16], cr_return_quantity_node_5=[$17], cr_return_amount_node_5=[$18], cr_return_tax_node_5=[$19], cr_return_amt_inc_tax_node_5=[$20], cr_fee_node_5=[$21], cr_return_ship_cost_node_5=[$22], cr_refunded_cash_node_5=[$23], cr_reversed_charge_node_5=[$24], cr_store_credit_node_5=[$25], cr_net_loss_node_5=[$26])
         :     +- LogicalTableScan(table=[[default_catalog, default_database, catalog_returns]])
         +- LogicalProject(sr_fee_node_6=[$14])
            +- LogicalTableScan(table=[[default_catalog, default_database, store_returns]])

== Optimized Physical Plan ==
Sort(orderBy=[cr_refunded_hdemo_sk_node_5 ASC])
+- Exchange(distribution=[single])
   +- Calc(select=[EXPR$0 AS cr_refunded_hdemo_sk_node_5])
      +- HashAggregate(isMerge=[true], groupBy=[cr_fee], select=[cr_fee, Final_MAX(max$0) AS EXPR$0])
         +- Exchange(distribution=[hash[cr_fee]])
            +- LocalHashAggregate(groupBy=[cr_fee], select=[cr_fee, Partial_MAX(cr_refunded_hdemo_sk) AS max$0])
               +- NestedLoopJoin(joinType=[InnerJoin], where=[=(cr_return_amt_inc_tax, sr_fee)], select=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss, sr_fee], build=[right])
                  :- Sort(orderBy=[cr_warehouse_sk ASC])
                  :  +- Exchange(distribution=[single])
                  :     +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])
                  +- Exchange(distribution=[broadcast])
                     +- TableSourceScan(table=[[default_catalog, default_database, store_returns, project=[sr_fee], metadata=[]]], fields=[sr_fee])

== Optimized Execution Plan ==
Sort(orderBy=[cr_refunded_hdemo_sk_node_5 ASC])
+- Exchange(distribution=[single])
   +- Calc(select=[EXPR$0 AS cr_refunded_hdemo_sk_node_5])
      +- HashAggregate(isMerge=[true], groupBy=[cr_fee], select=[cr_fee, Final_MAX(max$0) AS EXPR$0])
         +- Exchange(distribution=[hash[cr_fee]])
            +- LocalHashAggregate(groupBy=[cr_fee], select=[cr_fee, Partial_MAX(cr_refunded_hdemo_sk) AS max$0])
               +- NestedLoopJoin(joinType=[InnerJoin], where=[(cr_return_amt_inc_tax = sr_fee)], select=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss, sr_fee], build=[right])
                  :- Sort(orderBy=[cr_warehouse_sk ASC])
                  :  +- Exchange(distribution=[single])
                  :     +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])
                  +- Exchange(distribution=[broadcast])
                     +- TableSourceScan(table=[[default_catalog, default_database, store_returns, project=[sr_fee], metadata=[]]], fields=[sr_fee])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o307819696.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#620505171:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[14](input=RelSubset#620505169,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[14]), rel#620505168:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[14](input=RelSubset#620505167,groupBy=cr_return_amt_inc_tax, cr_fee,select=cr_return_amt_inc_tax, cr_fee, Partial_MAX(cr_refunded_hdemo_sk) AS max$0)]
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