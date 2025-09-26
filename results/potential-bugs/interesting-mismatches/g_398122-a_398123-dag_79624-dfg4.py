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
    return values.std()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_4 = table_env.from_path("web_returns").select(*[col(column_name).alias(f"{column_name}_node_4") for column_name in table_env.from_path("web_returns").get_schema().get_field_names()])
autonode_5 = table_env.from_path("web_returns").select(*[col(column_name).alias(f"{column_name}_node_5") for column_name in table_env.from_path("web_returns").get_schema().get_field_names()])
autonode_2 = autonode_4.distinct()
autonode_3 = autonode_5.order_by(col('wr_return_amt_inc_tax_node_5'))
autonode_1 = autonode_2.join(autonode_3, col('wr_web_page_sk_node_5') == col('wr_refunded_cdemo_sk_node_4'))
sink = autonode_1.group_by(col('wr_account_credit_node_5')).select(col('wr_item_sk_node_5').max.alias('wr_item_sk_node_5'))
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
LogicalProject(wr_item_sk_node_5=[$1])
+- LogicalAggregate(group=[{46}], EXPR$0=[MAX($26)])
   +- LogicalJoin(condition=[=($35, $4)], joinType=[inner])
      :- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23}])
      :  +- LogicalProject(wr_returned_date_sk_node_4=[$0], wr_returned_time_sk_node_4=[$1], wr_item_sk_node_4=[$2], wr_refunded_customer_sk_node_4=[$3], wr_refunded_cdemo_sk_node_4=[$4], wr_refunded_hdemo_sk_node_4=[$5], wr_refunded_addr_sk_node_4=[$6], wr_returning_customer_sk_node_4=[$7], wr_returning_cdemo_sk_node_4=[$8], wr_returning_hdemo_sk_node_4=[$9], wr_returning_addr_sk_node_4=[$10], wr_web_page_sk_node_4=[$11], wr_reason_sk_node_4=[$12], wr_order_number_node_4=[$13], wr_return_quantity_node_4=[$14], wr_return_amt_node_4=[$15], wr_return_tax_node_4=[$16], wr_return_amt_inc_tax_node_4=[$17], wr_fee_node_4=[$18], wr_return_ship_cost_node_4=[$19], wr_refunded_cash_node_4=[$20], wr_reversed_charge_node_4=[$21], wr_account_credit_node_4=[$22], wr_net_loss_node_4=[$23])
      :     +- LogicalTableScan(table=[[default_catalog, default_database, web_returns]])
      +- LogicalSort(sort0=[$17], dir0=[ASC])
         +- LogicalProject(wr_returned_date_sk_node_5=[$0], wr_returned_time_sk_node_5=[$1], wr_item_sk_node_5=[$2], wr_refunded_customer_sk_node_5=[$3], wr_refunded_cdemo_sk_node_5=[$4], wr_refunded_hdemo_sk_node_5=[$5], wr_refunded_addr_sk_node_5=[$6], wr_returning_customer_sk_node_5=[$7], wr_returning_cdemo_sk_node_5=[$8], wr_returning_hdemo_sk_node_5=[$9], wr_returning_addr_sk_node_5=[$10], wr_web_page_sk_node_5=[$11], wr_reason_sk_node_5=[$12], wr_order_number_node_5=[$13], wr_return_quantity_node_5=[$14], wr_return_amt_node_5=[$15], wr_return_tax_node_5=[$16], wr_return_amt_inc_tax_node_5=[$17], wr_fee_node_5=[$18], wr_return_ship_cost_node_5=[$19], wr_refunded_cash_node_5=[$20], wr_reversed_charge_node_5=[$21], wr_account_credit_node_5=[$22], wr_net_loss_node_5=[$23])
            +- LogicalTableScan(table=[[default_catalog, default_database, web_returns]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS wr_item_sk_node_5])
+- HashAggregate(isMerge=[false], groupBy=[wr_account_credit0], select=[wr_account_credit0, MAX(wr_item_sk0) AS EXPR$0])
   +- Exchange(distribution=[hash[wr_account_credit0]])
      +- NestedLoopJoin(joinType=[InnerJoin], where=[=(wr_web_page_sk0, wr_refunded_cdemo_sk)], select=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss, wr_returned_date_sk0, wr_returned_time_sk0, wr_item_sk0, wr_refunded_customer_sk0, wr_refunded_cdemo_sk0, wr_refunded_hdemo_sk0, wr_refunded_addr_sk0, wr_returning_customer_sk0, wr_returning_cdemo_sk0, wr_returning_hdemo_sk0, wr_returning_addr_sk0, wr_web_page_sk0, wr_reason_sk0, wr_order_number0, wr_return_quantity0, wr_return_amt0, wr_return_tax0, wr_return_amt_inc_tax0, wr_fee0, wr_return_ship_cost0, wr_refunded_cash0, wr_reversed_charge0, wr_account_credit0, wr_net_loss0], build=[left])
         :- Exchange(distribution=[broadcast])
         :  +- HashAggregate(isMerge=[false], groupBy=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], select=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])
         :     +- Exchange(distribution=[hash[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss]])
         :        +- TableSourceScan(table=[[default_catalog, default_database, web_returns]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])
         +- Sort(orderBy=[wr_return_amt_inc_tax ASC])
            +- Exchange(distribution=[single])
               +- TableSourceScan(table=[[default_catalog, default_database, web_returns]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS wr_item_sk_node_5])
+- HashAggregate(isMerge=[false], groupBy=[wr_account_credit0], select=[wr_account_credit0, MAX(wr_item_sk0) AS EXPR$0])
   +- Exchange(distribution=[hash[wr_account_credit0]])
      +- NestedLoopJoin(joinType=[InnerJoin], where=[(wr_web_page_sk0 = wr_refunded_cdemo_sk)], select=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss, wr_returned_date_sk0, wr_returned_time_sk0, wr_item_sk0, wr_refunded_customer_sk0, wr_refunded_cdemo_sk0, wr_refunded_hdemo_sk0, wr_refunded_addr_sk0, wr_returning_customer_sk0, wr_returning_cdemo_sk0, wr_returning_hdemo_sk0, wr_returning_addr_sk0, wr_web_page_sk0, wr_reason_sk0, wr_order_number0, wr_return_quantity0, wr_return_amt0, wr_return_tax0, wr_return_amt_inc_tax0, wr_fee0, wr_return_ship_cost0, wr_refunded_cash0, wr_reversed_charge0, wr_account_credit0, wr_net_loss0], build=[left])
         :- Exchange(distribution=[broadcast])
         :  +- HashAggregate(isMerge=[false], groupBy=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], select=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])
         :     +- Exchange(distribution=[hash[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss]])
         :        +- TableSourceScan(table=[[default_catalog, default_database, web_returns]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])(reuse_id=[1])
         +- Sort(orderBy=[wr_return_amt_inc_tax ASC])
            +- Exchange(distribution=[single])
               +- Reused(reference_id=[1])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o216697958.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#438215992:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[17](input=RelSubset#438215990,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[17]), rel#438215989:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[17](input=RelSubset#438215988,groupBy=wr_web_page_sk, wr_account_credit,select=wr_web_page_sk, wr_account_credit, Partial_MAX(wr_item_sk) AS max$0)]
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