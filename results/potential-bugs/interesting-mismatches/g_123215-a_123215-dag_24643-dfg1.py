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

autonode_6 = table_env.from_path("web_returns").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("web_returns").get_schema().get_field_names()])
autonode_7 = table_env.from_path("catalog_returns").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("catalog_returns").get_schema().get_field_names()])
autonode_4 = autonode_6.limit(37)
autonode_5 = autonode_7.limit(35)
autonode_2 = autonode_4.alias('NC0ln')
autonode_3 = autonode_5.order_by(col('cr_ship_mode_sk_node_7'))
autonode_1 = autonode_2.join(autonode_3, col('cr_store_credit_node_7') == col('wr_reversed_charge_node_6'))
sink = autonode_1.group_by(col('cr_net_loss_node_7')).select(col('wr_return_tax_node_6').min.alias('wr_return_tax_node_6'))
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
LogicalProject(wr_return_tax_node_6=[$1])
+- LogicalAggregate(group=[{50}], EXPR$0=[MIN($16)])
   +- LogicalJoin(condition=[=($49, $21)], joinType=[inner])
      :- LogicalProject(NC0ln=[AS($0, _UTF-16LE'NC0ln')], wr_returned_time_sk_node_6=[$1], wr_item_sk_node_6=[$2], wr_refunded_customer_sk_node_6=[$3], wr_refunded_cdemo_sk_node_6=[$4], wr_refunded_hdemo_sk_node_6=[$5], wr_refunded_addr_sk_node_6=[$6], wr_returning_customer_sk_node_6=[$7], wr_returning_cdemo_sk_node_6=[$8], wr_returning_hdemo_sk_node_6=[$9], wr_returning_addr_sk_node_6=[$10], wr_web_page_sk_node_6=[$11], wr_reason_sk_node_6=[$12], wr_order_number_node_6=[$13], wr_return_quantity_node_6=[$14], wr_return_amt_node_6=[$15], wr_return_tax_node_6=[$16], wr_return_amt_inc_tax_node_6=[$17], wr_fee_node_6=[$18], wr_return_ship_cost_node_6=[$19], wr_refunded_cash_node_6=[$20], wr_reversed_charge_node_6=[$21], wr_account_credit_node_6=[$22], wr_net_loss_node_6=[$23])
      :  +- LogicalSort(fetch=[37])
      :     +- LogicalProject(wr_returned_date_sk_node_6=[$0], wr_returned_time_sk_node_6=[$1], wr_item_sk_node_6=[$2], wr_refunded_customer_sk_node_6=[$3], wr_refunded_cdemo_sk_node_6=[$4], wr_refunded_hdemo_sk_node_6=[$5], wr_refunded_addr_sk_node_6=[$6], wr_returning_customer_sk_node_6=[$7], wr_returning_cdemo_sk_node_6=[$8], wr_returning_hdemo_sk_node_6=[$9], wr_returning_addr_sk_node_6=[$10], wr_web_page_sk_node_6=[$11], wr_reason_sk_node_6=[$12], wr_order_number_node_6=[$13], wr_return_quantity_node_6=[$14], wr_return_amt_node_6=[$15], wr_return_tax_node_6=[$16], wr_return_amt_inc_tax_node_6=[$17], wr_fee_node_6=[$18], wr_return_ship_cost_node_6=[$19], wr_refunded_cash_node_6=[$20], wr_reversed_charge_node_6=[$21], wr_account_credit_node_6=[$22], wr_net_loss_node_6=[$23])
      :        +- LogicalTableScan(table=[[default_catalog, default_database, web_returns]])
      +- LogicalSort(sort0=[$13], dir0=[ASC])
         +- LogicalSort(fetch=[35])
            +- LogicalProject(cr_returned_date_sk_node_7=[$0], cr_returned_time_sk_node_7=[$1], cr_item_sk_node_7=[$2], cr_refunded_customer_sk_node_7=[$3], cr_refunded_cdemo_sk_node_7=[$4], cr_refunded_hdemo_sk_node_7=[$5], cr_refunded_addr_sk_node_7=[$6], cr_returning_customer_sk_node_7=[$7], cr_returning_cdemo_sk_node_7=[$8], cr_returning_hdemo_sk_node_7=[$9], cr_returning_addr_sk_node_7=[$10], cr_call_center_sk_node_7=[$11], cr_catalog_page_sk_node_7=[$12], cr_ship_mode_sk_node_7=[$13], cr_warehouse_sk_node_7=[$14], cr_reason_sk_node_7=[$15], cr_order_number_node_7=[$16], cr_return_quantity_node_7=[$17], cr_return_amount_node_7=[$18], cr_return_tax_node_7=[$19], cr_return_amt_inc_tax_node_7=[$20], cr_fee_node_7=[$21], cr_return_ship_cost_node_7=[$22], cr_refunded_cash_node_7=[$23], cr_reversed_charge_node_7=[$24], cr_store_credit_node_7=[$25], cr_net_loss_node_7=[$26])
               +- LogicalTableScan(table=[[default_catalog, default_database, catalog_returns]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS wr_return_tax_node_6])
+- HashAggregate(isMerge=[false], groupBy=[cr_net_loss], select=[cr_net_loss, MIN(wr_return_tax_node_6) AS EXPR$0])
   +- Exchange(distribution=[hash[cr_net_loss]])
      +- HashJoin(joinType=[InnerJoin], where=[=(cr_store_credit, wr_reversed_charge_node_6)], select=[NC0ln, wr_returned_time_sk_node_6, wr_item_sk_node_6, wr_refunded_customer_sk_node_6, wr_refunded_cdemo_sk_node_6, wr_refunded_hdemo_sk_node_6, wr_refunded_addr_sk_node_6, wr_returning_customer_sk_node_6, wr_returning_cdemo_sk_node_6, wr_returning_hdemo_sk_node_6, wr_returning_addr_sk_node_6, wr_web_page_sk_node_6, wr_reason_sk_node_6, wr_order_number_node_6, wr_return_quantity_node_6, wr_return_amt_node_6, wr_return_tax_node_6, wr_return_amt_inc_tax_node_6, wr_fee_node_6, wr_return_ship_cost_node_6, wr_refunded_cash_node_6, wr_reversed_charge_node_6, wr_account_credit_node_6, wr_net_loss_node_6, cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss], isBroadcast=[true], build=[left])
         :- Exchange(distribution=[broadcast])
         :  +- Calc(select=[wr_returned_date_sk AS NC0ln, wr_returned_time_sk AS wr_returned_time_sk_node_6, wr_item_sk AS wr_item_sk_node_6, wr_refunded_customer_sk AS wr_refunded_customer_sk_node_6, wr_refunded_cdemo_sk AS wr_refunded_cdemo_sk_node_6, wr_refunded_hdemo_sk AS wr_refunded_hdemo_sk_node_6, wr_refunded_addr_sk AS wr_refunded_addr_sk_node_6, wr_returning_customer_sk AS wr_returning_customer_sk_node_6, wr_returning_cdemo_sk AS wr_returning_cdemo_sk_node_6, wr_returning_hdemo_sk AS wr_returning_hdemo_sk_node_6, wr_returning_addr_sk AS wr_returning_addr_sk_node_6, wr_web_page_sk AS wr_web_page_sk_node_6, wr_reason_sk AS wr_reason_sk_node_6, wr_order_number AS wr_order_number_node_6, wr_return_quantity AS wr_return_quantity_node_6, wr_return_amt AS wr_return_amt_node_6, wr_return_tax AS wr_return_tax_node_6, wr_return_amt_inc_tax AS wr_return_amt_inc_tax_node_6, wr_fee AS wr_fee_node_6, wr_return_ship_cost AS wr_return_ship_cost_node_6, wr_refunded_cash AS wr_refunded_cash_node_6, wr_reversed_charge AS wr_reversed_charge_node_6, wr_account_credit AS wr_account_credit_node_6, wr_net_loss AS wr_net_loss_node_6])
         :     +- Limit(offset=[0], fetch=[37], global=[true])
         :        +- Exchange(distribution=[single])
         :           +- Limit(offset=[0], fetch=[37], global=[false])
         :              +- TableSourceScan(table=[[default_catalog, default_database, web_returns, limit=[37]]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])
         +- Sort(orderBy=[cr_ship_mode_sk ASC])
            +- Limit(offset=[0], fetch=[35], global=[true])
               +- Exchange(distribution=[single])
                  +- Limit(offset=[0], fetch=[35], global=[false])
                     +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns, limit=[35]]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS wr_return_tax_node_6])
+- HashAggregate(isMerge=[false], groupBy=[cr_net_loss], select=[cr_net_loss, MIN(wr_return_tax_node_6) AS EXPR$0])
   +- Exchange(distribution=[hash[cr_net_loss]])
      +- HashJoin(joinType=[InnerJoin], where=[(cr_store_credit = wr_reversed_charge_node_6)], select=[NC0ln, wr_returned_time_sk_node_6, wr_item_sk_node_6, wr_refunded_customer_sk_node_6, wr_refunded_cdemo_sk_node_6, wr_refunded_hdemo_sk_node_6, wr_refunded_addr_sk_node_6, wr_returning_customer_sk_node_6, wr_returning_cdemo_sk_node_6, wr_returning_hdemo_sk_node_6, wr_returning_addr_sk_node_6, wr_web_page_sk_node_6, wr_reason_sk_node_6, wr_order_number_node_6, wr_return_quantity_node_6, wr_return_amt_node_6, wr_return_tax_node_6, wr_return_amt_inc_tax_node_6, wr_fee_node_6, wr_return_ship_cost_node_6, wr_refunded_cash_node_6, wr_reversed_charge_node_6, wr_account_credit_node_6, wr_net_loss_node_6, cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss], isBroadcast=[true], build=[left])
         :- Exchange(distribution=[broadcast])
         :  +- Calc(select=[wr_returned_date_sk AS NC0ln, wr_returned_time_sk AS wr_returned_time_sk_node_6, wr_item_sk AS wr_item_sk_node_6, wr_refunded_customer_sk AS wr_refunded_customer_sk_node_6, wr_refunded_cdemo_sk AS wr_refunded_cdemo_sk_node_6, wr_refunded_hdemo_sk AS wr_refunded_hdemo_sk_node_6, wr_refunded_addr_sk AS wr_refunded_addr_sk_node_6, wr_returning_customer_sk AS wr_returning_customer_sk_node_6, wr_returning_cdemo_sk AS wr_returning_cdemo_sk_node_6, wr_returning_hdemo_sk AS wr_returning_hdemo_sk_node_6, wr_returning_addr_sk AS wr_returning_addr_sk_node_6, wr_web_page_sk AS wr_web_page_sk_node_6, wr_reason_sk AS wr_reason_sk_node_6, wr_order_number AS wr_order_number_node_6, wr_return_quantity AS wr_return_quantity_node_6, wr_return_amt AS wr_return_amt_node_6, wr_return_tax AS wr_return_tax_node_6, wr_return_amt_inc_tax AS wr_return_amt_inc_tax_node_6, wr_fee AS wr_fee_node_6, wr_return_ship_cost AS wr_return_ship_cost_node_6, wr_refunded_cash AS wr_refunded_cash_node_6, wr_reversed_charge AS wr_reversed_charge_node_6, wr_account_credit AS wr_account_credit_node_6, wr_net_loss AS wr_net_loss_node_6])
         :     +- Limit(offset=[0], fetch=[37], global=[true])
         :        +- Exchange(distribution=[single])
         :           +- Limit(offset=[0], fetch=[37], global=[false])
         :              +- TableSourceScan(table=[[default_catalog, default_database, web_returns, limit=[37]]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])
         +- Sort(orderBy=[cr_ship_mode_sk ASC])
            +- Limit(offset=[0], fetch=[35], global=[true])
               +- Exchange(distribution=[single])
                  +- Limit(offset=[0], fetch=[35], global=[false])
                     +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns, limit=[35]]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o67334603.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#135062862:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[13](input=RelSubset#135062860,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[13]), rel#135062859:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[13](input=RelSubset#135062858,groupBy=cr_store_credit, cr_net_loss,select=cr_store_credit, cr_net_loss)]
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