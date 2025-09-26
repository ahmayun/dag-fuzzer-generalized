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

autonode_10 = table_env.from_path("time_dim").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("time_dim").get_schema().get_field_names()])
autonode_12 = table_env.from_path("catalog_returns").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("catalog_returns").get_schema().get_field_names()])
autonode_11 = table_env.from_path("web_returns").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("web_returns").get_schema().get_field_names()])
autonode_7 = autonode_10.select(col('t_time_sk_node_10'))
autonode_9 = autonode_12.order_by(col('cr_order_number_node_12'))
autonode_8 = autonode_11.order_by(col('wr_return_amt_inc_tax_node_11'))
autonode_4 = autonode_7.group_by(col('t_time_sk_node_10')).select(col('t_time_sk_node_10').max.alias('t_time_sk_node_10'))
autonode_6 = autonode_9.filter(col('cr_return_amt_inc_tax_node_12') <= -14.471656084060669)
autonode_5 = autonode_8.filter(col('wr_return_quantity_node_11') > 19)
autonode_3 = autonode_6.filter(col('cr_call_center_sk_node_12') <= -29)
autonode_2 = autonode_4.join(autonode_5, col('t_time_sk_node_10') == col('wr_returned_time_sk_node_11'))
autonode_1 = autonode_2.join(autonode_3, col('cr_return_amount_node_12') == col('wr_reversed_charge_node_11'))
sink = autonode_1.group_by(col('wr_refunded_addr_sk_node_11')).select(col('wr_returning_customer_sk_node_11').max.alias('wr_returning_customer_sk_node_11'))
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
LogicalProject(wr_returning_customer_sk_node_11=[$1])
+- LogicalAggregate(group=[{7}], EXPR$0=[MAX($8)])
   +- LogicalJoin(condition=[=($43, $22)], joinType=[inner])
      :- LogicalJoin(condition=[=($0, $2)], joinType=[inner])
      :  :- LogicalProject(t_time_sk_node_10=[$1])
      :  :  +- LogicalAggregate(group=[{0}], EXPR$0=[MAX($0)])
      :  :     +- LogicalProject(t_time_sk_node_10=[$0])
      :  :        +- LogicalTableScan(table=[[default_catalog, default_database, time_dim]])
      :  +- LogicalFilter(condition=[>($14, 19)])
      :     +- LogicalSort(sort0=[$17], dir0=[ASC])
      :        +- LogicalProject(wr_returned_date_sk_node_11=[$0], wr_returned_time_sk_node_11=[$1], wr_item_sk_node_11=[$2], wr_refunded_customer_sk_node_11=[$3], wr_refunded_cdemo_sk_node_11=[$4], wr_refunded_hdemo_sk_node_11=[$5], wr_refunded_addr_sk_node_11=[$6], wr_returning_customer_sk_node_11=[$7], wr_returning_cdemo_sk_node_11=[$8], wr_returning_hdemo_sk_node_11=[$9], wr_returning_addr_sk_node_11=[$10], wr_web_page_sk_node_11=[$11], wr_reason_sk_node_11=[$12], wr_order_number_node_11=[$13], wr_return_quantity_node_11=[$14], wr_return_amt_node_11=[$15], wr_return_tax_node_11=[$16], wr_return_amt_inc_tax_node_11=[$17], wr_fee_node_11=[$18], wr_return_ship_cost_node_11=[$19], wr_refunded_cash_node_11=[$20], wr_reversed_charge_node_11=[$21], wr_account_credit_node_11=[$22], wr_net_loss_node_11=[$23])
      :           +- LogicalTableScan(table=[[default_catalog, default_database, web_returns]])
      +- LogicalFilter(condition=[<=($11, -29)])
         +- LogicalFilter(condition=[<=($20, -1.4471656084060669E1:DOUBLE)])
            +- LogicalSort(sort0=[$16], dir0=[ASC])
               +- LogicalProject(cr_returned_date_sk_node_12=[$0], cr_returned_time_sk_node_12=[$1], cr_item_sk_node_12=[$2], cr_refunded_customer_sk_node_12=[$3], cr_refunded_cdemo_sk_node_12=[$4], cr_refunded_hdemo_sk_node_12=[$5], cr_refunded_addr_sk_node_12=[$6], cr_returning_customer_sk_node_12=[$7], cr_returning_cdemo_sk_node_12=[$8], cr_returning_hdemo_sk_node_12=[$9], cr_returning_addr_sk_node_12=[$10], cr_call_center_sk_node_12=[$11], cr_catalog_page_sk_node_12=[$12], cr_ship_mode_sk_node_12=[$13], cr_warehouse_sk_node_12=[$14], cr_reason_sk_node_12=[$15], cr_order_number_node_12=[$16], cr_return_quantity_node_12=[$17], cr_return_amount_node_12=[$18], cr_return_tax_node_12=[$19], cr_return_amt_inc_tax_node_12=[$20], cr_fee_node_12=[$21], cr_return_ship_cost_node_12=[$22], cr_refunded_cash_node_12=[$23], cr_reversed_charge_node_12=[$24], cr_store_credit_node_12=[$25], cr_net_loss_node_12=[$26])
                  +- LogicalTableScan(table=[[default_catalog, default_database, catalog_returns]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS wr_returning_customer_sk_node_11])
+- HashAggregate(isMerge=[true], groupBy=[wr_refunded_addr_sk], select=[wr_refunded_addr_sk, Final_MAX(max$0) AS EXPR$0])
   +- Exchange(distribution=[hash[wr_refunded_addr_sk]])
      +- LocalHashAggregate(groupBy=[wr_refunded_addr_sk], select=[wr_refunded_addr_sk, Partial_MAX(wr_returning_customer_sk) AS max$0])
         +- HashJoin(joinType=[InnerJoin], where=[=(cr_return_amount, wr_reversed_charge)], select=[t_time_sk_node_10, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss, cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss], build=[left])
            :- Exchange(distribution=[hash[wr_reversed_charge]])
            :  +- HashJoin(joinType=[InnerJoin], where=[=(t_time_sk_node_10, wr_returned_time_sk)], select=[t_time_sk_node_10, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], isBroadcast=[true], build=[left])
            :     :- Exchange(distribution=[broadcast])
            :     :  +- Calc(select=[EXPR$0 AS t_time_sk_node_10])
            :     :     +- HashAggregate(isMerge=[true], groupBy=[t_time_sk], select=[t_time_sk, Final_MAX(max$0) AS EXPR$0])
            :     :        +- Exchange(distribution=[hash[t_time_sk]])
            :     :           +- LocalHashAggregate(groupBy=[t_time_sk], select=[t_time_sk, Partial_MAX(t_time_sk) AS max$0])
            :     :              +- TableSourceScan(table=[[default_catalog, default_database, time_dim, project=[t_time_sk], metadata=[]]], fields=[t_time_sk])
            :     +- Calc(select=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], where=[>(wr_return_quantity, 19)])
            :        +- Sort(orderBy=[wr_return_amt_inc_tax ASC])
            :           +- Exchange(distribution=[single])
            :              +- TableSourceScan(table=[[default_catalog, default_database, web_returns]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])
            +- Exchange(distribution=[hash[cr_return_amount]])
               +- Calc(select=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss], where=[AND(<=(cr_return_amt_inc_tax, -1.4471656084060669E1), <=(cr_call_center_sk, -29))])
                  +- Sort(orderBy=[cr_order_number ASC])
                     +- Exchange(distribution=[single])
                        +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS wr_returning_customer_sk_node_11])
+- HashAggregate(isMerge=[true], groupBy=[wr_refunded_addr_sk], select=[wr_refunded_addr_sk, Final_MAX(max$0) AS EXPR$0])
   +- Exchange(distribution=[hash[wr_refunded_addr_sk]])
      +- LocalHashAggregate(groupBy=[wr_refunded_addr_sk], select=[wr_refunded_addr_sk, Partial_MAX(wr_returning_customer_sk) AS max$0])
         +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(cr_return_amount = wr_reversed_charge)], select=[t_time_sk_node_10, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss, cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss], build=[left])
            :- Exchange(distribution=[hash[wr_reversed_charge]])
            :  +- HashJoin(joinType=[InnerJoin], where=[(t_time_sk_node_10 = wr_returned_time_sk)], select=[t_time_sk_node_10, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], isBroadcast=[true], build=[left])
            :     :- Exchange(distribution=[broadcast])
            :     :  +- Calc(select=[EXPR$0 AS t_time_sk_node_10])
            :     :     +- HashAggregate(isMerge=[true], groupBy=[t_time_sk], select=[t_time_sk, Final_MAX(max$0) AS EXPR$0])
            :     :        +- Exchange(distribution=[hash[t_time_sk]])
            :     :           +- LocalHashAggregate(groupBy=[t_time_sk], select=[t_time_sk, Partial_MAX(t_time_sk) AS max$0])
            :     :              +- TableSourceScan(table=[[default_catalog, default_database, time_dim, project=[t_time_sk], metadata=[]]], fields=[t_time_sk])
            :     +- Calc(select=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], where=[(wr_return_quantity > 19)])
            :        +- Sort(orderBy=[wr_return_amt_inc_tax ASC])
            :           +- Exchange(distribution=[single])
            :              +- TableSourceScan(table=[[default_catalog, default_database, web_returns]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])
            +- Exchange(distribution=[hash[cr_return_amount]])
               +- Calc(select=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss], where=[((cr_return_amt_inc_tax <= -1.4471656084060669E1) AND (cr_call_center_sk <= -29))])
                  +- Sort(orderBy=[cr_order_number ASC])
                     +- Exchange(distribution=[single])
                        +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o38124275.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#75530370:AbstractConverter.BATCH_PHYSICAL.hash[0, 1, 2]true.[17](input=RelSubset#75530368,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1, 2]true,sort=[17]), rel#75530367:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[17](input=RelSubset#75530366,groupBy=wr_returned_time_sk, wr_refunded_addr_sk, wr_reversed_charge,select=wr_returned_time_sk, wr_refunded_addr_sk, wr_reversed_charge, Partial_MAX(wr_returning_customer_sk) AS max$0)]
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