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

autonode_4 = table_env.from_path("web_sales").select(*[col(column_name).alias(f"{column_name}_node_4") for column_name in table_env.from_path("web_sales").get_schema().get_field_names()])
autonode_6 = table_env.from_path("store_returns").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("store_returns").get_schema().get_field_names()])
autonode_5 = table_env.from_path("reason").select(*[col(column_name).alias(f"{column_name}_node_5") for column_name in table_env.from_path("reason").get_schema().get_field_names()])
autonode_2 = autonode_4.order_by(col('ws_ship_customer_sk_node_4'))
autonode_3 = autonode_5.join(autonode_6, col('r_reason_sk_node_5') == col('sr_cdemo_sk_node_6'))
autonode_1 = autonode_2.join(autonode_3, col('sr_item_sk_node_6') == col('ws_ship_date_sk_node_4'))
sink = autonode_1.group_by(col('ws_ship_cdemo_sk_node_4')).select(col('ws_item_sk_node_4').min.alias('ws_item_sk_node_4'))
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
LogicalProject(ws_item_sk_node_4=[$1])
+- LogicalAggregate(group=[{9}], EXPR$0=[MIN($3)])
   +- LogicalJoin(condition=[=($39, $2)], joinType=[inner])
      :- LogicalSort(sort0=[$8], dir0=[ASC])
      :  +- LogicalProject(ws_sold_date_sk_node_4=[$0], ws_sold_time_sk_node_4=[$1], ws_ship_date_sk_node_4=[$2], ws_item_sk_node_4=[$3], ws_bill_customer_sk_node_4=[$4], ws_bill_cdemo_sk_node_4=[$5], ws_bill_hdemo_sk_node_4=[$6], ws_bill_addr_sk_node_4=[$7], ws_ship_customer_sk_node_4=[$8], ws_ship_cdemo_sk_node_4=[$9], ws_ship_hdemo_sk_node_4=[$10], ws_ship_addr_sk_node_4=[$11], ws_web_page_sk_node_4=[$12], ws_web_site_sk_node_4=[$13], ws_ship_mode_sk_node_4=[$14], ws_warehouse_sk_node_4=[$15], ws_promo_sk_node_4=[$16], ws_order_number_node_4=[$17], ws_quantity_node_4=[$18], ws_wholesale_cost_node_4=[$19], ws_list_price_node_4=[$20], ws_sales_price_node_4=[$21], ws_ext_discount_amt_node_4=[$22], ws_ext_sales_price_node_4=[$23], ws_ext_wholesale_cost_node_4=[$24], ws_ext_list_price_node_4=[$25], ws_ext_tax_node_4=[$26], ws_coupon_amt_node_4=[$27], ws_ext_ship_cost_node_4=[$28], ws_net_paid_node_4=[$29], ws_net_paid_inc_tax_node_4=[$30], ws_net_paid_inc_ship_node_4=[$31], ws_net_paid_inc_ship_tax_node_4=[$32], ws_net_profit_node_4=[$33])
      :     +- LogicalTableScan(table=[[default_catalog, default_database, web_sales]])
      +- LogicalJoin(condition=[=($0, $7)], joinType=[inner])
         :- LogicalProject(r_reason_sk_node_5=[$0], r_reason_id_node_5=[$1], r_reason_desc_node_5=[$2])
         :  +- LogicalTableScan(table=[[default_catalog, default_database, reason]])
         +- LogicalProject(sr_returned_date_sk_node_6=[$0], sr_return_time_sk_node_6=[$1], sr_item_sk_node_6=[$2], sr_customer_sk_node_6=[$3], sr_cdemo_sk_node_6=[$4], sr_hdemo_sk_node_6=[$5], sr_addr_sk_node_6=[$6], sr_store_sk_node_6=[$7], sr_reason_sk_node_6=[$8], sr_ticket_number_node_6=[$9], sr_return_quantity_node_6=[$10], sr_return_amt_node_6=[$11], sr_return_tax_node_6=[$12], sr_return_amt_inc_tax_node_6=[$13], sr_fee_node_6=[$14], sr_return_ship_cost_node_6=[$15], sr_refunded_cash_node_6=[$16], sr_reversed_charge_node_6=[$17], sr_store_credit_node_6=[$18], sr_net_loss_node_6=[$19])
            +- LogicalTableScan(table=[[default_catalog, default_database, store_returns]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS ws_item_sk_node_4])
+- HashAggregate(isMerge=[true], groupBy=[ws_ship_cdemo_sk], select=[ws_ship_cdemo_sk, Final_MIN(min$0) AS EXPR$0])
   +- Exchange(distribution=[hash[ws_ship_cdemo_sk]])
      +- LocalHashAggregate(groupBy=[ws_ship_cdemo_sk], select=[ws_ship_cdemo_sk, Partial_MIN(ws_item_sk) AS min$0])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[=(sr_item_sk, ws_ship_date_sk)], select=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit, r_reason_sk, r_reason_id, r_reason_desc, sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss], build=[right])
            :- Sort(orderBy=[ws_ship_customer_sk ASC])
            :  +- Exchange(distribution=[single])
            :     +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
            +- Exchange(distribution=[broadcast])
               +- HashJoin(joinType=[InnerJoin], where=[=(r_reason_sk, sr_cdemo_sk)], select=[r_reason_sk, r_reason_id, r_reason_desc, sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss], isBroadcast=[true], build=[left])
                  :- Exchange(distribution=[broadcast])
                  :  +- TableSourceScan(table=[[default_catalog, default_database, reason]], fields=[r_reason_sk, r_reason_id, r_reason_desc])
                  +- TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS ws_item_sk_node_4])
+- HashAggregate(isMerge=[true], groupBy=[ws_ship_cdemo_sk], select=[ws_ship_cdemo_sk, Final_MIN(min$0) AS EXPR$0])
   +- Exchange(distribution=[hash[ws_ship_cdemo_sk]])
      +- LocalHashAggregate(groupBy=[ws_ship_cdemo_sk], select=[ws_ship_cdemo_sk, Partial_MIN(ws_item_sk) AS min$0])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[(sr_item_sk = ws_ship_date_sk)], select=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit, r_reason_sk, r_reason_id, r_reason_desc, sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss], build=[right])
            :- Sort(orderBy=[ws_ship_customer_sk ASC])
            :  +- Exchange(distribution=[single])
            :     +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
            +- Exchange(distribution=[broadcast])
               +- MultipleInput(readOrder=[0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(r_reason_sk = sr_cdemo_sk)], select=[r_reason_sk, r_reason_id, r_reason_desc, sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss], isBroadcast=[true], build=[left])\
:- [#1] Exchange(distribution=[broadcast])\
+- [#2] TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])\
])
                  :- Exchange(distribution=[broadcast])
                  :  +- TableSourceScan(table=[[default_catalog, default_database, reason]], fields=[r_reason_sk, r_reason_id, r_reason_desc])
                  +- TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o230312822.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#465888997:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[8](input=RelSubset#465888995,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[8]), rel#465888994:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[8](input=RelSubset#465888993,groupBy=ws_ship_date_sk, ws_ship_cdemo_sk,select=ws_ship_date_sk, ws_ship_cdemo_sk, Partial_MIN(ws_item_sk) AS min$0)]
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