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

autonode_9 = table_env.from_path("web_returns").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("web_returns").get_schema().get_field_names()])
autonode_8 = table_env.from_path("catalog_page").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("catalog_page").get_schema().get_field_names()])
autonode_7 = table_env.from_path("web_sales").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("web_sales").get_schema().get_field_names()])
autonode_6 = autonode_8.join(autonode_9, col('wr_return_quantity_node_9') == col('cp_catalog_page_number_node_8'))
autonode_5 = autonode_7.order_by(col('ws_quantity_node_7'))
autonode_4 = autonode_5.join(autonode_6, col('wr_fee_node_9') == col('ws_sales_price_node_7'))
autonode_3 = autonode_4.group_by(col('ws_order_number_node_7')).select(col('ws_ship_mode_sk_node_7').max.alias('ws_ship_mode_sk_node_7'))
autonode_2 = autonode_3.alias('Zo24y')
autonode_1 = autonode_2.alias('qLRIf')
sink = autonode_1.distinct()
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
LogicalAggregate(group=[{0}])
+- LogicalProject(qLRIf=[AS(AS($1, _UTF-16LE'Zo24y'), _UTF-16LE'qLRIf')])
   +- LogicalAggregate(group=[{17}], EXPR$0=[MAX($14)])
      +- LogicalJoin(condition=[=($61, $21)], joinType=[inner])
         :- LogicalSort(sort0=[$18], dir0=[ASC])
         :  +- LogicalProject(ws_sold_date_sk_node_7=[$0], ws_sold_time_sk_node_7=[$1], ws_ship_date_sk_node_7=[$2], ws_item_sk_node_7=[$3], ws_bill_customer_sk_node_7=[$4], ws_bill_cdemo_sk_node_7=[$5], ws_bill_hdemo_sk_node_7=[$6], ws_bill_addr_sk_node_7=[$7], ws_ship_customer_sk_node_7=[$8], ws_ship_cdemo_sk_node_7=[$9], ws_ship_hdemo_sk_node_7=[$10], ws_ship_addr_sk_node_7=[$11], ws_web_page_sk_node_7=[$12], ws_web_site_sk_node_7=[$13], ws_ship_mode_sk_node_7=[$14], ws_warehouse_sk_node_7=[$15], ws_promo_sk_node_7=[$16], ws_order_number_node_7=[$17], ws_quantity_node_7=[$18], ws_wholesale_cost_node_7=[$19], ws_list_price_node_7=[$20], ws_sales_price_node_7=[$21], ws_ext_discount_amt_node_7=[$22], ws_ext_sales_price_node_7=[$23], ws_ext_wholesale_cost_node_7=[$24], ws_ext_list_price_node_7=[$25], ws_ext_tax_node_7=[$26], ws_coupon_amt_node_7=[$27], ws_ext_ship_cost_node_7=[$28], ws_net_paid_node_7=[$29], ws_net_paid_inc_tax_node_7=[$30], ws_net_paid_inc_ship_node_7=[$31], ws_net_paid_inc_ship_tax_node_7=[$32], ws_net_profit_node_7=[$33])
         :     +- LogicalTableScan(table=[[default_catalog, default_database, web_sales]])
         +- LogicalJoin(condition=[=($23, $6)], joinType=[inner])
            :- LogicalProject(cp_catalog_page_sk_node_8=[$0], cp_catalog_page_id_node_8=[$1], cp_start_date_sk_node_8=[$2], cp_end_date_sk_node_8=[$3], cp_department_node_8=[$4], cp_catalog_number_node_8=[$5], cp_catalog_page_number_node_8=[$6], cp_description_node_8=[$7], cp_type_node_8=[$8])
            :  +- LogicalTableScan(table=[[default_catalog, default_database, catalog_page]])
            +- LogicalProject(wr_returned_date_sk_node_9=[$0], wr_returned_time_sk_node_9=[$1], wr_item_sk_node_9=[$2], wr_refunded_customer_sk_node_9=[$3], wr_refunded_cdemo_sk_node_9=[$4], wr_refunded_hdemo_sk_node_9=[$5], wr_refunded_addr_sk_node_9=[$6], wr_returning_customer_sk_node_9=[$7], wr_returning_cdemo_sk_node_9=[$8], wr_returning_hdemo_sk_node_9=[$9], wr_returning_addr_sk_node_9=[$10], wr_web_page_sk_node_9=[$11], wr_reason_sk_node_9=[$12], wr_order_number_node_9=[$13], wr_return_quantity_node_9=[$14], wr_return_amt_node_9=[$15], wr_return_tax_node_9=[$16], wr_return_amt_inc_tax_node_9=[$17], wr_fee_node_9=[$18], wr_return_ship_cost_node_9=[$19], wr_refunded_cash_node_9=[$20], wr_reversed_charge_node_9=[$21], wr_account_credit_node_9=[$22], wr_net_loss_node_9=[$23])
               +- LogicalTableScan(table=[[default_catalog, default_database, web_returns]])

== Optimized Physical Plan ==
HashAggregate(isMerge=[true], groupBy=[qLRIf], select=[qLRIf])
+- Exchange(distribution=[hash[qLRIf]])
   +- LocalHashAggregate(groupBy=[qLRIf], select=[qLRIf])
      +- Calc(select=[EXPR$0 AS qLRIf])
         +- HashAggregate(isMerge=[true], groupBy=[ws_order_number], select=[ws_order_number, Final_MAX(max$0) AS EXPR$0])
            +- Exchange(distribution=[hash[ws_order_number]])
               +- LocalHashAggregate(groupBy=[ws_order_number], select=[ws_order_number, Partial_MAX(ws_ship_mode_sk) AS max$0])
                  +- NestedLoopJoin(joinType=[InnerJoin], where=[=(wr_fee, ws_sales_price)], select=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit, cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], build=[right])
                     :- Sort(orderBy=[ws_quantity ASC])
                     :  +- Exchange(distribution=[single])
                     :     +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
                     +- Exchange(distribution=[broadcast])
                        +- HashJoin(joinType=[InnerJoin], where=[=(wr_return_quantity, cp_catalog_page_number)], select=[cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], build=[left])
                           :- Exchange(distribution=[hash[cp_catalog_page_number]])
                           :  +- TableSourceScan(table=[[default_catalog, default_database, catalog_page]], fields=[cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type])
                           +- Exchange(distribution=[hash[wr_return_quantity]])
                              +- TableSourceScan(table=[[default_catalog, default_database, web_returns]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])

== Optimized Execution Plan ==
HashAggregate(isMerge=[true], groupBy=[qLRIf], select=[qLRIf])
+- Exchange(distribution=[hash[qLRIf]])
   +- LocalHashAggregate(groupBy=[qLRIf], select=[qLRIf])
      +- Calc(select=[EXPR$0 AS qLRIf])
         +- HashAggregate(isMerge=[true], groupBy=[ws_order_number], select=[ws_order_number, Final_MAX(max$0) AS EXPR$0])
            +- Exchange(distribution=[hash[ws_order_number]])
               +- LocalHashAggregate(groupBy=[ws_order_number], select=[ws_order_number, Partial_MAX(ws_ship_mode_sk) AS max$0])
                  +- NestedLoopJoin(joinType=[InnerJoin], where=[(wr_fee = ws_sales_price)], select=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit, cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], build=[right])
                     :- Sort(orderBy=[ws_quantity ASC])
                     :  +- Exchange(distribution=[single])
                     :     +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
                     +- Exchange(distribution=[broadcast])
                        +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(wr_return_quantity = cp_catalog_page_number)], select=[cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], build=[left])
                           :- Exchange(distribution=[hash[cp_catalog_page_number]])
                           :  +- TableSourceScan(table=[[default_catalog, default_database, catalog_page]], fields=[cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type])
                           +- Exchange(distribution=[hash[wr_return_quantity]])
                              +- TableSourceScan(table=[[default_catalog, default_database, web_returns]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o77689649.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#156534638:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[18](input=RelSubset#156534636,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[18]), rel#156534635:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[18](input=RelSubset#156534634,groupBy=ws_order_number, ws_sales_price,select=ws_order_number, ws_sales_price, Partial_MAX(ws_ship_mode_sk) AS max$0)]
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