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
    return values.iloc[-1] if len(values) > 0 else None


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_10 = table_env.from_path("catalog_returns").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("catalog_returns").get_schema().get_field_names()])
autonode_12 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_11 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_7 = autonode_10.limit(94)
autonode_9 = autonode_12.order_by(col('ss_list_price_node_12'))
autonode_8 = autonode_11.select(col('ss_sales_price_node_11'))
autonode_5 = autonode_7.add_columns(lit("hello"))
autonode_6 = autonode_8.join(autonode_9, col('ss_sales_price_node_11') == col('ss_ext_tax_node_12'))
autonode_4 = autonode_5.join(autonode_6, col('cr_returned_date_sk_node_10') == col('ss_hdemo_sk_node_12'))
autonode_3 = autonode_4.group_by(col('cr_call_center_sk_node_10')).select(col('ss_ticket_number_node_12').max.alias('ss_ticket_number_node_12'))
autonode_2 = autonode_3.order_by(col('ss_ticket_number_node_12'))
autonode_1 = autonode_2.add_columns(lit("hello"))
sink = autonode_1.alias('LytQU')
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
LogicalProject(LytQU=[AS($0, _UTF-16LE'LytQU')], _c1=[_UTF-16LE'hello'])
+- LogicalSort(sort0=[$0], dir0=[ASC])
   +- LogicalProject(ss_ticket_number_node_12=[$1])
      +- LogicalAggregate(group=[{11}], EXPR$0=[MAX($38)])
         +- LogicalJoin(condition=[=($0, $34)], joinType=[inner])
            :- LogicalProject(cr_returned_date_sk_node_10=[$0], cr_returned_time_sk_node_10=[$1], cr_item_sk_node_10=[$2], cr_refunded_customer_sk_node_10=[$3], cr_refunded_cdemo_sk_node_10=[$4], cr_refunded_hdemo_sk_node_10=[$5], cr_refunded_addr_sk_node_10=[$6], cr_returning_customer_sk_node_10=[$7], cr_returning_cdemo_sk_node_10=[$8], cr_returning_hdemo_sk_node_10=[$9], cr_returning_addr_sk_node_10=[$10], cr_call_center_sk_node_10=[$11], cr_catalog_page_sk_node_10=[$12], cr_ship_mode_sk_node_10=[$13], cr_warehouse_sk_node_10=[$14], cr_reason_sk_node_10=[$15], cr_order_number_node_10=[$16], cr_return_quantity_node_10=[$17], cr_return_amount_node_10=[$18], cr_return_tax_node_10=[$19], cr_return_amt_inc_tax_node_10=[$20], cr_fee_node_10=[$21], cr_return_ship_cost_node_10=[$22], cr_refunded_cash_node_10=[$23], cr_reversed_charge_node_10=[$24], cr_store_credit_node_10=[$25], cr_net_loss_node_10=[$26], _c27=[_UTF-16LE'hello'])
            :  +- LogicalSort(fetch=[94])
            :     +- LogicalProject(cr_returned_date_sk_node_10=[$0], cr_returned_time_sk_node_10=[$1], cr_item_sk_node_10=[$2], cr_refunded_customer_sk_node_10=[$3], cr_refunded_cdemo_sk_node_10=[$4], cr_refunded_hdemo_sk_node_10=[$5], cr_refunded_addr_sk_node_10=[$6], cr_returning_customer_sk_node_10=[$7], cr_returning_cdemo_sk_node_10=[$8], cr_returning_hdemo_sk_node_10=[$9], cr_returning_addr_sk_node_10=[$10], cr_call_center_sk_node_10=[$11], cr_catalog_page_sk_node_10=[$12], cr_ship_mode_sk_node_10=[$13], cr_warehouse_sk_node_10=[$14], cr_reason_sk_node_10=[$15], cr_order_number_node_10=[$16], cr_return_quantity_node_10=[$17], cr_return_amount_node_10=[$18], cr_return_tax_node_10=[$19], cr_return_amt_inc_tax_node_10=[$20], cr_fee_node_10=[$21], cr_return_ship_cost_node_10=[$22], cr_refunded_cash_node_10=[$23], cr_reversed_charge_node_10=[$24], cr_store_credit_node_10=[$25], cr_net_loss_node_10=[$26])
            :        +- LogicalTableScan(table=[[default_catalog, default_database, catalog_returns]])
            +- LogicalJoin(condition=[=($0, $19)], joinType=[inner])
               :- LogicalProject(ss_sales_price_node_11=[$13])
               :  +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])
               +- LogicalSort(sort0=[$12], dir0=[ASC])
                  +- LogicalProject(ss_sold_date_sk_node_12=[$0], ss_sold_time_sk_node_12=[$1], ss_item_sk_node_12=[$2], ss_customer_sk_node_12=[$3], ss_cdemo_sk_node_12=[$4], ss_hdemo_sk_node_12=[$5], ss_addr_sk_node_12=[$6], ss_store_sk_node_12=[$7], ss_promo_sk_node_12=[$8], ss_ticket_number_node_12=[$9], ss_quantity_node_12=[$10], ss_wholesale_cost_node_12=[$11], ss_list_price_node_12=[$12], ss_sales_price_node_12=[$13], ss_ext_discount_amt_node_12=[$14], ss_ext_sales_price_node_12=[$15], ss_ext_wholesale_cost_node_12=[$16], ss_ext_list_price_node_12=[$17], ss_ext_tax_node_12=[$18], ss_coupon_amt_node_12=[$19], ss_net_paid_node_12=[$20], ss_net_paid_inc_tax_node_12=[$21], ss_net_profit_node_12=[$22])
                     +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS LytQU, 'hello' AS _c1])
+- Sort(orderBy=[EXPR$0 ASC])
   +- Exchange(distribution=[single])
      +- HashAggregate(isMerge=[true], groupBy=[cr_call_center_sk_node_10], select=[cr_call_center_sk_node_10, Final_MAX(max$0) AS EXPR$0])
         +- Exchange(distribution=[hash[cr_call_center_sk_node_10]])
            +- LocalHashAggregate(groupBy=[cr_call_center_sk_node_10], select=[cr_call_center_sk_node_10, Partial_MAX(ss_ticket_number) AS max$0])
               +- HashJoin(joinType=[InnerJoin], where=[=(cr_returned_date_sk_node_10, ss_hdemo_sk)], select=[cr_returned_date_sk_node_10, cr_returned_time_sk_node_10, cr_item_sk_node_10, cr_refunded_customer_sk_node_10, cr_refunded_cdemo_sk_node_10, cr_refunded_hdemo_sk_node_10, cr_refunded_addr_sk_node_10, cr_returning_customer_sk_node_10, cr_returning_cdemo_sk_node_10, cr_returning_hdemo_sk_node_10, cr_returning_addr_sk_node_10, cr_call_center_sk_node_10, cr_catalog_page_sk_node_10, cr_ship_mode_sk_node_10, cr_warehouse_sk_node_10, cr_reason_sk_node_10, cr_order_number_node_10, cr_return_quantity_node_10, cr_return_amount_node_10, cr_return_tax_node_10, cr_return_amt_inc_tax_node_10, cr_fee_node_10, cr_return_ship_cost_node_10, cr_refunded_cash_node_10, cr_reversed_charge_node_10, cr_store_credit_node_10, cr_net_loss_node_10, _c27, ss_sales_price, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price0, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], isBroadcast=[true], build=[left])
                  :- Exchange(distribution=[broadcast])
                  :  +- Calc(select=[cr_returned_date_sk AS cr_returned_date_sk_node_10, cr_returned_time_sk AS cr_returned_time_sk_node_10, cr_item_sk AS cr_item_sk_node_10, cr_refunded_customer_sk AS cr_refunded_customer_sk_node_10, cr_refunded_cdemo_sk AS cr_refunded_cdemo_sk_node_10, cr_refunded_hdemo_sk AS cr_refunded_hdemo_sk_node_10, cr_refunded_addr_sk AS cr_refunded_addr_sk_node_10, cr_returning_customer_sk AS cr_returning_customer_sk_node_10, cr_returning_cdemo_sk AS cr_returning_cdemo_sk_node_10, cr_returning_hdemo_sk AS cr_returning_hdemo_sk_node_10, cr_returning_addr_sk AS cr_returning_addr_sk_node_10, cr_call_center_sk AS cr_call_center_sk_node_10, cr_catalog_page_sk AS cr_catalog_page_sk_node_10, cr_ship_mode_sk AS cr_ship_mode_sk_node_10, cr_warehouse_sk AS cr_warehouse_sk_node_10, cr_reason_sk AS cr_reason_sk_node_10, cr_order_number AS cr_order_number_node_10, cr_return_quantity AS cr_return_quantity_node_10, cr_return_amount AS cr_return_amount_node_10, cr_return_tax AS cr_return_tax_node_10, cr_return_amt_inc_tax AS cr_return_amt_inc_tax_node_10, cr_fee AS cr_fee_node_10, cr_return_ship_cost AS cr_return_ship_cost_node_10, cr_refunded_cash AS cr_refunded_cash_node_10, cr_reversed_charge AS cr_reversed_charge_node_10, cr_store_credit AS cr_store_credit_node_10, cr_net_loss AS cr_net_loss_node_10, 'hello' AS _c27])
                  :     +- Limit(offset=[0], fetch=[94], global=[true])
                  :        +- Exchange(distribution=[single])
                  :           +- Limit(offset=[0], fetch=[94], global=[false])
                  :              +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns, limit=[94]]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])
                  +- NestedLoopJoin(joinType=[InnerJoin], where=[=(ss_sales_price, ss_ext_tax)], select=[ss_sales_price, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price0, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], build=[left])
                     :- Exchange(distribution=[broadcast])
                     :  +- Calc(select=[ss_sales_price])
                     :     +- TableSourceScan(table=[[default_catalog, default_database, store_sales, project=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], metadata=[]]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
                     +- Sort(orderBy=[ss_list_price ASC])
                        +- Exchange(distribution=[single])
                           +- TableSourceScan(table=[[default_catalog, default_database, store_sales, project=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], metadata=[]]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS LytQU, 'hello' AS _c1])
+- Sort(orderBy=[EXPR$0 ASC])
   +- Exchange(distribution=[single])
      +- HashAggregate(isMerge=[true], groupBy=[cr_call_center_sk_node_10], select=[cr_call_center_sk_node_10, Final_MAX(max$0) AS EXPR$0])
         +- Exchange(distribution=[hash[cr_call_center_sk_node_10]])
            +- LocalHashAggregate(groupBy=[cr_call_center_sk_node_10], select=[cr_call_center_sk_node_10, Partial_MAX(ss_ticket_number) AS max$0])
               +- MultipleInput(readOrder=[0,0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(cr_returned_date_sk_node_10 = ss_hdemo_sk)], select=[cr_returned_date_sk_node_10, cr_returned_time_sk_node_10, cr_item_sk_node_10, cr_refunded_customer_sk_node_10, cr_refunded_cdemo_sk_node_10, cr_refunded_hdemo_sk_node_10, cr_refunded_addr_sk_node_10, cr_returning_customer_sk_node_10, cr_returning_cdemo_sk_node_10, cr_returning_hdemo_sk_node_10, cr_returning_addr_sk_node_10, cr_call_center_sk_node_10, cr_catalog_page_sk_node_10, cr_ship_mode_sk_node_10, cr_warehouse_sk_node_10, cr_reason_sk_node_10, cr_order_number_node_10, cr_return_quantity_node_10, cr_return_amount_node_10, cr_return_tax_node_10, cr_return_amt_inc_tax_node_10, cr_fee_node_10, cr_return_ship_cost_node_10, cr_refunded_cash_node_10, cr_reversed_charge_node_10, cr_store_credit_node_10, cr_net_loss_node_10, _c27, ss_sales_price, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price0, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], isBroadcast=[true], build=[left])\
:- [#1] Exchange(distribution=[broadcast])\
+- NestedLoopJoin(joinType=[InnerJoin], where=[(ss_sales_price = ss_ext_tax)], select=[ss_sales_price, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price0, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], build=[left])\
   :- [#2] Exchange(distribution=[broadcast])\
   +- [#3] Sort(orderBy=[ss_list_price ASC])\
])
                  :- Exchange(distribution=[broadcast])
                  :  +- Calc(select=[cr_returned_date_sk AS cr_returned_date_sk_node_10, cr_returned_time_sk AS cr_returned_time_sk_node_10, cr_item_sk AS cr_item_sk_node_10, cr_refunded_customer_sk AS cr_refunded_customer_sk_node_10, cr_refunded_cdemo_sk AS cr_refunded_cdemo_sk_node_10, cr_refunded_hdemo_sk AS cr_refunded_hdemo_sk_node_10, cr_refunded_addr_sk AS cr_refunded_addr_sk_node_10, cr_returning_customer_sk AS cr_returning_customer_sk_node_10, cr_returning_cdemo_sk AS cr_returning_cdemo_sk_node_10, cr_returning_hdemo_sk AS cr_returning_hdemo_sk_node_10, cr_returning_addr_sk AS cr_returning_addr_sk_node_10, cr_call_center_sk AS cr_call_center_sk_node_10, cr_catalog_page_sk AS cr_catalog_page_sk_node_10, cr_ship_mode_sk AS cr_ship_mode_sk_node_10, cr_warehouse_sk AS cr_warehouse_sk_node_10, cr_reason_sk AS cr_reason_sk_node_10, cr_order_number AS cr_order_number_node_10, cr_return_quantity AS cr_return_quantity_node_10, cr_return_amount AS cr_return_amount_node_10, cr_return_tax AS cr_return_tax_node_10, cr_return_amt_inc_tax AS cr_return_amt_inc_tax_node_10, cr_fee AS cr_fee_node_10, cr_return_ship_cost AS cr_return_ship_cost_node_10, cr_refunded_cash AS cr_refunded_cash_node_10, cr_reversed_charge AS cr_reversed_charge_node_10, cr_store_credit AS cr_store_credit_node_10, cr_net_loss AS cr_net_loss_node_10, 'hello' AS _c27])
                  :     +- Limit(offset=[0], fetch=[94], global=[true])
                  :        +- Exchange(distribution=[single])
                  :           +- Limit(offset=[0], fetch=[94], global=[false])
                  :              +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns, limit=[94]]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])
                  :- Exchange(distribution=[broadcast])
                  :  +- Calc(select=[ss_sales_price])
                  :     +- TableSourceScan(table=[[default_catalog, default_database, store_sales, project=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], metadata=[]]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])(reuse_id=[1])
                  +- Sort(orderBy=[ss_list_price ASC])
                     +- Exchange(distribution=[single])
                        +- Reused(reference_id=[1])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o145699539.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#294222663:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[12](input=RelSubset#294222661,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[12]), rel#294222660:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[12](input=RelSubset#294222659,groupBy=ss_hdemo_sk, ss_ext_tax,select=ss_hdemo_sk, ss_ext_tax, Partial_MAX(ss_ticket_number) AS max$0)]
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