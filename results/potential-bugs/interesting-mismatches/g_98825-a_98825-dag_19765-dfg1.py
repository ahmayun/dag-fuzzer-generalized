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
    return values.skew()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_13 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_12 = table_env.from_path("catalog_returns").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("catalog_returns").get_schema().get_field_names()])
autonode_11 = table_env.from_path("catalog_sales").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("catalog_sales").get_schema().get_field_names()])
autonode_14 = table_env.from_path("web_site").select(*[col(column_name).alias(f"{column_name}_node_14") for column_name in table_env.from_path("web_site").get_schema().get_field_names()])
autonode_9 = autonode_13.alias('5yPH1')
autonode_8 = autonode_11.join(autonode_12, col('cr_ship_mode_sk_node_12') == col('cs_ship_date_sk_node_11'))
autonode_10 = autonode_14.limit(23)
autonode_6 = autonode_8.order_by(col('cs_ship_cdemo_sk_node_11'))
autonode_7 = autonode_9.join(autonode_10, col('web_close_date_sk_node_14') == col('ss_promo_sk_node_13'))
autonode_4 = autonode_6.filter(col('cs_bill_cdemo_sk_node_11') >= 20)
autonode_5 = autonode_7.order_by(col('web_name_node_14'))
autonode_2 = autonode_4.limit(4)
autonode_3 = autonode_5.alias('dZPme')
autonode_1 = autonode_2.join(autonode_3, col('cr_return_amount_node_12') == col('ss_ext_tax_node_13'))
sink = autonode_1.group_by(col('cr_returned_date_sk_node_12')).select(col('cs_ship_date_sk_node_11').avg.alias('cs_ship_date_sk_node_11'))
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
      "error_message": "An error occurred while calling o53939008.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#107984286:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[27](input=RelSubset#107984284,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[27]), rel#107984283:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[27](input=RelSubset#107984282,groupBy=ss_ext_tax_node_13,select=ss_ext_tax_node_13, Partial_COUNT(*) AS count1$0)]
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
    },
    "unopt": {
      "success": true,
      "error_name": "",
      "error_message": "",
      "stdout": "== Abstract Syntax Tree ==
LogicalProject(cs_ship_date_sk_node_11=[$1])
+- LogicalAggregate(group=[{34}], EXPR$0=[AVG($2)])
   +- LogicalJoin(condition=[=($52, $79)], joinType=[inner])
      :- LogicalSort(fetch=[4])
      :  +- LogicalFilter(condition=[>=($4, 20)])
      :     +- LogicalSort(sort0=[$8], dir0=[ASC])
      :        +- LogicalJoin(condition=[=($47, $2)], joinType=[inner])
      :           :- LogicalProject(cs_sold_date_sk_node_11=[$0], cs_sold_time_sk_node_11=[$1], cs_ship_date_sk_node_11=[$2], cs_bill_customer_sk_node_11=[$3], cs_bill_cdemo_sk_node_11=[$4], cs_bill_hdemo_sk_node_11=[$5], cs_bill_addr_sk_node_11=[$6], cs_ship_customer_sk_node_11=[$7], cs_ship_cdemo_sk_node_11=[$8], cs_ship_hdemo_sk_node_11=[$9], cs_ship_addr_sk_node_11=[$10], cs_call_center_sk_node_11=[$11], cs_catalog_page_sk_node_11=[$12], cs_ship_mode_sk_node_11=[$13], cs_warehouse_sk_node_11=[$14], cs_item_sk_node_11=[$15], cs_promo_sk_node_11=[$16], cs_order_number_node_11=[$17], cs_quantity_node_11=[$18], cs_wholesale_cost_node_11=[$19], cs_list_price_node_11=[$20], cs_sales_price_node_11=[$21], cs_ext_discount_amt_node_11=[$22], cs_ext_sales_price_node_11=[$23], cs_ext_wholesale_cost_node_11=[$24], cs_ext_list_price_node_11=[$25], cs_ext_tax_node_11=[$26], cs_coupon_amt_node_11=[$27], cs_ext_ship_cost_node_11=[$28], cs_net_paid_node_11=[$29], cs_net_paid_inc_tax_node_11=[$30], cs_net_paid_inc_ship_node_11=[$31], cs_net_paid_inc_ship_tax_node_11=[$32], cs_net_profit_node_11=[$33])
      :           :  +- LogicalTableScan(table=[[default_catalog, default_database, catalog_sales]])
      :           +- LogicalProject(cr_returned_date_sk_node_12=[$0], cr_returned_time_sk_node_12=[$1], cr_item_sk_node_12=[$2], cr_refunded_customer_sk_node_12=[$3], cr_refunded_cdemo_sk_node_12=[$4], cr_refunded_hdemo_sk_node_12=[$5], cr_refunded_addr_sk_node_12=[$6], cr_returning_customer_sk_node_12=[$7], cr_returning_cdemo_sk_node_12=[$8], cr_returning_hdemo_sk_node_12=[$9], cr_returning_addr_sk_node_12=[$10], cr_call_center_sk_node_12=[$11], cr_catalog_page_sk_node_12=[$12], cr_ship_mode_sk_node_12=[$13], cr_warehouse_sk_node_12=[$14], cr_reason_sk_node_12=[$15], cr_order_number_node_12=[$16], cr_return_quantity_node_12=[$17], cr_return_amount_node_12=[$18], cr_return_tax_node_12=[$19], cr_return_amt_inc_tax_node_12=[$20], cr_fee_node_12=[$21], cr_return_ship_cost_node_12=[$22], cr_refunded_cash_node_12=[$23], cr_reversed_charge_node_12=[$24], cr_store_credit_node_12=[$25], cr_net_loss_node_12=[$26])
      :              +- LogicalTableScan(table=[[default_catalog, default_database, catalog_returns]])
      +- LogicalProject(dZPme=[AS($0, _UTF-16LE'dZPme')], ss_sold_time_sk_node_13=[$1], ss_item_sk_node_13=[$2], ss_customer_sk_node_13=[$3], ss_cdemo_sk_node_13=[$4], ss_hdemo_sk_node_13=[$5], ss_addr_sk_node_13=[$6], ss_store_sk_node_13=[$7], ss_promo_sk_node_13=[$8], ss_ticket_number_node_13=[$9], ss_quantity_node_13=[$10], ss_wholesale_cost_node_13=[$11], ss_list_price_node_13=[$12], ss_sales_price_node_13=[$13], ss_ext_discount_amt_node_13=[$14], ss_ext_sales_price_node_13=[$15], ss_ext_wholesale_cost_node_13=[$16], ss_ext_list_price_node_13=[$17], ss_ext_tax_node_13=[$18], ss_coupon_amt_node_13=[$19], ss_net_paid_node_13=[$20], ss_net_paid_inc_tax_node_13=[$21], ss_net_profit_node_13=[$22], web_site_sk_node_14=[$23], web_site_id_node_14=[$24], web_rec_start_date_node_14=[$25], web_rec_end_date_node_14=[$26], web_name_node_14=[$27], web_open_date_sk_node_14=[$28], web_close_date_sk_node_14=[$29], web_class_node_14=[$30], web_manager_node_14=[$31], web_mkt_id_node_14=[$32], web_mkt_class_node_14=[$33], web_mkt_desc_node_14=[$34], web_market_manager_node_14=[$35], web_company_id_node_14=[$36], web_company_name_node_14=[$37], web_street_number_node_14=[$38], web_street_name_node_14=[$39], web_street_type_node_14=[$40], web_suite_number_node_14=[$41], web_city_node_14=[$42], web_county_node_14=[$43], web_state_node_14=[$44], web_zip_node_14=[$45], web_country_node_14=[$46], web_gmt_offset_node_14=[$47], web_tax_percentage_node_14=[$48])
         +- LogicalSort(sort0=[$27], dir0=[ASC])
            +- LogicalJoin(condition=[=($29, $8)], joinType=[inner])
               :- LogicalProject(5yPH1=[AS($0, _UTF-16LE'5yPH1')], ss_sold_time_sk_node_13=[$1], ss_item_sk_node_13=[$2], ss_customer_sk_node_13=[$3], ss_cdemo_sk_node_13=[$4], ss_hdemo_sk_node_13=[$5], ss_addr_sk_node_13=[$6], ss_store_sk_node_13=[$7], ss_promo_sk_node_13=[$8], ss_ticket_number_node_13=[$9], ss_quantity_node_13=[$10], ss_wholesale_cost_node_13=[$11], ss_list_price_node_13=[$12], ss_sales_price_node_13=[$13], ss_ext_discount_amt_node_13=[$14], ss_ext_sales_price_node_13=[$15], ss_ext_wholesale_cost_node_13=[$16], ss_ext_list_price_node_13=[$17], ss_ext_tax_node_13=[$18], ss_coupon_amt_node_13=[$19], ss_net_paid_node_13=[$20], ss_net_paid_inc_tax_node_13=[$21], ss_net_profit_node_13=[$22])
               :  +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])
               +- LogicalSort(fetch=[23])
                  +- LogicalProject(web_site_sk_node_14=[$0], web_site_id_node_14=[$1], web_rec_start_date_node_14=[$2], web_rec_end_date_node_14=[$3], web_name_node_14=[$4], web_open_date_sk_node_14=[$5], web_close_date_sk_node_14=[$6], web_class_node_14=[$7], web_manager_node_14=[$8], web_mkt_id_node_14=[$9], web_mkt_class_node_14=[$10], web_mkt_desc_node_14=[$11], web_market_manager_node_14=[$12], web_company_id_node_14=[$13], web_company_name_node_14=[$14], web_street_number_node_14=[$15], web_street_name_node_14=[$16], web_street_type_node_14=[$17], web_suite_number_node_14=[$18], web_city_node_14=[$19], web_county_node_14=[$20], web_state_node_14=[$21], web_zip_node_14=[$22], web_country_node_14=[$23], web_gmt_offset_node_14=[$24], web_tax_percentage_node_14=[$25])
                     +- LogicalTableScan(table=[[default_catalog, default_database, web_site]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS cs_ship_date_sk_node_11])
+- SortAggregate(isMerge=[false], groupBy=[cr_returned_date_sk], select=[cr_returned_date_sk, AVG(cs_ship_date_sk) AS EXPR$0])
   +- Sort(orderBy=[cr_returned_date_sk ASC])
      +- Exchange(distribution=[hash[cr_returned_date_sk]])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[=(cr_return_amount, ss_ext_tax_node_13)], select=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit, cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss, dZPme, ss_sold_time_sk_node_13, ss_item_sk_node_13, ss_customer_sk_node_13, ss_cdemo_sk_node_13, ss_hdemo_sk_node_13, ss_addr_sk_node_13, ss_store_sk_node_13, ss_promo_sk_node_13, ss_ticket_number_node_13, ss_quantity_node_13, ss_wholesale_cost_node_13, ss_list_price_node_13, ss_sales_price_node_13, ss_ext_discount_amt_node_13, ss_ext_sales_price_node_13, ss_ext_wholesale_cost_node_13, ss_ext_list_price_node_13, ss_ext_tax_node_13, ss_coupon_amt_node_13, ss_net_paid_node_13, ss_net_paid_inc_tax_node_13, ss_net_profit_node_13, web_site_sk_node_14, web_site_id_node_14, web_rec_start_date_node_14, web_rec_end_date_node_14, web_name_node_14, web_open_date_sk_node_14, web_close_date_sk_node_14, web_class_node_14, web_manager_node_14, web_mkt_id_node_14, web_mkt_class_node_14, web_mkt_desc_node_14, web_market_manager_node_14, web_company_id_node_14, web_company_name_node_14, web_street_number_node_14, web_street_name_node_14, web_street_type_node_14, web_suite_number_node_14, web_city_node_14, web_county_node_14, web_state_node_14, web_zip_node_14, web_country_node_14, web_gmt_offset_node_14, web_tax_percentage_node_14], build=[left])
            :- Exchange(distribution=[broadcast])
            :  +- Limit(offset=[0], fetch=[4], global=[true])
            :     +- Sort(orderBy=[cs_ship_cdemo_sk ASC])
            :        +- Exchange(distribution=[single])
            :           +- Limit(offset=[0], fetch=[4], global=[false])
            :              +- Calc(select=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit, cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss], where=[>=(cs_bill_cdemo_sk, 20)])
            :                 +- SortLimit(orderBy=[cs_ship_cdemo_sk ASC], offset=[0], fetch=[1], global=[true])
            :                    +- Exchange(distribution=[single])
            :                       +- SortLimit(orderBy=[cs_ship_cdemo_sk ASC], offset=[0], fetch=[1], global=[false])
            :                          +- HashJoin(joinType=[InnerJoin], where=[=(cr_ship_mode_sk, cs_ship_date_sk)], select=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit, cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss], build=[right])
            :                             :- Exchange(distribution=[hash[cs_ship_date_sk]])
            :                             :  +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])
            :                             +- Exchange(distribution=[hash[cr_ship_mode_sk]])
            :                                +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])
            +- Calc(select=[5yPH1 AS dZPme, ss_sold_time_sk_node_13, ss_item_sk_node_13, ss_customer_sk_node_13, ss_cdemo_sk_node_13, ss_hdemo_sk_node_13, ss_addr_sk_node_13, ss_store_sk_node_13, ss_promo_sk_node_13, ss_ticket_number_node_13, ss_quantity_node_13, ss_wholesale_cost_node_13, ss_list_price_node_13, ss_sales_price_node_13, ss_ext_discount_amt_node_13, ss_ext_sales_price_node_13, ss_ext_wholesale_cost_node_13, ss_ext_list_price_node_13, ss_ext_tax_node_13, ss_coupon_amt_node_13, ss_net_paid_node_13, ss_net_paid_inc_tax_node_13, ss_net_profit_node_13, web_site_sk AS web_site_sk_node_14, web_site_id AS web_site_id_node_14, web_rec_start_date AS web_rec_start_date_node_14, web_rec_end_date AS web_rec_end_date_node_14, web_name AS web_name_node_14, web_open_date_sk AS web_open_date_sk_node_14, web_close_date_sk AS web_close_date_sk_node_14, web_class AS web_class_node_14, web_manager AS web_manager_node_14, web_mkt_id AS web_mkt_id_node_14, web_mkt_class AS web_mkt_class_node_14, web_mkt_desc AS web_mkt_desc_node_14, web_market_manager AS web_market_manager_node_14, web_company_id AS web_company_id_node_14, web_company_name AS web_company_name_node_14, web_street_number AS web_street_number_node_14, web_street_name AS web_street_name_node_14, web_street_type AS web_street_type_node_14, web_suite_number AS web_suite_number_node_14, web_city AS web_city_node_14, web_county AS web_county_node_14, web_state AS web_state_node_14, web_zip AS web_zip_node_14, web_country AS web_country_node_14, web_gmt_offset AS web_gmt_offset_node_14, web_tax_percentage AS web_tax_percentage_node_14])
               +- SortLimit(orderBy=[web_name ASC], offset=[0], fetch=[1], global=[true])
                  +- Exchange(distribution=[single])
                     +- SortLimit(orderBy=[web_name ASC], offset=[0], fetch=[1], global=[false])
                        +- HashJoin(joinType=[InnerJoin], where=[=(web_close_date_sk, ss_promo_sk_node_13)], select=[5yPH1, ss_sold_time_sk_node_13, ss_item_sk_node_13, ss_customer_sk_node_13, ss_cdemo_sk_node_13, ss_hdemo_sk_node_13, ss_addr_sk_node_13, ss_store_sk_node_13, ss_promo_sk_node_13, ss_ticket_number_node_13, ss_quantity_node_13, ss_wholesale_cost_node_13, ss_list_price_node_13, ss_sales_price_node_13, ss_ext_discount_amt_node_13, ss_ext_sales_price_node_13, ss_ext_wholesale_cost_node_13, ss_ext_list_price_node_13, ss_ext_tax_node_13, ss_coupon_amt_node_13, ss_net_paid_node_13, ss_net_paid_inc_tax_node_13, ss_net_profit_node_13, web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage], isBroadcast=[true], build=[right])
                           :- Calc(select=[ss_sold_date_sk AS 5yPH1, ss_sold_time_sk AS ss_sold_time_sk_node_13, ss_item_sk AS ss_item_sk_node_13, ss_customer_sk AS ss_customer_sk_node_13, ss_cdemo_sk AS ss_cdemo_sk_node_13, ss_hdemo_sk AS ss_hdemo_sk_node_13, ss_addr_sk AS ss_addr_sk_node_13, ss_store_sk AS ss_store_sk_node_13, ss_promo_sk AS ss_promo_sk_node_13, ss_ticket_number AS ss_ticket_number_node_13, ss_quantity AS ss_quantity_node_13, ss_wholesale_cost AS ss_wholesale_cost_node_13, ss_list_price AS ss_list_price_node_13, ss_sales_price AS ss_sales_price_node_13, ss_ext_discount_amt AS ss_ext_discount_amt_node_13, ss_ext_sales_price AS ss_ext_sales_price_node_13, ss_ext_wholesale_cost AS ss_ext_wholesale_cost_node_13, ss_ext_list_price AS ss_ext_list_price_node_13, ss_ext_tax AS ss_ext_tax_node_13, ss_coupon_amt AS ss_coupon_amt_node_13, ss_net_paid AS ss_net_paid_node_13, ss_net_paid_inc_tax AS ss_net_paid_inc_tax_node_13, ss_net_profit AS ss_net_profit_node_13])
                           :  +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
                           +- Exchange(distribution=[broadcast])
                              +- Limit(offset=[0], fetch=[23], global=[true])
                                 +- Exchange(distribution=[single])
                                    +- Limit(offset=[0], fetch=[23], global=[false])
                                       +- TableSourceScan(table=[[default_catalog, default_database, web_site, limit=[23]]], fields=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS cs_ship_date_sk_node_11])
+- SortAggregate(isMerge=[false], groupBy=[cr_returned_date_sk], select=[cr_returned_date_sk, AVG(cs_ship_date_sk) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[cr_returned_date_sk ASC])
         +- Exchange(distribution=[hash[cr_returned_date_sk]])
            +- NestedLoopJoin(joinType=[InnerJoin], where=[(cr_return_amount = ss_ext_tax_node_13)], select=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit, cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss, dZPme, ss_sold_time_sk_node_13, ss_item_sk_node_13, ss_customer_sk_node_13, ss_cdemo_sk_node_13, ss_hdemo_sk_node_13, ss_addr_sk_node_13, ss_store_sk_node_13, ss_promo_sk_node_13, ss_ticket_number_node_13, ss_quantity_node_13, ss_wholesale_cost_node_13, ss_list_price_node_13, ss_sales_price_node_13, ss_ext_discount_amt_node_13, ss_ext_sales_price_node_13, ss_ext_wholesale_cost_node_13, ss_ext_list_price_node_13, ss_ext_tax_node_13, ss_coupon_amt_node_13, ss_net_paid_node_13, ss_net_paid_inc_tax_node_13, ss_net_profit_node_13, web_site_sk_node_14, web_site_id_node_14, web_rec_start_date_node_14, web_rec_end_date_node_14, web_name_node_14, web_open_date_sk_node_14, web_close_date_sk_node_14, web_class_node_14, web_manager_node_14, web_mkt_id_node_14, web_mkt_class_node_14, web_mkt_desc_node_14, web_market_manager_node_14, web_company_id_node_14, web_company_name_node_14, web_street_number_node_14, web_street_name_node_14, web_street_type_node_14, web_suite_number_node_14, web_city_node_14, web_county_node_14, web_state_node_14, web_zip_node_14, web_country_node_14, web_gmt_offset_node_14, web_tax_percentage_node_14], build=[left])
               :- Exchange(distribution=[broadcast])
               :  +- Limit(offset=[0], fetch=[4], global=[true])
               :     +- Sort(orderBy=[cs_ship_cdemo_sk ASC])
               :        +- Exchange(distribution=[single])
               :           +- Limit(offset=[0], fetch=[4], global=[false])
               :              +- Calc(select=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit, cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss], where=[(cs_bill_cdemo_sk >= 20)])
               :                 +- SortLimit(orderBy=[cs_ship_cdemo_sk ASC], offset=[0], fetch=[1], global=[true])
               :                    +- Exchange(distribution=[single])
               :                       +- SortLimit(orderBy=[cs_ship_cdemo_sk ASC], offset=[0], fetch=[1], global=[false])
               :                          +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(cr_ship_mode_sk = cs_ship_date_sk)], select=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit, cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss], build=[right])
               :                             :- Exchange(distribution=[hash[cs_ship_date_sk]])
               :                             :  +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])
               :                             +- Exchange(distribution=[hash[cr_ship_mode_sk]])
               :                                +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])
               +- Calc(select=[5yPH1 AS dZPme, ss_sold_time_sk_node_13, ss_item_sk_node_13, ss_customer_sk_node_13, ss_cdemo_sk_node_13, ss_hdemo_sk_node_13, ss_addr_sk_node_13, ss_store_sk_node_13, ss_promo_sk_node_13, ss_ticket_number_node_13, ss_quantity_node_13, ss_wholesale_cost_node_13, ss_list_price_node_13, ss_sales_price_node_13, ss_ext_discount_amt_node_13, ss_ext_sales_price_node_13, ss_ext_wholesale_cost_node_13, ss_ext_list_price_node_13, ss_ext_tax_node_13, ss_coupon_amt_node_13, ss_net_paid_node_13, ss_net_paid_inc_tax_node_13, ss_net_profit_node_13, web_site_sk AS web_site_sk_node_14, web_site_id AS web_site_id_node_14, web_rec_start_date AS web_rec_start_date_node_14, web_rec_end_date AS web_rec_end_date_node_14, web_name AS web_name_node_14, web_open_date_sk AS web_open_date_sk_node_14, web_close_date_sk AS web_close_date_sk_node_14, web_class AS web_class_node_14, web_manager AS web_manager_node_14, web_mkt_id AS web_mkt_id_node_14, web_mkt_class AS web_mkt_class_node_14, web_mkt_desc AS web_mkt_desc_node_14, web_market_manager AS web_market_manager_node_14, web_company_id AS web_company_id_node_14, web_company_name AS web_company_name_node_14, web_street_number AS web_street_number_node_14, web_street_name AS web_street_name_node_14, web_street_type AS web_street_type_node_14, web_suite_number AS web_suite_number_node_14, web_city AS web_city_node_14, web_county AS web_county_node_14, web_state AS web_state_node_14, web_zip AS web_zip_node_14, web_country AS web_country_node_14, web_gmt_offset AS web_gmt_offset_node_14, web_tax_percentage AS web_tax_percentage_node_14])
                  +- SortLimit(orderBy=[web_name ASC], offset=[0], fetch=[1], global=[true])
                     +- Exchange(distribution=[single])
                        +- SortLimit(orderBy=[web_name ASC], offset=[0], fetch=[1], global=[false])
                           +- HashJoin(joinType=[InnerJoin], where=[(web_close_date_sk = ss_promo_sk_node_13)], select=[5yPH1, ss_sold_time_sk_node_13, ss_item_sk_node_13, ss_customer_sk_node_13, ss_cdemo_sk_node_13, ss_hdemo_sk_node_13, ss_addr_sk_node_13, ss_store_sk_node_13, ss_promo_sk_node_13, ss_ticket_number_node_13, ss_quantity_node_13, ss_wholesale_cost_node_13, ss_list_price_node_13, ss_sales_price_node_13, ss_ext_discount_amt_node_13, ss_ext_sales_price_node_13, ss_ext_wholesale_cost_node_13, ss_ext_list_price_node_13, ss_ext_tax_node_13, ss_coupon_amt_node_13, ss_net_paid_node_13, ss_net_paid_inc_tax_node_13, ss_net_profit_node_13, web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage], isBroadcast=[true], build=[right])
                              :- Calc(select=[ss_sold_date_sk AS 5yPH1, ss_sold_time_sk AS ss_sold_time_sk_node_13, ss_item_sk AS ss_item_sk_node_13, ss_customer_sk AS ss_customer_sk_node_13, ss_cdemo_sk AS ss_cdemo_sk_node_13, ss_hdemo_sk AS ss_hdemo_sk_node_13, ss_addr_sk AS ss_addr_sk_node_13, ss_store_sk AS ss_store_sk_node_13, ss_promo_sk AS ss_promo_sk_node_13, ss_ticket_number AS ss_ticket_number_node_13, ss_quantity AS ss_quantity_node_13, ss_wholesale_cost AS ss_wholesale_cost_node_13, ss_list_price AS ss_list_price_node_13, ss_sales_price AS ss_sales_price_node_13, ss_ext_discount_amt AS ss_ext_discount_amt_node_13, ss_ext_sales_price AS ss_ext_sales_price_node_13, ss_ext_wholesale_cost AS ss_ext_wholesale_cost_node_13, ss_ext_list_price AS ss_ext_list_price_node_13, ss_ext_tax AS ss_ext_tax_node_13, ss_coupon_amt AS ss_coupon_amt_node_13, ss_net_paid AS ss_net_paid_node_13, ss_net_paid_inc_tax AS ss_net_paid_inc_tax_node_13, ss_net_profit AS ss_net_profit_node_13])
                              :  +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
                              +- Exchange(distribution=[broadcast])
                                 +- Limit(offset=[0], fetch=[23], global=[true])
                                    +- Exchange(distribution=[single])
                                       +- Limit(offset=[0], fetch=[23], global=[false])
                                          +- TableSourceScan(table=[[default_catalog, default_database, web_site, limit=[23]]], fields=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0