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

autonode_6 = table_env.from_path("catalog_sales").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("catalog_sales").get_schema().get_field_names()])
autonode_8 = table_env.from_path("web_page").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("web_page").get_schema().get_field_names()])
autonode_7 = table_env.from_path("customer_address").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("customer_address").get_schema().get_field_names()])
autonode_4 = autonode_6.order_by(col('cs_net_paid_inc_ship_node_6'))
autonode_5 = autonode_7.join(autonode_8, col('ca_address_sk_node_7') == col('wp_access_date_sk_node_8'))
autonode_2 = autonode_4.alias('cTcAx')
autonode_3 = autonode_5.order_by(col('wp_rec_end_date_node_8'))
autonode_1 = autonode_2.join(autonode_3, col('wp_creation_date_sk_node_8') == col('cs_call_center_sk_node_6'))
sink = autonode_1.group_by(col('ca_state_node_7')).select(col('cs_ship_cdemo_sk_node_6').count.alias('cs_ship_cdemo_sk_node_6'))
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
      "error_message": "An error occurred while calling o89581631.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#179948320:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[31](input=RelSubset#179948318,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[31]), rel#179948317:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[31](input=RelSubset#179948316,groupBy=cs_call_center_sk,select=cs_call_center_sk, Partial_COUNT(cs_ship_cdemo_sk) AS count$0)]
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
LogicalProject(cs_ship_cdemo_sk_node_6=[$1])
+- LogicalAggregate(group=[{42}], EXPR$0=[COUNT($8)])
   +- LogicalJoin(condition=[=($51, $11)], joinType=[inner])
      :- LogicalProject(cTcAx=[AS($0, _UTF-16LE'cTcAx')], cs_sold_time_sk_node_6=[$1], cs_ship_date_sk_node_6=[$2], cs_bill_customer_sk_node_6=[$3], cs_bill_cdemo_sk_node_6=[$4], cs_bill_hdemo_sk_node_6=[$5], cs_bill_addr_sk_node_6=[$6], cs_ship_customer_sk_node_6=[$7], cs_ship_cdemo_sk_node_6=[$8], cs_ship_hdemo_sk_node_6=[$9], cs_ship_addr_sk_node_6=[$10], cs_call_center_sk_node_6=[$11], cs_catalog_page_sk_node_6=[$12], cs_ship_mode_sk_node_6=[$13], cs_warehouse_sk_node_6=[$14], cs_item_sk_node_6=[$15], cs_promo_sk_node_6=[$16], cs_order_number_node_6=[$17], cs_quantity_node_6=[$18], cs_wholesale_cost_node_6=[$19], cs_list_price_node_6=[$20], cs_sales_price_node_6=[$21], cs_ext_discount_amt_node_6=[$22], cs_ext_sales_price_node_6=[$23], cs_ext_wholesale_cost_node_6=[$24], cs_ext_list_price_node_6=[$25], cs_ext_tax_node_6=[$26], cs_coupon_amt_node_6=[$27], cs_ext_ship_cost_node_6=[$28], cs_net_paid_node_6=[$29], cs_net_paid_inc_tax_node_6=[$30], cs_net_paid_inc_ship_node_6=[$31], cs_net_paid_inc_ship_tax_node_6=[$32], cs_net_profit_node_6=[$33])
      :  +- LogicalSort(sort0=[$31], dir0=[ASC])
      :     +- LogicalProject(cs_sold_date_sk_node_6=[$0], cs_sold_time_sk_node_6=[$1], cs_ship_date_sk_node_6=[$2], cs_bill_customer_sk_node_6=[$3], cs_bill_cdemo_sk_node_6=[$4], cs_bill_hdemo_sk_node_6=[$5], cs_bill_addr_sk_node_6=[$6], cs_ship_customer_sk_node_6=[$7], cs_ship_cdemo_sk_node_6=[$8], cs_ship_hdemo_sk_node_6=[$9], cs_ship_addr_sk_node_6=[$10], cs_call_center_sk_node_6=[$11], cs_catalog_page_sk_node_6=[$12], cs_ship_mode_sk_node_6=[$13], cs_warehouse_sk_node_6=[$14], cs_item_sk_node_6=[$15], cs_promo_sk_node_6=[$16], cs_order_number_node_6=[$17], cs_quantity_node_6=[$18], cs_wholesale_cost_node_6=[$19], cs_list_price_node_6=[$20], cs_sales_price_node_6=[$21], cs_ext_discount_amt_node_6=[$22], cs_ext_sales_price_node_6=[$23], cs_ext_wholesale_cost_node_6=[$24], cs_ext_list_price_node_6=[$25], cs_ext_tax_node_6=[$26], cs_coupon_amt_node_6=[$27], cs_ext_ship_cost_node_6=[$28], cs_net_paid_node_6=[$29], cs_net_paid_inc_tax_node_6=[$30], cs_net_paid_inc_ship_node_6=[$31], cs_net_paid_inc_ship_tax_node_6=[$32], cs_net_profit_node_6=[$33])
      :        +- LogicalTableScan(table=[[default_catalog, default_database, catalog_sales]])
      +- LogicalSort(sort0=[$16], dir0=[ASC])
         +- LogicalJoin(condition=[=($0, $18)], joinType=[inner])
            :- LogicalProject(ca_address_sk_node_7=[$0], ca_address_id_node_7=[$1], ca_street_number_node_7=[$2], ca_street_name_node_7=[$3], ca_street_type_node_7=[$4], ca_suite_number_node_7=[$5], ca_city_node_7=[$6], ca_county_node_7=[$7], ca_state_node_7=[$8], ca_zip_node_7=[$9], ca_country_node_7=[$10], ca_gmt_offset_node_7=[$11], ca_location_type_node_7=[$12])
            :  +- LogicalTableScan(table=[[default_catalog, default_database, customer_address]])
            +- LogicalProject(wp_web_page_sk_node_8=[$0], wp_web_page_id_node_8=[$1], wp_rec_start_date_node_8=[$2], wp_rec_end_date_node_8=[$3], wp_creation_date_sk_node_8=[$4], wp_access_date_sk_node_8=[$5], wp_autogen_flag_node_8=[$6], wp_customer_sk_node_8=[$7], wp_url_node_8=[$8], wp_type_node_8=[$9], wp_char_count_node_8=[$10], wp_link_count_node_8=[$11], wp_image_count_node_8=[$12], wp_max_ad_count_node_8=[$13])
               +- LogicalTableScan(table=[[default_catalog, default_database, web_page]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS cs_ship_cdemo_sk_node_6])
+- SortAggregate(isMerge=[false], groupBy=[ca_state], select=[ca_state, COUNT(cs_ship_cdemo_sk_node_6) AS EXPR$0])
   +- Sort(orderBy=[ca_state ASC])
      +- NestedLoopJoin(joinType=[InnerJoin], where=[=(wp_creation_date_sk, cs_call_center_sk_node_6)], select=[cTcAx, cs_sold_time_sk_node_6, cs_ship_date_sk_node_6, cs_bill_customer_sk_node_6, cs_bill_cdemo_sk_node_6, cs_bill_hdemo_sk_node_6, cs_bill_addr_sk_node_6, cs_ship_customer_sk_node_6, cs_ship_cdemo_sk_node_6, cs_ship_hdemo_sk_node_6, cs_ship_addr_sk_node_6, cs_call_center_sk_node_6, cs_catalog_page_sk_node_6, cs_ship_mode_sk_node_6, cs_warehouse_sk_node_6, cs_item_sk_node_6, cs_promo_sk_node_6, cs_order_number_node_6, cs_quantity_node_6, cs_wholesale_cost_node_6, cs_list_price_node_6, cs_sales_price_node_6, cs_ext_discount_amt_node_6, cs_ext_sales_price_node_6, cs_ext_wholesale_cost_node_6, cs_ext_list_price_node_6, cs_ext_tax_node_6, cs_coupon_amt_node_6, cs_ext_ship_cost_node_6, cs_net_paid_node_6, cs_net_paid_inc_tax_node_6, cs_net_paid_inc_ship_node_6, cs_net_paid_inc_ship_tax_node_6, cs_net_profit_node_6, ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type, wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count], build=[left])
         :- Exchange(distribution=[broadcast])
         :  +- Calc(select=[cs_sold_date_sk AS cTcAx, cs_sold_time_sk AS cs_sold_time_sk_node_6, cs_ship_date_sk AS cs_ship_date_sk_node_6, cs_bill_customer_sk AS cs_bill_customer_sk_node_6, cs_bill_cdemo_sk AS cs_bill_cdemo_sk_node_6, cs_bill_hdemo_sk AS cs_bill_hdemo_sk_node_6, cs_bill_addr_sk AS cs_bill_addr_sk_node_6, cs_ship_customer_sk AS cs_ship_customer_sk_node_6, cs_ship_cdemo_sk AS cs_ship_cdemo_sk_node_6, cs_ship_hdemo_sk AS cs_ship_hdemo_sk_node_6, cs_ship_addr_sk AS cs_ship_addr_sk_node_6, cs_call_center_sk AS cs_call_center_sk_node_6, cs_catalog_page_sk AS cs_catalog_page_sk_node_6, cs_ship_mode_sk AS cs_ship_mode_sk_node_6, cs_warehouse_sk AS cs_warehouse_sk_node_6, cs_item_sk AS cs_item_sk_node_6, cs_promo_sk AS cs_promo_sk_node_6, cs_order_number AS cs_order_number_node_6, cs_quantity AS cs_quantity_node_6, cs_wholesale_cost AS cs_wholesale_cost_node_6, cs_list_price AS cs_list_price_node_6, cs_sales_price AS cs_sales_price_node_6, cs_ext_discount_amt AS cs_ext_discount_amt_node_6, cs_ext_sales_price AS cs_ext_sales_price_node_6, cs_ext_wholesale_cost AS cs_ext_wholesale_cost_node_6, cs_ext_list_price AS cs_ext_list_price_node_6, cs_ext_tax AS cs_ext_tax_node_6, cs_coupon_amt AS cs_coupon_amt_node_6, cs_ext_ship_cost AS cs_ext_ship_cost_node_6, cs_net_paid AS cs_net_paid_node_6, cs_net_paid_inc_tax AS cs_net_paid_inc_tax_node_6, cs_net_paid_inc_ship AS cs_net_paid_inc_ship_node_6, cs_net_paid_inc_ship_tax AS cs_net_paid_inc_ship_tax_node_6, cs_net_profit AS cs_net_profit_node_6])
         :     +- SortLimit(orderBy=[cs_net_paid_inc_ship ASC], offset=[0], fetch=[1], global=[true])
         :        +- Exchange(distribution=[single])
         :           +- SortLimit(orderBy=[cs_net_paid_inc_ship ASC], offset=[0], fetch=[1], global=[false])
         :              +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])
         +- Exchange(distribution=[hash[ca_state]])
            +- SortLimit(orderBy=[wp_rec_end_date ASC], offset=[0], fetch=[1], global=[true])
               +- Exchange(distribution=[single])
                  +- SortLimit(orderBy=[wp_rec_end_date ASC], offset=[0], fetch=[1], global=[false])
                     +- HashJoin(joinType=[InnerJoin], where=[=(ca_address_sk, wp_access_date_sk)], select=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type, wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count], isBroadcast=[true], build=[right])
                        :- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])
                        +- Exchange(distribution=[broadcast])
                           +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS cs_ship_cdemo_sk_node_6])
+- SortAggregate(isMerge=[false], groupBy=[ca_state], select=[ca_state, COUNT(cs_ship_cdemo_sk_node_6) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[ca_state ASC])
         +- Exchange(distribution=[keep_input_as_is[hash[ca_state]]])
            +- NestedLoopJoin(joinType=[InnerJoin], where=[(wp_creation_date_sk = cs_call_center_sk_node_6)], select=[cTcAx, cs_sold_time_sk_node_6, cs_ship_date_sk_node_6, cs_bill_customer_sk_node_6, cs_bill_cdemo_sk_node_6, cs_bill_hdemo_sk_node_6, cs_bill_addr_sk_node_6, cs_ship_customer_sk_node_6, cs_ship_cdemo_sk_node_6, cs_ship_hdemo_sk_node_6, cs_ship_addr_sk_node_6, cs_call_center_sk_node_6, cs_catalog_page_sk_node_6, cs_ship_mode_sk_node_6, cs_warehouse_sk_node_6, cs_item_sk_node_6, cs_promo_sk_node_6, cs_order_number_node_6, cs_quantity_node_6, cs_wholesale_cost_node_6, cs_list_price_node_6, cs_sales_price_node_6, cs_ext_discount_amt_node_6, cs_ext_sales_price_node_6, cs_ext_wholesale_cost_node_6, cs_ext_list_price_node_6, cs_ext_tax_node_6, cs_coupon_amt_node_6, cs_ext_ship_cost_node_6, cs_net_paid_node_6, cs_net_paid_inc_tax_node_6, cs_net_paid_inc_ship_node_6, cs_net_paid_inc_ship_tax_node_6, cs_net_profit_node_6, ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type, wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count], build=[left])
               :- Exchange(distribution=[broadcast])
               :  +- Calc(select=[cs_sold_date_sk AS cTcAx, cs_sold_time_sk AS cs_sold_time_sk_node_6, cs_ship_date_sk AS cs_ship_date_sk_node_6, cs_bill_customer_sk AS cs_bill_customer_sk_node_6, cs_bill_cdemo_sk AS cs_bill_cdemo_sk_node_6, cs_bill_hdemo_sk AS cs_bill_hdemo_sk_node_6, cs_bill_addr_sk AS cs_bill_addr_sk_node_6, cs_ship_customer_sk AS cs_ship_customer_sk_node_6, cs_ship_cdemo_sk AS cs_ship_cdemo_sk_node_6, cs_ship_hdemo_sk AS cs_ship_hdemo_sk_node_6, cs_ship_addr_sk AS cs_ship_addr_sk_node_6, cs_call_center_sk AS cs_call_center_sk_node_6, cs_catalog_page_sk AS cs_catalog_page_sk_node_6, cs_ship_mode_sk AS cs_ship_mode_sk_node_6, cs_warehouse_sk AS cs_warehouse_sk_node_6, cs_item_sk AS cs_item_sk_node_6, cs_promo_sk AS cs_promo_sk_node_6, cs_order_number AS cs_order_number_node_6, cs_quantity AS cs_quantity_node_6, cs_wholesale_cost AS cs_wholesale_cost_node_6, cs_list_price AS cs_list_price_node_6, cs_sales_price AS cs_sales_price_node_6, cs_ext_discount_amt AS cs_ext_discount_amt_node_6, cs_ext_sales_price AS cs_ext_sales_price_node_6, cs_ext_wholesale_cost AS cs_ext_wholesale_cost_node_6, cs_ext_list_price AS cs_ext_list_price_node_6, cs_ext_tax AS cs_ext_tax_node_6, cs_coupon_amt AS cs_coupon_amt_node_6, cs_ext_ship_cost AS cs_ext_ship_cost_node_6, cs_net_paid AS cs_net_paid_node_6, cs_net_paid_inc_tax AS cs_net_paid_inc_tax_node_6, cs_net_paid_inc_ship AS cs_net_paid_inc_ship_node_6, cs_net_paid_inc_ship_tax AS cs_net_paid_inc_ship_tax_node_6, cs_net_profit AS cs_net_profit_node_6])
               :     +- SortLimit(orderBy=[cs_net_paid_inc_ship ASC], offset=[0], fetch=[1], global=[true])
               :        +- Exchange(distribution=[single])
               :           +- SortLimit(orderBy=[cs_net_paid_inc_ship ASC], offset=[0], fetch=[1], global=[false])
               :              +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])
               +- Exchange(distribution=[hash[ca_state]])
                  +- SortLimit(orderBy=[wp_rec_end_date ASC], offset=[0], fetch=[1], global=[true])
                     +- Exchange(distribution=[single])
                        +- SortLimit(orderBy=[wp_rec_end_date ASC], offset=[0], fetch=[1], global=[false])
                           +- HashJoin(joinType=[InnerJoin], where=[(ca_address_sk = wp_access_date_sk)], select=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type, wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count], isBroadcast=[true], build=[right])
                              :- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])
                              +- Exchange(distribution=[broadcast])
                                 +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0