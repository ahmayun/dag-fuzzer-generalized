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
    return len(values) / (1.0 / values).sum() if (values != 0).all() else None


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_9 = table_env.from_path("web_sales").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("web_sales").get_schema().get_field_names()])
autonode_8 = table_env.from_path("item").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("item").get_schema().get_field_names()])
autonode_7 = table_env.from_path("web_site").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("web_site").get_schema().get_field_names()])
autonode_6 = autonode_9.order_by(col('ws_quantity_node_9'))
autonode_5 = autonode_8.add_columns(lit("hello"))
autonode_4 = autonode_7.filter(col('web_tax_percentage_node_7') >= -18.943101167678833)
autonode_3 = autonode_6.alias('H0dhv')
autonode_2 = autonode_4.join(autonode_5, col('web_gmt_offset_node_7') == col('i_wholesale_cost_node_8'))
autonode_1 = autonode_2.join(autonode_3, col('ws_quantity_node_9') == col('web_open_date_sk_node_7'))
sink = autonode_1.group_by(col('i_manufact_node_8')).select(col('web_mkt_id_node_7').sum.alias('web_mkt_id_node_7'))
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
      "error_message": "An error occurred while calling o267410834.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#540934576:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[18](input=RelSubset#540934574,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[18]), rel#540934573:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[18](input=RelSubset#540934572,groupBy=ws_quantity,select=ws_quantity, Partial_COUNT(*) AS count1$0)]
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
LogicalProject(web_mkt_id_node_7=[$1])
+- LogicalAggregate(group=[{40}], EXPR$0=[SUM($9)])
   +- LogicalJoin(condition=[=($67, $5)], joinType=[inner])
      :- LogicalJoin(condition=[=($24, $32)], joinType=[inner])
      :  :- LogicalFilter(condition=[>=($25, -1.8943101167678833E1:DOUBLE)])
      :  :  +- LogicalProject(web_site_sk_node_7=[$0], web_site_id_node_7=[$1], web_rec_start_date_node_7=[$2], web_rec_end_date_node_7=[$3], web_name_node_7=[$4], web_open_date_sk_node_7=[$5], web_close_date_sk_node_7=[$6], web_class_node_7=[$7], web_manager_node_7=[$8], web_mkt_id_node_7=[$9], web_mkt_class_node_7=[$10], web_mkt_desc_node_7=[$11], web_market_manager_node_7=[$12], web_company_id_node_7=[$13], web_company_name_node_7=[$14], web_street_number_node_7=[$15], web_street_name_node_7=[$16], web_street_type_node_7=[$17], web_suite_number_node_7=[$18], web_city_node_7=[$19], web_county_node_7=[$20], web_state_node_7=[$21], web_zip_node_7=[$22], web_country_node_7=[$23], web_gmt_offset_node_7=[$24], web_tax_percentage_node_7=[$25])
      :  :     +- LogicalTableScan(table=[[default_catalog, default_database, web_site]])
      :  +- LogicalProject(i_item_sk_node_8=[$0], i_item_id_node_8=[$1], i_rec_start_date_node_8=[$2], i_rec_end_date_node_8=[$3], i_item_desc_node_8=[$4], i_current_price_node_8=[$5], i_wholesale_cost_node_8=[$6], i_brand_id_node_8=[$7], i_brand_node_8=[$8], i_class_id_node_8=[$9], i_class_node_8=[$10], i_category_id_node_8=[$11], i_category_node_8=[$12], i_manufact_id_node_8=[$13], i_manufact_node_8=[$14], i_size_node_8=[$15], i_formulation_node_8=[$16], i_color_node_8=[$17], i_units_node_8=[$18], i_container_node_8=[$19], i_manager_id_node_8=[$20], i_product_name_node_8=[$21], _c22=[_UTF-16LE'hello'])
      :     +- LogicalTableScan(table=[[default_catalog, default_database, item]])
      +- LogicalProject(H0dhv=[AS($0, _UTF-16LE'H0dhv')], ws_sold_time_sk_node_9=[$1], ws_ship_date_sk_node_9=[$2], ws_item_sk_node_9=[$3], ws_bill_customer_sk_node_9=[$4], ws_bill_cdemo_sk_node_9=[$5], ws_bill_hdemo_sk_node_9=[$6], ws_bill_addr_sk_node_9=[$7], ws_ship_customer_sk_node_9=[$8], ws_ship_cdemo_sk_node_9=[$9], ws_ship_hdemo_sk_node_9=[$10], ws_ship_addr_sk_node_9=[$11], ws_web_page_sk_node_9=[$12], ws_web_site_sk_node_9=[$13], ws_ship_mode_sk_node_9=[$14], ws_warehouse_sk_node_9=[$15], ws_promo_sk_node_9=[$16], ws_order_number_node_9=[$17], ws_quantity_node_9=[$18], ws_wholesale_cost_node_9=[$19], ws_list_price_node_9=[$20], ws_sales_price_node_9=[$21], ws_ext_discount_amt_node_9=[$22], ws_ext_sales_price_node_9=[$23], ws_ext_wholesale_cost_node_9=[$24], ws_ext_list_price_node_9=[$25], ws_ext_tax_node_9=[$26], ws_coupon_amt_node_9=[$27], ws_ext_ship_cost_node_9=[$28], ws_net_paid_node_9=[$29], ws_net_paid_inc_tax_node_9=[$30], ws_net_paid_inc_ship_node_9=[$31], ws_net_paid_inc_ship_tax_node_9=[$32], ws_net_profit_node_9=[$33])
         +- LogicalSort(sort0=[$18], dir0=[ASC])
            +- LogicalProject(ws_sold_date_sk_node_9=[$0], ws_sold_time_sk_node_9=[$1], ws_ship_date_sk_node_9=[$2], ws_item_sk_node_9=[$3], ws_bill_customer_sk_node_9=[$4], ws_bill_cdemo_sk_node_9=[$5], ws_bill_hdemo_sk_node_9=[$6], ws_bill_addr_sk_node_9=[$7], ws_ship_customer_sk_node_9=[$8], ws_ship_cdemo_sk_node_9=[$9], ws_ship_hdemo_sk_node_9=[$10], ws_ship_addr_sk_node_9=[$11], ws_web_page_sk_node_9=[$12], ws_web_site_sk_node_9=[$13], ws_ship_mode_sk_node_9=[$14], ws_warehouse_sk_node_9=[$15], ws_promo_sk_node_9=[$16], ws_order_number_node_9=[$17], ws_quantity_node_9=[$18], ws_wholesale_cost_node_9=[$19], ws_list_price_node_9=[$20], ws_sales_price_node_9=[$21], ws_ext_discount_amt_node_9=[$22], ws_ext_sales_price_node_9=[$23], ws_ext_wholesale_cost_node_9=[$24], ws_ext_list_price_node_9=[$25], ws_ext_tax_node_9=[$26], ws_coupon_amt_node_9=[$27], ws_ext_ship_cost_node_9=[$28], ws_net_paid_node_9=[$29], ws_net_paid_inc_tax_node_9=[$30], ws_net_paid_inc_ship_node_9=[$31], ws_net_paid_inc_ship_tax_node_9=[$32], ws_net_profit_node_9=[$33])
               +- LogicalTableScan(table=[[default_catalog, default_database, web_sales]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS web_mkt_id_node_7])
+- HashAggregate(isMerge=[false], groupBy=[i_manufact_node_8], select=[i_manufact_node_8, SUM(web_mkt_id_node_7) AS EXPR$0])
   +- Exchange(distribution=[hash[i_manufact_node_8]])
      +- NestedLoopJoin(joinType=[InnerJoin], where=[=(ws_quantity_node_9, web_open_date_sk_node_7)], select=[web_site_sk_node_7, web_site_id_node_7, web_rec_start_date_node_7, web_rec_end_date_node_7, web_name_node_7, web_open_date_sk_node_7, web_close_date_sk_node_7, web_class_node_7, web_manager_node_7, web_mkt_id_node_7, web_mkt_class_node_7, web_mkt_desc_node_7, web_market_manager_node_7, web_company_id_node_7, web_company_name_node_7, web_street_number_node_7, web_street_name_node_7, web_street_type_node_7, web_suite_number_node_7, web_city_node_7, web_county_node_7, web_state_node_7, web_zip_node_7, web_country_node_7, web_gmt_offset_node_7, web_tax_percentage_node_7, i_item_sk_node_8, i_item_id_node_8, i_rec_start_date_node_8, i_rec_end_date_node_8, i_item_desc_node_8, i_current_price_node_8, i_wholesale_cost_node_8, i_brand_id_node_8, i_brand_node_8, i_class_id_node_8, i_class_node_8, i_category_id_node_8, i_category_node_8, i_manufact_id_node_8, i_manufact_node_8, i_size_node_8, i_formulation_node_8, i_color_node_8, i_units_node_8, i_container_node_8, i_manager_id_node_8, i_product_name_node_8, _c22, H0dhv, ws_sold_time_sk_node_9, ws_ship_date_sk_node_9, ws_item_sk_node_9, ws_bill_customer_sk_node_9, ws_bill_cdemo_sk_node_9, ws_bill_hdemo_sk_node_9, ws_bill_addr_sk_node_9, ws_ship_customer_sk_node_9, ws_ship_cdemo_sk_node_9, ws_ship_hdemo_sk_node_9, ws_ship_addr_sk_node_9, ws_web_page_sk_node_9, ws_web_site_sk_node_9, ws_ship_mode_sk_node_9, ws_warehouse_sk_node_9, ws_promo_sk_node_9, ws_order_number_node_9, ws_quantity_node_9, ws_wholesale_cost_node_9, ws_list_price_node_9, ws_sales_price_node_9, ws_ext_discount_amt_node_9, ws_ext_sales_price_node_9, ws_ext_wholesale_cost_node_9, ws_ext_list_price_node_9, ws_ext_tax_node_9, ws_coupon_amt_node_9, ws_ext_ship_cost_node_9, ws_net_paid_node_9, ws_net_paid_inc_tax_node_9, ws_net_paid_inc_ship_node_9, ws_net_paid_inc_ship_tax_node_9, ws_net_profit_node_9], build=[right])
         :- Calc(select=[web_site_sk_node_7, web_site_id_node_7, web_rec_start_date_node_7, web_rec_end_date_node_7, web_name_node_7, web_open_date_sk_node_7, web_close_date_sk_node_7, web_class_node_7, web_manager_node_7, web_mkt_id_node_7, web_mkt_class_node_7, web_mkt_desc_node_7, web_market_manager_node_7, web_company_id_node_7, web_company_name_node_7, web_street_number_node_7, web_street_name_node_7, web_street_type_node_7, web_suite_number_node_7, web_city_node_7, web_county_node_7, web_state_node_7, web_zip_node_7, web_country_node_7, web_gmt_offset_node_7, web_tax_percentage_node_7, i_item_sk AS i_item_sk_node_8, i_item_id AS i_item_id_node_8, i_rec_start_date AS i_rec_start_date_node_8, i_rec_end_date AS i_rec_end_date_node_8, i_item_desc AS i_item_desc_node_8, i_current_price AS i_current_price_node_8, i_wholesale_cost AS i_wholesale_cost_node_8, i_brand_id AS i_brand_id_node_8, i_brand AS i_brand_node_8, i_class_id AS i_class_id_node_8, i_class AS i_class_node_8, i_category_id AS i_category_id_node_8, i_category AS i_category_node_8, i_manufact_id AS i_manufact_id_node_8, i_manufact AS i_manufact_node_8, i_size AS i_size_node_8, i_formulation AS i_formulation_node_8, i_color AS i_color_node_8, i_units AS i_units_node_8, i_container AS i_container_node_8, i_manager_id AS i_manager_id_node_8, i_product_name AS i_product_name_node_8, 'hello' AS _c22])
         :  +- HashJoin(joinType=[InnerJoin], where=[=(web_gmt_offset_node_70, i_wholesale_cost)], select=[web_site_sk_node_7, web_site_id_node_7, web_rec_start_date_node_7, web_rec_end_date_node_7, web_name_node_7, web_open_date_sk_node_7, web_close_date_sk_node_7, web_class_node_7, web_manager_node_7, web_mkt_id_node_7, web_mkt_class_node_7, web_mkt_desc_node_7, web_market_manager_node_7, web_company_id_node_7, web_company_name_node_7, web_street_number_node_7, web_street_name_node_7, web_street_type_node_7, web_suite_number_node_7, web_city_node_7, web_county_node_7, web_state_node_7, web_zip_node_7, web_country_node_7, web_gmt_offset_node_7, web_tax_percentage_node_7, web_gmt_offset_node_70, i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name], isBroadcast=[true], build=[left])
         :     :- Exchange(distribution=[broadcast])
         :     :  +- Calc(select=[web_site_sk AS web_site_sk_node_7, web_site_id AS web_site_id_node_7, web_rec_start_date AS web_rec_start_date_node_7, web_rec_end_date AS web_rec_end_date_node_7, web_name AS web_name_node_7, web_open_date_sk AS web_open_date_sk_node_7, web_close_date_sk AS web_close_date_sk_node_7, web_class AS web_class_node_7, web_manager AS web_manager_node_7, web_mkt_id AS web_mkt_id_node_7, web_mkt_class AS web_mkt_class_node_7, web_mkt_desc AS web_mkt_desc_node_7, web_market_manager AS web_market_manager_node_7, web_company_id AS web_company_id_node_7, web_company_name AS web_company_name_node_7, web_street_number AS web_street_number_node_7, web_street_name AS web_street_name_node_7, web_street_type AS web_street_type_node_7, web_suite_number AS web_suite_number_node_7, web_city AS web_city_node_7, web_county AS web_county_node_7, web_state AS web_state_node_7, web_zip AS web_zip_node_7, web_country AS web_country_node_7, web_gmt_offset AS web_gmt_offset_node_7, web_tax_percentage AS web_tax_percentage_node_7, CAST(web_gmt_offset AS DECIMAL(7, 2)) AS web_gmt_offset_node_70], where=[>=(web_tax_percentage, -1.8943101167678833E1)])
         :     :     +- TableSourceScan(table=[[default_catalog, default_database, web_site, filter=[>=(web_tax_percentage, -1.8943101167678833E1:DOUBLE)]]], fields=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage])
         :     +- TableSourceScan(table=[[default_catalog, default_database, item]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])
         +- Exchange(distribution=[broadcast])
            +- Calc(select=[ws_sold_date_sk AS H0dhv, ws_sold_time_sk AS ws_sold_time_sk_node_9, ws_ship_date_sk AS ws_ship_date_sk_node_9, ws_item_sk AS ws_item_sk_node_9, ws_bill_customer_sk AS ws_bill_customer_sk_node_9, ws_bill_cdemo_sk AS ws_bill_cdemo_sk_node_9, ws_bill_hdemo_sk AS ws_bill_hdemo_sk_node_9, ws_bill_addr_sk AS ws_bill_addr_sk_node_9, ws_ship_customer_sk AS ws_ship_customer_sk_node_9, ws_ship_cdemo_sk AS ws_ship_cdemo_sk_node_9, ws_ship_hdemo_sk AS ws_ship_hdemo_sk_node_9, ws_ship_addr_sk AS ws_ship_addr_sk_node_9, ws_web_page_sk AS ws_web_page_sk_node_9, ws_web_site_sk AS ws_web_site_sk_node_9, ws_ship_mode_sk AS ws_ship_mode_sk_node_9, ws_warehouse_sk AS ws_warehouse_sk_node_9, ws_promo_sk AS ws_promo_sk_node_9, ws_order_number AS ws_order_number_node_9, ws_quantity AS ws_quantity_node_9, ws_wholesale_cost AS ws_wholesale_cost_node_9, ws_list_price AS ws_list_price_node_9, ws_sales_price AS ws_sales_price_node_9, ws_ext_discount_amt AS ws_ext_discount_amt_node_9, ws_ext_sales_price AS ws_ext_sales_price_node_9, ws_ext_wholesale_cost AS ws_ext_wholesale_cost_node_9, ws_ext_list_price AS ws_ext_list_price_node_9, ws_ext_tax AS ws_ext_tax_node_9, ws_coupon_amt AS ws_coupon_amt_node_9, ws_ext_ship_cost AS ws_ext_ship_cost_node_9, ws_net_paid AS ws_net_paid_node_9, ws_net_paid_inc_tax AS ws_net_paid_inc_tax_node_9, ws_net_paid_inc_ship AS ws_net_paid_inc_ship_node_9, ws_net_paid_inc_ship_tax AS ws_net_paid_inc_ship_tax_node_9, ws_net_profit AS ws_net_profit_node_9])
               +- SortLimit(orderBy=[ws_quantity ASC], offset=[0], fetch=[1], global=[true])
                  +- Exchange(distribution=[single])
                     +- SortLimit(orderBy=[ws_quantity ASC], offset=[0], fetch=[1], global=[false])
                        +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS web_mkt_id_node_7])
+- HashAggregate(isMerge=[false], groupBy=[i_manufact_node_8], select=[i_manufact_node_8, SUM(web_mkt_id_node_7) AS EXPR$0])
   +- Exchange(distribution=[hash[i_manufact_node_8]])
      +- NestedLoopJoin(joinType=[InnerJoin], where=[(ws_quantity_node_9 = web_open_date_sk_node_7)], select=[web_site_sk_node_7, web_site_id_node_7, web_rec_start_date_node_7, web_rec_end_date_node_7, web_name_node_7, web_open_date_sk_node_7, web_close_date_sk_node_7, web_class_node_7, web_manager_node_7, web_mkt_id_node_7, web_mkt_class_node_7, web_mkt_desc_node_7, web_market_manager_node_7, web_company_id_node_7, web_company_name_node_7, web_street_number_node_7, web_street_name_node_7, web_street_type_node_7, web_suite_number_node_7, web_city_node_7, web_county_node_7, web_state_node_7, web_zip_node_7, web_country_node_7, web_gmt_offset_node_7, web_tax_percentage_node_7, i_item_sk_node_8, i_item_id_node_8, i_rec_start_date_node_8, i_rec_end_date_node_8, i_item_desc_node_8, i_current_price_node_8, i_wholesale_cost_node_8, i_brand_id_node_8, i_brand_node_8, i_class_id_node_8, i_class_node_8, i_category_id_node_8, i_category_node_8, i_manufact_id_node_8, i_manufact_node_8, i_size_node_8, i_formulation_node_8, i_color_node_8, i_units_node_8, i_container_node_8, i_manager_id_node_8, i_product_name_node_8, _c22, H0dhv, ws_sold_time_sk_node_9, ws_ship_date_sk_node_9, ws_item_sk_node_9, ws_bill_customer_sk_node_9, ws_bill_cdemo_sk_node_9, ws_bill_hdemo_sk_node_9, ws_bill_addr_sk_node_9, ws_ship_customer_sk_node_9, ws_ship_cdemo_sk_node_9, ws_ship_hdemo_sk_node_9, ws_ship_addr_sk_node_9, ws_web_page_sk_node_9, ws_web_site_sk_node_9, ws_ship_mode_sk_node_9, ws_warehouse_sk_node_9, ws_promo_sk_node_9, ws_order_number_node_9, ws_quantity_node_9, ws_wholesale_cost_node_9, ws_list_price_node_9, ws_sales_price_node_9, ws_ext_discount_amt_node_9, ws_ext_sales_price_node_9, ws_ext_wholesale_cost_node_9, ws_ext_list_price_node_9, ws_ext_tax_node_9, ws_coupon_amt_node_9, ws_ext_ship_cost_node_9, ws_net_paid_node_9, ws_net_paid_inc_tax_node_9, ws_net_paid_inc_ship_node_9, ws_net_paid_inc_ship_tax_node_9, ws_net_profit_node_9], build=[right])
         :- Calc(select=[web_site_sk_node_7, web_site_id_node_7, web_rec_start_date_node_7, web_rec_end_date_node_7, web_name_node_7, web_open_date_sk_node_7, web_close_date_sk_node_7, web_class_node_7, web_manager_node_7, web_mkt_id_node_7, web_mkt_class_node_7, web_mkt_desc_node_7, web_market_manager_node_7, web_company_id_node_7, web_company_name_node_7, web_street_number_node_7, web_street_name_node_7, web_street_type_node_7, web_suite_number_node_7, web_city_node_7, web_county_node_7, web_state_node_7, web_zip_node_7, web_country_node_7, web_gmt_offset_node_7, web_tax_percentage_node_7, i_item_sk AS i_item_sk_node_8, i_item_id AS i_item_id_node_8, i_rec_start_date AS i_rec_start_date_node_8, i_rec_end_date AS i_rec_end_date_node_8, i_item_desc AS i_item_desc_node_8, i_current_price AS i_current_price_node_8, i_wholesale_cost AS i_wholesale_cost_node_8, i_brand_id AS i_brand_id_node_8, i_brand AS i_brand_node_8, i_class_id AS i_class_id_node_8, i_class AS i_class_node_8, i_category_id AS i_category_id_node_8, i_category AS i_category_node_8, i_manufact_id AS i_manufact_id_node_8, i_manufact AS i_manufact_node_8, i_size AS i_size_node_8, i_formulation AS i_formulation_node_8, i_color AS i_color_node_8, i_units AS i_units_node_8, i_container AS i_container_node_8, i_manager_id AS i_manager_id_node_8, i_product_name AS i_product_name_node_8, 'hello' AS _c22])
         :  +- HashJoin(joinType=[InnerJoin], where=[(web_gmt_offset_node_70 = i_wholesale_cost)], select=[web_site_sk_node_7, web_site_id_node_7, web_rec_start_date_node_7, web_rec_end_date_node_7, web_name_node_7, web_open_date_sk_node_7, web_close_date_sk_node_7, web_class_node_7, web_manager_node_7, web_mkt_id_node_7, web_mkt_class_node_7, web_mkt_desc_node_7, web_market_manager_node_7, web_company_id_node_7, web_company_name_node_7, web_street_number_node_7, web_street_name_node_7, web_street_type_node_7, web_suite_number_node_7, web_city_node_7, web_county_node_7, web_state_node_7, web_zip_node_7, web_country_node_7, web_gmt_offset_node_7, web_tax_percentage_node_7, web_gmt_offset_node_70, i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name], isBroadcast=[true], build=[left])
         :     :- Exchange(distribution=[broadcast])
         :     :  +- Calc(select=[web_site_sk AS web_site_sk_node_7, web_site_id AS web_site_id_node_7, web_rec_start_date AS web_rec_start_date_node_7, web_rec_end_date AS web_rec_end_date_node_7, web_name AS web_name_node_7, web_open_date_sk AS web_open_date_sk_node_7, web_close_date_sk AS web_close_date_sk_node_7, web_class AS web_class_node_7, web_manager AS web_manager_node_7, web_mkt_id AS web_mkt_id_node_7, web_mkt_class AS web_mkt_class_node_7, web_mkt_desc AS web_mkt_desc_node_7, web_market_manager AS web_market_manager_node_7, web_company_id AS web_company_id_node_7, web_company_name AS web_company_name_node_7, web_street_number AS web_street_number_node_7, web_street_name AS web_street_name_node_7, web_street_type AS web_street_type_node_7, web_suite_number AS web_suite_number_node_7, web_city AS web_city_node_7, web_county AS web_county_node_7, web_state AS web_state_node_7, web_zip AS web_zip_node_7, web_country AS web_country_node_7, web_gmt_offset AS web_gmt_offset_node_7, web_tax_percentage AS web_tax_percentage_node_7, CAST(web_gmt_offset AS DECIMAL(7, 2)) AS web_gmt_offset_node_70], where=[(web_tax_percentage >= -1.8943101167678833E1)])
         :     :     +- TableSourceScan(table=[[default_catalog, default_database, web_site, filter=[>=(web_tax_percentage, -1.8943101167678833E1:DOUBLE)]]], fields=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage])
         :     +- TableSourceScan(table=[[default_catalog, default_database, item]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])
         +- Exchange(distribution=[broadcast])
            +- Calc(select=[ws_sold_date_sk AS H0dhv, ws_sold_time_sk AS ws_sold_time_sk_node_9, ws_ship_date_sk AS ws_ship_date_sk_node_9, ws_item_sk AS ws_item_sk_node_9, ws_bill_customer_sk AS ws_bill_customer_sk_node_9, ws_bill_cdemo_sk AS ws_bill_cdemo_sk_node_9, ws_bill_hdemo_sk AS ws_bill_hdemo_sk_node_9, ws_bill_addr_sk AS ws_bill_addr_sk_node_9, ws_ship_customer_sk AS ws_ship_customer_sk_node_9, ws_ship_cdemo_sk AS ws_ship_cdemo_sk_node_9, ws_ship_hdemo_sk AS ws_ship_hdemo_sk_node_9, ws_ship_addr_sk AS ws_ship_addr_sk_node_9, ws_web_page_sk AS ws_web_page_sk_node_9, ws_web_site_sk AS ws_web_site_sk_node_9, ws_ship_mode_sk AS ws_ship_mode_sk_node_9, ws_warehouse_sk AS ws_warehouse_sk_node_9, ws_promo_sk AS ws_promo_sk_node_9, ws_order_number AS ws_order_number_node_9, ws_quantity AS ws_quantity_node_9, ws_wholesale_cost AS ws_wholesale_cost_node_9, ws_list_price AS ws_list_price_node_9, ws_sales_price AS ws_sales_price_node_9, ws_ext_discount_amt AS ws_ext_discount_amt_node_9, ws_ext_sales_price AS ws_ext_sales_price_node_9, ws_ext_wholesale_cost AS ws_ext_wholesale_cost_node_9, ws_ext_list_price AS ws_ext_list_price_node_9, ws_ext_tax AS ws_ext_tax_node_9, ws_coupon_amt AS ws_coupon_amt_node_9, ws_ext_ship_cost AS ws_ext_ship_cost_node_9, ws_net_paid AS ws_net_paid_node_9, ws_net_paid_inc_tax AS ws_net_paid_inc_tax_node_9, ws_net_paid_inc_ship AS ws_net_paid_inc_ship_node_9, ws_net_paid_inc_ship_tax AS ws_net_paid_inc_ship_tax_node_9, ws_net_profit AS ws_net_profit_node_9])
               +- SortLimit(orderBy=[ws_quantity ASC], offset=[0], fetch=[1], global=[true])
                  +- Exchange(distribution=[single])
                     +- SortLimit(orderBy=[ws_quantity ASC], offset=[0], fetch=[1], global=[false])
                        +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0