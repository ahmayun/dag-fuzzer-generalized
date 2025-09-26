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

autonode_13 = table_env.from_path("household_demographics").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("household_demographics").get_schema().get_field_names()])
autonode_12 = table_env.from_path("web_page").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("web_page").get_schema().get_field_names()])
autonode_11 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_14 = table_env.from_path("web_site").select(*[col(column_name).alias(f"{column_name}_node_14") for column_name in table_env.from_path("web_site").get_schema().get_field_names()])
autonode_9 = autonode_12.join(autonode_13, col('wp_char_count_node_12') == col('hd_vehicle_count_node_13'))
autonode_8 = autonode_11.order_by(col('ss_addr_sk_node_11'))
autonode_10 = autonode_14.order_by(col('web_close_date_sk_node_14'))
autonode_6 = autonode_9.limit(86)
autonode_5 = autonode_8.order_by(col('ss_ext_list_price_node_11'))
autonode_7 = autonode_10.alias('bBem3')
autonode_3 = autonode_5.alias('2RyT9')
autonode_4 = autonode_6.join(autonode_7, col('wp_url_node_12') == col('web_class_node_14'))
autonode_2 = autonode_3.join(autonode_4, col('ss_ext_tax_node_11') == col('web_tax_percentage_node_14'))
autonode_1 = autonode_2.group_by(col('web_suite_number_node_14')).select(col('ss_quantity_node_11').count.alias('ss_quantity_node_11'))
sink = autonode_1.limit(23)
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
      "error_message": "An error occurred while calling o126997077.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#256016344:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[17](input=RelSubset#256016342,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[17]), rel#256016341:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[17](input=RelSubset#256016340,groupBy=ss_ext_tax,select=ss_ext_tax, Partial_COUNT(ss_quantity) AS count$0)]
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
LogicalSort(fetch=[23])
+- LogicalProject(ss_quantity_node_11=[$1])
   +- LogicalAggregate(group=[{60}], EXPR$0=[COUNT($10)])
      +- LogicalJoin(condition=[=($18, $67)], joinType=[inner])
         :- LogicalProject(2RyT9=[AS($0, _UTF-16LE'2RyT9')], ss_sold_time_sk_node_11=[$1], ss_item_sk_node_11=[$2], ss_customer_sk_node_11=[$3], ss_cdemo_sk_node_11=[$4], ss_hdemo_sk_node_11=[$5], ss_addr_sk_node_11=[$6], ss_store_sk_node_11=[$7], ss_promo_sk_node_11=[$8], ss_ticket_number_node_11=[$9], ss_quantity_node_11=[$10], ss_wholesale_cost_node_11=[$11], ss_list_price_node_11=[$12], ss_sales_price_node_11=[$13], ss_ext_discount_amt_node_11=[$14], ss_ext_sales_price_node_11=[$15], ss_ext_wholesale_cost_node_11=[$16], ss_ext_list_price_node_11=[$17], ss_ext_tax_node_11=[$18], ss_coupon_amt_node_11=[$19], ss_net_paid_node_11=[$20], ss_net_paid_inc_tax_node_11=[$21], ss_net_profit_node_11=[$22])
         :  +- LogicalSort(sort0=[$17], dir0=[ASC])
         :     +- LogicalSort(sort0=[$6], dir0=[ASC])
         :        +- LogicalProject(ss_sold_date_sk_node_11=[$0], ss_sold_time_sk_node_11=[$1], ss_item_sk_node_11=[$2], ss_customer_sk_node_11=[$3], ss_cdemo_sk_node_11=[$4], ss_hdemo_sk_node_11=[$5], ss_addr_sk_node_11=[$6], ss_store_sk_node_11=[$7], ss_promo_sk_node_11=[$8], ss_ticket_number_node_11=[$9], ss_quantity_node_11=[$10], ss_wholesale_cost_node_11=[$11], ss_list_price_node_11=[$12], ss_sales_price_node_11=[$13], ss_ext_discount_amt_node_11=[$14], ss_ext_sales_price_node_11=[$15], ss_ext_wholesale_cost_node_11=[$16], ss_ext_list_price_node_11=[$17], ss_ext_tax_node_11=[$18], ss_coupon_amt_node_11=[$19], ss_net_paid_node_11=[$20], ss_net_paid_inc_tax_node_11=[$21], ss_net_profit_node_11=[$22])
         :           +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])
         +- LogicalJoin(condition=[=($8, $26)], joinType=[inner])
            :- LogicalSort(fetch=[86])
            :  +- LogicalJoin(condition=[=($10, $18)], joinType=[inner])
            :     :- LogicalProject(wp_web_page_sk_node_12=[$0], wp_web_page_id_node_12=[$1], wp_rec_start_date_node_12=[$2], wp_rec_end_date_node_12=[$3], wp_creation_date_sk_node_12=[$4], wp_access_date_sk_node_12=[$5], wp_autogen_flag_node_12=[$6], wp_customer_sk_node_12=[$7], wp_url_node_12=[$8], wp_type_node_12=[$9], wp_char_count_node_12=[$10], wp_link_count_node_12=[$11], wp_image_count_node_12=[$12], wp_max_ad_count_node_12=[$13])
            :     :  +- LogicalTableScan(table=[[default_catalog, default_database, web_page]])
            :     +- LogicalProject(hd_demo_sk_node_13=[$0], hd_income_band_sk_node_13=[$1], hd_buy_potential_node_13=[$2], hd_dep_count_node_13=[$3], hd_vehicle_count_node_13=[$4])
            :        +- LogicalTableScan(table=[[default_catalog, default_database, household_demographics]])
            +- LogicalProject(bBem3=[AS($0, _UTF-16LE'bBem3')], web_site_id_node_14=[$1], web_rec_start_date_node_14=[$2], web_rec_end_date_node_14=[$3], web_name_node_14=[$4], web_open_date_sk_node_14=[$5], web_close_date_sk_node_14=[$6], web_class_node_14=[$7], web_manager_node_14=[$8], web_mkt_id_node_14=[$9], web_mkt_class_node_14=[$10], web_mkt_desc_node_14=[$11], web_market_manager_node_14=[$12], web_company_id_node_14=[$13], web_company_name_node_14=[$14], web_street_number_node_14=[$15], web_street_name_node_14=[$16], web_street_type_node_14=[$17], web_suite_number_node_14=[$18], web_city_node_14=[$19], web_county_node_14=[$20], web_state_node_14=[$21], web_zip_node_14=[$22], web_country_node_14=[$23], web_gmt_offset_node_14=[$24], web_tax_percentage_node_14=[$25])
               +- LogicalSort(sort0=[$6], dir0=[ASC])
                  +- LogicalProject(web_site_sk_node_14=[$0], web_site_id_node_14=[$1], web_rec_start_date_node_14=[$2], web_rec_end_date_node_14=[$3], web_name_node_14=[$4], web_open_date_sk_node_14=[$5], web_close_date_sk_node_14=[$6], web_class_node_14=[$7], web_manager_node_14=[$8], web_mkt_id_node_14=[$9], web_mkt_class_node_14=[$10], web_mkt_desc_node_14=[$11], web_market_manager_node_14=[$12], web_company_id_node_14=[$13], web_company_name_node_14=[$14], web_street_number_node_14=[$15], web_street_name_node_14=[$16], web_street_type_node_14=[$17], web_suite_number_node_14=[$18], web_city_node_14=[$19], web_county_node_14=[$20], web_state_node_14=[$21], web_zip_node_14=[$22], web_country_node_14=[$23], web_gmt_offset_node_14=[$24], web_tax_percentage_node_14=[$25])
                     +- LogicalTableScan(table=[[default_catalog, default_database, web_site]])

== Optimized Physical Plan ==
Limit(offset=[0], fetch=[23], global=[true])
+- Exchange(distribution=[single])
   +- Limit(offset=[0], fetch=[23], global=[false])
      +- Calc(select=[EXPR$0 AS ss_quantity_node_11])
         +- SortAggregate(isMerge=[false], groupBy=[web_suite_number_node_14], select=[web_suite_number_node_14, COUNT(ss_quantity_node_11) AS EXPR$0])
            +- Sort(orderBy=[web_suite_number_node_14 ASC])
               +- Exchange(distribution=[hash[web_suite_number_node_14]])
                  +- Calc(select=[2RyT9, ss_sold_time_sk_node_11, ss_item_sk_node_11, ss_customer_sk_node_11, ss_cdemo_sk_node_11, ss_hdemo_sk_node_11, ss_addr_sk_node_11, ss_store_sk_node_11, ss_promo_sk_node_11, ss_ticket_number_node_11, ss_quantity_node_11, ss_wholesale_cost_node_11, ss_list_price_node_11, ss_sales_price_node_11, ss_ext_discount_amt_node_11, ss_ext_sales_price_node_11, ss_ext_wholesale_cost_node_11, ss_ext_list_price_node_11, ss_ext_tax_node_11, ss_coupon_amt_node_11, ss_net_paid_node_11, ss_net_paid_inc_tax_node_11, ss_net_profit_node_11, wp_web_page_sk_node_12, wp_web_page_id_node_12, wp_rec_start_date_node_12, wp_rec_end_date_node_12, wp_creation_date_sk_node_12, wp_access_date_sk_node_12, wp_autogen_flag_node_12, wp_customer_sk_node_12, wp_url_node_12, wp_type_node_12, wp_char_count_node_12, wp_link_count_node_12, wp_image_count_node_12, wp_max_ad_count_node_12, hd_demo_sk_node_13, hd_income_band_sk_node_13, hd_buy_potential_node_13, hd_dep_count_node_13, hd_vehicle_count_node_13, bBem3, web_site_id_node_14, web_rec_start_date_node_14, web_rec_end_date_node_14, web_name_node_14, web_open_date_sk_node_14, web_close_date_sk_node_14, web_class_node_14, web_manager_node_14, web_mkt_id_node_14, web_mkt_class_node_14, web_mkt_desc_node_14, web_market_manager_node_14, web_company_id_node_14, web_company_name_node_14, web_street_number_node_14, web_street_name_node_14, web_street_type_node_14, web_suite_number_node_14, web_city_node_14, web_county_node_14, web_state_node_14, web_zip_node_14, web_country_node_14, web_gmt_offset_node_14, web_tax_percentage_node_14])
                     +- NestedLoopJoin(joinType=[InnerJoin], where=[=(ss_ext_tax_node_11, web_tax_percentage_node_140)], select=[2RyT9, ss_sold_time_sk_node_11, ss_item_sk_node_11, ss_customer_sk_node_11, ss_cdemo_sk_node_11, ss_hdemo_sk_node_11, ss_addr_sk_node_11, ss_store_sk_node_11, ss_promo_sk_node_11, ss_ticket_number_node_11, ss_quantity_node_11, ss_wholesale_cost_node_11, ss_list_price_node_11, ss_sales_price_node_11, ss_ext_discount_amt_node_11, ss_ext_sales_price_node_11, ss_ext_wholesale_cost_node_11, ss_ext_list_price_node_11, ss_ext_tax_node_11, ss_coupon_amt_node_11, ss_net_paid_node_11, ss_net_paid_inc_tax_node_11, ss_net_profit_node_11, wp_web_page_sk_node_12, wp_web_page_id_node_12, wp_rec_start_date_node_12, wp_rec_end_date_node_12, wp_creation_date_sk_node_12, wp_access_date_sk_node_12, wp_autogen_flag_node_12, wp_customer_sk_node_12, wp_url_node_12, wp_type_node_12, wp_char_count_node_12, wp_link_count_node_12, wp_image_count_node_12, wp_max_ad_count_node_12, hd_demo_sk_node_13, hd_income_band_sk_node_13, hd_buy_potential_node_13, hd_dep_count_node_13, hd_vehicle_count_node_13, bBem3, web_site_id_node_14, web_rec_start_date_node_14, web_rec_end_date_node_14, web_name_node_14, web_open_date_sk_node_14, web_close_date_sk_node_14, web_class_node_14, web_manager_node_14, web_mkt_id_node_14, web_mkt_class_node_14, web_mkt_desc_node_14, web_market_manager_node_14, web_company_id_node_14, web_company_name_node_14, web_street_number_node_14, web_street_name_node_14, web_street_type_node_14, web_suite_number_node_14, web_city_node_14, web_county_node_14, web_state_node_14, web_zip_node_14, web_country_node_14, web_gmt_offset_node_14, web_tax_percentage_node_14, web_tax_percentage_node_140], build=[left])
                        :- Exchange(distribution=[broadcast])
                        :  +- Calc(select=[ss_sold_date_sk AS 2RyT9, ss_sold_time_sk AS ss_sold_time_sk_node_11, ss_item_sk AS ss_item_sk_node_11, ss_customer_sk AS ss_customer_sk_node_11, ss_cdemo_sk AS ss_cdemo_sk_node_11, ss_hdemo_sk AS ss_hdemo_sk_node_11, ss_addr_sk AS ss_addr_sk_node_11, ss_store_sk AS ss_store_sk_node_11, ss_promo_sk AS ss_promo_sk_node_11, ss_ticket_number AS ss_ticket_number_node_11, ss_quantity AS ss_quantity_node_11, ss_wholesale_cost AS ss_wholesale_cost_node_11, ss_list_price AS ss_list_price_node_11, ss_sales_price AS ss_sales_price_node_11, ss_ext_discount_amt AS ss_ext_discount_amt_node_11, ss_ext_sales_price AS ss_ext_sales_price_node_11, ss_ext_wholesale_cost AS ss_ext_wholesale_cost_node_11, ss_ext_list_price AS ss_ext_list_price_node_11, ss_ext_tax AS ss_ext_tax_node_11, ss_coupon_amt AS ss_coupon_amt_node_11, ss_net_paid AS ss_net_paid_node_11, ss_net_paid_inc_tax AS ss_net_paid_inc_tax_node_11, ss_net_profit AS ss_net_profit_node_11])
                        :     +- SortLimit(orderBy=[ss_ext_list_price ASC], offset=[0], fetch=[1], global=[true])
                        :        +- Exchange(distribution=[single])
                        :           +- SortLimit(orderBy=[ss_ext_list_price ASC], offset=[0], fetch=[1], global=[false])
                        :              +- SortLimit(orderBy=[ss_addr_sk ASC], offset=[0], fetch=[1], global=[true])
                        :                 +- Exchange(distribution=[single])
                        :                    +- SortLimit(orderBy=[ss_addr_sk ASC], offset=[0], fetch=[1], global=[false])
                        :                       +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
                        +- Calc(select=[wp_web_page_sk AS wp_web_page_sk_node_12, wp_web_page_id AS wp_web_page_id_node_12, wp_rec_start_date AS wp_rec_start_date_node_12, wp_rec_end_date AS wp_rec_end_date_node_12, wp_creation_date_sk AS wp_creation_date_sk_node_12, wp_access_date_sk AS wp_access_date_sk_node_12, wp_autogen_flag AS wp_autogen_flag_node_12, wp_customer_sk AS wp_customer_sk_node_12, wp_url AS wp_url_node_12, wp_type AS wp_type_node_12, wp_char_count AS wp_char_count_node_12, wp_link_count AS wp_link_count_node_12, wp_image_count AS wp_image_count_node_12, wp_max_ad_count AS wp_max_ad_count_node_12, hd_demo_sk AS hd_demo_sk_node_13, hd_income_band_sk AS hd_income_band_sk_node_13, hd_buy_potential AS hd_buy_potential_node_13, hd_dep_count AS hd_dep_count_node_13, hd_vehicle_count AS hd_vehicle_count_node_13, bBem3, web_site_id_node_14, web_rec_start_date_node_14, web_rec_end_date_node_14, web_name_node_14, web_open_date_sk_node_14, web_close_date_sk_node_14, web_class_node_14, web_manager_node_14, web_mkt_id_node_14, web_mkt_class_node_14, web_mkt_desc_node_14, web_market_manager_node_14, web_company_id_node_14, web_company_name_node_14, web_street_number_node_14, web_street_name_node_14, web_street_type_node_14, web_suite_number_node_14, web_city_node_14, web_county_node_14, web_state_node_14, web_zip_node_14, web_country_node_14, web_gmt_offset_node_14, web_tax_percentage_node_14, CAST(web_tax_percentage_node_14 AS DECIMAL(7, 2)) AS web_tax_percentage_node_140])
                           +- NestedLoopJoin(joinType=[InnerJoin], where=[=(wp_url, web_class_node_14)], select=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count, hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, bBem3, web_site_id_node_14, web_rec_start_date_node_14, web_rec_end_date_node_14, web_name_node_14, web_open_date_sk_node_14, web_close_date_sk_node_14, web_class_node_14, web_manager_node_14, web_mkt_id_node_14, web_mkt_class_node_14, web_mkt_desc_node_14, web_market_manager_node_14, web_company_id_node_14, web_company_name_node_14, web_street_number_node_14, web_street_name_node_14, web_street_type_node_14, web_suite_number_node_14, web_city_node_14, web_county_node_14, web_state_node_14, web_zip_node_14, web_country_node_14, web_gmt_offset_node_14, web_tax_percentage_node_14], build=[right])
                              :- Limit(offset=[0], fetch=[86], global=[true])
                              :  +- Exchange(distribution=[single])
                              :     +- Limit(offset=[0], fetch=[86], global=[false])
                              :        +- HashJoin(joinType=[InnerJoin], where=[=(wp_char_count, hd_vehicle_count)], select=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count, hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count], isBroadcast=[true], build=[left])
                              :           :- Exchange(distribution=[broadcast])
                              :           :  +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
                              :           +- TableSourceScan(table=[[default_catalog, default_database, household_demographics]], fields=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
                              +- Exchange(distribution=[broadcast])
                                 +- Calc(select=[web_site_sk AS bBem3, web_site_id AS web_site_id_node_14, web_rec_start_date AS web_rec_start_date_node_14, web_rec_end_date AS web_rec_end_date_node_14, web_name AS web_name_node_14, web_open_date_sk AS web_open_date_sk_node_14, web_close_date_sk AS web_close_date_sk_node_14, web_class AS web_class_node_14, web_manager AS web_manager_node_14, web_mkt_id AS web_mkt_id_node_14, web_mkt_class AS web_mkt_class_node_14, web_mkt_desc AS web_mkt_desc_node_14, web_market_manager AS web_market_manager_node_14, web_company_id AS web_company_id_node_14, web_company_name AS web_company_name_node_14, web_street_number AS web_street_number_node_14, web_street_name AS web_street_name_node_14, web_street_type AS web_street_type_node_14, web_suite_number AS web_suite_number_node_14, web_city AS web_city_node_14, web_county AS web_county_node_14, web_state AS web_state_node_14, web_zip AS web_zip_node_14, web_country AS web_country_node_14, web_gmt_offset AS web_gmt_offset_node_14, web_tax_percentage AS web_tax_percentage_node_14])
                                    +- SortLimit(orderBy=[web_close_date_sk ASC], offset=[0], fetch=[1], global=[true])
                                       +- Exchange(distribution=[single])
                                          +- SortLimit(orderBy=[web_close_date_sk ASC], offset=[0], fetch=[1], global=[false])
                                             +- TableSourceScan(table=[[default_catalog, default_database, web_site]], fields=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage])

== Optimized Execution Plan ==
Limit(offset=[0], fetch=[23], global=[true])
+- Exchange(distribution=[single])
   +- Limit(offset=[0], fetch=[23], global=[false])
      +- Calc(select=[EXPR$0 AS ss_quantity_node_11])
         +- SortAggregate(isMerge=[false], groupBy=[web_suite_number_node_14], select=[web_suite_number_node_14, COUNT(ss_quantity_node_11) AS EXPR$0])
            +- Exchange(distribution=[forward])
               +- Sort(orderBy=[web_suite_number_node_14 ASC])
                  +- Exchange(distribution=[hash[web_suite_number_node_14]])
                     +- Calc(select=[2RyT9, ss_sold_time_sk_node_11, ss_item_sk_node_11, ss_customer_sk_node_11, ss_cdemo_sk_node_11, ss_hdemo_sk_node_11, ss_addr_sk_node_11, ss_store_sk_node_11, ss_promo_sk_node_11, ss_ticket_number_node_11, ss_quantity_node_11, ss_wholesale_cost_node_11, ss_list_price_node_11, ss_sales_price_node_11, ss_ext_discount_amt_node_11, ss_ext_sales_price_node_11, ss_ext_wholesale_cost_node_11, ss_ext_list_price_node_11, ss_ext_tax_node_11, ss_coupon_amt_node_11, ss_net_paid_node_11, ss_net_paid_inc_tax_node_11, ss_net_profit_node_11, wp_web_page_sk_node_12, wp_web_page_id_node_12, wp_rec_start_date_node_12, wp_rec_end_date_node_12, wp_creation_date_sk_node_12, wp_access_date_sk_node_12, wp_autogen_flag_node_12, wp_customer_sk_node_12, wp_url_node_12, wp_type_node_12, wp_char_count_node_12, wp_link_count_node_12, wp_image_count_node_12, wp_max_ad_count_node_12, hd_demo_sk_node_13, hd_income_band_sk_node_13, hd_buy_potential_node_13, hd_dep_count_node_13, hd_vehicle_count_node_13, bBem3, web_site_id_node_14, web_rec_start_date_node_14, web_rec_end_date_node_14, web_name_node_14, web_open_date_sk_node_14, web_close_date_sk_node_14, web_class_node_14, web_manager_node_14, web_mkt_id_node_14, web_mkt_class_node_14, web_mkt_desc_node_14, web_market_manager_node_14, web_company_id_node_14, web_company_name_node_14, web_street_number_node_14, web_street_name_node_14, web_street_type_node_14, web_suite_number_node_14, web_city_node_14, web_county_node_14, web_state_node_14, web_zip_node_14, web_country_node_14, web_gmt_offset_node_14, web_tax_percentage_node_14])
                        +- NestedLoopJoin(joinType=[InnerJoin], where=[(ss_ext_tax_node_11 = web_tax_percentage_node_140)], select=[2RyT9, ss_sold_time_sk_node_11, ss_item_sk_node_11, ss_customer_sk_node_11, ss_cdemo_sk_node_11, ss_hdemo_sk_node_11, ss_addr_sk_node_11, ss_store_sk_node_11, ss_promo_sk_node_11, ss_ticket_number_node_11, ss_quantity_node_11, ss_wholesale_cost_node_11, ss_list_price_node_11, ss_sales_price_node_11, ss_ext_discount_amt_node_11, ss_ext_sales_price_node_11, ss_ext_wholesale_cost_node_11, ss_ext_list_price_node_11, ss_ext_tax_node_11, ss_coupon_amt_node_11, ss_net_paid_node_11, ss_net_paid_inc_tax_node_11, ss_net_profit_node_11, wp_web_page_sk_node_12, wp_web_page_id_node_12, wp_rec_start_date_node_12, wp_rec_end_date_node_12, wp_creation_date_sk_node_12, wp_access_date_sk_node_12, wp_autogen_flag_node_12, wp_customer_sk_node_12, wp_url_node_12, wp_type_node_12, wp_char_count_node_12, wp_link_count_node_12, wp_image_count_node_12, wp_max_ad_count_node_12, hd_demo_sk_node_13, hd_income_band_sk_node_13, hd_buy_potential_node_13, hd_dep_count_node_13, hd_vehicle_count_node_13, bBem3, web_site_id_node_14, web_rec_start_date_node_14, web_rec_end_date_node_14, web_name_node_14, web_open_date_sk_node_14, web_close_date_sk_node_14, web_class_node_14, web_manager_node_14, web_mkt_id_node_14, web_mkt_class_node_14, web_mkt_desc_node_14, web_market_manager_node_14, web_company_id_node_14, web_company_name_node_14, web_street_number_node_14, web_street_name_node_14, web_street_type_node_14, web_suite_number_node_14, web_city_node_14, web_county_node_14, web_state_node_14, web_zip_node_14, web_country_node_14, web_gmt_offset_node_14, web_tax_percentage_node_14, web_tax_percentage_node_140], build=[left])
                           :- Exchange(distribution=[broadcast])
                           :  +- Calc(select=[ss_sold_date_sk AS 2RyT9, ss_sold_time_sk AS ss_sold_time_sk_node_11, ss_item_sk AS ss_item_sk_node_11, ss_customer_sk AS ss_customer_sk_node_11, ss_cdemo_sk AS ss_cdemo_sk_node_11, ss_hdemo_sk AS ss_hdemo_sk_node_11, ss_addr_sk AS ss_addr_sk_node_11, ss_store_sk AS ss_store_sk_node_11, ss_promo_sk AS ss_promo_sk_node_11, ss_ticket_number AS ss_ticket_number_node_11, ss_quantity AS ss_quantity_node_11, ss_wholesale_cost AS ss_wholesale_cost_node_11, ss_list_price AS ss_list_price_node_11, ss_sales_price AS ss_sales_price_node_11, ss_ext_discount_amt AS ss_ext_discount_amt_node_11, ss_ext_sales_price AS ss_ext_sales_price_node_11, ss_ext_wholesale_cost AS ss_ext_wholesale_cost_node_11, ss_ext_list_price AS ss_ext_list_price_node_11, ss_ext_tax AS ss_ext_tax_node_11, ss_coupon_amt AS ss_coupon_amt_node_11, ss_net_paid AS ss_net_paid_node_11, ss_net_paid_inc_tax AS ss_net_paid_inc_tax_node_11, ss_net_profit AS ss_net_profit_node_11])
                           :     +- SortLimit(orderBy=[ss_ext_list_price ASC], offset=[0], fetch=[1], global=[true])
                           :        +- Exchange(distribution=[single])
                           :           +- SortLimit(orderBy=[ss_ext_list_price ASC], offset=[0], fetch=[1], global=[false])
                           :              +- SortLimit(orderBy=[ss_addr_sk ASC], offset=[0], fetch=[1], global=[true])
                           :                 +- Exchange(distribution=[single])
                           :                    +- SortLimit(orderBy=[ss_addr_sk ASC], offset=[0], fetch=[1], global=[false])
                           :                       +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
                           +- Calc(select=[wp_web_page_sk AS wp_web_page_sk_node_12, wp_web_page_id AS wp_web_page_id_node_12, wp_rec_start_date AS wp_rec_start_date_node_12, wp_rec_end_date AS wp_rec_end_date_node_12, wp_creation_date_sk AS wp_creation_date_sk_node_12, wp_access_date_sk AS wp_access_date_sk_node_12, wp_autogen_flag AS wp_autogen_flag_node_12, wp_customer_sk AS wp_customer_sk_node_12, wp_url AS wp_url_node_12, wp_type AS wp_type_node_12, wp_char_count AS wp_char_count_node_12, wp_link_count AS wp_link_count_node_12, wp_image_count AS wp_image_count_node_12, wp_max_ad_count AS wp_max_ad_count_node_12, hd_demo_sk AS hd_demo_sk_node_13, hd_income_band_sk AS hd_income_band_sk_node_13, hd_buy_potential AS hd_buy_potential_node_13, hd_dep_count AS hd_dep_count_node_13, hd_vehicle_count AS hd_vehicle_count_node_13, bBem3, web_site_id_node_14, web_rec_start_date_node_14, web_rec_end_date_node_14, web_name_node_14, web_open_date_sk_node_14, web_close_date_sk_node_14, web_class_node_14, web_manager_node_14, web_mkt_id_node_14, web_mkt_class_node_14, web_mkt_desc_node_14, web_market_manager_node_14, web_company_id_node_14, web_company_name_node_14, web_street_number_node_14, web_street_name_node_14, web_street_type_node_14, web_suite_number_node_14, web_city_node_14, web_county_node_14, web_state_node_14, web_zip_node_14, web_country_node_14, web_gmt_offset_node_14, web_tax_percentage_node_14, CAST(web_tax_percentage_node_14 AS DECIMAL(7, 2)) AS web_tax_percentage_node_140])
                              +- NestedLoopJoin(joinType=[InnerJoin], where=[(wp_url = web_class_node_14)], select=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count, hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, bBem3, web_site_id_node_14, web_rec_start_date_node_14, web_rec_end_date_node_14, web_name_node_14, web_open_date_sk_node_14, web_close_date_sk_node_14, web_class_node_14, web_manager_node_14, web_mkt_id_node_14, web_mkt_class_node_14, web_mkt_desc_node_14, web_market_manager_node_14, web_company_id_node_14, web_company_name_node_14, web_street_number_node_14, web_street_name_node_14, web_street_type_node_14, web_suite_number_node_14, web_city_node_14, web_county_node_14, web_state_node_14, web_zip_node_14, web_country_node_14, web_gmt_offset_node_14, web_tax_percentage_node_14], build=[right])
                                 :- Limit(offset=[0], fetch=[86], global=[true])
                                 :  +- Exchange(distribution=[single])
                                 :     +- Limit(offset=[0], fetch=[86], global=[false])
                                 :        +- HashJoin(joinType=[InnerJoin], where=[(wp_char_count = hd_vehicle_count)], select=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count, hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count], isBroadcast=[true], build=[left])
                                 :           :- Exchange(distribution=[broadcast])
                                 :           :  +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
                                 :           +- TableSourceScan(table=[[default_catalog, default_database, household_demographics]], fields=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
                                 +- Exchange(distribution=[broadcast])
                                    +- Calc(select=[web_site_sk AS bBem3, web_site_id AS web_site_id_node_14, web_rec_start_date AS web_rec_start_date_node_14, web_rec_end_date AS web_rec_end_date_node_14, web_name AS web_name_node_14, web_open_date_sk AS web_open_date_sk_node_14, web_close_date_sk AS web_close_date_sk_node_14, web_class AS web_class_node_14, web_manager AS web_manager_node_14, web_mkt_id AS web_mkt_id_node_14, web_mkt_class AS web_mkt_class_node_14, web_mkt_desc AS web_mkt_desc_node_14, web_market_manager AS web_market_manager_node_14, web_company_id AS web_company_id_node_14, web_company_name AS web_company_name_node_14, web_street_number AS web_street_number_node_14, web_street_name AS web_street_name_node_14, web_street_type AS web_street_type_node_14, web_suite_number AS web_suite_number_node_14, web_city AS web_city_node_14, web_county AS web_county_node_14, web_state AS web_state_node_14, web_zip AS web_zip_node_14, web_country AS web_country_node_14, web_gmt_offset AS web_gmt_offset_node_14, web_tax_percentage AS web_tax_percentage_node_14])
                                       +- SortLimit(orderBy=[web_close_date_sk ASC], offset=[0], fetch=[1], global=[true])
                                          +- Exchange(distribution=[single])
                                             +- SortLimit(orderBy=[web_close_date_sk ASC], offset=[0], fetch=[1], global=[false])
                                                +- TableSourceScan(table=[[default_catalog, default_database, web_site]], fields=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0