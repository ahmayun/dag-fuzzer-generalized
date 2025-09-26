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
    return values.min()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_9 = table_env.from_path("customer_address").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("customer_address").get_schema().get_field_names()])
autonode_8 = table_env.from_path("web_page").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("web_page").get_schema().get_field_names()])
autonode_7 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_6 = autonode_8.join(autonode_9, col('ca_state_node_9') == col('wp_autogen_flag_node_8'))
autonode_5 = autonode_7.add_columns(lit("hello"))
autonode_4 = autonode_6.alias('OELQP')
autonode_3 = autonode_5.order_by(col('ss_customer_sk_node_7'))
autonode_2 = autonode_3.join(autonode_4, col('ca_address_sk_node_9') == col('ss_promo_sk_node_7'))
autonode_1 = autonode_2.group_by(col('ss_store_sk_node_7')).select(col('wp_link_count_node_8').count.alias('wp_link_count_node_8'))
sink = autonode_1.add_columns(lit("hello"))
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
LogicalProject(wp_link_count_node_8=[$1], _c1=[_UTF-16LE'hello'])
+- LogicalAggregate(group=[{7}], EXPR$0=[COUNT($35)])
   +- LogicalJoin(condition=[=($38, $8)], joinType=[inner])
      :- LogicalSort(sort0=[$3], dir0=[ASC])
      :  +- LogicalProject(ss_sold_date_sk_node_7=[$0], ss_sold_time_sk_node_7=[$1], ss_item_sk_node_7=[$2], ss_customer_sk_node_7=[$3], ss_cdemo_sk_node_7=[$4], ss_hdemo_sk_node_7=[$5], ss_addr_sk_node_7=[$6], ss_store_sk_node_7=[$7], ss_promo_sk_node_7=[$8], ss_ticket_number_node_7=[$9], ss_quantity_node_7=[$10], ss_wholesale_cost_node_7=[$11], ss_list_price_node_7=[$12], ss_sales_price_node_7=[$13], ss_ext_discount_amt_node_7=[$14], ss_ext_sales_price_node_7=[$15], ss_ext_wholesale_cost_node_7=[$16], ss_ext_list_price_node_7=[$17], ss_ext_tax_node_7=[$18], ss_coupon_amt_node_7=[$19], ss_net_paid_node_7=[$20], ss_net_paid_inc_tax_node_7=[$21], ss_net_profit_node_7=[$22], _c23=[_UTF-16LE'hello'])
      :     +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])
      +- LogicalProject(OELQP=[AS($0, _UTF-16LE'OELQP')], wp_web_page_id_node_8=[$1], wp_rec_start_date_node_8=[$2], wp_rec_end_date_node_8=[$3], wp_creation_date_sk_node_8=[$4], wp_access_date_sk_node_8=[$5], wp_autogen_flag_node_8=[$6], wp_customer_sk_node_8=[$7], wp_url_node_8=[$8], wp_type_node_8=[$9], wp_char_count_node_8=[$10], wp_link_count_node_8=[$11], wp_image_count_node_8=[$12], wp_max_ad_count_node_8=[$13], ca_address_sk_node_9=[$14], ca_address_id_node_9=[$15], ca_street_number_node_9=[$16], ca_street_name_node_9=[$17], ca_street_type_node_9=[$18], ca_suite_number_node_9=[$19], ca_city_node_9=[$20], ca_county_node_9=[$21], ca_state_node_9=[$22], ca_zip_node_9=[$23], ca_country_node_9=[$24], ca_gmt_offset_node_9=[$25], ca_location_type_node_9=[$26])
         +- LogicalJoin(condition=[=($22, $6)], joinType=[inner])
            :- LogicalProject(wp_web_page_sk_node_8=[$0], wp_web_page_id_node_8=[$1], wp_rec_start_date_node_8=[$2], wp_rec_end_date_node_8=[$3], wp_creation_date_sk_node_8=[$4], wp_access_date_sk_node_8=[$5], wp_autogen_flag_node_8=[$6], wp_customer_sk_node_8=[$7], wp_url_node_8=[$8], wp_type_node_8=[$9], wp_char_count_node_8=[$10], wp_link_count_node_8=[$11], wp_image_count_node_8=[$12], wp_max_ad_count_node_8=[$13])
            :  +- LogicalTableScan(table=[[default_catalog, default_database, web_page]])
            +- LogicalProject(ca_address_sk_node_9=[$0], ca_address_id_node_9=[$1], ca_street_number_node_9=[$2], ca_street_name_node_9=[$3], ca_street_type_node_9=[$4], ca_suite_number_node_9=[$5], ca_city_node_9=[$6], ca_county_node_9=[$7], ca_state_node_9=[$8], ca_zip_node_9=[$9], ca_country_node_9=[$10], ca_gmt_offset_node_9=[$11], ca_location_type_node_9=[$12])
               +- LogicalTableScan(table=[[default_catalog, default_database, customer_address]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS wp_link_count_node_8, 'hello' AS _c1])
+- HashAggregate(isMerge=[true], groupBy=[ss_store_sk_node_7], select=[ss_store_sk_node_7, Final_COUNT(count$0) AS EXPR$0])
   +- Exchange(distribution=[hash[ss_store_sk_node_7]])
      +- LocalHashAggregate(groupBy=[ss_store_sk_node_7], select=[ss_store_sk_node_7, Partial_COUNT(wp_link_count_node_8) AS count$0])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[=(ca_address_sk_node_9, ss_promo_sk_node_7)], select=[ss_sold_date_sk_node_7, ss_sold_time_sk_node_7, ss_item_sk_node_7, ss_customer_sk_node_7, ss_cdemo_sk_node_7, ss_hdemo_sk_node_7, ss_addr_sk_node_7, ss_store_sk_node_7, ss_promo_sk_node_7, ss_ticket_number_node_7, ss_quantity_node_7, ss_wholesale_cost_node_7, ss_list_price_node_7, ss_sales_price_node_7, ss_ext_discount_amt_node_7, ss_ext_sales_price_node_7, ss_ext_wholesale_cost_node_7, ss_ext_list_price_node_7, ss_ext_tax_node_7, ss_coupon_amt_node_7, ss_net_paid_node_7, ss_net_paid_inc_tax_node_7, ss_net_profit_node_7, _c23, OELQP, wp_web_page_id_node_8, wp_rec_start_date_node_8, wp_rec_end_date_node_8, wp_creation_date_sk_node_8, wp_access_date_sk_node_8, wp_autogen_flag_node_8, wp_customer_sk_node_8, wp_url_node_8, wp_type_node_8, wp_char_count_node_8, wp_link_count_node_8, wp_image_count_node_8, wp_max_ad_count_node_8, ca_address_sk_node_9, ca_address_id_node_9, ca_street_number_node_9, ca_street_name_node_9, ca_street_type_node_9, ca_suite_number_node_9, ca_city_node_9, ca_county_node_9, ca_state_node_9, ca_zip_node_9, ca_country_node_9, ca_gmt_offset_node_9, ca_location_type_node_9], build=[right])
            :- Sort(orderBy=[ss_customer_sk_node_7 ASC])
            :  +- Calc(select=[ss_sold_date_sk AS ss_sold_date_sk_node_7, ss_sold_time_sk AS ss_sold_time_sk_node_7, ss_item_sk AS ss_item_sk_node_7, ss_customer_sk AS ss_customer_sk_node_7, ss_cdemo_sk AS ss_cdemo_sk_node_7, ss_hdemo_sk AS ss_hdemo_sk_node_7, ss_addr_sk AS ss_addr_sk_node_7, ss_store_sk AS ss_store_sk_node_7, ss_promo_sk AS ss_promo_sk_node_7, ss_ticket_number AS ss_ticket_number_node_7, ss_quantity AS ss_quantity_node_7, ss_wholesale_cost AS ss_wholesale_cost_node_7, ss_list_price AS ss_list_price_node_7, ss_sales_price AS ss_sales_price_node_7, ss_ext_discount_amt AS ss_ext_discount_amt_node_7, ss_ext_sales_price AS ss_ext_sales_price_node_7, ss_ext_wholesale_cost AS ss_ext_wholesale_cost_node_7, ss_ext_list_price AS ss_ext_list_price_node_7, ss_ext_tax AS ss_ext_tax_node_7, ss_coupon_amt AS ss_coupon_amt_node_7, ss_net_paid AS ss_net_paid_node_7, ss_net_paid_inc_tax AS ss_net_paid_inc_tax_node_7, ss_net_profit AS ss_net_profit_node_7, 'hello' AS _c23])
            :     +- Exchange(distribution=[single])
            :        +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
            +- Exchange(distribution=[broadcast])
               +- Calc(select=[wp_web_page_sk AS OELQP, wp_web_page_id AS wp_web_page_id_node_8, wp_rec_start_date AS wp_rec_start_date_node_8, wp_rec_end_date AS wp_rec_end_date_node_8, wp_creation_date_sk AS wp_creation_date_sk_node_8, wp_access_date_sk AS wp_access_date_sk_node_8, wp_autogen_flag AS wp_autogen_flag_node_8, wp_customer_sk AS wp_customer_sk_node_8, wp_url AS wp_url_node_8, wp_type AS wp_type_node_8, wp_char_count AS wp_char_count_node_8, wp_link_count AS wp_link_count_node_8, wp_image_count AS wp_image_count_node_8, wp_max_ad_count AS wp_max_ad_count_node_8, ca_address_sk AS ca_address_sk_node_9, ca_address_id AS ca_address_id_node_9, ca_street_number AS ca_street_number_node_9, ca_street_name AS ca_street_name_node_9, ca_street_type AS ca_street_type_node_9, ca_suite_number AS ca_suite_number_node_9, ca_city AS ca_city_node_9, ca_county AS ca_county_node_9, ca_state AS ca_state_node_9, ca_zip AS ca_zip_node_9, ca_country AS ca_country_node_9, ca_gmt_offset AS ca_gmt_offset_node_9, ca_location_type AS ca_location_type_node_9])
                  +- HashJoin(joinType=[InnerJoin], where=[=(ca_state, wp_autogen_flag)], select=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count, ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type], isBroadcast=[true], build=[left])
                     :- Exchange(distribution=[broadcast])
                     :  +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
                     +- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS wp_link_count_node_8, 'hello' AS _c1])
+- HashAggregate(isMerge=[true], groupBy=[ss_store_sk_node_7], select=[ss_store_sk_node_7, Final_COUNT(count$0) AS EXPR$0])
   +- Exchange(distribution=[hash[ss_store_sk_node_7]])
      +- LocalHashAggregate(groupBy=[ss_store_sk_node_7], select=[ss_store_sk_node_7, Partial_COUNT(wp_link_count_node_8) AS count$0])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[(ca_address_sk_node_9 = ss_promo_sk_node_7)], select=[ss_sold_date_sk_node_7, ss_sold_time_sk_node_7, ss_item_sk_node_7, ss_customer_sk_node_7, ss_cdemo_sk_node_7, ss_hdemo_sk_node_7, ss_addr_sk_node_7, ss_store_sk_node_7, ss_promo_sk_node_7, ss_ticket_number_node_7, ss_quantity_node_7, ss_wholesale_cost_node_7, ss_list_price_node_7, ss_sales_price_node_7, ss_ext_discount_amt_node_7, ss_ext_sales_price_node_7, ss_ext_wholesale_cost_node_7, ss_ext_list_price_node_7, ss_ext_tax_node_7, ss_coupon_amt_node_7, ss_net_paid_node_7, ss_net_paid_inc_tax_node_7, ss_net_profit_node_7, _c23, OELQP, wp_web_page_id_node_8, wp_rec_start_date_node_8, wp_rec_end_date_node_8, wp_creation_date_sk_node_8, wp_access_date_sk_node_8, wp_autogen_flag_node_8, wp_customer_sk_node_8, wp_url_node_8, wp_type_node_8, wp_char_count_node_8, wp_link_count_node_8, wp_image_count_node_8, wp_max_ad_count_node_8, ca_address_sk_node_9, ca_address_id_node_9, ca_street_number_node_9, ca_street_name_node_9, ca_street_type_node_9, ca_suite_number_node_9, ca_city_node_9, ca_county_node_9, ca_state_node_9, ca_zip_node_9, ca_country_node_9, ca_gmt_offset_node_9, ca_location_type_node_9], build=[right])
            :- Sort(orderBy=[ss_customer_sk_node_7 ASC])
            :  +- Calc(select=[ss_sold_date_sk AS ss_sold_date_sk_node_7, ss_sold_time_sk AS ss_sold_time_sk_node_7, ss_item_sk AS ss_item_sk_node_7, ss_customer_sk AS ss_customer_sk_node_7, ss_cdemo_sk AS ss_cdemo_sk_node_7, ss_hdemo_sk AS ss_hdemo_sk_node_7, ss_addr_sk AS ss_addr_sk_node_7, ss_store_sk AS ss_store_sk_node_7, ss_promo_sk AS ss_promo_sk_node_7, ss_ticket_number AS ss_ticket_number_node_7, ss_quantity AS ss_quantity_node_7, ss_wholesale_cost AS ss_wholesale_cost_node_7, ss_list_price AS ss_list_price_node_7, ss_sales_price AS ss_sales_price_node_7, ss_ext_discount_amt AS ss_ext_discount_amt_node_7, ss_ext_sales_price AS ss_ext_sales_price_node_7, ss_ext_wholesale_cost AS ss_ext_wholesale_cost_node_7, ss_ext_list_price AS ss_ext_list_price_node_7, ss_ext_tax AS ss_ext_tax_node_7, ss_coupon_amt AS ss_coupon_amt_node_7, ss_net_paid AS ss_net_paid_node_7, ss_net_paid_inc_tax AS ss_net_paid_inc_tax_node_7, ss_net_profit AS ss_net_profit_node_7, 'hello' AS _c23])
            :     +- Exchange(distribution=[single])
            :        +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
            +- Exchange(distribution=[broadcast])
               +- Calc(select=[wp_web_page_sk AS OELQP, wp_web_page_id AS wp_web_page_id_node_8, wp_rec_start_date AS wp_rec_start_date_node_8, wp_rec_end_date AS wp_rec_end_date_node_8, wp_creation_date_sk AS wp_creation_date_sk_node_8, wp_access_date_sk AS wp_access_date_sk_node_8, wp_autogen_flag AS wp_autogen_flag_node_8, wp_customer_sk AS wp_customer_sk_node_8, wp_url AS wp_url_node_8, wp_type AS wp_type_node_8, wp_char_count AS wp_char_count_node_8, wp_link_count AS wp_link_count_node_8, wp_image_count AS wp_image_count_node_8, wp_max_ad_count AS wp_max_ad_count_node_8, ca_address_sk AS ca_address_sk_node_9, ca_address_id AS ca_address_id_node_9, ca_street_number AS ca_street_number_node_9, ca_street_name AS ca_street_name_node_9, ca_street_type AS ca_street_type_node_9, ca_suite_number AS ca_suite_number_node_9, ca_city AS ca_city_node_9, ca_county AS ca_county_node_9, ca_state AS ca_state_node_9, ca_zip AS ca_zip_node_9, ca_country AS ca_country_node_9, ca_gmt_offset AS ca_gmt_offset_node_9, ca_location_type AS ca_location_type_node_9])
                  +- MultipleInput(readOrder=[0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(ca_state = wp_autogen_flag)], select=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count, ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type], isBroadcast=[true], build=[left])\
:- [#1] Exchange(distribution=[broadcast])\
+- [#2] TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])\
])
                     :- Exchange(distribution=[broadcast])
                     :  +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
                     +- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o122893825.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#248099165:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[3](input=RelSubset#248099163,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[3]), rel#248099162:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[3](input=RelSubset#248099161,groupBy=ss_store_sk, ss_promo_sk,select=ss_store_sk, ss_promo_sk, Partial_COUNT(*) AS count1$0)]
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