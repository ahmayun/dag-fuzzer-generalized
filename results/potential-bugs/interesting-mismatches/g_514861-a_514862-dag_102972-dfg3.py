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

autonode_10 = table_env.from_path("store").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("store").get_schema().get_field_names()])
autonode_9 = table_env.from_path("catalog_sales").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("catalog_sales").get_schema().get_field_names()])
autonode_8 = autonode_10.add_columns(lit("hello"))
autonode_7 = autonode_9.limit(49)
autonode_6 = autonode_8.order_by(col('s_geography_class_node_10'))
autonode_5 = autonode_7.add_columns(lit("hello"))
autonode_4 = autonode_5.join(autonode_6, col('cs_net_paid_node_9') == col('s_gmt_offset_node_10'))
autonode_3 = autonode_4.group_by(col('s_street_number_node_10')).select(col('s_market_id_node_10').min.alias('s_market_id_node_10'))
autonode_2 = autonode_3.add_columns(lit("hello"))
autonode_1 = autonode_2.group_by(col('s_market_id_node_10')).select(col('s_market_id_node_10').sum.alias('s_market_id_node_10'))
sink = autonode_1.group_by(col('s_market_id_node_10')).select(col('s_market_id_node_10').sum.alias('s_market_id_node_10'))
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
LogicalProject(s_market_id_node_10=[$1])
+- LogicalAggregate(group=[{0}], EXPR$0=[SUM($0)])
   +- LogicalProject(s_market_id_node_10=[$1])
      +- LogicalAggregate(group=[{0}], EXPR$0=[SUM($0)])
         +- LogicalProject(s_market_id_node_10=[$1])
            +- LogicalAggregate(group=[{53}], EXPR$0=[MIN($45)])
               +- LogicalJoin(condition=[=($29, $62)], joinType=[inner])
                  :- LogicalProject(cs_sold_date_sk_node_9=[$0], cs_sold_time_sk_node_9=[$1], cs_ship_date_sk_node_9=[$2], cs_bill_customer_sk_node_9=[$3], cs_bill_cdemo_sk_node_9=[$4], cs_bill_hdemo_sk_node_9=[$5], cs_bill_addr_sk_node_9=[$6], cs_ship_customer_sk_node_9=[$7], cs_ship_cdemo_sk_node_9=[$8], cs_ship_hdemo_sk_node_9=[$9], cs_ship_addr_sk_node_9=[$10], cs_call_center_sk_node_9=[$11], cs_catalog_page_sk_node_9=[$12], cs_ship_mode_sk_node_9=[$13], cs_warehouse_sk_node_9=[$14], cs_item_sk_node_9=[$15], cs_promo_sk_node_9=[$16], cs_order_number_node_9=[$17], cs_quantity_node_9=[$18], cs_wholesale_cost_node_9=[$19], cs_list_price_node_9=[$20], cs_sales_price_node_9=[$21], cs_ext_discount_amt_node_9=[$22], cs_ext_sales_price_node_9=[$23], cs_ext_wholesale_cost_node_9=[$24], cs_ext_list_price_node_9=[$25], cs_ext_tax_node_9=[$26], cs_coupon_amt_node_9=[$27], cs_ext_ship_cost_node_9=[$28], cs_net_paid_node_9=[$29], cs_net_paid_inc_tax_node_9=[$30], cs_net_paid_inc_ship_node_9=[$31], cs_net_paid_inc_ship_tax_node_9=[$32], cs_net_profit_node_9=[$33], _c34=[_UTF-16LE'hello'])
                  :  +- LogicalSort(fetch=[49])
                  :     +- LogicalProject(cs_sold_date_sk_node_9=[$0], cs_sold_time_sk_node_9=[$1], cs_ship_date_sk_node_9=[$2], cs_bill_customer_sk_node_9=[$3], cs_bill_cdemo_sk_node_9=[$4], cs_bill_hdemo_sk_node_9=[$5], cs_bill_addr_sk_node_9=[$6], cs_ship_customer_sk_node_9=[$7], cs_ship_cdemo_sk_node_9=[$8], cs_ship_hdemo_sk_node_9=[$9], cs_ship_addr_sk_node_9=[$10], cs_call_center_sk_node_9=[$11], cs_catalog_page_sk_node_9=[$12], cs_ship_mode_sk_node_9=[$13], cs_warehouse_sk_node_9=[$14], cs_item_sk_node_9=[$15], cs_promo_sk_node_9=[$16], cs_order_number_node_9=[$17], cs_quantity_node_9=[$18], cs_wholesale_cost_node_9=[$19], cs_list_price_node_9=[$20], cs_sales_price_node_9=[$21], cs_ext_discount_amt_node_9=[$22], cs_ext_sales_price_node_9=[$23], cs_ext_wholesale_cost_node_9=[$24], cs_ext_list_price_node_9=[$25], cs_ext_tax_node_9=[$26], cs_coupon_amt_node_9=[$27], cs_ext_ship_cost_node_9=[$28], cs_net_paid_node_9=[$29], cs_net_paid_inc_tax_node_9=[$30], cs_net_paid_inc_ship_node_9=[$31], cs_net_paid_inc_ship_tax_node_9=[$32], cs_net_profit_node_9=[$33])
                  :        +- LogicalTableScan(table=[[default_catalog, default_database, catalog_sales]])
                  +- LogicalSort(sort0=[$11], dir0=[ASC])
                     +- LogicalProject(s_store_sk_node_10=[$0], s_store_id_node_10=[$1], s_rec_start_date_node_10=[$2], s_rec_end_date_node_10=[$3], s_closed_date_sk_node_10=[$4], s_store_name_node_10=[$5], s_number_employees_node_10=[$6], s_floor_space_node_10=[$7], s_hours_node_10=[$8], s_manager_node_10=[$9], s_market_id_node_10=[$10], s_geography_class_node_10=[$11], s_market_desc_node_10=[$12], s_market_manager_node_10=[$13], s_division_id_node_10=[$14], s_division_name_node_10=[$15], s_company_id_node_10=[$16], s_company_name_node_10=[$17], s_street_number_node_10=[$18], s_street_name_node_10=[$19], s_street_type_node_10=[$20], s_suite_number_node_10=[$21], s_city_node_10=[$22], s_county_node_10=[$23], s_state_node_10=[$24], s_zip_node_10=[$25], s_country_node_10=[$26], s_gmt_offset_node_10=[$27], s_tax_precentage_node_10=[$28], _c29=[_UTF-16LE'hello'])
                        +- LogicalTableScan(table=[[default_catalog, default_database, store]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS s_market_id_node_10])
+- SortAggregate(isMerge=[false], groupBy=[s_market_id_node_10], select=[s_market_id_node_10, SUM(s_market_id_node_10) AS EXPR$0])
   +- Sort(orderBy=[s_market_id_node_10 ASC])
      +- Exchange(distribution=[hash[s_market_id_node_10]])
         +- Calc(select=[EXPR$0 AS s_market_id_node_10])
            +- HashAggregate(isMerge=[true], groupBy=[s_market_id_node_10], select=[s_market_id_node_10, Final_SUM(sum$0) AS EXPR$0])
               +- Exchange(distribution=[hash[s_market_id_node_10]])
                  +- LocalHashAggregate(groupBy=[s_market_id_node_10], select=[s_market_id_node_10, Partial_SUM(s_market_id_node_10) AS sum$0])
                     +- Calc(select=[EXPR$0 AS s_market_id_node_10])
                        +- HashAggregate(isMerge=[false], groupBy=[s_street_number_node_10], select=[s_street_number_node_10, MIN(s_market_id_node_10) AS EXPR$0])
                           +- Exchange(distribution=[hash[s_street_number_node_10]])
                              +- HashJoin(joinType=[InnerJoin], where=[=(cs_net_paid, s_gmt_offset_node_100)], select=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit, s_store_sk_node_10, s_store_id_node_10, s_rec_start_date_node_10, s_rec_end_date_node_10, s_closed_date_sk_node_10, s_store_name_node_10, s_number_employees_node_10, s_floor_space_node_10, s_hours_node_10, s_manager_node_10, s_market_id_node_10, s_geography_class_node_10, s_market_desc_node_10, s_market_manager_node_10, s_division_id_node_10, s_division_name_node_10, s_company_id_node_10, s_company_name_node_10, s_street_number_node_10, s_street_name_node_10, s_street_type_node_10, s_suite_number_node_10, s_city_node_10, s_county_node_10, s_state_node_10, s_zip_node_10, s_country_node_10, s_gmt_offset_node_10, s_tax_precentage_node_10, s_gmt_offset_node_100], isBroadcast=[true], build=[right])
                                 :- Limit(offset=[0], fetch=[49], global=[true])
                                 :  +- Exchange(distribution=[single])
                                 :     +- Limit(offset=[0], fetch=[49], global=[false])
                                 :        +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales, limit=[49]]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])
                                 +- Exchange(distribution=[broadcast])
                                    +- Calc(select=[s_store_sk AS s_store_sk_node_10, s_store_id AS s_store_id_node_10, s_rec_start_date AS s_rec_start_date_node_10, s_rec_end_date AS s_rec_end_date_node_10, s_closed_date_sk AS s_closed_date_sk_node_10, s_store_name AS s_store_name_node_10, s_number_employees AS s_number_employees_node_10, s_floor_space AS s_floor_space_node_10, s_hours AS s_hours_node_10, s_manager AS s_manager_node_10, s_market_id AS s_market_id_node_10, s_geography_class AS s_geography_class_node_10, s_market_desc AS s_market_desc_node_10, s_market_manager AS s_market_manager_node_10, s_division_id AS s_division_id_node_10, s_division_name AS s_division_name_node_10, s_company_id AS s_company_id_node_10, s_company_name AS s_company_name_node_10, s_street_number AS s_street_number_node_10, s_street_name AS s_street_name_node_10, s_street_type AS s_street_type_node_10, s_suite_number AS s_suite_number_node_10, s_city AS s_city_node_10, s_county AS s_county_node_10, s_state AS s_state_node_10, s_zip AS s_zip_node_10, s_country AS s_country_node_10, s_gmt_offset AS s_gmt_offset_node_10, s_tax_precentage AS s_tax_precentage_node_10, CAST(s_gmt_offset AS DECIMAL(7, 2)) AS s_gmt_offset_node_100])
                                       +- Sort(orderBy=[s_geography_class ASC])
                                          +- Exchange(distribution=[single])
                                             +- TableSourceScan(table=[[default_catalog, default_database, store]], fields=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS s_market_id_node_10])
+- SortAggregate(isMerge=[false], groupBy=[s_market_id_node_10], select=[s_market_id_node_10, SUM(s_market_id_node_10) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[s_market_id_node_10 ASC])
         +- Exchange(distribution=[hash[s_market_id_node_10]])
            +- Calc(select=[EXPR$0 AS s_market_id_node_10])
               +- HashAggregate(isMerge=[true], groupBy=[s_market_id_node_10], select=[s_market_id_node_10, Final_SUM(sum$0) AS EXPR$0])
                  +- Exchange(distribution=[hash[s_market_id_node_10]])
                     +- LocalHashAggregate(groupBy=[s_market_id_node_10], select=[s_market_id_node_10, Partial_SUM(s_market_id_node_10) AS sum$0])
                        +- Calc(select=[EXPR$0 AS s_market_id_node_10])
                           +- HashAggregate(isMerge=[false], groupBy=[s_street_number_node_10], select=[s_street_number_node_10, MIN(s_market_id_node_10) AS EXPR$0])
                              +- Exchange(distribution=[hash[s_street_number_node_10]])
                                 +- HashJoin(joinType=[InnerJoin], where=[(cs_net_paid = s_gmt_offset_node_100)], select=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit, s_store_sk_node_10, s_store_id_node_10, s_rec_start_date_node_10, s_rec_end_date_node_10, s_closed_date_sk_node_10, s_store_name_node_10, s_number_employees_node_10, s_floor_space_node_10, s_hours_node_10, s_manager_node_10, s_market_id_node_10, s_geography_class_node_10, s_market_desc_node_10, s_market_manager_node_10, s_division_id_node_10, s_division_name_node_10, s_company_id_node_10, s_company_name_node_10, s_street_number_node_10, s_street_name_node_10, s_street_type_node_10, s_suite_number_node_10, s_city_node_10, s_county_node_10, s_state_node_10, s_zip_node_10, s_country_node_10, s_gmt_offset_node_10, s_tax_precentage_node_10, s_gmt_offset_node_100], isBroadcast=[true], build=[right])
                                    :- Limit(offset=[0], fetch=[49], global=[true])
                                    :  +- Exchange(distribution=[single])
                                    :     +- Limit(offset=[0], fetch=[49], global=[false])
                                    :        +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales, limit=[49]]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])
                                    +- Exchange(distribution=[broadcast])
                                       +- Calc(select=[s_store_sk AS s_store_sk_node_10, s_store_id AS s_store_id_node_10, s_rec_start_date AS s_rec_start_date_node_10, s_rec_end_date AS s_rec_end_date_node_10, s_closed_date_sk AS s_closed_date_sk_node_10, s_store_name AS s_store_name_node_10, s_number_employees AS s_number_employees_node_10, s_floor_space AS s_floor_space_node_10, s_hours AS s_hours_node_10, s_manager AS s_manager_node_10, s_market_id AS s_market_id_node_10, s_geography_class AS s_geography_class_node_10, s_market_desc AS s_market_desc_node_10, s_market_manager AS s_market_manager_node_10, s_division_id AS s_division_id_node_10, s_division_name AS s_division_name_node_10, s_company_id AS s_company_id_node_10, s_company_name AS s_company_name_node_10, s_street_number AS s_street_number_node_10, s_street_name AS s_street_name_node_10, s_street_type AS s_street_type_node_10, s_suite_number AS s_suite_number_node_10, s_city AS s_city_node_10, s_county AS s_county_node_10, s_state AS s_state_node_10, s_zip AS s_zip_node_10, s_country AS s_country_node_10, s_gmt_offset AS s_gmt_offset_node_10, s_tax_precentage AS s_tax_precentage_node_10, CAST(s_gmt_offset AS DECIMAL(7, 2)) AS s_gmt_offset_node_100])
                                          +- Sort(orderBy=[s_geography_class ASC])
                                             +- Exchange(distribution=[single])
                                                +- TableSourceScan(table=[[default_catalog, default_database, store]], fields=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o280459430.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#567185643:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[11](input=RelSubset#567185641,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[11]), rel#567185640:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[11](input=RelSubset#567185639,groupBy=s_street_number_node_10, s_gmt_offset_node_100,select=s_street_number_node_10, s_gmt_offset_node_100, Partial_MIN(s_market_id_node_10) AS min$0)]
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