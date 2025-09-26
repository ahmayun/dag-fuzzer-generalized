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

autonode_10 = table_env.from_path("catalog_returns").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("catalog_returns").get_schema().get_field_names()])
autonode_9 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_8 = table_env.from_path("store").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("store").get_schema().get_field_names()])
autonode_7 = autonode_9.join(autonode_10, col('ss_ext_wholesale_cost_node_9') == col('cr_refunded_cash_node_10'))
autonode_6 = autonode_8.distinct()
autonode_5 = autonode_7.alias('z7YPQ')
autonode_4 = autonode_6.add_columns(lit("hello"))
autonode_3 = autonode_5.order_by(col('cr_returning_cdemo_sk_node_10'))
autonode_2 = autonode_4.distinct()
autonode_1 = autonode_2.join(autonode_3, col('ss_net_paid_inc_tax_node_9') == col('s_tax_precentage_node_8'))
sink = autonode_1.group_by(col('s_market_desc_node_8')).select(col('ss_net_profit_node_9').count.alias('ss_net_profit_node_9'))
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
      "error_message": "An error occurred while calling o103701795.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#209411771:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[31](input=RelSubset#209411769,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[31]), rel#209411768:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[31](input=RelSubset#209411767,groupBy=ss_net_paid_inc_tax,select=ss_net_paid_inc_tax, Partial_COUNT(ss_net_profit) AS count$0)]
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
LogicalProject(ss_net_profit_node_9=[$1])
+- LogicalAggregate(group=[{12}], EXPR$0=[COUNT($52)])
   +- LogicalJoin(condition=[=($51, $28)], joinType=[inner])
      :- LogicalProject(s_store_sk_node_8=[$0], s_store_id_node_8=[$1], s_rec_start_date_node_8=[$2], s_rec_end_date_node_8=[$3], s_closed_date_sk_node_8=[$4], s_store_name_node_8=[$5], s_number_employees_node_8=[$6], s_floor_space_node_8=[$7], s_hours_node_8=[$8], s_manager_node_8=[$9], s_market_id_node_8=[$10], s_geography_class_node_8=[$11], s_market_desc_node_8=[$12], s_market_manager_node_8=[$13], s_division_id_node_8=[$14], s_division_name_node_8=[$15], s_company_id_node_8=[$16], s_company_name_node_8=[$17], s_street_number_node_8=[$18], s_street_name_node_8=[$19], s_street_type_node_8=[$20], s_suite_number_node_8=[$21], s_city_node_8=[$22], s_county_node_8=[$23], s_state_node_8=[$24], s_zip_node_8=[$25], s_country_node_8=[$26], s_gmt_offset_node_8=[$27], s_tax_precentage_node_8=[$28], _c29=[_UTF-16LE'hello'])
      :  +- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28}])
      :     +- LogicalProject(s_store_sk_node_8=[$0], s_store_id_node_8=[$1], s_rec_start_date_node_8=[$2], s_rec_end_date_node_8=[$3], s_closed_date_sk_node_8=[$4], s_store_name_node_8=[$5], s_number_employees_node_8=[$6], s_floor_space_node_8=[$7], s_hours_node_8=[$8], s_manager_node_8=[$9], s_market_id_node_8=[$10], s_geography_class_node_8=[$11], s_market_desc_node_8=[$12], s_market_manager_node_8=[$13], s_division_id_node_8=[$14], s_division_name_node_8=[$15], s_company_id_node_8=[$16], s_company_name_node_8=[$17], s_street_number_node_8=[$18], s_street_name_node_8=[$19], s_street_type_node_8=[$20], s_suite_number_node_8=[$21], s_city_node_8=[$22], s_county_node_8=[$23], s_state_node_8=[$24], s_zip_node_8=[$25], s_country_node_8=[$26], s_gmt_offset_node_8=[$27], s_tax_precentage_node_8=[$28])
      :        +- LogicalTableScan(table=[[default_catalog, default_database, store]])
      +- LogicalSort(sort0=[$31], dir0=[ASC])
         +- LogicalProject(z7YPQ=[AS($0, _UTF-16LE'z7YPQ')], ss_sold_time_sk_node_9=[$1], ss_item_sk_node_9=[$2], ss_customer_sk_node_9=[$3], ss_cdemo_sk_node_9=[$4], ss_hdemo_sk_node_9=[$5], ss_addr_sk_node_9=[$6], ss_store_sk_node_9=[$7], ss_promo_sk_node_9=[$8], ss_ticket_number_node_9=[$9], ss_quantity_node_9=[$10], ss_wholesale_cost_node_9=[$11], ss_list_price_node_9=[$12], ss_sales_price_node_9=[$13], ss_ext_discount_amt_node_9=[$14], ss_ext_sales_price_node_9=[$15], ss_ext_wholesale_cost_node_9=[$16], ss_ext_list_price_node_9=[$17], ss_ext_tax_node_9=[$18], ss_coupon_amt_node_9=[$19], ss_net_paid_node_9=[$20], ss_net_paid_inc_tax_node_9=[$21], ss_net_profit_node_9=[$22], cr_returned_date_sk_node_10=[$23], cr_returned_time_sk_node_10=[$24], cr_item_sk_node_10=[$25], cr_refunded_customer_sk_node_10=[$26], cr_refunded_cdemo_sk_node_10=[$27], cr_refunded_hdemo_sk_node_10=[$28], cr_refunded_addr_sk_node_10=[$29], cr_returning_customer_sk_node_10=[$30], cr_returning_cdemo_sk_node_10=[$31], cr_returning_hdemo_sk_node_10=[$32], cr_returning_addr_sk_node_10=[$33], cr_call_center_sk_node_10=[$34], cr_catalog_page_sk_node_10=[$35], cr_ship_mode_sk_node_10=[$36], cr_warehouse_sk_node_10=[$37], cr_reason_sk_node_10=[$38], cr_order_number_node_10=[$39], cr_return_quantity_node_10=[$40], cr_return_amount_node_10=[$41], cr_return_tax_node_10=[$42], cr_return_amt_inc_tax_node_10=[$43], cr_fee_node_10=[$44], cr_return_ship_cost_node_10=[$45], cr_refunded_cash_node_10=[$46], cr_reversed_charge_node_10=[$47], cr_store_credit_node_10=[$48], cr_net_loss_node_10=[$49])
            +- LogicalJoin(condition=[=($16, $46)], joinType=[inner])
               :- LogicalProject(ss_sold_date_sk_node_9=[$0], ss_sold_time_sk_node_9=[$1], ss_item_sk_node_9=[$2], ss_customer_sk_node_9=[$3], ss_cdemo_sk_node_9=[$4], ss_hdemo_sk_node_9=[$5], ss_addr_sk_node_9=[$6], ss_store_sk_node_9=[$7], ss_promo_sk_node_9=[$8], ss_ticket_number_node_9=[$9], ss_quantity_node_9=[$10], ss_wholesale_cost_node_9=[$11], ss_list_price_node_9=[$12], ss_sales_price_node_9=[$13], ss_ext_discount_amt_node_9=[$14], ss_ext_sales_price_node_9=[$15], ss_ext_wholesale_cost_node_9=[$16], ss_ext_list_price_node_9=[$17], ss_ext_tax_node_9=[$18], ss_coupon_amt_node_9=[$19], ss_net_paid_node_9=[$20], ss_net_paid_inc_tax_node_9=[$21], ss_net_profit_node_9=[$22])
               :  +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])
               +- LogicalProject(cr_returned_date_sk_node_10=[$0], cr_returned_time_sk_node_10=[$1], cr_item_sk_node_10=[$2], cr_refunded_customer_sk_node_10=[$3], cr_refunded_cdemo_sk_node_10=[$4], cr_refunded_hdemo_sk_node_10=[$5], cr_refunded_addr_sk_node_10=[$6], cr_returning_customer_sk_node_10=[$7], cr_returning_cdemo_sk_node_10=[$8], cr_returning_hdemo_sk_node_10=[$9], cr_returning_addr_sk_node_10=[$10], cr_call_center_sk_node_10=[$11], cr_catalog_page_sk_node_10=[$12], cr_ship_mode_sk_node_10=[$13], cr_warehouse_sk_node_10=[$14], cr_reason_sk_node_10=[$15], cr_order_number_node_10=[$16], cr_return_quantity_node_10=[$17], cr_return_amount_node_10=[$18], cr_return_tax_node_10=[$19], cr_return_amt_inc_tax_node_10=[$20], cr_fee_node_10=[$21], cr_return_ship_cost_node_10=[$22], cr_refunded_cash_node_10=[$23], cr_reversed_charge_node_10=[$24], cr_store_credit_node_10=[$25], cr_net_loss_node_10=[$26])
                  +- LogicalTableScan(table=[[default_catalog, default_database, catalog_returns]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS ss_net_profit_node_9])
+- SortAggregate(isMerge=[true], groupBy=[s_market_desc_node_8], select=[s_market_desc_node_8, Final_COUNT(count$0) AS EXPR$0])
   +- Sort(orderBy=[s_market_desc_node_8 ASC])
      +- Exchange(distribution=[hash[s_market_desc_node_8]])
         +- LocalSortAggregate(groupBy=[s_market_desc_node_8], select=[s_market_desc_node_8, Partial_COUNT(ss_net_profit_node_9) AS count$0])
            +- Calc(select=[s_store_sk_node_8, s_store_id_node_8, s_rec_start_date_node_8, s_rec_end_date_node_8, s_closed_date_sk_node_8, s_store_name_node_8, s_number_employees_node_8, s_floor_space_node_8, s_hours_node_8, s_manager_node_8, s_market_id_node_8, s_geography_class_node_8, s_market_desc_node_8, s_market_manager_node_8, s_division_id_node_8, s_division_name_node_8, s_company_id_node_8, s_company_name_node_8, s_street_number_node_8, s_street_name_node_8, s_street_type_node_8, s_suite_number_node_8, s_city_node_8, s_county_node_8, s_state_node_8, s_zip_node_8, s_country_node_8, s_gmt_offset_node_8, s_tax_precentage_node_8, 'hello' AS _c29, z7YPQ, ss_sold_time_sk_node_9, ss_item_sk_node_9, ss_customer_sk_node_9, ss_cdemo_sk_node_9, ss_hdemo_sk_node_9, ss_addr_sk_node_9, ss_store_sk_node_9, ss_promo_sk_node_9, ss_ticket_number_node_9, ss_quantity_node_9, ss_wholesale_cost_node_9, ss_list_price_node_9, ss_sales_price_node_9, ss_ext_discount_amt_node_9, ss_ext_sales_price_node_9, ss_ext_wholesale_cost_node_9, ss_ext_list_price_node_9, ss_ext_tax_node_9, ss_coupon_amt_node_9, ss_net_paid_node_9, ss_net_paid_inc_tax_node_9, ss_net_profit_node_9, cr_returned_date_sk_node_10, cr_returned_time_sk_node_10, cr_item_sk_node_10, cr_refunded_customer_sk_node_10, cr_refunded_cdemo_sk_node_10, cr_refunded_hdemo_sk_node_10, cr_refunded_addr_sk_node_10, cr_returning_customer_sk_node_10, cr_returning_cdemo_sk_node_10, cr_returning_hdemo_sk_node_10, cr_returning_addr_sk_node_10, cr_call_center_sk_node_10, cr_catalog_page_sk_node_10, cr_ship_mode_sk_node_10, cr_warehouse_sk_node_10, cr_reason_sk_node_10, cr_order_number_node_10, cr_return_quantity_node_10, cr_return_amount_node_10, cr_return_tax_node_10, cr_return_amt_inc_tax_node_10, cr_fee_node_10, cr_return_ship_cost_node_10, cr_refunded_cash_node_10, cr_reversed_charge_node_10, cr_store_credit_node_10, cr_net_loss_node_10])
               +- Sort(orderBy=[s_market_desc_node_8 ASC])
                  +- NestedLoopJoin(joinType=[InnerJoin], where=[=(ss_net_paid_inc_tax_node_9, s_tax_precentage_node_80)], select=[s_store_sk_node_8, s_store_id_node_8, s_rec_start_date_node_8, s_rec_end_date_node_8, s_closed_date_sk_node_8, s_store_name_node_8, s_number_employees_node_8, s_floor_space_node_8, s_hours_node_8, s_manager_node_8, s_market_id_node_8, s_geography_class_node_8, s_market_desc_node_8, s_market_manager_node_8, s_division_id_node_8, s_division_name_node_8, s_company_id_node_8, s_company_name_node_8, s_street_number_node_8, s_street_name_node_8, s_street_type_node_8, s_suite_number_node_8, s_city_node_8, s_county_node_8, s_state_node_8, s_zip_node_8, s_country_node_8, s_gmt_offset_node_8, s_tax_precentage_node_8, s_tax_precentage_node_80, z7YPQ, ss_sold_time_sk_node_9, ss_item_sk_node_9, ss_customer_sk_node_9, ss_cdemo_sk_node_9, ss_hdemo_sk_node_9, ss_addr_sk_node_9, ss_store_sk_node_9, ss_promo_sk_node_9, ss_ticket_number_node_9, ss_quantity_node_9, ss_wholesale_cost_node_9, ss_list_price_node_9, ss_sales_price_node_9, ss_ext_discount_amt_node_9, ss_ext_sales_price_node_9, ss_ext_wholesale_cost_node_9, ss_ext_list_price_node_9, ss_ext_tax_node_9, ss_coupon_amt_node_9, ss_net_paid_node_9, ss_net_paid_inc_tax_node_9, ss_net_profit_node_9, cr_returned_date_sk_node_10, cr_returned_time_sk_node_10, cr_item_sk_node_10, cr_refunded_customer_sk_node_10, cr_refunded_cdemo_sk_node_10, cr_refunded_hdemo_sk_node_10, cr_refunded_addr_sk_node_10, cr_returning_customer_sk_node_10, cr_returning_cdemo_sk_node_10, cr_returning_hdemo_sk_node_10, cr_returning_addr_sk_node_10, cr_call_center_sk_node_10, cr_catalog_page_sk_node_10, cr_ship_mode_sk_node_10, cr_warehouse_sk_node_10, cr_reason_sk_node_10, cr_order_number_node_10, cr_return_quantity_node_10, cr_return_amount_node_10, cr_return_tax_node_10, cr_return_amt_inc_tax_node_10, cr_fee_node_10, cr_return_ship_cost_node_10, cr_refunded_cash_node_10, cr_reversed_charge_node_10, cr_store_credit_node_10, cr_net_loss_node_10], build=[right])
                     :- Calc(select=[s_store_sk AS s_store_sk_node_8, s_store_id AS s_store_id_node_8, s_rec_start_date AS s_rec_start_date_node_8, s_rec_end_date AS s_rec_end_date_node_8, s_closed_date_sk AS s_closed_date_sk_node_8, s_store_name AS s_store_name_node_8, s_number_employees AS s_number_employees_node_8, s_floor_space AS s_floor_space_node_8, s_hours AS s_hours_node_8, s_manager AS s_manager_node_8, s_market_id AS s_market_id_node_8, s_geography_class AS s_geography_class_node_8, s_market_desc AS s_market_desc_node_8, s_market_manager AS s_market_manager_node_8, s_division_id AS s_division_id_node_8, s_division_name AS s_division_name_node_8, s_company_id AS s_company_id_node_8, s_company_name AS s_company_name_node_8, s_street_number AS s_street_number_node_8, s_street_name AS s_street_name_node_8, s_street_type AS s_street_type_node_8, s_suite_number AS s_suite_number_node_8, s_city AS s_city_node_8, s_county AS s_county_node_8, s_state AS s_state_node_8, s_zip AS s_zip_node_8, s_country AS s_country_node_8, s_gmt_offset AS s_gmt_offset_node_8, s_tax_precentage AS s_tax_precentage_node_8, CAST(s_tax_precentage AS DECIMAL(7, 2)) AS s_tax_precentage_node_80])
                     :  +- HashAggregate(isMerge=[false], groupBy=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage], select=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage])
                     :     +- Exchange(distribution=[hash[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage]])
                     :        +- TableSourceScan(table=[[default_catalog, default_database, store]], fields=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage])
                     +- Exchange(distribution=[broadcast])
                        +- Calc(select=[ss_sold_date_sk AS z7YPQ, ss_sold_time_sk AS ss_sold_time_sk_node_9, ss_item_sk AS ss_item_sk_node_9, ss_customer_sk AS ss_customer_sk_node_9, ss_cdemo_sk AS ss_cdemo_sk_node_9, ss_hdemo_sk AS ss_hdemo_sk_node_9, ss_addr_sk AS ss_addr_sk_node_9, ss_store_sk AS ss_store_sk_node_9, ss_promo_sk AS ss_promo_sk_node_9, ss_ticket_number AS ss_ticket_number_node_9, ss_quantity AS ss_quantity_node_9, ss_wholesale_cost AS ss_wholesale_cost_node_9, ss_list_price AS ss_list_price_node_9, ss_sales_price AS ss_sales_price_node_9, ss_ext_discount_amt AS ss_ext_discount_amt_node_9, ss_ext_sales_price AS ss_ext_sales_price_node_9, ss_ext_wholesale_cost AS ss_ext_wholesale_cost_node_9, ss_ext_list_price AS ss_ext_list_price_node_9, ss_ext_tax AS ss_ext_tax_node_9, ss_coupon_amt AS ss_coupon_amt_node_9, ss_net_paid AS ss_net_paid_node_9, ss_net_paid_inc_tax AS ss_net_paid_inc_tax_node_9, ss_net_profit AS ss_net_profit_node_9, cr_returned_date_sk AS cr_returned_date_sk_node_10, cr_returned_time_sk AS cr_returned_time_sk_node_10, cr_item_sk AS cr_item_sk_node_10, cr_refunded_customer_sk AS cr_refunded_customer_sk_node_10, cr_refunded_cdemo_sk AS cr_refunded_cdemo_sk_node_10, cr_refunded_hdemo_sk AS cr_refunded_hdemo_sk_node_10, cr_refunded_addr_sk AS cr_refunded_addr_sk_node_10, cr_returning_customer_sk AS cr_returning_customer_sk_node_10, cr_returning_cdemo_sk AS cr_returning_cdemo_sk_node_10, cr_returning_hdemo_sk AS cr_returning_hdemo_sk_node_10, cr_returning_addr_sk AS cr_returning_addr_sk_node_10, cr_call_center_sk AS cr_call_center_sk_node_10, cr_catalog_page_sk AS cr_catalog_page_sk_node_10, cr_ship_mode_sk AS cr_ship_mode_sk_node_10, cr_warehouse_sk AS cr_warehouse_sk_node_10, cr_reason_sk AS cr_reason_sk_node_10, cr_order_number AS cr_order_number_node_10, cr_return_quantity AS cr_return_quantity_node_10, cr_return_amount AS cr_return_amount_node_10, cr_return_tax AS cr_return_tax_node_10, cr_return_amt_inc_tax AS cr_return_amt_inc_tax_node_10, cr_fee AS cr_fee_node_10, cr_return_ship_cost AS cr_return_ship_cost_node_10, cr_refunded_cash AS cr_refunded_cash_node_10, cr_reversed_charge AS cr_reversed_charge_node_10, cr_store_credit AS cr_store_credit_node_10, cr_net_loss AS cr_net_loss_node_10])
                           +- SortLimit(orderBy=[cr_returning_cdemo_sk ASC], offset=[0], fetch=[1], global=[true])
                              +- Exchange(distribution=[single])
                                 +- SortLimit(orderBy=[cr_returning_cdemo_sk ASC], offset=[0], fetch=[1], global=[false])
                                    +- HashJoin(joinType=[InnerJoin], where=[=(ss_ext_wholesale_cost, cr_refunded_cash)], select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit, cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss], build=[right])
                                       :- Exchange(distribution=[hash[ss_ext_wholesale_cost]])
                                       :  +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
                                       +- Exchange(distribution=[hash[cr_refunded_cash]])
                                          +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS ss_net_profit_node_9])
+- SortAggregate(isMerge=[true], groupBy=[s_market_desc_node_8], select=[s_market_desc_node_8, Final_COUNT(count$0) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[s_market_desc_node_8 ASC])
         +- Exchange(distribution=[hash[s_market_desc_node_8]])
            +- LocalSortAggregate(groupBy=[s_market_desc_node_8], select=[s_market_desc_node_8, Partial_COUNT(ss_net_profit_node_9) AS count$0])
               +- Calc(select=[s_store_sk_node_8, s_store_id_node_8, s_rec_start_date_node_8, s_rec_end_date_node_8, s_closed_date_sk_node_8, s_store_name_node_8, s_number_employees_node_8, s_floor_space_node_8, s_hours_node_8, s_manager_node_8, s_market_id_node_8, s_geography_class_node_8, s_market_desc_node_8, s_market_manager_node_8, s_division_id_node_8, s_division_name_node_8, s_company_id_node_8, s_company_name_node_8, s_street_number_node_8, s_street_name_node_8, s_street_type_node_8, s_suite_number_node_8, s_city_node_8, s_county_node_8, s_state_node_8, s_zip_node_8, s_country_node_8, s_gmt_offset_node_8, s_tax_precentage_node_8, 'hello' AS _c29, z7YPQ, ss_sold_time_sk_node_9, ss_item_sk_node_9, ss_customer_sk_node_9, ss_cdemo_sk_node_9, ss_hdemo_sk_node_9, ss_addr_sk_node_9, ss_store_sk_node_9, ss_promo_sk_node_9, ss_ticket_number_node_9, ss_quantity_node_9, ss_wholesale_cost_node_9, ss_list_price_node_9, ss_sales_price_node_9, ss_ext_discount_amt_node_9, ss_ext_sales_price_node_9, ss_ext_wholesale_cost_node_9, ss_ext_list_price_node_9, ss_ext_tax_node_9, ss_coupon_amt_node_9, ss_net_paid_node_9, ss_net_paid_inc_tax_node_9, ss_net_profit_node_9, cr_returned_date_sk_node_10, cr_returned_time_sk_node_10, cr_item_sk_node_10, cr_refunded_customer_sk_node_10, cr_refunded_cdemo_sk_node_10, cr_refunded_hdemo_sk_node_10, cr_refunded_addr_sk_node_10, cr_returning_customer_sk_node_10, cr_returning_cdemo_sk_node_10, cr_returning_hdemo_sk_node_10, cr_returning_addr_sk_node_10, cr_call_center_sk_node_10, cr_catalog_page_sk_node_10, cr_ship_mode_sk_node_10, cr_warehouse_sk_node_10, cr_reason_sk_node_10, cr_order_number_node_10, cr_return_quantity_node_10, cr_return_amount_node_10, cr_return_tax_node_10, cr_return_amt_inc_tax_node_10, cr_fee_node_10, cr_return_ship_cost_node_10, cr_refunded_cash_node_10, cr_reversed_charge_node_10, cr_store_credit_node_10, cr_net_loss_node_10])
                  +- Sort(orderBy=[s_market_desc_node_8 ASC])
                     +- NestedLoopJoin(joinType=[InnerJoin], where=[(ss_net_paid_inc_tax_node_9 = s_tax_precentage_node_80)], select=[s_store_sk_node_8, s_store_id_node_8, s_rec_start_date_node_8, s_rec_end_date_node_8, s_closed_date_sk_node_8, s_store_name_node_8, s_number_employees_node_8, s_floor_space_node_8, s_hours_node_8, s_manager_node_8, s_market_id_node_8, s_geography_class_node_8, s_market_desc_node_8, s_market_manager_node_8, s_division_id_node_8, s_division_name_node_8, s_company_id_node_8, s_company_name_node_8, s_street_number_node_8, s_street_name_node_8, s_street_type_node_8, s_suite_number_node_8, s_city_node_8, s_county_node_8, s_state_node_8, s_zip_node_8, s_country_node_8, s_gmt_offset_node_8, s_tax_precentage_node_8, s_tax_precentage_node_80, z7YPQ, ss_sold_time_sk_node_9, ss_item_sk_node_9, ss_customer_sk_node_9, ss_cdemo_sk_node_9, ss_hdemo_sk_node_9, ss_addr_sk_node_9, ss_store_sk_node_9, ss_promo_sk_node_9, ss_ticket_number_node_9, ss_quantity_node_9, ss_wholesale_cost_node_9, ss_list_price_node_9, ss_sales_price_node_9, ss_ext_discount_amt_node_9, ss_ext_sales_price_node_9, ss_ext_wholesale_cost_node_9, ss_ext_list_price_node_9, ss_ext_tax_node_9, ss_coupon_amt_node_9, ss_net_paid_node_9, ss_net_paid_inc_tax_node_9, ss_net_profit_node_9, cr_returned_date_sk_node_10, cr_returned_time_sk_node_10, cr_item_sk_node_10, cr_refunded_customer_sk_node_10, cr_refunded_cdemo_sk_node_10, cr_refunded_hdemo_sk_node_10, cr_refunded_addr_sk_node_10, cr_returning_customer_sk_node_10, cr_returning_cdemo_sk_node_10, cr_returning_hdemo_sk_node_10, cr_returning_addr_sk_node_10, cr_call_center_sk_node_10, cr_catalog_page_sk_node_10, cr_ship_mode_sk_node_10, cr_warehouse_sk_node_10, cr_reason_sk_node_10, cr_order_number_node_10, cr_return_quantity_node_10, cr_return_amount_node_10, cr_return_tax_node_10, cr_return_amt_inc_tax_node_10, cr_fee_node_10, cr_return_ship_cost_node_10, cr_refunded_cash_node_10, cr_reversed_charge_node_10, cr_store_credit_node_10, cr_net_loss_node_10], build=[right])
                        :- Calc(select=[s_store_sk AS s_store_sk_node_8, s_store_id AS s_store_id_node_8, s_rec_start_date AS s_rec_start_date_node_8, s_rec_end_date AS s_rec_end_date_node_8, s_closed_date_sk AS s_closed_date_sk_node_8, s_store_name AS s_store_name_node_8, s_number_employees AS s_number_employees_node_8, s_floor_space AS s_floor_space_node_8, s_hours AS s_hours_node_8, s_manager AS s_manager_node_8, s_market_id AS s_market_id_node_8, s_geography_class AS s_geography_class_node_8, s_market_desc AS s_market_desc_node_8, s_market_manager AS s_market_manager_node_8, s_division_id AS s_division_id_node_8, s_division_name AS s_division_name_node_8, s_company_id AS s_company_id_node_8, s_company_name AS s_company_name_node_8, s_street_number AS s_street_number_node_8, s_street_name AS s_street_name_node_8, s_street_type AS s_street_type_node_8, s_suite_number AS s_suite_number_node_8, s_city AS s_city_node_8, s_county AS s_county_node_8, s_state AS s_state_node_8, s_zip AS s_zip_node_8, s_country AS s_country_node_8, s_gmt_offset AS s_gmt_offset_node_8, s_tax_precentage AS s_tax_precentage_node_8, CAST(s_tax_precentage AS DECIMAL(7, 2)) AS s_tax_precentage_node_80])
                        :  +- HashAggregate(isMerge=[false], groupBy=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage], select=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage])
                        :     +- Exchange(distribution=[hash[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage]])
                        :        +- TableSourceScan(table=[[default_catalog, default_database, store]], fields=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage])
                        +- Exchange(distribution=[broadcast])
                           +- Calc(select=[ss_sold_date_sk AS z7YPQ, ss_sold_time_sk AS ss_sold_time_sk_node_9, ss_item_sk AS ss_item_sk_node_9, ss_customer_sk AS ss_customer_sk_node_9, ss_cdemo_sk AS ss_cdemo_sk_node_9, ss_hdemo_sk AS ss_hdemo_sk_node_9, ss_addr_sk AS ss_addr_sk_node_9, ss_store_sk AS ss_store_sk_node_9, ss_promo_sk AS ss_promo_sk_node_9, ss_ticket_number AS ss_ticket_number_node_9, ss_quantity AS ss_quantity_node_9, ss_wholesale_cost AS ss_wholesale_cost_node_9, ss_list_price AS ss_list_price_node_9, ss_sales_price AS ss_sales_price_node_9, ss_ext_discount_amt AS ss_ext_discount_amt_node_9, ss_ext_sales_price AS ss_ext_sales_price_node_9, ss_ext_wholesale_cost AS ss_ext_wholesale_cost_node_9, ss_ext_list_price AS ss_ext_list_price_node_9, ss_ext_tax AS ss_ext_tax_node_9, ss_coupon_amt AS ss_coupon_amt_node_9, ss_net_paid AS ss_net_paid_node_9, ss_net_paid_inc_tax AS ss_net_paid_inc_tax_node_9, ss_net_profit AS ss_net_profit_node_9, cr_returned_date_sk AS cr_returned_date_sk_node_10, cr_returned_time_sk AS cr_returned_time_sk_node_10, cr_item_sk AS cr_item_sk_node_10, cr_refunded_customer_sk AS cr_refunded_customer_sk_node_10, cr_refunded_cdemo_sk AS cr_refunded_cdemo_sk_node_10, cr_refunded_hdemo_sk AS cr_refunded_hdemo_sk_node_10, cr_refunded_addr_sk AS cr_refunded_addr_sk_node_10, cr_returning_customer_sk AS cr_returning_customer_sk_node_10, cr_returning_cdemo_sk AS cr_returning_cdemo_sk_node_10, cr_returning_hdemo_sk AS cr_returning_hdemo_sk_node_10, cr_returning_addr_sk AS cr_returning_addr_sk_node_10, cr_call_center_sk AS cr_call_center_sk_node_10, cr_catalog_page_sk AS cr_catalog_page_sk_node_10, cr_ship_mode_sk AS cr_ship_mode_sk_node_10, cr_warehouse_sk AS cr_warehouse_sk_node_10, cr_reason_sk AS cr_reason_sk_node_10, cr_order_number AS cr_order_number_node_10, cr_return_quantity AS cr_return_quantity_node_10, cr_return_amount AS cr_return_amount_node_10, cr_return_tax AS cr_return_tax_node_10, cr_return_amt_inc_tax AS cr_return_amt_inc_tax_node_10, cr_fee AS cr_fee_node_10, cr_return_ship_cost AS cr_return_ship_cost_node_10, cr_refunded_cash AS cr_refunded_cash_node_10, cr_reversed_charge AS cr_reversed_charge_node_10, cr_store_credit AS cr_store_credit_node_10, cr_net_loss AS cr_net_loss_node_10])
                              +- SortLimit(orderBy=[cr_returning_cdemo_sk ASC], offset=[0], fetch=[1], global=[true])
                                 +- Exchange(distribution=[single])
                                    +- SortLimit(orderBy=[cr_returning_cdemo_sk ASC], offset=[0], fetch=[1], global=[false])
                                       +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(ss_ext_wholesale_cost = cr_refunded_cash)], select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit, cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss], build=[right])
                                          :- Exchange(distribution=[hash[ss_ext_wholesale_cost]])
                                          :  +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
                                          +- Exchange(distribution=[hash[cr_refunded_cash]])
                                             +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0