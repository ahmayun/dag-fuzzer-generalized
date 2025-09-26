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

autonode_10 = table_env.from_path("store").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("store").get_schema().get_field_names()])
autonode_9 = table_env.from_path("promotion").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("promotion").get_schema().get_field_names()])
autonode_8 = table_env.from_path("catalog_returns").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("catalog_returns").get_schema().get_field_names()])
autonode_11 = table_env.from_path("web_returns").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("web_returns").get_schema().get_field_names()])
autonode_6 = autonode_9.add_columns(lit("hello"))
autonode_5 = autonode_8.alias('QeFVW')
autonode_7 = autonode_10.join(autonode_11, col('s_market_id_node_10') == col('wr_returned_time_sk_node_11'))
autonode_3 = autonode_5.order_by(col('cr_ship_mode_sk_node_8'))
autonode_4 = autonode_6.join(autonode_7, col('s_closed_date_sk_node_10') == col('p_end_date_sk_node_9'))
autonode_2 = autonode_3.join(autonode_4, col('cr_return_tax_node_8') == col('wr_fee_node_11'))
autonode_1 = autonode_2.group_by(col('s_state_node_10')).select(col('wr_fee_node_11').count.alias('wr_fee_node_11'))
sink = autonode_1.filter(col('wr_fee_node_11') > 37.286537885665894)
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
      "error_message": "An error occurred while calling o72018869.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#144425368:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[13](input=RelSubset#144425366,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[13]), rel#144425365:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[13](input=RelSubset#144425364,groupBy=cr_return_tax,select=cr_return_tax, Partial_COUNT(*) AS count1$0)]
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
LogicalFilter(condition=[>($0, 3.7286537885665894E1:DOUBLE)])
+- LogicalProject(wr_fee_node_11=[$1])
   +- LogicalAggregate(group=[{71}], EXPR$0=[COUNT($94)])
      +- LogicalJoin(condition=[=($19, $94)], joinType=[inner])
         :- LogicalSort(sort0=[$13], dir0=[ASC])
         :  +- LogicalProject(QeFVW=[AS($0, _UTF-16LE'QeFVW')], cr_returned_time_sk_node_8=[$1], cr_item_sk_node_8=[$2], cr_refunded_customer_sk_node_8=[$3], cr_refunded_cdemo_sk_node_8=[$4], cr_refunded_hdemo_sk_node_8=[$5], cr_refunded_addr_sk_node_8=[$6], cr_returning_customer_sk_node_8=[$7], cr_returning_cdemo_sk_node_8=[$8], cr_returning_hdemo_sk_node_8=[$9], cr_returning_addr_sk_node_8=[$10], cr_call_center_sk_node_8=[$11], cr_catalog_page_sk_node_8=[$12], cr_ship_mode_sk_node_8=[$13], cr_warehouse_sk_node_8=[$14], cr_reason_sk_node_8=[$15], cr_order_number_node_8=[$16], cr_return_quantity_node_8=[$17], cr_return_amount_node_8=[$18], cr_return_tax_node_8=[$19], cr_return_amt_inc_tax_node_8=[$20], cr_fee_node_8=[$21], cr_return_ship_cost_node_8=[$22], cr_refunded_cash_node_8=[$23], cr_reversed_charge_node_8=[$24], cr_store_credit_node_8=[$25], cr_net_loss_node_8=[$26])
         :     +- LogicalTableScan(table=[[default_catalog, default_database, catalog_returns]])
         +- LogicalJoin(condition=[=($24, $3)], joinType=[inner])
            :- LogicalProject(p_promo_sk_node_9=[$0], p_promo_id_node_9=[$1], p_start_date_sk_node_9=[$2], p_end_date_sk_node_9=[$3], p_item_sk_node_9=[$4], p_cost_node_9=[$5], p_response_target_node_9=[$6], p_promo_name_node_9=[$7], p_channel_dmail_node_9=[$8], p_channel_email_node_9=[$9], p_channel_catalog_node_9=[$10], p_channel_tv_node_9=[$11], p_channel_radio_node_9=[$12], p_channel_press_node_9=[$13], p_channel_event_node_9=[$14], p_channel_demo_node_9=[$15], p_channel_details_node_9=[$16], p_purpose_node_9=[$17], p_discount_active_node_9=[$18], _c19=[_UTF-16LE'hello'])
            :  +- LogicalTableScan(table=[[default_catalog, default_database, promotion]])
            +- LogicalJoin(condition=[=($10, $30)], joinType=[inner])
               :- LogicalProject(s_store_sk_node_10=[$0], s_store_id_node_10=[$1], s_rec_start_date_node_10=[$2], s_rec_end_date_node_10=[$3], s_closed_date_sk_node_10=[$4], s_store_name_node_10=[$5], s_number_employees_node_10=[$6], s_floor_space_node_10=[$7], s_hours_node_10=[$8], s_manager_node_10=[$9], s_market_id_node_10=[$10], s_geography_class_node_10=[$11], s_market_desc_node_10=[$12], s_market_manager_node_10=[$13], s_division_id_node_10=[$14], s_division_name_node_10=[$15], s_company_id_node_10=[$16], s_company_name_node_10=[$17], s_street_number_node_10=[$18], s_street_name_node_10=[$19], s_street_type_node_10=[$20], s_suite_number_node_10=[$21], s_city_node_10=[$22], s_county_node_10=[$23], s_state_node_10=[$24], s_zip_node_10=[$25], s_country_node_10=[$26], s_gmt_offset_node_10=[$27], s_tax_precentage_node_10=[$28])
               :  +- LogicalTableScan(table=[[default_catalog, default_database, store]])
               +- LogicalProject(wr_returned_date_sk_node_11=[$0], wr_returned_time_sk_node_11=[$1], wr_item_sk_node_11=[$2], wr_refunded_customer_sk_node_11=[$3], wr_refunded_cdemo_sk_node_11=[$4], wr_refunded_hdemo_sk_node_11=[$5], wr_refunded_addr_sk_node_11=[$6], wr_returning_customer_sk_node_11=[$7], wr_returning_cdemo_sk_node_11=[$8], wr_returning_hdemo_sk_node_11=[$9], wr_returning_addr_sk_node_11=[$10], wr_web_page_sk_node_11=[$11], wr_reason_sk_node_11=[$12], wr_order_number_node_11=[$13], wr_return_quantity_node_11=[$14], wr_return_amt_node_11=[$15], wr_return_tax_node_11=[$16], wr_return_amt_inc_tax_node_11=[$17], wr_fee_node_11=[$18], wr_return_ship_cost_node_11=[$19], wr_refunded_cash_node_11=[$20], wr_reversed_charge_node_11=[$21], wr_account_credit_node_11=[$22], wr_net_loss_node_11=[$23])
                  +- LogicalTableScan(table=[[default_catalog, default_database, web_returns]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0], where=[>(EXPR$0, 3.7286537885665894E1)])
+- HashAggregate(isMerge=[false], groupBy=[s_state], select=[s_state, COUNT(wr_fee) AS EXPR$0])
   +- Exchange(distribution=[hash[s_state]])
      +- NestedLoopJoin(joinType=[InnerJoin], where=[=(cr_return_tax_node_8, wr_fee)], select=[QeFVW, cr_returned_time_sk_node_8, cr_item_sk_node_8, cr_refunded_customer_sk_node_8, cr_refunded_cdemo_sk_node_8, cr_refunded_hdemo_sk_node_8, cr_refunded_addr_sk_node_8, cr_returning_customer_sk_node_8, cr_returning_cdemo_sk_node_8, cr_returning_hdemo_sk_node_8, cr_returning_addr_sk_node_8, cr_call_center_sk_node_8, cr_catalog_page_sk_node_8, cr_ship_mode_sk_node_8, cr_warehouse_sk_node_8, cr_reason_sk_node_8, cr_order_number_node_8, cr_return_quantity_node_8, cr_return_amount_node_8, cr_return_tax_node_8, cr_return_amt_inc_tax_node_8, cr_fee_node_8, cr_return_ship_cost_node_8, cr_refunded_cash_node_8, cr_reversed_charge_node_8, cr_store_credit_node_8, cr_net_loss_node_8, p_promo_sk_node_9, p_promo_id_node_9, p_start_date_sk_node_9, p_end_date_sk_node_9, p_item_sk_node_9, p_cost_node_9, p_response_target_node_9, p_promo_name_node_9, p_channel_dmail_node_9, p_channel_email_node_9, p_channel_catalog_node_9, p_channel_tv_node_9, p_channel_radio_node_9, p_channel_press_node_9, p_channel_event_node_9, p_channel_demo_node_9, p_channel_details_node_9, p_purpose_node_9, p_discount_active_node_9, _c19, s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], build=[left])
         :- Exchange(distribution=[broadcast])
         :  +- Calc(select=[cr_returned_date_sk AS QeFVW, cr_returned_time_sk AS cr_returned_time_sk_node_8, cr_item_sk AS cr_item_sk_node_8, cr_refunded_customer_sk AS cr_refunded_customer_sk_node_8, cr_refunded_cdemo_sk AS cr_refunded_cdemo_sk_node_8, cr_refunded_hdemo_sk AS cr_refunded_hdemo_sk_node_8, cr_refunded_addr_sk AS cr_refunded_addr_sk_node_8, cr_returning_customer_sk AS cr_returning_customer_sk_node_8, cr_returning_cdemo_sk AS cr_returning_cdemo_sk_node_8, cr_returning_hdemo_sk AS cr_returning_hdemo_sk_node_8, cr_returning_addr_sk AS cr_returning_addr_sk_node_8, cr_call_center_sk AS cr_call_center_sk_node_8, cr_catalog_page_sk AS cr_catalog_page_sk_node_8, cr_ship_mode_sk AS cr_ship_mode_sk_node_8, cr_warehouse_sk AS cr_warehouse_sk_node_8, cr_reason_sk AS cr_reason_sk_node_8, cr_order_number AS cr_order_number_node_8, cr_return_quantity AS cr_return_quantity_node_8, cr_return_amount AS cr_return_amount_node_8, cr_return_tax AS cr_return_tax_node_8, cr_return_amt_inc_tax AS cr_return_amt_inc_tax_node_8, cr_fee AS cr_fee_node_8, cr_return_ship_cost AS cr_return_ship_cost_node_8, cr_refunded_cash AS cr_refunded_cash_node_8, cr_reversed_charge AS cr_reversed_charge_node_8, cr_store_credit AS cr_store_credit_node_8, cr_net_loss AS cr_net_loss_node_8])
         :     +- SortLimit(orderBy=[cr_ship_mode_sk ASC], offset=[0], fetch=[1], global=[true])
         :        +- Exchange(distribution=[single])
         :           +- SortLimit(orderBy=[cr_ship_mode_sk ASC], offset=[0], fetch=[1], global=[false])
         :              +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])
         +- HashJoin(joinType=[InnerJoin], where=[=(s_closed_date_sk, p_end_date_sk_node_9)], select=[p_promo_sk_node_9, p_promo_id_node_9, p_start_date_sk_node_9, p_end_date_sk_node_9, p_item_sk_node_9, p_cost_node_9, p_response_target_node_9, p_promo_name_node_9, p_channel_dmail_node_9, p_channel_email_node_9, p_channel_catalog_node_9, p_channel_tv_node_9, p_channel_radio_node_9, p_channel_press_node_9, p_channel_event_node_9, p_channel_demo_node_9, p_channel_details_node_9, p_purpose_node_9, p_discount_active_node_9, _c19, s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], isBroadcast=[true], build=[left])
            :- Exchange(distribution=[broadcast])
            :  +- Calc(select=[p_promo_sk AS p_promo_sk_node_9, p_promo_id AS p_promo_id_node_9, p_start_date_sk AS p_start_date_sk_node_9, p_end_date_sk AS p_end_date_sk_node_9, p_item_sk AS p_item_sk_node_9, p_cost AS p_cost_node_9, p_response_target AS p_response_target_node_9, p_promo_name AS p_promo_name_node_9, p_channel_dmail AS p_channel_dmail_node_9, p_channel_email AS p_channel_email_node_9, p_channel_catalog AS p_channel_catalog_node_9, p_channel_tv AS p_channel_tv_node_9, p_channel_radio AS p_channel_radio_node_9, p_channel_press AS p_channel_press_node_9, p_channel_event AS p_channel_event_node_9, p_channel_demo AS p_channel_demo_node_9, p_channel_details AS p_channel_details_node_9, p_purpose AS p_purpose_node_9, p_discount_active AS p_discount_active_node_9, 'hello' AS _c19])
            :     +- TableSourceScan(table=[[default_catalog, default_database, promotion]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])
            +- HashJoin(joinType=[InnerJoin], where=[=(s_market_id, wr_returned_time_sk)], select=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], isBroadcast=[true], build=[left])
               :- Exchange(distribution=[broadcast])
               :  +- TableSourceScan(table=[[default_catalog, default_database, store]], fields=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage])
               +- TableSourceScan(table=[[default_catalog, default_database, web_returns]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])

== Optimized Execution Plan ==
Calc(select=[EXPR$0], where=[(EXPR$0 > 3.7286537885665894E1)])
+- HashAggregate(isMerge=[false], groupBy=[s_state], select=[s_state, COUNT(wr_fee) AS EXPR$0])
   +- Exchange(distribution=[hash[s_state]])
      +- NestedLoopJoin(joinType=[InnerJoin], where=[(cr_return_tax_node_8 = wr_fee)], select=[QeFVW, cr_returned_time_sk_node_8, cr_item_sk_node_8, cr_refunded_customer_sk_node_8, cr_refunded_cdemo_sk_node_8, cr_refunded_hdemo_sk_node_8, cr_refunded_addr_sk_node_8, cr_returning_customer_sk_node_8, cr_returning_cdemo_sk_node_8, cr_returning_hdemo_sk_node_8, cr_returning_addr_sk_node_8, cr_call_center_sk_node_8, cr_catalog_page_sk_node_8, cr_ship_mode_sk_node_8, cr_warehouse_sk_node_8, cr_reason_sk_node_8, cr_order_number_node_8, cr_return_quantity_node_8, cr_return_amount_node_8, cr_return_tax_node_8, cr_return_amt_inc_tax_node_8, cr_fee_node_8, cr_return_ship_cost_node_8, cr_refunded_cash_node_8, cr_reversed_charge_node_8, cr_store_credit_node_8, cr_net_loss_node_8, p_promo_sk_node_9, p_promo_id_node_9, p_start_date_sk_node_9, p_end_date_sk_node_9, p_item_sk_node_9, p_cost_node_9, p_response_target_node_9, p_promo_name_node_9, p_channel_dmail_node_9, p_channel_email_node_9, p_channel_catalog_node_9, p_channel_tv_node_9, p_channel_radio_node_9, p_channel_press_node_9, p_channel_event_node_9, p_channel_demo_node_9, p_channel_details_node_9, p_purpose_node_9, p_discount_active_node_9, _c19, s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], build=[left])
         :- Exchange(distribution=[broadcast])
         :  +- Calc(select=[cr_returned_date_sk AS QeFVW, cr_returned_time_sk AS cr_returned_time_sk_node_8, cr_item_sk AS cr_item_sk_node_8, cr_refunded_customer_sk AS cr_refunded_customer_sk_node_8, cr_refunded_cdemo_sk AS cr_refunded_cdemo_sk_node_8, cr_refunded_hdemo_sk AS cr_refunded_hdemo_sk_node_8, cr_refunded_addr_sk AS cr_refunded_addr_sk_node_8, cr_returning_customer_sk AS cr_returning_customer_sk_node_8, cr_returning_cdemo_sk AS cr_returning_cdemo_sk_node_8, cr_returning_hdemo_sk AS cr_returning_hdemo_sk_node_8, cr_returning_addr_sk AS cr_returning_addr_sk_node_8, cr_call_center_sk AS cr_call_center_sk_node_8, cr_catalog_page_sk AS cr_catalog_page_sk_node_8, cr_ship_mode_sk AS cr_ship_mode_sk_node_8, cr_warehouse_sk AS cr_warehouse_sk_node_8, cr_reason_sk AS cr_reason_sk_node_8, cr_order_number AS cr_order_number_node_8, cr_return_quantity AS cr_return_quantity_node_8, cr_return_amount AS cr_return_amount_node_8, cr_return_tax AS cr_return_tax_node_8, cr_return_amt_inc_tax AS cr_return_amt_inc_tax_node_8, cr_fee AS cr_fee_node_8, cr_return_ship_cost AS cr_return_ship_cost_node_8, cr_refunded_cash AS cr_refunded_cash_node_8, cr_reversed_charge AS cr_reversed_charge_node_8, cr_store_credit AS cr_store_credit_node_8, cr_net_loss AS cr_net_loss_node_8])
         :     +- SortLimit(orderBy=[cr_ship_mode_sk ASC], offset=[0], fetch=[1], global=[true])
         :        +- Exchange(distribution=[single])
         :           +- SortLimit(orderBy=[cr_ship_mode_sk ASC], offset=[0], fetch=[1], global=[false])
         :              +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])
         +- HashJoin(joinType=[InnerJoin], where=[(s_closed_date_sk = p_end_date_sk_node_9)], select=[p_promo_sk_node_9, p_promo_id_node_9, p_start_date_sk_node_9, p_end_date_sk_node_9, p_item_sk_node_9, p_cost_node_9, p_response_target_node_9, p_promo_name_node_9, p_channel_dmail_node_9, p_channel_email_node_9, p_channel_catalog_node_9, p_channel_tv_node_9, p_channel_radio_node_9, p_channel_press_node_9, p_channel_event_node_9, p_channel_demo_node_9, p_channel_details_node_9, p_purpose_node_9, p_discount_active_node_9, _c19, s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], isBroadcast=[true], build=[left])
            :- Exchange(distribution=[broadcast])
            :  +- Calc(select=[p_promo_sk AS p_promo_sk_node_9, p_promo_id AS p_promo_id_node_9, p_start_date_sk AS p_start_date_sk_node_9, p_end_date_sk AS p_end_date_sk_node_9, p_item_sk AS p_item_sk_node_9, p_cost AS p_cost_node_9, p_response_target AS p_response_target_node_9, p_promo_name AS p_promo_name_node_9, p_channel_dmail AS p_channel_dmail_node_9, p_channel_email AS p_channel_email_node_9, p_channel_catalog AS p_channel_catalog_node_9, p_channel_tv AS p_channel_tv_node_9, p_channel_radio AS p_channel_radio_node_9, p_channel_press AS p_channel_press_node_9, p_channel_event AS p_channel_event_node_9, p_channel_demo AS p_channel_demo_node_9, p_channel_details AS p_channel_details_node_9, p_purpose AS p_purpose_node_9, p_discount_active AS p_discount_active_node_9, 'hello' AS _c19])
            :     +- TableSourceScan(table=[[default_catalog, default_database, promotion]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])
            +- HashJoin(joinType=[InnerJoin], where=[(s_market_id = wr_returned_time_sk)], select=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], isBroadcast=[true], build=[left])
               :- Exchange(distribution=[broadcast])
               :  +- TableSourceScan(table=[[default_catalog, default_database, store]], fields=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage])
               +- TableSourceScan(table=[[default_catalog, default_database, web_returns]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0