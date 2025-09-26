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
autonode_12 = table_env.from_path("date_dim").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("date_dim").get_schema().get_field_names()])
autonode_17 = table_env.from_path("promotion").select(*[col(column_name).alias(f"{column_name}_node_17") for column_name in table_env.from_path("promotion").get_schema().get_field_names()])
autonode_18 = table_env.from_path("customer_address").select(*[col(column_name).alias(f"{column_name}_node_18") for column_name in table_env.from_path("customer_address").get_schema().get_field_names()])
autonode_19 = table_env.from_path("catalog_sales").select(*[col(column_name).alias(f"{column_name}_node_19") for column_name in table_env.from_path("catalog_sales").get_schema().get_field_names()])
autonode_14 = table_env.from_path("warehouse").select(*[col(column_name).alias(f"{column_name}_node_14") for column_name in table_env.from_path("warehouse").get_schema().get_field_names()])
autonode_15 = table_env.from_path("catalog_returns").select(*[col(column_name).alias(f"{column_name}_node_15") for column_name in table_env.from_path("catalog_returns").get_schema().get_field_names()])
autonode_16 = table_env.from_path("web_sales").select(*[col(column_name).alias(f"{column_name}_node_16") for column_name in table_env.from_path("web_sales").get_schema().get_field_names()])
autonode_7 = autonode_12.join(autonode_13, col('d_current_year_node_12') == col('hd_buy_potential_node_13'))
autonode_10 = autonode_17.join(autonode_18, col('ca_gmt_offset_node_18') == col('p_cost_node_17'))
autonode_11 = autonode_19.alias('T0DDt')
autonode_8 = autonode_14.join(autonode_15, col('w_gmt_offset_node_14') == col('cr_fee_node_15'))
autonode_9 = autonode_16.order_by(col('ws_ship_hdemo_sk_node_16'))
autonode_4 = autonode_7.filter(col('hd_demo_sk_node_13') >= 14)
autonode_6 = autonode_10.join(autonode_11, col('p_item_sk_node_17') == col('cs_promo_sk_node_19'))
autonode_5 = autonode_8.join(autonode_9, col('ws_coupon_amt_node_16') == col('cr_refunded_cash_node_15'))
autonode_2 = autonode_4.filter(col('d_fy_quarter_seq_node_12') >= 18)
autonode_3 = autonode_5.join(autonode_6, col('p_channel_tv_node_17') == col('w_suite_number_node_14'))
autonode_1 = autonode_2.join(autonode_3, col('ca_address_id_node_18') == col('hd_buy_potential_node_13'))
sink = autonode_1.group_by(col('w_street_number_node_14')).select(col('w_country_node_14').max.alias('w_country_node_14'))
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
      "error_message": "An error occurred while calling o81537962.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#164537793:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[10](input=RelSubset#164537791,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[10]), rel#164537790:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[10](input=RelSubset#164537789,groupBy=ws_coupon_amt,select=ws_coupon_amt)]
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
Caused by: java.lang.IndexOutOfBoundsException: index (10) must be less than size (1)
\tat org.apache.flink.calcite.shaded.com.google.common.base.Preconditions.checkElementIndex(Preconditions.java:1371)
\tat org.apache.flink.calcite.shaded.com.google.common.base.Preconditions.checkElementIndex(Preconditions.java:1353)
\tat org.apache.flink.calcite.shaded.com.google.common.collect.SingletonImmutableList.get(SingletonImmutableList.java:44)
\tat org.apache.calcite.util.Util$TransformingList.get(Util.java:2794)
\tat scala.collection.convert.Wrappers$JListWrapper.apply(Wrappers.scala:100)
\tat org.apache.flink.table.planner.plan.utils.RelExplainUtil$.$anonfun$collationToString$1(RelExplainUtil.scala:83)
\tat scala.collection.TraversableLike.$anonfun$map$1(TraversableLike.scala:286)
\tat scala.collection.Iterator.foreach(Iterator.scala:943)
\tat scala.collection.Iterator.foreach$(Iterator.scala:943)
\tat scala.collection.AbstractIterator.foreach(Iterator.scala:1431)
\tat scala.collection.IterableLike.foreach(IterableLike.scala:74)
\tat scala.collection.IterableLike.foreach$(IterableLike.scala:73)
\tat scala.collection.AbstractIterable.foreach(Iterable.scala:56)
\tat scala.collection.TraversableLike.map(TraversableLike.scala:286)
\tat scala.collection.TraversableLike.map$(TraversableLike.scala:279)
\tat scala.collection.AbstractTraversable.map(Traversable.scala:108)
\tat org.apache.flink.table.planner.plan.utils.RelExplainUtil$.collationToString(RelExplainUtil.scala:83)
\tat org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalSort.explainTerms(BatchPhysicalSort.scala:61)
\tat org.apache.calcite.rel.AbstractRelNode.getDigestItems(AbstractRelNode.java:414)
\tat org.apache.calcite.rel.AbstractRelNode.deepHashCode(AbstractRelNode.java:396)
\tat org.apache.calcite.rel.AbstractRelNode$InnerRelDigest.hashCode(AbstractRelNode.java:448)
\tat java.base/java.util.HashMap.hash(HashMap.java:340)
\tat java.base/java.util.HashMap.get(HashMap.java:553)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.registerImpl(VolcanoPlanner.java:1289)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.register(VolcanoPlanner.java:598)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.ensureRegistered(VolcanoPlanner.java:613)
\tat org.apache.calcite.plan.volcano.VolcanoRuleCall.transformTo(VolcanoRuleCall.java:144)
\t... 45 more
",
      "stdout": "",
      "stderr": ""
    },
    "unopt": {
      "success": true,
      "error_name": "",
      "error_message": "",
      "stdout": "== Abstract Syntax Tree ==
LogicalProject(w_country_node_14=[$1])
+- LogicalAggregate(group=[{37}], EXPR$0=[MAX($45)])
   +- LogicalJoin(condition=[=($128, $30)], joinType=[inner])
      :- LogicalFilter(condition=[>=($12, 18)])
      :  +- LogicalFilter(condition=[>=($28, 14)])
      :     +- LogicalJoin(condition=[=($27, $30)], joinType=[inner])
      :        :- LogicalProject(d_date_sk_node_12=[$0], d_date_id_node_12=[$1], d_date_node_12=[$2], d_month_seq_node_12=[$3], d_week_seq_node_12=[$4], d_quarter_seq_node_12=[$5], d_year_node_12=[$6], d_dow_node_12=[$7], d_moy_node_12=[$8], d_dom_node_12=[$9], d_qoy_node_12=[$10], d_fy_year_node_12=[$11], d_fy_quarter_seq_node_12=[$12], d_fy_week_seq_node_12=[$13], d_day_name_node_12=[$14], d_quarter_name_node_12=[$15], d_holiday_node_12=[$16], d_weekend_node_12=[$17], d_following_holiday_node_12=[$18], d_first_dom_node_12=[$19], d_last_dom_node_12=[$20], d_same_day_ly_node_12=[$21], d_same_day_lq_node_12=[$22], d_current_day_node_12=[$23], d_current_week_node_12=[$24], d_current_month_node_12=[$25], d_current_quarter_node_12=[$26], d_current_year_node_12=[$27])
      :        :  +- LogicalTableScan(table=[[default_catalog, default_database, date_dim]])
      :        +- LogicalProject(hd_demo_sk_node_13=[$0], hd_income_band_sk_node_13=[$1], hd_buy_potential_node_13=[$2], hd_dep_count_node_13=[$3], hd_vehicle_count_node_13=[$4])
      :           +- LogicalTableScan(table=[[default_catalog, default_database, household_demographics]])
      +- LogicalJoin(condition=[=($86, $7)], joinType=[inner])
         :- LogicalJoin(condition=[=($68, $37)], joinType=[inner])
         :  :- LogicalJoin(condition=[=($13, $35)], joinType=[inner])
         :  :  :- LogicalProject(w_warehouse_sk_node_14=[$0], w_warehouse_id_node_14=[$1], w_warehouse_name_node_14=[$2], w_warehouse_sq_ft_node_14=[$3], w_street_number_node_14=[$4], w_street_name_node_14=[$5], w_street_type_node_14=[$6], w_suite_number_node_14=[$7], w_city_node_14=[$8], w_county_node_14=[$9], w_state_node_14=[$10], w_zip_node_14=[$11], w_country_node_14=[$12], w_gmt_offset_node_14=[$13])
         :  :  :  +- LogicalTableScan(table=[[default_catalog, default_database, warehouse]])
         :  :  +- LogicalProject(cr_returned_date_sk_node_15=[$0], cr_returned_time_sk_node_15=[$1], cr_item_sk_node_15=[$2], cr_refunded_customer_sk_node_15=[$3], cr_refunded_cdemo_sk_node_15=[$4], cr_refunded_hdemo_sk_node_15=[$5], cr_refunded_addr_sk_node_15=[$6], cr_returning_customer_sk_node_15=[$7], cr_returning_cdemo_sk_node_15=[$8], cr_returning_hdemo_sk_node_15=[$9], cr_returning_addr_sk_node_15=[$10], cr_call_center_sk_node_15=[$11], cr_catalog_page_sk_node_15=[$12], cr_ship_mode_sk_node_15=[$13], cr_warehouse_sk_node_15=[$14], cr_reason_sk_node_15=[$15], cr_order_number_node_15=[$16], cr_return_quantity_node_15=[$17], cr_return_amount_node_15=[$18], cr_return_tax_node_15=[$19], cr_return_amt_inc_tax_node_15=[$20], cr_fee_node_15=[$21], cr_return_ship_cost_node_15=[$22], cr_refunded_cash_node_15=[$23], cr_reversed_charge_node_15=[$24], cr_store_credit_node_15=[$25], cr_net_loss_node_15=[$26])
         :  :     +- LogicalTableScan(table=[[default_catalog, default_database, catalog_returns]])
         :  +- LogicalSort(sort0=[$10], dir0=[ASC])
         :     +- LogicalProject(ws_sold_date_sk_node_16=[$0], ws_sold_time_sk_node_16=[$1], ws_ship_date_sk_node_16=[$2], ws_item_sk_node_16=[$3], ws_bill_customer_sk_node_16=[$4], ws_bill_cdemo_sk_node_16=[$5], ws_bill_hdemo_sk_node_16=[$6], ws_bill_addr_sk_node_16=[$7], ws_ship_customer_sk_node_16=[$8], ws_ship_cdemo_sk_node_16=[$9], ws_ship_hdemo_sk_node_16=[$10], ws_ship_addr_sk_node_16=[$11], ws_web_page_sk_node_16=[$12], ws_web_site_sk_node_16=[$13], ws_ship_mode_sk_node_16=[$14], ws_warehouse_sk_node_16=[$15], ws_promo_sk_node_16=[$16], ws_order_number_node_16=[$17], ws_quantity_node_16=[$18], ws_wholesale_cost_node_16=[$19], ws_list_price_node_16=[$20], ws_sales_price_node_16=[$21], ws_ext_discount_amt_node_16=[$22], ws_ext_sales_price_node_16=[$23], ws_ext_wholesale_cost_node_16=[$24], ws_ext_list_price_node_16=[$25], ws_ext_tax_node_16=[$26], ws_coupon_amt_node_16=[$27], ws_ext_ship_cost_node_16=[$28], ws_net_paid_node_16=[$29], ws_net_paid_inc_tax_node_16=[$30], ws_net_paid_inc_ship_node_16=[$31], ws_net_paid_inc_ship_tax_node_16=[$32], ws_net_profit_node_16=[$33])
         :        +- LogicalTableScan(table=[[default_catalog, default_database, web_sales]])
         +- LogicalJoin(condition=[=($4, $48)], joinType=[inner])
            :- LogicalJoin(condition=[=($30, $5)], joinType=[inner])
            :  :- LogicalProject(p_promo_sk_node_17=[$0], p_promo_id_node_17=[$1], p_start_date_sk_node_17=[$2], p_end_date_sk_node_17=[$3], p_item_sk_node_17=[$4], p_cost_node_17=[$5], p_response_target_node_17=[$6], p_promo_name_node_17=[$7], p_channel_dmail_node_17=[$8], p_channel_email_node_17=[$9], p_channel_catalog_node_17=[$10], p_channel_tv_node_17=[$11], p_channel_radio_node_17=[$12], p_channel_press_node_17=[$13], p_channel_event_node_17=[$14], p_channel_demo_node_17=[$15], p_channel_details_node_17=[$16], p_purpose_node_17=[$17], p_discount_active_node_17=[$18])
            :  :  +- LogicalTableScan(table=[[default_catalog, default_database, promotion]])
            :  +- LogicalProject(ca_address_sk_node_18=[$0], ca_address_id_node_18=[$1], ca_street_number_node_18=[$2], ca_street_name_node_18=[$3], ca_street_type_node_18=[$4], ca_suite_number_node_18=[$5], ca_city_node_18=[$6], ca_county_node_18=[$7], ca_state_node_18=[$8], ca_zip_node_18=[$9], ca_country_node_18=[$10], ca_gmt_offset_node_18=[$11], ca_location_type_node_18=[$12])
            :     +- LogicalTableScan(table=[[default_catalog, default_database, customer_address]])
            +- LogicalProject(T0DDt=[AS($0, _UTF-16LE'T0DDt')], cs_sold_time_sk_node_19=[$1], cs_ship_date_sk_node_19=[$2], cs_bill_customer_sk_node_19=[$3], cs_bill_cdemo_sk_node_19=[$4], cs_bill_hdemo_sk_node_19=[$5], cs_bill_addr_sk_node_19=[$6], cs_ship_customer_sk_node_19=[$7], cs_ship_cdemo_sk_node_19=[$8], cs_ship_hdemo_sk_node_19=[$9], cs_ship_addr_sk_node_19=[$10], cs_call_center_sk_node_19=[$11], cs_catalog_page_sk_node_19=[$12], cs_ship_mode_sk_node_19=[$13], cs_warehouse_sk_node_19=[$14], cs_item_sk_node_19=[$15], cs_promo_sk_node_19=[$16], cs_order_number_node_19=[$17], cs_quantity_node_19=[$18], cs_wholesale_cost_node_19=[$19], cs_list_price_node_19=[$20], cs_sales_price_node_19=[$21], cs_ext_discount_amt_node_19=[$22], cs_ext_sales_price_node_19=[$23], cs_ext_wholesale_cost_node_19=[$24], cs_ext_list_price_node_19=[$25], cs_ext_tax_node_19=[$26], cs_coupon_amt_node_19=[$27], cs_ext_ship_cost_node_19=[$28], cs_net_paid_node_19=[$29], cs_net_paid_inc_tax_node_19=[$30], cs_net_paid_inc_ship_node_19=[$31], cs_net_paid_inc_ship_tax_node_19=[$32], cs_net_profit_node_19=[$33])
               +- LogicalTableScan(table=[[default_catalog, default_database, catalog_sales]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS w_country_node_14])
+- SortAggregate(isMerge=[true], groupBy=[w_street_number_node_14], select=[w_street_number_node_14, Final_MAX(max$0) AS EXPR$0])
   +- Sort(orderBy=[w_street_number_node_14 ASC])
      +- Exchange(distribution=[hash[w_street_number_node_14]])
         +- LocalSortAggregate(groupBy=[w_street_number_node_14], select=[w_street_number_node_14, Partial_MAX(EXPR$0) AS max$0])
            +- Sort(orderBy=[w_street_number_node_14 ASC])
               +- HashJoin(joinType=[InnerJoin], where=[=(ca_address_id_node_18, hd_buy_potential)], select=[hd_buy_potential, w_street_number_node_14, ca_address_id_node_18, EXPR$0], isBroadcast=[true], build=[right])
                  :- HashAggregate(isMerge=[false], groupBy=[hd_buy_potential], select=[hd_buy_potential])
                  :  +- Exchange(distribution=[hash[hd_buy_potential]])
                  :     +- HashJoin(joinType=[InnerJoin], where=[=(d_current_year, hd_buy_potential)], select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count], isBroadcast=[true], build=[right])
                  :        :- Calc(select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year], where=[>=(d_fy_quarter_seq, 18)])
                  :        :  +- TableSourceScan(table=[[default_catalog, default_database, date_dim, filter=[>=(d_fy_quarter_seq, 18)]]], fields=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year])
                  :        +- Exchange(distribution=[broadcast])
                  :           +- Calc(select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count], where=[>=(hd_demo_sk, 14)])
                  :              +- TableSourceScan(table=[[default_catalog, default_database, household_demographics, filter=[>=(hd_demo_sk, 14)]]], fields=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
                  +- Exchange(distribution=[broadcast])
                     +- SortAggregate(isMerge=[true], groupBy=[w_street_number_node_14, ca_address_id_node_18], select=[w_street_number_node_14, ca_address_id_node_18, Final_MAX(max$0) AS EXPR$0])
                        +- Sort(orderBy=[w_street_number_node_14 ASC, ca_address_id_node_18 ASC])
                           +- Exchange(distribution=[hash[w_street_number_node_14, ca_address_id_node_18]])
                              +- LocalSortAggregate(groupBy=[w_street_number_node_14, ca_address_id_node_18], select=[w_street_number_node_14, ca_address_id_node_18, Partial_MAX(EXPR$0) AS max$0])
                                 +- Sort(orderBy=[w_street_number_node_14 ASC, ca_address_id_node_18 ASC])
                                    +- HashJoin(joinType=[InnerJoin], where=[=(p_channel_tv_node_17, w_suite_number_node_14)], select=[w_street_number_node_14, w_suite_number_node_14, EXPR$0, p_channel_tv_node_17, ca_address_id_node_18], isBroadcast=[true], build=[right])
                                       :- SortAggregate(isMerge=[true], groupBy=[w_street_number_node_14, w_suite_number_node_14], select=[w_street_number_node_14, w_suite_number_node_14, Final_MAX(max$0) AS EXPR$0])
                                       :  +- Sort(orderBy=[w_street_number_node_14 ASC, w_suite_number_node_14 ASC])
                                       :     +- Exchange(distribution=[hash[w_street_number_node_14, w_suite_number_node_14]])
                                       :        +- LocalSortAggregate(groupBy=[w_street_number_node_14, w_suite_number_node_14], select=[w_street_number_node_14, w_suite_number_node_14, Partial_MAX(w_country_node_14) AS max$0])
                                       :           +- Sort(orderBy=[w_street_number_node_14 ASC, w_suite_number_node_14 ASC])
                                       :              +- NestedLoopJoin(joinType=[InnerJoin], where=[=(ws_coupon_amt, cr_refunded_cash_node_15)], select=[w_warehouse_sk_node_14, w_warehouse_id_node_14, w_warehouse_name_node_14, w_warehouse_sq_ft_node_14, w_street_number_node_14, w_street_name_node_14, w_street_type_node_14, w_suite_number_node_14, w_city_node_14, w_county_node_14, w_state_node_14, w_zip_node_14, w_country_node_14, w_gmt_offset_node_14, cr_returned_date_sk_node_15, cr_returned_time_sk_node_15, cr_item_sk_node_15, cr_refunded_customer_sk_node_15, cr_refunded_cdemo_sk_node_15, cr_refunded_hdemo_sk_node_15, cr_refunded_addr_sk_node_15, cr_returning_customer_sk_node_15, cr_returning_cdemo_sk_node_15, cr_returning_hdemo_sk_node_15, cr_returning_addr_sk_node_15, cr_call_center_sk_node_15, cr_catalog_page_sk_node_15, cr_ship_mode_sk_node_15, cr_warehouse_sk_node_15, cr_reason_sk_node_15, cr_order_number_node_15, cr_return_quantity_node_15, cr_return_amount_node_15, cr_return_tax_node_15, cr_return_amt_inc_tax_node_15, cr_fee_node_15, cr_return_ship_cost_node_15, cr_refunded_cash_node_15, cr_reversed_charge_node_15, cr_store_credit_node_15, cr_net_loss_node_15, ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit], build=[right])
                                       :                 :- Calc(select=[w_warehouse_sk_node_14, w_warehouse_id_node_14, w_warehouse_name_node_14, w_warehouse_sq_ft_node_14, w_street_number_node_14, w_street_name_node_14, w_street_type_node_14, w_suite_number_node_14, w_city_node_14, w_county_node_14, w_state_node_14, w_zip_node_14, w_country_node_14, w_gmt_offset_node_14, cr_returned_date_sk AS cr_returned_date_sk_node_15, cr_returned_time_sk AS cr_returned_time_sk_node_15, cr_item_sk AS cr_item_sk_node_15, cr_refunded_customer_sk AS cr_refunded_customer_sk_node_15, cr_refunded_cdemo_sk AS cr_refunded_cdemo_sk_node_15, cr_refunded_hdemo_sk AS cr_refunded_hdemo_sk_node_15, cr_refunded_addr_sk AS cr_refunded_addr_sk_node_15, cr_returning_customer_sk AS cr_returning_customer_sk_node_15, cr_returning_cdemo_sk AS cr_returning_cdemo_sk_node_15, cr_returning_hdemo_sk AS cr_returning_hdemo_sk_node_15, cr_returning_addr_sk AS cr_returning_addr_sk_node_15, cr_call_center_sk AS cr_call_center_sk_node_15, cr_catalog_page_sk AS cr_catalog_page_sk_node_15, cr_ship_mode_sk AS cr_ship_mode_sk_node_15, cr_warehouse_sk AS cr_warehouse_sk_node_15, cr_reason_sk AS cr_reason_sk_node_15, cr_order_number AS cr_order_number_node_15, cr_return_quantity AS cr_return_quantity_node_15, cr_return_amount AS cr_return_amount_node_15, cr_return_tax AS cr_return_tax_node_15, cr_return_amt_inc_tax AS cr_return_amt_inc_tax_node_15, cr_fee AS cr_fee_node_15, cr_return_ship_cost AS cr_return_ship_cost_node_15, cr_refunded_cash AS cr_refunded_cash_node_15, cr_reversed_charge AS cr_reversed_charge_node_15, cr_store_credit AS cr_store_credit_node_15, cr_net_loss AS cr_net_loss_node_15])
                                       :                 :  +- NestedLoopJoin(joinType=[InnerJoin], where=[=(w_gmt_offset_node_140, cr_fee)], select=[w_warehouse_sk_node_14, w_warehouse_id_node_14, w_warehouse_name_node_14, w_warehouse_sq_ft_node_14, w_street_number_node_14, w_street_name_node_14, w_street_type_node_14, w_suite_number_node_14, w_city_node_14, w_county_node_14, w_state_node_14, w_zip_node_14, w_country_node_14, w_gmt_offset_node_14, w_gmt_offset_node_140, cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss], build=[left])
                                       :                 :     :- Exchange(distribution=[broadcast])
                                       :                 :     :  +- Calc(select=[w_warehouse_sk AS w_warehouse_sk_node_14, w_warehouse_id AS w_warehouse_id_node_14, w_warehouse_name AS w_warehouse_name_node_14, w_warehouse_sq_ft AS w_warehouse_sq_ft_node_14, w_street_number AS w_street_number_node_14, w_street_name AS w_street_name_node_14, w_street_type AS w_street_type_node_14, w_suite_number AS w_suite_number_node_14, w_city AS w_city_node_14, w_county AS w_county_node_14, w_state AS w_state_node_14, w_zip AS w_zip_node_14, w_country AS w_country_node_14, w_gmt_offset AS w_gmt_offset_node_14, CAST(w_gmt_offset AS DECIMAL(7, 2)) AS w_gmt_offset_node_140])
                                       :                 :     :     +- TableSourceScan(table=[[default_catalog, default_database, warehouse]], fields=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset])
                                       :                 :     +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])
                                       :                 +- Exchange(distribution=[broadcast])
                                       :                    +- SortLimit(orderBy=[ws_ship_hdemo_sk ASC], offset=[0], fetch=[1], global=[true])
                                       :                       +- Exchange(distribution=[single])
                                       :                          +- SortLimit(orderBy=[ws_ship_hdemo_sk ASC], offset=[0], fetch=[1], global=[false])
                                       :                             +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
                                       +- Exchange(distribution=[broadcast])
                                          +- HashAggregate(isMerge=[true], groupBy=[p_channel_tv_node_17, ca_address_id_node_18], select=[p_channel_tv_node_17, ca_address_id_node_18])
                                             +- Exchange(distribution=[hash[p_channel_tv_node_17, ca_address_id_node_18]])
                                                +- LocalHashAggregate(groupBy=[p_channel_tv_node_17, ca_address_id_node_18], select=[p_channel_tv_node_17, ca_address_id_node_18])
                                                   +- HashJoin(joinType=[InnerJoin], where=[=(p_item_sk_node_17, cs_promo_sk)], select=[p_item_sk_node_17, p_channel_tv_node_17, ca_address_id_node_18, cs_promo_sk], isBroadcast=[true], build=[left])
                                                      :- Exchange(distribution=[broadcast])
                                                      :  +- HashAggregate(isMerge=[true], groupBy=[p_item_sk_node_17, p_channel_tv_node_17, ca_address_id_node_18], select=[p_item_sk_node_17, p_channel_tv_node_17, ca_address_id_node_18])
                                                      :     +- Exchange(distribution=[hash[p_item_sk_node_17, p_channel_tv_node_17, ca_address_id_node_18]])
                                                      :        +- LocalHashAggregate(groupBy=[p_item_sk_node_17, p_channel_tv_node_17, ca_address_id_node_18], select=[p_item_sk_node_17, p_channel_tv_node_17, ca_address_id_node_18])
                                                      :           +- Calc(select=[p_promo_sk AS p_promo_sk_node_17, p_promo_id AS p_promo_id_node_17, p_start_date_sk AS p_start_date_sk_node_17, p_end_date_sk AS p_end_date_sk_node_17, p_item_sk AS p_item_sk_node_17, p_cost AS p_cost_node_17, p_response_target AS p_response_target_node_17, p_promo_name AS p_promo_name_node_17, p_channel_dmail AS p_channel_dmail_node_17, p_channel_email AS p_channel_email_node_17, p_channel_catalog AS p_channel_catalog_node_17, p_channel_tv AS p_channel_tv_node_17, p_channel_radio AS p_channel_radio_node_17, p_channel_press AS p_channel_press_node_17, p_channel_event AS p_channel_event_node_17, p_channel_demo AS p_channel_demo_node_17, p_channel_details AS p_channel_details_node_17, p_purpose AS p_purpose_node_17, p_discount_active AS p_discount_active_node_17, ca_address_sk_node_18, ca_address_id_node_18, ca_street_number_node_18, ca_street_name_node_18, ca_street_type_node_18, ca_suite_number_node_18, ca_city_node_18, ca_county_node_18, ca_state_node_18, ca_zip_node_18, ca_country_node_18, ca_gmt_offset_node_18, ca_location_type_node_18])
                                                      :              +- HashJoin(joinType=[InnerJoin], where=[=(ca_gmt_offset_node_180, p_cost)], select=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active, ca_address_sk_node_18, ca_address_id_node_18, ca_street_number_node_18, ca_street_name_node_18, ca_street_type_node_18, ca_suite_number_node_18, ca_city_node_18, ca_county_node_18, ca_state_node_18, ca_zip_node_18, ca_country_node_18, ca_gmt_offset_node_18, ca_location_type_node_18, ca_gmt_offset_node_180], isBroadcast=[true], build=[left])
                                                      :                 :- Exchange(distribution=[broadcast])
                                                      :                 :  +- TableSourceScan(table=[[default_catalog, default_database, promotion]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])
                                                      :                 +- Calc(select=[ca_address_sk AS ca_address_sk_node_18, ca_address_id AS ca_address_id_node_18, ca_street_number AS ca_street_number_node_18, ca_street_name AS ca_street_name_node_18, ca_street_type AS ca_street_type_node_18, ca_suite_number AS ca_suite_number_node_18, ca_city AS ca_city_node_18, ca_county AS ca_county_node_18, ca_state AS ca_state_node_18, ca_zip AS ca_zip_node_18, ca_country AS ca_country_node_18, ca_gmt_offset AS ca_gmt_offset_node_18, ca_location_type AS ca_location_type_node_18, CAST(ca_gmt_offset AS DECIMAL(15, 2)) AS ca_gmt_offset_node_180])
                                                      :                    +- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])
                                                      +- HashAggregate(isMerge=[true], groupBy=[cs_promo_sk], select=[cs_promo_sk])
                                                         +- Exchange(distribution=[hash[cs_promo_sk]])
                                                            +- LocalHashAggregate(groupBy=[cs_promo_sk], select=[cs_promo_sk])
                                                               +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS w_country_node_14])
+- SortAggregate(isMerge=[true], groupBy=[w_street_number_node_14], select=[w_street_number_node_14, Final_MAX(max$0) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[w_street_number_node_14 ASC])
         +- Exchange(distribution=[hash[w_street_number_node_14]])
            +- LocalSortAggregate(groupBy=[w_street_number_node_14], select=[w_street_number_node_14, Partial_MAX(EXPR$0) AS max$0])
               +- Exchange(distribution=[forward])
                  +- Sort(orderBy=[w_street_number_node_14 ASC])
                     +- HashJoin(joinType=[InnerJoin], where=[(ca_address_id_node_18 = hd_buy_potential)], select=[hd_buy_potential, w_street_number_node_14, ca_address_id_node_18, EXPR$0], isBroadcast=[true], build=[right])
                        :- HashAggregate(isMerge=[false], groupBy=[hd_buy_potential], select=[hd_buy_potential])
                        :  +- Exchange(distribution=[hash[hd_buy_potential]])
                        :     +- HashJoin(joinType=[InnerJoin], where=[(d_current_year = hd_buy_potential)], select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count], isBroadcast=[true], build=[right])
                        :        :- Calc(select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year], where=[(d_fy_quarter_seq >= 18)])
                        :        :  +- TableSourceScan(table=[[default_catalog, default_database, date_dim, filter=[>=(d_fy_quarter_seq, 18)]]], fields=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year])
                        :        +- Exchange(distribution=[broadcast])
                        :           +- Calc(select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count], where=[(hd_demo_sk >= 14)])
                        :              +- TableSourceScan(table=[[default_catalog, default_database, household_demographics, filter=[>=(hd_demo_sk, 14)]]], fields=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
                        +- Exchange(distribution=[broadcast])
                           +- SortAggregate(isMerge=[true], groupBy=[w_street_number_node_14, ca_address_id_node_18], select=[w_street_number_node_14, ca_address_id_node_18, Final_MAX(max$0) AS EXPR$0])
                              +- Exchange(distribution=[forward])
                                 +- Sort(orderBy=[w_street_number_node_14 ASC, ca_address_id_node_18 ASC])
                                    +- Exchange(distribution=[hash[w_street_number_node_14, ca_address_id_node_18]])
                                       +- LocalSortAggregate(groupBy=[w_street_number_node_14, ca_address_id_node_18], select=[w_street_number_node_14, ca_address_id_node_18, Partial_MAX(EXPR$0) AS max$0])
                                          +- Exchange(distribution=[forward])
                                             +- Sort(orderBy=[w_street_number_node_14 ASC, ca_address_id_node_18 ASC])
                                                +- HashJoin(joinType=[InnerJoin], where=[(p_channel_tv_node_17 = w_suite_number_node_14)], select=[w_street_number_node_14, w_suite_number_node_14, EXPR$0, p_channel_tv_node_17, ca_address_id_node_18], isBroadcast=[true], build=[right])
                                                   :- SortAggregate(isMerge=[true], groupBy=[w_street_number_node_14, w_suite_number_node_14], select=[w_street_number_node_14, w_suite_number_node_14, Final_MAX(max$0) AS EXPR$0])
                                                   :  +- Exchange(distribution=[forward])
                                                   :     +- Sort(orderBy=[w_street_number_node_14 ASC, w_suite_number_node_14 ASC])
                                                   :        +- Exchange(distribution=[hash[w_street_number_node_14, w_suite_number_node_14]])
                                                   :           +- LocalSortAggregate(groupBy=[w_street_number_node_14, w_suite_number_node_14], select=[w_street_number_node_14, w_suite_number_node_14, Partial_MAX(w_country_node_14) AS max$0])
                                                   :              +- Exchange(distribution=[forward])
                                                   :                 +- Sort(orderBy=[w_street_number_node_14 ASC, w_suite_number_node_14 ASC])
                                                   :                    +- NestedLoopJoin(joinType=[InnerJoin], where=[(ws_coupon_amt = cr_refunded_cash_node_15)], select=[w_warehouse_sk_node_14, w_warehouse_id_node_14, w_warehouse_name_node_14, w_warehouse_sq_ft_node_14, w_street_number_node_14, w_street_name_node_14, w_street_type_node_14, w_suite_number_node_14, w_city_node_14, w_county_node_14, w_state_node_14, w_zip_node_14, w_country_node_14, w_gmt_offset_node_14, cr_returned_date_sk_node_15, cr_returned_time_sk_node_15, cr_item_sk_node_15, cr_refunded_customer_sk_node_15, cr_refunded_cdemo_sk_node_15, cr_refunded_hdemo_sk_node_15, cr_refunded_addr_sk_node_15, cr_returning_customer_sk_node_15, cr_returning_cdemo_sk_node_15, cr_returning_hdemo_sk_node_15, cr_returning_addr_sk_node_15, cr_call_center_sk_node_15, cr_catalog_page_sk_node_15, cr_ship_mode_sk_node_15, cr_warehouse_sk_node_15, cr_reason_sk_node_15, cr_order_number_node_15, cr_return_quantity_node_15, cr_return_amount_node_15, cr_return_tax_node_15, cr_return_amt_inc_tax_node_15, cr_fee_node_15, cr_return_ship_cost_node_15, cr_refunded_cash_node_15, cr_reversed_charge_node_15, cr_store_credit_node_15, cr_net_loss_node_15, ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit], build=[right])
                                                   :                       :- Calc(select=[w_warehouse_sk_node_14, w_warehouse_id_node_14, w_warehouse_name_node_14, w_warehouse_sq_ft_node_14, w_street_number_node_14, w_street_name_node_14, w_street_type_node_14, w_suite_number_node_14, w_city_node_14, w_county_node_14, w_state_node_14, w_zip_node_14, w_country_node_14, w_gmt_offset_node_14, cr_returned_date_sk AS cr_returned_date_sk_node_15, cr_returned_time_sk AS cr_returned_time_sk_node_15, cr_item_sk AS cr_item_sk_node_15, cr_refunded_customer_sk AS cr_refunded_customer_sk_node_15, cr_refunded_cdemo_sk AS cr_refunded_cdemo_sk_node_15, cr_refunded_hdemo_sk AS cr_refunded_hdemo_sk_node_15, cr_refunded_addr_sk AS cr_refunded_addr_sk_node_15, cr_returning_customer_sk AS cr_returning_customer_sk_node_15, cr_returning_cdemo_sk AS cr_returning_cdemo_sk_node_15, cr_returning_hdemo_sk AS cr_returning_hdemo_sk_node_15, cr_returning_addr_sk AS cr_returning_addr_sk_node_15, cr_call_center_sk AS cr_call_center_sk_node_15, cr_catalog_page_sk AS cr_catalog_page_sk_node_15, cr_ship_mode_sk AS cr_ship_mode_sk_node_15, cr_warehouse_sk AS cr_warehouse_sk_node_15, cr_reason_sk AS cr_reason_sk_node_15, cr_order_number AS cr_order_number_node_15, cr_return_quantity AS cr_return_quantity_node_15, cr_return_amount AS cr_return_amount_node_15, cr_return_tax AS cr_return_tax_node_15, cr_return_amt_inc_tax AS cr_return_amt_inc_tax_node_15, cr_fee AS cr_fee_node_15, cr_return_ship_cost AS cr_return_ship_cost_node_15, cr_refunded_cash AS cr_refunded_cash_node_15, cr_reversed_charge AS cr_reversed_charge_node_15, cr_store_credit AS cr_store_credit_node_15, cr_net_loss AS cr_net_loss_node_15])
                                                   :                       :  +- NestedLoopJoin(joinType=[InnerJoin], where=[(w_gmt_offset_node_140 = cr_fee)], select=[w_warehouse_sk_node_14, w_warehouse_id_node_14, w_warehouse_name_node_14, w_warehouse_sq_ft_node_14, w_street_number_node_14, w_street_name_node_14, w_street_type_node_14, w_suite_number_node_14, w_city_node_14, w_county_node_14, w_state_node_14, w_zip_node_14, w_country_node_14, w_gmt_offset_node_14, w_gmt_offset_node_140, cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss], build=[left])
                                                   :                       :     :- Exchange(distribution=[broadcast])
                                                   :                       :     :  +- Calc(select=[w_warehouse_sk AS w_warehouse_sk_node_14, w_warehouse_id AS w_warehouse_id_node_14, w_warehouse_name AS w_warehouse_name_node_14, w_warehouse_sq_ft AS w_warehouse_sq_ft_node_14, w_street_number AS w_street_number_node_14, w_street_name AS w_street_name_node_14, w_street_type AS w_street_type_node_14, w_suite_number AS w_suite_number_node_14, w_city AS w_city_node_14, w_county AS w_county_node_14, w_state AS w_state_node_14, w_zip AS w_zip_node_14, w_country AS w_country_node_14, w_gmt_offset AS w_gmt_offset_node_14, CAST(w_gmt_offset AS DECIMAL(7, 2)) AS w_gmt_offset_node_140])
                                                   :                       :     :     +- TableSourceScan(table=[[default_catalog, default_database, warehouse]], fields=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset])
                                                   :                       :     +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])
                                                   :                       +- Exchange(distribution=[broadcast])
                                                   :                          +- SortLimit(orderBy=[ws_ship_hdemo_sk ASC], offset=[0], fetch=[1], global=[true])
                                                   :                             +- Exchange(distribution=[single])
                                                   :                                +- SortLimit(orderBy=[ws_ship_hdemo_sk ASC], offset=[0], fetch=[1], global=[false])
                                                   :                                   +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
                                                   +- Exchange(distribution=[broadcast])
                                                      +- HashAggregate(isMerge=[true], groupBy=[p_channel_tv_node_17, ca_address_id_node_18], select=[p_channel_tv_node_17, ca_address_id_node_18])
                                                         +- Exchange(distribution=[hash[p_channel_tv_node_17, ca_address_id_node_18]])
                                                            +- LocalHashAggregate(groupBy=[p_channel_tv_node_17, ca_address_id_node_18], select=[p_channel_tv_node_17, ca_address_id_node_18])
                                                               +- HashJoin(joinType=[InnerJoin], where=[(p_item_sk_node_17 = cs_promo_sk)], select=[p_item_sk_node_17, p_channel_tv_node_17, ca_address_id_node_18, cs_promo_sk], isBroadcast=[true], build=[left])
                                                                  :- Exchange(distribution=[broadcast])
                                                                  :  +- HashAggregate(isMerge=[true], groupBy=[p_item_sk_node_17, p_channel_tv_node_17, ca_address_id_node_18], select=[p_item_sk_node_17, p_channel_tv_node_17, ca_address_id_node_18])
                                                                  :     +- Exchange(distribution=[hash[p_item_sk_node_17, p_channel_tv_node_17, ca_address_id_node_18]])
                                                                  :        +- LocalHashAggregate(groupBy=[p_item_sk_node_17, p_channel_tv_node_17, ca_address_id_node_18], select=[p_item_sk_node_17, p_channel_tv_node_17, ca_address_id_node_18])
                                                                  :           +- Calc(select=[p_promo_sk AS p_promo_sk_node_17, p_promo_id AS p_promo_id_node_17, p_start_date_sk AS p_start_date_sk_node_17, p_end_date_sk AS p_end_date_sk_node_17, p_item_sk AS p_item_sk_node_17, p_cost AS p_cost_node_17, p_response_target AS p_response_target_node_17, p_promo_name AS p_promo_name_node_17, p_channel_dmail AS p_channel_dmail_node_17, p_channel_email AS p_channel_email_node_17, p_channel_catalog AS p_channel_catalog_node_17, p_channel_tv AS p_channel_tv_node_17, p_channel_radio AS p_channel_radio_node_17, p_channel_press AS p_channel_press_node_17, p_channel_event AS p_channel_event_node_17, p_channel_demo AS p_channel_demo_node_17, p_channel_details AS p_channel_details_node_17, p_purpose AS p_purpose_node_17, p_discount_active AS p_discount_active_node_17, ca_address_sk_node_18, ca_address_id_node_18, ca_street_number_node_18, ca_street_name_node_18, ca_street_type_node_18, ca_suite_number_node_18, ca_city_node_18, ca_county_node_18, ca_state_node_18, ca_zip_node_18, ca_country_node_18, ca_gmt_offset_node_18, ca_location_type_node_18])
                                                                  :              +- HashJoin(joinType=[InnerJoin], where=[(ca_gmt_offset_node_180 = p_cost)], select=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active, ca_address_sk_node_18, ca_address_id_node_18, ca_street_number_node_18, ca_street_name_node_18, ca_street_type_node_18, ca_suite_number_node_18, ca_city_node_18, ca_county_node_18, ca_state_node_18, ca_zip_node_18, ca_country_node_18, ca_gmt_offset_node_18, ca_location_type_node_18, ca_gmt_offset_node_180], isBroadcast=[true], build=[left])
                                                                  :                 :- Exchange(distribution=[broadcast])
                                                                  :                 :  +- TableSourceScan(table=[[default_catalog, default_database, promotion]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])
                                                                  :                 +- Calc(select=[ca_address_sk AS ca_address_sk_node_18, ca_address_id AS ca_address_id_node_18, ca_street_number AS ca_street_number_node_18, ca_street_name AS ca_street_name_node_18, ca_street_type AS ca_street_type_node_18, ca_suite_number AS ca_suite_number_node_18, ca_city AS ca_city_node_18, ca_county AS ca_county_node_18, ca_state AS ca_state_node_18, ca_zip AS ca_zip_node_18, ca_country AS ca_country_node_18, ca_gmt_offset AS ca_gmt_offset_node_18, ca_location_type AS ca_location_type_node_18, CAST(ca_gmt_offset AS DECIMAL(15, 2)) AS ca_gmt_offset_node_180])
                                                                  :                    +- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])
                                                                  +- HashAggregate(isMerge=[true], groupBy=[cs_promo_sk], select=[cs_promo_sk])
                                                                     +- Exchange(distribution=[hash[cs_promo_sk]])
                                                                        +- LocalHashAggregate(groupBy=[cs_promo_sk], select=[cs_promo_sk])
                                                                           +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0