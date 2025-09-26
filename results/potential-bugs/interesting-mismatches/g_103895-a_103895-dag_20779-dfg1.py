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
    return values.product()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_17 = table_env.from_path("web_returns").select(*[col(column_name).alias(f"{column_name}_node_17") for column_name in table_env.from_path("web_returns").get_schema().get_field_names()])
autonode_18 = table_env.from_path("call_center").select(*[col(column_name).alias(f"{column_name}_node_18") for column_name in table_env.from_path("call_center").get_schema().get_field_names()])
autonode_19 = table_env.from_path("reason").select(*[col(column_name).alias(f"{column_name}_node_19") for column_name in table_env.from_path("reason").get_schema().get_field_names()])
autonode_21 = table_env.from_path("web_sales").select(*[col(column_name).alias(f"{column_name}_node_21") for column_name in table_env.from_path("web_sales").get_schema().get_field_names()])
autonode_20 = table_env.from_path("store").select(*[col(column_name).alias(f"{column_name}_node_20") for column_name in table_env.from_path("store").get_schema().get_field_names()])
autonode_16 = table_env.from_path("web_returns").select(*[col(column_name).alias(f"{column_name}_node_16") for column_name in table_env.from_path("web_returns").get_schema().get_field_names()])
autonode_23 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_23") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_22 = table_env.from_path("ship_mode").select(*[col(column_name).alias(f"{column_name}_node_22") for column_name in table_env.from_path("ship_mode").get_schema().get_field_names()])
autonode_12 = autonode_18.join(autonode_19, col('r_reason_sk_node_19') == col('cc_call_center_sk_node_18'))
autonode_14 = autonode_21.limit(64)
autonode_13 = autonode_20.order_by(col('s_division_id_node_20'))
autonode_11 = autonode_16.join(autonode_17, col('wr_returning_addr_sk_node_17') == col('wr_reason_sk_node_16'))
autonode_15 = autonode_22.join(autonode_23, col('sm_ship_mode_sk_node_22') == col('ss_cdemo_sk_node_23'))
autonode_8 = autonode_12.add_columns(lit("hello"))
autonode_9 = autonode_13.order_by(col('s_city_node_20'))
autonode_7 = autonode_11.filter(col('wr_returning_customer_sk_node_16') <= 22)
autonode_10 = autonode_14.join(autonode_15, col('ws_wholesale_cost_node_21') == col('ss_net_paid_node_23'))
autonode_5 = autonode_8.join(autonode_9, col('s_gmt_offset_node_20') == col('cc_gmt_offset_node_18'))
autonode_4 = autonode_7.order_by(col('wr_refunded_cdemo_sk_node_16'))
autonode_6 = autonode_10.filter(col('ss_quantity_node_23') < -24)
autonode_2 = autonode_4.join(autonode_5, col('wr_returned_time_sk_node_17') == col('cc_employees_node_18'))
autonode_3 = autonode_6.alias('mgrT6')
autonode_1 = autonode_2.join(autonode_3, col('ss_ticket_number_node_23') == col('cc_mkt_id_node_18'))
sink = autonode_1.group_by(col('cc_closed_date_sk_node_18')).select(col('r_reason_sk_node_19').min.alias('r_reason_sk_node_19'))
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
      "error_message": "An error occurred while calling o56664672.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#113668726:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[4](input=RelSubset#113668724,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[4]), rel#113668723:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[4](input=RelSubset#113668722,groupBy=wr_returned_time_sk0,select=wr_returned_time_sk0)]
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
Caused by: java.lang.IndexOutOfBoundsException: index (4) must be less than size (1)
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
LogicalProject(r_reason_sk_node_19=[$1])
+- LogicalAggregate(group=[{52}], EXPR$0=[MIN($79)])
   +- LogicalJoin(condition=[=($161, $60)], joinType=[inner])
      :- LogicalJoin(condition=[=($25, $56)], joinType=[inner])
      :  :- LogicalSort(sort0=[$4], dir0=[ASC])
      :  :  +- LogicalFilter(condition=[<=($7, 22)])
      :  :     +- LogicalJoin(condition=[=($34, $12)], joinType=[inner])
      :  :        :- LogicalProject(wr_returned_date_sk_node_16=[$0], wr_returned_time_sk_node_16=[$1], wr_item_sk_node_16=[$2], wr_refunded_customer_sk_node_16=[$3], wr_refunded_cdemo_sk_node_16=[$4], wr_refunded_hdemo_sk_node_16=[$5], wr_refunded_addr_sk_node_16=[$6], wr_returning_customer_sk_node_16=[$7], wr_returning_cdemo_sk_node_16=[$8], wr_returning_hdemo_sk_node_16=[$9], wr_returning_addr_sk_node_16=[$10], wr_web_page_sk_node_16=[$11], wr_reason_sk_node_16=[$12], wr_order_number_node_16=[$13], wr_return_quantity_node_16=[$14], wr_return_amt_node_16=[$15], wr_return_tax_node_16=[$16], wr_return_amt_inc_tax_node_16=[$17], wr_fee_node_16=[$18], wr_return_ship_cost_node_16=[$19], wr_refunded_cash_node_16=[$20], wr_reversed_charge_node_16=[$21], wr_account_credit_node_16=[$22], wr_net_loss_node_16=[$23])
      :  :        :  +- LogicalTableScan(table=[[default_catalog, default_database, web_returns]])
      :  :        +- LogicalProject(wr_returned_date_sk_node_17=[$0], wr_returned_time_sk_node_17=[$1], wr_item_sk_node_17=[$2], wr_refunded_customer_sk_node_17=[$3], wr_refunded_cdemo_sk_node_17=[$4], wr_refunded_hdemo_sk_node_17=[$5], wr_refunded_addr_sk_node_17=[$6], wr_returning_customer_sk_node_17=[$7], wr_returning_cdemo_sk_node_17=[$8], wr_returning_hdemo_sk_node_17=[$9], wr_returning_addr_sk_node_17=[$10], wr_web_page_sk_node_17=[$11], wr_reason_sk_node_17=[$12], wr_order_number_node_17=[$13], wr_return_quantity_node_17=[$14], wr_return_amt_node_17=[$15], wr_return_tax_node_17=[$16], wr_return_amt_inc_tax_node_17=[$17], wr_fee_node_17=[$18], wr_return_ship_cost_node_17=[$19], wr_refunded_cash_node_17=[$20], wr_reversed_charge_node_17=[$21], wr_account_credit_node_17=[$22], wr_net_loss_node_17=[$23])
      :  :           +- LogicalTableScan(table=[[default_catalog, default_database, web_returns]])
      :  +- LogicalJoin(condition=[=($62, $29)], joinType=[inner])
      :     :- LogicalProject(cc_call_center_sk_node_18=[$0], cc_call_center_id_node_18=[$1], cc_rec_start_date_node_18=[$2], cc_rec_end_date_node_18=[$3], cc_closed_date_sk_node_18=[$4], cc_open_date_sk_node_18=[$5], cc_name_node_18=[$6], cc_class_node_18=[$7], cc_employees_node_18=[$8], cc_sq_ft_node_18=[$9], cc_hours_node_18=[$10], cc_manager_node_18=[$11], cc_mkt_id_node_18=[$12], cc_mkt_class_node_18=[$13], cc_mkt_desc_node_18=[$14], cc_market_manager_node_18=[$15], cc_division_node_18=[$16], cc_division_name_node_18=[$17], cc_company_node_18=[$18], cc_company_name_node_18=[$19], cc_street_number_node_18=[$20], cc_street_name_node_18=[$21], cc_street_type_node_18=[$22], cc_suite_number_node_18=[$23], cc_city_node_18=[$24], cc_county_node_18=[$25], cc_state_node_18=[$26], cc_zip_node_18=[$27], cc_country_node_18=[$28], cc_gmt_offset_node_18=[$29], cc_tax_percentage_node_18=[$30], r_reason_sk_node_19=[$31], r_reason_id_node_19=[$32], r_reason_desc_node_19=[$33], _c34=[_UTF-16LE'hello'])
      :     :  +- LogicalJoin(condition=[=($31, $0)], joinType=[inner])
      :     :     :- LogicalProject(cc_call_center_sk_node_18=[$0], cc_call_center_id_node_18=[$1], cc_rec_start_date_node_18=[$2], cc_rec_end_date_node_18=[$3], cc_closed_date_sk_node_18=[$4], cc_open_date_sk_node_18=[$5], cc_name_node_18=[$6], cc_class_node_18=[$7], cc_employees_node_18=[$8], cc_sq_ft_node_18=[$9], cc_hours_node_18=[$10], cc_manager_node_18=[$11], cc_mkt_id_node_18=[$12], cc_mkt_class_node_18=[$13], cc_mkt_desc_node_18=[$14], cc_market_manager_node_18=[$15], cc_division_node_18=[$16], cc_division_name_node_18=[$17], cc_company_node_18=[$18], cc_company_name_node_18=[$19], cc_street_number_node_18=[$20], cc_street_name_node_18=[$21], cc_street_type_node_18=[$22], cc_suite_number_node_18=[$23], cc_city_node_18=[$24], cc_county_node_18=[$25], cc_state_node_18=[$26], cc_zip_node_18=[$27], cc_country_node_18=[$28], cc_gmt_offset_node_18=[$29], cc_tax_percentage_node_18=[$30])
      :     :     :  +- LogicalTableScan(table=[[default_catalog, default_database, call_center]])
      :     :     +- LogicalProject(r_reason_sk_node_19=[$0], r_reason_id_node_19=[$1], r_reason_desc_node_19=[$2])
      :     :        +- LogicalTableScan(table=[[default_catalog, default_database, reason]])
      :     +- LogicalSort(sort0=[$22], dir0=[ASC])
      :        +- LogicalSort(sort0=[$14], dir0=[ASC])
      :           +- LogicalProject(s_store_sk_node_20=[$0], s_store_id_node_20=[$1], s_rec_start_date_node_20=[$2], s_rec_end_date_node_20=[$3], s_closed_date_sk_node_20=[$4], s_store_name_node_20=[$5], s_number_employees_node_20=[$6], s_floor_space_node_20=[$7], s_hours_node_20=[$8], s_manager_node_20=[$9], s_market_id_node_20=[$10], s_geography_class_node_20=[$11], s_market_desc_node_20=[$12], s_market_manager_node_20=[$13], s_division_id_node_20=[$14], s_division_name_node_20=[$15], s_company_id_node_20=[$16], s_company_name_node_20=[$17], s_street_number_node_20=[$18], s_street_name_node_20=[$19], s_street_type_node_20=[$20], s_suite_number_node_20=[$21], s_city_node_20=[$22], s_county_node_20=[$23], s_state_node_20=[$24], s_zip_node_20=[$25], s_country_node_20=[$26], s_gmt_offset_node_20=[$27], s_tax_precentage_node_20=[$28])
      :              +- LogicalTableScan(table=[[default_catalog, default_database, store]])
      +- LogicalProject(mgrT6=[AS($0, _UTF-16LE'mgrT6')], ws_sold_time_sk_node_21=[$1], ws_ship_date_sk_node_21=[$2], ws_item_sk_node_21=[$3], ws_bill_customer_sk_node_21=[$4], ws_bill_cdemo_sk_node_21=[$5], ws_bill_hdemo_sk_node_21=[$6], ws_bill_addr_sk_node_21=[$7], ws_ship_customer_sk_node_21=[$8], ws_ship_cdemo_sk_node_21=[$9], ws_ship_hdemo_sk_node_21=[$10], ws_ship_addr_sk_node_21=[$11], ws_web_page_sk_node_21=[$12], ws_web_site_sk_node_21=[$13], ws_ship_mode_sk_node_21=[$14], ws_warehouse_sk_node_21=[$15], ws_promo_sk_node_21=[$16], ws_order_number_node_21=[$17], ws_quantity_node_21=[$18], ws_wholesale_cost_node_21=[$19], ws_list_price_node_21=[$20], ws_sales_price_node_21=[$21], ws_ext_discount_amt_node_21=[$22], ws_ext_sales_price_node_21=[$23], ws_ext_wholesale_cost_node_21=[$24], ws_ext_list_price_node_21=[$25], ws_ext_tax_node_21=[$26], ws_coupon_amt_node_21=[$27], ws_ext_ship_cost_node_21=[$28], ws_net_paid_node_21=[$29], ws_net_paid_inc_tax_node_21=[$30], ws_net_paid_inc_ship_node_21=[$31], ws_net_paid_inc_ship_tax_node_21=[$32], ws_net_profit_node_21=[$33], sm_ship_mode_sk_node_22=[$34], sm_ship_mode_id_node_22=[$35], sm_type_node_22=[$36], sm_code_node_22=[$37], sm_carrier_node_22=[$38], sm_contract_node_22=[$39], ss_sold_date_sk_node_23=[$40], ss_sold_time_sk_node_23=[$41], ss_item_sk_node_23=[$42], ss_customer_sk_node_23=[$43], ss_cdemo_sk_node_23=[$44], ss_hdemo_sk_node_23=[$45], ss_addr_sk_node_23=[$46], ss_store_sk_node_23=[$47], ss_promo_sk_node_23=[$48], ss_ticket_number_node_23=[$49], ss_quantity_node_23=[$50], ss_wholesale_cost_node_23=[$51], ss_list_price_node_23=[$52], ss_sales_price_node_23=[$53], ss_ext_discount_amt_node_23=[$54], ss_ext_sales_price_node_23=[$55], ss_ext_wholesale_cost_node_23=[$56], ss_ext_list_price_node_23=[$57], ss_ext_tax_node_23=[$58], ss_coupon_amt_node_23=[$59], ss_net_paid_node_23=[$60], ss_net_paid_inc_tax_node_23=[$61], ss_net_profit_node_23=[$62])
         +- LogicalFilter(condition=[<($50, -24)])
            +- LogicalJoin(condition=[=($19, $60)], joinType=[inner])
               :- LogicalSort(fetch=[64])
               :  +- LogicalProject(ws_sold_date_sk_node_21=[$0], ws_sold_time_sk_node_21=[$1], ws_ship_date_sk_node_21=[$2], ws_item_sk_node_21=[$3], ws_bill_customer_sk_node_21=[$4], ws_bill_cdemo_sk_node_21=[$5], ws_bill_hdemo_sk_node_21=[$6], ws_bill_addr_sk_node_21=[$7], ws_ship_customer_sk_node_21=[$8], ws_ship_cdemo_sk_node_21=[$9], ws_ship_hdemo_sk_node_21=[$10], ws_ship_addr_sk_node_21=[$11], ws_web_page_sk_node_21=[$12], ws_web_site_sk_node_21=[$13], ws_ship_mode_sk_node_21=[$14], ws_warehouse_sk_node_21=[$15], ws_promo_sk_node_21=[$16], ws_order_number_node_21=[$17], ws_quantity_node_21=[$18], ws_wholesale_cost_node_21=[$19], ws_list_price_node_21=[$20], ws_sales_price_node_21=[$21], ws_ext_discount_amt_node_21=[$22], ws_ext_sales_price_node_21=[$23], ws_ext_wholesale_cost_node_21=[$24], ws_ext_list_price_node_21=[$25], ws_ext_tax_node_21=[$26], ws_coupon_amt_node_21=[$27], ws_ext_ship_cost_node_21=[$28], ws_net_paid_node_21=[$29], ws_net_paid_inc_tax_node_21=[$30], ws_net_paid_inc_ship_node_21=[$31], ws_net_paid_inc_ship_tax_node_21=[$32], ws_net_profit_node_21=[$33])
               :     +- LogicalTableScan(table=[[default_catalog, default_database, web_sales]])
               +- LogicalJoin(condition=[=($0, $10)], joinType=[inner])
                  :- LogicalProject(sm_ship_mode_sk_node_22=[$0], sm_ship_mode_id_node_22=[$1], sm_type_node_22=[$2], sm_code_node_22=[$3], sm_carrier_node_22=[$4], sm_contract_node_22=[$5])
                  :  +- LogicalTableScan(table=[[default_catalog, default_database, ship_mode]])
                  +- LogicalProject(ss_sold_date_sk_node_23=[$0], ss_sold_time_sk_node_23=[$1], ss_item_sk_node_23=[$2], ss_customer_sk_node_23=[$3], ss_cdemo_sk_node_23=[$4], ss_hdemo_sk_node_23=[$5], ss_addr_sk_node_23=[$6], ss_store_sk_node_23=[$7], ss_promo_sk_node_23=[$8], ss_ticket_number_node_23=[$9], ss_quantity_node_23=[$10], ss_wholesale_cost_node_23=[$11], ss_list_price_node_23=[$12], ss_sales_price_node_23=[$13], ss_ext_discount_amt_node_23=[$14], ss_ext_sales_price_node_23=[$15], ss_ext_wholesale_cost_node_23=[$16], ss_ext_list_price_node_23=[$17], ss_ext_tax_node_23=[$18], ss_coupon_amt_node_23=[$19], ss_net_paid_node_23=[$20], ss_net_paid_inc_tax_node_23=[$21], ss_net_profit_node_23=[$22])
                     +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS r_reason_sk_node_19])
+- SortAggregate(isMerge=[false], groupBy=[cc_closed_date_sk_node_18], select=[cc_closed_date_sk_node_18, MIN(EXPR$0) AS EXPR$0])
   +- Sort(orderBy=[cc_closed_date_sk_node_18 ASC])
      +- Exchange(distribution=[hash[cc_closed_date_sk_node_18]])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[=(ss_ticket_number, cc_mkt_id_node_18)], select=[cc_closed_date_sk_node_18, cc_mkt_id_node_18, EXPR$0, ss_ticket_number], build=[left])
            :- Exchange(distribution=[broadcast])
            :  +- SortAggregate(isMerge=[false], groupBy=[cc_closed_date_sk_node_18, cc_mkt_id_node_18], select=[cc_closed_date_sk_node_18, cc_mkt_id_node_18, MIN(r_reason_sk_node_19) AS EXPR$0])
            :     +- Sort(orderBy=[cc_closed_date_sk_node_18 ASC, cc_mkt_id_node_18 ASC])
            :        +- Exchange(distribution=[hash[cc_closed_date_sk_node_18, cc_mkt_id_node_18]])
            :           +- NestedLoopJoin(joinType=[InnerJoin], where=[=(wr_returned_time_sk0, cc_employees_node_18)], select=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss, wr_returned_date_sk0, wr_returned_time_sk0, wr_item_sk0, wr_refunded_customer_sk0, wr_refunded_cdemo_sk0, wr_refunded_hdemo_sk0, wr_refunded_addr_sk0, wr_returning_customer_sk0, wr_returning_cdemo_sk0, wr_returning_hdemo_sk0, wr_returning_addr_sk0, wr_web_page_sk0, wr_reason_sk0, wr_order_number0, wr_return_quantity0, wr_return_amt0, wr_return_tax0, wr_return_amt_inc_tax0, wr_fee0, wr_return_ship_cost0, wr_refunded_cash0, wr_reversed_charge0, wr_account_credit0, wr_net_loss0, cc_call_center_sk_node_18, cc_call_center_id_node_18, cc_rec_start_date_node_18, cc_rec_end_date_node_18, cc_closed_date_sk_node_18, cc_open_date_sk_node_18, cc_name_node_18, cc_class_node_18, cc_employees_node_18, cc_sq_ft_node_18, cc_hours_node_18, cc_manager_node_18, cc_mkt_id_node_18, cc_mkt_class_node_18, cc_mkt_desc_node_18, cc_market_manager_node_18, cc_division_node_18, cc_division_name_node_18, cc_company_node_18, cc_company_name_node_18, cc_street_number_node_18, cc_street_name_node_18, cc_street_type_node_18, cc_suite_number_node_18, cc_city_node_18, cc_county_node_18, cc_state_node_18, cc_zip_node_18, cc_country_node_18, cc_gmt_offset_node_18, cc_tax_percentage_node_18, r_reason_sk_node_19, r_reason_id_node_19, r_reason_desc_node_19, _c34, s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage], build=[left])
            :              :- Exchange(distribution=[broadcast])
            :              :  +- SortLimit(orderBy=[wr_refunded_cdemo_sk ASC], offset=[0], fetch=[1], global=[true])
            :              :     +- Exchange(distribution=[single])
            :              :        +- SortLimit(orderBy=[wr_refunded_cdemo_sk ASC], offset=[0], fetch=[1], global=[false])
            :              :           +- HashJoin(joinType=[InnerJoin], where=[=(wr_returning_addr_sk0, wr_reason_sk)], select=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss, wr_returned_date_sk0, wr_returned_time_sk0, wr_item_sk0, wr_refunded_customer_sk0, wr_refunded_cdemo_sk0, wr_refunded_hdemo_sk0, wr_refunded_addr_sk0, wr_returning_customer_sk0, wr_returning_cdemo_sk0, wr_returning_hdemo_sk0, wr_returning_addr_sk0, wr_web_page_sk0, wr_reason_sk0, wr_order_number0, wr_return_quantity0, wr_return_amt0, wr_return_tax0, wr_return_amt_inc_tax0, wr_fee0, wr_return_ship_cost0, wr_refunded_cash0, wr_reversed_charge0, wr_account_credit0, wr_net_loss0], build=[left])
            :              :              :- Exchange(distribution=[hash[wr_reason_sk]])
            :              :              :  +- Calc(select=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], where=[<=(wr_returning_customer_sk, 22)])
            :              :              :     +- TableSourceScan(table=[[default_catalog, default_database, web_returns, filter=[<=(wr_returning_customer_sk, 22)]]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])
            :              :              +- Exchange(distribution=[hash[wr_returning_addr_sk]])
            :              :                 +- TableSourceScan(table=[[default_catalog, default_database, web_returns]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])
            :              +- NestedLoopJoin(joinType=[InnerJoin], where=[=(s_gmt_offset, cc_gmt_offset_node_18)], select=[cc_call_center_sk_node_18, cc_call_center_id_node_18, cc_rec_start_date_node_18, cc_rec_end_date_node_18, cc_closed_date_sk_node_18, cc_open_date_sk_node_18, cc_name_node_18, cc_class_node_18, cc_employees_node_18, cc_sq_ft_node_18, cc_hours_node_18, cc_manager_node_18, cc_mkt_id_node_18, cc_mkt_class_node_18, cc_mkt_desc_node_18, cc_market_manager_node_18, cc_division_node_18, cc_division_name_node_18, cc_company_node_18, cc_company_name_node_18, cc_street_number_node_18, cc_street_name_node_18, cc_street_type_node_18, cc_suite_number_node_18, cc_city_node_18, cc_county_node_18, cc_state_node_18, cc_zip_node_18, cc_country_node_18, cc_gmt_offset_node_18, cc_tax_percentage_node_18, r_reason_sk_node_19, r_reason_id_node_19, r_reason_desc_node_19, _c34, s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage], build=[right])
            :                 :- Calc(select=[cc_call_center_sk AS cc_call_center_sk_node_18, cc_call_center_id AS cc_call_center_id_node_18, cc_rec_start_date AS cc_rec_start_date_node_18, cc_rec_end_date AS cc_rec_end_date_node_18, cc_closed_date_sk AS cc_closed_date_sk_node_18, cc_open_date_sk AS cc_open_date_sk_node_18, cc_name AS cc_name_node_18, cc_class AS cc_class_node_18, cc_employees AS cc_employees_node_18, cc_sq_ft AS cc_sq_ft_node_18, cc_hours AS cc_hours_node_18, cc_manager AS cc_manager_node_18, cc_mkt_id AS cc_mkt_id_node_18, cc_mkt_class AS cc_mkt_class_node_18, cc_mkt_desc AS cc_mkt_desc_node_18, cc_market_manager AS cc_market_manager_node_18, cc_division AS cc_division_node_18, cc_division_name AS cc_division_name_node_18, cc_company AS cc_company_node_18, cc_company_name AS cc_company_name_node_18, cc_street_number AS cc_street_number_node_18, cc_street_name AS cc_street_name_node_18, cc_street_type AS cc_street_type_node_18, cc_suite_number AS cc_suite_number_node_18, cc_city AS cc_city_node_18, cc_county AS cc_county_node_18, cc_state AS cc_state_node_18, cc_zip AS cc_zip_node_18, cc_country AS cc_country_node_18, cc_gmt_offset AS cc_gmt_offset_node_18, cc_tax_percentage AS cc_tax_percentage_node_18, r_reason_sk AS r_reason_sk_node_19, r_reason_id AS r_reason_id_node_19, r_reason_desc AS r_reason_desc_node_19, 'hello' AS _c34])
            :                 :  +- NestedLoopJoin(joinType=[InnerJoin], where=[=(r_reason_sk, cc_call_center_sk)], select=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage, r_reason_sk, r_reason_id, r_reason_desc], build=[right])
            :                 :     :- TableSourceScan(table=[[default_catalog, default_database, call_center]], fields=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage])
            :                 :     +- Exchange(distribution=[broadcast])
            :                 :        +- TableSourceScan(table=[[default_catalog, default_database, reason]], fields=[r_reason_sk, r_reason_id, r_reason_desc])
            :                 +- Exchange(distribution=[broadcast])
            :                    +- SortLimit(orderBy=[s_city ASC], offset=[0], fetch=[1], global=[true])
            :                       +- Exchange(distribution=[single])
            :                          +- SortLimit(orderBy=[s_city ASC], offset=[0], fetch=[1], global=[false])
            :                             +- SortLimit(orderBy=[s_division_id ASC], offset=[0], fetch=[1], global=[true])
            :                                +- Exchange(distribution=[single])
            :                                   +- SortLimit(orderBy=[s_division_id ASC], offset=[0], fetch=[1], global=[false])
            :                                      +- TableSourceScan(table=[[default_catalog, default_database, store]], fields=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage])
            +- HashAggregate(isMerge=[false], groupBy=[ss_ticket_number], select=[ss_ticket_number])
               +- Exchange(distribution=[hash[ss_ticket_number]])
                  +- HashJoin(joinType=[InnerJoin], where=[=(ws_wholesale_cost, ss_net_paid)], select=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit, sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], isBroadcast=[true], build=[left])
                     :- Exchange(distribution=[broadcast])
                     :  +- Limit(offset=[0], fetch=[64], global=[true])
                     :     +- Exchange(distribution=[single])
                     :        +- Limit(offset=[0], fetch=[64], global=[false])
                     :           +- TableSourceScan(table=[[default_catalog, default_database, web_sales, limit=[64]]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
                     +- HashJoin(joinType=[InnerJoin], where=[=(sm_ship_mode_sk, ss_cdemo_sk)], select=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], isBroadcast=[true], build=[left])
                        :- Exchange(distribution=[broadcast])
                        :  +- TableSourceScan(table=[[default_catalog, default_database, ship_mode]], fields=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract])
                        +- Calc(select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], where=[<(ss_quantity, -24)])
                           +- TableSourceScan(table=[[default_catalog, default_database, store_sales, filter=[<(ss_quantity, -24)]]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS r_reason_sk_node_19])
+- SortAggregate(isMerge=[false], groupBy=[cc_closed_date_sk_node_18], select=[cc_closed_date_sk_node_18, MIN(EXPR$0) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[cc_closed_date_sk_node_18 ASC])
         +- Exchange(distribution=[hash[cc_closed_date_sk_node_18]])
            +- NestedLoopJoin(joinType=[InnerJoin], where=[(ss_ticket_number = cc_mkt_id_node_18)], select=[cc_closed_date_sk_node_18, cc_mkt_id_node_18, EXPR$0, ss_ticket_number], build=[left])
               :- Exchange(distribution=[broadcast])
               :  +- SortAggregate(isMerge=[false], groupBy=[cc_closed_date_sk_node_18, cc_mkt_id_node_18], select=[cc_closed_date_sk_node_18, cc_mkt_id_node_18, MIN(r_reason_sk_node_19) AS EXPR$0])
               :     +- Exchange(distribution=[forward])
               :        +- Sort(orderBy=[cc_closed_date_sk_node_18 ASC, cc_mkt_id_node_18 ASC])
               :           +- Exchange(distribution=[hash[cc_closed_date_sk_node_18, cc_mkt_id_node_18]])
               :              +- NestedLoopJoin(joinType=[InnerJoin], where=[(wr_returned_time_sk0 = cc_employees_node_18)], select=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss, wr_returned_date_sk0, wr_returned_time_sk0, wr_item_sk0, wr_refunded_customer_sk0, wr_refunded_cdemo_sk0, wr_refunded_hdemo_sk0, wr_refunded_addr_sk0, wr_returning_customer_sk0, wr_returning_cdemo_sk0, wr_returning_hdemo_sk0, wr_returning_addr_sk0, wr_web_page_sk0, wr_reason_sk0, wr_order_number0, wr_return_quantity0, wr_return_amt0, wr_return_tax0, wr_return_amt_inc_tax0, wr_fee0, wr_return_ship_cost0, wr_refunded_cash0, wr_reversed_charge0, wr_account_credit0, wr_net_loss0, cc_call_center_sk_node_18, cc_call_center_id_node_18, cc_rec_start_date_node_18, cc_rec_end_date_node_18, cc_closed_date_sk_node_18, cc_open_date_sk_node_18, cc_name_node_18, cc_class_node_18, cc_employees_node_18, cc_sq_ft_node_18, cc_hours_node_18, cc_manager_node_18, cc_mkt_id_node_18, cc_mkt_class_node_18, cc_mkt_desc_node_18, cc_market_manager_node_18, cc_division_node_18, cc_division_name_node_18, cc_company_node_18, cc_company_name_node_18, cc_street_number_node_18, cc_street_name_node_18, cc_street_type_node_18, cc_suite_number_node_18, cc_city_node_18, cc_county_node_18, cc_state_node_18, cc_zip_node_18, cc_country_node_18, cc_gmt_offset_node_18, cc_tax_percentage_node_18, r_reason_sk_node_19, r_reason_id_node_19, r_reason_desc_node_19, _c34, s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage], build=[left])
               :                 :- Exchange(distribution=[broadcast])
               :                 :  +- SortLimit(orderBy=[wr_refunded_cdemo_sk ASC], offset=[0], fetch=[1], global=[true])
               :                 :     +- Exchange(distribution=[single])
               :                 :        +- SortLimit(orderBy=[wr_refunded_cdemo_sk ASC], offset=[0], fetch=[1], global=[false])
               :                 :           +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(wr_returning_addr_sk0 = wr_reason_sk)], select=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss, wr_returned_date_sk0, wr_returned_time_sk0, wr_item_sk0, wr_refunded_customer_sk0, wr_refunded_cdemo_sk0, wr_refunded_hdemo_sk0, wr_refunded_addr_sk0, wr_returning_customer_sk0, wr_returning_cdemo_sk0, wr_returning_hdemo_sk0, wr_returning_addr_sk0, wr_web_page_sk0, wr_reason_sk0, wr_order_number0, wr_return_quantity0, wr_return_amt0, wr_return_tax0, wr_return_amt_inc_tax0, wr_fee0, wr_return_ship_cost0, wr_refunded_cash0, wr_reversed_charge0, wr_account_credit0, wr_net_loss0], build=[left])
               :                 :              :- Exchange(distribution=[hash[wr_reason_sk]])
               :                 :              :  +- Calc(select=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], where=[(wr_returning_customer_sk <= 22)])
               :                 :              :     +- TableSourceScan(table=[[default_catalog, default_database, web_returns, filter=[<=(wr_returning_customer_sk, 22)]]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])
               :                 :              +- Exchange(distribution=[hash[wr_returning_addr_sk]])
               :                 :                 +- TableSourceScan(table=[[default_catalog, default_database, web_returns]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])
               :                 +- NestedLoopJoin(joinType=[InnerJoin], where=[(s_gmt_offset = cc_gmt_offset_node_18)], select=[cc_call_center_sk_node_18, cc_call_center_id_node_18, cc_rec_start_date_node_18, cc_rec_end_date_node_18, cc_closed_date_sk_node_18, cc_open_date_sk_node_18, cc_name_node_18, cc_class_node_18, cc_employees_node_18, cc_sq_ft_node_18, cc_hours_node_18, cc_manager_node_18, cc_mkt_id_node_18, cc_mkt_class_node_18, cc_mkt_desc_node_18, cc_market_manager_node_18, cc_division_node_18, cc_division_name_node_18, cc_company_node_18, cc_company_name_node_18, cc_street_number_node_18, cc_street_name_node_18, cc_street_type_node_18, cc_suite_number_node_18, cc_city_node_18, cc_county_node_18, cc_state_node_18, cc_zip_node_18, cc_country_node_18, cc_gmt_offset_node_18, cc_tax_percentage_node_18, r_reason_sk_node_19, r_reason_id_node_19, r_reason_desc_node_19, _c34, s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage], build=[right])
               :                    :- Calc(select=[cc_call_center_sk AS cc_call_center_sk_node_18, cc_call_center_id AS cc_call_center_id_node_18, cc_rec_start_date AS cc_rec_start_date_node_18, cc_rec_end_date AS cc_rec_end_date_node_18, cc_closed_date_sk AS cc_closed_date_sk_node_18, cc_open_date_sk AS cc_open_date_sk_node_18, cc_name AS cc_name_node_18, cc_class AS cc_class_node_18, cc_employees AS cc_employees_node_18, cc_sq_ft AS cc_sq_ft_node_18, cc_hours AS cc_hours_node_18, cc_manager AS cc_manager_node_18, cc_mkt_id AS cc_mkt_id_node_18, cc_mkt_class AS cc_mkt_class_node_18, cc_mkt_desc AS cc_mkt_desc_node_18, cc_market_manager AS cc_market_manager_node_18, cc_division AS cc_division_node_18, cc_division_name AS cc_division_name_node_18, cc_company AS cc_company_node_18, cc_company_name AS cc_company_name_node_18, cc_street_number AS cc_street_number_node_18, cc_street_name AS cc_street_name_node_18, cc_street_type AS cc_street_type_node_18, cc_suite_number AS cc_suite_number_node_18, cc_city AS cc_city_node_18, cc_county AS cc_county_node_18, cc_state AS cc_state_node_18, cc_zip AS cc_zip_node_18, cc_country AS cc_country_node_18, cc_gmt_offset AS cc_gmt_offset_node_18, cc_tax_percentage AS cc_tax_percentage_node_18, r_reason_sk AS r_reason_sk_node_19, r_reason_id AS r_reason_id_node_19, r_reason_desc AS r_reason_desc_node_19, 'hello' AS _c34])
               :                    :  +- NestedLoopJoin(joinType=[InnerJoin], where=[(r_reason_sk = cc_call_center_sk)], select=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage, r_reason_sk, r_reason_id, r_reason_desc], build=[right])
               :                    :     :- TableSourceScan(table=[[default_catalog, default_database, call_center]], fields=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage])
               :                    :     +- Exchange(distribution=[broadcast])
               :                    :        +- TableSourceScan(table=[[default_catalog, default_database, reason]], fields=[r_reason_sk, r_reason_id, r_reason_desc])
               :                    +- Exchange(distribution=[broadcast])
               :                       +- SortLimit(orderBy=[s_city ASC], offset=[0], fetch=[1], global=[true])
               :                          +- Exchange(distribution=[single])
               :                             +- SortLimit(orderBy=[s_city ASC], offset=[0], fetch=[1], global=[false])
               :                                +- SortLimit(orderBy=[s_division_id ASC], offset=[0], fetch=[1], global=[true])
               :                                   +- Exchange(distribution=[single])
               :                                      +- SortLimit(orderBy=[s_division_id ASC], offset=[0], fetch=[1], global=[false])
               :                                         +- TableSourceScan(table=[[default_catalog, default_database, store]], fields=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage])
               +- HashAggregate(isMerge=[false], groupBy=[ss_ticket_number], select=[ss_ticket_number])
                  +- Exchange(distribution=[hash[ss_ticket_number]])
                     +- HashJoin(joinType=[InnerJoin], where=[(ws_wholesale_cost = ss_net_paid)], select=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit, sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], isBroadcast=[true], build=[left])
                        :- Exchange(distribution=[broadcast])
                        :  +- Limit(offset=[0], fetch=[64], global=[true])
                        :     +- Exchange(distribution=[single])
                        :        +- Limit(offset=[0], fetch=[64], global=[false])
                        :           +- TableSourceScan(table=[[default_catalog, default_database, web_sales, limit=[64]]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
                        +- HashJoin(joinType=[InnerJoin], where=[(sm_ship_mode_sk = ss_cdemo_sk)], select=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], isBroadcast=[true], build=[left])
                           :- Exchange(distribution=[broadcast])
                           :  +- TableSourceScan(table=[[default_catalog, default_database, ship_mode]], fields=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract])
                           +- Calc(select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], where=[(ss_quantity < -24)])
                              +- TableSourceScan(table=[[default_catalog, default_database, store_sales, filter=[<(ss_quantity, -24)]]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0