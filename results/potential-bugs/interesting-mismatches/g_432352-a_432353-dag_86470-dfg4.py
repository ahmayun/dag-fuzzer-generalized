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
    return values.iloc[0] if len(values) > 0 else None


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_13 = table_env.from_path("date_dim").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("date_dim").get_schema().get_field_names()])
autonode_12 = table_env.from_path("catalog_returns").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("catalog_returns").get_schema().get_field_names()])
autonode_14 = table_env.from_path("promotion").select(*[col(column_name).alias(f"{column_name}_node_14") for column_name in table_env.from_path("promotion").get_schema().get_field_names()])
autonode_10 = autonode_13.order_by(col('d_same_day_ly_node_13'))
autonode_9 = autonode_12.limit(55)
autonode_11 = autonode_14.order_by(col('p_response_target_node_14'))
autonode_7 = autonode_9.distinct()
autonode_8 = autonode_10.join(autonode_11, col('p_promo_sk_node_14') == col('d_year_node_13'))
autonode_5 = autonode_7.order_by(col('cr_reason_sk_node_12'))
autonode_6 = autonode_8.filter(col('d_month_seq_node_13') < 22)
autonode_3 = autonode_5.order_by(col('cr_refunded_cash_node_12'))
autonode_4 = autonode_6.alias('gNioB')
autonode_2 = autonode_3.join(autonode_4, col('cr_return_tax_node_12') == col('p_cost_node_14'))
autonode_1 = autonode_2.group_by(col('cr_order_number_node_12')).select(col('cr_return_amount_node_12').min.alias('cr_return_amount_node_12'))
sink = autonode_1.order_by(col('cr_return_amount_node_12'))
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
      "error_message": "An error occurred while calling o235432145.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#476110563:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[23](input=RelSubset#476110561,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[23]), rel#476110560:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[23](input=RelSubset#476110559,groupBy=cr_order_number_node_12, cr_return_tax_node_120,select=cr_order_number_node_12, cr_return_tax_node_120, Partial_MIN(cr_return_amount_node_12) AS min$0)]
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
LogicalSort(sort0=[$0], dir0=[ASC])
+- LogicalProject(cr_return_amount_node_12=[$1])
   +- LogicalAggregate(group=[{16}], EXPR$0=[MIN($18)])
      +- LogicalJoin(condition=[=($19, $60)], joinType=[inner])
         :- LogicalSort(sort0=[$23], dir0=[ASC])
         :  +- LogicalSort(sort0=[$15], dir0=[ASC])
         :     +- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26}])
         :        +- LogicalSort(fetch=[55])
         :           +- LogicalProject(cr_returned_date_sk_node_12=[$0], cr_returned_time_sk_node_12=[$1], cr_item_sk_node_12=[$2], cr_refunded_customer_sk_node_12=[$3], cr_refunded_cdemo_sk_node_12=[$4], cr_refunded_hdemo_sk_node_12=[$5], cr_refunded_addr_sk_node_12=[$6], cr_returning_customer_sk_node_12=[$7], cr_returning_cdemo_sk_node_12=[$8], cr_returning_hdemo_sk_node_12=[$9], cr_returning_addr_sk_node_12=[$10], cr_call_center_sk_node_12=[$11], cr_catalog_page_sk_node_12=[$12], cr_ship_mode_sk_node_12=[$13], cr_warehouse_sk_node_12=[$14], cr_reason_sk_node_12=[$15], cr_order_number_node_12=[$16], cr_return_quantity_node_12=[$17], cr_return_amount_node_12=[$18], cr_return_tax_node_12=[$19], cr_return_amt_inc_tax_node_12=[$20], cr_fee_node_12=[$21], cr_return_ship_cost_node_12=[$22], cr_refunded_cash_node_12=[$23], cr_reversed_charge_node_12=[$24], cr_store_credit_node_12=[$25], cr_net_loss_node_12=[$26])
         :              +- LogicalTableScan(table=[[default_catalog, default_database, catalog_returns]])
         +- LogicalProject(gNioB=[AS($0, _UTF-16LE'gNioB')], d_date_id_node_13=[$1], d_date_node_13=[$2], d_month_seq_node_13=[$3], d_week_seq_node_13=[$4], d_quarter_seq_node_13=[$5], d_year_node_13=[$6], d_dow_node_13=[$7], d_moy_node_13=[$8], d_dom_node_13=[$9], d_qoy_node_13=[$10], d_fy_year_node_13=[$11], d_fy_quarter_seq_node_13=[$12], d_fy_week_seq_node_13=[$13], d_day_name_node_13=[$14], d_quarter_name_node_13=[$15], d_holiday_node_13=[$16], d_weekend_node_13=[$17], d_following_holiday_node_13=[$18], d_first_dom_node_13=[$19], d_last_dom_node_13=[$20], d_same_day_ly_node_13=[$21], d_same_day_lq_node_13=[$22], d_current_day_node_13=[$23], d_current_week_node_13=[$24], d_current_month_node_13=[$25], d_current_quarter_node_13=[$26], d_current_year_node_13=[$27], p_promo_sk_node_14=[$28], p_promo_id_node_14=[$29], p_start_date_sk_node_14=[$30], p_end_date_sk_node_14=[$31], p_item_sk_node_14=[$32], p_cost_node_14=[$33], p_response_target_node_14=[$34], p_promo_name_node_14=[$35], p_channel_dmail_node_14=[$36], p_channel_email_node_14=[$37], p_channel_catalog_node_14=[$38], p_channel_tv_node_14=[$39], p_channel_radio_node_14=[$40], p_channel_press_node_14=[$41], p_channel_event_node_14=[$42], p_channel_demo_node_14=[$43], p_channel_details_node_14=[$44], p_purpose_node_14=[$45], p_discount_active_node_14=[$46])
            +- LogicalFilter(condition=[<($3, 22)])
               +- LogicalJoin(condition=[=($28, $6)], joinType=[inner])
                  :- LogicalSort(sort0=[$21], dir0=[ASC])
                  :  +- LogicalProject(d_date_sk_node_13=[$0], d_date_id_node_13=[$1], d_date_node_13=[$2], d_month_seq_node_13=[$3], d_week_seq_node_13=[$4], d_quarter_seq_node_13=[$5], d_year_node_13=[$6], d_dow_node_13=[$7], d_moy_node_13=[$8], d_dom_node_13=[$9], d_qoy_node_13=[$10], d_fy_year_node_13=[$11], d_fy_quarter_seq_node_13=[$12], d_fy_week_seq_node_13=[$13], d_day_name_node_13=[$14], d_quarter_name_node_13=[$15], d_holiday_node_13=[$16], d_weekend_node_13=[$17], d_following_holiday_node_13=[$18], d_first_dom_node_13=[$19], d_last_dom_node_13=[$20], d_same_day_ly_node_13=[$21], d_same_day_lq_node_13=[$22], d_current_day_node_13=[$23], d_current_week_node_13=[$24], d_current_month_node_13=[$25], d_current_quarter_node_13=[$26], d_current_year_node_13=[$27])
                  :     +- LogicalTableScan(table=[[default_catalog, default_database, date_dim]])
                  +- LogicalSort(sort0=[$6], dir0=[ASC])
                     +- LogicalProject(p_promo_sk_node_14=[$0], p_promo_id_node_14=[$1], p_start_date_sk_node_14=[$2], p_end_date_sk_node_14=[$3], p_item_sk_node_14=[$4], p_cost_node_14=[$5], p_response_target_node_14=[$6], p_promo_name_node_14=[$7], p_channel_dmail_node_14=[$8], p_channel_email_node_14=[$9], p_channel_catalog_node_14=[$10], p_channel_tv_node_14=[$11], p_channel_radio_node_14=[$12], p_channel_press_node_14=[$13], p_channel_event_node_14=[$14], p_channel_demo_node_14=[$15], p_channel_details_node_14=[$16], p_purpose_node_14=[$17], p_discount_active_node_14=[$18])
                        +- LogicalTableScan(table=[[default_catalog, default_database, promotion]])

== Optimized Physical Plan ==
SortLimit(orderBy=[cr_return_amount_node_12 ASC], offset=[0], fetch=[1], global=[true])
+- Exchange(distribution=[single])
   +- SortLimit(orderBy=[cr_return_amount_node_12 ASC], offset=[0], fetch=[1], global=[false])
      +- Calc(select=[EXPR$0 AS cr_return_amount_node_12])
         +- SortAggregate(isMerge=[false], groupBy=[cr_order_number_node_12], select=[cr_order_number_node_12, MIN(cr_return_amount_node_12) AS EXPR$0])
            +- Sort(orderBy=[cr_order_number_node_12 ASC])
               +- Exchange(distribution=[hash[cr_order_number_node_12]])
                  +- Calc(select=[cr_returned_date_sk_node_12, cr_returned_time_sk_node_12, cr_item_sk_node_12, cr_refunded_customer_sk_node_12, cr_refunded_cdemo_sk_node_12, cr_refunded_hdemo_sk_node_12, cr_refunded_addr_sk_node_12, cr_returning_customer_sk_node_12, cr_returning_cdemo_sk_node_12, cr_returning_hdemo_sk_node_12, cr_returning_addr_sk_node_12, cr_call_center_sk_node_12, cr_catalog_page_sk_node_12, cr_ship_mode_sk_node_12, cr_warehouse_sk_node_12, cr_reason_sk_node_12, cr_order_number_node_12, cr_return_quantity_node_12, cr_return_amount_node_12, cr_return_tax_node_12, cr_return_amt_inc_tax_node_12, cr_fee_node_12, cr_return_ship_cost_node_12, cr_refunded_cash_node_12, cr_reversed_charge_node_12, cr_store_credit_node_12, cr_net_loss_node_12, gNioB, d_date_id_node_13, d_date_node_13, d_month_seq_node_13, d_week_seq_node_13, d_quarter_seq_node_13, d_year_node_13, d_dow_node_13, d_moy_node_13, d_dom_node_13, d_qoy_node_13, d_fy_year_node_13, d_fy_quarter_seq_node_13, d_fy_week_seq_node_13, d_day_name_node_13, d_quarter_name_node_13, d_holiday_node_13, d_weekend_node_13, d_following_holiday_node_13, d_first_dom_node_13, d_last_dom_node_13, d_same_day_ly_node_13, d_same_day_lq_node_13, d_current_day_node_13, d_current_week_node_13, d_current_month_node_13, d_current_quarter_node_13, d_current_year_node_13, p_promo_sk_node_14, p_promo_id_node_14, p_start_date_sk_node_14, p_end_date_sk_node_14, p_item_sk_node_14, p_cost_node_14, p_response_target_node_14, p_promo_name_node_14, p_channel_dmail_node_14, p_channel_email_node_14, p_channel_catalog_node_14, p_channel_tv_node_14, p_channel_radio_node_14, p_channel_press_node_14, p_channel_event_node_14, p_channel_demo_node_14, p_channel_details_node_14, p_purpose_node_14, p_discount_active_node_14])
                     +- NestedLoopJoin(joinType=[InnerJoin], where=[=(cr_return_tax_node_120, p_cost_node_14)], select=[cr_returned_date_sk_node_12, cr_returned_time_sk_node_12, cr_item_sk_node_12, cr_refunded_customer_sk_node_12, cr_refunded_cdemo_sk_node_12, cr_refunded_hdemo_sk_node_12, cr_refunded_addr_sk_node_12, cr_returning_customer_sk_node_12, cr_returning_cdemo_sk_node_12, cr_returning_hdemo_sk_node_12, cr_returning_addr_sk_node_12, cr_call_center_sk_node_12, cr_catalog_page_sk_node_12, cr_ship_mode_sk_node_12, cr_warehouse_sk_node_12, cr_reason_sk_node_12, cr_order_number_node_12, cr_return_quantity_node_12, cr_return_amount_node_12, cr_return_tax_node_12, cr_return_amt_inc_tax_node_12, cr_fee_node_12, cr_return_ship_cost_node_12, cr_refunded_cash_node_12, cr_reversed_charge_node_12, cr_store_credit_node_12, cr_net_loss_node_12, cr_return_tax_node_120, gNioB, d_date_id_node_13, d_date_node_13, d_month_seq_node_13, d_week_seq_node_13, d_quarter_seq_node_13, d_year_node_13, d_dow_node_13, d_moy_node_13, d_dom_node_13, d_qoy_node_13, d_fy_year_node_13, d_fy_quarter_seq_node_13, d_fy_week_seq_node_13, d_day_name_node_13, d_quarter_name_node_13, d_holiday_node_13, d_weekend_node_13, d_following_holiday_node_13, d_first_dom_node_13, d_last_dom_node_13, d_same_day_ly_node_13, d_same_day_lq_node_13, d_current_day_node_13, d_current_week_node_13, d_current_month_node_13, d_current_quarter_node_13, d_current_year_node_13, p_promo_sk_node_14, p_promo_id_node_14, p_start_date_sk_node_14, p_end_date_sk_node_14, p_item_sk_node_14, p_cost_node_14, p_response_target_node_14, p_promo_name_node_14, p_channel_dmail_node_14, p_channel_email_node_14, p_channel_catalog_node_14, p_channel_tv_node_14, p_channel_radio_node_14, p_channel_press_node_14, p_channel_event_node_14, p_channel_demo_node_14, p_channel_details_node_14, p_purpose_node_14, p_discount_active_node_14], build=[left])
                        :- Exchange(distribution=[broadcast])
                        :  +- Calc(select=[cr_returned_date_sk AS cr_returned_date_sk_node_12, cr_returned_time_sk AS cr_returned_time_sk_node_12, cr_item_sk AS cr_item_sk_node_12, cr_refunded_customer_sk AS cr_refunded_customer_sk_node_12, cr_refunded_cdemo_sk AS cr_refunded_cdemo_sk_node_12, cr_refunded_hdemo_sk AS cr_refunded_hdemo_sk_node_12, cr_refunded_addr_sk AS cr_refunded_addr_sk_node_12, cr_returning_customer_sk AS cr_returning_customer_sk_node_12, cr_returning_cdemo_sk AS cr_returning_cdemo_sk_node_12, cr_returning_hdemo_sk AS cr_returning_hdemo_sk_node_12, cr_returning_addr_sk AS cr_returning_addr_sk_node_12, cr_call_center_sk AS cr_call_center_sk_node_12, cr_catalog_page_sk AS cr_catalog_page_sk_node_12, cr_ship_mode_sk AS cr_ship_mode_sk_node_12, cr_warehouse_sk AS cr_warehouse_sk_node_12, cr_reason_sk AS cr_reason_sk_node_12, cr_order_number AS cr_order_number_node_12, cr_return_quantity AS cr_return_quantity_node_12, cr_return_amount AS cr_return_amount_node_12, cr_return_tax AS cr_return_tax_node_12, cr_return_amt_inc_tax AS cr_return_amt_inc_tax_node_12, cr_fee AS cr_fee_node_12, cr_return_ship_cost AS cr_return_ship_cost_node_12, cr_refunded_cash AS cr_refunded_cash_node_12, cr_reversed_charge AS cr_reversed_charge_node_12, cr_store_credit AS cr_store_credit_node_12, cr_net_loss AS cr_net_loss_node_12, CAST(cr_return_tax AS DECIMAL(15, 2)) AS cr_return_tax_node_120])
                        :     +- SortLimit(orderBy=[cr_refunded_cash ASC], offset=[0], fetch=[1], global=[true])
                        :        +- Exchange(distribution=[single])
                        :           +- SortLimit(orderBy=[cr_refunded_cash ASC], offset=[0], fetch=[1], global=[false])
                        :              +- SortLimit(orderBy=[cr_reason_sk ASC], offset=[0], fetch=[1], global=[true])
                        :                 +- Exchange(distribution=[single])
                        :                    +- SortLimit(orderBy=[cr_reason_sk ASC], offset=[0], fetch=[1], global=[false])
                        :                       +- HashAggregate(isMerge=[false], groupBy=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss], select=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])
                        :                          +- Exchange(distribution=[hash[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss]])
                        :                             +- Limit(offset=[0], fetch=[55], global=[true])
                        :                                +- Exchange(distribution=[single])
                        :                                   +- Limit(offset=[0], fetch=[55], global=[false])
                        :                                      +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns, limit=[55]]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])
                        +- Calc(select=[d_date_sk AS gNioB, d_date_id AS d_date_id_node_13, d_date AS d_date_node_13, d_month_seq AS d_month_seq_node_13, d_week_seq AS d_week_seq_node_13, d_quarter_seq AS d_quarter_seq_node_13, d_year AS d_year_node_13, d_dow AS d_dow_node_13, d_moy AS d_moy_node_13, d_dom AS d_dom_node_13, d_qoy AS d_qoy_node_13, d_fy_year AS d_fy_year_node_13, d_fy_quarter_seq AS d_fy_quarter_seq_node_13, d_fy_week_seq AS d_fy_week_seq_node_13, d_day_name AS d_day_name_node_13, d_quarter_name AS d_quarter_name_node_13, d_holiday AS d_holiday_node_13, d_weekend AS d_weekend_node_13, d_following_holiday AS d_following_holiday_node_13, d_first_dom AS d_first_dom_node_13, d_last_dom AS d_last_dom_node_13, d_same_day_ly AS d_same_day_ly_node_13, d_same_day_lq AS d_same_day_lq_node_13, d_current_day AS d_current_day_node_13, d_current_week AS d_current_week_node_13, d_current_month AS d_current_month_node_13, d_current_quarter AS d_current_quarter_node_13, d_current_year AS d_current_year_node_13, p_promo_sk AS p_promo_sk_node_14, p_promo_id AS p_promo_id_node_14, p_start_date_sk AS p_start_date_sk_node_14, p_end_date_sk AS p_end_date_sk_node_14, p_item_sk AS p_item_sk_node_14, p_cost AS p_cost_node_14, p_response_target AS p_response_target_node_14, p_promo_name AS p_promo_name_node_14, p_channel_dmail AS p_channel_dmail_node_14, p_channel_email AS p_channel_email_node_14, p_channel_catalog AS p_channel_catalog_node_14, p_channel_tv AS p_channel_tv_node_14, p_channel_radio AS p_channel_radio_node_14, p_channel_press AS p_channel_press_node_14, p_channel_event AS p_channel_event_node_14, p_channel_demo AS p_channel_demo_node_14, p_channel_details AS p_channel_details_node_14, p_purpose AS p_purpose_node_14, p_discount_active AS p_discount_active_node_14])
                           +- NestedLoopJoin(joinType=[InnerJoin], where=[=(p_promo_sk, d_year)], select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active], build=[right])
                              :- Calc(select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year], where=[<(d_month_seq, 22)])
                              :  +- SortLimit(orderBy=[d_same_day_ly ASC], offset=[0], fetch=[1], global=[true])
                              :     +- Exchange(distribution=[single])
                              :        +- SortLimit(orderBy=[d_same_day_ly ASC], offset=[0], fetch=[1], global=[false])
                              :           +- TableSourceScan(table=[[default_catalog, default_database, date_dim]], fields=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year])
                              +- Exchange(distribution=[broadcast])
                                 +- SortLimit(orderBy=[p_response_target ASC], offset=[0], fetch=[1], global=[true])
                                    +- Exchange(distribution=[single])
                                       +- SortLimit(orderBy=[p_response_target ASC], offset=[0], fetch=[1], global=[false])
                                          +- TableSourceScan(table=[[default_catalog, default_database, promotion]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])

== Optimized Execution Plan ==
SortLimit(orderBy=[cr_return_amount_node_12 ASC], offset=[0], fetch=[1], global=[true])
+- Exchange(distribution=[single])
   +- SortLimit(orderBy=[cr_return_amount_node_12 ASC], offset=[0], fetch=[1], global=[false])
      +- Calc(select=[EXPR$0 AS cr_return_amount_node_12])
         +- SortAggregate(isMerge=[false], groupBy=[cr_order_number_node_12], select=[cr_order_number_node_12, MIN(cr_return_amount_node_12) AS EXPR$0])
            +- Exchange(distribution=[forward])
               +- Sort(orderBy=[cr_order_number_node_12 ASC])
                  +- Exchange(distribution=[hash[cr_order_number_node_12]])
                     +- Calc(select=[cr_returned_date_sk_node_12, cr_returned_time_sk_node_12, cr_item_sk_node_12, cr_refunded_customer_sk_node_12, cr_refunded_cdemo_sk_node_12, cr_refunded_hdemo_sk_node_12, cr_refunded_addr_sk_node_12, cr_returning_customer_sk_node_12, cr_returning_cdemo_sk_node_12, cr_returning_hdemo_sk_node_12, cr_returning_addr_sk_node_12, cr_call_center_sk_node_12, cr_catalog_page_sk_node_12, cr_ship_mode_sk_node_12, cr_warehouse_sk_node_12, cr_reason_sk_node_12, cr_order_number_node_12, cr_return_quantity_node_12, cr_return_amount_node_12, cr_return_tax_node_12, cr_return_amt_inc_tax_node_12, cr_fee_node_12, cr_return_ship_cost_node_12, cr_refunded_cash_node_12, cr_reversed_charge_node_12, cr_store_credit_node_12, cr_net_loss_node_12, gNioB, d_date_id_node_13, d_date_node_13, d_month_seq_node_13, d_week_seq_node_13, d_quarter_seq_node_13, d_year_node_13, d_dow_node_13, d_moy_node_13, d_dom_node_13, d_qoy_node_13, d_fy_year_node_13, d_fy_quarter_seq_node_13, d_fy_week_seq_node_13, d_day_name_node_13, d_quarter_name_node_13, d_holiday_node_13, d_weekend_node_13, d_following_holiday_node_13, d_first_dom_node_13, d_last_dom_node_13, d_same_day_ly_node_13, d_same_day_lq_node_13, d_current_day_node_13, d_current_week_node_13, d_current_month_node_13, d_current_quarter_node_13, d_current_year_node_13, p_promo_sk_node_14, p_promo_id_node_14, p_start_date_sk_node_14, p_end_date_sk_node_14, p_item_sk_node_14, p_cost_node_14, p_response_target_node_14, p_promo_name_node_14, p_channel_dmail_node_14, p_channel_email_node_14, p_channel_catalog_node_14, p_channel_tv_node_14, p_channel_radio_node_14, p_channel_press_node_14, p_channel_event_node_14, p_channel_demo_node_14, p_channel_details_node_14, p_purpose_node_14, p_discount_active_node_14])
                        +- NestedLoopJoin(joinType=[InnerJoin], where=[(cr_return_tax_node_120 = p_cost_node_14)], select=[cr_returned_date_sk_node_12, cr_returned_time_sk_node_12, cr_item_sk_node_12, cr_refunded_customer_sk_node_12, cr_refunded_cdemo_sk_node_12, cr_refunded_hdemo_sk_node_12, cr_refunded_addr_sk_node_12, cr_returning_customer_sk_node_12, cr_returning_cdemo_sk_node_12, cr_returning_hdemo_sk_node_12, cr_returning_addr_sk_node_12, cr_call_center_sk_node_12, cr_catalog_page_sk_node_12, cr_ship_mode_sk_node_12, cr_warehouse_sk_node_12, cr_reason_sk_node_12, cr_order_number_node_12, cr_return_quantity_node_12, cr_return_amount_node_12, cr_return_tax_node_12, cr_return_amt_inc_tax_node_12, cr_fee_node_12, cr_return_ship_cost_node_12, cr_refunded_cash_node_12, cr_reversed_charge_node_12, cr_store_credit_node_12, cr_net_loss_node_12, cr_return_tax_node_120, gNioB, d_date_id_node_13, d_date_node_13, d_month_seq_node_13, d_week_seq_node_13, d_quarter_seq_node_13, d_year_node_13, d_dow_node_13, d_moy_node_13, d_dom_node_13, d_qoy_node_13, d_fy_year_node_13, d_fy_quarter_seq_node_13, d_fy_week_seq_node_13, d_day_name_node_13, d_quarter_name_node_13, d_holiday_node_13, d_weekend_node_13, d_following_holiday_node_13, d_first_dom_node_13, d_last_dom_node_13, d_same_day_ly_node_13, d_same_day_lq_node_13, d_current_day_node_13, d_current_week_node_13, d_current_month_node_13, d_current_quarter_node_13, d_current_year_node_13, p_promo_sk_node_14, p_promo_id_node_14, p_start_date_sk_node_14, p_end_date_sk_node_14, p_item_sk_node_14, p_cost_node_14, p_response_target_node_14, p_promo_name_node_14, p_channel_dmail_node_14, p_channel_email_node_14, p_channel_catalog_node_14, p_channel_tv_node_14, p_channel_radio_node_14, p_channel_press_node_14, p_channel_event_node_14, p_channel_demo_node_14, p_channel_details_node_14, p_purpose_node_14, p_discount_active_node_14], build=[left])
                           :- Exchange(distribution=[broadcast])
                           :  +- Calc(select=[cr_returned_date_sk AS cr_returned_date_sk_node_12, cr_returned_time_sk AS cr_returned_time_sk_node_12, cr_item_sk AS cr_item_sk_node_12, cr_refunded_customer_sk AS cr_refunded_customer_sk_node_12, cr_refunded_cdemo_sk AS cr_refunded_cdemo_sk_node_12, cr_refunded_hdemo_sk AS cr_refunded_hdemo_sk_node_12, cr_refunded_addr_sk AS cr_refunded_addr_sk_node_12, cr_returning_customer_sk AS cr_returning_customer_sk_node_12, cr_returning_cdemo_sk AS cr_returning_cdemo_sk_node_12, cr_returning_hdemo_sk AS cr_returning_hdemo_sk_node_12, cr_returning_addr_sk AS cr_returning_addr_sk_node_12, cr_call_center_sk AS cr_call_center_sk_node_12, cr_catalog_page_sk AS cr_catalog_page_sk_node_12, cr_ship_mode_sk AS cr_ship_mode_sk_node_12, cr_warehouse_sk AS cr_warehouse_sk_node_12, cr_reason_sk AS cr_reason_sk_node_12, cr_order_number AS cr_order_number_node_12, cr_return_quantity AS cr_return_quantity_node_12, cr_return_amount AS cr_return_amount_node_12, cr_return_tax AS cr_return_tax_node_12, cr_return_amt_inc_tax AS cr_return_amt_inc_tax_node_12, cr_fee AS cr_fee_node_12, cr_return_ship_cost AS cr_return_ship_cost_node_12, cr_refunded_cash AS cr_refunded_cash_node_12, cr_reversed_charge AS cr_reversed_charge_node_12, cr_store_credit AS cr_store_credit_node_12, cr_net_loss AS cr_net_loss_node_12, CAST(cr_return_tax AS DECIMAL(15, 2)) AS cr_return_tax_node_120])
                           :     +- SortLimit(orderBy=[cr_refunded_cash ASC], offset=[0], fetch=[1], global=[true])
                           :        +- Exchange(distribution=[single])
                           :           +- SortLimit(orderBy=[cr_refunded_cash ASC], offset=[0], fetch=[1], global=[false])
                           :              +- SortLimit(orderBy=[cr_reason_sk ASC], offset=[0], fetch=[1], global=[true])
                           :                 +- Exchange(distribution=[single])
                           :                    +- SortLimit(orderBy=[cr_reason_sk ASC], offset=[0], fetch=[1], global=[false])
                           :                       +- HashAggregate(isMerge=[false], groupBy=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss], select=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])
                           :                          +- Exchange(distribution=[hash[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss]])
                           :                             +- Limit(offset=[0], fetch=[55], global=[true])
                           :                                +- Exchange(distribution=[single])
                           :                                   +- Limit(offset=[0], fetch=[55], global=[false])
                           :                                      +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns, limit=[55]]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])
                           +- Calc(select=[d_date_sk AS gNioB, d_date_id AS d_date_id_node_13, d_date AS d_date_node_13, d_month_seq AS d_month_seq_node_13, d_week_seq AS d_week_seq_node_13, d_quarter_seq AS d_quarter_seq_node_13, d_year AS d_year_node_13, d_dow AS d_dow_node_13, d_moy AS d_moy_node_13, d_dom AS d_dom_node_13, d_qoy AS d_qoy_node_13, d_fy_year AS d_fy_year_node_13, d_fy_quarter_seq AS d_fy_quarter_seq_node_13, d_fy_week_seq AS d_fy_week_seq_node_13, d_day_name AS d_day_name_node_13, d_quarter_name AS d_quarter_name_node_13, d_holiday AS d_holiday_node_13, d_weekend AS d_weekend_node_13, d_following_holiday AS d_following_holiday_node_13, d_first_dom AS d_first_dom_node_13, d_last_dom AS d_last_dom_node_13, d_same_day_ly AS d_same_day_ly_node_13, d_same_day_lq AS d_same_day_lq_node_13, d_current_day AS d_current_day_node_13, d_current_week AS d_current_week_node_13, d_current_month AS d_current_month_node_13, d_current_quarter AS d_current_quarter_node_13, d_current_year AS d_current_year_node_13, p_promo_sk AS p_promo_sk_node_14, p_promo_id AS p_promo_id_node_14, p_start_date_sk AS p_start_date_sk_node_14, p_end_date_sk AS p_end_date_sk_node_14, p_item_sk AS p_item_sk_node_14, p_cost AS p_cost_node_14, p_response_target AS p_response_target_node_14, p_promo_name AS p_promo_name_node_14, p_channel_dmail AS p_channel_dmail_node_14, p_channel_email AS p_channel_email_node_14, p_channel_catalog AS p_channel_catalog_node_14, p_channel_tv AS p_channel_tv_node_14, p_channel_radio AS p_channel_radio_node_14, p_channel_press AS p_channel_press_node_14, p_channel_event AS p_channel_event_node_14, p_channel_demo AS p_channel_demo_node_14, p_channel_details AS p_channel_details_node_14, p_purpose AS p_purpose_node_14, p_discount_active AS p_discount_active_node_14])
                              +- NestedLoopJoin(joinType=[InnerJoin], where=[(p_promo_sk = d_year)], select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active], build=[right])
                                 :- Calc(select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year], where=[(d_month_seq < 22)])
                                 :  +- SortLimit(orderBy=[d_same_day_ly ASC], offset=[0], fetch=[1], global=[true])
                                 :     +- Exchange(distribution=[single])
                                 :        +- SortLimit(orderBy=[d_same_day_ly ASC], offset=[0], fetch=[1], global=[false])
                                 :           +- TableSourceScan(table=[[default_catalog, default_database, date_dim]], fields=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year])
                                 +- Exchange(distribution=[broadcast])
                                    +- SortLimit(orderBy=[p_response_target ASC], offset=[0], fetch=[1], global=[true])
                                       +- Exchange(distribution=[single])
                                          +- SortLimit(orderBy=[p_response_target ASC], offset=[0], fetch=[1], global=[false])
                                             +- TableSourceScan(table=[[default_catalog, default_database, promotion]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0