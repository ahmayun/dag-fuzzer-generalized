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
    return values.max()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_17 = table_env.from_path("inventory").select(*[col(column_name).alias(f"{column_name}_node_17") for column_name in table_env.from_path("inventory").get_schema().get_field_names()])
autonode_18 = table_env.from_path("inventory").select(*[col(column_name).alias(f"{column_name}_node_18") for column_name in table_env.from_path("inventory").get_schema().get_field_names()])
autonode_19 = table_env.from_path("web_page").select(*[col(column_name).alias(f"{column_name}_node_19") for column_name in table_env.from_path("web_page").get_schema().get_field_names()])
autonode_15 = table_env.from_path("catalog_returns").select(*[col(column_name).alias(f"{column_name}_node_15") for column_name in table_env.from_path("catalog_returns").get_schema().get_field_names()])
autonode_16 = table_env.from_path("date_dim").select(*[col(column_name).alias(f"{column_name}_node_16") for column_name in table_env.from_path("date_dim").get_schema().get_field_names()])
autonode_12 = autonode_17.order_by(col('inv_date_sk_node_17'))
autonode_13 = autonode_18.group_by(col('inv_date_sk_node_18')).select(col('inv_warehouse_sk_node_18').avg.alias('inv_warehouse_sk_node_18'))
autonode_14 = autonode_19.distinct()
autonode_11 = autonode_15.join(autonode_16, col('d_month_seq_node_16') == col('cr_item_sk_node_15'))
autonode_9 = autonode_12.distinct()
autonode_10 = autonode_13.join(autonode_14, col('wp_creation_date_sk_node_19') == col('inv_warehouse_sk_node_18'))
autonode_8 = autonode_11.add_columns(lit("hello"))
autonode_7 = autonode_10.order_by(col('wp_link_count_node_19'))
autonode_6 = autonode_8.join(autonode_9, col('inv_item_sk_node_17') == col('d_fy_year_node_16'))
autonode_5 = autonode_7.filter(col('wp_link_count_node_19') > -21)
autonode_4 = autonode_6.alias('NLsJk')
autonode_3 = autonode_5.limit(25)
autonode_2 = autonode_4.order_by(col('d_current_week_node_16'))
autonode_1 = autonode_2.join(autonode_3, col('d_following_holiday_node_16') == col('wp_web_page_id_node_19'))
sink = autonode_1.group_by(col('d_following_holiday_node_16')).select(col('d_fy_quarter_seq_node_16').count.alias('d_fy_quarter_seq_node_16'))
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
      "error_message": "An error occurred while calling o251730778.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#509019689:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[51](input=RelSubset#509019687,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[51]), rel#509019686:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[51](input=RelSubset#509019685,groupBy=d_following_holiday,select=d_following_holiday, Partial_COUNT(d_fy_quarter_seq) AS count$0)]
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
LogicalProject(d_fy_quarter_seq_node_16=[$1])
+- LogicalAggregate(group=[{45}], EXPR$0=[COUNT($39)])
   +- LogicalJoin(condition=[=($45, $62)], joinType=[inner])
      :- LogicalSort(sort0=[$51], dir0=[ASC])
      :  +- LogicalProject(NLsJk=[AS($0, _UTF-16LE'NLsJk')], cr_returned_time_sk_node_15=[$1], cr_item_sk_node_15=[$2], cr_refunded_customer_sk_node_15=[$3], cr_refunded_cdemo_sk_node_15=[$4], cr_refunded_hdemo_sk_node_15=[$5], cr_refunded_addr_sk_node_15=[$6], cr_returning_customer_sk_node_15=[$7], cr_returning_cdemo_sk_node_15=[$8], cr_returning_hdemo_sk_node_15=[$9], cr_returning_addr_sk_node_15=[$10], cr_call_center_sk_node_15=[$11], cr_catalog_page_sk_node_15=[$12], cr_ship_mode_sk_node_15=[$13], cr_warehouse_sk_node_15=[$14], cr_reason_sk_node_15=[$15], cr_order_number_node_15=[$16], cr_return_quantity_node_15=[$17], cr_return_amount_node_15=[$18], cr_return_tax_node_15=[$19], cr_return_amt_inc_tax_node_15=[$20], cr_fee_node_15=[$21], cr_return_ship_cost_node_15=[$22], cr_refunded_cash_node_15=[$23], cr_reversed_charge_node_15=[$24], cr_store_credit_node_15=[$25], cr_net_loss_node_15=[$26], d_date_sk_node_16=[$27], d_date_id_node_16=[$28], d_date_node_16=[$29], d_month_seq_node_16=[$30], d_week_seq_node_16=[$31], d_quarter_seq_node_16=[$32], d_year_node_16=[$33], d_dow_node_16=[$34], d_moy_node_16=[$35], d_dom_node_16=[$36], d_qoy_node_16=[$37], d_fy_year_node_16=[$38], d_fy_quarter_seq_node_16=[$39], d_fy_week_seq_node_16=[$40], d_day_name_node_16=[$41], d_quarter_name_node_16=[$42], d_holiday_node_16=[$43], d_weekend_node_16=[$44], d_following_holiday_node_16=[$45], d_first_dom_node_16=[$46], d_last_dom_node_16=[$47], d_same_day_ly_node_16=[$48], d_same_day_lq_node_16=[$49], d_current_day_node_16=[$50], d_current_week_node_16=[$51], d_current_month_node_16=[$52], d_current_quarter_node_16=[$53], d_current_year_node_16=[$54], _c55=[$55], inv_date_sk_node_17=[$56], inv_item_sk_node_17=[$57], inv_warehouse_sk_node_17=[$58], inv_quantity_on_hand_node_17=[$59])
      :     +- LogicalJoin(condition=[=($57, $38)], joinType=[inner])
      :        :- LogicalProject(cr_returned_date_sk_node_15=[$0], cr_returned_time_sk_node_15=[$1], cr_item_sk_node_15=[$2], cr_refunded_customer_sk_node_15=[$3], cr_refunded_cdemo_sk_node_15=[$4], cr_refunded_hdemo_sk_node_15=[$5], cr_refunded_addr_sk_node_15=[$6], cr_returning_customer_sk_node_15=[$7], cr_returning_cdemo_sk_node_15=[$8], cr_returning_hdemo_sk_node_15=[$9], cr_returning_addr_sk_node_15=[$10], cr_call_center_sk_node_15=[$11], cr_catalog_page_sk_node_15=[$12], cr_ship_mode_sk_node_15=[$13], cr_warehouse_sk_node_15=[$14], cr_reason_sk_node_15=[$15], cr_order_number_node_15=[$16], cr_return_quantity_node_15=[$17], cr_return_amount_node_15=[$18], cr_return_tax_node_15=[$19], cr_return_amt_inc_tax_node_15=[$20], cr_fee_node_15=[$21], cr_return_ship_cost_node_15=[$22], cr_refunded_cash_node_15=[$23], cr_reversed_charge_node_15=[$24], cr_store_credit_node_15=[$25], cr_net_loss_node_15=[$26], d_date_sk_node_16=[$27], d_date_id_node_16=[$28], d_date_node_16=[$29], d_month_seq_node_16=[$30], d_week_seq_node_16=[$31], d_quarter_seq_node_16=[$32], d_year_node_16=[$33], d_dow_node_16=[$34], d_moy_node_16=[$35], d_dom_node_16=[$36], d_qoy_node_16=[$37], d_fy_year_node_16=[$38], d_fy_quarter_seq_node_16=[$39], d_fy_week_seq_node_16=[$40], d_day_name_node_16=[$41], d_quarter_name_node_16=[$42], d_holiday_node_16=[$43], d_weekend_node_16=[$44], d_following_holiday_node_16=[$45], d_first_dom_node_16=[$46], d_last_dom_node_16=[$47], d_same_day_ly_node_16=[$48], d_same_day_lq_node_16=[$49], d_current_day_node_16=[$50], d_current_week_node_16=[$51], d_current_month_node_16=[$52], d_current_quarter_node_16=[$53], d_current_year_node_16=[$54], _c55=[_UTF-16LE'hello'])
      :        :  +- LogicalJoin(condition=[=($30, $2)], joinType=[inner])
      :        :     :- LogicalProject(cr_returned_date_sk_node_15=[$0], cr_returned_time_sk_node_15=[$1], cr_item_sk_node_15=[$2], cr_refunded_customer_sk_node_15=[$3], cr_refunded_cdemo_sk_node_15=[$4], cr_refunded_hdemo_sk_node_15=[$5], cr_refunded_addr_sk_node_15=[$6], cr_returning_customer_sk_node_15=[$7], cr_returning_cdemo_sk_node_15=[$8], cr_returning_hdemo_sk_node_15=[$9], cr_returning_addr_sk_node_15=[$10], cr_call_center_sk_node_15=[$11], cr_catalog_page_sk_node_15=[$12], cr_ship_mode_sk_node_15=[$13], cr_warehouse_sk_node_15=[$14], cr_reason_sk_node_15=[$15], cr_order_number_node_15=[$16], cr_return_quantity_node_15=[$17], cr_return_amount_node_15=[$18], cr_return_tax_node_15=[$19], cr_return_amt_inc_tax_node_15=[$20], cr_fee_node_15=[$21], cr_return_ship_cost_node_15=[$22], cr_refunded_cash_node_15=[$23], cr_reversed_charge_node_15=[$24], cr_store_credit_node_15=[$25], cr_net_loss_node_15=[$26])
      :        :     :  +- LogicalTableScan(table=[[default_catalog, default_database, catalog_returns]])
      :        :     +- LogicalProject(d_date_sk_node_16=[$0], d_date_id_node_16=[$1], d_date_node_16=[$2], d_month_seq_node_16=[$3], d_week_seq_node_16=[$4], d_quarter_seq_node_16=[$5], d_year_node_16=[$6], d_dow_node_16=[$7], d_moy_node_16=[$8], d_dom_node_16=[$9], d_qoy_node_16=[$10], d_fy_year_node_16=[$11], d_fy_quarter_seq_node_16=[$12], d_fy_week_seq_node_16=[$13], d_day_name_node_16=[$14], d_quarter_name_node_16=[$15], d_holiday_node_16=[$16], d_weekend_node_16=[$17], d_following_holiday_node_16=[$18], d_first_dom_node_16=[$19], d_last_dom_node_16=[$20], d_same_day_ly_node_16=[$21], d_same_day_lq_node_16=[$22], d_current_day_node_16=[$23], d_current_week_node_16=[$24], d_current_month_node_16=[$25], d_current_quarter_node_16=[$26], d_current_year_node_16=[$27])
      :        :        +- LogicalTableScan(table=[[default_catalog, default_database, date_dim]])
      :        +- LogicalAggregate(group=[{0, 1, 2, 3}])
      :           +- LogicalSort(sort0=[$0], dir0=[ASC])
      :              +- LogicalProject(inv_date_sk_node_17=[$0], inv_item_sk_node_17=[$1], inv_warehouse_sk_node_17=[$2], inv_quantity_on_hand_node_17=[$3])
      :                 +- LogicalTableScan(table=[[default_catalog, default_database, inventory]])
      +- LogicalSort(fetch=[25])
         +- LogicalFilter(condition=[>($12, -21)])
            +- LogicalSort(sort0=[$12], dir0=[ASC])
               +- LogicalJoin(condition=[=($5, $0)], joinType=[inner])
                  :- LogicalProject(inv_warehouse_sk_node_18=[$1])
                  :  +- LogicalAggregate(group=[{0}], EXPR$0=[AVG($1)])
                  :     +- LogicalProject(inv_date_sk_node_18=[$0], inv_warehouse_sk_node_18=[$2])
                  :        +- LogicalTableScan(table=[[default_catalog, default_database, inventory]])
                  +- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}])
                     +- LogicalProject(wp_web_page_sk_node_19=[$0], wp_web_page_id_node_19=[$1], wp_rec_start_date_node_19=[$2], wp_rec_end_date_node_19=[$3], wp_creation_date_sk_node_19=[$4], wp_access_date_sk_node_19=[$5], wp_autogen_flag_node_19=[$6], wp_customer_sk_node_19=[$7], wp_url_node_19=[$8], wp_type_node_19=[$9], wp_char_count_node_19=[$10], wp_link_count_node_19=[$11], wp_image_count_node_19=[$12], wp_max_ad_count_node_19=[$13])
                        +- LogicalTableScan(table=[[default_catalog, default_database, web_page]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS d_fy_quarter_seq_node_16])
+- SortAggregate(isMerge=[false], groupBy=[d_following_holiday_node_16], select=[d_following_holiday_node_16, COUNT(d_fy_quarter_seq_node_16) AS EXPR$0])
   +- Sort(orderBy=[d_following_holiday_node_16 ASC])
      +- NestedLoopJoin(joinType=[InnerJoin], where=[=(d_following_holiday_node_16, wp_web_page_id)], select=[NLsJk, cr_returned_time_sk_node_15, cr_item_sk_node_15, cr_refunded_customer_sk_node_15, cr_refunded_cdemo_sk_node_15, cr_refunded_hdemo_sk_node_15, cr_refunded_addr_sk_node_15, cr_returning_customer_sk_node_15, cr_returning_cdemo_sk_node_15, cr_returning_hdemo_sk_node_15, cr_returning_addr_sk_node_15, cr_call_center_sk_node_15, cr_catalog_page_sk_node_15, cr_ship_mode_sk_node_15, cr_warehouse_sk_node_15, cr_reason_sk_node_15, cr_order_number_node_15, cr_return_quantity_node_15, cr_return_amount_node_15, cr_return_tax_node_15, cr_return_amt_inc_tax_node_15, cr_fee_node_15, cr_return_ship_cost_node_15, cr_refunded_cash_node_15, cr_reversed_charge_node_15, cr_store_credit_node_15, cr_net_loss_node_15, d_date_sk_node_16, d_date_id_node_16, d_date_node_16, d_month_seq_node_16, d_week_seq_node_16, d_quarter_seq_node_16, d_year_node_16, d_dow_node_16, d_moy_node_16, d_dom_node_16, d_qoy_node_16, d_fy_year_node_16, d_fy_quarter_seq_node_16, d_fy_week_seq_node_16, d_day_name_node_16, d_quarter_name_node_16, d_holiday_node_16, d_weekend_node_16, d_following_holiday_node_16, d_first_dom_node_16, d_last_dom_node_16, d_same_day_ly_node_16, d_same_day_lq_node_16, d_current_day_node_16, d_current_week_node_16, d_current_month_node_16, d_current_quarter_node_16, d_current_year_node_16, _c55, inv_date_sk_node_17, inv_item_sk_node_17, inv_warehouse_sk_node_17, inv_quantity_on_hand_node_17, inv_warehouse_sk_node_18, wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count], build=[right])
         :- Exchange(distribution=[hash[d_following_holiday_node_16]])
         :  +- Calc(select=[cr_returned_date_sk AS NLsJk, cr_returned_time_sk AS cr_returned_time_sk_node_15, cr_item_sk AS cr_item_sk_node_15, cr_refunded_customer_sk AS cr_refunded_customer_sk_node_15, cr_refunded_cdemo_sk AS cr_refunded_cdemo_sk_node_15, cr_refunded_hdemo_sk AS cr_refunded_hdemo_sk_node_15, cr_refunded_addr_sk AS cr_refunded_addr_sk_node_15, cr_returning_customer_sk AS cr_returning_customer_sk_node_15, cr_returning_cdemo_sk AS cr_returning_cdemo_sk_node_15, cr_returning_hdemo_sk AS cr_returning_hdemo_sk_node_15, cr_returning_addr_sk AS cr_returning_addr_sk_node_15, cr_call_center_sk AS cr_call_center_sk_node_15, cr_catalog_page_sk AS cr_catalog_page_sk_node_15, cr_ship_mode_sk AS cr_ship_mode_sk_node_15, cr_warehouse_sk AS cr_warehouse_sk_node_15, cr_reason_sk AS cr_reason_sk_node_15, cr_order_number AS cr_order_number_node_15, cr_return_quantity AS cr_return_quantity_node_15, cr_return_amount AS cr_return_amount_node_15, cr_return_tax AS cr_return_tax_node_15, cr_return_amt_inc_tax AS cr_return_amt_inc_tax_node_15, cr_fee AS cr_fee_node_15, cr_return_ship_cost AS cr_return_ship_cost_node_15, cr_refunded_cash AS cr_refunded_cash_node_15, cr_reversed_charge AS cr_reversed_charge_node_15, cr_store_credit AS cr_store_credit_node_15, cr_net_loss AS cr_net_loss_node_15, d_date_sk AS d_date_sk_node_16, d_date_id AS d_date_id_node_16, d_date AS d_date_node_16, d_month_seq AS d_month_seq_node_16, d_week_seq AS d_week_seq_node_16, d_quarter_seq AS d_quarter_seq_node_16, d_year AS d_year_node_16, d_dow AS d_dow_node_16, d_moy AS d_moy_node_16, d_dom AS d_dom_node_16, d_qoy AS d_qoy_node_16, d_fy_year AS d_fy_year_node_16, d_fy_quarter_seq AS d_fy_quarter_seq_node_16, d_fy_week_seq AS d_fy_week_seq_node_16, d_day_name AS d_day_name_node_16, d_quarter_name AS d_quarter_name_node_16, d_holiday AS d_holiday_node_16, d_weekend AS d_weekend_node_16, d_following_holiday AS d_following_holiday_node_16, d_first_dom AS d_first_dom_node_16, d_last_dom AS d_last_dom_node_16, d_same_day_ly AS d_same_day_ly_node_16, d_same_day_lq AS d_same_day_lq_node_16, d_current_day AS d_current_day_node_16, d_current_week AS d_current_week_node_16, d_current_month AS d_current_month_node_16, d_current_quarter AS d_current_quarter_node_16, d_current_year AS d_current_year_node_16, 'hello' AS _c55, inv_date_sk AS inv_date_sk_node_17, inv_item_sk AS inv_item_sk_node_17, inv_warehouse_sk AS inv_warehouse_sk_node_17, inv_quantity_on_hand AS inv_quantity_on_hand_node_17])
         :     +- SortLimit(orderBy=[d_current_week ASC], offset=[0], fetch=[1], global=[true])
         :        +- Exchange(distribution=[single])
         :           +- SortLimit(orderBy=[d_current_week ASC], offset=[0], fetch=[1], global=[false])
         :              +- NestedLoopJoin(joinType=[InnerJoin], where=[=(inv_item_sk, d_fy_year)], select=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss, d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand], build=[right])
         :                 :- HashJoin(joinType=[InnerJoin], where=[=(d_month_seq, cr_item_sk)], select=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss, d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year], build=[right])
         :                 :  :- Exchange(distribution=[hash[cr_item_sk]])
         :                 :  :  +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])
         :                 :  +- Exchange(distribution=[hash[d_month_seq]])
         :                 :     +- TableSourceScan(table=[[default_catalog, default_database, date_dim]], fields=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year])
         :                 +- Exchange(distribution=[broadcast])
         :                    +- SortAggregate(isMerge=[false], groupBy=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand], select=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand])
         :                       +- Sort(orderBy=[inv_date_sk ASC, inv_item_sk ASC, inv_warehouse_sk ASC, inv_quantity_on_hand ASC])
         :                          +- Exchange(distribution=[hash[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand]])
         :                             +- SortLimit(orderBy=[inv_date_sk ASC], offset=[0], fetch=[1], global=[true])
         :                                +- Exchange(distribution=[single])
         :                                   +- SortLimit(orderBy=[inv_date_sk ASC], offset=[0], fetch=[1], global=[false])
         :                                      +- TableSourceScan(table=[[default_catalog, default_database, inventory]], fields=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand])
         +- Exchange(distribution=[broadcast])
            +- Limit(offset=[0], fetch=[25], global=[true])
               +- Sort(orderBy=[wp_link_count ASC])
                  +- Exchange(distribution=[single])
                     +- Limit(offset=[0], fetch=[25], global=[false])
                        +- Calc(select=[inv_warehouse_sk_node_18, wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count], where=[>(wp_link_count, -21)])
                           +- SortLimit(orderBy=[wp_link_count ASC], offset=[0], fetch=[1], global=[true])
                              +- Exchange(distribution=[single])
                                 +- SortLimit(orderBy=[wp_link_count ASC], offset=[0], fetch=[1], global=[false])
                                    +- HashJoin(joinType=[InnerJoin], where=[=(wp_creation_date_sk, inv_warehouse_sk_node_18)], select=[inv_warehouse_sk_node_18, wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count], isBroadcast=[true], build=[right])
                                       :- Calc(select=[EXPR$0 AS inv_warehouse_sk_node_18])
                                       :  +- HashAggregate(isMerge=[true], groupBy=[inv_date_sk], select=[inv_date_sk, Final_AVG(sum$0, count$1) AS EXPR$0])
                                       :     +- Exchange(distribution=[hash[inv_date_sk]])
                                       :        +- LocalHashAggregate(groupBy=[inv_date_sk], select=[inv_date_sk, Partial_AVG(inv_warehouse_sk) AS (sum$0, count$1)])
                                       :           +- TableSourceScan(table=[[default_catalog, default_database, inventory, project=[inv_date_sk, inv_warehouse_sk], metadata=[]]], fields=[inv_date_sk, inv_warehouse_sk])
                                       +- Exchange(distribution=[broadcast])
                                          +- HashAggregate(isMerge=[false], groupBy=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count], select=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
                                             +- Exchange(distribution=[hash[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count]])
                                                +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS d_fy_quarter_seq_node_16])
+- SortAggregate(isMerge=[false], groupBy=[d_following_holiday_node_16], select=[d_following_holiday_node_16, COUNT(d_fy_quarter_seq_node_16) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[d_following_holiday_node_16 ASC])
         +- Exchange(distribution=[keep_input_as_is[hash[d_following_holiday_node_16]]])
            +- NestedLoopJoin(joinType=[InnerJoin], where=[(d_following_holiday_node_16 = wp_web_page_id)], select=[NLsJk, cr_returned_time_sk_node_15, cr_item_sk_node_15, cr_refunded_customer_sk_node_15, cr_refunded_cdemo_sk_node_15, cr_refunded_hdemo_sk_node_15, cr_refunded_addr_sk_node_15, cr_returning_customer_sk_node_15, cr_returning_cdemo_sk_node_15, cr_returning_hdemo_sk_node_15, cr_returning_addr_sk_node_15, cr_call_center_sk_node_15, cr_catalog_page_sk_node_15, cr_ship_mode_sk_node_15, cr_warehouse_sk_node_15, cr_reason_sk_node_15, cr_order_number_node_15, cr_return_quantity_node_15, cr_return_amount_node_15, cr_return_tax_node_15, cr_return_amt_inc_tax_node_15, cr_fee_node_15, cr_return_ship_cost_node_15, cr_refunded_cash_node_15, cr_reversed_charge_node_15, cr_store_credit_node_15, cr_net_loss_node_15, d_date_sk_node_16, d_date_id_node_16, d_date_node_16, d_month_seq_node_16, d_week_seq_node_16, d_quarter_seq_node_16, d_year_node_16, d_dow_node_16, d_moy_node_16, d_dom_node_16, d_qoy_node_16, d_fy_year_node_16, d_fy_quarter_seq_node_16, d_fy_week_seq_node_16, d_day_name_node_16, d_quarter_name_node_16, d_holiday_node_16, d_weekend_node_16, d_following_holiday_node_16, d_first_dom_node_16, d_last_dom_node_16, d_same_day_ly_node_16, d_same_day_lq_node_16, d_current_day_node_16, d_current_week_node_16, d_current_month_node_16, d_current_quarter_node_16, d_current_year_node_16, _c55, inv_date_sk_node_17, inv_item_sk_node_17, inv_warehouse_sk_node_17, inv_quantity_on_hand_node_17, inv_warehouse_sk_node_18, wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count], build=[right])
               :- Exchange(distribution=[hash[d_following_holiday_node_16]])
               :  +- Calc(select=[cr_returned_date_sk AS NLsJk, cr_returned_time_sk AS cr_returned_time_sk_node_15, cr_item_sk AS cr_item_sk_node_15, cr_refunded_customer_sk AS cr_refunded_customer_sk_node_15, cr_refunded_cdemo_sk AS cr_refunded_cdemo_sk_node_15, cr_refunded_hdemo_sk AS cr_refunded_hdemo_sk_node_15, cr_refunded_addr_sk AS cr_refunded_addr_sk_node_15, cr_returning_customer_sk AS cr_returning_customer_sk_node_15, cr_returning_cdemo_sk AS cr_returning_cdemo_sk_node_15, cr_returning_hdemo_sk AS cr_returning_hdemo_sk_node_15, cr_returning_addr_sk AS cr_returning_addr_sk_node_15, cr_call_center_sk AS cr_call_center_sk_node_15, cr_catalog_page_sk AS cr_catalog_page_sk_node_15, cr_ship_mode_sk AS cr_ship_mode_sk_node_15, cr_warehouse_sk AS cr_warehouse_sk_node_15, cr_reason_sk AS cr_reason_sk_node_15, cr_order_number AS cr_order_number_node_15, cr_return_quantity AS cr_return_quantity_node_15, cr_return_amount AS cr_return_amount_node_15, cr_return_tax AS cr_return_tax_node_15, cr_return_amt_inc_tax AS cr_return_amt_inc_tax_node_15, cr_fee AS cr_fee_node_15, cr_return_ship_cost AS cr_return_ship_cost_node_15, cr_refunded_cash AS cr_refunded_cash_node_15, cr_reversed_charge AS cr_reversed_charge_node_15, cr_store_credit AS cr_store_credit_node_15, cr_net_loss AS cr_net_loss_node_15, d_date_sk AS d_date_sk_node_16, d_date_id AS d_date_id_node_16, d_date AS d_date_node_16, d_month_seq AS d_month_seq_node_16, d_week_seq AS d_week_seq_node_16, d_quarter_seq AS d_quarter_seq_node_16, d_year AS d_year_node_16, d_dow AS d_dow_node_16, d_moy AS d_moy_node_16, d_dom AS d_dom_node_16, d_qoy AS d_qoy_node_16, d_fy_year AS d_fy_year_node_16, d_fy_quarter_seq AS d_fy_quarter_seq_node_16, d_fy_week_seq AS d_fy_week_seq_node_16, d_day_name AS d_day_name_node_16, d_quarter_name AS d_quarter_name_node_16, d_holiday AS d_holiday_node_16, d_weekend AS d_weekend_node_16, d_following_holiday AS d_following_holiday_node_16, d_first_dom AS d_first_dom_node_16, d_last_dom AS d_last_dom_node_16, d_same_day_ly AS d_same_day_ly_node_16, d_same_day_lq AS d_same_day_lq_node_16, d_current_day AS d_current_day_node_16, d_current_week AS d_current_week_node_16, d_current_month AS d_current_month_node_16, d_current_quarter AS d_current_quarter_node_16, d_current_year AS d_current_year_node_16, 'hello' AS _c55, inv_date_sk AS inv_date_sk_node_17, inv_item_sk AS inv_item_sk_node_17, inv_warehouse_sk AS inv_warehouse_sk_node_17, inv_quantity_on_hand AS inv_quantity_on_hand_node_17])
               :     +- SortLimit(orderBy=[d_current_week ASC], offset=[0], fetch=[1], global=[true])
               :        +- Exchange(distribution=[single])
               :           +- SortLimit(orderBy=[d_current_week ASC], offset=[0], fetch=[1], global=[false])
               :              +- NestedLoopJoin(joinType=[InnerJoin], where=[(inv_item_sk = d_fy_year)], select=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss, d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand], build=[right])
               :                 :- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(d_month_seq = cr_item_sk)], select=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss, d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year], build=[right])
               :                 :  :- Exchange(distribution=[hash[cr_item_sk]])
               :                 :  :  +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])
               :                 :  +- Exchange(distribution=[hash[d_month_seq]])
               :                 :     +- TableSourceScan(table=[[default_catalog, default_database, date_dim]], fields=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year])
               :                 +- Exchange(distribution=[broadcast])
               :                    +- SortAggregate(isMerge=[false], groupBy=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand], select=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand])
               :                       +- Exchange(distribution=[forward])
               :                          +- Sort(orderBy=[inv_date_sk ASC, inv_item_sk ASC, inv_warehouse_sk ASC, inv_quantity_on_hand ASC])
               :                             +- Exchange(distribution=[hash[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand]])
               :                                +- SortLimit(orderBy=[inv_date_sk ASC], offset=[0], fetch=[1], global=[true])
               :                                   +- Exchange(distribution=[single])
               :                                      +- SortLimit(orderBy=[inv_date_sk ASC], offset=[0], fetch=[1], global=[false])
               :                                         +- TableSourceScan(table=[[default_catalog, default_database, inventory]], fields=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand])
               +- Exchange(distribution=[broadcast])
                  +- Limit(offset=[0], fetch=[25], global=[true])
                     +- Sort(orderBy=[wp_link_count ASC])
                        +- Exchange(distribution=[single])
                           +- Limit(offset=[0], fetch=[25], global=[false])
                              +- Calc(select=[inv_warehouse_sk_node_18, wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count], where=[(wp_link_count > -21)])
                                 +- SortLimit(orderBy=[wp_link_count ASC], offset=[0], fetch=[1], global=[true])
                                    +- Exchange(distribution=[single])
                                       +- SortLimit(orderBy=[wp_link_count ASC], offset=[0], fetch=[1], global=[false])
                                          +- HashJoin(joinType=[InnerJoin], where=[(wp_creation_date_sk = inv_warehouse_sk_node_18)], select=[inv_warehouse_sk_node_18, wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count], isBroadcast=[true], build=[right])
                                             :- Calc(select=[EXPR$0 AS inv_warehouse_sk_node_18])
                                             :  +- HashAggregate(isMerge=[true], groupBy=[inv_date_sk], select=[inv_date_sk, Final_AVG(sum$0, count$1) AS EXPR$0])
                                             :     +- Exchange(distribution=[hash[inv_date_sk]])
                                             :        +- LocalHashAggregate(groupBy=[inv_date_sk], select=[inv_date_sk, Partial_AVG(inv_warehouse_sk) AS (sum$0, count$1)])
                                             :           +- TableSourceScan(table=[[default_catalog, default_database, inventory, project=[inv_date_sk, inv_warehouse_sk], metadata=[]]], fields=[inv_date_sk, inv_warehouse_sk])
                                             +- Exchange(distribution=[broadcast])
                                                +- HashAggregate(isMerge=[false], groupBy=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count], select=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
                                                   +- Exchange(distribution=[hash[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count]])
                                                      +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0