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

autonode_4 = table_env.from_path("date_dim").select(*[col(column_name).alias(f"{column_name}_node_4") for column_name in table_env.from_path("date_dim").get_schema().get_field_names()])
autonode_6 = table_env.from_path("catalog_returns").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("catalog_returns").get_schema().get_field_names()])
autonode_5 = table_env.from_path("store").select(*[col(column_name).alias(f"{column_name}_node_5") for column_name in table_env.from_path("store").get_schema().get_field_names()])
autonode_3 = autonode_6.order_by(col('cr_refunded_customer_sk_node_6'))
autonode_2 = autonode_4.join(autonode_5, col('d_fy_year_node_4') == col('s_store_sk_node_5'))
autonode_1 = autonode_2.join(autonode_3, col('cr_refunded_customer_sk_node_6') == col('s_market_id_node_5'))
sink = autonode_1.group_by(col('cr_return_amt_inc_tax_node_6')).select(col('s_street_name_node_5').max.alias('s_street_name_node_5'))
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
LogicalProject(s_street_name_node_5=[$1])
+- LogicalAggregate(group=[{77}], EXPR$0=[MAX($47)])
   +- LogicalJoin(condition=[=($60, $38)], joinType=[inner])
      :- LogicalJoin(condition=[=($11, $28)], joinType=[inner])
      :  :- LogicalProject(d_date_sk_node_4=[$0], d_date_id_node_4=[$1], d_date_node_4=[$2], d_month_seq_node_4=[$3], d_week_seq_node_4=[$4], d_quarter_seq_node_4=[$5], d_year_node_4=[$6], d_dow_node_4=[$7], d_moy_node_4=[$8], d_dom_node_4=[$9], d_qoy_node_4=[$10], d_fy_year_node_4=[$11], d_fy_quarter_seq_node_4=[$12], d_fy_week_seq_node_4=[$13], d_day_name_node_4=[$14], d_quarter_name_node_4=[$15], d_holiday_node_4=[$16], d_weekend_node_4=[$17], d_following_holiday_node_4=[$18], d_first_dom_node_4=[$19], d_last_dom_node_4=[$20], d_same_day_ly_node_4=[$21], d_same_day_lq_node_4=[$22], d_current_day_node_4=[$23], d_current_week_node_4=[$24], d_current_month_node_4=[$25], d_current_quarter_node_4=[$26], d_current_year_node_4=[$27])
      :  :  +- LogicalTableScan(table=[[default_catalog, default_database, date_dim]])
      :  +- LogicalProject(s_store_sk_node_5=[$0], s_store_id_node_5=[$1], s_rec_start_date_node_5=[$2], s_rec_end_date_node_5=[$3], s_closed_date_sk_node_5=[$4], s_store_name_node_5=[$5], s_number_employees_node_5=[$6], s_floor_space_node_5=[$7], s_hours_node_5=[$8], s_manager_node_5=[$9], s_market_id_node_5=[$10], s_geography_class_node_5=[$11], s_market_desc_node_5=[$12], s_market_manager_node_5=[$13], s_division_id_node_5=[$14], s_division_name_node_5=[$15], s_company_id_node_5=[$16], s_company_name_node_5=[$17], s_street_number_node_5=[$18], s_street_name_node_5=[$19], s_street_type_node_5=[$20], s_suite_number_node_5=[$21], s_city_node_5=[$22], s_county_node_5=[$23], s_state_node_5=[$24], s_zip_node_5=[$25], s_country_node_5=[$26], s_gmt_offset_node_5=[$27], s_tax_precentage_node_5=[$28])
      :     +- LogicalTableScan(table=[[default_catalog, default_database, store]])
      +- LogicalSort(sort0=[$3], dir0=[ASC])
         +- LogicalProject(cr_returned_date_sk_node_6=[$0], cr_returned_time_sk_node_6=[$1], cr_item_sk_node_6=[$2], cr_refunded_customer_sk_node_6=[$3], cr_refunded_cdemo_sk_node_6=[$4], cr_refunded_hdemo_sk_node_6=[$5], cr_refunded_addr_sk_node_6=[$6], cr_returning_customer_sk_node_6=[$7], cr_returning_cdemo_sk_node_6=[$8], cr_returning_hdemo_sk_node_6=[$9], cr_returning_addr_sk_node_6=[$10], cr_call_center_sk_node_6=[$11], cr_catalog_page_sk_node_6=[$12], cr_ship_mode_sk_node_6=[$13], cr_warehouse_sk_node_6=[$14], cr_reason_sk_node_6=[$15], cr_order_number_node_6=[$16], cr_return_quantity_node_6=[$17], cr_return_amount_node_6=[$18], cr_return_tax_node_6=[$19], cr_return_amt_inc_tax_node_6=[$20], cr_fee_node_6=[$21], cr_return_ship_cost_node_6=[$22], cr_refunded_cash_node_6=[$23], cr_reversed_charge_node_6=[$24], cr_store_credit_node_6=[$25], cr_net_loss_node_6=[$26])
            +- LogicalTableScan(table=[[default_catalog, default_database, catalog_returns]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS s_street_name_node_5])
+- SortAggregate(isMerge=[false], groupBy=[cr_return_amt_inc_tax], select=[cr_return_amt_inc_tax, MAX(s_street_name) AS EXPR$0])
   +- Sort(orderBy=[cr_return_amt_inc_tax ASC])
      +- Exchange(distribution=[hash[cr_return_amt_inc_tax]])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[=(cr_refunded_customer_sk, s_market_id)], select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage, cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss], build=[left])
            :- Exchange(distribution=[broadcast])
            :  +- HashJoin(joinType=[InnerJoin], where=[=(d_fy_year, s_store_sk)], select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage], isBroadcast=[true], build=[right])
            :     :- TableSourceScan(table=[[default_catalog, default_database, date_dim]], fields=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year])
            :     +- Exchange(distribution=[broadcast])
            :        +- TableSourceScan(table=[[default_catalog, default_database, store]], fields=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage])
            +- Sort(orderBy=[cr_refunded_customer_sk ASC])
               +- Exchange(distribution=[single])
                  +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS s_street_name_node_5])
+- SortAggregate(isMerge=[false], groupBy=[cr_return_amt_inc_tax], select=[cr_return_amt_inc_tax, MAX(s_street_name) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[cr_return_amt_inc_tax ASC])
         +- Exchange(distribution=[hash[cr_return_amt_inc_tax]])
            +- NestedLoopJoin(joinType=[InnerJoin], where=[(cr_refunded_customer_sk = s_market_id)], select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage, cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss], build=[left])
               :- Exchange(distribution=[broadcast])
               :  +- MultipleInput(readOrder=[1,0], members=[\
HashJoin(joinType=[InnerJoin], where=[(d_fy_year = s_store_sk)], select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage], isBroadcast=[true], build=[right])\
:- [#1] TableSourceScan(table=[[default_catalog, default_database, date_dim]], fields=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year])\
+- [#2] Exchange(distribution=[broadcast])\
])
               :     :- TableSourceScan(table=[[default_catalog, default_database, date_dim]], fields=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year])
               :     +- Exchange(distribution=[broadcast])
               :        +- TableSourceScan(table=[[default_catalog, default_database, store]], fields=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage])
               +- Sort(orderBy=[cr_refunded_customer_sk ASC])
                  +- Exchange(distribution=[single])
                     +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o274157987.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#554598691:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[3](input=RelSubset#554598689,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[3]), rel#554598688:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[3](input=RelSubset#554598687,groupBy=cr_refunded_customer_sk, cr_return_amt_inc_tax,select=cr_refunded_customer_sk, cr_return_amt_inc_tax)]
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