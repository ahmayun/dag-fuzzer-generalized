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
    return values.quantile(0.25)


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_10 = table_env.from_path("household_demographics").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("household_demographics").get_schema().get_field_names()])
autonode_12 = table_env.from_path("call_center").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("call_center").get_schema().get_field_names()])
autonode_11 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_9 = autonode_12.add_columns(lit("hello"))
autonode_8 = autonode_10.join(autonode_11, col('ss_item_sk_node_11') == col('hd_vehicle_count_node_10'))
autonode_7 = autonode_9.order_by(col('cc_class_node_12'))
autonode_6 = autonode_8.alias('vvMbE')
autonode_5 = autonode_7.alias('Q5MuU')
autonode_4 = autonode_6.order_by(col('ss_coupon_amt_node_11'))
autonode_3 = autonode_5.filter(col('cc_mkt_desc_node_12').char_length > 5)
autonode_2 = autonode_4.filter(col('ss_hdemo_sk_node_11') > 0)
autonode_1 = autonode_2.join(autonode_3, col('hd_buy_potential_node_10') == col('cc_mkt_desc_node_12'))
sink = autonode_1.group_by(col('cc_hours_node_12')).select(col('cc_mkt_id_node_12').avg.alias('cc_mkt_id_node_12'))
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
      "error_message": "An error occurred while calling o117849096.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#237635525:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[24](input=RelSubset#237635523,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[24]), rel#237635522:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[24](input=RelSubset#237635521,groupBy=hd_buy_potential,select=hd_buy_potential, Partial_COUNT(*) AS count1$0)]
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
LogicalProject(cc_mkt_id_node_12=[$1])
+- LogicalAggregate(group=[{38}], EXPR$0=[AVG($40)])
   +- LogicalJoin(condition=[=($2, $42)], joinType=[inner])
      :- LogicalFilter(condition=[>($10, 0)])
      :  +- LogicalSort(sort0=[$24], dir0=[ASC])
      :     +- LogicalProject(vvMbE=[AS($0, _UTF-16LE'vvMbE')], hd_income_band_sk_node_10=[$1], hd_buy_potential_node_10=[$2], hd_dep_count_node_10=[$3], hd_vehicle_count_node_10=[$4], ss_sold_date_sk_node_11=[$5], ss_sold_time_sk_node_11=[$6], ss_item_sk_node_11=[$7], ss_customer_sk_node_11=[$8], ss_cdemo_sk_node_11=[$9], ss_hdemo_sk_node_11=[$10], ss_addr_sk_node_11=[$11], ss_store_sk_node_11=[$12], ss_promo_sk_node_11=[$13], ss_ticket_number_node_11=[$14], ss_quantity_node_11=[$15], ss_wholesale_cost_node_11=[$16], ss_list_price_node_11=[$17], ss_sales_price_node_11=[$18], ss_ext_discount_amt_node_11=[$19], ss_ext_sales_price_node_11=[$20], ss_ext_wholesale_cost_node_11=[$21], ss_ext_list_price_node_11=[$22], ss_ext_tax_node_11=[$23], ss_coupon_amt_node_11=[$24], ss_net_paid_node_11=[$25], ss_net_paid_inc_tax_node_11=[$26], ss_net_profit_node_11=[$27])
      :        +- LogicalJoin(condition=[=($7, $4)], joinType=[inner])
      :           :- LogicalProject(hd_demo_sk_node_10=[$0], hd_income_band_sk_node_10=[$1], hd_buy_potential_node_10=[$2], hd_dep_count_node_10=[$3], hd_vehicle_count_node_10=[$4])
      :           :  +- LogicalTableScan(table=[[default_catalog, default_database, household_demographics]])
      :           +- LogicalProject(ss_sold_date_sk_node_11=[$0], ss_sold_time_sk_node_11=[$1], ss_item_sk_node_11=[$2], ss_customer_sk_node_11=[$3], ss_cdemo_sk_node_11=[$4], ss_hdemo_sk_node_11=[$5], ss_addr_sk_node_11=[$6], ss_store_sk_node_11=[$7], ss_promo_sk_node_11=[$8], ss_ticket_number_node_11=[$9], ss_quantity_node_11=[$10], ss_wholesale_cost_node_11=[$11], ss_list_price_node_11=[$12], ss_sales_price_node_11=[$13], ss_ext_discount_amt_node_11=[$14], ss_ext_sales_price_node_11=[$15], ss_ext_wholesale_cost_node_11=[$16], ss_ext_list_price_node_11=[$17], ss_ext_tax_node_11=[$18], ss_coupon_amt_node_11=[$19], ss_net_paid_node_11=[$20], ss_net_paid_inc_tax_node_11=[$21], ss_net_profit_node_11=[$22])
      :              +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])
      +- LogicalFilter(condition=[>(CHAR_LENGTH($14), 5)])
         +- LogicalProject(Q5MuU=[AS($0, _UTF-16LE'Q5MuU')], cc_call_center_id_node_12=[$1], cc_rec_start_date_node_12=[$2], cc_rec_end_date_node_12=[$3], cc_closed_date_sk_node_12=[$4], cc_open_date_sk_node_12=[$5], cc_name_node_12=[$6], cc_class_node_12=[$7], cc_employees_node_12=[$8], cc_sq_ft_node_12=[$9], cc_hours_node_12=[$10], cc_manager_node_12=[$11], cc_mkt_id_node_12=[$12], cc_mkt_class_node_12=[$13], cc_mkt_desc_node_12=[$14], cc_market_manager_node_12=[$15], cc_division_node_12=[$16], cc_division_name_node_12=[$17], cc_company_node_12=[$18], cc_company_name_node_12=[$19], cc_street_number_node_12=[$20], cc_street_name_node_12=[$21], cc_street_type_node_12=[$22], cc_suite_number_node_12=[$23], cc_city_node_12=[$24], cc_county_node_12=[$25], cc_state_node_12=[$26], cc_zip_node_12=[$27], cc_country_node_12=[$28], cc_gmt_offset_node_12=[$29], cc_tax_percentage_node_12=[$30], _c31=[$31])
            +- LogicalSort(sort0=[$7], dir0=[ASC])
               +- LogicalProject(cc_call_center_sk_node_12=[$0], cc_call_center_id_node_12=[$1], cc_rec_start_date_node_12=[$2], cc_rec_end_date_node_12=[$3], cc_closed_date_sk_node_12=[$4], cc_open_date_sk_node_12=[$5], cc_name_node_12=[$6], cc_class_node_12=[$7], cc_employees_node_12=[$8], cc_sq_ft_node_12=[$9], cc_hours_node_12=[$10], cc_manager_node_12=[$11], cc_mkt_id_node_12=[$12], cc_mkt_class_node_12=[$13], cc_mkt_desc_node_12=[$14], cc_market_manager_node_12=[$15], cc_division_node_12=[$16], cc_division_name_node_12=[$17], cc_company_node_12=[$18], cc_company_name_node_12=[$19], cc_street_number_node_12=[$20], cc_street_name_node_12=[$21], cc_street_type_node_12=[$22], cc_suite_number_node_12=[$23], cc_city_node_12=[$24], cc_county_node_12=[$25], cc_state_node_12=[$26], cc_zip_node_12=[$27], cc_country_node_12=[$28], cc_gmt_offset_node_12=[$29], cc_tax_percentage_node_12=[$30], _c31=[_UTF-16LE'hello'])
                  +- LogicalTableScan(table=[[default_catalog, default_database, call_center]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS cc_mkt_id_node_12])
+- SortAggregate(isMerge=[false], groupBy=[cc_hours_node_12], select=[cc_hours_node_12, AVG(cc_mkt_id_node_12) AS EXPR$0])
   +- Sort(orderBy=[cc_hours_node_12 ASC])
      +- NestedLoopJoin(joinType=[InnerJoin], where=[=(hd_buy_potential_node_10, cc_mkt_desc_node_12)], select=[vvMbE, hd_income_band_sk_node_10, hd_buy_potential_node_10, hd_dep_count_node_10, hd_vehicle_count_node_10, ss_sold_date_sk_node_11, ss_sold_time_sk_node_11, ss_item_sk_node_11, ss_customer_sk_node_11, ss_cdemo_sk_node_11, ss_hdemo_sk_node_11, ss_addr_sk_node_11, ss_store_sk_node_11, ss_promo_sk_node_11, ss_ticket_number_node_11, ss_quantity_node_11, ss_wholesale_cost_node_11, ss_list_price_node_11, ss_sales_price_node_11, ss_ext_discount_amt_node_11, ss_ext_sales_price_node_11, ss_ext_wholesale_cost_node_11, ss_ext_list_price_node_11, ss_ext_tax_node_11, ss_coupon_amt_node_11, ss_net_paid_node_11, ss_net_paid_inc_tax_node_11, ss_net_profit_node_11, Q5MuU, cc_call_center_id_node_12, cc_rec_start_date_node_12, cc_rec_end_date_node_12, cc_closed_date_sk_node_12, cc_open_date_sk_node_12, cc_name_node_12, cc_class_node_12, cc_employees_node_12, cc_sq_ft_node_12, cc_hours_node_12, cc_manager_node_12, cc_mkt_id_node_12, cc_mkt_class_node_12, cc_mkt_desc_node_12, cc_market_manager_node_12, cc_division_node_12, cc_division_name_node_12, cc_company_node_12, cc_company_name_node_12, cc_street_number_node_12, cc_street_name_node_12, cc_street_type_node_12, cc_suite_number_node_12, cc_city_node_12, cc_county_node_12, cc_state_node_12, cc_zip_node_12, cc_country_node_12, cc_gmt_offset_node_12, cc_tax_percentage_node_12, _c31], build=[left])
         :- Exchange(distribution=[broadcast])
         :  +- Calc(select=[hd_demo_sk AS vvMbE, hd_income_band_sk AS hd_income_band_sk_node_10, hd_buy_potential AS hd_buy_potential_node_10, hd_dep_count AS hd_dep_count_node_10, hd_vehicle_count AS hd_vehicle_count_node_10, ss_sold_date_sk AS ss_sold_date_sk_node_11, ss_sold_time_sk AS ss_sold_time_sk_node_11, ss_item_sk AS ss_item_sk_node_11, ss_customer_sk AS ss_customer_sk_node_11, ss_cdemo_sk AS ss_cdemo_sk_node_11, ss_hdemo_sk AS ss_hdemo_sk_node_11, ss_addr_sk AS ss_addr_sk_node_11, ss_store_sk AS ss_store_sk_node_11, ss_promo_sk AS ss_promo_sk_node_11, ss_ticket_number AS ss_ticket_number_node_11, ss_quantity AS ss_quantity_node_11, ss_wholesale_cost AS ss_wholesale_cost_node_11, ss_list_price AS ss_list_price_node_11, ss_sales_price AS ss_sales_price_node_11, ss_ext_discount_amt AS ss_ext_discount_amt_node_11, ss_ext_sales_price AS ss_ext_sales_price_node_11, ss_ext_wholesale_cost AS ss_ext_wholesale_cost_node_11, ss_ext_list_price AS ss_ext_list_price_node_11, ss_ext_tax AS ss_ext_tax_node_11, ss_coupon_amt AS ss_coupon_amt_node_11, ss_net_paid AS ss_net_paid_node_11, ss_net_paid_inc_tax AS ss_net_paid_inc_tax_node_11, ss_net_profit AS ss_net_profit_node_11], where=[>(ss_hdemo_sk, 0)])
         :     +- SortLimit(orderBy=[ss_coupon_amt ASC], offset=[0], fetch=[1], global=[true])
         :        +- Exchange(distribution=[single])
         :           +- SortLimit(orderBy=[ss_coupon_amt ASC], offset=[0], fetch=[1], global=[false])
         :              +- HashJoin(joinType=[InnerJoin], where=[=(ss_item_sk, hd_vehicle_count)], select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], isBroadcast=[true], build=[left])
         :                 :- Exchange(distribution=[broadcast])
         :                 :  +- TableSourceScan(table=[[default_catalog, default_database, household_demographics]], fields=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
         :                 +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
         +- Exchange(distribution=[hash[cc_hours_node_12]])
            +- Calc(select=[cc_call_center_sk AS Q5MuU, cc_call_center_id AS cc_call_center_id_node_12, cc_rec_start_date AS cc_rec_start_date_node_12, cc_rec_end_date AS cc_rec_end_date_node_12, cc_closed_date_sk AS cc_closed_date_sk_node_12, cc_open_date_sk AS cc_open_date_sk_node_12, cc_name AS cc_name_node_12, cc_class AS cc_class_node_12, cc_employees AS cc_employees_node_12, cc_sq_ft AS cc_sq_ft_node_12, cc_hours AS cc_hours_node_12, cc_manager AS cc_manager_node_12, cc_mkt_id AS cc_mkt_id_node_12, cc_mkt_class AS cc_mkt_class_node_12, cc_mkt_desc AS cc_mkt_desc_node_12, cc_market_manager AS cc_market_manager_node_12, cc_division AS cc_division_node_12, cc_division_name AS cc_division_name_node_12, cc_company AS cc_company_node_12, cc_company_name AS cc_company_name_node_12, cc_street_number AS cc_street_number_node_12, cc_street_name AS cc_street_name_node_12, cc_street_type AS cc_street_type_node_12, cc_suite_number AS cc_suite_number_node_12, cc_city AS cc_city_node_12, cc_county AS cc_county_node_12, cc_state AS cc_state_node_12, cc_zip AS cc_zip_node_12, cc_country AS cc_country_node_12, cc_gmt_offset AS cc_gmt_offset_node_12, cc_tax_percentage AS cc_tax_percentage_node_12, 'hello' AS _c31], where=[>(CHAR_LENGTH(cc_mkt_desc), 5)])
               +- SortLimit(orderBy=[cc_class ASC], offset=[0], fetch=[1], global=[true])
                  +- Exchange(distribution=[single])
                     +- SortLimit(orderBy=[cc_class ASC], offset=[0], fetch=[1], global=[false])
                        +- TableSourceScan(table=[[default_catalog, default_database, call_center]], fields=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS cc_mkt_id_node_12])
+- SortAggregate(isMerge=[false], groupBy=[cc_hours_node_12], select=[cc_hours_node_12, AVG(cc_mkt_id_node_12) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[cc_hours_node_12 ASC])
         +- Exchange(distribution=[keep_input_as_is[hash[cc_hours_node_12]]])
            +- NestedLoopJoin(joinType=[InnerJoin], where=[(hd_buy_potential_node_10 = cc_mkt_desc_node_12)], select=[vvMbE, hd_income_band_sk_node_10, hd_buy_potential_node_10, hd_dep_count_node_10, hd_vehicle_count_node_10, ss_sold_date_sk_node_11, ss_sold_time_sk_node_11, ss_item_sk_node_11, ss_customer_sk_node_11, ss_cdemo_sk_node_11, ss_hdemo_sk_node_11, ss_addr_sk_node_11, ss_store_sk_node_11, ss_promo_sk_node_11, ss_ticket_number_node_11, ss_quantity_node_11, ss_wholesale_cost_node_11, ss_list_price_node_11, ss_sales_price_node_11, ss_ext_discount_amt_node_11, ss_ext_sales_price_node_11, ss_ext_wholesale_cost_node_11, ss_ext_list_price_node_11, ss_ext_tax_node_11, ss_coupon_amt_node_11, ss_net_paid_node_11, ss_net_paid_inc_tax_node_11, ss_net_profit_node_11, Q5MuU, cc_call_center_id_node_12, cc_rec_start_date_node_12, cc_rec_end_date_node_12, cc_closed_date_sk_node_12, cc_open_date_sk_node_12, cc_name_node_12, cc_class_node_12, cc_employees_node_12, cc_sq_ft_node_12, cc_hours_node_12, cc_manager_node_12, cc_mkt_id_node_12, cc_mkt_class_node_12, cc_mkt_desc_node_12, cc_market_manager_node_12, cc_division_node_12, cc_division_name_node_12, cc_company_node_12, cc_company_name_node_12, cc_street_number_node_12, cc_street_name_node_12, cc_street_type_node_12, cc_suite_number_node_12, cc_city_node_12, cc_county_node_12, cc_state_node_12, cc_zip_node_12, cc_country_node_12, cc_gmt_offset_node_12, cc_tax_percentage_node_12, _c31], build=[left])
               :- Exchange(distribution=[broadcast])
               :  +- Calc(select=[hd_demo_sk AS vvMbE, hd_income_band_sk AS hd_income_band_sk_node_10, hd_buy_potential AS hd_buy_potential_node_10, hd_dep_count AS hd_dep_count_node_10, hd_vehicle_count AS hd_vehicle_count_node_10, ss_sold_date_sk AS ss_sold_date_sk_node_11, ss_sold_time_sk AS ss_sold_time_sk_node_11, ss_item_sk AS ss_item_sk_node_11, ss_customer_sk AS ss_customer_sk_node_11, ss_cdemo_sk AS ss_cdemo_sk_node_11, ss_hdemo_sk AS ss_hdemo_sk_node_11, ss_addr_sk AS ss_addr_sk_node_11, ss_store_sk AS ss_store_sk_node_11, ss_promo_sk AS ss_promo_sk_node_11, ss_ticket_number AS ss_ticket_number_node_11, ss_quantity AS ss_quantity_node_11, ss_wholesale_cost AS ss_wholesale_cost_node_11, ss_list_price AS ss_list_price_node_11, ss_sales_price AS ss_sales_price_node_11, ss_ext_discount_amt AS ss_ext_discount_amt_node_11, ss_ext_sales_price AS ss_ext_sales_price_node_11, ss_ext_wholesale_cost AS ss_ext_wholesale_cost_node_11, ss_ext_list_price AS ss_ext_list_price_node_11, ss_ext_tax AS ss_ext_tax_node_11, ss_coupon_amt AS ss_coupon_amt_node_11, ss_net_paid AS ss_net_paid_node_11, ss_net_paid_inc_tax AS ss_net_paid_inc_tax_node_11, ss_net_profit AS ss_net_profit_node_11], where=[(ss_hdemo_sk > 0)])
               :     +- SortLimit(orderBy=[ss_coupon_amt ASC], offset=[0], fetch=[1], global=[true])
               :        +- Exchange(distribution=[single])
               :           +- SortLimit(orderBy=[ss_coupon_amt ASC], offset=[0], fetch=[1], global=[false])
               :              +- HashJoin(joinType=[InnerJoin], where=[(ss_item_sk = hd_vehicle_count)], select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], isBroadcast=[true], build=[left])
               :                 :- Exchange(distribution=[broadcast])
               :                 :  +- TableSourceScan(table=[[default_catalog, default_database, household_demographics]], fields=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
               :                 +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
               +- Exchange(distribution=[hash[cc_hours_node_12]])
                  +- Calc(select=[cc_call_center_sk AS Q5MuU, cc_call_center_id AS cc_call_center_id_node_12, cc_rec_start_date AS cc_rec_start_date_node_12, cc_rec_end_date AS cc_rec_end_date_node_12, cc_closed_date_sk AS cc_closed_date_sk_node_12, cc_open_date_sk AS cc_open_date_sk_node_12, cc_name AS cc_name_node_12, cc_class AS cc_class_node_12, cc_employees AS cc_employees_node_12, cc_sq_ft AS cc_sq_ft_node_12, cc_hours AS cc_hours_node_12, cc_manager AS cc_manager_node_12, cc_mkt_id AS cc_mkt_id_node_12, cc_mkt_class AS cc_mkt_class_node_12, cc_mkt_desc AS cc_mkt_desc_node_12, cc_market_manager AS cc_market_manager_node_12, cc_division AS cc_division_node_12, cc_division_name AS cc_division_name_node_12, cc_company AS cc_company_node_12, cc_company_name AS cc_company_name_node_12, cc_street_number AS cc_street_number_node_12, cc_street_name AS cc_street_name_node_12, cc_street_type AS cc_street_type_node_12, cc_suite_number AS cc_suite_number_node_12, cc_city AS cc_city_node_12, cc_county AS cc_county_node_12, cc_state AS cc_state_node_12, cc_zip AS cc_zip_node_12, cc_country AS cc_country_node_12, cc_gmt_offset AS cc_gmt_offset_node_12, cc_tax_percentage AS cc_tax_percentage_node_12, 'hello' AS _c31], where=[(CHAR_LENGTH(cc_mkt_desc) > 5)])
                     +- SortLimit(orderBy=[cc_class ASC], offset=[0], fetch=[1], global=[true])
                        +- Exchange(distribution=[single])
                           +- SortLimit(orderBy=[cc_class ASC], offset=[0], fetch=[1], global=[false])
                              +- TableSourceScan(table=[[default_catalog, default_database, call_center]], fields=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0