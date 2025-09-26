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
    return values.quantile(0.75)


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_6 = table_env.from_path("web_returns").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("web_returns").get_schema().get_field_names()])
autonode_7 = table_env.from_path("web_site").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("web_site").get_schema().get_field_names()])
autonode_4 = autonode_6.alias('dDzdi')
autonode_5 = autonode_7.distinct()
autonode_2 = autonode_4.order_by(col('wr_refunded_customer_sk_node_6'))
autonode_3 = autonode_5.distinct()
autonode_1 = autonode_2.join(autonode_3, col('wr_account_credit_node_6') == col('web_gmt_offset_node_7'))
sink = autonode_1.group_by(col('web_open_date_sk_node_7')).select(col('wr_returning_customer_sk_node_6').sum.alias('wr_returning_customer_sk_node_6'))
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
      "error_message": "An error occurred while calling o114527219.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#231079020:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[3](input=RelSubset#231079018,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[3]), rel#231079017:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[3](input=RelSubset#231079016,groupBy=wr_account_credit,select=wr_account_credit, Partial_SUM(wr_returning_customer_sk) AS sum$0)]
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
LogicalProject(wr_returning_customer_sk_node_6=[$1])
+- LogicalAggregate(group=[{29}], EXPR$0=[SUM($7)])
   +- LogicalJoin(condition=[=($22, $48)], joinType=[inner])
      :- LogicalSort(sort0=[$3], dir0=[ASC])
      :  +- LogicalProject(dDzdi=[AS($0, _UTF-16LE'dDzdi')], wr_returned_time_sk_node_6=[$1], wr_item_sk_node_6=[$2], wr_refunded_customer_sk_node_6=[$3], wr_refunded_cdemo_sk_node_6=[$4], wr_refunded_hdemo_sk_node_6=[$5], wr_refunded_addr_sk_node_6=[$6], wr_returning_customer_sk_node_6=[$7], wr_returning_cdemo_sk_node_6=[$8], wr_returning_hdemo_sk_node_6=[$9], wr_returning_addr_sk_node_6=[$10], wr_web_page_sk_node_6=[$11], wr_reason_sk_node_6=[$12], wr_order_number_node_6=[$13], wr_return_quantity_node_6=[$14], wr_return_amt_node_6=[$15], wr_return_tax_node_6=[$16], wr_return_amt_inc_tax_node_6=[$17], wr_fee_node_6=[$18], wr_return_ship_cost_node_6=[$19], wr_refunded_cash_node_6=[$20], wr_reversed_charge_node_6=[$21], wr_account_credit_node_6=[$22], wr_net_loss_node_6=[$23])
      :     +- LogicalTableScan(table=[[default_catalog, default_database, web_returns]])
      +- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25}])
         +- LogicalProject(web_site_sk_node_7=[$0], web_site_id_node_7=[$1], web_rec_start_date_node_7=[$2], web_rec_end_date_node_7=[$3], web_name_node_7=[$4], web_open_date_sk_node_7=[$5], web_close_date_sk_node_7=[$6], web_class_node_7=[$7], web_manager_node_7=[$8], web_mkt_id_node_7=[$9], web_mkt_class_node_7=[$10], web_mkt_desc_node_7=[$11], web_market_manager_node_7=[$12], web_company_id_node_7=[$13], web_company_name_node_7=[$14], web_street_number_node_7=[$15], web_street_name_node_7=[$16], web_street_type_node_7=[$17], web_suite_number_node_7=[$18], web_city_node_7=[$19], web_county_node_7=[$20], web_state_node_7=[$21], web_zip_node_7=[$22], web_country_node_7=[$23], web_gmt_offset_node_7=[$24], web_tax_percentage_node_7=[$25])
            +- LogicalTableScan(table=[[default_catalog, default_database, web_site]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS wr_returning_customer_sk_node_6])
+- SortAggregate(isMerge=[false], groupBy=[web_open_date_sk_node_7], select=[web_open_date_sk_node_7, SUM(wr_returning_customer_sk_node_6) AS EXPR$0])
   +- Sort(orderBy=[web_open_date_sk_node_7 ASC])
      +- Exchange(distribution=[hash[web_open_date_sk_node_7]])
         +- Calc(select=[dDzdi, wr_returned_time_sk_node_6, wr_item_sk_node_6, wr_refunded_customer_sk_node_6, wr_refunded_cdemo_sk_node_6, wr_refunded_hdemo_sk_node_6, wr_refunded_addr_sk_node_6, wr_returning_customer_sk_node_6, wr_returning_cdemo_sk_node_6, wr_returning_hdemo_sk_node_6, wr_returning_addr_sk_node_6, wr_web_page_sk_node_6, wr_reason_sk_node_6, wr_order_number_node_6, wr_return_quantity_node_6, wr_return_amt_node_6, wr_return_tax_node_6, wr_return_amt_inc_tax_node_6, wr_fee_node_6, wr_return_ship_cost_node_6, wr_refunded_cash_node_6, wr_reversed_charge_node_6, wr_account_credit_node_6, wr_net_loss_node_6, web_site_sk_node_7, web_site_id_node_7, web_rec_start_date_node_7, web_rec_end_date_node_7, web_name_node_7, web_open_date_sk_node_7, web_close_date_sk_node_7, web_class_node_7, web_manager_node_7, web_mkt_id_node_7, web_mkt_class_node_7, web_mkt_desc_node_7, web_market_manager_node_7, web_company_id_node_7, web_company_name_node_7, web_street_number_node_7, web_street_name_node_7, web_street_type_node_7, web_suite_number_node_7, web_city_node_7, web_county_node_7, web_state_node_7, web_zip_node_7, web_country_node_7, web_gmt_offset_node_7, web_tax_percentage_node_7])
            +- NestedLoopJoin(joinType=[InnerJoin], where=[=(wr_account_credit_node_6, web_gmt_offset_node_70)], select=[dDzdi, wr_returned_time_sk_node_6, wr_item_sk_node_6, wr_refunded_customer_sk_node_6, wr_refunded_cdemo_sk_node_6, wr_refunded_hdemo_sk_node_6, wr_refunded_addr_sk_node_6, wr_returning_customer_sk_node_6, wr_returning_cdemo_sk_node_6, wr_returning_hdemo_sk_node_6, wr_returning_addr_sk_node_6, wr_web_page_sk_node_6, wr_reason_sk_node_6, wr_order_number_node_6, wr_return_quantity_node_6, wr_return_amt_node_6, wr_return_tax_node_6, wr_return_amt_inc_tax_node_6, wr_fee_node_6, wr_return_ship_cost_node_6, wr_refunded_cash_node_6, wr_reversed_charge_node_6, wr_account_credit_node_6, wr_net_loss_node_6, web_site_sk_node_7, web_site_id_node_7, web_rec_start_date_node_7, web_rec_end_date_node_7, web_name_node_7, web_open_date_sk_node_7, web_close_date_sk_node_7, web_class_node_7, web_manager_node_7, web_mkt_id_node_7, web_mkt_class_node_7, web_mkt_desc_node_7, web_market_manager_node_7, web_company_id_node_7, web_company_name_node_7, web_street_number_node_7, web_street_name_node_7, web_street_type_node_7, web_suite_number_node_7, web_city_node_7, web_county_node_7, web_state_node_7, web_zip_node_7, web_country_node_7, web_gmt_offset_node_7, web_tax_percentage_node_7, web_gmt_offset_node_70], build=[left])
               :- Exchange(distribution=[broadcast])
               :  +- Calc(select=[wr_returned_date_sk AS dDzdi, wr_returned_time_sk AS wr_returned_time_sk_node_6, wr_item_sk AS wr_item_sk_node_6, wr_refunded_customer_sk AS wr_refunded_customer_sk_node_6, wr_refunded_cdemo_sk AS wr_refunded_cdemo_sk_node_6, wr_refunded_hdemo_sk AS wr_refunded_hdemo_sk_node_6, wr_refunded_addr_sk AS wr_refunded_addr_sk_node_6, wr_returning_customer_sk AS wr_returning_customer_sk_node_6, wr_returning_cdemo_sk AS wr_returning_cdemo_sk_node_6, wr_returning_hdemo_sk AS wr_returning_hdemo_sk_node_6, wr_returning_addr_sk AS wr_returning_addr_sk_node_6, wr_web_page_sk AS wr_web_page_sk_node_6, wr_reason_sk AS wr_reason_sk_node_6, wr_order_number AS wr_order_number_node_6, wr_return_quantity AS wr_return_quantity_node_6, wr_return_amt AS wr_return_amt_node_6, wr_return_tax AS wr_return_tax_node_6, wr_return_amt_inc_tax AS wr_return_amt_inc_tax_node_6, wr_fee AS wr_fee_node_6, wr_return_ship_cost AS wr_return_ship_cost_node_6, wr_refunded_cash AS wr_refunded_cash_node_6, wr_reversed_charge AS wr_reversed_charge_node_6, wr_account_credit AS wr_account_credit_node_6, wr_net_loss AS wr_net_loss_node_6])
               :     +- SortLimit(orderBy=[wr_refunded_customer_sk ASC], offset=[0], fetch=[1], global=[true])
               :        +- Exchange(distribution=[single])
               :           +- SortLimit(orderBy=[wr_refunded_customer_sk ASC], offset=[0], fetch=[1], global=[false])
               :              +- TableSourceScan(table=[[default_catalog, default_database, web_returns]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])
               +- Calc(select=[web_site_sk AS web_site_sk_node_7, web_site_id AS web_site_id_node_7, web_rec_start_date AS web_rec_start_date_node_7, web_rec_end_date AS web_rec_end_date_node_7, web_name AS web_name_node_7, web_open_date_sk AS web_open_date_sk_node_7, web_close_date_sk AS web_close_date_sk_node_7, web_class AS web_class_node_7, web_manager AS web_manager_node_7, web_mkt_id AS web_mkt_id_node_7, web_mkt_class AS web_mkt_class_node_7, web_mkt_desc AS web_mkt_desc_node_7, web_market_manager AS web_market_manager_node_7, web_company_id AS web_company_id_node_7, web_company_name AS web_company_name_node_7, web_street_number AS web_street_number_node_7, web_street_name AS web_street_name_node_7, web_street_type AS web_street_type_node_7, web_suite_number AS web_suite_number_node_7, web_city AS web_city_node_7, web_county AS web_county_node_7, web_state AS web_state_node_7, web_zip AS web_zip_node_7, web_country AS web_country_node_7, web_gmt_offset AS web_gmt_offset_node_7, web_tax_percentage AS web_tax_percentage_node_7, CAST(web_gmt_offset AS DECIMAL(7, 2)) AS web_gmt_offset_node_70])
                  +- HashAggregate(isMerge=[false], groupBy=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage], select=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage])
                     +- Exchange(distribution=[hash[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage]])
                        +- TableSourceScan(table=[[default_catalog, default_database, web_site]], fields=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS wr_returning_customer_sk_node_6])
+- SortAggregate(isMerge=[false], groupBy=[web_open_date_sk_node_7], select=[web_open_date_sk_node_7, SUM(wr_returning_customer_sk_node_6) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[web_open_date_sk_node_7 ASC])
         +- Exchange(distribution=[hash[web_open_date_sk_node_7]])
            +- Calc(select=[dDzdi, wr_returned_time_sk_node_6, wr_item_sk_node_6, wr_refunded_customer_sk_node_6, wr_refunded_cdemo_sk_node_6, wr_refunded_hdemo_sk_node_6, wr_refunded_addr_sk_node_6, wr_returning_customer_sk_node_6, wr_returning_cdemo_sk_node_6, wr_returning_hdemo_sk_node_6, wr_returning_addr_sk_node_6, wr_web_page_sk_node_6, wr_reason_sk_node_6, wr_order_number_node_6, wr_return_quantity_node_6, wr_return_amt_node_6, wr_return_tax_node_6, wr_return_amt_inc_tax_node_6, wr_fee_node_6, wr_return_ship_cost_node_6, wr_refunded_cash_node_6, wr_reversed_charge_node_6, wr_account_credit_node_6, wr_net_loss_node_6, web_site_sk_node_7, web_site_id_node_7, web_rec_start_date_node_7, web_rec_end_date_node_7, web_name_node_7, web_open_date_sk_node_7, web_close_date_sk_node_7, web_class_node_7, web_manager_node_7, web_mkt_id_node_7, web_mkt_class_node_7, web_mkt_desc_node_7, web_market_manager_node_7, web_company_id_node_7, web_company_name_node_7, web_street_number_node_7, web_street_name_node_7, web_street_type_node_7, web_suite_number_node_7, web_city_node_7, web_county_node_7, web_state_node_7, web_zip_node_7, web_country_node_7, web_gmt_offset_node_7, web_tax_percentage_node_7])
               +- NestedLoopJoin(joinType=[InnerJoin], where=[(wr_account_credit_node_6 = web_gmt_offset_node_70)], select=[dDzdi, wr_returned_time_sk_node_6, wr_item_sk_node_6, wr_refunded_customer_sk_node_6, wr_refunded_cdemo_sk_node_6, wr_refunded_hdemo_sk_node_6, wr_refunded_addr_sk_node_6, wr_returning_customer_sk_node_6, wr_returning_cdemo_sk_node_6, wr_returning_hdemo_sk_node_6, wr_returning_addr_sk_node_6, wr_web_page_sk_node_6, wr_reason_sk_node_6, wr_order_number_node_6, wr_return_quantity_node_6, wr_return_amt_node_6, wr_return_tax_node_6, wr_return_amt_inc_tax_node_6, wr_fee_node_6, wr_return_ship_cost_node_6, wr_refunded_cash_node_6, wr_reversed_charge_node_6, wr_account_credit_node_6, wr_net_loss_node_6, web_site_sk_node_7, web_site_id_node_7, web_rec_start_date_node_7, web_rec_end_date_node_7, web_name_node_7, web_open_date_sk_node_7, web_close_date_sk_node_7, web_class_node_7, web_manager_node_7, web_mkt_id_node_7, web_mkt_class_node_7, web_mkt_desc_node_7, web_market_manager_node_7, web_company_id_node_7, web_company_name_node_7, web_street_number_node_7, web_street_name_node_7, web_street_type_node_7, web_suite_number_node_7, web_city_node_7, web_county_node_7, web_state_node_7, web_zip_node_7, web_country_node_7, web_gmt_offset_node_7, web_tax_percentage_node_7, web_gmt_offset_node_70], build=[left])
                  :- Exchange(distribution=[broadcast])
                  :  +- Calc(select=[wr_returned_date_sk AS dDzdi, wr_returned_time_sk AS wr_returned_time_sk_node_6, wr_item_sk AS wr_item_sk_node_6, wr_refunded_customer_sk AS wr_refunded_customer_sk_node_6, wr_refunded_cdemo_sk AS wr_refunded_cdemo_sk_node_6, wr_refunded_hdemo_sk AS wr_refunded_hdemo_sk_node_6, wr_refunded_addr_sk AS wr_refunded_addr_sk_node_6, wr_returning_customer_sk AS wr_returning_customer_sk_node_6, wr_returning_cdemo_sk AS wr_returning_cdemo_sk_node_6, wr_returning_hdemo_sk AS wr_returning_hdemo_sk_node_6, wr_returning_addr_sk AS wr_returning_addr_sk_node_6, wr_web_page_sk AS wr_web_page_sk_node_6, wr_reason_sk AS wr_reason_sk_node_6, wr_order_number AS wr_order_number_node_6, wr_return_quantity AS wr_return_quantity_node_6, wr_return_amt AS wr_return_amt_node_6, wr_return_tax AS wr_return_tax_node_6, wr_return_amt_inc_tax AS wr_return_amt_inc_tax_node_6, wr_fee AS wr_fee_node_6, wr_return_ship_cost AS wr_return_ship_cost_node_6, wr_refunded_cash AS wr_refunded_cash_node_6, wr_reversed_charge AS wr_reversed_charge_node_6, wr_account_credit AS wr_account_credit_node_6, wr_net_loss AS wr_net_loss_node_6])
                  :     +- SortLimit(orderBy=[wr_refunded_customer_sk ASC], offset=[0], fetch=[1], global=[true])
                  :        +- Exchange(distribution=[single])
                  :           +- SortLimit(orderBy=[wr_refunded_customer_sk ASC], offset=[0], fetch=[1], global=[false])
                  :              +- TableSourceScan(table=[[default_catalog, default_database, web_returns]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])
                  +- Calc(select=[web_site_sk AS web_site_sk_node_7, web_site_id AS web_site_id_node_7, web_rec_start_date AS web_rec_start_date_node_7, web_rec_end_date AS web_rec_end_date_node_7, web_name AS web_name_node_7, web_open_date_sk AS web_open_date_sk_node_7, web_close_date_sk AS web_close_date_sk_node_7, web_class AS web_class_node_7, web_manager AS web_manager_node_7, web_mkt_id AS web_mkt_id_node_7, web_mkt_class AS web_mkt_class_node_7, web_mkt_desc AS web_mkt_desc_node_7, web_market_manager AS web_market_manager_node_7, web_company_id AS web_company_id_node_7, web_company_name AS web_company_name_node_7, web_street_number AS web_street_number_node_7, web_street_name AS web_street_name_node_7, web_street_type AS web_street_type_node_7, web_suite_number AS web_suite_number_node_7, web_city AS web_city_node_7, web_county AS web_county_node_7, web_state AS web_state_node_7, web_zip AS web_zip_node_7, web_country AS web_country_node_7, web_gmt_offset AS web_gmt_offset_node_7, web_tax_percentage AS web_tax_percentage_node_7, CAST(web_gmt_offset AS DECIMAL(7, 2)) AS web_gmt_offset_node_70])
                     +- HashAggregate(isMerge=[false], groupBy=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage], select=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage])
                        +- Exchange(distribution=[hash[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage]])
                           +- TableSourceScan(table=[[default_catalog, default_database, web_site]], fields=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0