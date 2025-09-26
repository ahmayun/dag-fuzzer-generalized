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
    return values.median()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_6 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_5 = table_env.from_path("store").select(*[col(column_name).alias(f"{column_name}_node_5") for column_name in table_env.from_path("store").get_schema().get_field_names()])
autonode_4 = autonode_6.order_by(col('ss_sales_price_node_6'))
autonode_3 = autonode_5.order_by(col('s_country_node_5'))
autonode_2 = autonode_3.join(autonode_4, col('ss_ext_discount_amt_node_6') == col('s_tax_precentage_node_5'))
autonode_1 = autonode_2.filter(col('s_rec_start_date_node_5').char_length >= 5)
sink = autonode_1.group_by(col('s_market_manager_node_5')).select(col('s_gmt_offset_node_5').max.alias('s_gmt_offset_node_5'))
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
      "error_message": "An error occurred while calling o292824769.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#591093732:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[26](input=RelSubset#591093730,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[26]), rel#591093729:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[26](input=RelSubset#591093728,groupBy=s_market_manager_node_5, s_tax_precentage_node_50,select=s_market_manager_node_5, s_tax_precentage_node_50, Partial_MAX(s_gmt_offset_node_5) AS max$0)]
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
LogicalProject(s_gmt_offset_node_5=[$1])
+- LogicalAggregate(group=[{13}], EXPR$0=[MAX($27)])
   +- LogicalFilter(condition=[>=(CHAR_LENGTH($2), 5)])
      +- LogicalJoin(condition=[=($43, $28)], joinType=[inner])
         :- LogicalSort(sort0=[$26], dir0=[ASC])
         :  +- LogicalProject(s_store_sk_node_5=[$0], s_store_id_node_5=[$1], s_rec_start_date_node_5=[$2], s_rec_end_date_node_5=[$3], s_closed_date_sk_node_5=[$4], s_store_name_node_5=[$5], s_number_employees_node_5=[$6], s_floor_space_node_5=[$7], s_hours_node_5=[$8], s_manager_node_5=[$9], s_market_id_node_5=[$10], s_geography_class_node_5=[$11], s_market_desc_node_5=[$12], s_market_manager_node_5=[$13], s_division_id_node_5=[$14], s_division_name_node_5=[$15], s_company_id_node_5=[$16], s_company_name_node_5=[$17], s_street_number_node_5=[$18], s_street_name_node_5=[$19], s_street_type_node_5=[$20], s_suite_number_node_5=[$21], s_city_node_5=[$22], s_county_node_5=[$23], s_state_node_5=[$24], s_zip_node_5=[$25], s_country_node_5=[$26], s_gmt_offset_node_5=[$27], s_tax_precentage_node_5=[$28])
         :     +- LogicalTableScan(table=[[default_catalog, default_database, store]])
         +- LogicalSort(sort0=[$13], dir0=[ASC])
            +- LogicalProject(ss_sold_date_sk_node_6=[$0], ss_sold_time_sk_node_6=[$1], ss_item_sk_node_6=[$2], ss_customer_sk_node_6=[$3], ss_cdemo_sk_node_6=[$4], ss_hdemo_sk_node_6=[$5], ss_addr_sk_node_6=[$6], ss_store_sk_node_6=[$7], ss_promo_sk_node_6=[$8], ss_ticket_number_node_6=[$9], ss_quantity_node_6=[$10], ss_wholesale_cost_node_6=[$11], ss_list_price_node_6=[$12], ss_sales_price_node_6=[$13], ss_ext_discount_amt_node_6=[$14], ss_ext_sales_price_node_6=[$15], ss_ext_wholesale_cost_node_6=[$16], ss_ext_list_price_node_6=[$17], ss_ext_tax_node_6=[$18], ss_coupon_amt_node_6=[$19], ss_net_paid_node_6=[$20], ss_net_paid_inc_tax_node_6=[$21], ss_net_profit_node_6=[$22])
               +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS s_gmt_offset_node_5])
+- SortAggregate(isMerge=[false], groupBy=[s_market_manager_node_5], select=[s_market_manager_node_5, MAX(s_gmt_offset_node_5) AS EXPR$0])
   +- Sort(orderBy=[s_market_manager_node_5 ASC])
      +- Calc(select=[s_store_sk_node_5, s_store_id_node_5, s_rec_start_date_node_5, s_rec_end_date_node_5, s_closed_date_sk_node_5, s_store_name_node_5, s_number_employees_node_5, s_floor_space_node_5, s_hours_node_5, s_manager_node_5, s_market_id_node_5, s_geography_class_node_5, s_market_desc_node_5, s_market_manager_node_5, s_division_id_node_5, s_division_name_node_5, s_company_id_node_5, s_company_name_node_5, s_street_number_node_5, s_street_name_node_5, s_street_type_node_5, s_suite_number_node_5, s_city_node_5, s_county_node_5, s_state_node_5, s_zip_node_5, s_country_node_5, s_gmt_offset_node_5, s_tax_precentage_node_5, ss_sold_date_sk AS ss_sold_date_sk_node_6, ss_sold_time_sk AS ss_sold_time_sk_node_6, ss_item_sk AS ss_item_sk_node_6, ss_customer_sk AS ss_customer_sk_node_6, ss_cdemo_sk AS ss_cdemo_sk_node_6, ss_hdemo_sk AS ss_hdemo_sk_node_6, ss_addr_sk AS ss_addr_sk_node_6, ss_store_sk AS ss_store_sk_node_6, ss_promo_sk AS ss_promo_sk_node_6, ss_ticket_number AS ss_ticket_number_node_6, ss_quantity AS ss_quantity_node_6, ss_wholesale_cost AS ss_wholesale_cost_node_6, ss_list_price AS ss_list_price_node_6, ss_sales_price AS ss_sales_price_node_6, ss_ext_discount_amt AS ss_ext_discount_amt_node_6, ss_ext_sales_price AS ss_ext_sales_price_node_6, ss_ext_wholesale_cost AS ss_ext_wholesale_cost_node_6, ss_ext_list_price AS ss_ext_list_price_node_6, ss_ext_tax AS ss_ext_tax_node_6, ss_coupon_amt AS ss_coupon_amt_node_6, ss_net_paid AS ss_net_paid_node_6, ss_net_paid_inc_tax AS ss_net_paid_inc_tax_node_6, ss_net_profit AS ss_net_profit_node_6])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[=(ss_ext_discount_amt, s_tax_precentage_node_50)], select=[s_store_sk_node_5, s_store_id_node_5, s_rec_start_date_node_5, s_rec_end_date_node_5, s_closed_date_sk_node_5, s_store_name_node_5, s_number_employees_node_5, s_floor_space_node_5, s_hours_node_5, s_manager_node_5, s_market_id_node_5, s_geography_class_node_5, s_market_desc_node_5, s_market_manager_node_5, s_division_id_node_5, s_division_name_node_5, s_company_id_node_5, s_company_name_node_5, s_street_number_node_5, s_street_name_node_5, s_street_type_node_5, s_suite_number_node_5, s_city_node_5, s_county_node_5, s_state_node_5, s_zip_node_5, s_country_node_5, s_gmt_offset_node_5, s_tax_precentage_node_5, s_tax_precentage_node_50, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], build=[right])
            :- Exchange(distribution=[hash[s_market_manager_node_5]])
            :  +- Calc(select=[s_store_sk AS s_store_sk_node_5, s_store_id AS s_store_id_node_5, s_rec_start_date AS s_rec_start_date_node_5, s_rec_end_date AS s_rec_end_date_node_5, s_closed_date_sk AS s_closed_date_sk_node_5, s_store_name AS s_store_name_node_5, s_number_employees AS s_number_employees_node_5, s_floor_space AS s_floor_space_node_5, s_hours AS s_hours_node_5, s_manager AS s_manager_node_5, s_market_id AS s_market_id_node_5, s_geography_class AS s_geography_class_node_5, s_market_desc AS s_market_desc_node_5, s_market_manager AS s_market_manager_node_5, s_division_id AS s_division_id_node_5, s_division_name AS s_division_name_node_5, s_company_id AS s_company_id_node_5, s_company_name AS s_company_name_node_5, s_street_number AS s_street_number_node_5, s_street_name AS s_street_name_node_5, s_street_type AS s_street_type_node_5, s_suite_number AS s_suite_number_node_5, s_city AS s_city_node_5, s_county AS s_county_node_5, s_state AS s_state_node_5, s_zip AS s_zip_node_5, s_country AS s_country_node_5, s_gmt_offset AS s_gmt_offset_node_5, s_tax_precentage AS s_tax_precentage_node_5, CAST(s_tax_precentage AS DECIMAL(7, 2)) AS s_tax_precentage_node_50], where=[>=(CHAR_LENGTH(s_rec_start_date), 5)])
            :     +- SortLimit(orderBy=[s_country ASC], offset=[0], fetch=[1], global=[true])
            :        +- Exchange(distribution=[single])
            :           +- SortLimit(orderBy=[s_country ASC], offset=[0], fetch=[1], global=[false])
            :              +- TableSourceScan(table=[[default_catalog, default_database, store]], fields=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage])
            +- Exchange(distribution=[broadcast])
               +- SortLimit(orderBy=[ss_sales_price ASC], offset=[0], fetch=[1], global=[true])
                  +- Exchange(distribution=[single])
                     +- SortLimit(orderBy=[ss_sales_price ASC], offset=[0], fetch=[1], global=[false])
                        +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS s_gmt_offset_node_5])
+- SortAggregate(isMerge=[false], groupBy=[s_market_manager_node_5], select=[s_market_manager_node_5, MAX(s_gmt_offset_node_5) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[s_market_manager_node_5 ASC])
         +- Exchange(distribution=[keep_input_as_is[hash[s_market_manager_node_5]]])
            +- Calc(select=[s_store_sk_node_5, s_store_id_node_5, s_rec_start_date_node_5, s_rec_end_date_node_5, s_closed_date_sk_node_5, s_store_name_node_5, s_number_employees_node_5, s_floor_space_node_5, s_hours_node_5, s_manager_node_5, s_market_id_node_5, s_geography_class_node_5, s_market_desc_node_5, s_market_manager_node_5, s_division_id_node_5, s_division_name_node_5, s_company_id_node_5, s_company_name_node_5, s_street_number_node_5, s_street_name_node_5, s_street_type_node_5, s_suite_number_node_5, s_city_node_5, s_county_node_5, s_state_node_5, s_zip_node_5, s_country_node_5, s_gmt_offset_node_5, s_tax_precentage_node_5, ss_sold_date_sk AS ss_sold_date_sk_node_6, ss_sold_time_sk AS ss_sold_time_sk_node_6, ss_item_sk AS ss_item_sk_node_6, ss_customer_sk AS ss_customer_sk_node_6, ss_cdemo_sk AS ss_cdemo_sk_node_6, ss_hdemo_sk AS ss_hdemo_sk_node_6, ss_addr_sk AS ss_addr_sk_node_6, ss_store_sk AS ss_store_sk_node_6, ss_promo_sk AS ss_promo_sk_node_6, ss_ticket_number AS ss_ticket_number_node_6, ss_quantity AS ss_quantity_node_6, ss_wholesale_cost AS ss_wholesale_cost_node_6, ss_list_price AS ss_list_price_node_6, ss_sales_price AS ss_sales_price_node_6, ss_ext_discount_amt AS ss_ext_discount_amt_node_6, ss_ext_sales_price AS ss_ext_sales_price_node_6, ss_ext_wholesale_cost AS ss_ext_wholesale_cost_node_6, ss_ext_list_price AS ss_ext_list_price_node_6, ss_ext_tax AS ss_ext_tax_node_6, ss_coupon_amt AS ss_coupon_amt_node_6, ss_net_paid AS ss_net_paid_node_6, ss_net_paid_inc_tax AS ss_net_paid_inc_tax_node_6, ss_net_profit AS ss_net_profit_node_6])
               +- Exchange(distribution=[keep_input_as_is[hash[s_market_manager_node_5]]])
                  +- NestedLoopJoin(joinType=[InnerJoin], where=[(ss_ext_discount_amt = s_tax_precentage_node_50)], select=[s_store_sk_node_5, s_store_id_node_5, s_rec_start_date_node_5, s_rec_end_date_node_5, s_closed_date_sk_node_5, s_store_name_node_5, s_number_employees_node_5, s_floor_space_node_5, s_hours_node_5, s_manager_node_5, s_market_id_node_5, s_geography_class_node_5, s_market_desc_node_5, s_market_manager_node_5, s_division_id_node_5, s_division_name_node_5, s_company_id_node_5, s_company_name_node_5, s_street_number_node_5, s_street_name_node_5, s_street_type_node_5, s_suite_number_node_5, s_city_node_5, s_county_node_5, s_state_node_5, s_zip_node_5, s_country_node_5, s_gmt_offset_node_5, s_tax_precentage_node_5, s_tax_precentage_node_50, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], build=[right])
                     :- Exchange(distribution=[hash[s_market_manager_node_5]])
                     :  +- Calc(select=[s_store_sk AS s_store_sk_node_5, s_store_id AS s_store_id_node_5, s_rec_start_date AS s_rec_start_date_node_5, s_rec_end_date AS s_rec_end_date_node_5, s_closed_date_sk AS s_closed_date_sk_node_5, s_store_name AS s_store_name_node_5, s_number_employees AS s_number_employees_node_5, s_floor_space AS s_floor_space_node_5, s_hours AS s_hours_node_5, s_manager AS s_manager_node_5, s_market_id AS s_market_id_node_5, s_geography_class AS s_geography_class_node_5, s_market_desc AS s_market_desc_node_5, s_market_manager AS s_market_manager_node_5, s_division_id AS s_division_id_node_5, s_division_name AS s_division_name_node_5, s_company_id AS s_company_id_node_5, s_company_name AS s_company_name_node_5, s_street_number AS s_street_number_node_5, s_street_name AS s_street_name_node_5, s_street_type AS s_street_type_node_5, s_suite_number AS s_suite_number_node_5, s_city AS s_city_node_5, s_county AS s_county_node_5, s_state AS s_state_node_5, s_zip AS s_zip_node_5, s_country AS s_country_node_5, s_gmt_offset AS s_gmt_offset_node_5, s_tax_precentage AS s_tax_precentage_node_5, CAST(s_tax_precentage AS DECIMAL(7, 2)) AS s_tax_precentage_node_50], where=[(CHAR_LENGTH(s_rec_start_date) >= 5)])
                     :     +- SortLimit(orderBy=[s_country ASC], offset=[0], fetch=[1], global=[true])
                     :        +- Exchange(distribution=[single])
                     :           +- SortLimit(orderBy=[s_country ASC], offset=[0], fetch=[1], global=[false])
                     :              +- TableSourceScan(table=[[default_catalog, default_database, store]], fields=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage])
                     +- Exchange(distribution=[broadcast])
                        +- SortLimit(orderBy=[ss_sales_price ASC], offset=[0], fetch=[1], global=[true])
                           +- Exchange(distribution=[single])
                              +- SortLimit(orderBy=[ss_sales_price ASC], offset=[0], fetch=[1], global=[false])
                                 +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0