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
    return values.min()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_13 = table_env.from_path("web_site").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("web_site").get_schema().get_field_names()])
autonode_12 = table_env.from_path("call_center").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("call_center").get_schema().get_field_names()])
autonode_11 = table_env.from_path("inventory").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("inventory").get_schema().get_field_names()])
autonode_10 = autonode_12.join(autonode_13, col('cc_closed_date_sk_node_12') == col('web_mkt_id_node_13'))
autonode_9 = autonode_11.order_by(col('inv_quantity_on_hand_node_11'))
autonode_8 = autonode_10.order_by(col('web_zip_node_13'))
autonode_7 = autonode_9.alias('TAKrS')
autonode_6 = autonode_8.distinct()
autonode_5 = autonode_7.alias('9zrAn')
autonode_4 = autonode_6.select(col('cc_closed_date_sk_node_12'))
autonode_3 = autonode_5.order_by(col('inv_warehouse_sk_node_11'))
autonode_2 = autonode_3.join(autonode_4, col('inv_item_sk_node_11') == col('cc_closed_date_sk_node_12'))
autonode_1 = autonode_2.group_by(col('cc_closed_date_sk_node_12')).select(col('inv_warehouse_sk_node_11').count.alias('inv_warehouse_sk_node_11'))
sink = autonode_1.select(col('inv_warehouse_sk_node_11'))
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
      "error_message": "An error occurred while calling o278924989.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#564317055:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[2](input=RelSubset#564317053,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[2]), rel#564317052:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[2](input=RelSubset#564317051,groupBy=inv_item_sk,select=inv_item_sk, Partial_COUNT(inv_warehouse_sk) AS count$0)]
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
LogicalProject(inv_warehouse_sk_node_11=[$1])
+- LogicalAggregate(group=[{4}], EXPR$0=[COUNT($2)])
   +- LogicalJoin(condition=[=($1, $4)], joinType=[inner])
      :- LogicalSort(sort0=[$2], dir0=[ASC])
      :  +- LogicalProject(9zrAn=[AS(AS($0, _UTF-16LE'TAKrS'), _UTF-16LE'9zrAn')], inv_item_sk_node_11=[$1], inv_warehouse_sk_node_11=[$2], inv_quantity_on_hand_node_11=[$3])
      :     +- LogicalSort(sort0=[$3], dir0=[ASC])
      :        +- LogicalProject(inv_date_sk_node_11=[$0], inv_item_sk_node_11=[$1], inv_warehouse_sk_node_11=[$2], inv_quantity_on_hand_node_11=[$3])
      :           +- LogicalTableScan(table=[[default_catalog, default_database, inventory]])
      +- LogicalProject(cc_closed_date_sk_node_12=[$4])
         +- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56}])
            +- LogicalSort(sort0=[$53], dir0=[ASC])
               +- LogicalJoin(condition=[=($4, $40)], joinType=[inner])
                  :- LogicalProject(cc_call_center_sk_node_12=[$0], cc_call_center_id_node_12=[$1], cc_rec_start_date_node_12=[$2], cc_rec_end_date_node_12=[$3], cc_closed_date_sk_node_12=[$4], cc_open_date_sk_node_12=[$5], cc_name_node_12=[$6], cc_class_node_12=[$7], cc_employees_node_12=[$8], cc_sq_ft_node_12=[$9], cc_hours_node_12=[$10], cc_manager_node_12=[$11], cc_mkt_id_node_12=[$12], cc_mkt_class_node_12=[$13], cc_mkt_desc_node_12=[$14], cc_market_manager_node_12=[$15], cc_division_node_12=[$16], cc_division_name_node_12=[$17], cc_company_node_12=[$18], cc_company_name_node_12=[$19], cc_street_number_node_12=[$20], cc_street_name_node_12=[$21], cc_street_type_node_12=[$22], cc_suite_number_node_12=[$23], cc_city_node_12=[$24], cc_county_node_12=[$25], cc_state_node_12=[$26], cc_zip_node_12=[$27], cc_country_node_12=[$28], cc_gmt_offset_node_12=[$29], cc_tax_percentage_node_12=[$30])
                  :  +- LogicalTableScan(table=[[default_catalog, default_database, call_center]])
                  +- LogicalProject(web_site_sk_node_13=[$0], web_site_id_node_13=[$1], web_rec_start_date_node_13=[$2], web_rec_end_date_node_13=[$3], web_name_node_13=[$4], web_open_date_sk_node_13=[$5], web_close_date_sk_node_13=[$6], web_class_node_13=[$7], web_manager_node_13=[$8], web_mkt_id_node_13=[$9], web_mkt_class_node_13=[$10], web_mkt_desc_node_13=[$11], web_market_manager_node_13=[$12], web_company_id_node_13=[$13], web_company_name_node_13=[$14], web_street_number_node_13=[$15], web_street_name_node_13=[$16], web_street_type_node_13=[$17], web_suite_number_node_13=[$18], web_city_node_13=[$19], web_county_node_13=[$20], web_state_node_13=[$21], web_zip_node_13=[$22], web_country_node_13=[$23], web_gmt_offset_node_13=[$24], web_tax_percentage_node_13=[$25])
                     +- LogicalTableScan(table=[[default_catalog, default_database, web_site]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS inv_warehouse_sk_node_11])
+- SortAggregate(isMerge=[false], groupBy=[cc_closed_date_sk_node_12], select=[cc_closed_date_sk_node_12, COUNT(inv_warehouse_sk_node_11) AS EXPR$0])
   +- Sort(orderBy=[cc_closed_date_sk_node_12 ASC])
      +- Exchange(distribution=[hash[cc_closed_date_sk_node_12]])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[=(inv_item_sk_node_11, cc_closed_date_sk_node_12)], select=[9zrAn, inv_item_sk_node_11, inv_warehouse_sk_node_11, inv_quantity_on_hand_node_11, cc_closed_date_sk_node_12], build=[right])
            :- SortLimit(orderBy=[inv_warehouse_sk_node_11 ASC], offset=[0], fetch=[1], global=[true])
            :  +- Exchange(distribution=[single])
            :     +- SortLimit(orderBy=[inv_warehouse_sk_node_11 ASC], offset=[0], fetch=[1], global=[false])
            :        +- Calc(select=[inv_date_sk AS 9zrAn, inv_item_sk AS inv_item_sk_node_11, inv_warehouse_sk AS inv_warehouse_sk_node_11, inv_quantity_on_hand AS inv_quantity_on_hand_node_11])
            :           +- SortLimit(orderBy=[inv_quantity_on_hand ASC], offset=[0], fetch=[1], global=[true])
            :              +- Exchange(distribution=[single])
            :                 +- SortLimit(orderBy=[inv_quantity_on_hand ASC], offset=[0], fetch=[1], global=[false])
            :                    +- TableSourceScan(table=[[default_catalog, default_database, inventory]], fields=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand])
            +- Exchange(distribution=[broadcast])
               +- Calc(select=[cc_closed_date_sk AS cc_closed_date_sk_node_12])
                  +- SortAggregate(isMerge=[false], groupBy=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage, web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage], select=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage, web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage])
                     +- Sort(orderBy=[cc_call_center_sk ASC, cc_call_center_id ASC, cc_rec_start_date ASC, cc_rec_end_date ASC, cc_closed_date_sk ASC, cc_open_date_sk ASC, cc_name ASC, cc_class ASC, cc_employees ASC, cc_sq_ft ASC, cc_hours ASC, cc_manager ASC, cc_mkt_id ASC, cc_mkt_class ASC, cc_mkt_desc ASC, cc_market_manager ASC, cc_division ASC, cc_division_name ASC, cc_company ASC, cc_company_name ASC, cc_street_number ASC, cc_street_name ASC, cc_street_type ASC, cc_suite_number ASC, cc_city ASC, cc_county ASC, cc_state ASC, cc_zip ASC, cc_country ASC, cc_gmt_offset ASC, cc_tax_percentage ASC, web_site_sk ASC, web_site_id ASC, web_rec_start_date ASC, web_rec_end_date ASC, web_name ASC, web_open_date_sk ASC, web_close_date_sk ASC, web_class ASC, web_manager ASC, web_mkt_id ASC, web_mkt_class ASC, web_mkt_desc ASC, web_market_manager ASC, web_company_id ASC, web_company_name ASC, web_street_number ASC, web_street_name ASC, web_street_type ASC, web_suite_number ASC, web_city ASC, web_county ASC, web_state ASC, web_zip ASC, web_country ASC, web_gmt_offset ASC, web_tax_percentage ASC])
                        +- Exchange(distribution=[hash[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage, web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage]])
                           +- SortLimit(orderBy=[web_zip ASC], offset=[0], fetch=[1], global=[true])
                              +- Exchange(distribution=[single])
                                 +- SortLimit(orderBy=[web_zip ASC], offset=[0], fetch=[1], global=[false])
                                    +- NestedLoopJoin(joinType=[InnerJoin], where=[=(cc_closed_date_sk, web_mkt_id)], select=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage, web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage], build=[left])
                                       :- Exchange(distribution=[broadcast])
                                       :  +- TableSourceScan(table=[[default_catalog, default_database, call_center]], fields=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage])
                                       +- TableSourceScan(table=[[default_catalog, default_database, web_site]], fields=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS inv_warehouse_sk_node_11])
+- SortAggregate(isMerge=[false], groupBy=[cc_closed_date_sk_node_12], select=[cc_closed_date_sk_node_12, COUNT(inv_warehouse_sk_node_11) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[cc_closed_date_sk_node_12 ASC])
         +- Exchange(distribution=[hash[cc_closed_date_sk_node_12]])
            +- NestedLoopJoin(joinType=[InnerJoin], where=[(inv_item_sk_node_11 = cc_closed_date_sk_node_12)], select=[9zrAn, inv_item_sk_node_11, inv_warehouse_sk_node_11, inv_quantity_on_hand_node_11, cc_closed_date_sk_node_12], build=[right])
               :- SortLimit(orderBy=[inv_warehouse_sk_node_11 ASC], offset=[0], fetch=[1], global=[true])
               :  +- Exchange(distribution=[single])
               :     +- SortLimit(orderBy=[inv_warehouse_sk_node_11 ASC], offset=[0], fetch=[1], global=[false])
               :        +- Calc(select=[inv_date_sk AS 9zrAn, inv_item_sk AS inv_item_sk_node_11, inv_warehouse_sk AS inv_warehouse_sk_node_11, inv_quantity_on_hand AS inv_quantity_on_hand_node_11])
               :           +- SortLimit(orderBy=[inv_quantity_on_hand ASC], offset=[0], fetch=[1], global=[true])
               :              +- Exchange(distribution=[single])
               :                 +- SortLimit(orderBy=[inv_quantity_on_hand ASC], offset=[0], fetch=[1], global=[false])
               :                    +- TableSourceScan(table=[[default_catalog, default_database, inventory]], fields=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand])
               +- Exchange(distribution=[broadcast])
                  +- Calc(select=[cc_closed_date_sk AS cc_closed_date_sk_node_12])
                     +- SortAggregate(isMerge=[false], groupBy=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage, web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage], select=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage, web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage])
                        +- Exchange(distribution=[forward])
                           +- Sort(orderBy=[cc_call_center_sk ASC, cc_call_center_id ASC, cc_rec_start_date ASC, cc_rec_end_date ASC, cc_closed_date_sk ASC, cc_open_date_sk ASC, cc_name ASC, cc_class ASC, cc_employees ASC, cc_sq_ft ASC, cc_hours ASC, cc_manager ASC, cc_mkt_id ASC, cc_mkt_class ASC, cc_mkt_desc ASC, cc_market_manager ASC, cc_division ASC, cc_division_name ASC, cc_company ASC, cc_company_name ASC, cc_street_number ASC, cc_street_name ASC, cc_street_type ASC, cc_suite_number ASC, cc_city ASC, cc_county ASC, cc_state ASC, cc_zip ASC, cc_country ASC, cc_gmt_offset ASC, cc_tax_percentage ASC, web_site_sk ASC, web_site_id ASC, web_rec_start_date ASC, web_rec_end_date ASC, web_name ASC, web_open_date_sk ASC, web_close_date_sk ASC, web_class ASC, web_manager ASC, web_mkt_id ASC, web_mkt_class ASC, web_mkt_desc ASC, web_market_manager ASC, web_company_id ASC, web_company_name ASC, web_street_number ASC, web_street_name ASC, web_street_type ASC, web_suite_number ASC, web_city ASC, web_county ASC, web_state ASC, web_zip ASC, web_country ASC, web_gmt_offset ASC, web_tax_percentage ASC])
                              +- Exchange(distribution=[hash[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage, web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage]])
                                 +- SortLimit(orderBy=[web_zip ASC], offset=[0], fetch=[1], global=[true])
                                    +- Exchange(distribution=[single])
                                       +- SortLimit(orderBy=[web_zip ASC], offset=[0], fetch=[1], global=[false])
                                          +- NestedLoopJoin(joinType=[InnerJoin], where=[(cc_closed_date_sk = web_mkt_id)], select=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage, web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage], build=[left])
                                             :- Exchange(distribution=[broadcast])
                                             :  +- TableSourceScan(table=[[default_catalog, default_database, call_center]], fields=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage])
                                             +- TableSourceScan(table=[[default_catalog, default_database, web_site]], fields=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0