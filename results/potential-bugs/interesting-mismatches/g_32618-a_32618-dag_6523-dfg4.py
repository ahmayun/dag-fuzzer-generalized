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

autonode_6 = table_env.from_path("call_center").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("call_center").get_schema().get_field_names()])
autonode_7 = table_env.from_path("date_dim").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("date_dim").get_schema().get_field_names()])
autonode_4 = autonode_6.alias('eCF38')
autonode_5 = autonode_7.alias('wuZkY')
autonode_2 = autonode_4.order_by(col('cc_city_node_6'))
autonode_3 = autonode_5.order_by(col('d_date_id_node_7'))
autonode_1 = autonode_2.join(autonode_3, col('d_current_month_node_7') == col('cc_market_manager_node_6'))
sink = autonode_1.group_by(col('cc_mkt_id_node_6')).select(col('cc_tax_percentage_node_6').sum.alias('cc_tax_percentage_node_6'))
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
      "error_message": "An error occurred while calling o17957503.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#35345610:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[24](input=RelSubset#35345608,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[24]), rel#35345607:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[24](input=RelSubset#35345606,groupBy=cc_mkt_id, cc_market_manager,select=cc_mkt_id, cc_market_manager, Partial_SUM(cc_tax_percentage) AS sum$0)]
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
LogicalProject(cc_tax_percentage_node_6=[$1])
+- LogicalAggregate(group=[{12}], EXPR$0=[SUM($30)])
   +- LogicalJoin(condition=[=($56, $15)], joinType=[inner])
      :- LogicalSort(sort0=[$24], dir0=[ASC])
      :  +- LogicalProject(eCF38=[AS($0, _UTF-16LE'eCF38')], cc_call_center_id_node_6=[$1], cc_rec_start_date_node_6=[$2], cc_rec_end_date_node_6=[$3], cc_closed_date_sk_node_6=[$4], cc_open_date_sk_node_6=[$5], cc_name_node_6=[$6], cc_class_node_6=[$7], cc_employees_node_6=[$8], cc_sq_ft_node_6=[$9], cc_hours_node_6=[$10], cc_manager_node_6=[$11], cc_mkt_id_node_6=[$12], cc_mkt_class_node_6=[$13], cc_mkt_desc_node_6=[$14], cc_market_manager_node_6=[$15], cc_division_node_6=[$16], cc_division_name_node_6=[$17], cc_company_node_6=[$18], cc_company_name_node_6=[$19], cc_street_number_node_6=[$20], cc_street_name_node_6=[$21], cc_street_type_node_6=[$22], cc_suite_number_node_6=[$23], cc_city_node_6=[$24], cc_county_node_6=[$25], cc_state_node_6=[$26], cc_zip_node_6=[$27], cc_country_node_6=[$28], cc_gmt_offset_node_6=[$29], cc_tax_percentage_node_6=[$30])
      :     +- LogicalTableScan(table=[[default_catalog, default_database, call_center]])
      +- LogicalSort(sort0=[$1], dir0=[ASC])
         +- LogicalProject(wuZkY=[AS($0, _UTF-16LE'wuZkY')], d_date_id_node_7=[$1], d_date_node_7=[$2], d_month_seq_node_7=[$3], d_week_seq_node_7=[$4], d_quarter_seq_node_7=[$5], d_year_node_7=[$6], d_dow_node_7=[$7], d_moy_node_7=[$8], d_dom_node_7=[$9], d_qoy_node_7=[$10], d_fy_year_node_7=[$11], d_fy_quarter_seq_node_7=[$12], d_fy_week_seq_node_7=[$13], d_day_name_node_7=[$14], d_quarter_name_node_7=[$15], d_holiday_node_7=[$16], d_weekend_node_7=[$17], d_following_holiday_node_7=[$18], d_first_dom_node_7=[$19], d_last_dom_node_7=[$20], d_same_day_ly_node_7=[$21], d_same_day_lq_node_7=[$22], d_current_day_node_7=[$23], d_current_week_node_7=[$24], d_current_month_node_7=[$25], d_current_quarter_node_7=[$26], d_current_year_node_7=[$27])
            +- LogicalTableScan(table=[[default_catalog, default_database, date_dim]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS cc_tax_percentage_node_6])
+- SortAggregate(isMerge=[false], groupBy=[cc_mkt_id_node_6], select=[cc_mkt_id_node_6, SUM(cc_tax_percentage_node_6) AS EXPR$0])
   +- Sort(orderBy=[cc_mkt_id_node_6 ASC])
      +- NestedLoopJoin(joinType=[InnerJoin], where=[=(d_current_month_node_7, cc_market_manager_node_6)], select=[eCF38, cc_call_center_id_node_6, cc_rec_start_date_node_6, cc_rec_end_date_node_6, cc_closed_date_sk_node_6, cc_open_date_sk_node_6, cc_name_node_6, cc_class_node_6, cc_employees_node_6, cc_sq_ft_node_6, cc_hours_node_6, cc_manager_node_6, cc_mkt_id_node_6, cc_mkt_class_node_6, cc_mkt_desc_node_6, cc_market_manager_node_6, cc_division_node_6, cc_division_name_node_6, cc_company_node_6, cc_company_name_node_6, cc_street_number_node_6, cc_street_name_node_6, cc_street_type_node_6, cc_suite_number_node_6, cc_city_node_6, cc_county_node_6, cc_state_node_6, cc_zip_node_6, cc_country_node_6, cc_gmt_offset_node_6, cc_tax_percentage_node_6, wuZkY, d_date_id_node_7, d_date_node_7, d_month_seq_node_7, d_week_seq_node_7, d_quarter_seq_node_7, d_year_node_7, d_dow_node_7, d_moy_node_7, d_dom_node_7, d_qoy_node_7, d_fy_year_node_7, d_fy_quarter_seq_node_7, d_fy_week_seq_node_7, d_day_name_node_7, d_quarter_name_node_7, d_holiday_node_7, d_weekend_node_7, d_following_holiday_node_7, d_first_dom_node_7, d_last_dom_node_7, d_same_day_ly_node_7, d_same_day_lq_node_7, d_current_day_node_7, d_current_week_node_7, d_current_month_node_7, d_current_quarter_node_7, d_current_year_node_7], build=[right])
         :- Exchange(distribution=[hash[cc_mkt_id_node_6]])
         :  +- Calc(select=[cc_call_center_sk AS eCF38, cc_call_center_id AS cc_call_center_id_node_6, cc_rec_start_date AS cc_rec_start_date_node_6, cc_rec_end_date AS cc_rec_end_date_node_6, cc_closed_date_sk AS cc_closed_date_sk_node_6, cc_open_date_sk AS cc_open_date_sk_node_6, cc_name AS cc_name_node_6, cc_class AS cc_class_node_6, cc_employees AS cc_employees_node_6, cc_sq_ft AS cc_sq_ft_node_6, cc_hours AS cc_hours_node_6, cc_manager AS cc_manager_node_6, cc_mkt_id AS cc_mkt_id_node_6, cc_mkt_class AS cc_mkt_class_node_6, cc_mkt_desc AS cc_mkt_desc_node_6, cc_market_manager AS cc_market_manager_node_6, cc_division AS cc_division_node_6, cc_division_name AS cc_division_name_node_6, cc_company AS cc_company_node_6, cc_company_name AS cc_company_name_node_6, cc_street_number AS cc_street_number_node_6, cc_street_name AS cc_street_name_node_6, cc_street_type AS cc_street_type_node_6, cc_suite_number AS cc_suite_number_node_6, cc_city AS cc_city_node_6, cc_county AS cc_county_node_6, cc_state AS cc_state_node_6, cc_zip AS cc_zip_node_6, cc_country AS cc_country_node_6, cc_gmt_offset AS cc_gmt_offset_node_6, cc_tax_percentage AS cc_tax_percentage_node_6])
         :     +- SortLimit(orderBy=[cc_city ASC], offset=[0], fetch=[1], global=[true])
         :        +- Exchange(distribution=[single])
         :           +- SortLimit(orderBy=[cc_city ASC], offset=[0], fetch=[1], global=[false])
         :              +- TableSourceScan(table=[[default_catalog, default_database, call_center]], fields=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage])
         +- Exchange(distribution=[broadcast])
            +- Calc(select=[d_date_sk AS wuZkY, d_date_id AS d_date_id_node_7, d_date AS d_date_node_7, d_month_seq AS d_month_seq_node_7, d_week_seq AS d_week_seq_node_7, d_quarter_seq AS d_quarter_seq_node_7, d_year AS d_year_node_7, d_dow AS d_dow_node_7, d_moy AS d_moy_node_7, d_dom AS d_dom_node_7, d_qoy AS d_qoy_node_7, d_fy_year AS d_fy_year_node_7, d_fy_quarter_seq AS d_fy_quarter_seq_node_7, d_fy_week_seq AS d_fy_week_seq_node_7, d_day_name AS d_day_name_node_7, d_quarter_name AS d_quarter_name_node_7, d_holiday AS d_holiday_node_7, d_weekend AS d_weekend_node_7, d_following_holiday AS d_following_holiday_node_7, d_first_dom AS d_first_dom_node_7, d_last_dom AS d_last_dom_node_7, d_same_day_ly AS d_same_day_ly_node_7, d_same_day_lq AS d_same_day_lq_node_7, d_current_day AS d_current_day_node_7, d_current_week AS d_current_week_node_7, d_current_month AS d_current_month_node_7, d_current_quarter AS d_current_quarter_node_7, d_current_year AS d_current_year_node_7])
               +- SortLimit(orderBy=[d_date_id ASC], offset=[0], fetch=[1], global=[true])
                  +- Exchange(distribution=[single])
                     +- SortLimit(orderBy=[d_date_id ASC], offset=[0], fetch=[1], global=[false])
                        +- TableSourceScan(table=[[default_catalog, default_database, date_dim]], fields=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS cc_tax_percentage_node_6])
+- SortAggregate(isMerge=[false], groupBy=[cc_mkt_id_node_6], select=[cc_mkt_id_node_6, SUM(cc_tax_percentage_node_6) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[cc_mkt_id_node_6 ASC])
         +- Exchange(distribution=[keep_input_as_is[hash[cc_mkt_id_node_6]]])
            +- NestedLoopJoin(joinType=[InnerJoin], where=[(d_current_month_node_7 = cc_market_manager_node_6)], select=[eCF38, cc_call_center_id_node_6, cc_rec_start_date_node_6, cc_rec_end_date_node_6, cc_closed_date_sk_node_6, cc_open_date_sk_node_6, cc_name_node_6, cc_class_node_6, cc_employees_node_6, cc_sq_ft_node_6, cc_hours_node_6, cc_manager_node_6, cc_mkt_id_node_6, cc_mkt_class_node_6, cc_mkt_desc_node_6, cc_market_manager_node_6, cc_division_node_6, cc_division_name_node_6, cc_company_node_6, cc_company_name_node_6, cc_street_number_node_6, cc_street_name_node_6, cc_street_type_node_6, cc_suite_number_node_6, cc_city_node_6, cc_county_node_6, cc_state_node_6, cc_zip_node_6, cc_country_node_6, cc_gmt_offset_node_6, cc_tax_percentage_node_6, wuZkY, d_date_id_node_7, d_date_node_7, d_month_seq_node_7, d_week_seq_node_7, d_quarter_seq_node_7, d_year_node_7, d_dow_node_7, d_moy_node_7, d_dom_node_7, d_qoy_node_7, d_fy_year_node_7, d_fy_quarter_seq_node_7, d_fy_week_seq_node_7, d_day_name_node_7, d_quarter_name_node_7, d_holiday_node_7, d_weekend_node_7, d_following_holiday_node_7, d_first_dom_node_7, d_last_dom_node_7, d_same_day_ly_node_7, d_same_day_lq_node_7, d_current_day_node_7, d_current_week_node_7, d_current_month_node_7, d_current_quarter_node_7, d_current_year_node_7], build=[right])
               :- Exchange(distribution=[hash[cc_mkt_id_node_6]])
               :  +- Calc(select=[cc_call_center_sk AS eCF38, cc_call_center_id AS cc_call_center_id_node_6, cc_rec_start_date AS cc_rec_start_date_node_6, cc_rec_end_date AS cc_rec_end_date_node_6, cc_closed_date_sk AS cc_closed_date_sk_node_6, cc_open_date_sk AS cc_open_date_sk_node_6, cc_name AS cc_name_node_6, cc_class AS cc_class_node_6, cc_employees AS cc_employees_node_6, cc_sq_ft AS cc_sq_ft_node_6, cc_hours AS cc_hours_node_6, cc_manager AS cc_manager_node_6, cc_mkt_id AS cc_mkt_id_node_6, cc_mkt_class AS cc_mkt_class_node_6, cc_mkt_desc AS cc_mkt_desc_node_6, cc_market_manager AS cc_market_manager_node_6, cc_division AS cc_division_node_6, cc_division_name AS cc_division_name_node_6, cc_company AS cc_company_node_6, cc_company_name AS cc_company_name_node_6, cc_street_number AS cc_street_number_node_6, cc_street_name AS cc_street_name_node_6, cc_street_type AS cc_street_type_node_6, cc_suite_number AS cc_suite_number_node_6, cc_city AS cc_city_node_6, cc_county AS cc_county_node_6, cc_state AS cc_state_node_6, cc_zip AS cc_zip_node_6, cc_country AS cc_country_node_6, cc_gmt_offset AS cc_gmt_offset_node_6, cc_tax_percentage AS cc_tax_percentage_node_6])
               :     +- SortLimit(orderBy=[cc_city ASC], offset=[0], fetch=[1], global=[true])
               :        +- Exchange(distribution=[single])
               :           +- SortLimit(orderBy=[cc_city ASC], offset=[0], fetch=[1], global=[false])
               :              +- TableSourceScan(table=[[default_catalog, default_database, call_center]], fields=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage])
               +- Exchange(distribution=[broadcast])
                  +- Calc(select=[d_date_sk AS wuZkY, d_date_id AS d_date_id_node_7, d_date AS d_date_node_7, d_month_seq AS d_month_seq_node_7, d_week_seq AS d_week_seq_node_7, d_quarter_seq AS d_quarter_seq_node_7, d_year AS d_year_node_7, d_dow AS d_dow_node_7, d_moy AS d_moy_node_7, d_dom AS d_dom_node_7, d_qoy AS d_qoy_node_7, d_fy_year AS d_fy_year_node_7, d_fy_quarter_seq AS d_fy_quarter_seq_node_7, d_fy_week_seq AS d_fy_week_seq_node_7, d_day_name AS d_day_name_node_7, d_quarter_name AS d_quarter_name_node_7, d_holiday AS d_holiday_node_7, d_weekend AS d_weekend_node_7, d_following_holiday AS d_following_holiday_node_7, d_first_dom AS d_first_dom_node_7, d_last_dom AS d_last_dom_node_7, d_same_day_ly AS d_same_day_ly_node_7, d_same_day_lq AS d_same_day_lq_node_7, d_current_day AS d_current_day_node_7, d_current_week AS d_current_week_node_7, d_current_month AS d_current_month_node_7, d_current_quarter AS d_current_quarter_node_7, d_current_year AS d_current_year_node_7])
                     +- SortLimit(orderBy=[d_date_id ASC], offset=[0], fetch=[1], global=[true])
                        +- Exchange(distribution=[single])
                           +- SortLimit(orderBy=[d_date_id ASC], offset=[0], fetch=[1], global=[false])
                              +- TableSourceScan(table=[[default_catalog, default_database, date_dim]], fields=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0