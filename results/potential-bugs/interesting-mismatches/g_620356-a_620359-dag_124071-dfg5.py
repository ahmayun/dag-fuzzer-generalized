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


def preloaded_aggregation(values: pd.Series) -> int:
    return len(values)


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_13 = table_env.from_path("household_demographics").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("household_demographics").get_schema().get_field_names()])
autonode_12 = table_env.from_path("web_site").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("web_site").get_schema().get_field_names()])
autonode_11 = table_env.from_path("date_dim").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("date_dim").get_schema().get_field_names()])
autonode_14 = table_env.from_path("customer_demographics").select(*[col(column_name).alias(f"{column_name}_node_14") for column_name in table_env.from_path("customer_demographics").get_schema().get_field_names()])
autonode_9 = autonode_13.group_by(col('hd_vehicle_count_node_13')).select(col('hd_vehicle_count_node_13').min.alias('hd_vehicle_count_node_13'))
autonode_8 = autonode_12.distinct()
autonode_7 = autonode_11.add_columns(lit("hello"))
autonode_10 = autonode_14.order_by(col('cd_demo_sk_node_14'))
autonode_5 = autonode_8.join(autonode_9, col('web_open_date_sk_node_12') == col('hd_vehicle_count_node_13'))
autonode_4 = autonode_7.add_columns(lit("hello"))
autonode_6 = autonode_10.alias('26S5K')
autonode_2 = autonode_4.join(autonode_5, col('d_current_month_node_11') == col('web_state_node_12'))
autonode_3 = autonode_6.order_by(col('cd_dep_employed_count_node_14'))
autonode_1 = autonode_2.join(autonode_3, col('cd_purchase_estimate_node_14') == col('web_open_date_sk_node_12'))
sink = autonode_1.group_by(col('web_city_node_12')).select(col('d_current_year_node_11').count.alias('d_current_year_node_11'))
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
      "error_message": "An error occurred while calling o338359693.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#681682760:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[7](input=RelSubset#681682758,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[7]), rel#681682757:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[7](input=RelSubset#681682756,groupBy=cd_purchase_estimate,select=cd_purchase_estimate, Partial_COUNT(*) AS count1$0)]
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
LogicalProject(d_current_year_node_11=[$1])
+- LogicalAggregate(group=[{49}], EXPR$0=[COUNT($27)])
   +- LogicalJoin(condition=[=($61, $35)], joinType=[inner])
      :- LogicalJoin(condition=[=($25, $51)], joinType=[inner])
      :  :- LogicalProject(d_date_sk_node_11=[$0], d_date_id_node_11=[$1], d_date_node_11=[$2], d_month_seq_node_11=[$3], d_week_seq_node_11=[$4], d_quarter_seq_node_11=[$5], d_year_node_11=[$6], d_dow_node_11=[$7], d_moy_node_11=[$8], d_dom_node_11=[$9], d_qoy_node_11=[$10], d_fy_year_node_11=[$11], d_fy_quarter_seq_node_11=[$12], d_fy_week_seq_node_11=[$13], d_day_name_node_11=[$14], d_quarter_name_node_11=[$15], d_holiday_node_11=[$16], d_weekend_node_11=[$17], d_following_holiday_node_11=[$18], d_first_dom_node_11=[$19], d_last_dom_node_11=[$20], d_same_day_ly_node_11=[$21], d_same_day_lq_node_11=[$22], d_current_day_node_11=[$23], d_current_week_node_11=[$24], d_current_month_node_11=[$25], d_current_quarter_node_11=[$26], d_current_year_node_11=[$27], _c28=[_UTF-16LE'hello'], _c29=[_UTF-16LE'hello'])
      :  :  +- LogicalTableScan(table=[[default_catalog, default_database, date_dim]])
      :  +- LogicalJoin(condition=[=($5, $26)], joinType=[inner])
      :     :- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25}])
      :     :  +- LogicalProject(web_site_sk_node_12=[$0], web_site_id_node_12=[$1], web_rec_start_date_node_12=[$2], web_rec_end_date_node_12=[$3], web_name_node_12=[$4], web_open_date_sk_node_12=[$5], web_close_date_sk_node_12=[$6], web_class_node_12=[$7], web_manager_node_12=[$8], web_mkt_id_node_12=[$9], web_mkt_class_node_12=[$10], web_mkt_desc_node_12=[$11], web_market_manager_node_12=[$12], web_company_id_node_12=[$13], web_company_name_node_12=[$14], web_street_number_node_12=[$15], web_street_name_node_12=[$16], web_street_type_node_12=[$17], web_suite_number_node_12=[$18], web_city_node_12=[$19], web_county_node_12=[$20], web_state_node_12=[$21], web_zip_node_12=[$22], web_country_node_12=[$23], web_gmt_offset_node_12=[$24], web_tax_percentage_node_12=[$25])
      :     :     +- LogicalTableScan(table=[[default_catalog, default_database, web_site]])
      :     +- LogicalProject(hd_vehicle_count_node_13=[$1])
      :        +- LogicalAggregate(group=[{0}], EXPR$0=[MIN($0)])
      :           +- LogicalProject(hd_vehicle_count_node_13=[$4])
      :              +- LogicalTableScan(table=[[default_catalog, default_database, household_demographics]])
      +- LogicalSort(sort0=[$7], dir0=[ASC])
         +- LogicalProject(26S5K=[AS($0, _UTF-16LE'26S5K')], cd_gender_node_14=[$1], cd_marital_status_node_14=[$2], cd_education_status_node_14=[$3], cd_purchase_estimate_node_14=[$4], cd_credit_rating_node_14=[$5], cd_dep_count_node_14=[$6], cd_dep_employed_count_node_14=[$7], cd_dep_college_count_node_14=[$8])
            +- LogicalSort(sort0=[$0], dir0=[ASC])
               +- LogicalProject(cd_demo_sk_node_14=[$0], cd_gender_node_14=[$1], cd_marital_status_node_14=[$2], cd_education_status_node_14=[$3], cd_purchase_estimate_node_14=[$4], cd_credit_rating_node_14=[$5], cd_dep_count_node_14=[$6], cd_dep_employed_count_node_14=[$7], cd_dep_college_count_node_14=[$8])
                  +- LogicalTableScan(table=[[default_catalog, default_database, customer_demographics]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS d_current_year_node_11])
+- HashAggregate(isMerge=[false], groupBy=[web_city], select=[web_city, COUNT(d_current_year_node_11) AS EXPR$0])
   +- Exchange(distribution=[hash[web_city]])
      +- NestedLoopJoin(joinType=[InnerJoin], where=[=(cd_purchase_estimate_node_14, web_open_date_sk)], select=[d_date_sk_node_11, d_date_id_node_11, d_date_node_11, d_month_seq_node_11, d_week_seq_node_11, d_quarter_seq_node_11, d_year_node_11, d_dow_node_11, d_moy_node_11, d_dom_node_11, d_qoy_node_11, d_fy_year_node_11, d_fy_quarter_seq_node_11, d_fy_week_seq_node_11, d_day_name_node_11, d_quarter_name_node_11, d_holiday_node_11, d_weekend_node_11, d_following_holiday_node_11, d_first_dom_node_11, d_last_dom_node_11, d_same_day_ly_node_11, d_same_day_lq_node_11, d_current_day_node_11, d_current_week_node_11, d_current_month_node_11, d_current_quarter_node_11, d_current_year_node_11, _c28, _c29, web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage, hd_vehicle_count_node_13, 26S5K, cd_gender_node_14, cd_marital_status_node_14, cd_education_status_node_14, cd_purchase_estimate_node_14, cd_credit_rating_node_14, cd_dep_count_node_14, cd_dep_employed_count_node_14, cd_dep_college_count_node_14], build=[right])
         :- HashJoin(joinType=[InnerJoin], where=[=(d_current_month_node_11, web_state)], select=[d_date_sk_node_11, d_date_id_node_11, d_date_node_11, d_month_seq_node_11, d_week_seq_node_11, d_quarter_seq_node_11, d_year_node_11, d_dow_node_11, d_moy_node_11, d_dom_node_11, d_qoy_node_11, d_fy_year_node_11, d_fy_quarter_seq_node_11, d_fy_week_seq_node_11, d_day_name_node_11, d_quarter_name_node_11, d_holiday_node_11, d_weekend_node_11, d_following_holiday_node_11, d_first_dom_node_11, d_last_dom_node_11, d_same_day_ly_node_11, d_same_day_lq_node_11, d_current_day_node_11, d_current_week_node_11, d_current_month_node_11, d_current_quarter_node_11, d_current_year_node_11, _c28, _c29, web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage, hd_vehicle_count_node_13], isBroadcast=[true], build=[right])
         :  :- Calc(select=[d_date_sk AS d_date_sk_node_11, d_date_id AS d_date_id_node_11, d_date AS d_date_node_11, d_month_seq AS d_month_seq_node_11, d_week_seq AS d_week_seq_node_11, d_quarter_seq AS d_quarter_seq_node_11, d_year AS d_year_node_11, d_dow AS d_dow_node_11, d_moy AS d_moy_node_11, d_dom AS d_dom_node_11, d_qoy AS d_qoy_node_11, d_fy_year AS d_fy_year_node_11, d_fy_quarter_seq AS d_fy_quarter_seq_node_11, d_fy_week_seq AS d_fy_week_seq_node_11, d_day_name AS d_day_name_node_11, d_quarter_name AS d_quarter_name_node_11, d_holiday AS d_holiday_node_11, d_weekend AS d_weekend_node_11, d_following_holiday AS d_following_holiday_node_11, d_first_dom AS d_first_dom_node_11, d_last_dom AS d_last_dom_node_11, d_same_day_ly AS d_same_day_ly_node_11, d_same_day_lq AS d_same_day_lq_node_11, d_current_day AS d_current_day_node_11, d_current_week AS d_current_week_node_11, d_current_month AS d_current_month_node_11, d_current_quarter AS d_current_quarter_node_11, d_current_year AS d_current_year_node_11, 'hello' AS _c28, 'hello' AS _c29])
         :  :  +- TableSourceScan(table=[[default_catalog, default_database, date_dim]], fields=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year])
         :  +- Exchange(distribution=[broadcast])
         :     +- HashJoin(joinType=[InnerJoin], where=[=(web_open_date_sk, hd_vehicle_count_node_13)], select=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage, hd_vehicle_count_node_13], isBroadcast=[true], build=[right])
         :        :- HashAggregate(isMerge=[false], groupBy=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage], select=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage])
         :        :  +- Exchange(distribution=[hash[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage]])
         :        :     +- TableSourceScan(table=[[default_catalog, default_database, web_site]], fields=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage])
         :        +- Exchange(distribution=[broadcast])
         :           +- Calc(select=[EXPR$0 AS hd_vehicle_count_node_13])
         :              +- HashAggregate(isMerge=[true], groupBy=[hd_vehicle_count], select=[hd_vehicle_count, Final_MIN(min$0) AS EXPR$0])
         :                 +- Exchange(distribution=[hash[hd_vehicle_count]])
         :                    +- LocalHashAggregate(groupBy=[hd_vehicle_count], select=[hd_vehicle_count, Partial_MIN(hd_vehicle_count) AS min$0])
         :                       +- TableSourceScan(table=[[default_catalog, default_database, household_demographics, project=[hd_vehicle_count], metadata=[]]], fields=[hd_vehicle_count])
         +- Exchange(distribution=[broadcast])
            +- SortLimit(orderBy=[cd_dep_employed_count_node_14 ASC], offset=[0], fetch=[1], global=[true])
               +- Exchange(distribution=[single])
                  +- SortLimit(orderBy=[cd_dep_employed_count_node_14 ASC], offset=[0], fetch=[1], global=[false])
                     +- Calc(select=[cd_demo_sk AS 26S5K, cd_gender AS cd_gender_node_14, cd_marital_status AS cd_marital_status_node_14, cd_education_status AS cd_education_status_node_14, cd_purchase_estimate AS cd_purchase_estimate_node_14, cd_credit_rating AS cd_credit_rating_node_14, cd_dep_count AS cd_dep_count_node_14, cd_dep_employed_count AS cd_dep_employed_count_node_14, cd_dep_college_count AS cd_dep_college_count_node_14])
                        +- SortLimit(orderBy=[cd_demo_sk ASC], offset=[0], fetch=[1], global=[true])
                           +- Exchange(distribution=[single])
                              +- SortLimit(orderBy=[cd_demo_sk ASC], offset=[0], fetch=[1], global=[false])
                                 +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS d_current_year_node_11])
+- HashAggregate(isMerge=[false], groupBy=[web_city], select=[web_city, COUNT(d_current_year_node_11) AS EXPR$0])
   +- Exchange(distribution=[hash[web_city]])
      +- NestedLoopJoin(joinType=[InnerJoin], where=[(cd_purchase_estimate_node_14 = web_open_date_sk)], select=[d_date_sk_node_11, d_date_id_node_11, d_date_node_11, d_month_seq_node_11, d_week_seq_node_11, d_quarter_seq_node_11, d_year_node_11, d_dow_node_11, d_moy_node_11, d_dom_node_11, d_qoy_node_11, d_fy_year_node_11, d_fy_quarter_seq_node_11, d_fy_week_seq_node_11, d_day_name_node_11, d_quarter_name_node_11, d_holiday_node_11, d_weekend_node_11, d_following_holiday_node_11, d_first_dom_node_11, d_last_dom_node_11, d_same_day_ly_node_11, d_same_day_lq_node_11, d_current_day_node_11, d_current_week_node_11, d_current_month_node_11, d_current_quarter_node_11, d_current_year_node_11, _c28, _c29, web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage, hd_vehicle_count_node_13, 26S5K, cd_gender_node_14, cd_marital_status_node_14, cd_education_status_node_14, cd_purchase_estimate_node_14, cd_credit_rating_node_14, cd_dep_count_node_14, cd_dep_employed_count_node_14, cd_dep_college_count_node_14], build=[right])
         :- HashJoin(joinType=[InnerJoin], where=[(d_current_month_node_11 = web_state)], select=[d_date_sk_node_11, d_date_id_node_11, d_date_node_11, d_month_seq_node_11, d_week_seq_node_11, d_quarter_seq_node_11, d_year_node_11, d_dow_node_11, d_moy_node_11, d_dom_node_11, d_qoy_node_11, d_fy_year_node_11, d_fy_quarter_seq_node_11, d_fy_week_seq_node_11, d_day_name_node_11, d_quarter_name_node_11, d_holiday_node_11, d_weekend_node_11, d_following_holiday_node_11, d_first_dom_node_11, d_last_dom_node_11, d_same_day_ly_node_11, d_same_day_lq_node_11, d_current_day_node_11, d_current_week_node_11, d_current_month_node_11, d_current_quarter_node_11, d_current_year_node_11, _c28, _c29, web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage, hd_vehicle_count_node_13], isBroadcast=[true], build=[right])
         :  :- Calc(select=[d_date_sk AS d_date_sk_node_11, d_date_id AS d_date_id_node_11, d_date AS d_date_node_11, d_month_seq AS d_month_seq_node_11, d_week_seq AS d_week_seq_node_11, d_quarter_seq AS d_quarter_seq_node_11, d_year AS d_year_node_11, d_dow AS d_dow_node_11, d_moy AS d_moy_node_11, d_dom AS d_dom_node_11, d_qoy AS d_qoy_node_11, d_fy_year AS d_fy_year_node_11, d_fy_quarter_seq AS d_fy_quarter_seq_node_11, d_fy_week_seq AS d_fy_week_seq_node_11, d_day_name AS d_day_name_node_11, d_quarter_name AS d_quarter_name_node_11, d_holiday AS d_holiday_node_11, d_weekend AS d_weekend_node_11, d_following_holiday AS d_following_holiday_node_11, d_first_dom AS d_first_dom_node_11, d_last_dom AS d_last_dom_node_11, d_same_day_ly AS d_same_day_ly_node_11, d_same_day_lq AS d_same_day_lq_node_11, d_current_day AS d_current_day_node_11, d_current_week AS d_current_week_node_11, d_current_month AS d_current_month_node_11, d_current_quarter AS d_current_quarter_node_11, d_current_year AS d_current_year_node_11, 'hello' AS _c28, 'hello' AS _c29])
         :  :  +- TableSourceScan(table=[[default_catalog, default_database, date_dim]], fields=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year])
         :  +- Exchange(distribution=[broadcast])
         :     +- HashJoin(joinType=[InnerJoin], where=[(web_open_date_sk = hd_vehicle_count_node_13)], select=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage, hd_vehicle_count_node_13], isBroadcast=[true], build=[right])
         :        :- HashAggregate(isMerge=[false], groupBy=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage], select=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage])
         :        :  +- Exchange(distribution=[hash[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage]])
         :        :     +- TableSourceScan(table=[[default_catalog, default_database, web_site]], fields=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage])
         :        +- Exchange(distribution=[broadcast])
         :           +- Calc(select=[EXPR$0 AS hd_vehicle_count_node_13])
         :              +- HashAggregate(isMerge=[true], groupBy=[hd_vehicle_count], select=[hd_vehicle_count, Final_MIN(min$0) AS EXPR$0])
         :                 +- Exchange(distribution=[hash[hd_vehicle_count]])
         :                    +- LocalHashAggregate(groupBy=[hd_vehicle_count], select=[hd_vehicle_count, Partial_MIN(hd_vehicle_count) AS min$0])
         :                       +- TableSourceScan(table=[[default_catalog, default_database, household_demographics, project=[hd_vehicle_count], metadata=[]]], fields=[hd_vehicle_count])
         +- Exchange(distribution=[broadcast])
            +- SortLimit(orderBy=[cd_dep_employed_count_node_14 ASC], offset=[0], fetch=[1], global=[true])
               +- Exchange(distribution=[single])
                  +- SortLimit(orderBy=[cd_dep_employed_count_node_14 ASC], offset=[0], fetch=[1], global=[false])
                     +- Calc(select=[cd_demo_sk AS 26S5K, cd_gender AS cd_gender_node_14, cd_marital_status AS cd_marital_status_node_14, cd_education_status AS cd_education_status_node_14, cd_purchase_estimate AS cd_purchase_estimate_node_14, cd_credit_rating AS cd_credit_rating_node_14, cd_dep_count AS cd_dep_count_node_14, cd_dep_employed_count AS cd_dep_employed_count_node_14, cd_dep_college_count AS cd_dep_college_count_node_14])
                        +- SortLimit(orderBy=[cd_demo_sk ASC], offset=[0], fetch=[1], global=[true])
                           +- Exchange(distribution=[single])
                              +- SortLimit(orderBy=[cd_demo_sk ASC], offset=[0], fetch=[1], global=[false])
                                 +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0