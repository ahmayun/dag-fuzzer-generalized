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

autonode_10 = table_env.from_path("time_dim").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("time_dim").get_schema().get_field_names()])
autonode_9 = table_env.from_path("customer_demographics").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("customer_demographics").get_schema().get_field_names()])
autonode_12 = table_env.from_path("date_dim").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("date_dim").get_schema().get_field_names()])
autonode_11 = table_env.from_path("promotion").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("promotion").get_schema().get_field_names()])
autonode_6 = autonode_9.join(autonode_10, col('t_time_node_10') == col('cd_dep_college_count_node_9'))
autonode_8 = autonode_12.order_by(col('d_same_day_lq_node_12'))
autonode_7 = autonode_11.filter(col('p_channel_catalog_node_11').char_length >= 5)
autonode_5 = autonode_8.filter(preloaded_udf_boolean(col('d_day_name_node_12')))
autonode_4 = autonode_6.join(autonode_7, col('t_am_pm_node_10') == col('p_channel_demo_node_11'))
autonode_3 = autonode_5.alias('rVooX')
autonode_2 = autonode_4.select(col('cd_education_status_node_9'))
autonode_1 = autonode_2.join(autonode_3, col('cd_education_status_node_9') == col('d_current_week_node_12'))
sink = autonode_1.group_by(col('d_first_dom_node_12')).select(col('d_fy_quarter_seq_node_12').max.alias('d_fy_quarter_seq_node_12'))
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
LogicalProject(d_fy_quarter_seq_node_12=[$1])
+- LogicalAggregate(group=[{20}], EXPR$0=[MAX($13)])
   +- LogicalJoin(condition=[=($0, $25)], joinType=[inner])
      :- LogicalProject(cd_education_status_node_9=[$3])
      :  +- LogicalJoin(condition=[=($15, $34)], joinType=[inner])
      :     :- LogicalJoin(condition=[=($11, $8)], joinType=[inner])
      :     :  :- LogicalProject(cd_demo_sk_node_9=[$0], cd_gender_node_9=[$1], cd_marital_status_node_9=[$2], cd_education_status_node_9=[$3], cd_purchase_estimate_node_9=[$4], cd_credit_rating_node_9=[$5], cd_dep_count_node_9=[$6], cd_dep_employed_count_node_9=[$7], cd_dep_college_count_node_9=[$8])
      :     :  :  +- LogicalTableScan(table=[[default_catalog, default_database, customer_demographics]])
      :     :  +- LogicalProject(t_time_sk_node_10=[$0], t_time_id_node_10=[$1], t_time_node_10=[$2], t_hour_node_10=[$3], t_minute_node_10=[$4], t_second_node_10=[$5], t_am_pm_node_10=[$6], t_shift_node_10=[$7], t_sub_shift_node_10=[$8], t_meal_time_node_10=[$9])
      :     :     +- LogicalTableScan(table=[[default_catalog, default_database, time_dim]])
      :     +- LogicalFilter(condition=[>=(CHAR_LENGTH($10), 5)])
      :        +- LogicalProject(p_promo_sk_node_11=[$0], p_promo_id_node_11=[$1], p_start_date_sk_node_11=[$2], p_end_date_sk_node_11=[$3], p_item_sk_node_11=[$4], p_cost_node_11=[$5], p_response_target_node_11=[$6], p_promo_name_node_11=[$7], p_channel_dmail_node_11=[$8], p_channel_email_node_11=[$9], p_channel_catalog_node_11=[$10], p_channel_tv_node_11=[$11], p_channel_radio_node_11=[$12], p_channel_press_node_11=[$13], p_channel_event_node_11=[$14], p_channel_demo_node_11=[$15], p_channel_details_node_11=[$16], p_purpose_node_11=[$17], p_discount_active_node_11=[$18])
      :           +- LogicalTableScan(table=[[default_catalog, default_database, promotion]])
      +- LogicalProject(rVooX=[AS($0, _UTF-16LE'rVooX')], d_date_id_node_12=[$1], d_date_node_12=[$2], d_month_seq_node_12=[$3], d_week_seq_node_12=[$4], d_quarter_seq_node_12=[$5], d_year_node_12=[$6], d_dow_node_12=[$7], d_moy_node_12=[$8], d_dom_node_12=[$9], d_qoy_node_12=[$10], d_fy_year_node_12=[$11], d_fy_quarter_seq_node_12=[$12], d_fy_week_seq_node_12=[$13], d_day_name_node_12=[$14], d_quarter_name_node_12=[$15], d_holiday_node_12=[$16], d_weekend_node_12=[$17], d_following_holiday_node_12=[$18], d_first_dom_node_12=[$19], d_last_dom_node_12=[$20], d_same_day_ly_node_12=[$21], d_same_day_lq_node_12=[$22], d_current_day_node_12=[$23], d_current_week_node_12=[$24], d_current_month_node_12=[$25], d_current_quarter_node_12=[$26], d_current_year_node_12=[$27])
         +- LogicalFilter(condition=[*org.apache.flink.table.functions.python.PythonScalarFunction$2719bb62ba73d6c77acabda93ef50d3e*($14)])
            +- LogicalSort(sort0=[$22], dir0=[ASC])
               +- LogicalProject(d_date_sk_node_12=[$0], d_date_id_node_12=[$1], d_date_node_12=[$2], d_month_seq_node_12=[$3], d_week_seq_node_12=[$4], d_quarter_seq_node_12=[$5], d_year_node_12=[$6], d_dow_node_12=[$7], d_moy_node_12=[$8], d_dom_node_12=[$9], d_qoy_node_12=[$10], d_fy_year_node_12=[$11], d_fy_quarter_seq_node_12=[$12], d_fy_week_seq_node_12=[$13], d_day_name_node_12=[$14], d_quarter_name_node_12=[$15], d_holiday_node_12=[$16], d_weekend_node_12=[$17], d_following_holiday_node_12=[$18], d_first_dom_node_12=[$19], d_last_dom_node_12=[$20], d_same_day_ly_node_12=[$21], d_same_day_lq_node_12=[$22], d_current_day_node_12=[$23], d_current_week_node_12=[$24], d_current_month_node_12=[$25], d_current_quarter_node_12=[$26], d_current_year_node_12=[$27])
                  +- LogicalTableScan(table=[[default_catalog, default_database, date_dim]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS d_fy_quarter_seq_node_12])
+- HashAggregate(isMerge=[true], groupBy=[d_first_dom_node_12], select=[d_first_dom_node_12, Final_MAX(max$0) AS EXPR$0])
   +- Exchange(distribution=[hash[d_first_dom_node_12]])
      +- LocalHashAggregate(groupBy=[d_first_dom_node_12], select=[d_first_dom_node_12, Partial_MAX(d_fy_quarter_seq_node_12) AS max$0])
         +- HashJoin(joinType=[InnerJoin], where=[=(cd_education_status_node_9, d_current_week_node_12)], select=[cd_education_status_node_9, rVooX, d_date_id_node_12, d_date_node_12, d_month_seq_node_12, d_week_seq_node_12, d_quarter_seq_node_12, d_year_node_12, d_dow_node_12, d_moy_node_12, d_dom_node_12, d_qoy_node_12, d_fy_year_node_12, d_fy_quarter_seq_node_12, d_fy_week_seq_node_12, d_day_name_node_12, d_quarter_name_node_12, d_holiday_node_12, d_weekend_node_12, d_following_holiday_node_12, d_first_dom_node_12, d_last_dom_node_12, d_same_day_ly_node_12, d_same_day_lq_node_12, d_current_day_node_12, d_current_week_node_12, d_current_month_node_12, d_current_quarter_node_12, d_current_year_node_12], build=[left])
            :- Exchange(distribution=[hash[cd_education_status_node_9]])
            :  +- Calc(select=[cd_education_status_node_9])
            :     +- HashJoin(joinType=[InnerJoin], where=[=(t_am_pm_node_10, p_channel_demo)], select=[cd_education_status_node_9, t_am_pm_node_10, p_channel_demo], isBroadcast=[true], build=[right])
            :        :- Calc(select=[cd_education_status AS cd_education_status_node_9, t_am_pm AS t_am_pm_node_10])
            :        :  +- HashJoin(joinType=[InnerJoin], where=[=(t_time, cd_dep_college_count)], select=[cd_education_status, cd_dep_college_count, t_time, t_am_pm], build=[right])
            :        :     :- Exchange(distribution=[hash[cd_dep_college_count]])
            :        :     :  +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics, project=[cd_education_status, cd_dep_college_count], metadata=[]]], fields=[cd_education_status, cd_dep_college_count])
            :        :     +- Exchange(distribution=[hash[t_time]])
            :        :        +- TableSourceScan(table=[[default_catalog, default_database, time_dim, project=[t_time, t_am_pm], metadata=[]]], fields=[t_time, t_am_pm])
            :        +- Exchange(distribution=[broadcast])
            :           +- Calc(select=[p_channel_demo], where=[>=(CHAR_LENGTH(p_channel_catalog), 5)])
            :              +- TableSourceScan(table=[[default_catalog, default_database, promotion, filter=[>=(CHAR_LENGTH(p_channel_catalog), 5)], project=[p_channel_catalog, p_channel_demo], metadata=[]]], fields=[p_channel_catalog, p_channel_demo])
            +- Exchange(distribution=[hash[d_current_week_node_12]])
               +- Calc(select=[d_date_sk AS rVooX, d_date_id AS d_date_id_node_12, d_date AS d_date_node_12, d_month_seq AS d_month_seq_node_12, d_week_seq AS d_week_seq_node_12, d_quarter_seq AS d_quarter_seq_node_12, d_year AS d_year_node_12, d_dow AS d_dow_node_12, d_moy AS d_moy_node_12, d_dom AS d_dom_node_12, d_qoy AS d_qoy_node_12, d_fy_year AS d_fy_year_node_12, d_fy_quarter_seq AS d_fy_quarter_seq_node_12, d_fy_week_seq AS d_fy_week_seq_node_12, d_day_name AS d_day_name_node_12, d_quarter_name AS d_quarter_name_node_12, d_holiday AS d_holiday_node_12, d_weekend AS d_weekend_node_12, d_following_holiday AS d_following_holiday_node_12, d_first_dom AS d_first_dom_node_12, d_last_dom AS d_last_dom_node_12, d_same_day_ly AS d_same_day_ly_node_12, d_same_day_lq AS d_same_day_lq_node_12, d_current_day AS d_current_day_node_12, d_current_week AS d_current_week_node_12, d_current_month AS d_current_month_node_12, d_current_quarter AS d_current_quarter_node_12, d_current_year AS d_current_year_node_12], where=[f0])
                  +- PythonCalc(select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, *org.apache.flink.table.functions.python.PythonScalarFunction$2719bb62ba73d6c77acabda93ef50d3e*(d_day_name) AS f0])
                     +- Sort(orderBy=[d_same_day_lq ASC])
                        +- Exchange(distribution=[single])
                           +- TableSourceScan(table=[[default_catalog, default_database, date_dim]], fields=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS d_fy_quarter_seq_node_12])
+- HashAggregate(isMerge=[true], groupBy=[d_first_dom_node_12], select=[d_first_dom_node_12, Final_MAX(max$0) AS EXPR$0])
   +- Exchange(distribution=[hash[d_first_dom_node_12]])
      +- LocalHashAggregate(groupBy=[d_first_dom_node_12], select=[d_first_dom_node_12, Partial_MAX(d_fy_quarter_seq_node_12) AS max$0])
         +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(cd_education_status_node_9 = d_current_week_node_12)], select=[cd_education_status_node_9, rVooX, d_date_id_node_12, d_date_node_12, d_month_seq_node_12, d_week_seq_node_12, d_quarter_seq_node_12, d_year_node_12, d_dow_node_12, d_moy_node_12, d_dom_node_12, d_qoy_node_12, d_fy_year_node_12, d_fy_quarter_seq_node_12, d_fy_week_seq_node_12, d_day_name_node_12, d_quarter_name_node_12, d_holiday_node_12, d_weekend_node_12, d_following_holiday_node_12, d_first_dom_node_12, d_last_dom_node_12, d_same_day_ly_node_12, d_same_day_lq_node_12, d_current_day_node_12, d_current_week_node_12, d_current_month_node_12, d_current_quarter_node_12, d_current_year_node_12], build=[left])
            :- Exchange(distribution=[hash[cd_education_status_node_9]])
            :  +- Calc(select=[cd_education_status_node_9])
            :     +- MultipleInput(readOrder=[0,1,0], members=[\
HashJoin(joinType=[InnerJoin], where=[(t_am_pm_node_10 = p_channel_demo)], select=[cd_education_status_node_9, t_am_pm_node_10, p_channel_demo], isBroadcast=[true], build=[right])\
:- Calc(select=[cd_education_status AS cd_education_status_node_9, t_am_pm AS t_am_pm_node_10])\
:  +- HashJoin(joinType=[InnerJoin], where=[(t_time = cd_dep_college_count)], select=[cd_education_status, cd_dep_college_count, t_time, t_am_pm], build=[right])\
:     :- [#2] Exchange(distribution=[hash[cd_dep_college_count]])\
:     +- [#3] Exchange(distribution=[hash[t_time]])\
+- [#1] Exchange(distribution=[broadcast])\
])
            :        :- Exchange(distribution=[broadcast])
            :        :  +- Calc(select=[p_channel_demo], where=[(CHAR_LENGTH(p_channel_catalog) >= 5)])
            :        :     +- TableSourceScan(table=[[default_catalog, default_database, promotion, filter=[>=(CHAR_LENGTH(p_channel_catalog), 5)], project=[p_channel_catalog, p_channel_demo], metadata=[]]], fields=[p_channel_catalog, p_channel_demo])
            :        :- Exchange(distribution=[hash[cd_dep_college_count]])
            :        :  +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics, project=[cd_education_status, cd_dep_college_count], metadata=[]]], fields=[cd_education_status, cd_dep_college_count])
            :        +- Exchange(distribution=[hash[t_time]])
            :           +- TableSourceScan(table=[[default_catalog, default_database, time_dim, project=[t_time, t_am_pm], metadata=[]]], fields=[t_time, t_am_pm])
            +- Exchange(distribution=[hash[d_current_week_node_12]])
               +- Calc(select=[d_date_sk AS rVooX, d_date_id AS d_date_id_node_12, d_date AS d_date_node_12, d_month_seq AS d_month_seq_node_12, d_week_seq AS d_week_seq_node_12, d_quarter_seq AS d_quarter_seq_node_12, d_year AS d_year_node_12, d_dow AS d_dow_node_12, d_moy AS d_moy_node_12, d_dom AS d_dom_node_12, d_qoy AS d_qoy_node_12, d_fy_year AS d_fy_year_node_12, d_fy_quarter_seq AS d_fy_quarter_seq_node_12, d_fy_week_seq AS d_fy_week_seq_node_12, d_day_name AS d_day_name_node_12, d_quarter_name AS d_quarter_name_node_12, d_holiday AS d_holiday_node_12, d_weekend AS d_weekend_node_12, d_following_holiday AS d_following_holiday_node_12, d_first_dom AS d_first_dom_node_12, d_last_dom AS d_last_dom_node_12, d_same_day_ly AS d_same_day_ly_node_12, d_same_day_lq AS d_same_day_lq_node_12, d_current_day AS d_current_day_node_12, d_current_week AS d_current_week_node_12, d_current_month AS d_current_month_node_12, d_current_quarter AS d_current_quarter_node_12, d_current_year AS d_current_year_node_12], where=[f0])
                  +- PythonCalc(select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, *org.apache.flink.table.functions.python.PythonScalarFunction$2719bb62ba73d6c77acabda93ef50d3e*(d_day_name) AS f0])
                     +- Sort(orderBy=[d_same_day_lq ASC])
                        +- Exchange(distribution=[single])
                           +- TableSourceScan(table=[[default_catalog, default_database, date_dim]], fields=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o306944412.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#618948549:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[22](input=RelSubset#618948547,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[22]), rel#618948546:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[22](input=RelSubset#618948545,groupBy=d_first_dom, d_current_week,select=d_first_dom, d_current_week, Partial_MAX(d_fy_quarter_seq) AS max$0)]
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