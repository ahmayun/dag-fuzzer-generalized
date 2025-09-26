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
    return values.product()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_10 = table_env.from_path("customer_demographics").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("customer_demographics").get_schema().get_field_names()])
autonode_9 = table_env.from_path("store_returns").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("store_returns").get_schema().get_field_names()])
autonode_8 = table_env.from_path("date_dim").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("date_dim").get_schema().get_field_names()])
autonode_11 = table_env.from_path("warehouse").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("warehouse").get_schema().get_field_names()])
autonode_6 = autonode_8.join(autonode_9, col('sr_addr_sk_node_9') == col('d_year_node_8'))
autonode_7 = autonode_10.join(autonode_11, col('w_warehouse_sk_node_11') == col('cd_dep_count_node_10'))
autonode_4 = autonode_6.select(col('d_fy_week_seq_node_8'))
autonode_5 = autonode_7.distinct()
autonode_2 = autonode_4.select(col('d_fy_week_seq_node_8'))
autonode_3 = autonode_5.order_by(col('w_country_node_11'))
autonode_1 = autonode_2.join(autonode_3, col('d_fy_week_seq_node_8') == col('w_warehouse_sq_ft_node_11'))
sink = autonode_1.group_by(col('cd_gender_node_10')).select(col('cd_dep_college_count_node_10').min.alias('cd_dep_college_count_node_10'))
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
LogicalProject(cd_dep_college_count_node_10=[$1])
+- LogicalAggregate(group=[{2}], EXPR$0=[MIN($9)])
   +- LogicalJoin(condition=[=($0, $13)], joinType=[inner])
      :- LogicalProject(d_fy_week_seq_node_8=[$13])
      :  +- LogicalJoin(condition=[=($34, $6)], joinType=[inner])
      :     :- LogicalProject(d_date_sk_node_8=[$0], d_date_id_node_8=[$1], d_date_node_8=[$2], d_month_seq_node_8=[$3], d_week_seq_node_8=[$4], d_quarter_seq_node_8=[$5], d_year_node_8=[$6], d_dow_node_8=[$7], d_moy_node_8=[$8], d_dom_node_8=[$9], d_qoy_node_8=[$10], d_fy_year_node_8=[$11], d_fy_quarter_seq_node_8=[$12], d_fy_week_seq_node_8=[$13], d_day_name_node_8=[$14], d_quarter_name_node_8=[$15], d_holiday_node_8=[$16], d_weekend_node_8=[$17], d_following_holiday_node_8=[$18], d_first_dom_node_8=[$19], d_last_dom_node_8=[$20], d_same_day_ly_node_8=[$21], d_same_day_lq_node_8=[$22], d_current_day_node_8=[$23], d_current_week_node_8=[$24], d_current_month_node_8=[$25], d_current_quarter_node_8=[$26], d_current_year_node_8=[$27])
      :     :  +- LogicalTableScan(table=[[default_catalog, default_database, date_dim]])
      :     +- LogicalProject(sr_returned_date_sk_node_9=[$0], sr_return_time_sk_node_9=[$1], sr_item_sk_node_9=[$2], sr_customer_sk_node_9=[$3], sr_cdemo_sk_node_9=[$4], sr_hdemo_sk_node_9=[$5], sr_addr_sk_node_9=[$6], sr_store_sk_node_9=[$7], sr_reason_sk_node_9=[$8], sr_ticket_number_node_9=[$9], sr_return_quantity_node_9=[$10], sr_return_amt_node_9=[$11], sr_return_tax_node_9=[$12], sr_return_amt_inc_tax_node_9=[$13], sr_fee_node_9=[$14], sr_return_ship_cost_node_9=[$15], sr_refunded_cash_node_9=[$16], sr_reversed_charge_node_9=[$17], sr_store_credit_node_9=[$18], sr_net_loss_node_9=[$19])
      :        +- LogicalTableScan(table=[[default_catalog, default_database, store_returns]])
      +- LogicalSort(sort0=[$21], dir0=[ASC])
         +- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22}])
            +- LogicalJoin(condition=[=($9, $6)], joinType=[inner])
               :- LogicalProject(cd_demo_sk_node_10=[$0], cd_gender_node_10=[$1], cd_marital_status_node_10=[$2], cd_education_status_node_10=[$3], cd_purchase_estimate_node_10=[$4], cd_credit_rating_node_10=[$5], cd_dep_count_node_10=[$6], cd_dep_employed_count_node_10=[$7], cd_dep_college_count_node_10=[$8])
               :  +- LogicalTableScan(table=[[default_catalog, default_database, customer_demographics]])
               +- LogicalProject(w_warehouse_sk_node_11=[$0], w_warehouse_id_node_11=[$1], w_warehouse_name_node_11=[$2], w_warehouse_sq_ft_node_11=[$3], w_street_number_node_11=[$4], w_street_name_node_11=[$5], w_street_type_node_11=[$6], w_suite_number_node_11=[$7], w_city_node_11=[$8], w_county_node_11=[$9], w_state_node_11=[$10], w_zip_node_11=[$11], w_country_node_11=[$12], w_gmt_offset_node_11=[$13])
                  +- LogicalTableScan(table=[[default_catalog, default_database, warehouse]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS cd_dep_college_count_node_10])
+- HashAggregate(isMerge=[true], groupBy=[cd_gender], select=[cd_gender, Final_MIN(min$0) AS EXPR$0])
   +- Exchange(distribution=[hash[cd_gender]])
      +- LocalHashAggregate(groupBy=[cd_gender], select=[cd_gender, Partial_MIN(cd_dep_college_count) AS min$0])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[=(d_fy_week_seq_node_8, w_warehouse_sq_ft)], select=[d_fy_week_seq_node_8, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset], build=[left])
            :- Exchange(distribution=[broadcast])
            :  +- Calc(select=[d_fy_week_seq AS d_fy_week_seq_node_8])
            :     +- HashJoin(joinType=[InnerJoin], where=[=(sr_addr_sk, d_year)], select=[d_year, d_fy_week_seq, sr_addr_sk], build=[left])
            :        :- Exchange(distribution=[hash[d_year]])
            :        :  +- TableSourceScan(table=[[default_catalog, default_database, date_dim, project=[d_year, d_fy_week_seq], metadata=[]]], fields=[d_year, d_fy_week_seq])
            :        +- Exchange(distribution=[hash[sr_addr_sk]])
            :           +- TableSourceScan(table=[[default_catalog, default_database, store_returns, project=[sr_addr_sk], metadata=[]]], fields=[sr_addr_sk])
            +- Sort(orderBy=[w_country ASC])
               +- Exchange(distribution=[single])
                  +- HashAggregate(isMerge=[false], groupBy=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset], select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset])
                     +- Exchange(distribution=[hash[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset]])
                        +- NestedLoopJoin(joinType=[InnerJoin], where=[=(w_warehouse_sk, cd_dep_count)], select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset], build=[right])
                           :- TableSourceScan(table=[[default_catalog, default_database, customer_demographics]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])
                           +- Exchange(distribution=[broadcast])
                              +- TableSourceScan(table=[[default_catalog, default_database, warehouse]], fields=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS cd_dep_college_count_node_10])
+- HashAggregate(isMerge=[true], groupBy=[cd_gender], select=[cd_gender, Final_MIN(min$0) AS EXPR$0])
   +- Exchange(distribution=[hash[cd_gender]])
      +- LocalHashAggregate(groupBy=[cd_gender], select=[cd_gender, Partial_MIN(cd_dep_college_count) AS min$0])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[(d_fy_week_seq_node_8 = w_warehouse_sq_ft)], select=[d_fy_week_seq_node_8, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset], build=[left])
            :- Exchange(distribution=[broadcast])
            :  +- Calc(select=[d_fy_week_seq AS d_fy_week_seq_node_8])
            :     +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(sr_addr_sk = d_year)], select=[d_year, d_fy_week_seq, sr_addr_sk], build=[left])
            :        :- Exchange(distribution=[hash[d_year]])
            :        :  +- TableSourceScan(table=[[default_catalog, default_database, date_dim, project=[d_year, d_fy_week_seq], metadata=[]]], fields=[d_year, d_fy_week_seq])
            :        +- Exchange(distribution=[hash[sr_addr_sk]])
            :           +- TableSourceScan(table=[[default_catalog, default_database, store_returns, project=[sr_addr_sk], metadata=[]]], fields=[sr_addr_sk])
            +- Sort(orderBy=[w_country ASC])
               +- Exchange(distribution=[single])
                  +- HashAggregate(isMerge=[false], groupBy=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset], select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset])
                     +- Exchange(distribution=[hash[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset]])
                        +- MultipleInput(readOrder=[1,0], members=[\
NestedLoopJoin(joinType=[InnerJoin], where=[(w_warehouse_sk = cd_dep_count)], select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset], build=[right])\
:- [#1] TableSourceScan(table=[[default_catalog, default_database, customer_demographics]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])\
+- [#2] Exchange(distribution=[broadcast])\
])
                           :- TableSourceScan(table=[[default_catalog, default_database, customer_demographics]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])
                           +- Exchange(distribution=[broadcast])
                              +- TableSourceScan(table=[[default_catalog, default_database, warehouse]], fields=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o137763828.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#278318737:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[21](input=RelSubset#278318735,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[21]), rel#278318734:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[21](input=RelSubset#278318733,groupBy=cd_gender, w_warehouse_sq_ft,select=cd_gender, w_warehouse_sq_ft, Partial_MIN(cd_dep_college_count) AS min$0)]
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