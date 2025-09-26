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
    return values.max() - values.min()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_10 = table_env.from_path("warehouse").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("warehouse").get_schema().get_field_names()])
autonode_9 = table_env.from_path("customer_address").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("customer_address").get_schema().get_field_names()])
autonode_12 = table_env.from_path("customer").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("customer").get_schema().get_field_names()])
autonode_11 = table_env.from_path("customer_demographics").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("customer_demographics").get_schema().get_field_names()])
autonode_7 = autonode_9.join(autonode_10, col('ca_address_sk_node_9') == col('w_warehouse_sq_ft_node_10'))
autonode_8 = autonode_11.join(autonode_12, col('c_last_review_date_node_12') == col('cd_education_status_node_11'))
autonode_5 = autonode_7.group_by(col('ca_gmt_offset_node_9')).select(col('w_street_type_node_10').min.alias('w_street_type_node_10'))
autonode_6 = autonode_8.distinct()
autonode_3 = autonode_5.select(col('w_street_type_node_10'))
autonode_4 = autonode_6.order_by(col('c_last_name_node_12'))
autonode_2 = autonode_3.join(autonode_4, col('c_first_name_node_12') == col('w_street_type_node_10'))
autonode_1 = autonode_2.group_by(col('c_login_node_12')).select(col('c_birth_month_node_12').max.alias('c_birth_month_node_12'))
sink = autonode_1.order_by(col('c_birth_month_node_12'))
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
LogicalSort(sort0=[$0], dir0=[ASC])
+- LogicalProject(c_birth_month_node_12=[$1])
   +- LogicalAggregate(group=[{25}], EXPR$0=[MAX($22)])
      +- LogicalJoin(condition=[=($18, $0)], joinType=[inner])
         :- LogicalProject(w_street_type_node_10=[$1])
         :  +- LogicalAggregate(group=[{11}], EXPR$0=[MIN($19)])
         :     +- LogicalJoin(condition=[=($0, $16)], joinType=[inner])
         :        :- LogicalProject(ca_address_sk_node_9=[$0], ca_address_id_node_9=[$1], ca_street_number_node_9=[$2], ca_street_name_node_9=[$3], ca_street_type_node_9=[$4], ca_suite_number_node_9=[$5], ca_city_node_9=[$6], ca_county_node_9=[$7], ca_state_node_9=[$8], ca_zip_node_9=[$9], ca_country_node_9=[$10], ca_gmt_offset_node_9=[$11], ca_location_type_node_9=[$12])
         :        :  +- LogicalTableScan(table=[[default_catalog, default_database, customer_address]])
         :        +- LogicalProject(w_warehouse_sk_node_10=[$0], w_warehouse_id_node_10=[$1], w_warehouse_name_node_10=[$2], w_warehouse_sq_ft_node_10=[$3], w_street_number_node_10=[$4], w_street_name_node_10=[$5], w_street_type_node_10=[$6], w_suite_number_node_10=[$7], w_city_node_10=[$8], w_county_node_10=[$9], w_state_node_10=[$10], w_zip_node_10=[$11], w_country_node_10=[$12], w_gmt_offset_node_10=[$13])
         :           +- LogicalTableScan(table=[[default_catalog, default_database, warehouse]])
         +- LogicalSort(sort0=[$18], dir0=[ASC])
            +- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26}])
               +- LogicalJoin(condition=[=($26, $3)], joinType=[inner])
                  :- LogicalProject(cd_demo_sk_node_11=[$0], cd_gender_node_11=[$1], cd_marital_status_node_11=[$2], cd_education_status_node_11=[$3], cd_purchase_estimate_node_11=[$4], cd_credit_rating_node_11=[$5], cd_dep_count_node_11=[$6], cd_dep_employed_count_node_11=[$7], cd_dep_college_count_node_11=[$8])
                  :  +- LogicalTableScan(table=[[default_catalog, default_database, customer_demographics]])
                  +- LogicalProject(c_customer_sk_node_12=[$0], c_customer_id_node_12=[$1], c_current_cdemo_sk_node_12=[$2], c_current_hdemo_sk_node_12=[$3], c_current_addr_sk_node_12=[$4], c_first_shipto_date_sk_node_12=[$5], c_first_sales_date_sk_node_12=[$6], c_salutation_node_12=[$7], c_first_name_node_12=[$8], c_last_name_node_12=[$9], c_preferred_cust_flag_node_12=[$10], c_birth_day_node_12=[$11], c_birth_month_node_12=[$12], c_birth_year_node_12=[$13], c_birth_country_node_12=[$14], c_login_node_12=[$15], c_email_address_node_12=[$16], c_last_review_date_node_12=[$17])
                     +- LogicalTableScan(table=[[default_catalog, default_database, customer]])

== Optimized Physical Plan ==
Sort(orderBy=[c_birth_month_node_12 ASC])
+- Exchange(distribution=[single])
   +- Calc(select=[EXPR$0 AS c_birth_month_node_12])
      +- HashAggregate(isMerge=[false], groupBy=[c_login], select=[c_login, MAX(c_birth_month) AS EXPR$0])
         +- Exchange(distribution=[hash[c_login]])
            +- HashJoin(joinType=[InnerJoin], where=[=(c_first_name, w_street_type_node_10)], select=[w_street_type_node_10, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date], isBroadcast=[true], build=[left])
               :- Exchange(distribution=[broadcast])
               :  +- Calc(select=[EXPR$0 AS w_street_type_node_10])
               :     +- SortAggregate(isMerge=[true], groupBy=[ca_gmt_offset], select=[ca_gmt_offset, Final_MIN(min$0) AS EXPR$0])
               :        +- Sort(orderBy=[ca_gmt_offset ASC])
               :           +- Exchange(distribution=[hash[ca_gmt_offset]])
               :              +- LocalSortAggregate(groupBy=[ca_gmt_offset], select=[ca_gmt_offset, Partial_MIN(w_street_type) AS min$0])
               :                 +- Sort(orderBy=[ca_gmt_offset ASC])
               :                    +- NestedLoopJoin(joinType=[InnerJoin], where=[=(ca_address_sk, w_warehouse_sq_ft)], select=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type, w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset], build=[right])
               :                       :- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])
               :                       +- Exchange(distribution=[broadcast])
               :                          +- TableSourceScan(table=[[default_catalog, default_database, warehouse]], fields=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset])
               +- Sort(orderBy=[c_last_name ASC])
                  +- Exchange(distribution=[single])
                     +- HashAggregate(isMerge=[false], groupBy=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date], select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])
                        +- HashJoin(joinType=[InnerJoin], where=[=(c_last_review_date, cd_education_status)], select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date], build=[right])
                           :- Exchange(distribution=[hash[cd_education_status]])
                           :  +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])
                           +- Exchange(distribution=[hash[c_last_review_date]])
                              +- TableSourceScan(table=[[default_catalog, default_database, customer]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])

== Optimized Execution Plan ==
Sort(orderBy=[c_birth_month_node_12 ASC])
+- Exchange(distribution=[single])
   +- Calc(select=[EXPR$0 AS c_birth_month_node_12])
      +- HashAggregate(isMerge=[false], groupBy=[c_login], select=[c_login, MAX(c_birth_month) AS EXPR$0])
         +- Exchange(distribution=[hash[c_login]])
            +- HashJoin(joinType=[InnerJoin], where=[(c_first_name = w_street_type_node_10)], select=[w_street_type_node_10, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date], isBroadcast=[true], build=[left])
               :- Exchange(distribution=[broadcast])
               :  +- Calc(select=[EXPR$0 AS w_street_type_node_10])
               :     +- SortAggregate(isMerge=[true], groupBy=[ca_gmt_offset], select=[ca_gmt_offset, Final_MIN(min$0) AS EXPR$0])
               :        +- Exchange(distribution=[forward])
               :           +- Sort(orderBy=[ca_gmt_offset ASC])
               :              +- Exchange(distribution=[hash[ca_gmt_offset]])
               :                 +- LocalSortAggregate(groupBy=[ca_gmt_offset], select=[ca_gmt_offset, Partial_MIN(w_street_type) AS min$0])
               :                    +- Exchange(distribution=[forward])
               :                       +- Sort(orderBy=[ca_gmt_offset ASC])
               :                          +- MultipleInput(readOrder=[1,0], members=[\
NestedLoopJoin(joinType=[InnerJoin], where=[(ca_address_sk = w_warehouse_sq_ft)], select=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type, w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset], build=[right])\
:- [#1] TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])\
+- [#2] Exchange(distribution=[broadcast])\
])
               :                             :- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])
               :                             +- Exchange(distribution=[broadcast])
               :                                +- TableSourceScan(table=[[default_catalog, default_database, warehouse]], fields=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset])
               +- Sort(orderBy=[c_last_name ASC])
                  +- Exchange(distribution=[single])
                     +- HashAggregate(isMerge=[false], groupBy=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date], select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])
                        +- Exchange(distribution=[keep_input_as_is[hash[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date]]])
                           +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(c_last_review_date = cd_education_status)], select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date], build=[right])
                              :- Exchange(distribution=[hash[cd_education_status]])
                              :  +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])
                              +- Exchange(distribution=[hash[c_last_review_date]])
                                 +- TableSourceScan(table=[[default_catalog, default_database, customer]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o1340122.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#2654332:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[18](input=RelSubset#2654330,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[18]), rel#2654329:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[18](input=RelSubset#2654328,groupBy=c_first_name, c_login,select=c_first_name, c_login, Partial_MAX(c_birth_month) AS max$0)]
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