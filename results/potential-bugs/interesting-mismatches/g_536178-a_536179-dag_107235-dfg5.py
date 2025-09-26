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

autonode_10 = table_env.from_path("web_sales").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("web_sales").get_schema().get_field_names()])
autonode_9 = table_env.from_path("customer").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("customer").get_schema().get_field_names()])
autonode_12 = table_env.from_path("customer_demographics").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("customer_demographics").get_schema().get_field_names()])
autonode_11 = table_env.from_path("customer").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("customer").get_schema().get_field_names()])
autonode_7 = autonode_10.group_by(col('ws_warehouse_sk_node_10')).select(col('ws_ship_mode_sk_node_10').count.alias('ws_ship_mode_sk_node_10'))
autonode_6 = autonode_9.limit(53)
autonode_8 = autonode_11.join(autonode_12, col('c_email_address_node_11') == col('cd_credit_rating_node_12'))
autonode_4 = autonode_6.join(autonode_7, col('c_first_shipto_date_sk_node_9') == col('ws_ship_mode_sk_node_10'))
autonode_5 = autonode_8.order_by(col('cd_gender_node_12'))
autonode_3 = autonode_4.join(autonode_5, col('c_login_node_11') == col('c_birth_country_node_9'))
autonode_2 = autonode_3.group_by(col('c_last_name_node_11')).select(col('c_customer_id_node_9').min.alias('c_customer_id_node_9'))
autonode_1 = autonode_2.distinct()
sink = autonode_1.distinct()
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
LogicalAggregate(group=[{0}])
+- LogicalProject(c_customer_id_node_9=[$1])
   +- LogicalAggregate(group=[{28}], EXPR$0=[MIN($1)])
      +- LogicalJoin(condition=[=($34, $14)], joinType=[inner])
         :- LogicalJoin(condition=[=($5, $18)], joinType=[inner])
         :  :- LogicalSort(fetch=[53])
         :  :  +- LogicalProject(c_customer_sk_node_9=[$0], c_customer_id_node_9=[$1], c_current_cdemo_sk_node_9=[$2], c_current_hdemo_sk_node_9=[$3], c_current_addr_sk_node_9=[$4], c_first_shipto_date_sk_node_9=[$5], c_first_sales_date_sk_node_9=[$6], c_salutation_node_9=[$7], c_first_name_node_9=[$8], c_last_name_node_9=[$9], c_preferred_cust_flag_node_9=[$10], c_birth_day_node_9=[$11], c_birth_month_node_9=[$12], c_birth_year_node_9=[$13], c_birth_country_node_9=[$14], c_login_node_9=[$15], c_email_address_node_9=[$16], c_last_review_date_node_9=[$17])
         :  :     +- LogicalTableScan(table=[[default_catalog, default_database, customer]])
         :  +- LogicalProject(ws_ship_mode_sk_node_10=[$1])
         :     +- LogicalAggregate(group=[{1}], EXPR$0=[COUNT($0)])
         :        +- LogicalProject(ws_ship_mode_sk_node_10=[$14], ws_warehouse_sk_node_10=[$15])
         :           +- LogicalTableScan(table=[[default_catalog, default_database, web_sales]])
         +- LogicalSort(sort0=[$19], dir0=[ASC])
            +- LogicalJoin(condition=[=($16, $23)], joinType=[inner])
               :- LogicalProject(c_customer_sk_node_11=[$0], c_customer_id_node_11=[$1], c_current_cdemo_sk_node_11=[$2], c_current_hdemo_sk_node_11=[$3], c_current_addr_sk_node_11=[$4], c_first_shipto_date_sk_node_11=[$5], c_first_sales_date_sk_node_11=[$6], c_salutation_node_11=[$7], c_first_name_node_11=[$8], c_last_name_node_11=[$9], c_preferred_cust_flag_node_11=[$10], c_birth_day_node_11=[$11], c_birth_month_node_11=[$12], c_birth_year_node_11=[$13], c_birth_country_node_11=[$14], c_login_node_11=[$15], c_email_address_node_11=[$16], c_last_review_date_node_11=[$17])
               :  +- LogicalTableScan(table=[[default_catalog, default_database, customer]])
               +- LogicalProject(cd_demo_sk_node_12=[$0], cd_gender_node_12=[$1], cd_marital_status_node_12=[$2], cd_education_status_node_12=[$3], cd_purchase_estimate_node_12=[$4], cd_credit_rating_node_12=[$5], cd_dep_count_node_12=[$6], cd_dep_employed_count_node_12=[$7], cd_dep_college_count_node_12=[$8])
                  +- LogicalTableScan(table=[[default_catalog, default_database, customer_demographics]])

== Optimized Physical Plan ==
HashAggregate(isMerge=[true], groupBy=[c_customer_id_node_9], select=[c_customer_id_node_9])
+- Exchange(distribution=[hash[c_customer_id_node_9]])
   +- LocalHashAggregate(groupBy=[c_customer_id_node_9], select=[c_customer_id_node_9])
      +- Calc(select=[EXPR$0 AS c_customer_id_node_9])
         +- SortAggregate(isMerge=[false], groupBy=[c_last_name], select=[c_last_name, MIN(c_customer_id_node_9) AS EXPR$0])
            +- Sort(orderBy=[c_last_name ASC])
               +- Exchange(distribution=[hash[c_last_name]])
                  +- NestedLoopJoin(joinType=[InnerJoin], where=[=(c_login, c_birth_country_node_9)], select=[c_customer_sk_node_9, c_customer_id_node_9, c_current_cdemo_sk_node_9, c_current_hdemo_sk_node_9, c_current_addr_sk_node_9, c_first_shipto_date_sk_node_9, c_first_sales_date_sk_node_9, c_salutation_node_9, c_first_name_node_9, c_last_name_node_9, c_preferred_cust_flag_node_9, c_birth_day_node_9, c_birth_month_node_9, c_birth_year_node_9, c_birth_country_node_9, c_login_node_9, c_email_address_node_9, c_last_review_date_node_9, ws_ship_mode_sk_node_10, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], build=[left])
                     :- Exchange(distribution=[broadcast])
                     :  +- Calc(select=[c_customer_sk_node_9, c_customer_id_node_9, c_current_cdemo_sk_node_9, c_current_hdemo_sk_node_9, c_current_addr_sk_node_9, c_first_shipto_date_sk_node_9, c_first_sales_date_sk_node_9, c_salutation_node_9, c_first_name_node_9, c_last_name_node_9, c_preferred_cust_flag_node_9, c_birth_day_node_9, c_birth_month_node_9, c_birth_year_node_9, c_birth_country_node_9, c_login_node_9, c_email_address_node_9, c_last_review_date_node_9, ws_ship_mode_sk_node_10])
                     :     +- HashJoin(joinType=[InnerJoin], where=[=(c_first_shipto_date_sk_node_90, ws_ship_mode_sk_node_10)], select=[c_customer_sk_node_9, c_customer_id_node_9, c_current_cdemo_sk_node_9, c_current_hdemo_sk_node_9, c_current_addr_sk_node_9, c_first_shipto_date_sk_node_9, c_first_sales_date_sk_node_9, c_salutation_node_9, c_first_name_node_9, c_last_name_node_9, c_preferred_cust_flag_node_9, c_birth_day_node_9, c_birth_month_node_9, c_birth_year_node_9, c_birth_country_node_9, c_login_node_9, c_email_address_node_9, c_last_review_date_node_9, c_first_shipto_date_sk_node_90, ws_ship_mode_sk_node_10], isBroadcast=[true], build=[left])
                     :        :- Exchange(distribution=[broadcast])
                     :        :  +- Calc(select=[c_customer_sk AS c_customer_sk_node_9, c_customer_id AS c_customer_id_node_9, c_current_cdemo_sk AS c_current_cdemo_sk_node_9, c_current_hdemo_sk AS c_current_hdemo_sk_node_9, c_current_addr_sk AS c_current_addr_sk_node_9, c_first_shipto_date_sk AS c_first_shipto_date_sk_node_9, c_first_sales_date_sk AS c_first_sales_date_sk_node_9, c_salutation AS c_salutation_node_9, c_first_name AS c_first_name_node_9, c_last_name AS c_last_name_node_9, c_preferred_cust_flag AS c_preferred_cust_flag_node_9, c_birth_day AS c_birth_day_node_9, c_birth_month AS c_birth_month_node_9, c_birth_year AS c_birth_year_node_9, c_birth_country AS c_birth_country_node_9, c_login AS c_login_node_9, c_email_address AS c_email_address_node_9, c_last_review_date AS c_last_review_date_node_9, CAST(c_first_shipto_date_sk AS BIGINT) AS c_first_shipto_date_sk_node_90])
                     :        :     +- Limit(offset=[0], fetch=[53], global=[true])
                     :        :        +- Exchange(distribution=[single])
                     :        :           +- Limit(offset=[0], fetch=[53], global=[false])
                     :        :              +- TableSourceScan(table=[[default_catalog, default_database, customer, limit=[53]]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])
                     :        +- Calc(select=[EXPR$0 AS ws_ship_mode_sk_node_10])
                     :           +- HashAggregate(isMerge=[true], groupBy=[ws_warehouse_sk], select=[ws_warehouse_sk, Final_COUNT(count$0) AS EXPR$0])
                     :              +- Exchange(distribution=[hash[ws_warehouse_sk]])
                     :                 +- LocalHashAggregate(groupBy=[ws_warehouse_sk], select=[ws_warehouse_sk, Partial_COUNT(ws_ship_mode_sk) AS count$0])
                     :                    +- TableSourceScan(table=[[default_catalog, default_database, web_sales, project=[ws_ship_mode_sk, ws_warehouse_sk], metadata=[]]], fields=[ws_ship_mode_sk, ws_warehouse_sk])
                     +- Sort(orderBy=[cd_gender ASC])
                        +- Exchange(distribution=[single])
                           +- HashJoin(joinType=[InnerJoin], where=[=(c_email_address, cd_credit_rating)], select=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], build=[left])
                              :- Exchange(distribution=[hash[c_email_address]])
                              :  +- TableSourceScan(table=[[default_catalog, default_database, customer]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])
                              +- Exchange(distribution=[hash[cd_credit_rating]])
                                 +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])

== Optimized Execution Plan ==
HashAggregate(isMerge=[true], groupBy=[c_customer_id_node_9], select=[c_customer_id_node_9])
+- Exchange(distribution=[hash[c_customer_id_node_9]])
   +- LocalHashAggregate(groupBy=[c_customer_id_node_9], select=[c_customer_id_node_9])
      +- Calc(select=[EXPR$0 AS c_customer_id_node_9])
         +- SortAggregate(isMerge=[false], groupBy=[c_last_name], select=[c_last_name, MIN(c_customer_id_node_9) AS EXPR$0])
            +- Exchange(distribution=[forward])
               +- Sort(orderBy=[c_last_name ASC])
                  +- Exchange(distribution=[hash[c_last_name]])
                     +- NestedLoopJoin(joinType=[InnerJoin], where=[(c_login = c_birth_country_node_9)], select=[c_customer_sk_node_9, c_customer_id_node_9, c_current_cdemo_sk_node_9, c_current_hdemo_sk_node_9, c_current_addr_sk_node_9, c_first_shipto_date_sk_node_9, c_first_sales_date_sk_node_9, c_salutation_node_9, c_first_name_node_9, c_last_name_node_9, c_preferred_cust_flag_node_9, c_birth_day_node_9, c_birth_month_node_9, c_birth_year_node_9, c_birth_country_node_9, c_login_node_9, c_email_address_node_9, c_last_review_date_node_9, ws_ship_mode_sk_node_10, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], build=[left])
                        :- Exchange(distribution=[broadcast])
                        :  +- Calc(select=[c_customer_sk_node_9, c_customer_id_node_9, c_current_cdemo_sk_node_9, c_current_hdemo_sk_node_9, c_current_addr_sk_node_9, c_first_shipto_date_sk_node_9, c_first_sales_date_sk_node_9, c_salutation_node_9, c_first_name_node_9, c_last_name_node_9, c_preferred_cust_flag_node_9, c_birth_day_node_9, c_birth_month_node_9, c_birth_year_node_9, c_birth_country_node_9, c_login_node_9, c_email_address_node_9, c_last_review_date_node_9, ws_ship_mode_sk_node_10])
                        :     +- MultipleInput(readOrder=[0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(c_first_shipto_date_sk_node_90 = ws_ship_mode_sk_node_10)], select=[c_customer_sk_node_9, c_customer_id_node_9, c_current_cdemo_sk_node_9, c_current_hdemo_sk_node_9, c_current_addr_sk_node_9, c_first_shipto_date_sk_node_9, c_first_sales_date_sk_node_9, c_salutation_node_9, c_first_name_node_9, c_last_name_node_9, c_preferred_cust_flag_node_9, c_birth_day_node_9, c_birth_month_node_9, c_birth_year_node_9, c_birth_country_node_9, c_login_node_9, c_email_address_node_9, c_last_review_date_node_9, c_first_shipto_date_sk_node_90, ws_ship_mode_sk_node_10], isBroadcast=[true], build=[left])\
:- [#1] Exchange(distribution=[broadcast])\
+- Calc(select=[EXPR$0 AS ws_ship_mode_sk_node_10])\
   +- HashAggregate(isMerge=[true], groupBy=[ws_warehouse_sk], select=[ws_warehouse_sk, Final_COUNT(count$0) AS EXPR$0])\
      +- [#2] Exchange(distribution=[hash[ws_warehouse_sk]])\
])
                        :        :- Exchange(distribution=[broadcast])
                        :        :  +- Calc(select=[c_customer_sk AS c_customer_sk_node_9, c_customer_id AS c_customer_id_node_9, c_current_cdemo_sk AS c_current_cdemo_sk_node_9, c_current_hdemo_sk AS c_current_hdemo_sk_node_9, c_current_addr_sk AS c_current_addr_sk_node_9, c_first_shipto_date_sk AS c_first_shipto_date_sk_node_9, c_first_sales_date_sk AS c_first_sales_date_sk_node_9, c_salutation AS c_salutation_node_9, c_first_name AS c_first_name_node_9, c_last_name AS c_last_name_node_9, c_preferred_cust_flag AS c_preferred_cust_flag_node_9, c_birth_day AS c_birth_day_node_9, c_birth_month AS c_birth_month_node_9, c_birth_year AS c_birth_year_node_9, c_birth_country AS c_birth_country_node_9, c_login AS c_login_node_9, c_email_address AS c_email_address_node_9, c_last_review_date AS c_last_review_date_node_9, CAST(c_first_shipto_date_sk AS BIGINT) AS c_first_shipto_date_sk_node_90])
                        :        :     +- Limit(offset=[0], fetch=[53], global=[true])
                        :        :        +- Exchange(distribution=[single])
                        :        :           +- Limit(offset=[0], fetch=[53], global=[false])
                        :        :              +- TableSourceScan(table=[[default_catalog, default_database, customer, limit=[53]]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])
                        :        +- Exchange(distribution=[hash[ws_warehouse_sk]])
                        :           +- LocalHashAggregate(groupBy=[ws_warehouse_sk], select=[ws_warehouse_sk, Partial_COUNT(ws_ship_mode_sk) AS count$0])
                        :              +- TableSourceScan(table=[[default_catalog, default_database, web_sales, project=[ws_ship_mode_sk, ws_warehouse_sk], metadata=[]]], fields=[ws_ship_mode_sk, ws_warehouse_sk])
                        +- Sort(orderBy=[cd_gender ASC])
                           +- Exchange(distribution=[single])
                              +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(c_email_address = cd_credit_rating)], select=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], build=[left])
                                 :- Exchange(distribution=[hash[c_email_address]])
                                 :  +- TableSourceScan(table=[[default_catalog, default_database, customer]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])
                                 +- Exchange(distribution=[hash[cd_credit_rating]])
                                    +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o292261541.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#590120277:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[19](input=RelSubset#590120275,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[19]), rel#590120274:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[19](input=RelSubset#590120273,groupBy=c_last_name, c_login,select=c_last_name, c_login)]
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