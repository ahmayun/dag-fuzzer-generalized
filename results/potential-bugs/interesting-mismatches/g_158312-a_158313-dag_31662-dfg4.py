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

autonode_13 = table_env.from_path("date_dim").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("date_dim").get_schema().get_field_names()])
autonode_12 = table_env.from_path("web_page").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("web_page").get_schema().get_field_names()])
autonode_11 = table_env.from_path("customer").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("customer").get_schema().get_field_names()])
autonode_14 = table_env.from_path("reason").select(*[col(column_name).alias(f"{column_name}_node_14") for column_name in table_env.from_path("reason").get_schema().get_field_names()])
autonode_9 = autonode_13.group_by(col('d_fy_week_seq_node_13')).select(col('d_last_dom_node_13').avg.alias('d_last_dom_node_13'))
autonode_8 = autonode_12.distinct()
autonode_7 = autonode_11.order_by(col('c_birth_country_node_11'))
autonode_10 = autonode_14.limit(70)
autonode_5 = autonode_7.join(autonode_8, col('wp_type_node_12') == col('c_last_name_node_11'))
autonode_6 = autonode_9.join(autonode_10, col('d_last_dom_node_13') == col('r_reason_sk_node_14'))
autonode_3 = autonode_5.group_by(col('c_customer_id_node_11')).select(col('c_birth_year_node_11').max.alias('c_birth_year_node_11'))
autonode_4 = autonode_6.group_by(col('r_reason_sk_node_14')).select(col('d_last_dom_node_13').min.alias('d_last_dom_node_13'))
autonode_2 = autonode_3.join(autonode_4, col('d_last_dom_node_13') == col('c_birth_year_node_11'))
autonode_1 = autonode_2.filter(col('d_last_dom_node_13') > 29)
sink = autonode_1.limit(32)
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
LogicalSort(fetch=[32])
+- LogicalFilter(condition=[>($1, 29)])
   +- LogicalJoin(condition=[=($1, $0)], joinType=[inner])
      :- LogicalProject(c_birth_year_node_11=[$1])
      :  +- LogicalAggregate(group=[{1}], EXPR$0=[MAX($13)])
      :     +- LogicalJoin(condition=[=($27, $9)], joinType=[inner])
      :        :- LogicalSort(sort0=[$14], dir0=[ASC])
      :        :  +- LogicalProject(c_customer_sk_node_11=[$0], c_customer_id_node_11=[$1], c_current_cdemo_sk_node_11=[$2], c_current_hdemo_sk_node_11=[$3], c_current_addr_sk_node_11=[$4], c_first_shipto_date_sk_node_11=[$5], c_first_sales_date_sk_node_11=[$6], c_salutation_node_11=[$7], c_first_name_node_11=[$8], c_last_name_node_11=[$9], c_preferred_cust_flag_node_11=[$10], c_birth_day_node_11=[$11], c_birth_month_node_11=[$12], c_birth_year_node_11=[$13], c_birth_country_node_11=[$14], c_login_node_11=[$15], c_email_address_node_11=[$16], c_last_review_date_node_11=[$17])
      :        :     +- LogicalTableScan(table=[[default_catalog, default_database, customer]])
      :        +- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}])
      :           +- LogicalProject(wp_web_page_sk_node_12=[$0], wp_web_page_id_node_12=[$1], wp_rec_start_date_node_12=[$2], wp_rec_end_date_node_12=[$3], wp_creation_date_sk_node_12=[$4], wp_access_date_sk_node_12=[$5], wp_autogen_flag_node_12=[$6], wp_customer_sk_node_12=[$7], wp_url_node_12=[$8], wp_type_node_12=[$9], wp_char_count_node_12=[$10], wp_link_count_node_12=[$11], wp_image_count_node_12=[$12], wp_max_ad_count_node_12=[$13])
      :              +- LogicalTableScan(table=[[default_catalog, default_database, web_page]])
      +- LogicalProject(d_last_dom_node_13=[$1])
         +- LogicalAggregate(group=[{1}], EXPR$0=[MIN($0)])
            +- LogicalJoin(condition=[=($0, $1)], joinType=[inner])
               :- LogicalProject(d_last_dom_node_13=[$1])
               :  +- LogicalAggregate(group=[{0}], EXPR$0=[AVG($1)])
               :     +- LogicalProject(d_fy_week_seq_node_13=[$13], d_last_dom_node_13=[$20])
               :        +- LogicalTableScan(table=[[default_catalog, default_database, date_dim]])
               +- LogicalSort(fetch=[70])
                  +- LogicalProject(r_reason_sk_node_14=[$0], r_reason_id_node_14=[$1], r_reason_desc_node_14=[$2])
                     +- LogicalTableScan(table=[[default_catalog, default_database, reason]])

== Optimized Physical Plan ==
Limit(offset=[0], fetch=[32], global=[true])
+- Exchange(distribution=[single])
   +- Limit(offset=[0], fetch=[32], global=[false])
      +- HashJoin(joinType=[InnerJoin], where=[=(EXPR$00, EXPR$0)], select=[EXPR$0, EXPR$00], isBroadcast=[true], build=[right])
         :- Calc(select=[EXPR$0], where=[>(EXPR$0, 29)])
         :  +- HashAggregate(isMerge=[true], groupBy=[c_customer_id], select=[c_customer_id, Final_MAX(max$0) AS EXPR$0])
         :     +- Exchange(distribution=[hash[c_customer_id]])
         :        +- LocalHashAggregate(groupBy=[c_customer_id], select=[c_customer_id, Partial_MAX(c_birth_year) AS max$0])
         :           +- HashJoin(joinType=[InnerJoin], where=[=(wp_type, c_last_name)], select=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date, wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count], isBroadcast=[true], build=[right])
         :              :- Sort(orderBy=[c_birth_country ASC])
         :              :  +- Exchange(distribution=[single])
         :              :     +- TableSourceScan(table=[[default_catalog, default_database, customer]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])
         :              +- Exchange(distribution=[broadcast])
         :                 +- HashAggregate(isMerge=[false], groupBy=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count], select=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
         :                    +- Exchange(distribution=[hash[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count]])
         :                       +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
         +- Exchange(distribution=[broadcast])
            +- Calc(select=[EXPR$0], where=[>(EXPR$0, 29)])
               +- HashAggregate(isMerge=[true], groupBy=[r_reason_sk], select=[r_reason_sk, Final_MIN(min$0) AS EXPR$0])
                  +- Exchange(distribution=[hash[r_reason_sk]])
                     +- LocalHashAggregate(groupBy=[r_reason_sk], select=[r_reason_sk, Partial_MIN(d_last_dom_node_13) AS min$0])
                        +- HashJoin(joinType=[InnerJoin], where=[=(d_last_dom_node_13, r_reason_sk)], select=[d_last_dom_node_13, r_reason_sk, r_reason_id, r_reason_desc], isBroadcast=[true], build=[right])
                           :- Calc(select=[EXPR$0 AS d_last_dom_node_13])
                           :  +- HashAggregate(isMerge=[true], groupBy=[d_fy_week_seq], select=[d_fy_week_seq, Final_AVG(sum$0, count$1) AS EXPR$0])
                           :     +- Exchange(distribution=[hash[d_fy_week_seq]])
                           :        +- LocalHashAggregate(groupBy=[d_fy_week_seq], select=[d_fy_week_seq, Partial_AVG(d_last_dom) AS (sum$0, count$1)])
                           :           +- TableSourceScan(table=[[default_catalog, default_database, date_dim, project=[d_fy_week_seq, d_last_dom], metadata=[]]], fields=[d_fy_week_seq, d_last_dom])
                           +- Exchange(distribution=[broadcast])
                              +- Limit(offset=[0], fetch=[70], global=[true])
                                 +- Exchange(distribution=[single])
                                    +- Limit(offset=[0], fetch=[70], global=[false])
                                       +- TableSourceScan(table=[[default_catalog, default_database, reason]], fields=[r_reason_sk, r_reason_id, r_reason_desc])

== Optimized Execution Plan ==
Limit(offset=[0], fetch=[32], global=[true])
+- Exchange(distribution=[single])
   +- Limit(offset=[0], fetch=[32], global=[false])
      +- MultipleInput(readOrder=[0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(EXPR$00 = EXPR$0)], select=[EXPR$0, EXPR$00], isBroadcast=[true], build=[right])\
:- Calc(select=[EXPR$0], where=[(EXPR$0 > 29)])\
:  +- HashAggregate(isMerge=[true], groupBy=[c_customer_id], select=[c_customer_id, Final_MAX(max$0) AS EXPR$0])\
:     +- [#2] Exchange(distribution=[hash[c_customer_id]])\
+- [#1] Exchange(distribution=[broadcast])\
])
         :- Exchange(distribution=[broadcast])
         :  +- Calc(select=[EXPR$0], where=[(EXPR$0 > 29)])
         :     +- HashAggregate(isMerge=[true], groupBy=[r_reason_sk], select=[r_reason_sk, Final_MIN(min$0) AS EXPR$0])
         :        +- Exchange(distribution=[hash[r_reason_sk]])
         :           +- LocalHashAggregate(groupBy=[r_reason_sk], select=[r_reason_sk, Partial_MIN(d_last_dom_node_13) AS min$0])
         :              +- MultipleInput(readOrder=[0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(d_last_dom_node_13 = r_reason_sk)], select=[d_last_dom_node_13, r_reason_sk, r_reason_id, r_reason_desc], isBroadcast=[true], build=[right])\
:- Calc(select=[EXPR$0 AS d_last_dom_node_13])\
:  +- HashAggregate(isMerge=[true], groupBy=[d_fy_week_seq], select=[d_fy_week_seq, Final_AVG(sum$0, count$1) AS EXPR$0])\
:     +- [#2] Exchange(distribution=[hash[d_fy_week_seq]])\
+- [#1] Exchange(distribution=[broadcast])\
])
         :                 :- Exchange(distribution=[broadcast])
         :                 :  +- Limit(offset=[0], fetch=[70], global=[true])
         :                 :     +- Exchange(distribution=[single])
         :                 :        +- Limit(offset=[0], fetch=[70], global=[false])
         :                 :           +- TableSourceScan(table=[[default_catalog, default_database, reason]], fields=[r_reason_sk, r_reason_id, r_reason_desc])
         :                 +- Exchange(distribution=[hash[d_fy_week_seq]])
         :                    +- LocalHashAggregate(groupBy=[d_fy_week_seq], select=[d_fy_week_seq, Partial_AVG(d_last_dom) AS (sum$0, count$1)])
         :                       +- TableSourceScan(table=[[default_catalog, default_database, date_dim, project=[d_fy_week_seq, d_last_dom], metadata=[]]], fields=[d_fy_week_seq, d_last_dom])
         +- Exchange(distribution=[hash[c_customer_id]])
            +- LocalHashAggregate(groupBy=[c_customer_id], select=[c_customer_id, Partial_MAX(c_birth_year) AS max$0])
               +- HashJoin(joinType=[InnerJoin], where=[(wp_type = c_last_name)], select=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date, wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count], isBroadcast=[true], build=[right])
                  :- Sort(orderBy=[c_birth_country ASC])
                  :  +- Exchange(distribution=[single])
                  :     +- TableSourceScan(table=[[default_catalog, default_database, customer]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])
                  +- Exchange(distribution=[broadcast])
                     +- HashAggregate(isMerge=[false], groupBy=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count], select=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
                        +- Exchange(distribution=[hash[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count]])
                           +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o86316452.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#173699189:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[14](input=RelSubset#173699187,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[14]), rel#173699186:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[14](input=RelSubset#173699185,groupBy=c_customer_id, c_last_name,select=c_customer_id, c_last_name, Partial_MAX(c_birth_year) AS max$0)]
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