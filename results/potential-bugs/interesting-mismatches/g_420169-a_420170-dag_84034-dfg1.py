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
    return values.max()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_13 = table_env.from_path("customer_demographics").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("customer_demographics").get_schema().get_field_names()])
autonode_12 = table_env.from_path("customer_demographics").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("customer_demographics").get_schema().get_field_names()])
autonode_11 = table_env.from_path("promotion").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("promotion").get_schema().get_field_names()])
autonode_10 = autonode_13.filter(col('cd_purchase_estimate_node_13') >= 23)
autonode_9 = autonode_12.filter(col('cd_demo_sk_node_12') > 19)
autonode_8 = autonode_11.limit(81)
autonode_7 = autonode_10.order_by(col('cd_credit_rating_node_13'))
autonode_6 = autonode_9.group_by(col('cd_purchase_estimate_node_12')).select(col('cd_credit_rating_node_12').max.alias('cd_credit_rating_node_12'))
autonode_5 = autonode_8.add_columns(lit("hello"))
autonode_4 = autonode_7.order_by(col('cd_education_status_node_13'))
autonode_3 = autonode_5.join(autonode_6, col('cd_credit_rating_node_12') == col('p_promo_id_node_11'))
autonode_2 = autonode_3.join(autonode_4, col('cd_credit_rating_node_13') == col('p_channel_catalog_node_11'))
autonode_1 = autonode_2.group_by(col('cd_demo_sk_node_13')).select(col('cd_dep_college_count_node_13').max.alias('cd_dep_college_count_node_13'))
sink = autonode_1.order_by(col('cd_dep_college_count_node_13'))
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
+- LogicalProject(cd_dep_college_count_node_13=[$1])
   +- LogicalAggregate(group=[{21}], EXPR$0=[MAX($29)])
      +- LogicalJoin(condition=[=($26, $10)], joinType=[inner])
         :- LogicalJoin(condition=[=($20, $1)], joinType=[inner])
         :  :- LogicalProject(p_promo_sk_node_11=[$0], p_promo_id_node_11=[$1], p_start_date_sk_node_11=[$2], p_end_date_sk_node_11=[$3], p_item_sk_node_11=[$4], p_cost_node_11=[$5], p_response_target_node_11=[$6], p_promo_name_node_11=[$7], p_channel_dmail_node_11=[$8], p_channel_email_node_11=[$9], p_channel_catalog_node_11=[$10], p_channel_tv_node_11=[$11], p_channel_radio_node_11=[$12], p_channel_press_node_11=[$13], p_channel_event_node_11=[$14], p_channel_demo_node_11=[$15], p_channel_details_node_11=[$16], p_purpose_node_11=[$17], p_discount_active_node_11=[$18], _c19=[_UTF-16LE'hello'])
         :  :  +- LogicalSort(fetch=[81])
         :  :     +- LogicalProject(p_promo_sk_node_11=[$0], p_promo_id_node_11=[$1], p_start_date_sk_node_11=[$2], p_end_date_sk_node_11=[$3], p_item_sk_node_11=[$4], p_cost_node_11=[$5], p_response_target_node_11=[$6], p_promo_name_node_11=[$7], p_channel_dmail_node_11=[$8], p_channel_email_node_11=[$9], p_channel_catalog_node_11=[$10], p_channel_tv_node_11=[$11], p_channel_radio_node_11=[$12], p_channel_press_node_11=[$13], p_channel_event_node_11=[$14], p_channel_demo_node_11=[$15], p_channel_details_node_11=[$16], p_purpose_node_11=[$17], p_discount_active_node_11=[$18])
         :  :        +- LogicalTableScan(table=[[default_catalog, default_database, promotion]])
         :  +- LogicalProject(cd_credit_rating_node_12=[$1])
         :     +- LogicalAggregate(group=[{4}], EXPR$0=[MAX($5)])
         :        +- LogicalFilter(condition=[>($0, 19)])
         :           +- LogicalProject(cd_demo_sk_node_12=[$0], cd_gender_node_12=[$1], cd_marital_status_node_12=[$2], cd_education_status_node_12=[$3], cd_purchase_estimate_node_12=[$4], cd_credit_rating_node_12=[$5], cd_dep_count_node_12=[$6], cd_dep_employed_count_node_12=[$7], cd_dep_college_count_node_12=[$8])
         :              +- LogicalTableScan(table=[[default_catalog, default_database, customer_demographics]])
         +- LogicalSort(sort0=[$3], dir0=[ASC])
            +- LogicalSort(sort0=[$5], dir0=[ASC])
               +- LogicalFilter(condition=[>=($4, 23)])
                  +- LogicalProject(cd_demo_sk_node_13=[$0], cd_gender_node_13=[$1], cd_marital_status_node_13=[$2], cd_education_status_node_13=[$3], cd_purchase_estimate_node_13=[$4], cd_credit_rating_node_13=[$5], cd_dep_count_node_13=[$6], cd_dep_employed_count_node_13=[$7], cd_dep_college_count_node_13=[$8])
                     +- LogicalTableScan(table=[[default_catalog, default_database, customer_demographics]])

== Optimized Physical Plan ==
Sort(orderBy=[cd_dep_college_count_node_13 ASC])
+- Exchange(distribution=[single])
   +- Calc(select=[EXPR$0 AS cd_dep_college_count_node_13])
      +- HashAggregate(isMerge=[true], groupBy=[cd_demo_sk], select=[cd_demo_sk, Final_MAX(max$0) AS EXPR$0])
         +- Exchange(distribution=[hash[cd_demo_sk]])
            +- LocalHashAggregate(groupBy=[cd_demo_sk], select=[cd_demo_sk, Partial_MAX(cd_dep_college_count) AS max$0])
               +- NestedLoopJoin(joinType=[InnerJoin], where=[=(cd_credit_rating, p_channel_catalog_node_11)], select=[p_promo_sk_node_11, p_promo_id_node_11, p_start_date_sk_node_11, p_end_date_sk_node_11, p_item_sk_node_11, p_cost_node_11, p_response_target_node_11, p_promo_name_node_11, p_channel_dmail_node_11, p_channel_email_node_11, p_channel_catalog_node_11, p_channel_tv_node_11, p_channel_radio_node_11, p_channel_press_node_11, p_channel_event_node_11, p_channel_demo_node_11, p_channel_details_node_11, p_purpose_node_11, p_discount_active_node_11, _c19, cd_credit_rating_node_12, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], build=[left])
                  :- Exchange(distribution=[broadcast])
                  :  +- HashJoin(joinType=[InnerJoin], where=[=(cd_credit_rating_node_12, p_promo_id_node_11)], select=[p_promo_sk_node_11, p_promo_id_node_11, p_start_date_sk_node_11, p_end_date_sk_node_11, p_item_sk_node_11, p_cost_node_11, p_response_target_node_11, p_promo_name_node_11, p_channel_dmail_node_11, p_channel_email_node_11, p_channel_catalog_node_11, p_channel_tv_node_11, p_channel_radio_node_11, p_channel_press_node_11, p_channel_event_node_11, p_channel_demo_node_11, p_channel_details_node_11, p_purpose_node_11, p_discount_active_node_11, _c19, cd_credit_rating_node_12], isBroadcast=[true], build=[left])
                  :     :- Exchange(distribution=[broadcast])
                  :     :  +- Calc(select=[p_promo_sk AS p_promo_sk_node_11, p_promo_id AS p_promo_id_node_11, p_start_date_sk AS p_start_date_sk_node_11, p_end_date_sk AS p_end_date_sk_node_11, p_item_sk AS p_item_sk_node_11, p_cost AS p_cost_node_11, p_response_target AS p_response_target_node_11, p_promo_name AS p_promo_name_node_11, p_channel_dmail AS p_channel_dmail_node_11, p_channel_email AS p_channel_email_node_11, p_channel_catalog AS p_channel_catalog_node_11, p_channel_tv AS p_channel_tv_node_11, p_channel_radio AS p_channel_radio_node_11, p_channel_press AS p_channel_press_node_11, p_channel_event AS p_channel_event_node_11, p_channel_demo AS p_channel_demo_node_11, p_channel_details AS p_channel_details_node_11, p_purpose AS p_purpose_node_11, p_discount_active AS p_discount_active_node_11, 'hello' AS _c19])
                  :     :     +- Limit(offset=[0], fetch=[81], global=[true])
                  :     :        +- Exchange(distribution=[single])
                  :     :           +- Limit(offset=[0], fetch=[81], global=[false])
                  :     :              +- TableSourceScan(table=[[default_catalog, default_database, promotion, limit=[81]]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])
                  :     +- Calc(select=[EXPR$0 AS cd_credit_rating_node_12])
                  :        +- SortAggregate(isMerge=[true], groupBy=[cd_purchase_estimate], select=[cd_purchase_estimate, Final_MAX(max$0) AS EXPR$0])
                  :           +- Sort(orderBy=[cd_purchase_estimate ASC])
                  :              +- Exchange(distribution=[hash[cd_purchase_estimate]])
                  :                 +- LocalSortAggregate(groupBy=[cd_purchase_estimate], select=[cd_purchase_estimate, Partial_MAX(cd_credit_rating) AS max$0])
                  :                    +- Sort(orderBy=[cd_purchase_estimate ASC])
                  :                       +- Calc(select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], where=[>(cd_demo_sk, 19)])
                  :                          +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics, filter=[>(cd_demo_sk, 19)]]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])
                  +- Sort(orderBy=[cd_education_status ASC])
                     +- Sort(orderBy=[cd_credit_rating ASC])
                        +- Exchange(distribution=[single])
                           +- Calc(select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], where=[>=(cd_purchase_estimate, 23)])
                              +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics, filter=[>=(cd_purchase_estimate, 23)]]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])

== Optimized Execution Plan ==
Sort(orderBy=[cd_dep_college_count_node_13 ASC])
+- Exchange(distribution=[single])
   +- Calc(select=[EXPR$0 AS cd_dep_college_count_node_13])
      +- HashAggregate(isMerge=[true], groupBy=[cd_demo_sk], select=[cd_demo_sk, Final_MAX(max$0) AS EXPR$0])
         +- Exchange(distribution=[hash[cd_demo_sk]])
            +- LocalHashAggregate(groupBy=[cd_demo_sk], select=[cd_demo_sk, Partial_MAX(cd_dep_college_count) AS max$0])
               +- NestedLoopJoin(joinType=[InnerJoin], where=[(cd_credit_rating = p_channel_catalog_node_11)], select=[p_promo_sk_node_11, p_promo_id_node_11, p_start_date_sk_node_11, p_end_date_sk_node_11, p_item_sk_node_11, p_cost_node_11, p_response_target_node_11, p_promo_name_node_11, p_channel_dmail_node_11, p_channel_email_node_11, p_channel_catalog_node_11, p_channel_tv_node_11, p_channel_radio_node_11, p_channel_press_node_11, p_channel_event_node_11, p_channel_demo_node_11, p_channel_details_node_11, p_purpose_node_11, p_discount_active_node_11, _c19, cd_credit_rating_node_12, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], build=[left])
                  :- Exchange(distribution=[broadcast])
                  :  +- MultipleInput(readOrder=[0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(cd_credit_rating_node_12 = p_promo_id_node_11)], select=[p_promo_sk_node_11, p_promo_id_node_11, p_start_date_sk_node_11, p_end_date_sk_node_11, p_item_sk_node_11, p_cost_node_11, p_response_target_node_11, p_promo_name_node_11, p_channel_dmail_node_11, p_channel_email_node_11, p_channel_catalog_node_11, p_channel_tv_node_11, p_channel_radio_node_11, p_channel_press_node_11, p_channel_event_node_11, p_channel_demo_node_11, p_channel_details_node_11, p_purpose_node_11, p_discount_active_node_11, _c19, cd_credit_rating_node_12], isBroadcast=[true], build=[left])\
:- [#1] Exchange(distribution=[broadcast])\
+- Calc(select=[EXPR$0 AS cd_credit_rating_node_12])\
   +- SortAggregate(isMerge=[true], groupBy=[cd_purchase_estimate], select=[cd_purchase_estimate, Final_MAX(max$0) AS EXPR$0])\
      +- Sort(orderBy=[cd_purchase_estimate ASC])\
         +- [#2] Exchange(distribution=[hash[cd_purchase_estimate]])\
])
                  :     :- Exchange(distribution=[broadcast])
                  :     :  +- Calc(select=[p_promo_sk AS p_promo_sk_node_11, p_promo_id AS p_promo_id_node_11, p_start_date_sk AS p_start_date_sk_node_11, p_end_date_sk AS p_end_date_sk_node_11, p_item_sk AS p_item_sk_node_11, p_cost AS p_cost_node_11, p_response_target AS p_response_target_node_11, p_promo_name AS p_promo_name_node_11, p_channel_dmail AS p_channel_dmail_node_11, p_channel_email AS p_channel_email_node_11, p_channel_catalog AS p_channel_catalog_node_11, p_channel_tv AS p_channel_tv_node_11, p_channel_radio AS p_channel_radio_node_11, p_channel_press AS p_channel_press_node_11, p_channel_event AS p_channel_event_node_11, p_channel_demo AS p_channel_demo_node_11, p_channel_details AS p_channel_details_node_11, p_purpose AS p_purpose_node_11, p_discount_active AS p_discount_active_node_11, 'hello' AS _c19])
                  :     :     +- Limit(offset=[0], fetch=[81], global=[true])
                  :     :        +- Exchange(distribution=[single])
                  :     :           +- Limit(offset=[0], fetch=[81], global=[false])
                  :     :              +- TableSourceScan(table=[[default_catalog, default_database, promotion, limit=[81]]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])
                  :     +- Exchange(distribution=[hash[cd_purchase_estimate]])
                  :        +- LocalSortAggregate(groupBy=[cd_purchase_estimate], select=[cd_purchase_estimate, Partial_MAX(cd_credit_rating) AS max$0])
                  :           +- Exchange(distribution=[forward])
                  :              +- Sort(orderBy=[cd_purchase_estimate ASC])
                  :                 +- Calc(select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], where=[(cd_demo_sk > 19)])
                  :                    +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics, filter=[>(cd_demo_sk, 19)]]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])
                  +- Sort(orderBy=[cd_education_status ASC])
                     +- Sort(orderBy=[cd_credit_rating ASC])
                        +- Exchange(distribution=[single])
                           +- Calc(select=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], where=[(cd_purchase_estimate >= 23)])
                              +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics, filter=[>=(cd_purchase_estimate, 23)]]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o228815107.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#462826845:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[3](input=RelSubset#462826843,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[3]), rel#462826842:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[3](input=RelSubset#462826841,groupBy=cd_demo_sk, cd_credit_rating,select=cd_demo_sk, cd_credit_rating, Partial_MAX(cd_dep_college_count) AS max$0)]
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