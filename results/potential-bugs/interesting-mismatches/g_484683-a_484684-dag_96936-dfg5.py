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
    return values.kurtosis()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_9 = table_env.from_path("promotion").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("promotion").get_schema().get_field_names()])
autonode_8 = table_env.from_path("customer_demographics").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("customer_demographics").get_schema().get_field_names()])
autonode_7 = autonode_9.distinct()
autonode_6 = autonode_8.order_by(col('cd_credit_rating_node_8'))
autonode_5 = autonode_7.filter(col('p_channel_details_node_9').char_length >= 5)
autonode_4 = autonode_6.add_columns(lit("hello"))
autonode_3 = autonode_4.join(autonode_5, col('p_item_sk_node_9') == col('cd_dep_count_node_8'))
autonode_2 = autonode_3.group_by(col('p_channel_catalog_node_9')).select(col('p_cost_node_9').min.alias('p_cost_node_9'))
autonode_1 = autonode_2.add_columns(lit("hello"))
sink = autonode_1.filter(col('p_cost_node_9') < -1.532977819442749)
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
      "error_message": "An error occurred while calling o263892098.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#533580237:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[5](input=RelSubset#533580235,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[5]), rel#533580234:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[5](input=RelSubset#533580233,groupBy=cd_dep_count,select=cd_dep_count)]
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
Caused by: java.lang.IndexOutOfBoundsException: index (5) must be less than size (1)
\tat org.apache.flink.calcite.shaded.com.google.common.base.Preconditions.checkElementIndex(Preconditions.java:1371)
\tat org.apache.flink.calcite.shaded.com.google.common.base.Preconditions.checkElementIndex(Preconditions.java:1353)
\tat org.apache.flink.calcite.shaded.com.google.common.collect.SingletonImmutableList.get(SingletonImmutableList.java:44)
\tat org.apache.calcite.util.Util$TransformingList.get(Util.java:2794)
\tat scala.collection.convert.Wrappers$JListWrapper.apply(Wrappers.scala:100)
\tat org.apache.flink.table.planner.plan.utils.RelExplainUtil$.$anonfun$collationToString$1(RelExplainUtil.scala:83)
\tat scala.collection.TraversableLike.$anonfun$map$1(TraversableLike.scala:286)
\tat scala.collection.Iterator.foreach(Iterator.scala:943)
\tat scala.collection.Iterator.foreach$(Iterator.scala:943)
\tat scala.collection.AbstractIterator.foreach(Iterator.scala:1431)
\tat scala.collection.IterableLike.foreach(IterableLike.scala:74)
\tat scala.collection.IterableLike.foreach$(IterableLike.scala:73)
\tat scala.collection.AbstractIterable.foreach(Iterable.scala:56)
\tat scala.collection.TraversableLike.map(TraversableLike.scala:286)
\tat scala.collection.TraversableLike.map$(TraversableLike.scala:279)
\tat scala.collection.AbstractTraversable.map(Traversable.scala:108)
\tat org.apache.flink.table.planner.plan.utils.RelExplainUtil$.collationToString(RelExplainUtil.scala:83)
\tat org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalSort.explainTerms(BatchPhysicalSort.scala:61)
\tat org.apache.calcite.rel.AbstractRelNode.getDigestItems(AbstractRelNode.java:414)
\tat org.apache.calcite.rel.AbstractRelNode.deepHashCode(AbstractRelNode.java:396)
\tat org.apache.calcite.rel.AbstractRelNode$InnerRelDigest.hashCode(AbstractRelNode.java:448)
\tat java.base/java.util.HashMap.hash(HashMap.java:340)
\tat java.base/java.util.HashMap.get(HashMap.java:553)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.registerImpl(VolcanoPlanner.java:1289)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.register(VolcanoPlanner.java:598)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.ensureRegistered(VolcanoPlanner.java:613)
\tat org.apache.calcite.plan.volcano.VolcanoRuleCall.transformTo(VolcanoRuleCall.java:144)
\t... 45 more
",
      "stdout": "",
      "stderr": ""
    },
    "unopt": {
      "success": true,
      "error_name": "",
      "error_message": "",
      "stdout": "== Abstract Syntax Tree ==
LogicalFilter(condition=[<($0, -1.532977819442749E0:DOUBLE)])
+- LogicalProject(p_cost_node_9=[$1], _c1=[_UTF-16LE'hello'])
   +- LogicalAggregate(group=[{20}], EXPR$0=[MIN($15)])
      +- LogicalJoin(condition=[=($14, $6)], joinType=[inner])
         :- LogicalProject(cd_demo_sk_node_8=[$0], cd_gender_node_8=[$1], cd_marital_status_node_8=[$2], cd_education_status_node_8=[$3], cd_purchase_estimate_node_8=[$4], cd_credit_rating_node_8=[$5], cd_dep_count_node_8=[$6], cd_dep_employed_count_node_8=[$7], cd_dep_college_count_node_8=[$8], _c9=[_UTF-16LE'hello'])
         :  +- LogicalSort(sort0=[$5], dir0=[ASC])
         :     +- LogicalProject(cd_demo_sk_node_8=[$0], cd_gender_node_8=[$1], cd_marital_status_node_8=[$2], cd_education_status_node_8=[$3], cd_purchase_estimate_node_8=[$4], cd_credit_rating_node_8=[$5], cd_dep_count_node_8=[$6], cd_dep_employed_count_node_8=[$7], cd_dep_college_count_node_8=[$8])
         :        +- LogicalTableScan(table=[[default_catalog, default_database, customer_demographics]])
         +- LogicalFilter(condition=[>=(CHAR_LENGTH($16), 5)])
            +- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18}])
               +- LogicalProject(p_promo_sk_node_9=[$0], p_promo_id_node_9=[$1], p_start_date_sk_node_9=[$2], p_end_date_sk_node_9=[$3], p_item_sk_node_9=[$4], p_cost_node_9=[$5], p_response_target_node_9=[$6], p_promo_name_node_9=[$7], p_channel_dmail_node_9=[$8], p_channel_email_node_9=[$9], p_channel_catalog_node_9=[$10], p_channel_tv_node_9=[$11], p_channel_radio_node_9=[$12], p_channel_press_node_9=[$13], p_channel_event_node_9=[$14], p_channel_demo_node_9=[$15], p_channel_details_node_9=[$16], p_purpose_node_9=[$17], p_discount_active_node_9=[$18])
                  +- LogicalTableScan(table=[[default_catalog, default_database, promotion]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS p_cost_node_9, 'hello' AS _c1], where=[<(EXPR$0, -1.532977819442749E0)])
+- HashAggregate(isMerge=[true], groupBy=[p_channel_catalog], select=[p_channel_catalog, Final_MIN(min$0) AS EXPR$0])
   +- Exchange(distribution=[hash[p_channel_catalog]])
      +- LocalHashAggregate(groupBy=[p_channel_catalog], select=[p_channel_catalog, Partial_MIN(p_cost) AS min$0])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[=(p_item_sk, cd_dep_count_node_8)], select=[cd_demo_sk_node_8, cd_gender_node_8, cd_marital_status_node_8, cd_education_status_node_8, cd_purchase_estimate_node_8, cd_credit_rating_node_8, cd_dep_count_node_8, cd_dep_employed_count_node_8, cd_dep_college_count_node_8, _c9, p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active], build=[left])
            :- Exchange(distribution=[broadcast])
            :  +- Calc(select=[cd_demo_sk AS cd_demo_sk_node_8, cd_gender AS cd_gender_node_8, cd_marital_status AS cd_marital_status_node_8, cd_education_status AS cd_education_status_node_8, cd_purchase_estimate AS cd_purchase_estimate_node_8, cd_credit_rating AS cd_credit_rating_node_8, cd_dep_count AS cd_dep_count_node_8, cd_dep_employed_count AS cd_dep_employed_count_node_8, cd_dep_college_count AS cd_dep_college_count_node_8, 'hello' AS _c9])
            :     +- SortLimit(orderBy=[cd_credit_rating ASC], offset=[0], fetch=[1], global=[true])
            :        +- Exchange(distribution=[single])
            :           +- SortLimit(orderBy=[cd_credit_rating ASC], offset=[0], fetch=[1], global=[false])
            :              +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])
            +- HashAggregate(isMerge=[false], groupBy=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active], select=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])
               +- Exchange(distribution=[hash[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active]])
                  +- Calc(select=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active], where=[>=(CHAR_LENGTH(p_channel_details), 5)])
                     +- TableSourceScan(table=[[default_catalog, default_database, promotion, filter=[>=(CHAR_LENGTH(p_channel_details), 5)]]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS p_cost_node_9, 'hello' AS _c1], where=[(EXPR$0 < -1.532977819442749E0)])
+- HashAggregate(isMerge=[true], groupBy=[p_channel_catalog], select=[p_channel_catalog, Final_MIN(min$0) AS EXPR$0])
   +- Exchange(distribution=[hash[p_channel_catalog]])
      +- LocalHashAggregate(groupBy=[p_channel_catalog], select=[p_channel_catalog, Partial_MIN(p_cost) AS min$0])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[(p_item_sk = cd_dep_count_node_8)], select=[cd_demo_sk_node_8, cd_gender_node_8, cd_marital_status_node_8, cd_education_status_node_8, cd_purchase_estimate_node_8, cd_credit_rating_node_8, cd_dep_count_node_8, cd_dep_employed_count_node_8, cd_dep_college_count_node_8, _c9, p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active], build=[left])
            :- Exchange(distribution=[broadcast])
            :  +- Calc(select=[cd_demo_sk AS cd_demo_sk_node_8, cd_gender AS cd_gender_node_8, cd_marital_status AS cd_marital_status_node_8, cd_education_status AS cd_education_status_node_8, cd_purchase_estimate AS cd_purchase_estimate_node_8, cd_credit_rating AS cd_credit_rating_node_8, cd_dep_count AS cd_dep_count_node_8, cd_dep_employed_count AS cd_dep_employed_count_node_8, cd_dep_college_count AS cd_dep_college_count_node_8, 'hello' AS _c9])
            :     +- SortLimit(orderBy=[cd_credit_rating ASC], offset=[0], fetch=[1], global=[true])
            :        +- Exchange(distribution=[single])
            :           +- SortLimit(orderBy=[cd_credit_rating ASC], offset=[0], fetch=[1], global=[false])
            :              +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])
            +- HashAggregate(isMerge=[false], groupBy=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active], select=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])
               +- Exchange(distribution=[hash[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active]])
                  +- Calc(select=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active], where=[(CHAR_LENGTH(p_channel_details) >= 5)])
                     +- TableSourceScan(table=[[default_catalog, default_database, promotion, filter=[>=(CHAR_LENGTH(p_channel_details), 5)]]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0