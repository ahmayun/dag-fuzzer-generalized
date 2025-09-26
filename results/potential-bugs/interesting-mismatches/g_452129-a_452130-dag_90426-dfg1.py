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

autonode_10 = table_env.from_path("customer_demographics").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("customer_demographics").get_schema().get_field_names()])
autonode_9 = table_env.from_path("promotion").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("promotion").get_schema().get_field_names()])
autonode_8 = table_env.from_path("household_demographics").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("household_demographics").get_schema().get_field_names()])
autonode_7 = table_env.from_path("time_dim").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("time_dim").get_schema().get_field_names()])
autonode_6 = autonode_9.join(autonode_10, col('p_channel_tv_node_9') == col('cd_credit_rating_node_10'))
autonode_5 = autonode_8.filter(col('hd_vehicle_count_node_8') < -32)
autonode_4 = autonode_7.order_by(col('t_meal_time_node_7'))
autonode_3 = autonode_6.order_by(col('cd_demo_sk_node_10'))
autonode_2 = autonode_4.join(autonode_5, col('t_second_node_7') == col('hd_income_band_sk_node_8'))
autonode_1 = autonode_2.join(autonode_3, col('t_minute_node_7') == col('p_promo_sk_node_9'))
sink = autonode_1.group_by(col('t_shift_node_7')).select(col('t_meal_time_node_7').max.alias('t_meal_time_node_7'))
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
      "error_message": "An error occurred while calling o246135580.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#498073689:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[19](input=RelSubset#498073687,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[19]), rel#498073686:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[19](input=RelSubset#498073685,groupBy=p_promo_sk,select=p_promo_sk)]
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
Caused by: java.lang.IndexOutOfBoundsException: index (19) must be less than size (1)
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
LogicalProject(t_meal_time_node_7=[$1])
+- LogicalAggregate(group=[{7}], EXPR$0=[MAX($9)])
   +- LogicalJoin(condition=[=($4, $15)], joinType=[inner])
      :- LogicalJoin(condition=[=($5, $11)], joinType=[inner])
      :  :- LogicalSort(sort0=[$9], dir0=[ASC])
      :  :  +- LogicalProject(t_time_sk_node_7=[$0], t_time_id_node_7=[$1], t_time_node_7=[$2], t_hour_node_7=[$3], t_minute_node_7=[$4], t_second_node_7=[$5], t_am_pm_node_7=[$6], t_shift_node_7=[$7], t_sub_shift_node_7=[$8], t_meal_time_node_7=[$9])
      :  :     +- LogicalTableScan(table=[[default_catalog, default_database, time_dim]])
      :  +- LogicalFilter(condition=[<($4, -32)])
      :     +- LogicalProject(hd_demo_sk_node_8=[$0], hd_income_band_sk_node_8=[$1], hd_buy_potential_node_8=[$2], hd_dep_count_node_8=[$3], hd_vehicle_count_node_8=[$4])
      :        +- LogicalTableScan(table=[[default_catalog, default_database, household_demographics]])
      +- LogicalSort(sort0=[$19], dir0=[ASC])
         +- LogicalJoin(condition=[=($11, $24)], joinType=[inner])
            :- LogicalProject(p_promo_sk_node_9=[$0], p_promo_id_node_9=[$1], p_start_date_sk_node_9=[$2], p_end_date_sk_node_9=[$3], p_item_sk_node_9=[$4], p_cost_node_9=[$5], p_response_target_node_9=[$6], p_promo_name_node_9=[$7], p_channel_dmail_node_9=[$8], p_channel_email_node_9=[$9], p_channel_catalog_node_9=[$10], p_channel_tv_node_9=[$11], p_channel_radio_node_9=[$12], p_channel_press_node_9=[$13], p_channel_event_node_9=[$14], p_channel_demo_node_9=[$15], p_channel_details_node_9=[$16], p_purpose_node_9=[$17], p_discount_active_node_9=[$18])
            :  +- LogicalTableScan(table=[[default_catalog, default_database, promotion]])
            +- LogicalProject(cd_demo_sk_node_10=[$0], cd_gender_node_10=[$1], cd_marital_status_node_10=[$2], cd_education_status_node_10=[$3], cd_purchase_estimate_node_10=[$4], cd_credit_rating_node_10=[$5], cd_dep_count_node_10=[$6], cd_dep_employed_count_node_10=[$7], cd_dep_college_count_node_10=[$8])
               +- LogicalTableScan(table=[[default_catalog, default_database, customer_demographics]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS t_meal_time_node_7])
+- SortAggregate(isMerge=[true], groupBy=[t_shift], select=[t_shift, Final_MAX(max$0) AS EXPR$0])
   +- Sort(orderBy=[t_shift ASC])
      +- Exchange(distribution=[hash[t_shift]])
         +- LocalSortAggregate(groupBy=[t_shift], select=[t_shift, Partial_MAX(t_meal_time) AS max$0])
            +- Sort(orderBy=[t_shift ASC])
               +- NestedLoopJoin(joinType=[InnerJoin], where=[=(t_minute, p_promo_sk)], select=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time, hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], build=[right])
                  :- NestedLoopJoin(joinType=[InnerJoin], where=[=(t_second, hd_income_band_sk)], select=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time, hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count], build=[left])
                  :  :- Exchange(distribution=[broadcast])
                  :  :  +- SortLimit(orderBy=[t_meal_time ASC], offset=[0], fetch=[1], global=[true])
                  :  :     +- Exchange(distribution=[single])
                  :  :        +- SortLimit(orderBy=[t_meal_time ASC], offset=[0], fetch=[1], global=[false])
                  :  :           +- TableSourceScan(table=[[default_catalog, default_database, time_dim]], fields=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time])
                  :  +- Calc(select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count], where=[<(hd_vehicle_count, -32)])
                  :     +- TableSourceScan(table=[[default_catalog, default_database, household_demographics, filter=[<(hd_vehicle_count, -32)]]], fields=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
                  +- Exchange(distribution=[broadcast])
                     +- SortLimit(orderBy=[cd_demo_sk ASC], offset=[0], fetch=[1], global=[true])
                        +- Exchange(distribution=[single])
                           +- SortLimit(orderBy=[cd_demo_sk ASC], offset=[0], fetch=[1], global=[false])
                              +- HashJoin(joinType=[InnerJoin], where=[=(p_channel_tv, cd_credit_rating)], select=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], isBroadcast=[true], build=[left])
                                 :- Exchange(distribution=[broadcast])
                                 :  +- TableSourceScan(table=[[default_catalog, default_database, promotion]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])
                                 +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS t_meal_time_node_7])
+- SortAggregate(isMerge=[true], groupBy=[t_shift], select=[t_shift, Final_MAX(max$0) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[t_shift ASC])
         +- Exchange(distribution=[hash[t_shift]])
            +- LocalSortAggregate(groupBy=[t_shift], select=[t_shift, Partial_MAX(t_meal_time) AS max$0])
               +- Exchange(distribution=[forward])
                  +- Sort(orderBy=[t_shift ASC])
                     +- NestedLoopJoin(joinType=[InnerJoin], where=[(t_minute = p_promo_sk)], select=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time, hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], build=[right])
                        :- NestedLoopJoin(joinType=[InnerJoin], where=[(t_second = hd_income_band_sk)], select=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time, hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count], build=[left])
                        :  :- Exchange(distribution=[broadcast])
                        :  :  +- SortLimit(orderBy=[t_meal_time ASC], offset=[0], fetch=[1], global=[true])
                        :  :     +- Exchange(distribution=[single])
                        :  :        +- SortLimit(orderBy=[t_meal_time ASC], offset=[0], fetch=[1], global=[false])
                        :  :           +- TableSourceScan(table=[[default_catalog, default_database, time_dim]], fields=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time])
                        :  +- Calc(select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count], where=[(hd_vehicle_count < -32)])
                        :     +- TableSourceScan(table=[[default_catalog, default_database, household_demographics, filter=[<(hd_vehicle_count, -32)]]], fields=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
                        +- Exchange(distribution=[broadcast])
                           +- SortLimit(orderBy=[cd_demo_sk ASC], offset=[0], fetch=[1], global=[true])
                              +- Exchange(distribution=[single])
                                 +- SortLimit(orderBy=[cd_demo_sk ASC], offset=[0], fetch=[1], global=[false])
                                    +- HashJoin(joinType=[InnerJoin], where=[(p_channel_tv = cd_credit_rating)], select=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], isBroadcast=[true], build=[left])
                                       :- Exchange(distribution=[broadcast])
                                       :  +- TableSourceScan(table=[[default_catalog, default_database, promotion]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])
                                       +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0