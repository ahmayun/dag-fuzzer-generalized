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
    return values.iloc[-1] if len(values) > 0 else None


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_4 = table_env.from_path("call_center").select(*[col(column_name).alias(f"{column_name}_node_4") for column_name in table_env.from_path("call_center").get_schema().get_field_names()])
autonode_5 = table_env.from_path("promotion").select(*[col(column_name).alias(f"{column_name}_node_5") for column_name in table_env.from_path("promotion").get_schema().get_field_names()])
autonode_2 = autonode_4.limit(50)
autonode_3 = autonode_5.order_by(col('p_channel_tv_node_5'))
autonode_1 = autonode_2.join(autonode_3, col('p_start_date_sk_node_5') == col('cc_employees_node_4'))
sink = autonode_1.group_by(col('cc_company_name_node_4')).select(col('cc_country_node_4').min.alias('cc_country_node_4'))
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
      "error_message": "An error occurred while calling o168502092.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#340487934:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[11](input=RelSubset#340487932,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[11]), rel#340487931:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[11](input=RelSubset#340487930,groupBy=p_start_date_sk,select=p_start_date_sk)]
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
Caused by: java.lang.IndexOutOfBoundsException: index (11) must be less than size (1)
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
LogicalProject(cc_country_node_4=[$1])
+- LogicalAggregate(group=[{19}], EXPR$0=[MIN($28)])
   +- LogicalJoin(condition=[=($33, $8)], joinType=[inner])
      :- LogicalSort(fetch=[50])
      :  +- LogicalProject(cc_call_center_sk_node_4=[$0], cc_call_center_id_node_4=[$1], cc_rec_start_date_node_4=[$2], cc_rec_end_date_node_4=[$3], cc_closed_date_sk_node_4=[$4], cc_open_date_sk_node_4=[$5], cc_name_node_4=[$6], cc_class_node_4=[$7], cc_employees_node_4=[$8], cc_sq_ft_node_4=[$9], cc_hours_node_4=[$10], cc_manager_node_4=[$11], cc_mkt_id_node_4=[$12], cc_mkt_class_node_4=[$13], cc_mkt_desc_node_4=[$14], cc_market_manager_node_4=[$15], cc_division_node_4=[$16], cc_division_name_node_4=[$17], cc_company_node_4=[$18], cc_company_name_node_4=[$19], cc_street_number_node_4=[$20], cc_street_name_node_4=[$21], cc_street_type_node_4=[$22], cc_suite_number_node_4=[$23], cc_city_node_4=[$24], cc_county_node_4=[$25], cc_state_node_4=[$26], cc_zip_node_4=[$27], cc_country_node_4=[$28], cc_gmt_offset_node_4=[$29], cc_tax_percentage_node_4=[$30])
      :     +- LogicalTableScan(table=[[default_catalog, default_database, call_center]])
      +- LogicalSort(sort0=[$11], dir0=[ASC])
         +- LogicalProject(p_promo_sk_node_5=[$0], p_promo_id_node_5=[$1], p_start_date_sk_node_5=[$2], p_end_date_sk_node_5=[$3], p_item_sk_node_5=[$4], p_cost_node_5=[$5], p_response_target_node_5=[$6], p_promo_name_node_5=[$7], p_channel_dmail_node_5=[$8], p_channel_email_node_5=[$9], p_channel_catalog_node_5=[$10], p_channel_tv_node_5=[$11], p_channel_radio_node_5=[$12], p_channel_press_node_5=[$13], p_channel_event_node_5=[$14], p_channel_demo_node_5=[$15], p_channel_details_node_5=[$16], p_purpose_node_5=[$17], p_discount_active_node_5=[$18])
            +- LogicalTableScan(table=[[default_catalog, default_database, promotion]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS cc_country_node_4])
+- SortAggregate(isMerge=[true], groupBy=[cc_company_name], select=[cc_company_name, Final_MIN(min$0) AS EXPR$0])
   +- Sort(orderBy=[cc_company_name ASC])
      +- Exchange(distribution=[hash[cc_company_name]])
         +- LocalSortAggregate(groupBy=[cc_company_name], select=[cc_company_name, Partial_MIN(cc_country) AS min$0])
            +- Sort(orderBy=[cc_company_name ASC])
               +- NestedLoopJoin(joinType=[InnerJoin], where=[=(p_start_date_sk, cc_employees)], select=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage, p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active], build=[right])
                  :- Limit(offset=[0], fetch=[50], global=[true])
                  :  +- Exchange(distribution=[single])
                  :     +- Limit(offset=[0], fetch=[50], global=[false])
                  :        +- TableSourceScan(table=[[default_catalog, default_database, call_center]], fields=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage])
                  +- Exchange(distribution=[broadcast])
                     +- SortLimit(orderBy=[p_channel_tv ASC], offset=[0], fetch=[1], global=[true])
                        +- Exchange(distribution=[single])
                           +- SortLimit(orderBy=[p_channel_tv ASC], offset=[0], fetch=[1], global=[false])
                              +- TableSourceScan(table=[[default_catalog, default_database, promotion]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS cc_country_node_4])
+- SortAggregate(isMerge=[true], groupBy=[cc_company_name], select=[cc_company_name, Final_MIN(min$0) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[cc_company_name ASC])
         +- Exchange(distribution=[hash[cc_company_name]])
            +- LocalSortAggregate(groupBy=[cc_company_name], select=[cc_company_name, Partial_MIN(cc_country) AS min$0])
               +- Exchange(distribution=[forward])
                  +- Sort(orderBy=[cc_company_name ASC])
                     +- NestedLoopJoin(joinType=[InnerJoin], where=[(p_start_date_sk = cc_employees)], select=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage, p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active], build=[right])
                        :- Limit(offset=[0], fetch=[50], global=[true])
                        :  +- Exchange(distribution=[single])
                        :     +- Limit(offset=[0], fetch=[50], global=[false])
                        :        +- TableSourceScan(table=[[default_catalog, default_database, call_center]], fields=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage])
                        +- Exchange(distribution=[broadcast])
                           +- SortLimit(orderBy=[p_channel_tv ASC], offset=[0], fetch=[1], global=[true])
                              +- Exchange(distribution=[single])
                                 +- SortLimit(orderBy=[p_channel_tv ASC], offset=[0], fetch=[1], global=[false])
                                    +- TableSourceScan(table=[[default_catalog, default_database, promotion]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0