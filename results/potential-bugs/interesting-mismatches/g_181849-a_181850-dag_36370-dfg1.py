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
    return values.iloc[0] if len(values) > 0 else None


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_8 = table_env.from_path("promotion").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("promotion").get_schema().get_field_names()])
autonode_7 = table_env.from_path("store").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("store").get_schema().get_field_names()])
autonode_6 = autonode_8.distinct()
autonode_5 = autonode_7.order_by(col('s_city_node_7'))
autonode_4 = autonode_6.order_by(col('p_purpose_node_8'))
autonode_3 = autonode_5.alias('4LAcf')
autonode_2 = autonode_3.join(autonode_4, col('s_tax_precentage_node_7') == col('p_cost_node_8'))
autonode_1 = autonode_2.distinct()
sink = autonode_1.group_by(col('s_closed_date_sk_node_7')).select(col('s_hours_node_7').min.alias('s_hours_node_7'))
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
      "error_message": "An error occurred while calling o99163409.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#199928206:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[17](input=RelSubset#199928204,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[17]), rel#199928203:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[17](input=RelSubset#199928202,groupBy=p_cost,select=p_cost)]
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
Caused by: java.lang.IndexOutOfBoundsException: index (17) must be less than size (1)
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
LogicalProject(s_hours_node_7=[$1])
+- LogicalAggregate(group=[{4}], EXPR$0=[MIN($8)])
   +- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47}])
      +- LogicalJoin(condition=[=($28, $34)], joinType=[inner])
         :- LogicalProject(4LAcf=[AS($0, _UTF-16LE'4LAcf')], s_store_id_node_7=[$1], s_rec_start_date_node_7=[$2], s_rec_end_date_node_7=[$3], s_closed_date_sk_node_7=[$4], s_store_name_node_7=[$5], s_number_employees_node_7=[$6], s_floor_space_node_7=[$7], s_hours_node_7=[$8], s_manager_node_7=[$9], s_market_id_node_7=[$10], s_geography_class_node_7=[$11], s_market_desc_node_7=[$12], s_market_manager_node_7=[$13], s_division_id_node_7=[$14], s_division_name_node_7=[$15], s_company_id_node_7=[$16], s_company_name_node_7=[$17], s_street_number_node_7=[$18], s_street_name_node_7=[$19], s_street_type_node_7=[$20], s_suite_number_node_7=[$21], s_city_node_7=[$22], s_county_node_7=[$23], s_state_node_7=[$24], s_zip_node_7=[$25], s_country_node_7=[$26], s_gmt_offset_node_7=[$27], s_tax_precentage_node_7=[$28])
         :  +- LogicalSort(sort0=[$22], dir0=[ASC])
         :     +- LogicalProject(s_store_sk_node_7=[$0], s_store_id_node_7=[$1], s_rec_start_date_node_7=[$2], s_rec_end_date_node_7=[$3], s_closed_date_sk_node_7=[$4], s_store_name_node_7=[$5], s_number_employees_node_7=[$6], s_floor_space_node_7=[$7], s_hours_node_7=[$8], s_manager_node_7=[$9], s_market_id_node_7=[$10], s_geography_class_node_7=[$11], s_market_desc_node_7=[$12], s_market_manager_node_7=[$13], s_division_id_node_7=[$14], s_division_name_node_7=[$15], s_company_id_node_7=[$16], s_company_name_node_7=[$17], s_street_number_node_7=[$18], s_street_name_node_7=[$19], s_street_type_node_7=[$20], s_suite_number_node_7=[$21], s_city_node_7=[$22], s_county_node_7=[$23], s_state_node_7=[$24], s_zip_node_7=[$25], s_country_node_7=[$26], s_gmt_offset_node_7=[$27], s_tax_precentage_node_7=[$28])
         :        +- LogicalTableScan(table=[[default_catalog, default_database, store]])
         +- LogicalSort(sort0=[$17], dir0=[ASC])
            +- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18}])
               +- LogicalProject(p_promo_sk_node_8=[$0], p_promo_id_node_8=[$1], p_start_date_sk_node_8=[$2], p_end_date_sk_node_8=[$3], p_item_sk_node_8=[$4], p_cost_node_8=[$5], p_response_target_node_8=[$6], p_promo_name_node_8=[$7], p_channel_dmail_node_8=[$8], p_channel_email_node_8=[$9], p_channel_catalog_node_8=[$10], p_channel_tv_node_8=[$11], p_channel_radio_node_8=[$12], p_channel_press_node_8=[$13], p_channel_event_node_8=[$14], p_channel_demo_node_8=[$15], p_channel_details_node_8=[$16], p_purpose_node_8=[$17], p_discount_active_node_8=[$18])
                  +- LogicalTableScan(table=[[default_catalog, default_database, promotion]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS s_hours_node_7])
+- SortAggregate(isMerge=[false], groupBy=[s_closed_date_sk_node_7], select=[s_closed_date_sk_node_7, MIN(s_hours_node_7) AS EXPR$0])
   +- Calc(select=[4LAcf, s_store_id_node_7, s_rec_start_date_node_7, s_rec_end_date_node_7, s_closed_date_sk_node_7, s_store_name_node_7, s_number_employees_node_7, s_floor_space_node_7, s_hours_node_7, s_manager_node_7, s_market_id_node_7, s_geography_class_node_7, s_market_desc_node_7, s_market_manager_node_7, s_division_id_node_7, s_division_name_node_7, s_company_id_node_7, s_company_name_node_7, s_street_number_node_7, s_street_name_node_7, s_street_type_node_7, s_suite_number_node_7, s_city_node_7, s_county_node_7, s_state_node_7, s_zip_node_7, s_country_node_7, s_gmt_offset_node_7, s_tax_precentage_node_7, p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])
      +- Sort(orderBy=[s_closed_date_sk_node_7 ASC])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[=(s_tax_precentage_node_70, p_cost)], select=[4LAcf, s_store_id_node_7, s_rec_start_date_node_7, s_rec_end_date_node_7, s_closed_date_sk_node_7, s_store_name_node_7, s_number_employees_node_7, s_floor_space_node_7, s_hours_node_7, s_manager_node_7, s_market_id_node_7, s_geography_class_node_7, s_market_desc_node_7, s_market_manager_node_7, s_division_id_node_7, s_division_name_node_7, s_company_id_node_7, s_company_name_node_7, s_street_number_node_7, s_street_name_node_7, s_street_type_node_7, s_suite_number_node_7, s_city_node_7, s_county_node_7, s_state_node_7, s_zip_node_7, s_country_node_7, s_gmt_offset_node_7, s_tax_precentage_node_7, s_tax_precentage_node_70, p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active], build=[right])
            :- Exchange(distribution=[hash[s_closed_date_sk_node_7]])
            :  +- SortAggregate(isMerge=[false], groupBy=[4LAcf, s_store_id_node_7, s_rec_start_date_node_7, s_rec_end_date_node_7, s_closed_date_sk_node_7, s_store_name_node_7, s_number_employees_node_7, s_floor_space_node_7, s_hours_node_7, s_manager_node_7, s_market_id_node_7, s_geography_class_node_7, s_market_desc_node_7, s_market_manager_node_7, s_division_id_node_7, s_division_name_node_7, s_company_id_node_7, s_company_name_node_7, s_street_number_node_7, s_street_name_node_7, s_street_type_node_7, s_suite_number_node_7, s_city_node_7, s_county_node_7, s_state_node_7, s_zip_node_7, s_country_node_7, s_gmt_offset_node_7, s_tax_precentage_node_7, s_tax_precentage_node_70], select=[4LAcf, s_store_id_node_7, s_rec_start_date_node_7, s_rec_end_date_node_7, s_closed_date_sk_node_7, s_store_name_node_7, s_number_employees_node_7, s_floor_space_node_7, s_hours_node_7, s_manager_node_7, s_market_id_node_7, s_geography_class_node_7, s_market_desc_node_7, s_market_manager_node_7, s_division_id_node_7, s_division_name_node_7, s_company_id_node_7, s_company_name_node_7, s_street_number_node_7, s_street_name_node_7, s_street_type_node_7, s_suite_number_node_7, s_city_node_7, s_county_node_7, s_state_node_7, s_zip_node_7, s_country_node_7, s_gmt_offset_node_7, s_tax_precentage_node_7, s_tax_precentage_node_70])
            :     +- Sort(orderBy=[4LAcf ASC, s_store_id_node_7 ASC, s_rec_start_date_node_7 ASC, s_rec_end_date_node_7 ASC, s_closed_date_sk_node_7 ASC, s_store_name_node_7 ASC, s_number_employees_node_7 ASC, s_floor_space_node_7 ASC, s_hours_node_7 ASC, s_manager_node_7 ASC, s_market_id_node_7 ASC, s_geography_class_node_7 ASC, s_market_desc_node_7 ASC, s_market_manager_node_7 ASC, s_division_id_node_7 ASC, s_division_name_node_7 ASC, s_company_id_node_7 ASC, s_company_name_node_7 ASC, s_street_number_node_7 ASC, s_street_name_node_7 ASC, s_street_type_node_7 ASC, s_suite_number_node_7 ASC, s_city_node_7 ASC, s_county_node_7 ASC, s_state_node_7 ASC, s_zip_node_7 ASC, s_country_node_7 ASC, s_gmt_offset_node_7 ASC, s_tax_precentage_node_7 ASC, s_tax_precentage_node_70 ASC])
            :        +- Exchange(distribution=[hash[4LAcf, s_store_id_node_7, s_rec_start_date_node_7, s_rec_end_date_node_7, s_closed_date_sk_node_7, s_store_name_node_7, s_number_employees_node_7, s_floor_space_node_7, s_hours_node_7, s_manager_node_7, s_market_id_node_7, s_geography_class_node_7, s_market_desc_node_7, s_market_manager_node_7, s_division_id_node_7, s_division_name_node_7, s_company_id_node_7, s_company_name_node_7, s_street_number_node_7, s_street_name_node_7, s_street_type_node_7, s_suite_number_node_7, s_city_node_7, s_county_node_7, s_state_node_7, s_zip_node_7, s_country_node_7, s_gmt_offset_node_7, s_tax_precentage_node_7, s_tax_precentage_node_70]])
            :           +- Calc(select=[s_store_sk AS 4LAcf, s_store_id AS s_store_id_node_7, s_rec_start_date AS s_rec_start_date_node_7, s_rec_end_date AS s_rec_end_date_node_7, s_closed_date_sk AS s_closed_date_sk_node_7, s_store_name AS s_store_name_node_7, s_number_employees AS s_number_employees_node_7, s_floor_space AS s_floor_space_node_7, s_hours AS s_hours_node_7, s_manager AS s_manager_node_7, s_market_id AS s_market_id_node_7, s_geography_class AS s_geography_class_node_7, s_market_desc AS s_market_desc_node_7, s_market_manager AS s_market_manager_node_7, s_division_id AS s_division_id_node_7, s_division_name AS s_division_name_node_7, s_company_id AS s_company_id_node_7, s_company_name AS s_company_name_node_7, s_street_number AS s_street_number_node_7, s_street_name AS s_street_name_node_7, s_street_type AS s_street_type_node_7, s_suite_number AS s_suite_number_node_7, s_city AS s_city_node_7, s_county AS s_county_node_7, s_state AS s_state_node_7, s_zip AS s_zip_node_7, s_country AS s_country_node_7, s_gmt_offset AS s_gmt_offset_node_7, s_tax_precentage AS s_tax_precentage_node_7, CAST(s_tax_precentage AS DECIMAL(15, 2)) AS s_tax_precentage_node_70])
            :              +- SortLimit(orderBy=[s_city ASC], offset=[0], fetch=[1], global=[true])
            :                 +- Exchange(distribution=[single])
            :                    +- SortLimit(orderBy=[s_city ASC], offset=[0], fetch=[1], global=[false])
            :                       +- TableSourceScan(table=[[default_catalog, default_database, store]], fields=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage])
            +- Exchange(distribution=[broadcast])
               +- SortLimit(orderBy=[p_purpose ASC], offset=[0], fetch=[1], global=[true])
                  +- Exchange(distribution=[single])
                     +- SortLimit(orderBy=[p_purpose ASC], offset=[0], fetch=[1], global=[false])
                        +- HashAggregate(isMerge=[false], groupBy=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active], select=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])
                           +- Exchange(distribution=[hash[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active]])
                              +- TableSourceScan(table=[[default_catalog, default_database, promotion]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS s_hours_node_7])
+- SortAggregate(isMerge=[false], groupBy=[s_closed_date_sk_node_7], select=[s_closed_date_sk_node_7, MIN(s_hours_node_7) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Calc(select=[4LAcf, s_store_id_node_7, s_rec_start_date_node_7, s_rec_end_date_node_7, s_closed_date_sk_node_7, s_store_name_node_7, s_number_employees_node_7, s_floor_space_node_7, s_hours_node_7, s_manager_node_7, s_market_id_node_7, s_geography_class_node_7, s_market_desc_node_7, s_market_manager_node_7, s_division_id_node_7, s_division_name_node_7, s_company_id_node_7, s_company_name_node_7, s_street_number_node_7, s_street_name_node_7, s_street_type_node_7, s_suite_number_node_7, s_city_node_7, s_county_node_7, s_state_node_7, s_zip_node_7, s_country_node_7, s_gmt_offset_node_7, s_tax_precentage_node_7, p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])
         +- Exchange(distribution=[forward])
            +- Sort(orderBy=[s_closed_date_sk_node_7 ASC])
               +- Exchange(distribution=[forward])
                  +- NestedLoopJoin(joinType=[InnerJoin], where=[(s_tax_precentage_node_70 = p_cost)], select=[4LAcf, s_store_id_node_7, s_rec_start_date_node_7, s_rec_end_date_node_7, s_closed_date_sk_node_7, s_store_name_node_7, s_number_employees_node_7, s_floor_space_node_7, s_hours_node_7, s_manager_node_7, s_market_id_node_7, s_geography_class_node_7, s_market_desc_node_7, s_market_manager_node_7, s_division_id_node_7, s_division_name_node_7, s_company_id_node_7, s_company_name_node_7, s_street_number_node_7, s_street_name_node_7, s_street_type_node_7, s_suite_number_node_7, s_city_node_7, s_county_node_7, s_state_node_7, s_zip_node_7, s_country_node_7, s_gmt_offset_node_7, s_tax_precentage_node_7, s_tax_precentage_node_70, p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active], build=[right])
                     :- Exchange(distribution=[hash[s_closed_date_sk_node_7]])
                     :  +- SortAggregate(isMerge=[false], groupBy=[4LAcf, s_store_id_node_7, s_rec_start_date_node_7, s_rec_end_date_node_7, s_closed_date_sk_node_7, s_store_name_node_7, s_number_employees_node_7, s_floor_space_node_7, s_hours_node_7, s_manager_node_7, s_market_id_node_7, s_geography_class_node_7, s_market_desc_node_7, s_market_manager_node_7, s_division_id_node_7, s_division_name_node_7, s_company_id_node_7, s_company_name_node_7, s_street_number_node_7, s_street_name_node_7, s_street_type_node_7, s_suite_number_node_7, s_city_node_7, s_county_node_7, s_state_node_7, s_zip_node_7, s_country_node_7, s_gmt_offset_node_7, s_tax_precentage_node_7, s_tax_precentage_node_70], select=[4LAcf, s_store_id_node_7, s_rec_start_date_node_7, s_rec_end_date_node_7, s_closed_date_sk_node_7, s_store_name_node_7, s_number_employees_node_7, s_floor_space_node_7, s_hours_node_7, s_manager_node_7, s_market_id_node_7, s_geography_class_node_7, s_market_desc_node_7, s_market_manager_node_7, s_division_id_node_7, s_division_name_node_7, s_company_id_node_7, s_company_name_node_7, s_street_number_node_7, s_street_name_node_7, s_street_type_node_7, s_suite_number_node_7, s_city_node_7, s_county_node_7, s_state_node_7, s_zip_node_7, s_country_node_7, s_gmt_offset_node_7, s_tax_precentage_node_7, s_tax_precentage_node_70])
                     :     +- Exchange(distribution=[forward])
                     :        +- Sort(orderBy=[4LAcf ASC, s_store_id_node_7 ASC, s_rec_start_date_node_7 ASC, s_rec_end_date_node_7 ASC, s_closed_date_sk_node_7 ASC, s_store_name_node_7 ASC, s_number_employees_node_7 ASC, s_floor_space_node_7 ASC, s_hours_node_7 ASC, s_manager_node_7 ASC, s_market_id_node_7 ASC, s_geography_class_node_7 ASC, s_market_desc_node_7 ASC, s_market_manager_node_7 ASC, s_division_id_node_7 ASC, s_division_name_node_7 ASC, s_company_id_node_7 ASC, s_company_name_node_7 ASC, s_street_number_node_7 ASC, s_street_name_node_7 ASC, s_street_type_node_7 ASC, s_suite_number_node_7 ASC, s_city_node_7 ASC, s_county_node_7 ASC, s_state_node_7 ASC, s_zip_node_7 ASC, s_country_node_7 ASC, s_gmt_offset_node_7 ASC, s_tax_precentage_node_7 ASC, s_tax_precentage_node_70 ASC])
                     :           +- Exchange(distribution=[hash[4LAcf, s_store_id_node_7, s_rec_start_date_node_7, s_rec_end_date_node_7, s_closed_date_sk_node_7, s_store_name_node_7, s_number_employees_node_7, s_floor_space_node_7, s_hours_node_7, s_manager_node_7, s_market_id_node_7, s_geography_class_node_7, s_market_desc_node_7, s_market_manager_node_7, s_division_id_node_7, s_division_name_node_7, s_company_id_node_7, s_company_name_node_7, s_street_number_node_7, s_street_name_node_7, s_street_type_node_7, s_suite_number_node_7, s_city_node_7, s_county_node_7, s_state_node_7, s_zip_node_7, s_country_node_7, s_gmt_offset_node_7, s_tax_precentage_node_7, s_tax_precentage_node_70]])
                     :              +- Calc(select=[s_store_sk AS 4LAcf, s_store_id AS s_store_id_node_7, s_rec_start_date AS s_rec_start_date_node_7, s_rec_end_date AS s_rec_end_date_node_7, s_closed_date_sk AS s_closed_date_sk_node_7, s_store_name AS s_store_name_node_7, s_number_employees AS s_number_employees_node_7, s_floor_space AS s_floor_space_node_7, s_hours AS s_hours_node_7, s_manager AS s_manager_node_7, s_market_id AS s_market_id_node_7, s_geography_class AS s_geography_class_node_7, s_market_desc AS s_market_desc_node_7, s_market_manager AS s_market_manager_node_7, s_division_id AS s_division_id_node_7, s_division_name AS s_division_name_node_7, s_company_id AS s_company_id_node_7, s_company_name AS s_company_name_node_7, s_street_number AS s_street_number_node_7, s_street_name AS s_street_name_node_7, s_street_type AS s_street_type_node_7, s_suite_number AS s_suite_number_node_7, s_city AS s_city_node_7, s_county AS s_county_node_7, s_state AS s_state_node_7, s_zip AS s_zip_node_7, s_country AS s_country_node_7, s_gmt_offset AS s_gmt_offset_node_7, s_tax_precentage AS s_tax_precentage_node_7, CAST(s_tax_precentage AS DECIMAL(15, 2)) AS s_tax_precentage_node_70])
                     :                 +- SortLimit(orderBy=[s_city ASC], offset=[0], fetch=[1], global=[true])
                     :                    +- Exchange(distribution=[single])
                     :                       +- SortLimit(orderBy=[s_city ASC], offset=[0], fetch=[1], global=[false])
                     :                          +- TableSourceScan(table=[[default_catalog, default_database, store]], fields=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage])
                     +- Exchange(distribution=[broadcast])
                        +- SortLimit(orderBy=[p_purpose ASC], offset=[0], fetch=[1], global=[true])
                           +- Exchange(distribution=[single])
                              +- SortLimit(orderBy=[p_purpose ASC], offset=[0], fetch=[1], global=[false])
                                 +- HashAggregate(isMerge=[false], groupBy=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active], select=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])
                                    +- Exchange(distribution=[hash[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active]])
                                       +- TableSourceScan(table=[[default_catalog, default_database, promotion]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0