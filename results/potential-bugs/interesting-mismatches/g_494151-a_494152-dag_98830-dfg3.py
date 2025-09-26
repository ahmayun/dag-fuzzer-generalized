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
    return values.skew()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_25 = table_env.from_path("web_page").select(*[col(column_name).alias(f"{column_name}_node_25") for column_name in table_env.from_path("web_page").get_schema().get_field_names()])
autonode_24 = table_env.from_path("store").select(*[col(column_name).alias(f"{column_name}_node_24") for column_name in table_env.from_path("store").get_schema().get_field_names()])
autonode_21 = table_env.from_path("catalog_returns").select(*[col(column_name).alias(f"{column_name}_node_21") for column_name in table_env.from_path("catalog_returns").get_schema().get_field_names()])
autonode_23 = table_env.from_path("ship_mode").select(*[col(column_name).alias(f"{column_name}_node_23") for column_name in table_env.from_path("ship_mode").get_schema().get_field_names()])
autonode_22 = table_env.from_path("call_center").select(*[col(column_name).alias(f"{column_name}_node_22") for column_name in table_env.from_path("call_center").get_schema().get_field_names()])
autonode_20 = autonode_25.alias('DQAeC')
autonode_19 = autonode_24.select(col('s_street_type_node_24'))
autonode_16 = autonode_21.select(col('cr_catalog_page_sk_node_21'))
autonode_18 = autonode_23.add_columns(lit("hello"))
autonode_17 = autonode_22.limit(11)
autonode_15 = autonode_20.order_by(col('wp_link_count_node_25'))
autonode_14 = autonode_19.filter(col('s_street_type_node_24').char_length <= 5)
autonode_12 = autonode_16.group_by(col('cr_catalog_page_sk_node_21')).select(col('cr_catalog_page_sk_node_21').sum.alias('cr_catalog_page_sk_node_21'))
autonode_13 = autonode_17.join(autonode_18, col('sm_ship_mode_sk_node_23') == col('cc_sq_ft_node_22'))
autonode_11 = autonode_15.distinct()
autonode_10 = autonode_14.filter(col('s_street_type_node_24').char_length < 5)
autonode_8 = autonode_12.add_columns(lit("hello"))
autonode_9 = autonode_13.alias('2sfxN')
autonode_7 = autonode_11.add_columns(lit("hello"))
autonode_5 = autonode_8.order_by(col('cr_catalog_page_sk_node_21'))
autonode_6 = autonode_9.join(autonode_10, col('cc_rec_start_date_node_22') == col('s_street_type_node_24'))
autonode_4 = autonode_7.limit(27)
autonode_3 = autonode_5.join(autonode_6, col('cc_company_node_22') == col('cr_catalog_page_sk_node_21'))
autonode_2 = autonode_4.distinct()
autonode_1 = autonode_3.group_by(col('cc_company_name_node_22')).select(col('cc_company_name_node_22').min.alias('cc_company_name_node_22'))
sink = autonode_1.join(autonode_2, col('wp_rec_start_date_node_25') == col('cc_company_name_node_22'))
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
      "error_message": "An error occurred while calling o269063754.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#544070556:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[1](input=RelSubset#544070554,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[1]), rel#544070553:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[1](input=RelSubset#544070552,groupBy=EXPR$0,select=EXPR$0)]
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
Caused by: java.lang.IndexOutOfBoundsException: index (1) must be less than size (1)
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
LogicalJoin(condition=[=($3, $0)], joinType=[inner])
:- LogicalProject(cc_company_name_node_22=[$1])
:  +- LogicalAggregate(group=[{21}], EXPR$0=[MIN($21)])
:     +- LogicalJoin(condition=[=($20, $0)], joinType=[inner])
:        :- LogicalSort(sort0=[$0], dir0=[ASC])
:        :  +- LogicalProject(cr_catalog_page_sk_node_21=[$1], _c1=[_UTF-16LE'hello'])
:        :     +- LogicalAggregate(group=[{0}], EXPR$0=[SUM($0)])
:        :        +- LogicalProject(cr_catalog_page_sk_node_21=[$12])
:        :           +- LogicalTableScan(table=[[default_catalog, default_database, catalog_returns]])
:        +- LogicalJoin(condition=[=($2, $38)], joinType=[inner])
:           :- LogicalProject(2sfxN=[AS($0, _UTF-16LE'2sfxN')], cc_call_center_id_node_22=[$1], cc_rec_start_date_node_22=[$2], cc_rec_end_date_node_22=[$3], cc_closed_date_sk_node_22=[$4], cc_open_date_sk_node_22=[$5], cc_name_node_22=[$6], cc_class_node_22=[$7], cc_employees_node_22=[$8], cc_sq_ft_node_22=[$9], cc_hours_node_22=[$10], cc_manager_node_22=[$11], cc_mkt_id_node_22=[$12], cc_mkt_class_node_22=[$13], cc_mkt_desc_node_22=[$14], cc_market_manager_node_22=[$15], cc_division_node_22=[$16], cc_division_name_node_22=[$17], cc_company_node_22=[$18], cc_company_name_node_22=[$19], cc_street_number_node_22=[$20], cc_street_name_node_22=[$21], cc_street_type_node_22=[$22], cc_suite_number_node_22=[$23], cc_city_node_22=[$24], cc_county_node_22=[$25], cc_state_node_22=[$26], cc_zip_node_22=[$27], cc_country_node_22=[$28], cc_gmt_offset_node_22=[$29], cc_tax_percentage_node_22=[$30], sm_ship_mode_sk_node_23=[$31], sm_ship_mode_id_node_23=[$32], sm_type_node_23=[$33], sm_code_node_23=[$34], sm_carrier_node_23=[$35], sm_contract_node_23=[$36], _c6=[$37])
:           :  +- LogicalJoin(condition=[=($31, $9)], joinType=[inner])
:           :     :- LogicalSort(fetch=[11])
:           :     :  +- LogicalProject(cc_call_center_sk_node_22=[$0], cc_call_center_id_node_22=[$1], cc_rec_start_date_node_22=[$2], cc_rec_end_date_node_22=[$3], cc_closed_date_sk_node_22=[$4], cc_open_date_sk_node_22=[$5], cc_name_node_22=[$6], cc_class_node_22=[$7], cc_employees_node_22=[$8], cc_sq_ft_node_22=[$9], cc_hours_node_22=[$10], cc_manager_node_22=[$11], cc_mkt_id_node_22=[$12], cc_mkt_class_node_22=[$13], cc_mkt_desc_node_22=[$14], cc_market_manager_node_22=[$15], cc_division_node_22=[$16], cc_division_name_node_22=[$17], cc_company_node_22=[$18], cc_company_name_node_22=[$19], cc_street_number_node_22=[$20], cc_street_name_node_22=[$21], cc_street_type_node_22=[$22], cc_suite_number_node_22=[$23], cc_city_node_22=[$24], cc_county_node_22=[$25], cc_state_node_22=[$26], cc_zip_node_22=[$27], cc_country_node_22=[$28], cc_gmt_offset_node_22=[$29], cc_tax_percentage_node_22=[$30])
:           :     :     +- LogicalTableScan(table=[[default_catalog, default_database, call_center]])
:           :     +- LogicalProject(sm_ship_mode_sk_node_23=[$0], sm_ship_mode_id_node_23=[$1], sm_type_node_23=[$2], sm_code_node_23=[$3], sm_carrier_node_23=[$4], sm_contract_node_23=[$5], _c6=[_UTF-16LE'hello'])
:           :        +- LogicalTableScan(table=[[default_catalog, default_database, ship_mode]])
:           +- LogicalFilter(condition=[<(CHAR_LENGTH($0), 5)])
:              +- LogicalFilter(condition=[<=(CHAR_LENGTH($0), 5)])
:                 +- LogicalProject(s_street_type_node_24=[$20])
:                    +- LogicalTableScan(table=[[default_catalog, default_database, store]])
+- LogicalSort(fetch=[27])
   +- LogicalProject(DQAeC=[$0], wp_web_page_id_node_25=[$1], wp_rec_start_date_node_25=[$2], wp_rec_end_date_node_25=[$3], wp_creation_date_sk_node_25=[$4], wp_access_date_sk_node_25=[$5], wp_autogen_flag_node_25=[$6], wp_customer_sk_node_25=[$7], wp_url_node_25=[$8], wp_type_node_25=[$9], wp_char_count_node_25=[$10], wp_link_count_node_25=[$11], wp_image_count_node_25=[$12], wp_max_ad_count_node_25=[$13], _c14=[_UTF-16LE'hello'])
      +- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}])
         +- LogicalSort(sort0=[$11], dir0=[ASC])
            +- LogicalProject(DQAeC=[AS($0, _UTF-16LE'DQAeC')], wp_web_page_id_node_25=[$1], wp_rec_start_date_node_25=[$2], wp_rec_end_date_node_25=[$3], wp_creation_date_sk_node_25=[$4], wp_access_date_sk_node_25=[$5], wp_autogen_flag_node_25=[$6], wp_customer_sk_node_25=[$7], wp_url_node_25=[$8], wp_type_node_25=[$9], wp_char_count_node_25=[$10], wp_link_count_node_25=[$11], wp_image_count_node_25=[$12], wp_max_ad_count_node_25=[$13])
               +- LogicalTableScan(table=[[default_catalog, default_database, web_page]])

== Optimized Physical Plan ==
NestedLoopJoin(joinType=[InnerJoin], where=[=(wp_rec_start_date_node_25, cc_company_name_node_22)], select=[cc_company_name_node_22, DQAeC, wp_web_page_id_node_25, wp_rec_start_date_node_25, wp_rec_end_date_node_25, wp_creation_date_sk_node_25, wp_access_date_sk_node_25, wp_autogen_flag_node_25, wp_customer_sk_node_25, wp_url_node_25, wp_type_node_25, wp_char_count_node_25, wp_link_count_node_25, wp_image_count_node_25, wp_max_ad_count_node_25, _c14], build=[left])
:- Exchange(distribution=[broadcast])
:  +- Calc(select=[EXPR$0 AS cc_company_name_node_22])
:     +- SortAggregate(isMerge=[false], groupBy=[cc_company_name_node_22], select=[cc_company_name_node_22, MIN(cc_company_name_node_22) AS EXPR$0])
:        +- Sort(orderBy=[cc_company_name_node_22 ASC])
:           +- Exchange(distribution=[hash[cc_company_name_node_22]])
:              +- NestedLoopJoin(joinType=[InnerJoin], where=[=(cc_company_node_22, cr_catalog_page_sk_node_21)], select=[cr_catalog_page_sk_node_21, _c1, 2sfxN, cc_call_center_id_node_22, cc_rec_start_date_node_22, cc_rec_end_date_node_22, cc_closed_date_sk_node_22, cc_open_date_sk_node_22, cc_name_node_22, cc_class_node_22, cc_employees_node_22, cc_sq_ft_node_22, cc_hours_node_22, cc_manager_node_22, cc_mkt_id_node_22, cc_mkt_class_node_22, cc_mkt_desc_node_22, cc_market_manager_node_22, cc_division_node_22, cc_division_name_node_22, cc_company_node_22, cc_company_name_node_22, cc_street_number_node_22, cc_street_name_node_22, cc_street_type_node_22, cc_suite_number_node_22, cc_city_node_22, cc_county_node_22, cc_state_node_22, cc_zip_node_22, cc_country_node_22, cc_gmt_offset_node_22, cc_tax_percentage_node_22, sm_ship_mode_sk_node_23, sm_ship_mode_id_node_23, sm_type_node_23, sm_code_node_23, sm_carrier_node_23, sm_contract_node_23, _c6, s_street_type], build=[left])
:                 :- Exchange(distribution=[broadcast])
:                 :  +- Calc(select=[EXPR$0 AS cr_catalog_page_sk_node_21, 'hello' AS _c1])
:                 :     +- SortLimit(orderBy=[EXPR$0 ASC], offset=[0], fetch=[1], global=[true])
:                 :        +- Exchange(distribution=[single])
:                 :           +- SortLimit(orderBy=[EXPR$0 ASC], offset=[0], fetch=[1], global=[false])
:                 :              +- HashAggregate(isMerge=[true], groupBy=[cr_catalog_page_sk], select=[cr_catalog_page_sk, Final_SUM(sum$0) AS EXPR$0])
:                 :                 +- Exchange(distribution=[hash[cr_catalog_page_sk]])
:                 :                    +- LocalHashAggregate(groupBy=[cr_catalog_page_sk], select=[cr_catalog_page_sk, Partial_SUM(cr_catalog_page_sk) AS sum$0])
:                 :                       +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns, project=[cr_catalog_page_sk], metadata=[]]], fields=[cr_catalog_page_sk])
:                 +- NestedLoopJoin(joinType=[InnerJoin], where=[=(cc_rec_start_date_node_22, s_street_type)], select=[2sfxN, cc_call_center_id_node_22, cc_rec_start_date_node_22, cc_rec_end_date_node_22, cc_closed_date_sk_node_22, cc_open_date_sk_node_22, cc_name_node_22, cc_class_node_22, cc_employees_node_22, cc_sq_ft_node_22, cc_hours_node_22, cc_manager_node_22, cc_mkt_id_node_22, cc_mkt_class_node_22, cc_mkt_desc_node_22, cc_market_manager_node_22, cc_division_node_22, cc_division_name_node_22, cc_company_node_22, cc_company_name_node_22, cc_street_number_node_22, cc_street_name_node_22, cc_street_type_node_22, cc_suite_number_node_22, cc_city_node_22, cc_county_node_22, cc_state_node_22, cc_zip_node_22, cc_country_node_22, cc_gmt_offset_node_22, cc_tax_percentage_node_22, sm_ship_mode_sk_node_23, sm_ship_mode_id_node_23, sm_type_node_23, sm_code_node_23, sm_carrier_node_23, sm_contract_node_23, _c6, s_street_type], build=[right])
:                    :- Calc(select=[cc_call_center_sk AS 2sfxN, cc_call_center_id AS cc_call_center_id_node_22, cc_rec_start_date AS cc_rec_start_date_node_22, cc_rec_end_date AS cc_rec_end_date_node_22, cc_closed_date_sk AS cc_closed_date_sk_node_22, cc_open_date_sk AS cc_open_date_sk_node_22, cc_name AS cc_name_node_22, cc_class AS cc_class_node_22, cc_employees AS cc_employees_node_22, cc_sq_ft AS cc_sq_ft_node_22, cc_hours AS cc_hours_node_22, cc_manager AS cc_manager_node_22, cc_mkt_id AS cc_mkt_id_node_22, cc_mkt_class AS cc_mkt_class_node_22, cc_mkt_desc AS cc_mkt_desc_node_22, cc_market_manager AS cc_market_manager_node_22, cc_division AS cc_division_node_22, cc_division_name AS cc_division_name_node_22, cc_company AS cc_company_node_22, cc_company_name AS cc_company_name_node_22, cc_street_number AS cc_street_number_node_22, cc_street_name AS cc_street_name_node_22, cc_street_type AS cc_street_type_node_22, cc_suite_number AS cc_suite_number_node_22, cc_city AS cc_city_node_22, cc_county AS cc_county_node_22, cc_state AS cc_state_node_22, cc_zip AS cc_zip_node_22, cc_country AS cc_country_node_22, cc_gmt_offset AS cc_gmt_offset_node_22, cc_tax_percentage AS cc_tax_percentage_node_22, sm_ship_mode_sk AS sm_ship_mode_sk_node_23, sm_ship_mode_id AS sm_ship_mode_id_node_23, sm_type AS sm_type_node_23, sm_code AS sm_code_node_23, sm_carrier AS sm_carrier_node_23, sm_contract AS sm_contract_node_23, 'hello' AS _c6])
:                    :  +- NestedLoopJoin(joinType=[InnerJoin], where=[=(sm_ship_mode_sk, cc_sq_ft)], select=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage, sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract], build=[right])
:                    :     :- Limit(offset=[0], fetch=[11], global=[true])
:                    :     :  +- Exchange(distribution=[single])
:                    :     :     +- Limit(offset=[0], fetch=[11], global=[false])
:                    :     :        +- TableSourceScan(table=[[default_catalog, default_database, call_center]], fields=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage])
:                    :     +- Exchange(distribution=[broadcast])
:                    :        +- TableSourceScan(table=[[default_catalog, default_database, ship_mode]], fields=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract])
:                    +- Exchange(distribution=[broadcast])
:                       +- Calc(select=[s_street_type], where=[AND(<=(CHAR_LENGTH(s_street_type), 5), <(CHAR_LENGTH(s_street_type), 5))])
:                          +- TableSourceScan(table=[[default_catalog, default_database, store, filter=[and(<=(CHAR_LENGTH(s_street_type), 5), <(CHAR_LENGTH(s_street_type), 5))], project=[s_street_type], metadata=[]]], fields=[s_street_type])
+- Limit(offset=[0], fetch=[27], global=[true])
   +- Exchange(distribution=[single])
      +- Limit(offset=[0], fetch=[27], global=[false])
         +- Calc(select=[DQAeC, wp_web_page_id_node_25, wp_rec_start_date_node_25, wp_rec_end_date_node_25, wp_creation_date_sk_node_25, wp_access_date_sk_node_25, wp_autogen_flag_node_25, wp_customer_sk_node_25, wp_url_node_25, wp_type_node_25, wp_char_count_node_25, wp_link_count_node_25, wp_image_count_node_25, wp_max_ad_count_node_25, 'hello' AS _c14])
            +- SortAggregate(isMerge=[false], groupBy=[DQAeC, wp_web_page_id_node_25, wp_rec_start_date_node_25, wp_rec_end_date_node_25, wp_creation_date_sk_node_25, wp_access_date_sk_node_25, wp_autogen_flag_node_25, wp_customer_sk_node_25, wp_url_node_25, wp_type_node_25, wp_char_count_node_25, wp_link_count_node_25, wp_image_count_node_25, wp_max_ad_count_node_25], select=[DQAeC, wp_web_page_id_node_25, wp_rec_start_date_node_25, wp_rec_end_date_node_25, wp_creation_date_sk_node_25, wp_access_date_sk_node_25, wp_autogen_flag_node_25, wp_customer_sk_node_25, wp_url_node_25, wp_type_node_25, wp_char_count_node_25, wp_link_count_node_25, wp_image_count_node_25, wp_max_ad_count_node_25])
               +- Sort(orderBy=[DQAeC ASC, wp_web_page_id_node_25 ASC, wp_rec_start_date_node_25 ASC, wp_rec_end_date_node_25 ASC, wp_creation_date_sk_node_25 ASC, wp_access_date_sk_node_25 ASC, wp_autogen_flag_node_25 ASC, wp_customer_sk_node_25 ASC, wp_url_node_25 ASC, wp_type_node_25 ASC, wp_char_count_node_25 ASC, wp_link_count_node_25 ASC, wp_image_count_node_25 ASC, wp_max_ad_count_node_25 ASC])
                  +- Exchange(distribution=[hash[DQAeC, wp_web_page_id_node_25, wp_rec_start_date_node_25, wp_rec_end_date_node_25, wp_creation_date_sk_node_25, wp_access_date_sk_node_25, wp_autogen_flag_node_25, wp_customer_sk_node_25, wp_url_node_25, wp_type_node_25, wp_char_count_node_25, wp_link_count_node_25, wp_image_count_node_25, wp_max_ad_count_node_25]])
                     +- Calc(select=[wp_web_page_sk AS DQAeC, wp_web_page_id AS wp_web_page_id_node_25, wp_rec_start_date AS wp_rec_start_date_node_25, wp_rec_end_date AS wp_rec_end_date_node_25, wp_creation_date_sk AS wp_creation_date_sk_node_25, wp_access_date_sk AS wp_access_date_sk_node_25, wp_autogen_flag AS wp_autogen_flag_node_25, wp_customer_sk AS wp_customer_sk_node_25, wp_url AS wp_url_node_25, wp_type AS wp_type_node_25, wp_char_count AS wp_char_count_node_25, wp_link_count AS wp_link_count_node_25, wp_image_count AS wp_image_count_node_25, wp_max_ad_count AS wp_max_ad_count_node_25])
                        +- SortLimit(orderBy=[wp_link_count ASC], offset=[0], fetch=[1], global=[true])
                           +- Exchange(distribution=[single])
                              +- SortLimit(orderBy=[wp_link_count ASC], offset=[0], fetch=[1], global=[false])
                                 +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])

== Optimized Execution Plan ==
NestedLoopJoin(joinType=[InnerJoin], where=[(wp_rec_start_date_node_25 = cc_company_name_node_22)], select=[cc_company_name_node_22, DQAeC, wp_web_page_id_node_25, wp_rec_start_date_node_25, wp_rec_end_date_node_25, wp_creation_date_sk_node_25, wp_access_date_sk_node_25, wp_autogen_flag_node_25, wp_customer_sk_node_25, wp_url_node_25, wp_type_node_25, wp_char_count_node_25, wp_link_count_node_25, wp_image_count_node_25, wp_max_ad_count_node_25, _c14], build=[left])
:- Exchange(distribution=[broadcast])
:  +- Calc(select=[EXPR$0 AS cc_company_name_node_22])
:     +- SortAggregate(isMerge=[false], groupBy=[cc_company_name_node_22], select=[cc_company_name_node_22, MIN(cc_company_name_node_22) AS EXPR$0])
:        +- Exchange(distribution=[forward])
:           +- Sort(orderBy=[cc_company_name_node_22 ASC])
:              +- Exchange(distribution=[hash[cc_company_name_node_22]])
:                 +- NestedLoopJoin(joinType=[InnerJoin], where=[(cc_company_node_22 = cr_catalog_page_sk_node_21)], select=[cr_catalog_page_sk_node_21, _c1, 2sfxN, cc_call_center_id_node_22, cc_rec_start_date_node_22, cc_rec_end_date_node_22, cc_closed_date_sk_node_22, cc_open_date_sk_node_22, cc_name_node_22, cc_class_node_22, cc_employees_node_22, cc_sq_ft_node_22, cc_hours_node_22, cc_manager_node_22, cc_mkt_id_node_22, cc_mkt_class_node_22, cc_mkt_desc_node_22, cc_market_manager_node_22, cc_division_node_22, cc_division_name_node_22, cc_company_node_22, cc_company_name_node_22, cc_street_number_node_22, cc_street_name_node_22, cc_street_type_node_22, cc_suite_number_node_22, cc_city_node_22, cc_county_node_22, cc_state_node_22, cc_zip_node_22, cc_country_node_22, cc_gmt_offset_node_22, cc_tax_percentage_node_22, sm_ship_mode_sk_node_23, sm_ship_mode_id_node_23, sm_type_node_23, sm_code_node_23, sm_carrier_node_23, sm_contract_node_23, _c6, s_street_type], build=[left])
:                    :- Exchange(distribution=[broadcast])
:                    :  +- Calc(select=[EXPR$0 AS cr_catalog_page_sk_node_21, 'hello' AS _c1])
:                    :     +- SortLimit(orderBy=[EXPR$0 ASC], offset=[0], fetch=[1], global=[true])
:                    :        +- Exchange(distribution=[single])
:                    :           +- SortLimit(orderBy=[EXPR$0 ASC], offset=[0], fetch=[1], global=[false])
:                    :              +- HashAggregate(isMerge=[true], groupBy=[cr_catalog_page_sk], select=[cr_catalog_page_sk, Final_SUM(sum$0) AS EXPR$0])
:                    :                 +- Exchange(distribution=[hash[cr_catalog_page_sk]])
:                    :                    +- LocalHashAggregate(groupBy=[cr_catalog_page_sk], select=[cr_catalog_page_sk, Partial_SUM(cr_catalog_page_sk) AS sum$0])
:                    :                       +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns, project=[cr_catalog_page_sk], metadata=[]]], fields=[cr_catalog_page_sk])
:                    +- NestedLoopJoin(joinType=[InnerJoin], where=[(cc_rec_start_date_node_22 = s_street_type)], select=[2sfxN, cc_call_center_id_node_22, cc_rec_start_date_node_22, cc_rec_end_date_node_22, cc_closed_date_sk_node_22, cc_open_date_sk_node_22, cc_name_node_22, cc_class_node_22, cc_employees_node_22, cc_sq_ft_node_22, cc_hours_node_22, cc_manager_node_22, cc_mkt_id_node_22, cc_mkt_class_node_22, cc_mkt_desc_node_22, cc_market_manager_node_22, cc_division_node_22, cc_division_name_node_22, cc_company_node_22, cc_company_name_node_22, cc_street_number_node_22, cc_street_name_node_22, cc_street_type_node_22, cc_suite_number_node_22, cc_city_node_22, cc_county_node_22, cc_state_node_22, cc_zip_node_22, cc_country_node_22, cc_gmt_offset_node_22, cc_tax_percentage_node_22, sm_ship_mode_sk_node_23, sm_ship_mode_id_node_23, sm_type_node_23, sm_code_node_23, sm_carrier_node_23, sm_contract_node_23, _c6, s_street_type], build=[right])
:                       :- Calc(select=[cc_call_center_sk AS 2sfxN, cc_call_center_id AS cc_call_center_id_node_22, cc_rec_start_date AS cc_rec_start_date_node_22, cc_rec_end_date AS cc_rec_end_date_node_22, cc_closed_date_sk AS cc_closed_date_sk_node_22, cc_open_date_sk AS cc_open_date_sk_node_22, cc_name AS cc_name_node_22, cc_class AS cc_class_node_22, cc_employees AS cc_employees_node_22, cc_sq_ft AS cc_sq_ft_node_22, cc_hours AS cc_hours_node_22, cc_manager AS cc_manager_node_22, cc_mkt_id AS cc_mkt_id_node_22, cc_mkt_class AS cc_mkt_class_node_22, cc_mkt_desc AS cc_mkt_desc_node_22, cc_market_manager AS cc_market_manager_node_22, cc_division AS cc_division_node_22, cc_division_name AS cc_division_name_node_22, cc_company AS cc_company_node_22, cc_company_name AS cc_company_name_node_22, cc_street_number AS cc_street_number_node_22, cc_street_name AS cc_street_name_node_22, cc_street_type AS cc_street_type_node_22, cc_suite_number AS cc_suite_number_node_22, cc_city AS cc_city_node_22, cc_county AS cc_county_node_22, cc_state AS cc_state_node_22, cc_zip AS cc_zip_node_22, cc_country AS cc_country_node_22, cc_gmt_offset AS cc_gmt_offset_node_22, cc_tax_percentage AS cc_tax_percentage_node_22, sm_ship_mode_sk AS sm_ship_mode_sk_node_23, sm_ship_mode_id AS sm_ship_mode_id_node_23, sm_type AS sm_type_node_23, sm_code AS sm_code_node_23, sm_carrier AS sm_carrier_node_23, sm_contract AS sm_contract_node_23, 'hello' AS _c6])
:                       :  +- NestedLoopJoin(joinType=[InnerJoin], where=[(sm_ship_mode_sk = cc_sq_ft)], select=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage, sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract], build=[right])
:                       :     :- Limit(offset=[0], fetch=[11], global=[true])
:                       :     :  +- Exchange(distribution=[single])
:                       :     :     +- Limit(offset=[0], fetch=[11], global=[false])
:                       :     :        +- TableSourceScan(table=[[default_catalog, default_database, call_center]], fields=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage])
:                       :     +- Exchange(distribution=[broadcast])
:                       :        +- TableSourceScan(table=[[default_catalog, default_database, ship_mode]], fields=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract])
:                       +- Exchange(distribution=[broadcast])
:                          +- Calc(select=[s_street_type], where=[((CHAR_LENGTH(s_street_type) <= 5) AND (CHAR_LENGTH(s_street_type) < 5))])
:                             +- TableSourceScan(table=[[default_catalog, default_database, store, filter=[and(<=(CHAR_LENGTH(s_street_type), 5), <(CHAR_LENGTH(s_street_type), 5))], project=[s_street_type], metadata=[]]], fields=[s_street_type])
+- Limit(offset=[0], fetch=[27], global=[true])
   +- Exchange(distribution=[single])
      +- Limit(offset=[0], fetch=[27], global=[false])
         +- Calc(select=[DQAeC, wp_web_page_id_node_25, wp_rec_start_date_node_25, wp_rec_end_date_node_25, wp_creation_date_sk_node_25, wp_access_date_sk_node_25, wp_autogen_flag_node_25, wp_customer_sk_node_25, wp_url_node_25, wp_type_node_25, wp_char_count_node_25, wp_link_count_node_25, wp_image_count_node_25, wp_max_ad_count_node_25, 'hello' AS _c14])
            +- SortAggregate(isMerge=[false], groupBy=[DQAeC, wp_web_page_id_node_25, wp_rec_start_date_node_25, wp_rec_end_date_node_25, wp_creation_date_sk_node_25, wp_access_date_sk_node_25, wp_autogen_flag_node_25, wp_customer_sk_node_25, wp_url_node_25, wp_type_node_25, wp_char_count_node_25, wp_link_count_node_25, wp_image_count_node_25, wp_max_ad_count_node_25], select=[DQAeC, wp_web_page_id_node_25, wp_rec_start_date_node_25, wp_rec_end_date_node_25, wp_creation_date_sk_node_25, wp_access_date_sk_node_25, wp_autogen_flag_node_25, wp_customer_sk_node_25, wp_url_node_25, wp_type_node_25, wp_char_count_node_25, wp_link_count_node_25, wp_image_count_node_25, wp_max_ad_count_node_25])
               +- Exchange(distribution=[forward])
                  +- Sort(orderBy=[DQAeC ASC, wp_web_page_id_node_25 ASC, wp_rec_start_date_node_25 ASC, wp_rec_end_date_node_25 ASC, wp_creation_date_sk_node_25 ASC, wp_access_date_sk_node_25 ASC, wp_autogen_flag_node_25 ASC, wp_customer_sk_node_25 ASC, wp_url_node_25 ASC, wp_type_node_25 ASC, wp_char_count_node_25 ASC, wp_link_count_node_25 ASC, wp_image_count_node_25 ASC, wp_max_ad_count_node_25 ASC])
                     +- Exchange(distribution=[hash[DQAeC, wp_web_page_id_node_25, wp_rec_start_date_node_25, wp_rec_end_date_node_25, wp_creation_date_sk_node_25, wp_access_date_sk_node_25, wp_autogen_flag_node_25, wp_customer_sk_node_25, wp_url_node_25, wp_type_node_25, wp_char_count_node_25, wp_link_count_node_25, wp_image_count_node_25, wp_max_ad_count_node_25]])
                        +- Calc(select=[wp_web_page_sk AS DQAeC, wp_web_page_id AS wp_web_page_id_node_25, wp_rec_start_date AS wp_rec_start_date_node_25, wp_rec_end_date AS wp_rec_end_date_node_25, wp_creation_date_sk AS wp_creation_date_sk_node_25, wp_access_date_sk AS wp_access_date_sk_node_25, wp_autogen_flag AS wp_autogen_flag_node_25, wp_customer_sk AS wp_customer_sk_node_25, wp_url AS wp_url_node_25, wp_type AS wp_type_node_25, wp_char_count AS wp_char_count_node_25, wp_link_count AS wp_link_count_node_25, wp_image_count AS wp_image_count_node_25, wp_max_ad_count AS wp_max_ad_count_node_25])
                           +- SortLimit(orderBy=[wp_link_count ASC], offset=[0], fetch=[1], global=[true])
                              +- Exchange(distribution=[single])
                                 +- SortLimit(orderBy=[wp_link_count ASC], offset=[0], fetch=[1], global=[false])
                                    +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0