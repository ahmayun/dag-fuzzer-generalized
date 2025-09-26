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


def preloaded_aggregation(values: pd.Series) -> int:
    return len(values)


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_18 = table_env.from_path("income_band").select(*[col(column_name).alias(f"{column_name}_node_18") for column_name in table_env.from_path("income_band").get_schema().get_field_names()])
autonode_19 = table_env.from_path("ship_mode").select(*[col(column_name).alias(f"{column_name}_node_19") for column_name in table_env.from_path("ship_mode").get_schema().get_field_names()])
autonode_21 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_21") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_20 = table_env.from_path("web_page").select(*[col(column_name).alias(f"{column_name}_node_20") for column_name in table_env.from_path("web_page").get_schema().get_field_names()])
autonode_22 = table_env.from_path("store").select(*[col(column_name).alias(f"{column_name}_node_22") for column_name in table_env.from_path("store").get_schema().get_field_names()])
autonode_14 = autonode_18.join(autonode_19, col('sm_ship_mode_sk_node_19') == col('ib_income_band_sk_node_18'))
autonode_16 = autonode_21.group_by(col('ss_ext_discount_amt_node_21')).select(col('ss_ext_list_price_node_21').sum.alias('ss_ext_list_price_node_21'))
autonode_15 = autonode_20.order_by(col('wp_url_node_20'))
autonode_17 = autonode_22.order_by(col('s_market_desc_node_22'))
autonode_10 = autonode_14.select(col('sm_ship_mode_id_node_19'))
autonode_12 = autonode_16.add_columns(lit("hello"))
autonode_11 = autonode_15.alias('RX4hB')
autonode_13 = autonode_17.add_columns(lit("hello"))
autonode_8 = autonode_12.group_by(col('ss_ext_list_price_node_21')).select(col('ss_ext_list_price_node_21').count.alias('ss_ext_list_price_node_21'))
autonode_7 = autonode_10.join(autonode_11, col('wp_type_node_20') == col('sm_ship_mode_id_node_19'))
autonode_9 = autonode_13.filter(col('s_division_name_node_22').char_length >= 5)
autonode_5 = autonode_7.group_by(col('wp_type_node_20')).select(col('sm_ship_mode_id_node_19').min.alias('sm_ship_mode_id_node_19'))
autonode_6 = autonode_8.join(autonode_9, col('s_tax_precentage_node_22') == col('ss_ext_list_price_node_21'))
autonode_3 = autonode_5.limit(54)
autonode_4 = autonode_6.filter(col('s_country_node_22').char_length > 5)
autonode_1 = autonode_3.select(col('sm_ship_mode_id_node_19'))
autonode_2 = autonode_4.distinct()
sink = autonode_1.join(autonode_2, col('s_street_name_node_22') == col('sm_ship_mode_id_node_19'))
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
      "error_message": "An error occurred while calling o285749178.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#577003032:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[8](input=RelSubset#577003030,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[8]), rel#577003029:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[8](input=RelSubset#577003028,groupBy=wp_type,select=wp_type)]
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
Caused by: java.lang.IndexOutOfBoundsException: index (8) must be less than size (1)
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
LogicalJoin(condition=[=($21, $0)], joinType=[inner])
:- LogicalProject(sm_ship_mode_id_node_19=[$0])
:  +- LogicalSort(fetch=[54])
:     +- LogicalProject(sm_ship_mode_id_node_19=[$1])
:        +- LogicalAggregate(group=[{10}], EXPR$0=[MIN($0)])
:           +- LogicalJoin(condition=[=($10, $0)], joinType=[inner])
:              :- LogicalProject(sm_ship_mode_id_node_19=[$4])
:              :  +- LogicalJoin(condition=[=($3, $0)], joinType=[inner])
:              :     :- LogicalProject(ib_income_band_sk_node_18=[$0], ib_lower_bound_node_18=[$1], ib_upper_bound_node_18=[$2])
:              :     :  +- LogicalTableScan(table=[[default_catalog, default_database, income_band]])
:              :     +- LogicalProject(sm_ship_mode_sk_node_19=[$0], sm_ship_mode_id_node_19=[$1], sm_type_node_19=[$2], sm_code_node_19=[$3], sm_carrier_node_19=[$4], sm_contract_node_19=[$5])
:              :        +- LogicalTableScan(table=[[default_catalog, default_database, ship_mode]])
:              +- LogicalProject(RX4hB=[AS($0, _UTF-16LE'RX4hB')], wp_web_page_id_node_20=[$1], wp_rec_start_date_node_20=[$2], wp_rec_end_date_node_20=[$3], wp_creation_date_sk_node_20=[$4], wp_access_date_sk_node_20=[$5], wp_autogen_flag_node_20=[$6], wp_customer_sk_node_20=[$7], wp_url_node_20=[$8], wp_type_node_20=[$9], wp_char_count_node_20=[$10], wp_link_count_node_20=[$11], wp_image_count_node_20=[$12], wp_max_ad_count_node_20=[$13])
:                 +- LogicalSort(sort0=[$8], dir0=[ASC])
:                    +- LogicalProject(wp_web_page_sk_node_20=[$0], wp_web_page_id_node_20=[$1], wp_rec_start_date_node_20=[$2], wp_rec_end_date_node_20=[$3], wp_creation_date_sk_node_20=[$4], wp_access_date_sk_node_20=[$5], wp_autogen_flag_node_20=[$6], wp_customer_sk_node_20=[$7], wp_url_node_20=[$8], wp_type_node_20=[$9], wp_char_count_node_20=[$10], wp_link_count_node_20=[$11], wp_image_count_node_20=[$12], wp_max_ad_count_node_20=[$13])
:                       +- LogicalTableScan(table=[[default_catalog, default_database, web_page]])
+- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30}])
   +- LogicalFilter(condition=[>(CHAR_LENGTH($27), 5)])
      +- LogicalJoin(condition=[=($29, $0)], joinType=[inner])
         :- LogicalProject(ss_ext_list_price_node_21=[$1])
         :  +- LogicalAggregate(group=[{0}], EXPR$0=[COUNT($0)])
         :     +- LogicalProject(ss_ext_list_price_node_21=[$1])
         :        +- LogicalAggregate(group=[{0}], EXPR$0=[SUM($1)])
         :           +- LogicalProject(ss_ext_discount_amt_node_21=[$14], ss_ext_list_price_node_21=[$17])
         :              +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])
         +- LogicalFilter(condition=[>=(CHAR_LENGTH($15), 5)])
            +- LogicalProject(s_store_sk_node_22=[$0], s_store_id_node_22=[$1], s_rec_start_date_node_22=[$2], s_rec_end_date_node_22=[$3], s_closed_date_sk_node_22=[$4], s_store_name_node_22=[$5], s_number_employees_node_22=[$6], s_floor_space_node_22=[$7], s_hours_node_22=[$8], s_manager_node_22=[$9], s_market_id_node_22=[$10], s_geography_class_node_22=[$11], s_market_desc_node_22=[$12], s_market_manager_node_22=[$13], s_division_id_node_22=[$14], s_division_name_node_22=[$15], s_company_id_node_22=[$16], s_company_name_node_22=[$17], s_street_number_node_22=[$18], s_street_name_node_22=[$19], s_street_type_node_22=[$20], s_suite_number_node_22=[$21], s_city_node_22=[$22], s_county_node_22=[$23], s_state_node_22=[$24], s_zip_node_22=[$25], s_country_node_22=[$26], s_gmt_offset_node_22=[$27], s_tax_precentage_node_22=[$28], _c29=[_UTF-16LE'hello'])
               +- LogicalSort(sort0=[$12], dir0=[ASC])
                  +- LogicalProject(s_store_sk_node_22=[$0], s_store_id_node_22=[$1], s_rec_start_date_node_22=[$2], s_rec_end_date_node_22=[$3], s_closed_date_sk_node_22=[$4], s_store_name_node_22=[$5], s_number_employees_node_22=[$6], s_floor_space_node_22=[$7], s_hours_node_22=[$8], s_manager_node_22=[$9], s_market_id_node_22=[$10], s_geography_class_node_22=[$11], s_market_desc_node_22=[$12], s_market_manager_node_22=[$13], s_division_id_node_22=[$14], s_division_name_node_22=[$15], s_company_id_node_22=[$16], s_company_name_node_22=[$17], s_street_number_node_22=[$18], s_street_name_node_22=[$19], s_street_type_node_22=[$20], s_suite_number_node_22=[$21], s_city_node_22=[$22], s_county_node_22=[$23], s_state_node_22=[$24], s_zip_node_22=[$25], s_country_node_22=[$26], s_gmt_offset_node_22=[$27], s_tax_precentage_node_22=[$28])
                     +- LogicalTableScan(table=[[default_catalog, default_database, store]])

== Optimized Physical Plan ==
NestedLoopJoin(joinType=[InnerJoin], where=[=(s_street_name_node_22, sm_ship_mode_id_node_19)], select=[sm_ship_mode_id_node_19, ss_ext_list_price_node_21, s_store_sk_node_22, s_store_id_node_22, s_rec_start_date_node_22, s_rec_end_date_node_22, s_closed_date_sk_node_22, s_store_name_node_22, s_number_employees_node_22, s_floor_space_node_22, s_hours_node_22, s_manager_node_22, s_market_id_node_22, s_geography_class_node_22, s_market_desc_node_22, s_market_manager_node_22, s_division_id_node_22, s_division_name_node_22, s_company_id_node_22, s_company_name_node_22, s_street_number_node_22, s_street_name_node_22, s_street_type_node_22, s_suite_number_node_22, s_city_node_22, s_county_node_22, s_state_node_22, s_zip_node_22, s_country_node_22, s_gmt_offset_node_22, s_tax_precentage_node_22, _c29], build=[left])
:- Exchange(distribution=[broadcast])
:  +- Limit(offset=[0], fetch=[54], global=[true])
:     +- Exchange(distribution=[single])
:        +- Limit(offset=[0], fetch=[54], global=[false])
:           +- Calc(select=[EXPR$0 AS sm_ship_mode_id_node_19])
:              +- SortAggregate(isMerge=[false], groupBy=[wp_type_node_20], select=[wp_type_node_20, MIN(sm_ship_mode_id_node_19) AS EXPR$0])
:                 +- Sort(orderBy=[wp_type_node_20 ASC])
:                    +- NestedLoopJoin(joinType=[InnerJoin], where=[=(wp_type_node_20, sm_ship_mode_id_node_19)], select=[sm_ship_mode_id_node_19, RX4hB, wp_web_page_id_node_20, wp_rec_start_date_node_20, wp_rec_end_date_node_20, wp_creation_date_sk_node_20, wp_access_date_sk_node_20, wp_autogen_flag_node_20, wp_customer_sk_node_20, wp_url_node_20, wp_type_node_20, wp_char_count_node_20, wp_link_count_node_20, wp_image_count_node_20, wp_max_ad_count_node_20], build=[left])
:                       :- Exchange(distribution=[broadcast])
:                       :  +- Calc(select=[sm_ship_mode_id AS sm_ship_mode_id_node_19])
:                       :     +- HashJoin(joinType=[InnerJoin], where=[=(sm_ship_mode_sk, ib_income_band_sk)], select=[ib_income_band_sk, sm_ship_mode_sk, sm_ship_mode_id], isBroadcast=[true], build=[left])
:                       :        :- Exchange(distribution=[broadcast])
:                       :        :  +- TableSourceScan(table=[[default_catalog, default_database, income_band, project=[ib_income_band_sk], metadata=[]]], fields=[ib_income_band_sk])
:                       :        +- TableSourceScan(table=[[default_catalog, default_database, ship_mode, project=[sm_ship_mode_sk, sm_ship_mode_id], metadata=[]]], fields=[sm_ship_mode_sk, sm_ship_mode_id])
:                       +- Exchange(distribution=[hash[wp_type_node_20]])
:                          +- Calc(select=[wp_web_page_sk AS RX4hB, wp_web_page_id AS wp_web_page_id_node_20, wp_rec_start_date AS wp_rec_start_date_node_20, wp_rec_end_date AS wp_rec_end_date_node_20, wp_creation_date_sk AS wp_creation_date_sk_node_20, wp_access_date_sk AS wp_access_date_sk_node_20, wp_autogen_flag AS wp_autogen_flag_node_20, wp_customer_sk AS wp_customer_sk_node_20, wp_url AS wp_url_node_20, wp_type AS wp_type_node_20, wp_char_count AS wp_char_count_node_20, wp_link_count AS wp_link_count_node_20, wp_image_count AS wp_image_count_node_20, wp_max_ad_count AS wp_max_ad_count_node_20])
:                             +- SortLimit(orderBy=[wp_url ASC], offset=[0], fetch=[1], global=[true])
:                                +- Exchange(distribution=[single])
:                                   +- SortLimit(orderBy=[wp_url ASC], offset=[0], fetch=[1], global=[false])
:                                      +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
+- Calc(select=[ss_ext_list_price_node_21, s_store_sk_node_22, s_store_id_node_22, s_rec_start_date_node_22, s_rec_end_date_node_22, s_closed_date_sk_node_22, s_store_name_node_22, s_number_employees_node_22, s_floor_space_node_22, s_hours_node_22, s_manager_node_22, s_market_id_node_22, s_geography_class_node_22, s_market_desc_node_22, s_market_manager_node_22, s_division_id_node_22, s_division_name_node_22, s_company_id_node_22, s_company_name_node_22, s_street_number_node_22, s_street_name_node_22, s_street_type_node_22, s_suite_number_node_22, s_city_node_22, s_county_node_22, s_state_node_22, s_zip_node_22, s_country_node_22, s_gmt_offset_node_22, s_tax_precentage_node_22, 'hello' AS _c29])
   +- HashAggregate(isMerge=[false], groupBy=[ss_ext_list_price_node_21, s_store_sk_node_22, s_store_id_node_22, s_rec_start_date_node_22, s_rec_end_date_node_22, s_closed_date_sk_node_22, s_store_name_node_22, s_number_employees_node_22, s_floor_space_node_22, s_hours_node_22, s_manager_node_22, s_market_id_node_22, s_geography_class_node_22, s_market_desc_node_22, s_market_manager_node_22, s_division_id_node_22, s_division_name_node_22, s_company_id_node_22, s_company_name_node_22, s_street_number_node_22, s_street_name_node_22, s_street_type_node_22, s_suite_number_node_22, s_city_node_22, s_county_node_22, s_state_node_22, s_zip_node_22, s_country_node_22, s_gmt_offset_node_22, s_tax_precentage_node_22], select=[ss_ext_list_price_node_21, s_store_sk_node_22, s_store_id_node_22, s_rec_start_date_node_22, s_rec_end_date_node_22, s_closed_date_sk_node_22, s_store_name_node_22, s_number_employees_node_22, s_floor_space_node_22, s_hours_node_22, s_manager_node_22, s_market_id_node_22, s_geography_class_node_22, s_market_desc_node_22, s_market_manager_node_22, s_division_id_node_22, s_division_name_node_22, s_company_id_node_22, s_company_name_node_22, s_street_number_node_22, s_street_name_node_22, s_street_type_node_22, s_suite_number_node_22, s_city_node_22, s_county_node_22, s_state_node_22, s_zip_node_22, s_country_node_22, s_gmt_offset_node_22, s_tax_precentage_node_22])
      +- Exchange(distribution=[hash[ss_ext_list_price_node_21, s_store_sk_node_22, s_store_id_node_22, s_rec_start_date_node_22, s_rec_end_date_node_22, s_closed_date_sk_node_22, s_store_name_node_22, s_number_employees_node_22, s_floor_space_node_22, s_hours_node_22, s_manager_node_22, s_market_id_node_22, s_geography_class_node_22, s_market_desc_node_22, s_market_manager_node_22, s_division_id_node_22, s_division_name_node_22, s_company_id_node_22, s_company_name_node_22, s_street_number_node_22, s_street_name_node_22, s_street_type_node_22, s_suite_number_node_22, s_city_node_22, s_county_node_22, s_state_node_22, s_zip_node_22, s_country_node_22, s_gmt_offset_node_22, s_tax_precentage_node_22]])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[=(s_tax_precentage_node_220, ss_ext_list_price_node_210)], select=[ss_ext_list_price_node_21, ss_ext_list_price_node_210, s_store_sk_node_22, s_store_id_node_22, s_rec_start_date_node_22, s_rec_end_date_node_22, s_closed_date_sk_node_22, s_store_name_node_22, s_number_employees_node_22, s_floor_space_node_22, s_hours_node_22, s_manager_node_22, s_market_id_node_22, s_geography_class_node_22, s_market_desc_node_22, s_market_manager_node_22, s_division_id_node_22, s_division_name_node_22, s_company_id_node_22, s_company_name_node_22, s_street_number_node_22, s_street_name_node_22, s_street_type_node_22, s_suite_number_node_22, s_city_node_22, s_county_node_22, s_state_node_22, s_zip_node_22, s_country_node_22, s_gmt_offset_node_22, s_tax_precentage_node_22, s_tax_precentage_node_220], build=[right])
            :- HashAggregate(isMerge=[true], groupBy=[ss_ext_list_price_node_21, ss_ext_list_price_node_210], select=[ss_ext_list_price_node_21, ss_ext_list_price_node_210])
            :  +- Exchange(distribution=[hash[ss_ext_list_price_node_21, ss_ext_list_price_node_210]])
            :     +- LocalHashAggregate(groupBy=[ss_ext_list_price_node_21, ss_ext_list_price_node_210], select=[ss_ext_list_price_node_21, ss_ext_list_price_node_210])
            :        +- Calc(select=[EXPR$0 AS ss_ext_list_price_node_21, CAST(EXPR$0 AS DECIMAL(21, 2)) AS ss_ext_list_price_node_210])
            :           +- HashAggregate(isMerge=[true], groupBy=[ss_ext_list_price_node_21], select=[ss_ext_list_price_node_21, Final_COUNT(count$0) AS EXPR$0])
            :              +- Exchange(distribution=[hash[ss_ext_list_price_node_21]])
            :                 +- LocalHashAggregate(groupBy=[ss_ext_list_price_node_21], select=[ss_ext_list_price_node_21, Partial_COUNT(ss_ext_list_price_node_21) AS count$0])
            :                    +- Calc(select=[EXPR$0 AS ss_ext_list_price_node_21])
            :                       +- HashAggregate(isMerge=[true], groupBy=[ss_ext_discount_amt], select=[ss_ext_discount_amt, Final_SUM(sum$0) AS EXPR$0])
            :                          +- Exchange(distribution=[hash[ss_ext_discount_amt]])
            :                             +- LocalHashAggregate(groupBy=[ss_ext_discount_amt], select=[ss_ext_discount_amt, Partial_SUM(ss_ext_list_price) AS sum$0])
            :                                +- TableSourceScan(table=[[default_catalog, default_database, store_sales, project=[ss_ext_discount_amt, ss_ext_list_price], metadata=[]]], fields=[ss_ext_discount_amt, ss_ext_list_price])
            +- Exchange(distribution=[broadcast])
               +- SortAggregate(isMerge=[false], groupBy=[s_store_sk_node_22, s_store_id_node_22, s_rec_start_date_node_22, s_rec_end_date_node_22, s_closed_date_sk_node_22, s_store_name_node_22, s_number_employees_node_22, s_floor_space_node_22, s_hours_node_22, s_manager_node_22, s_market_id_node_22, s_geography_class_node_22, s_market_desc_node_22, s_market_manager_node_22, s_division_id_node_22, s_division_name_node_22, s_company_id_node_22, s_company_name_node_22, s_street_number_node_22, s_street_name_node_22, s_street_type_node_22, s_suite_number_node_22, s_city_node_22, s_county_node_22, s_state_node_22, s_zip_node_22, s_country_node_22, s_gmt_offset_node_22, s_tax_precentage_node_22, s_tax_precentage_node_220], select=[s_store_sk_node_22, s_store_id_node_22, s_rec_start_date_node_22, s_rec_end_date_node_22, s_closed_date_sk_node_22, s_store_name_node_22, s_number_employees_node_22, s_floor_space_node_22, s_hours_node_22, s_manager_node_22, s_market_id_node_22, s_geography_class_node_22, s_market_desc_node_22, s_market_manager_node_22, s_division_id_node_22, s_division_name_node_22, s_company_id_node_22, s_company_name_node_22, s_street_number_node_22, s_street_name_node_22, s_street_type_node_22, s_suite_number_node_22, s_city_node_22, s_county_node_22, s_state_node_22, s_zip_node_22, s_country_node_22, s_gmt_offset_node_22, s_tax_precentage_node_22, s_tax_precentage_node_220])
                  +- Sort(orderBy=[s_store_sk_node_22 ASC, s_store_id_node_22 ASC, s_rec_start_date_node_22 ASC, s_rec_end_date_node_22 ASC, s_closed_date_sk_node_22 ASC, s_store_name_node_22 ASC, s_number_employees_node_22 ASC, s_floor_space_node_22 ASC, s_hours_node_22 ASC, s_manager_node_22 ASC, s_market_id_node_22 ASC, s_geography_class_node_22 ASC, s_market_desc_node_22 ASC, s_market_manager_node_22 ASC, s_division_id_node_22 ASC, s_division_name_node_22 ASC, s_company_id_node_22 ASC, s_company_name_node_22 ASC, s_street_number_node_22 ASC, s_street_name_node_22 ASC, s_street_type_node_22 ASC, s_suite_number_node_22 ASC, s_city_node_22 ASC, s_county_node_22 ASC, s_state_node_22 ASC, s_zip_node_22 ASC, s_country_node_22 ASC, s_gmt_offset_node_22 ASC, s_tax_precentage_node_22 ASC, s_tax_precentage_node_220 ASC])
                     +- Exchange(distribution=[hash[s_store_sk_node_22, s_store_id_node_22, s_rec_start_date_node_22, s_rec_end_date_node_22, s_closed_date_sk_node_22, s_store_name_node_22, s_number_employees_node_22, s_floor_space_node_22, s_hours_node_22, s_manager_node_22, s_market_id_node_22, s_geography_class_node_22, s_market_desc_node_22, s_market_manager_node_22, s_division_id_node_22, s_division_name_node_22, s_company_id_node_22, s_company_name_node_22, s_street_number_node_22, s_street_name_node_22, s_street_type_node_22, s_suite_number_node_22, s_city_node_22, s_county_node_22, s_state_node_22, s_zip_node_22, s_country_node_22, s_gmt_offset_node_22, s_tax_precentage_node_22, s_tax_precentage_node_220]])
                        +- Calc(select=[s_store_sk AS s_store_sk_node_22, s_store_id AS s_store_id_node_22, s_rec_start_date AS s_rec_start_date_node_22, s_rec_end_date AS s_rec_end_date_node_22, s_closed_date_sk AS s_closed_date_sk_node_22, s_store_name AS s_store_name_node_22, s_number_employees AS s_number_employees_node_22, s_floor_space AS s_floor_space_node_22, s_hours AS s_hours_node_22, s_manager AS s_manager_node_22, s_market_id AS s_market_id_node_22, s_geography_class AS s_geography_class_node_22, s_market_desc AS s_market_desc_node_22, s_market_manager AS s_market_manager_node_22, s_division_id AS s_division_id_node_22, s_division_name AS s_division_name_node_22, s_company_id AS s_company_id_node_22, s_company_name AS s_company_name_node_22, s_street_number AS s_street_number_node_22, s_street_name AS s_street_name_node_22, s_street_type AS s_street_type_node_22, s_suite_number AS s_suite_number_node_22, s_city AS s_city_node_22, s_county AS s_county_node_22, s_state AS s_state_node_22, s_zip AS s_zip_node_22, s_country AS s_country_node_22, s_gmt_offset AS s_gmt_offset_node_22, s_tax_precentage AS s_tax_precentage_node_22, CAST(s_tax_precentage AS DECIMAL(21, 2)) AS s_tax_precentage_node_220], where=[AND(>=(CHAR_LENGTH(s_division_name), 5), >(CHAR_LENGTH(s_country), 5))])
                           +- SortLimit(orderBy=[s_market_desc ASC], offset=[0], fetch=[1], global=[true])
                              +- Exchange(distribution=[single])
                                 +- SortLimit(orderBy=[s_market_desc ASC], offset=[0], fetch=[1], global=[false])
                                    +- TableSourceScan(table=[[default_catalog, default_database, store]], fields=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage])

== Optimized Execution Plan ==
NestedLoopJoin(joinType=[InnerJoin], where=[(s_street_name_node_22 = sm_ship_mode_id_node_19)], select=[sm_ship_mode_id_node_19, ss_ext_list_price_node_21, s_store_sk_node_22, s_store_id_node_22, s_rec_start_date_node_22, s_rec_end_date_node_22, s_closed_date_sk_node_22, s_store_name_node_22, s_number_employees_node_22, s_floor_space_node_22, s_hours_node_22, s_manager_node_22, s_market_id_node_22, s_geography_class_node_22, s_market_desc_node_22, s_market_manager_node_22, s_division_id_node_22, s_division_name_node_22, s_company_id_node_22, s_company_name_node_22, s_street_number_node_22, s_street_name_node_22, s_street_type_node_22, s_suite_number_node_22, s_city_node_22, s_county_node_22, s_state_node_22, s_zip_node_22, s_country_node_22, s_gmt_offset_node_22, s_tax_precentage_node_22, _c29], build=[left])
:- Exchange(distribution=[broadcast])
:  +- Limit(offset=[0], fetch=[54], global=[true])
:     +- Exchange(distribution=[single])
:        +- Limit(offset=[0], fetch=[54], global=[false])
:           +- Calc(select=[EXPR$0 AS sm_ship_mode_id_node_19])
:              +- SortAggregate(isMerge=[false], groupBy=[wp_type_node_20], select=[wp_type_node_20, MIN(sm_ship_mode_id_node_19) AS EXPR$0])
:                 +- Exchange(distribution=[forward])
:                    +- Sort(orderBy=[wp_type_node_20 ASC])
:                       +- Exchange(distribution=[keep_input_as_is[hash[wp_type_node_20]]])
:                          +- NestedLoopJoin(joinType=[InnerJoin], where=[(wp_type_node_20 = sm_ship_mode_id_node_19)], select=[sm_ship_mode_id_node_19, RX4hB, wp_web_page_id_node_20, wp_rec_start_date_node_20, wp_rec_end_date_node_20, wp_creation_date_sk_node_20, wp_access_date_sk_node_20, wp_autogen_flag_node_20, wp_customer_sk_node_20, wp_url_node_20, wp_type_node_20, wp_char_count_node_20, wp_link_count_node_20, wp_image_count_node_20, wp_max_ad_count_node_20], build=[left])
:                             :- Exchange(distribution=[broadcast])
:                             :  +- Calc(select=[sm_ship_mode_id AS sm_ship_mode_id_node_19])
:                             :     +- HashJoin(joinType=[InnerJoin], where=[(sm_ship_mode_sk = ib_income_band_sk)], select=[ib_income_band_sk, sm_ship_mode_sk, sm_ship_mode_id], isBroadcast=[true], build=[left])
:                             :        :- Exchange(distribution=[broadcast])
:                             :        :  +- TableSourceScan(table=[[default_catalog, default_database, income_band, project=[ib_income_band_sk], metadata=[]]], fields=[ib_income_band_sk])
:                             :        +- TableSourceScan(table=[[default_catalog, default_database, ship_mode, project=[sm_ship_mode_sk, sm_ship_mode_id], metadata=[]]], fields=[sm_ship_mode_sk, sm_ship_mode_id])
:                             +- Exchange(distribution=[hash[wp_type_node_20]])
:                                +- Calc(select=[wp_web_page_sk AS RX4hB, wp_web_page_id AS wp_web_page_id_node_20, wp_rec_start_date AS wp_rec_start_date_node_20, wp_rec_end_date AS wp_rec_end_date_node_20, wp_creation_date_sk AS wp_creation_date_sk_node_20, wp_access_date_sk AS wp_access_date_sk_node_20, wp_autogen_flag AS wp_autogen_flag_node_20, wp_customer_sk AS wp_customer_sk_node_20, wp_url AS wp_url_node_20, wp_type AS wp_type_node_20, wp_char_count AS wp_char_count_node_20, wp_link_count AS wp_link_count_node_20, wp_image_count AS wp_image_count_node_20, wp_max_ad_count AS wp_max_ad_count_node_20])
:                                   +- SortLimit(orderBy=[wp_url ASC], offset=[0], fetch=[1], global=[true])
:                                      +- Exchange(distribution=[single])
:                                         +- SortLimit(orderBy=[wp_url ASC], offset=[0], fetch=[1], global=[false])
:                                            +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
+- Calc(select=[ss_ext_list_price_node_21, s_store_sk_node_22, s_store_id_node_22, s_rec_start_date_node_22, s_rec_end_date_node_22, s_closed_date_sk_node_22, s_store_name_node_22, s_number_employees_node_22, s_floor_space_node_22, s_hours_node_22, s_manager_node_22, s_market_id_node_22, s_geography_class_node_22, s_market_desc_node_22, s_market_manager_node_22, s_division_id_node_22, s_division_name_node_22, s_company_id_node_22, s_company_name_node_22, s_street_number_node_22, s_street_name_node_22, s_street_type_node_22, s_suite_number_node_22, s_city_node_22, s_county_node_22, s_state_node_22, s_zip_node_22, s_country_node_22, s_gmt_offset_node_22, s_tax_precentage_node_22, 'hello' AS _c29])
   +- HashAggregate(isMerge=[false], groupBy=[ss_ext_list_price_node_21, s_store_sk_node_22, s_store_id_node_22, s_rec_start_date_node_22, s_rec_end_date_node_22, s_closed_date_sk_node_22, s_store_name_node_22, s_number_employees_node_22, s_floor_space_node_22, s_hours_node_22, s_manager_node_22, s_market_id_node_22, s_geography_class_node_22, s_market_desc_node_22, s_market_manager_node_22, s_division_id_node_22, s_division_name_node_22, s_company_id_node_22, s_company_name_node_22, s_street_number_node_22, s_street_name_node_22, s_street_type_node_22, s_suite_number_node_22, s_city_node_22, s_county_node_22, s_state_node_22, s_zip_node_22, s_country_node_22, s_gmt_offset_node_22, s_tax_precentage_node_22], select=[ss_ext_list_price_node_21, s_store_sk_node_22, s_store_id_node_22, s_rec_start_date_node_22, s_rec_end_date_node_22, s_closed_date_sk_node_22, s_store_name_node_22, s_number_employees_node_22, s_floor_space_node_22, s_hours_node_22, s_manager_node_22, s_market_id_node_22, s_geography_class_node_22, s_market_desc_node_22, s_market_manager_node_22, s_division_id_node_22, s_division_name_node_22, s_company_id_node_22, s_company_name_node_22, s_street_number_node_22, s_street_name_node_22, s_street_type_node_22, s_suite_number_node_22, s_city_node_22, s_county_node_22, s_state_node_22, s_zip_node_22, s_country_node_22, s_gmt_offset_node_22, s_tax_precentage_node_22])
      +- Exchange(distribution=[hash[ss_ext_list_price_node_21, s_store_sk_node_22, s_store_id_node_22, s_rec_start_date_node_22, s_rec_end_date_node_22, s_closed_date_sk_node_22, s_store_name_node_22, s_number_employees_node_22, s_floor_space_node_22, s_hours_node_22, s_manager_node_22, s_market_id_node_22, s_geography_class_node_22, s_market_desc_node_22, s_market_manager_node_22, s_division_id_node_22, s_division_name_node_22, s_company_id_node_22, s_company_name_node_22, s_street_number_node_22, s_street_name_node_22, s_street_type_node_22, s_suite_number_node_22, s_city_node_22, s_county_node_22, s_state_node_22, s_zip_node_22, s_country_node_22, s_gmt_offset_node_22, s_tax_precentage_node_22]])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[(s_tax_precentage_node_220 = ss_ext_list_price_node_210)], select=[ss_ext_list_price_node_21, ss_ext_list_price_node_210, s_store_sk_node_22, s_store_id_node_22, s_rec_start_date_node_22, s_rec_end_date_node_22, s_closed_date_sk_node_22, s_store_name_node_22, s_number_employees_node_22, s_floor_space_node_22, s_hours_node_22, s_manager_node_22, s_market_id_node_22, s_geography_class_node_22, s_market_desc_node_22, s_market_manager_node_22, s_division_id_node_22, s_division_name_node_22, s_company_id_node_22, s_company_name_node_22, s_street_number_node_22, s_street_name_node_22, s_street_type_node_22, s_suite_number_node_22, s_city_node_22, s_county_node_22, s_state_node_22, s_zip_node_22, s_country_node_22, s_gmt_offset_node_22, s_tax_precentage_node_22, s_tax_precentage_node_220], build=[right])
            :- HashAggregate(isMerge=[true], groupBy=[ss_ext_list_price_node_21, ss_ext_list_price_node_210], select=[ss_ext_list_price_node_21, ss_ext_list_price_node_210])
            :  +- Exchange(distribution=[hash[ss_ext_list_price_node_21, ss_ext_list_price_node_210]])
            :     +- LocalHashAggregate(groupBy=[ss_ext_list_price_node_21, ss_ext_list_price_node_210], select=[ss_ext_list_price_node_21, ss_ext_list_price_node_210])
            :        +- Calc(select=[EXPR$0 AS ss_ext_list_price_node_21, CAST(EXPR$0 AS DECIMAL(21, 2)) AS ss_ext_list_price_node_210])
            :           +- HashAggregate(isMerge=[true], groupBy=[ss_ext_list_price_node_21], select=[ss_ext_list_price_node_21, Final_COUNT(count$0) AS EXPR$0])
            :              +- Exchange(distribution=[hash[ss_ext_list_price_node_21]])
            :                 +- LocalHashAggregate(groupBy=[ss_ext_list_price_node_21], select=[ss_ext_list_price_node_21, Partial_COUNT(ss_ext_list_price_node_21) AS count$0])
            :                    +- Calc(select=[EXPR$0 AS ss_ext_list_price_node_21])
            :                       +- HashAggregate(isMerge=[true], groupBy=[ss_ext_discount_amt], select=[ss_ext_discount_amt, Final_SUM(sum$0) AS EXPR$0])
            :                          +- Exchange(distribution=[hash[ss_ext_discount_amt]])
            :                             +- LocalHashAggregate(groupBy=[ss_ext_discount_amt], select=[ss_ext_discount_amt, Partial_SUM(ss_ext_list_price) AS sum$0])
            :                                +- TableSourceScan(table=[[default_catalog, default_database, store_sales, project=[ss_ext_discount_amt, ss_ext_list_price], metadata=[]]], fields=[ss_ext_discount_amt, ss_ext_list_price])
            +- Exchange(distribution=[broadcast])
               +- SortAggregate(isMerge=[false], groupBy=[s_store_sk_node_22, s_store_id_node_22, s_rec_start_date_node_22, s_rec_end_date_node_22, s_closed_date_sk_node_22, s_store_name_node_22, s_number_employees_node_22, s_floor_space_node_22, s_hours_node_22, s_manager_node_22, s_market_id_node_22, s_geography_class_node_22, s_market_desc_node_22, s_market_manager_node_22, s_division_id_node_22, s_division_name_node_22, s_company_id_node_22, s_company_name_node_22, s_street_number_node_22, s_street_name_node_22, s_street_type_node_22, s_suite_number_node_22, s_city_node_22, s_county_node_22, s_state_node_22, s_zip_node_22, s_country_node_22, s_gmt_offset_node_22, s_tax_precentage_node_22, s_tax_precentage_node_220], select=[s_store_sk_node_22, s_store_id_node_22, s_rec_start_date_node_22, s_rec_end_date_node_22, s_closed_date_sk_node_22, s_store_name_node_22, s_number_employees_node_22, s_floor_space_node_22, s_hours_node_22, s_manager_node_22, s_market_id_node_22, s_geography_class_node_22, s_market_desc_node_22, s_market_manager_node_22, s_division_id_node_22, s_division_name_node_22, s_company_id_node_22, s_company_name_node_22, s_street_number_node_22, s_street_name_node_22, s_street_type_node_22, s_suite_number_node_22, s_city_node_22, s_county_node_22, s_state_node_22, s_zip_node_22, s_country_node_22, s_gmt_offset_node_22, s_tax_precentage_node_22, s_tax_precentage_node_220])
                  +- Exchange(distribution=[forward])
                     +- Sort(orderBy=[s_store_sk_node_22 ASC, s_store_id_node_22 ASC, s_rec_start_date_node_22 ASC, s_rec_end_date_node_22 ASC, s_closed_date_sk_node_22 ASC, s_store_name_node_22 ASC, s_number_employees_node_22 ASC, s_floor_space_node_22 ASC, s_hours_node_22 ASC, s_manager_node_22 ASC, s_market_id_node_22 ASC, s_geography_class_node_22 ASC, s_market_desc_node_22 ASC, s_market_manager_node_22 ASC, s_division_id_node_22 ASC, s_division_name_node_22 ASC, s_company_id_node_22 ASC, s_company_name_node_22 ASC, s_street_number_node_22 ASC, s_street_name_node_22 ASC, s_street_type_node_22 ASC, s_suite_number_node_22 ASC, s_city_node_22 ASC, s_county_node_22 ASC, s_state_node_22 ASC, s_zip_node_22 ASC, s_country_node_22 ASC, s_gmt_offset_node_22 ASC, s_tax_precentage_node_22 ASC, s_tax_precentage_node_220 ASC])
                        +- Exchange(distribution=[hash[s_store_sk_node_22, s_store_id_node_22, s_rec_start_date_node_22, s_rec_end_date_node_22, s_closed_date_sk_node_22, s_store_name_node_22, s_number_employees_node_22, s_floor_space_node_22, s_hours_node_22, s_manager_node_22, s_market_id_node_22, s_geography_class_node_22, s_market_desc_node_22, s_market_manager_node_22, s_division_id_node_22, s_division_name_node_22, s_company_id_node_22, s_company_name_node_22, s_street_number_node_22, s_street_name_node_22, s_street_type_node_22, s_suite_number_node_22, s_city_node_22, s_county_node_22, s_state_node_22, s_zip_node_22, s_country_node_22, s_gmt_offset_node_22, s_tax_precentage_node_22, s_tax_precentage_node_220]])
                           +- Calc(select=[s_store_sk AS s_store_sk_node_22, s_store_id AS s_store_id_node_22, s_rec_start_date AS s_rec_start_date_node_22, s_rec_end_date AS s_rec_end_date_node_22, s_closed_date_sk AS s_closed_date_sk_node_22, s_store_name AS s_store_name_node_22, s_number_employees AS s_number_employees_node_22, s_floor_space AS s_floor_space_node_22, s_hours AS s_hours_node_22, s_manager AS s_manager_node_22, s_market_id AS s_market_id_node_22, s_geography_class AS s_geography_class_node_22, s_market_desc AS s_market_desc_node_22, s_market_manager AS s_market_manager_node_22, s_division_id AS s_division_id_node_22, s_division_name AS s_division_name_node_22, s_company_id AS s_company_id_node_22, s_company_name AS s_company_name_node_22, s_street_number AS s_street_number_node_22, s_street_name AS s_street_name_node_22, s_street_type AS s_street_type_node_22, s_suite_number AS s_suite_number_node_22, s_city AS s_city_node_22, s_county AS s_county_node_22, s_state AS s_state_node_22, s_zip AS s_zip_node_22, s_country AS s_country_node_22, s_gmt_offset AS s_gmt_offset_node_22, s_tax_precentage AS s_tax_precentage_node_22, CAST(s_tax_precentage AS DECIMAL(21, 2)) AS s_tax_precentage_node_220], where=[((CHAR_LENGTH(s_division_name) >= 5) AND (CHAR_LENGTH(s_country) > 5))])
                              +- SortLimit(orderBy=[s_market_desc ASC], offset=[0], fetch=[1], global=[true])
                                 +- Exchange(distribution=[single])
                                    +- SortLimit(orderBy=[s_market_desc ASC], offset=[0], fetch=[1], global=[false])
                                       +- TableSourceScan(table=[[default_catalog, default_database, store]], fields=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0