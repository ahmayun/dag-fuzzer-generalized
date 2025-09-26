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

autonode_13 = table_env.from_path("item").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("item").get_schema().get_field_names()])
autonode_12 = table_env.from_path("warehouse").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("warehouse").get_schema().get_field_names()])
autonode_11 = table_env.from_path("web_page").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("web_page").get_schema().get_field_names()])
autonode_10 = autonode_13.limit(63)
autonode_9 = autonode_12.select(col('w_county_node_12'))
autonode_8 = autonode_11.alias('aI6ra')
autonode_7 = autonode_10.filter(col('i_color_node_13').char_length < 5)
autonode_6 = autonode_9.select(col('w_county_node_12'))
autonode_5 = autonode_8.order_by(col('wp_char_count_node_11'))
autonode_4 = autonode_7.add_columns(lit("hello"))
autonode_3 = autonode_5.join(autonode_6, col('w_county_node_12') == col('wp_type_node_11'))
autonode_2 = autonode_3.join(autonode_4, col('i_product_name_node_13') == col('w_county_node_12'))
autonode_1 = autonode_2.filter(col('i_rec_start_date_node_13').char_length > 5)
sink = autonode_1.group_by(col('i_category_node_13')).select(col('i_class_id_node_13').min.alias('i_class_id_node_13'))
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
      "error_message": "An error occurred while calling o195889401.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#396165808:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[10](input=RelSubset#396165806,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[10]), rel#396165805:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[10](input=RelSubset#396165804,groupBy=wp_type,select=wp_type)]
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
Caused by: java.lang.IndexOutOfBoundsException: index (10) must be less than size (1)
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
LogicalProject(i_class_id_node_13=[$1])
+- LogicalAggregate(group=[{27}], EXPR$0=[MIN($24)])
   +- LogicalFilter(condition=[>(CHAR_LENGTH($17), 5)])
      +- LogicalJoin(condition=[=($36, $14)], joinType=[inner])
         :- LogicalJoin(condition=[=($14, $9)], joinType=[inner])
         :  :- LogicalSort(sort0=[$10], dir0=[ASC])
         :  :  +- LogicalProject(aI6ra=[AS($0, _UTF-16LE'aI6ra')], wp_web_page_id_node_11=[$1], wp_rec_start_date_node_11=[$2], wp_rec_end_date_node_11=[$3], wp_creation_date_sk_node_11=[$4], wp_access_date_sk_node_11=[$5], wp_autogen_flag_node_11=[$6], wp_customer_sk_node_11=[$7], wp_url_node_11=[$8], wp_type_node_11=[$9], wp_char_count_node_11=[$10], wp_link_count_node_11=[$11], wp_image_count_node_11=[$12], wp_max_ad_count_node_11=[$13])
         :  :     +- LogicalTableScan(table=[[default_catalog, default_database, web_page]])
         :  +- LogicalProject(w_county_node_12=[$9])
         :     +- LogicalTableScan(table=[[default_catalog, default_database, warehouse]])
         +- LogicalProject(i_item_sk_node_13=[$0], i_item_id_node_13=[$1], i_rec_start_date_node_13=[$2], i_rec_end_date_node_13=[$3], i_item_desc_node_13=[$4], i_current_price_node_13=[$5], i_wholesale_cost_node_13=[$6], i_brand_id_node_13=[$7], i_brand_node_13=[$8], i_class_id_node_13=[$9], i_class_node_13=[$10], i_category_id_node_13=[$11], i_category_node_13=[$12], i_manufact_id_node_13=[$13], i_manufact_node_13=[$14], i_size_node_13=[$15], i_formulation_node_13=[$16], i_color_node_13=[$17], i_units_node_13=[$18], i_container_node_13=[$19], i_manager_id_node_13=[$20], i_product_name_node_13=[$21], _c22=[_UTF-16LE'hello'])
            +- LogicalFilter(condition=[<(CHAR_LENGTH($17), 5)])
               +- LogicalSort(fetch=[63])
                  +- LogicalProject(i_item_sk_node_13=[$0], i_item_id_node_13=[$1], i_rec_start_date_node_13=[$2], i_rec_end_date_node_13=[$3], i_item_desc_node_13=[$4], i_current_price_node_13=[$5], i_wholesale_cost_node_13=[$6], i_brand_id_node_13=[$7], i_brand_node_13=[$8], i_class_id_node_13=[$9], i_class_node_13=[$10], i_category_id_node_13=[$11], i_category_node_13=[$12], i_manufact_id_node_13=[$13], i_manufact_node_13=[$14], i_size_node_13=[$15], i_formulation_node_13=[$16], i_color_node_13=[$17], i_units_node_13=[$18], i_container_node_13=[$19], i_manager_id_node_13=[$20], i_product_name_node_13=[$21])
                     +- LogicalTableScan(table=[[default_catalog, default_database, item]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS i_class_id_node_13])
+- SortAggregate(isMerge=[false], groupBy=[i_category_node_13], select=[i_category_node_13, MIN(i_class_id_node_13) AS EXPR$0])
   +- Sort(orderBy=[i_category_node_13 ASC])
      +- Exchange(distribution=[hash[i_category_node_13]])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[=(i_product_name_node_13, w_county)], select=[aI6ra, wp_web_page_id_node_11, wp_rec_start_date_node_11, wp_rec_end_date_node_11, wp_creation_date_sk_node_11, wp_access_date_sk_node_11, wp_autogen_flag_node_11, wp_customer_sk_node_11, wp_url_node_11, wp_type_node_11, wp_char_count_node_11, wp_link_count_node_11, wp_image_count_node_11, wp_max_ad_count_node_11, w_county, i_item_sk_node_13, i_item_id_node_13, i_rec_start_date_node_13, i_rec_end_date_node_13, i_item_desc_node_13, i_current_price_node_13, i_wholesale_cost_node_13, i_brand_id_node_13, i_brand_node_13, i_class_id_node_13, i_class_node_13, i_category_id_node_13, i_category_node_13, i_manufact_id_node_13, i_manufact_node_13, i_size_node_13, i_formulation_node_13, i_color_node_13, i_units_node_13, i_container_node_13, i_manager_id_node_13, i_product_name_node_13, _c22], build=[left])
            :- Exchange(distribution=[broadcast])
            :  +- NestedLoopJoin(joinType=[InnerJoin], where=[=(w_county, wp_type_node_11)], select=[aI6ra, wp_web_page_id_node_11, wp_rec_start_date_node_11, wp_rec_end_date_node_11, wp_creation_date_sk_node_11, wp_access_date_sk_node_11, wp_autogen_flag_node_11, wp_customer_sk_node_11, wp_url_node_11, wp_type_node_11, wp_char_count_node_11, wp_link_count_node_11, wp_image_count_node_11, wp_max_ad_count_node_11, w_county], build=[right])
            :     :- Calc(select=[wp_web_page_sk AS aI6ra, wp_web_page_id AS wp_web_page_id_node_11, wp_rec_start_date AS wp_rec_start_date_node_11, wp_rec_end_date AS wp_rec_end_date_node_11, wp_creation_date_sk AS wp_creation_date_sk_node_11, wp_access_date_sk AS wp_access_date_sk_node_11, wp_autogen_flag AS wp_autogen_flag_node_11, wp_customer_sk AS wp_customer_sk_node_11, wp_url AS wp_url_node_11, wp_type AS wp_type_node_11, wp_char_count AS wp_char_count_node_11, wp_link_count AS wp_link_count_node_11, wp_image_count AS wp_image_count_node_11, wp_max_ad_count AS wp_max_ad_count_node_11])
            :     :  +- SortLimit(orderBy=[wp_char_count ASC], offset=[0], fetch=[1], global=[true])
            :     :     +- Exchange(distribution=[single])
            :     :        +- SortLimit(orderBy=[wp_char_count ASC], offset=[0], fetch=[1], global=[false])
            :     :           +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
            :     +- Exchange(distribution=[broadcast])
            :        +- TableSourceScan(table=[[default_catalog, default_database, warehouse, project=[w_county], metadata=[]]], fields=[w_county])
            +- Calc(select=[i_item_sk AS i_item_sk_node_13, i_item_id AS i_item_id_node_13, i_rec_start_date AS i_rec_start_date_node_13, i_rec_end_date AS i_rec_end_date_node_13, i_item_desc AS i_item_desc_node_13, i_current_price AS i_current_price_node_13, i_wholesale_cost AS i_wholesale_cost_node_13, i_brand_id AS i_brand_id_node_13, i_brand AS i_brand_node_13, i_class_id AS i_class_id_node_13, i_class AS i_class_node_13, i_category_id AS i_category_id_node_13, i_category AS i_category_node_13, i_manufact_id AS i_manufact_id_node_13, i_manufact AS i_manufact_node_13, i_size AS i_size_node_13, i_formulation AS i_formulation_node_13, i_color AS i_color_node_13, i_units AS i_units_node_13, i_container AS i_container_node_13, i_manager_id AS i_manager_id_node_13, i_product_name AS i_product_name_node_13, 'hello' AS _c22], where=[AND(<(CHAR_LENGTH(i_color), 5), >(CHAR_LENGTH(i_rec_start_date), 5))])
               +- Limit(offset=[0], fetch=[63], global=[true])
                  +- Exchange(distribution=[single])
                     +- Limit(offset=[0], fetch=[63], global=[false])
                        +- TableSourceScan(table=[[default_catalog, default_database, item, limit=[63]]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS i_class_id_node_13])
+- SortAggregate(isMerge=[false], groupBy=[i_category_node_13], select=[i_category_node_13, MIN(i_class_id_node_13) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[i_category_node_13 ASC])
         +- Exchange(distribution=[hash[i_category_node_13]])
            +- NestedLoopJoin(joinType=[InnerJoin], where=[(i_product_name_node_13 = w_county)], select=[aI6ra, wp_web_page_id_node_11, wp_rec_start_date_node_11, wp_rec_end_date_node_11, wp_creation_date_sk_node_11, wp_access_date_sk_node_11, wp_autogen_flag_node_11, wp_customer_sk_node_11, wp_url_node_11, wp_type_node_11, wp_char_count_node_11, wp_link_count_node_11, wp_image_count_node_11, wp_max_ad_count_node_11, w_county, i_item_sk_node_13, i_item_id_node_13, i_rec_start_date_node_13, i_rec_end_date_node_13, i_item_desc_node_13, i_current_price_node_13, i_wholesale_cost_node_13, i_brand_id_node_13, i_brand_node_13, i_class_id_node_13, i_class_node_13, i_category_id_node_13, i_category_node_13, i_manufact_id_node_13, i_manufact_node_13, i_size_node_13, i_formulation_node_13, i_color_node_13, i_units_node_13, i_container_node_13, i_manager_id_node_13, i_product_name_node_13, _c22], build=[left])
               :- Exchange(distribution=[broadcast])
               :  +- NestedLoopJoin(joinType=[InnerJoin], where=[(w_county = wp_type_node_11)], select=[aI6ra, wp_web_page_id_node_11, wp_rec_start_date_node_11, wp_rec_end_date_node_11, wp_creation_date_sk_node_11, wp_access_date_sk_node_11, wp_autogen_flag_node_11, wp_customer_sk_node_11, wp_url_node_11, wp_type_node_11, wp_char_count_node_11, wp_link_count_node_11, wp_image_count_node_11, wp_max_ad_count_node_11, w_county], build=[right])
               :     :- Calc(select=[wp_web_page_sk AS aI6ra, wp_web_page_id AS wp_web_page_id_node_11, wp_rec_start_date AS wp_rec_start_date_node_11, wp_rec_end_date AS wp_rec_end_date_node_11, wp_creation_date_sk AS wp_creation_date_sk_node_11, wp_access_date_sk AS wp_access_date_sk_node_11, wp_autogen_flag AS wp_autogen_flag_node_11, wp_customer_sk AS wp_customer_sk_node_11, wp_url AS wp_url_node_11, wp_type AS wp_type_node_11, wp_char_count AS wp_char_count_node_11, wp_link_count AS wp_link_count_node_11, wp_image_count AS wp_image_count_node_11, wp_max_ad_count AS wp_max_ad_count_node_11])
               :     :  +- SortLimit(orderBy=[wp_char_count ASC], offset=[0], fetch=[1], global=[true])
               :     :     +- Exchange(distribution=[single])
               :     :        +- SortLimit(orderBy=[wp_char_count ASC], offset=[0], fetch=[1], global=[false])
               :     :           +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
               :     +- Exchange(distribution=[broadcast])
               :        +- TableSourceScan(table=[[default_catalog, default_database, warehouse, project=[w_county], metadata=[]]], fields=[w_county])
               +- Calc(select=[i_item_sk AS i_item_sk_node_13, i_item_id AS i_item_id_node_13, i_rec_start_date AS i_rec_start_date_node_13, i_rec_end_date AS i_rec_end_date_node_13, i_item_desc AS i_item_desc_node_13, i_current_price AS i_current_price_node_13, i_wholesale_cost AS i_wholesale_cost_node_13, i_brand_id AS i_brand_id_node_13, i_brand AS i_brand_node_13, i_class_id AS i_class_id_node_13, i_class AS i_class_node_13, i_category_id AS i_category_id_node_13, i_category AS i_category_node_13, i_manufact_id AS i_manufact_id_node_13, i_manufact AS i_manufact_node_13, i_size AS i_size_node_13, i_formulation AS i_formulation_node_13, i_color AS i_color_node_13, i_units AS i_units_node_13, i_container AS i_container_node_13, i_manager_id AS i_manager_id_node_13, i_product_name AS i_product_name_node_13, 'hello' AS _c22], where=[((CHAR_LENGTH(i_color) < 5) AND (CHAR_LENGTH(i_rec_start_date) > 5))])
                  +- Limit(offset=[0], fetch=[63], global=[true])
                     +- Exchange(distribution=[single])
                        +- Limit(offset=[0], fetch=[63], global=[false])
                           +- TableSourceScan(table=[[default_catalog, default_database, item, limit=[63]]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0