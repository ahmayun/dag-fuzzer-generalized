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

autonode_9 = table_env.from_path("customer_address").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("customer_address").get_schema().get_field_names()])
autonode_8 = table_env.from_path("web_site").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("web_site").get_schema().get_field_names()])
autonode_7 = table_env.from_path("warehouse").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("warehouse").get_schema().get_field_names()])
autonode_6 = autonode_9.order_by(col('ca_street_name_node_9'))
autonode_5 = autonode_8.add_columns(lit("hello"))
autonode_4 = autonode_7.distinct()
autonode_3 = autonode_6.add_columns(lit("hello"))
autonode_2 = autonode_4.join(autonode_5, col('w_warehouse_sq_ft_node_7') == col('web_site_sk_node_8'))
autonode_1 = autonode_2.join(autonode_3, col('ca_gmt_offset_node_9') == col('web_gmt_offset_node_8'))
sink = autonode_1.group_by(col('w_state_node_7')).select(col('web_open_date_sk_node_8').min.alias('web_open_date_sk_node_8'))
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
      "error_message": "An error occurred while calling o205259500.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#415831721:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[3](input=RelSubset#415831719,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[3]), rel#415831718:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[3](input=RelSubset#415831717,groupBy=ca_gmt_offset,select=ca_gmt_offset)]
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
Caused by: java.lang.IndexOutOfBoundsException: index (3) must be less than size (1)
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
LogicalProject(web_open_date_sk_node_8=[$1])
+- LogicalAggregate(group=[{10}], EXPR$0=[MIN($19)])
   +- LogicalJoin(condition=[=($52, $38)], joinType=[inner])
      :- LogicalJoin(condition=[=($3, $14)], joinType=[inner])
      :  :- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}])
      :  :  +- LogicalProject(w_warehouse_sk_node_7=[$0], w_warehouse_id_node_7=[$1], w_warehouse_name_node_7=[$2], w_warehouse_sq_ft_node_7=[$3], w_street_number_node_7=[$4], w_street_name_node_7=[$5], w_street_type_node_7=[$6], w_suite_number_node_7=[$7], w_city_node_7=[$8], w_county_node_7=[$9], w_state_node_7=[$10], w_zip_node_7=[$11], w_country_node_7=[$12], w_gmt_offset_node_7=[$13])
      :  :     +- LogicalTableScan(table=[[default_catalog, default_database, warehouse]])
      :  +- LogicalProject(web_site_sk_node_8=[$0], web_site_id_node_8=[$1], web_rec_start_date_node_8=[$2], web_rec_end_date_node_8=[$3], web_name_node_8=[$4], web_open_date_sk_node_8=[$5], web_close_date_sk_node_8=[$6], web_class_node_8=[$7], web_manager_node_8=[$8], web_mkt_id_node_8=[$9], web_mkt_class_node_8=[$10], web_mkt_desc_node_8=[$11], web_market_manager_node_8=[$12], web_company_id_node_8=[$13], web_company_name_node_8=[$14], web_street_number_node_8=[$15], web_street_name_node_8=[$16], web_street_type_node_8=[$17], web_suite_number_node_8=[$18], web_city_node_8=[$19], web_county_node_8=[$20], web_state_node_8=[$21], web_zip_node_8=[$22], web_country_node_8=[$23], web_gmt_offset_node_8=[$24], web_tax_percentage_node_8=[$25], _c26=[_UTF-16LE'hello'])
      :     +- LogicalTableScan(table=[[default_catalog, default_database, web_site]])
      +- LogicalProject(ca_address_sk_node_9=[$0], ca_address_id_node_9=[$1], ca_street_number_node_9=[$2], ca_street_name_node_9=[$3], ca_street_type_node_9=[$4], ca_suite_number_node_9=[$5], ca_city_node_9=[$6], ca_county_node_9=[$7], ca_state_node_9=[$8], ca_zip_node_9=[$9], ca_country_node_9=[$10], ca_gmt_offset_node_9=[$11], ca_location_type_node_9=[$12], _c13=[_UTF-16LE'hello'])
         +- LogicalSort(sort0=[$3], dir0=[ASC])
            +- LogicalProject(ca_address_sk_node_9=[$0], ca_address_id_node_9=[$1], ca_street_number_node_9=[$2], ca_street_name_node_9=[$3], ca_street_type_node_9=[$4], ca_suite_number_node_9=[$5], ca_city_node_9=[$6], ca_county_node_9=[$7], ca_state_node_9=[$8], ca_zip_node_9=[$9], ca_country_node_9=[$10], ca_gmt_offset_node_9=[$11], ca_location_type_node_9=[$12])
               +- LogicalTableScan(table=[[default_catalog, default_database, customer_address]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS web_open_date_sk_node_8])
+- SortAggregate(isMerge=[false], groupBy=[w_state], select=[w_state, MIN(web_open_date_sk_node_8) AS EXPR$0])
   +- Sort(orderBy=[w_state ASC])
      +- Exchange(distribution=[hash[w_state]])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[=(ca_gmt_offset_node_9, web_gmt_offset_node_8)], select=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset, web_site_sk_node_8, web_site_id_node_8, web_rec_start_date_node_8, web_rec_end_date_node_8, web_name_node_8, web_open_date_sk_node_8, web_close_date_sk_node_8, web_class_node_8, web_manager_node_8, web_mkt_id_node_8, web_mkt_class_node_8, web_mkt_desc_node_8, web_market_manager_node_8, web_company_id_node_8, web_company_name_node_8, web_street_number_node_8, web_street_name_node_8, web_street_type_node_8, web_suite_number_node_8, web_city_node_8, web_county_node_8, web_state_node_8, web_zip_node_8, web_country_node_8, web_gmt_offset_node_8, web_tax_percentage_node_8, _c26, ca_address_sk_node_9, ca_address_id_node_9, ca_street_number_node_9, ca_street_name_node_9, ca_street_type_node_9, ca_suite_number_node_9, ca_city_node_9, ca_county_node_9, ca_state_node_9, ca_zip_node_9, ca_country_node_9, ca_gmt_offset_node_9, ca_location_type_node_9, _c13], build=[right])
            :- NestedLoopJoin(joinType=[InnerJoin], where=[=(w_warehouse_sq_ft, web_site_sk_node_8)], select=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset, web_site_sk_node_8, web_site_id_node_8, web_rec_start_date_node_8, web_rec_end_date_node_8, web_name_node_8, web_open_date_sk_node_8, web_close_date_sk_node_8, web_class_node_8, web_manager_node_8, web_mkt_id_node_8, web_mkt_class_node_8, web_mkt_desc_node_8, web_market_manager_node_8, web_company_id_node_8, web_company_name_node_8, web_street_number_node_8, web_street_name_node_8, web_street_type_node_8, web_suite_number_node_8, web_city_node_8, web_county_node_8, web_state_node_8, web_zip_node_8, web_country_node_8, web_gmt_offset_node_8, web_tax_percentage_node_8, _c26], build=[left])
            :  :- Exchange(distribution=[broadcast])
            :  :  +- SortAggregate(isMerge=[true], groupBy=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset], select=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset])
            :  :     +- Sort(orderBy=[w_warehouse_sk ASC, w_warehouse_id ASC, w_warehouse_name ASC, w_warehouse_sq_ft ASC, w_street_number ASC, w_street_name ASC, w_street_type ASC, w_suite_number ASC, w_city ASC, w_county ASC, w_state ASC, w_zip ASC, w_country ASC, w_gmt_offset ASC])
            :  :        +- Exchange(distribution=[hash[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset]])
            :  :           +- LocalSortAggregate(groupBy=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset], select=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset])
            :  :              +- Sort(orderBy=[w_warehouse_sk ASC, w_warehouse_id ASC, w_warehouse_name ASC, w_warehouse_sq_ft ASC, w_street_number ASC, w_street_name ASC, w_street_type ASC, w_suite_number ASC, w_city ASC, w_county ASC, w_state ASC, w_zip ASC, w_country ASC, w_gmt_offset ASC])
            :  :                 +- TableSourceScan(table=[[default_catalog, default_database, warehouse]], fields=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset])
            :  +- Calc(select=[web_site_sk AS web_site_sk_node_8, web_site_id AS web_site_id_node_8, web_rec_start_date AS web_rec_start_date_node_8, web_rec_end_date AS web_rec_end_date_node_8, web_name AS web_name_node_8, web_open_date_sk AS web_open_date_sk_node_8, web_close_date_sk AS web_close_date_sk_node_8, web_class AS web_class_node_8, web_manager AS web_manager_node_8, web_mkt_id AS web_mkt_id_node_8, web_mkt_class AS web_mkt_class_node_8, web_mkt_desc AS web_mkt_desc_node_8, web_market_manager AS web_market_manager_node_8, web_company_id AS web_company_id_node_8, web_company_name AS web_company_name_node_8, web_street_number AS web_street_number_node_8, web_street_name AS web_street_name_node_8, web_street_type AS web_street_type_node_8, web_suite_number AS web_suite_number_node_8, web_city AS web_city_node_8, web_county AS web_county_node_8, web_state AS web_state_node_8, web_zip AS web_zip_node_8, web_country AS web_country_node_8, web_gmt_offset AS web_gmt_offset_node_8, web_tax_percentage AS web_tax_percentage_node_8, 'hello' AS _c26])
            :     +- TableSourceScan(table=[[default_catalog, default_database, web_site]], fields=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage])
            +- Exchange(distribution=[broadcast])
               +- Calc(select=[ca_address_sk AS ca_address_sk_node_9, ca_address_id AS ca_address_id_node_9, ca_street_number AS ca_street_number_node_9, ca_street_name AS ca_street_name_node_9, ca_street_type AS ca_street_type_node_9, ca_suite_number AS ca_suite_number_node_9, ca_city AS ca_city_node_9, ca_county AS ca_county_node_9, ca_state AS ca_state_node_9, ca_zip AS ca_zip_node_9, ca_country AS ca_country_node_9, ca_gmt_offset AS ca_gmt_offset_node_9, ca_location_type AS ca_location_type_node_9, 'hello' AS _c13])
                  +- SortLimit(orderBy=[ca_street_name ASC], offset=[0], fetch=[1], global=[true])
                     +- Exchange(distribution=[single])
                        +- SortLimit(orderBy=[ca_street_name ASC], offset=[0], fetch=[1], global=[false])
                           +- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS web_open_date_sk_node_8])
+- SortAggregate(isMerge=[false], groupBy=[w_state], select=[w_state, MIN(web_open_date_sk_node_8) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[w_state ASC])
         +- Exchange(distribution=[hash[w_state]])
            +- NestedLoopJoin(joinType=[InnerJoin], where=[(ca_gmt_offset_node_9 = web_gmt_offset_node_8)], select=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset, web_site_sk_node_8, web_site_id_node_8, web_rec_start_date_node_8, web_rec_end_date_node_8, web_name_node_8, web_open_date_sk_node_8, web_close_date_sk_node_8, web_class_node_8, web_manager_node_8, web_mkt_id_node_8, web_mkt_class_node_8, web_mkt_desc_node_8, web_market_manager_node_8, web_company_id_node_8, web_company_name_node_8, web_street_number_node_8, web_street_name_node_8, web_street_type_node_8, web_suite_number_node_8, web_city_node_8, web_county_node_8, web_state_node_8, web_zip_node_8, web_country_node_8, web_gmt_offset_node_8, web_tax_percentage_node_8, _c26, ca_address_sk_node_9, ca_address_id_node_9, ca_street_number_node_9, ca_street_name_node_9, ca_street_type_node_9, ca_suite_number_node_9, ca_city_node_9, ca_county_node_9, ca_state_node_9, ca_zip_node_9, ca_country_node_9, ca_gmt_offset_node_9, ca_location_type_node_9, _c13], build=[right])
               :- NestedLoopJoin(joinType=[InnerJoin], where=[(w_warehouse_sq_ft = web_site_sk_node_8)], select=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset, web_site_sk_node_8, web_site_id_node_8, web_rec_start_date_node_8, web_rec_end_date_node_8, web_name_node_8, web_open_date_sk_node_8, web_close_date_sk_node_8, web_class_node_8, web_manager_node_8, web_mkt_id_node_8, web_mkt_class_node_8, web_mkt_desc_node_8, web_market_manager_node_8, web_company_id_node_8, web_company_name_node_8, web_street_number_node_8, web_street_name_node_8, web_street_type_node_8, web_suite_number_node_8, web_city_node_8, web_county_node_8, web_state_node_8, web_zip_node_8, web_country_node_8, web_gmt_offset_node_8, web_tax_percentage_node_8, _c26], build=[left])
               :  :- Exchange(distribution=[broadcast])
               :  :  +- SortAggregate(isMerge=[true], groupBy=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset], select=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset])
               :  :     +- Exchange(distribution=[forward])
               :  :        +- Sort(orderBy=[w_warehouse_sk ASC, w_warehouse_id ASC, w_warehouse_name ASC, w_warehouse_sq_ft ASC, w_street_number ASC, w_street_name ASC, w_street_type ASC, w_suite_number ASC, w_city ASC, w_county ASC, w_state ASC, w_zip ASC, w_country ASC, w_gmt_offset ASC])
               :  :           +- Exchange(distribution=[hash[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset]])
               :  :              +- LocalSortAggregate(groupBy=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset], select=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset])
               :  :                 +- Exchange(distribution=[forward])
               :  :                    +- Sort(orderBy=[w_warehouse_sk ASC, w_warehouse_id ASC, w_warehouse_name ASC, w_warehouse_sq_ft ASC, w_street_number ASC, w_street_name ASC, w_street_type ASC, w_suite_number ASC, w_city ASC, w_county ASC, w_state ASC, w_zip ASC, w_country ASC, w_gmt_offset ASC])
               :  :                       +- TableSourceScan(table=[[default_catalog, default_database, warehouse]], fields=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset])
               :  +- Calc(select=[web_site_sk AS web_site_sk_node_8, web_site_id AS web_site_id_node_8, web_rec_start_date AS web_rec_start_date_node_8, web_rec_end_date AS web_rec_end_date_node_8, web_name AS web_name_node_8, web_open_date_sk AS web_open_date_sk_node_8, web_close_date_sk AS web_close_date_sk_node_8, web_class AS web_class_node_8, web_manager AS web_manager_node_8, web_mkt_id AS web_mkt_id_node_8, web_mkt_class AS web_mkt_class_node_8, web_mkt_desc AS web_mkt_desc_node_8, web_market_manager AS web_market_manager_node_8, web_company_id AS web_company_id_node_8, web_company_name AS web_company_name_node_8, web_street_number AS web_street_number_node_8, web_street_name AS web_street_name_node_8, web_street_type AS web_street_type_node_8, web_suite_number AS web_suite_number_node_8, web_city AS web_city_node_8, web_county AS web_county_node_8, web_state AS web_state_node_8, web_zip AS web_zip_node_8, web_country AS web_country_node_8, web_gmt_offset AS web_gmt_offset_node_8, web_tax_percentage AS web_tax_percentage_node_8, 'hello' AS _c26])
               :     +- TableSourceScan(table=[[default_catalog, default_database, web_site]], fields=[web_site_sk, web_site_id, web_rec_start_date, web_rec_end_date, web_name, web_open_date_sk, web_close_date_sk, web_class, web_manager, web_mkt_id, web_mkt_class, web_mkt_desc, web_market_manager, web_company_id, web_company_name, web_street_number, web_street_name, web_street_type, web_suite_number, web_city, web_county, web_state, web_zip, web_country, web_gmt_offset, web_tax_percentage])
               +- Exchange(distribution=[broadcast])
                  +- Calc(select=[ca_address_sk AS ca_address_sk_node_9, ca_address_id AS ca_address_id_node_9, ca_street_number AS ca_street_number_node_9, ca_street_name AS ca_street_name_node_9, ca_street_type AS ca_street_type_node_9, ca_suite_number AS ca_suite_number_node_9, ca_city AS ca_city_node_9, ca_county AS ca_county_node_9, ca_state AS ca_state_node_9, ca_zip AS ca_zip_node_9, ca_country AS ca_country_node_9, ca_gmt_offset AS ca_gmt_offset_node_9, ca_location_type AS ca_location_type_node_9, 'hello' AS _c13])
                     +- SortLimit(orderBy=[ca_street_name ASC], offset=[0], fetch=[1], global=[true])
                        +- Exchange(distribution=[single])
                           +- SortLimit(orderBy=[ca_street_name ASC], offset=[0], fetch=[1], global=[false])
                              +- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0