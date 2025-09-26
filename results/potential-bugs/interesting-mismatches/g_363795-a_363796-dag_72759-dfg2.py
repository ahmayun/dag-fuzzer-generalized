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

autonode_6 = table_env.from_path("customer_address").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("customer_address").get_schema().get_field_names()])
autonode_7 = table_env.from_path("store").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("store").get_schema().get_field_names()])
autonode_4 = autonode_6.distinct()
autonode_5 = autonode_7.alias('grxkm')
autonode_2 = autonode_4.order_by(col('ca_location_type_node_6'))
autonode_3 = autonode_5.order_by(col('s_tax_precentage_node_7'))
autonode_1 = autonode_2.join(autonode_3, col('s_tax_precentage_node_7') == col('ca_gmt_offset_node_6'))
sink = autonode_1.group_by(col('s_tax_precentage_node_7')).select(col('s_gmt_offset_node_7').min.alias('s_gmt_offset_node_7'))
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
      "error_message": "An error occurred while calling o197971303.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#400366642:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[12](input=RelSubset#400366640,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[12]), rel#400366639:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[12](input=RelSubset#400366638,groupBy=ca_gmt_offset,select=ca_gmt_offset)]
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
Caused by: java.lang.IndexOutOfBoundsException: index (12) must be less than size (1)
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
LogicalProject(s_gmt_offset_node_7=[$1])
+- LogicalAggregate(group=[{41}], EXPR$0=[MIN($40)])
   +- LogicalJoin(condition=[=($41, $11)], joinType=[inner])
      :- LogicalSort(sort0=[$12], dir0=[ASC])
      :  +- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}])
      :     +- LogicalProject(ca_address_sk_node_6=[$0], ca_address_id_node_6=[$1], ca_street_number_node_6=[$2], ca_street_name_node_6=[$3], ca_street_type_node_6=[$4], ca_suite_number_node_6=[$5], ca_city_node_6=[$6], ca_county_node_6=[$7], ca_state_node_6=[$8], ca_zip_node_6=[$9], ca_country_node_6=[$10], ca_gmt_offset_node_6=[$11], ca_location_type_node_6=[$12])
      :        +- LogicalTableScan(table=[[default_catalog, default_database, customer_address]])
      +- LogicalSort(sort0=[$28], dir0=[ASC])
         +- LogicalProject(grxkm=[AS($0, _UTF-16LE'grxkm')], s_store_id_node_7=[$1], s_rec_start_date_node_7=[$2], s_rec_end_date_node_7=[$3], s_closed_date_sk_node_7=[$4], s_store_name_node_7=[$5], s_number_employees_node_7=[$6], s_floor_space_node_7=[$7], s_hours_node_7=[$8], s_manager_node_7=[$9], s_market_id_node_7=[$10], s_geography_class_node_7=[$11], s_market_desc_node_7=[$12], s_market_manager_node_7=[$13], s_division_id_node_7=[$14], s_division_name_node_7=[$15], s_company_id_node_7=[$16], s_company_name_node_7=[$17], s_street_number_node_7=[$18], s_street_name_node_7=[$19], s_street_type_node_7=[$20], s_suite_number_node_7=[$21], s_city_node_7=[$22], s_county_node_7=[$23], s_state_node_7=[$24], s_zip_node_7=[$25], s_country_node_7=[$26], s_gmt_offset_node_7=[$27], s_tax_precentage_node_7=[$28])
            +- LogicalTableScan(table=[[default_catalog, default_database, store]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS s_gmt_offset_node_7])
+- SortAggregate(isMerge=[false], groupBy=[s_tax_precentage_node_7], select=[s_tax_precentage_node_7, MIN(s_gmt_offset_node_7) AS EXPR$0])
   +- Sort(orderBy=[s_tax_precentage_node_7 ASC])
      +- NestedLoopJoin(joinType=[InnerJoin], where=[=(s_tax_precentage_node_7, ca_gmt_offset)], select=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type, grxkm, s_store_id_node_7, s_rec_start_date_node_7, s_rec_end_date_node_7, s_closed_date_sk_node_7, s_store_name_node_7, s_number_employees_node_7, s_floor_space_node_7, s_hours_node_7, s_manager_node_7, s_market_id_node_7, s_geography_class_node_7, s_market_desc_node_7, s_market_manager_node_7, s_division_id_node_7, s_division_name_node_7, s_company_id_node_7, s_company_name_node_7, s_street_number_node_7, s_street_name_node_7, s_street_type_node_7, s_suite_number_node_7, s_city_node_7, s_county_node_7, s_state_node_7, s_zip_node_7, s_country_node_7, s_gmt_offset_node_7, s_tax_precentage_node_7], build=[left])
         :- Exchange(distribution=[broadcast])
         :  +- SortLimit(orderBy=[ca_location_type ASC], offset=[0], fetch=[1], global=[true])
         :     +- Exchange(distribution=[single])
         :        +- SortLimit(orderBy=[ca_location_type ASC], offset=[0], fetch=[1], global=[false])
         :           +- HashAggregate(isMerge=[false], groupBy=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type], select=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])
         :              +- Exchange(distribution=[hash[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type]])
         :                 +- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])
         +- Exchange(distribution=[hash[s_tax_precentage_node_7]])
            +- Calc(select=[s_store_sk AS grxkm, s_store_id AS s_store_id_node_7, s_rec_start_date AS s_rec_start_date_node_7, s_rec_end_date AS s_rec_end_date_node_7, s_closed_date_sk AS s_closed_date_sk_node_7, s_store_name AS s_store_name_node_7, s_number_employees AS s_number_employees_node_7, s_floor_space AS s_floor_space_node_7, s_hours AS s_hours_node_7, s_manager AS s_manager_node_7, s_market_id AS s_market_id_node_7, s_geography_class AS s_geography_class_node_7, s_market_desc AS s_market_desc_node_7, s_market_manager AS s_market_manager_node_7, s_division_id AS s_division_id_node_7, s_division_name AS s_division_name_node_7, s_company_id AS s_company_id_node_7, s_company_name AS s_company_name_node_7, s_street_number AS s_street_number_node_7, s_street_name AS s_street_name_node_7, s_street_type AS s_street_type_node_7, s_suite_number AS s_suite_number_node_7, s_city AS s_city_node_7, s_county AS s_county_node_7, s_state AS s_state_node_7, s_zip AS s_zip_node_7, s_country AS s_country_node_7, s_gmt_offset AS s_gmt_offset_node_7, s_tax_precentage AS s_tax_precentage_node_7])
               +- SortLimit(orderBy=[s_tax_precentage ASC], offset=[0], fetch=[1], global=[true])
                  +- Exchange(distribution=[single])
                     +- SortLimit(orderBy=[s_tax_precentage ASC], offset=[0], fetch=[1], global=[false])
                        +- TableSourceScan(table=[[default_catalog, default_database, store]], fields=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS s_gmt_offset_node_7])
+- SortAggregate(isMerge=[false], groupBy=[s_tax_precentage_node_7], select=[s_tax_precentage_node_7, MIN(s_gmt_offset_node_7) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[s_tax_precentage_node_7 ASC])
         +- Exchange(distribution=[keep_input_as_is[hash[s_tax_precentage_node_7]]])
            +- NestedLoopJoin(joinType=[InnerJoin], where=[(s_tax_precentage_node_7 = ca_gmt_offset)], select=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type, grxkm, s_store_id_node_7, s_rec_start_date_node_7, s_rec_end_date_node_7, s_closed_date_sk_node_7, s_store_name_node_7, s_number_employees_node_7, s_floor_space_node_7, s_hours_node_7, s_manager_node_7, s_market_id_node_7, s_geography_class_node_7, s_market_desc_node_7, s_market_manager_node_7, s_division_id_node_7, s_division_name_node_7, s_company_id_node_7, s_company_name_node_7, s_street_number_node_7, s_street_name_node_7, s_street_type_node_7, s_suite_number_node_7, s_city_node_7, s_county_node_7, s_state_node_7, s_zip_node_7, s_country_node_7, s_gmt_offset_node_7, s_tax_precentage_node_7], build=[left])
               :- Exchange(distribution=[broadcast])
               :  +- SortLimit(orderBy=[ca_location_type ASC], offset=[0], fetch=[1], global=[true])
               :     +- Exchange(distribution=[single])
               :        +- SortLimit(orderBy=[ca_location_type ASC], offset=[0], fetch=[1], global=[false])
               :           +- HashAggregate(isMerge=[false], groupBy=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type], select=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])
               :              +- Exchange(distribution=[hash[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type]])
               :                 +- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])
               +- Exchange(distribution=[hash[s_tax_precentage_node_7]])
                  +- Calc(select=[s_store_sk AS grxkm, s_store_id AS s_store_id_node_7, s_rec_start_date AS s_rec_start_date_node_7, s_rec_end_date AS s_rec_end_date_node_7, s_closed_date_sk AS s_closed_date_sk_node_7, s_store_name AS s_store_name_node_7, s_number_employees AS s_number_employees_node_7, s_floor_space AS s_floor_space_node_7, s_hours AS s_hours_node_7, s_manager AS s_manager_node_7, s_market_id AS s_market_id_node_7, s_geography_class AS s_geography_class_node_7, s_market_desc AS s_market_desc_node_7, s_market_manager AS s_market_manager_node_7, s_division_id AS s_division_id_node_7, s_division_name AS s_division_name_node_7, s_company_id AS s_company_id_node_7, s_company_name AS s_company_name_node_7, s_street_number AS s_street_number_node_7, s_street_name AS s_street_name_node_7, s_street_type AS s_street_type_node_7, s_suite_number AS s_suite_number_node_7, s_city AS s_city_node_7, s_county AS s_county_node_7, s_state AS s_state_node_7, s_zip AS s_zip_node_7, s_country AS s_country_node_7, s_gmt_offset AS s_gmt_offset_node_7, s_tax_precentage AS s_tax_precentage_node_7])
                     +- SortLimit(orderBy=[s_tax_precentage ASC], offset=[0], fetch=[1], global=[true])
                        +- Exchange(distribution=[single])
                           +- SortLimit(orderBy=[s_tax_precentage ASC], offset=[0], fetch=[1], global=[false])
                              +- TableSourceScan(table=[[default_catalog, default_database, store]], fields=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0