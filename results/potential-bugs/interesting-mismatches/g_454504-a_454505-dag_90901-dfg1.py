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
    return values.product()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_8 = table_env.from_path("inventory").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("inventory").get_schema().get_field_names()])
autonode_7 = table_env.from_path("store").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("store").get_schema().get_field_names()])
autonode_6 = autonode_8.add_columns(lit("hello"))
autonode_5 = autonode_7.add_columns(lit("hello"))
autonode_4 = autonode_6.order_by(col('inv_warehouse_sk_node_8'))
autonode_3 = autonode_5.limit(31)
autonode_2 = autonode_3.join(autonode_4, col('inv_date_sk_node_8') == col('s_market_id_node_7'))
autonode_1 = autonode_2.filter(col('s_manager_node_7').char_length < 5)
sink = autonode_1.group_by(col('s_store_id_node_7')).select(col('s_market_desc_node_7').min.alias('s_market_desc_node_7'))
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
      "error_message": "An error occurred while calling o247415413.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#500680582:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[2](input=RelSubset#500680580,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[2]), rel#500680579:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[2](input=RelSubset#500680578,groupBy=inv_date_sk,select=inv_date_sk)]
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
Caused by: java.lang.IndexOutOfBoundsException: index (2) must be less than size (1)
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
LogicalProject(s_market_desc_node_7=[$1])
+- LogicalAggregate(group=[{1}], EXPR$0=[MIN($12)])
   +- LogicalFilter(condition=[<(CHAR_LENGTH($9), 5)])
      +- LogicalJoin(condition=[=($30, $10)], joinType=[inner])
         :- LogicalSort(fetch=[31])
         :  +- LogicalProject(s_store_sk_node_7=[$0], s_store_id_node_7=[$1], s_rec_start_date_node_7=[$2], s_rec_end_date_node_7=[$3], s_closed_date_sk_node_7=[$4], s_store_name_node_7=[$5], s_number_employees_node_7=[$6], s_floor_space_node_7=[$7], s_hours_node_7=[$8], s_manager_node_7=[$9], s_market_id_node_7=[$10], s_geography_class_node_7=[$11], s_market_desc_node_7=[$12], s_market_manager_node_7=[$13], s_division_id_node_7=[$14], s_division_name_node_7=[$15], s_company_id_node_7=[$16], s_company_name_node_7=[$17], s_street_number_node_7=[$18], s_street_name_node_7=[$19], s_street_type_node_7=[$20], s_suite_number_node_7=[$21], s_city_node_7=[$22], s_county_node_7=[$23], s_state_node_7=[$24], s_zip_node_7=[$25], s_country_node_7=[$26], s_gmt_offset_node_7=[$27], s_tax_precentage_node_7=[$28], _c29=[_UTF-16LE'hello'])
         :     +- LogicalTableScan(table=[[default_catalog, default_database, store]])
         +- LogicalSort(sort0=[$2], dir0=[ASC])
            +- LogicalProject(inv_date_sk_node_8=[$0], inv_item_sk_node_8=[$1], inv_warehouse_sk_node_8=[$2], inv_quantity_on_hand_node_8=[$3], _c4=[_UTF-16LE'hello'])
               +- LogicalTableScan(table=[[default_catalog, default_database, inventory]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS s_market_desc_node_7])
+- SortAggregate(isMerge=[true], groupBy=[s_store_id_node_7], select=[s_store_id_node_7, Final_MIN(min$0) AS EXPR$0])
   +- Sort(orderBy=[s_store_id_node_7 ASC])
      +- Exchange(distribution=[hash[s_store_id_node_7]])
         +- LocalSortAggregate(groupBy=[s_store_id_node_7], select=[s_store_id_node_7, Partial_MIN(s_market_desc_node_7) AS min$0])
            +- Sort(orderBy=[s_store_id_node_7 ASC])
               +- NestedLoopJoin(joinType=[InnerJoin], where=[=(inv_date_sk_node_8, s_market_id_node_7)], select=[s_store_sk_node_7, s_store_id_node_7, s_rec_start_date_node_7, s_rec_end_date_node_7, s_closed_date_sk_node_7, s_store_name_node_7, s_number_employees_node_7, s_floor_space_node_7, s_hours_node_7, s_manager_node_7, s_market_id_node_7, s_geography_class_node_7, s_market_desc_node_7, s_market_manager_node_7, s_division_id_node_7, s_division_name_node_7, s_company_id_node_7, s_company_name_node_7, s_street_number_node_7, s_street_name_node_7, s_street_type_node_7, s_suite_number_node_7, s_city_node_7, s_county_node_7, s_state_node_7, s_zip_node_7, s_country_node_7, s_gmt_offset_node_7, s_tax_precentage_node_7, _c29, inv_date_sk_node_8, inv_item_sk_node_8, inv_warehouse_sk_node_8, inv_quantity_on_hand_node_8, _c4], build=[right])
                  :- Calc(select=[s_store_sk AS s_store_sk_node_7, s_store_id AS s_store_id_node_7, s_rec_start_date AS s_rec_start_date_node_7, s_rec_end_date AS s_rec_end_date_node_7, s_closed_date_sk AS s_closed_date_sk_node_7, s_store_name AS s_store_name_node_7, s_number_employees AS s_number_employees_node_7, s_floor_space AS s_floor_space_node_7, s_hours AS s_hours_node_7, s_manager AS s_manager_node_7, s_market_id AS s_market_id_node_7, s_geography_class AS s_geography_class_node_7, s_market_desc AS s_market_desc_node_7, s_market_manager AS s_market_manager_node_7, s_division_id AS s_division_id_node_7, s_division_name AS s_division_name_node_7, s_company_id AS s_company_id_node_7, s_company_name AS s_company_name_node_7, s_street_number AS s_street_number_node_7, s_street_name AS s_street_name_node_7, s_street_type AS s_street_type_node_7, s_suite_number AS s_suite_number_node_7, s_city AS s_city_node_7, s_county AS s_county_node_7, s_state AS s_state_node_7, s_zip AS s_zip_node_7, s_country AS s_country_node_7, s_gmt_offset AS s_gmt_offset_node_7, s_tax_precentage AS s_tax_precentage_node_7, 'hello' AS _c29], where=[<(CHAR_LENGTH(s_manager), 5)])
                  :  +- Limit(offset=[0], fetch=[31], global=[true])
                  :     +- Exchange(distribution=[single])
                  :        +- Limit(offset=[0], fetch=[31], global=[false])
                  :           +- TableSourceScan(table=[[default_catalog, default_database, store]], fields=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage])
                  +- Exchange(distribution=[broadcast])
                     +- Calc(select=[inv_date_sk AS inv_date_sk_node_8, inv_item_sk AS inv_item_sk_node_8, inv_warehouse_sk AS inv_warehouse_sk_node_8, inv_quantity_on_hand AS inv_quantity_on_hand_node_8, 'hello' AS _c4])
                        +- SortLimit(orderBy=[inv_warehouse_sk ASC], offset=[0], fetch=[1], global=[true])
                           +- Exchange(distribution=[single])
                              +- SortLimit(orderBy=[inv_warehouse_sk ASC], offset=[0], fetch=[1], global=[false])
                                 +- TableSourceScan(table=[[default_catalog, default_database, inventory]], fields=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS s_market_desc_node_7])
+- SortAggregate(isMerge=[true], groupBy=[s_store_id_node_7], select=[s_store_id_node_7, Final_MIN(min$0) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[s_store_id_node_7 ASC])
         +- Exchange(distribution=[hash[s_store_id_node_7]])
            +- LocalSortAggregate(groupBy=[s_store_id_node_7], select=[s_store_id_node_7, Partial_MIN(s_market_desc_node_7) AS min$0])
               +- Exchange(distribution=[forward])
                  +- Sort(orderBy=[s_store_id_node_7 ASC])
                     +- NestedLoopJoin(joinType=[InnerJoin], where=[(inv_date_sk_node_8 = s_market_id_node_7)], select=[s_store_sk_node_7, s_store_id_node_7, s_rec_start_date_node_7, s_rec_end_date_node_7, s_closed_date_sk_node_7, s_store_name_node_7, s_number_employees_node_7, s_floor_space_node_7, s_hours_node_7, s_manager_node_7, s_market_id_node_7, s_geography_class_node_7, s_market_desc_node_7, s_market_manager_node_7, s_division_id_node_7, s_division_name_node_7, s_company_id_node_7, s_company_name_node_7, s_street_number_node_7, s_street_name_node_7, s_street_type_node_7, s_suite_number_node_7, s_city_node_7, s_county_node_7, s_state_node_7, s_zip_node_7, s_country_node_7, s_gmt_offset_node_7, s_tax_precentage_node_7, _c29, inv_date_sk_node_8, inv_item_sk_node_8, inv_warehouse_sk_node_8, inv_quantity_on_hand_node_8, _c4], build=[right])
                        :- Calc(select=[s_store_sk AS s_store_sk_node_7, s_store_id AS s_store_id_node_7, s_rec_start_date AS s_rec_start_date_node_7, s_rec_end_date AS s_rec_end_date_node_7, s_closed_date_sk AS s_closed_date_sk_node_7, s_store_name AS s_store_name_node_7, s_number_employees AS s_number_employees_node_7, s_floor_space AS s_floor_space_node_7, s_hours AS s_hours_node_7, s_manager AS s_manager_node_7, s_market_id AS s_market_id_node_7, s_geography_class AS s_geography_class_node_7, s_market_desc AS s_market_desc_node_7, s_market_manager AS s_market_manager_node_7, s_division_id AS s_division_id_node_7, s_division_name AS s_division_name_node_7, s_company_id AS s_company_id_node_7, s_company_name AS s_company_name_node_7, s_street_number AS s_street_number_node_7, s_street_name AS s_street_name_node_7, s_street_type AS s_street_type_node_7, s_suite_number AS s_suite_number_node_7, s_city AS s_city_node_7, s_county AS s_county_node_7, s_state AS s_state_node_7, s_zip AS s_zip_node_7, s_country AS s_country_node_7, s_gmt_offset AS s_gmt_offset_node_7, s_tax_precentage AS s_tax_precentage_node_7, 'hello' AS _c29], where=[(CHAR_LENGTH(s_manager) < 5)])
                        :  +- Limit(offset=[0], fetch=[31], global=[true])
                        :     +- Exchange(distribution=[single])
                        :        +- Limit(offset=[0], fetch=[31], global=[false])
                        :           +- TableSourceScan(table=[[default_catalog, default_database, store]], fields=[s_store_sk, s_store_id, s_rec_start_date, s_rec_end_date, s_closed_date_sk, s_store_name, s_number_employees, s_floor_space, s_hours, s_manager, s_market_id, s_geography_class, s_market_desc, s_market_manager, s_division_id, s_division_name, s_company_id, s_company_name, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, s_country, s_gmt_offset, s_tax_precentage])
                        +- Exchange(distribution=[broadcast])
                           +- Calc(select=[inv_date_sk AS inv_date_sk_node_8, inv_item_sk AS inv_item_sk_node_8, inv_warehouse_sk AS inv_warehouse_sk_node_8, inv_quantity_on_hand AS inv_quantity_on_hand_node_8, 'hello' AS _c4])
                              +- SortLimit(orderBy=[inv_warehouse_sk ASC], offset=[0], fetch=[1], global=[true])
                                 +- Exchange(distribution=[single])
                                    +- SortLimit(orderBy=[inv_warehouse_sk ASC], offset=[0], fetch=[1], global=[false])
                                       +- TableSourceScan(table=[[default_catalog, default_database, inventory]], fields=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0