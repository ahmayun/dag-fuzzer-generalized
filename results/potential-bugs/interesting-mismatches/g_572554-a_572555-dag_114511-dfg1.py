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

autonode_10 = table_env.from_path("inventory").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("inventory").get_schema().get_field_names()])
autonode_9 = table_env.from_path("call_center").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("call_center").get_schema().get_field_names()])
autonode_11 = table_env.from_path("customer_address").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("customer_address").get_schema().get_field_names()])
autonode_7 = autonode_10.group_by(col('inv_quantity_on_hand_node_10')).select(col('inv_date_sk_node_10').min.alias('inv_date_sk_node_10'))
autonode_6 = autonode_9.limit(45)
autonode_8 = autonode_11.distinct()
autonode_4 = autonode_6.add_columns(lit("hello"))
autonode_5 = autonode_7.join(autonode_8, col('ca_address_sk_node_11') == col('inv_date_sk_node_10'))
autonode_2 = autonode_4.add_columns(lit("hello"))
autonode_3 = autonode_5.order_by(col('ca_state_node_11'))
autonode_1 = autonode_2.join(autonode_3, col('ca_gmt_offset_node_11') == col('cc_tax_percentage_node_9'))
sink = autonode_1.group_by(col('cc_rec_start_date_node_9')).select(col('cc_gmt_offset_node_9').min.alias('cc_gmt_offset_node_9'))
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
      "error_message": "An error occurred while calling o312298667.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#629194512:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[9](input=RelSubset#629194510,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[9]), rel#629194509:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[9](input=RelSubset#629194508,groupBy=ca_gmt_offset,select=ca_gmt_offset)]
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
Caused by: java.lang.IndexOutOfBoundsException: index (9) must be less than size (1)
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
LogicalProject(cc_gmt_offset_node_9=[$1])
+- LogicalAggregate(group=[{2}], EXPR$0=[MIN($29)])
   +- LogicalJoin(condition=[=($45, $30)], joinType=[inner])
      :- LogicalProject(cc_call_center_sk_node_9=[$0], cc_call_center_id_node_9=[$1], cc_rec_start_date_node_9=[$2], cc_rec_end_date_node_9=[$3], cc_closed_date_sk_node_9=[$4], cc_open_date_sk_node_9=[$5], cc_name_node_9=[$6], cc_class_node_9=[$7], cc_employees_node_9=[$8], cc_sq_ft_node_9=[$9], cc_hours_node_9=[$10], cc_manager_node_9=[$11], cc_mkt_id_node_9=[$12], cc_mkt_class_node_9=[$13], cc_mkt_desc_node_9=[$14], cc_market_manager_node_9=[$15], cc_division_node_9=[$16], cc_division_name_node_9=[$17], cc_company_node_9=[$18], cc_company_name_node_9=[$19], cc_street_number_node_9=[$20], cc_street_name_node_9=[$21], cc_street_type_node_9=[$22], cc_suite_number_node_9=[$23], cc_city_node_9=[$24], cc_county_node_9=[$25], cc_state_node_9=[$26], cc_zip_node_9=[$27], cc_country_node_9=[$28], cc_gmt_offset_node_9=[$29], cc_tax_percentage_node_9=[$30], _c31=[_UTF-16LE'hello'], _c32=[_UTF-16LE'hello'])
      :  +- LogicalSort(fetch=[45])
      :     +- LogicalProject(cc_call_center_sk_node_9=[$0], cc_call_center_id_node_9=[$1], cc_rec_start_date_node_9=[$2], cc_rec_end_date_node_9=[$3], cc_closed_date_sk_node_9=[$4], cc_open_date_sk_node_9=[$5], cc_name_node_9=[$6], cc_class_node_9=[$7], cc_employees_node_9=[$8], cc_sq_ft_node_9=[$9], cc_hours_node_9=[$10], cc_manager_node_9=[$11], cc_mkt_id_node_9=[$12], cc_mkt_class_node_9=[$13], cc_mkt_desc_node_9=[$14], cc_market_manager_node_9=[$15], cc_division_node_9=[$16], cc_division_name_node_9=[$17], cc_company_node_9=[$18], cc_company_name_node_9=[$19], cc_street_number_node_9=[$20], cc_street_name_node_9=[$21], cc_street_type_node_9=[$22], cc_suite_number_node_9=[$23], cc_city_node_9=[$24], cc_county_node_9=[$25], cc_state_node_9=[$26], cc_zip_node_9=[$27], cc_country_node_9=[$28], cc_gmt_offset_node_9=[$29], cc_tax_percentage_node_9=[$30])
      :        +- LogicalTableScan(table=[[default_catalog, default_database, call_center]])
      +- LogicalSort(sort0=[$9], dir0=[ASC])
         +- LogicalJoin(condition=[=($1, $0)], joinType=[inner])
            :- LogicalProject(inv_date_sk_node_10=[$1])
            :  +- LogicalAggregate(group=[{1}], EXPR$0=[MIN($0)])
            :     +- LogicalProject(inv_date_sk_node_10=[$0], inv_quantity_on_hand_node_10=[$3])
            :        +- LogicalTableScan(table=[[default_catalog, default_database, inventory]])
            +- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}])
               +- LogicalProject(ca_address_sk_node_11=[$0], ca_address_id_node_11=[$1], ca_street_number_node_11=[$2], ca_street_name_node_11=[$3], ca_street_type_node_11=[$4], ca_suite_number_node_11=[$5], ca_city_node_11=[$6], ca_county_node_11=[$7], ca_state_node_11=[$8], ca_zip_node_11=[$9], ca_country_node_11=[$10], ca_gmt_offset_node_11=[$11], ca_location_type_node_11=[$12])
                  +- LogicalTableScan(table=[[default_catalog, default_database, customer_address]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS cc_gmt_offset_node_9])
+- SortAggregate(isMerge=[true], groupBy=[cc_rec_start_date_node_9], select=[cc_rec_start_date_node_9, Final_MIN(min$0) AS EXPR$0])
   +- Sort(orderBy=[cc_rec_start_date_node_9 ASC])
      +- Exchange(distribution=[hash[cc_rec_start_date_node_9]])
         +- LocalSortAggregate(groupBy=[cc_rec_start_date_node_9], select=[cc_rec_start_date_node_9, Partial_MIN(cc_gmt_offset_node_9) AS min$0])
            +- Sort(orderBy=[cc_rec_start_date_node_9 ASC])
               +- NestedLoopJoin(joinType=[InnerJoin], where=[=(ca_gmt_offset, cc_tax_percentage_node_9)], select=[cc_call_center_sk_node_9, cc_call_center_id_node_9, cc_rec_start_date_node_9, cc_rec_end_date_node_9, cc_closed_date_sk_node_9, cc_open_date_sk_node_9, cc_name_node_9, cc_class_node_9, cc_employees_node_9, cc_sq_ft_node_9, cc_hours_node_9, cc_manager_node_9, cc_mkt_id_node_9, cc_mkt_class_node_9, cc_mkt_desc_node_9, cc_market_manager_node_9, cc_division_node_9, cc_division_name_node_9, cc_company_node_9, cc_company_name_node_9, cc_street_number_node_9, cc_street_name_node_9, cc_street_type_node_9, cc_suite_number_node_9, cc_city_node_9, cc_county_node_9, cc_state_node_9, cc_zip_node_9, cc_country_node_9, cc_gmt_offset_node_9, cc_tax_percentage_node_9, _c31, _c32, inv_date_sk_node_10, ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type], build=[right])
                  :- Calc(select=[cc_call_center_sk AS cc_call_center_sk_node_9, cc_call_center_id AS cc_call_center_id_node_9, cc_rec_start_date AS cc_rec_start_date_node_9, cc_rec_end_date AS cc_rec_end_date_node_9, cc_closed_date_sk AS cc_closed_date_sk_node_9, cc_open_date_sk AS cc_open_date_sk_node_9, cc_name AS cc_name_node_9, cc_class AS cc_class_node_9, cc_employees AS cc_employees_node_9, cc_sq_ft AS cc_sq_ft_node_9, cc_hours AS cc_hours_node_9, cc_manager AS cc_manager_node_9, cc_mkt_id AS cc_mkt_id_node_9, cc_mkt_class AS cc_mkt_class_node_9, cc_mkt_desc AS cc_mkt_desc_node_9, cc_market_manager AS cc_market_manager_node_9, cc_division AS cc_division_node_9, cc_division_name AS cc_division_name_node_9, cc_company AS cc_company_node_9, cc_company_name AS cc_company_name_node_9, cc_street_number AS cc_street_number_node_9, cc_street_name AS cc_street_name_node_9, cc_street_type AS cc_street_type_node_9, cc_suite_number AS cc_suite_number_node_9, cc_city AS cc_city_node_9, cc_county AS cc_county_node_9, cc_state AS cc_state_node_9, cc_zip AS cc_zip_node_9, cc_country AS cc_country_node_9, cc_gmt_offset AS cc_gmt_offset_node_9, cc_tax_percentage AS cc_tax_percentage_node_9, 'hello' AS _c31, 'hello' AS _c32])
                  :  +- Limit(offset=[0], fetch=[45], global=[true])
                  :     +- Exchange(distribution=[single])
                  :        +- Limit(offset=[0], fetch=[45], global=[false])
                  :           +- TableSourceScan(table=[[default_catalog, default_database, call_center]], fields=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage])
                  +- Exchange(distribution=[broadcast])
                     +- SortLimit(orderBy=[ca_state ASC], offset=[0], fetch=[1], global=[true])
                        +- Exchange(distribution=[single])
                           +- SortLimit(orderBy=[ca_state ASC], offset=[0], fetch=[1], global=[false])
                              +- HashJoin(joinType=[InnerJoin], where=[=(ca_address_sk, inv_date_sk_node_10)], select=[inv_date_sk_node_10, ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type], build=[right])
                                 :- Exchange(distribution=[hash[inv_date_sk_node_10]])
                                 :  +- Calc(select=[EXPR$0 AS inv_date_sk_node_10])
                                 :     +- HashAggregate(isMerge=[true], groupBy=[inv_quantity_on_hand], select=[inv_quantity_on_hand, Final_MIN(min$0) AS EXPR$0])
                                 :        +- Exchange(distribution=[hash[inv_quantity_on_hand]])
                                 :           +- LocalHashAggregate(groupBy=[inv_quantity_on_hand], select=[inv_quantity_on_hand, Partial_MIN(inv_date_sk) AS min$0])
                                 :              +- TableSourceScan(table=[[default_catalog, default_database, inventory, project=[inv_date_sk, inv_quantity_on_hand], metadata=[]]], fields=[inv_date_sk, inv_quantity_on_hand])
                                 +- Exchange(distribution=[hash[ca_address_sk]])
                                    +- HashAggregate(isMerge=[false], groupBy=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type], select=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])
                                       +- Exchange(distribution=[hash[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type]])
                                          +- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS cc_gmt_offset_node_9])
+- SortAggregate(isMerge=[true], groupBy=[cc_rec_start_date_node_9], select=[cc_rec_start_date_node_9, Final_MIN(min$0) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[cc_rec_start_date_node_9 ASC])
         +- Exchange(distribution=[hash[cc_rec_start_date_node_9]])
            +- LocalSortAggregate(groupBy=[cc_rec_start_date_node_9], select=[cc_rec_start_date_node_9, Partial_MIN(cc_gmt_offset_node_9) AS min$0])
               +- Exchange(distribution=[forward])
                  +- Sort(orderBy=[cc_rec_start_date_node_9 ASC])
                     +- NestedLoopJoin(joinType=[InnerJoin], where=[(ca_gmt_offset = cc_tax_percentage_node_9)], select=[cc_call_center_sk_node_9, cc_call_center_id_node_9, cc_rec_start_date_node_9, cc_rec_end_date_node_9, cc_closed_date_sk_node_9, cc_open_date_sk_node_9, cc_name_node_9, cc_class_node_9, cc_employees_node_9, cc_sq_ft_node_9, cc_hours_node_9, cc_manager_node_9, cc_mkt_id_node_9, cc_mkt_class_node_9, cc_mkt_desc_node_9, cc_market_manager_node_9, cc_division_node_9, cc_division_name_node_9, cc_company_node_9, cc_company_name_node_9, cc_street_number_node_9, cc_street_name_node_9, cc_street_type_node_9, cc_suite_number_node_9, cc_city_node_9, cc_county_node_9, cc_state_node_9, cc_zip_node_9, cc_country_node_9, cc_gmt_offset_node_9, cc_tax_percentage_node_9, _c31, _c32, inv_date_sk_node_10, ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type], build=[right])
                        :- Calc(select=[cc_call_center_sk AS cc_call_center_sk_node_9, cc_call_center_id AS cc_call_center_id_node_9, cc_rec_start_date AS cc_rec_start_date_node_9, cc_rec_end_date AS cc_rec_end_date_node_9, cc_closed_date_sk AS cc_closed_date_sk_node_9, cc_open_date_sk AS cc_open_date_sk_node_9, cc_name AS cc_name_node_9, cc_class AS cc_class_node_9, cc_employees AS cc_employees_node_9, cc_sq_ft AS cc_sq_ft_node_9, cc_hours AS cc_hours_node_9, cc_manager AS cc_manager_node_9, cc_mkt_id AS cc_mkt_id_node_9, cc_mkt_class AS cc_mkt_class_node_9, cc_mkt_desc AS cc_mkt_desc_node_9, cc_market_manager AS cc_market_manager_node_9, cc_division AS cc_division_node_9, cc_division_name AS cc_division_name_node_9, cc_company AS cc_company_node_9, cc_company_name AS cc_company_name_node_9, cc_street_number AS cc_street_number_node_9, cc_street_name AS cc_street_name_node_9, cc_street_type AS cc_street_type_node_9, cc_suite_number AS cc_suite_number_node_9, cc_city AS cc_city_node_9, cc_county AS cc_county_node_9, cc_state AS cc_state_node_9, cc_zip AS cc_zip_node_9, cc_country AS cc_country_node_9, cc_gmt_offset AS cc_gmt_offset_node_9, cc_tax_percentage AS cc_tax_percentage_node_9, 'hello' AS _c31, 'hello' AS _c32])
                        :  +- Limit(offset=[0], fetch=[45], global=[true])
                        :     +- Exchange(distribution=[single])
                        :        +- Limit(offset=[0], fetch=[45], global=[false])
                        :           +- TableSourceScan(table=[[default_catalog, default_database, call_center]], fields=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage])
                        +- Exchange(distribution=[broadcast])
                           +- SortLimit(orderBy=[ca_state ASC], offset=[0], fetch=[1], global=[true])
                              +- Exchange(distribution=[single])
                                 +- SortLimit(orderBy=[ca_state ASC], offset=[0], fetch=[1], global=[false])
                                    +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(ca_address_sk = inv_date_sk_node_10)], select=[inv_date_sk_node_10, ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type], build=[right])
                                       :- Exchange(distribution=[hash[inv_date_sk_node_10]])
                                       :  +- Calc(select=[EXPR$0 AS inv_date_sk_node_10])
                                       :     +- HashAggregate(isMerge=[true], groupBy=[inv_quantity_on_hand], select=[inv_quantity_on_hand, Final_MIN(min$0) AS EXPR$0])
                                       :        +- Exchange(distribution=[hash[inv_quantity_on_hand]])
                                       :           +- LocalHashAggregate(groupBy=[inv_quantity_on_hand], select=[inv_quantity_on_hand, Partial_MIN(inv_date_sk) AS min$0])
                                       :              +- TableSourceScan(table=[[default_catalog, default_database, inventory, project=[inv_date_sk, inv_quantity_on_hand], metadata=[]]], fields=[inv_date_sk, inv_quantity_on_hand])
                                       +- Exchange(distribution=[hash[ca_address_sk]])
                                          +- HashAggregate(isMerge=[false], groupBy=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type], select=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])
                                             +- Exchange(distribution=[hash[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type]])
                                                +- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0