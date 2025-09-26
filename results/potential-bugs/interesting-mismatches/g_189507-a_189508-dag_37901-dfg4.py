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
    return values.var()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_10 = table_env.from_path("income_band").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("income_band").get_schema().get_field_names()])
autonode_12 = table_env.from_path("inventory").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("inventory").get_schema().get_field_names()])
autonode_11 = table_env.from_path("warehouse").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("warehouse").get_schema().get_field_names()])
autonode_7 = autonode_10.distinct()
autonode_9 = autonode_12.alias('a3EMR')
autonode_8 = autonode_11.alias('26v3u')
autonode_6 = autonode_9.add_columns(lit("hello"))
autonode_5 = autonode_7.join(autonode_8, col('w_warehouse_sq_ft_node_11') == col('ib_income_band_sk_node_10'))
autonode_4 = autonode_6.order_by(col('inv_warehouse_sk_node_12'))
autonode_3 = autonode_5.filter(col('w_warehouse_name_node_11').char_length >= 5)
autonode_2 = autonode_3.join(autonode_4, col('inv_warehouse_sk_node_12') == col('ib_upper_bound_node_10'))
autonode_1 = autonode_2.group_by(col('w_suite_number_node_11')).select(col('inv_item_sk_node_12').sum.alias('inv_item_sk_node_12'))
sink = autonode_1.alias('C6FAr')
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
      "error_message": "An error occurred while calling o103285581.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#208575099:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[2](input=RelSubset#208575097,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[2]), rel#208575096:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[2](input=RelSubset#208575095,groupBy=inv_warehouse_sk,select=inv_warehouse_sk, Partial_SUM(inv_item_sk) AS sum$0)]
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
Caused by: java.lang.ArrayIndexOutOfBoundsException
",
      "stdout": "",
      "stderr": ""
    },
    "unopt": {
      "success": true,
      "error_name": "",
      "error_message": "",
      "stdout": "== Abstract Syntax Tree ==
LogicalProject(C6FAr=[AS($1, _UTF-16LE'C6FAr')])
+- LogicalAggregate(group=[{10}], EXPR$0=[SUM($18)])
   +- LogicalJoin(condition=[=($19, $2)], joinType=[inner])
      :- LogicalFilter(condition=[>=(CHAR_LENGTH($5), 5)])
      :  +- LogicalJoin(condition=[=($6, $0)], joinType=[inner])
      :     :- LogicalAggregate(group=[{0, 1, 2}])
      :     :  +- LogicalProject(ib_income_band_sk_node_10=[$0], ib_lower_bound_node_10=[$1], ib_upper_bound_node_10=[$2])
      :     :     +- LogicalTableScan(table=[[default_catalog, default_database, income_band]])
      :     +- LogicalProject(26v3u=[AS($0, _UTF-16LE'26v3u')], w_warehouse_id_node_11=[$1], w_warehouse_name_node_11=[$2], w_warehouse_sq_ft_node_11=[$3], w_street_number_node_11=[$4], w_street_name_node_11=[$5], w_street_type_node_11=[$6], w_suite_number_node_11=[$7], w_city_node_11=[$8], w_county_node_11=[$9], w_state_node_11=[$10], w_zip_node_11=[$11], w_country_node_11=[$12], w_gmt_offset_node_11=[$13])
      :        +- LogicalTableScan(table=[[default_catalog, default_database, warehouse]])
      +- LogicalSort(sort0=[$2], dir0=[ASC])
         +- LogicalProject(a3EMR=[AS($0, _UTF-16LE'a3EMR')], inv_item_sk_node_12=[$1], inv_warehouse_sk_node_12=[$2], inv_quantity_on_hand_node_12=[$3], _c4=[_UTF-16LE'hello'])
            +- LogicalTableScan(table=[[default_catalog, default_database, inventory]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS C6FAr])
+- SortAggregate(isMerge=[false], groupBy=[w_suite_number_node_11], select=[w_suite_number_node_11, SUM(inv_item_sk_node_12) AS EXPR$0])
   +- Sort(orderBy=[w_suite_number_node_11 ASC])
      +- Exchange(distribution=[hash[w_suite_number_node_11]])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[=(inv_warehouse_sk_node_12, ib_upper_bound)], select=[ib_income_band_sk, ib_lower_bound, ib_upper_bound, 26v3u, w_warehouse_id_node_11, w_warehouse_name_node_11, w_warehouse_sq_ft_node_11, w_street_number_node_11, w_street_name_node_11, w_street_type_node_11, w_suite_number_node_11, w_city_node_11, w_county_node_11, w_state_node_11, w_zip_node_11, w_country_node_11, w_gmt_offset_node_11, a3EMR, inv_item_sk_node_12, inv_warehouse_sk_node_12, inv_quantity_on_hand_node_12, _c4], build=[right])
            :- NestedLoopJoin(joinType=[InnerJoin], where=[=(w_warehouse_sq_ft_node_11, ib_income_band_sk)], select=[ib_income_band_sk, ib_lower_bound, ib_upper_bound, 26v3u, w_warehouse_id_node_11, w_warehouse_name_node_11, w_warehouse_sq_ft_node_11, w_street_number_node_11, w_street_name_node_11, w_street_type_node_11, w_suite_number_node_11, w_city_node_11, w_county_node_11, w_state_node_11, w_zip_node_11, w_country_node_11, w_gmt_offset_node_11], build=[left])
            :  :- Exchange(distribution=[broadcast])
            :  :  +- HashAggregate(isMerge=[true], groupBy=[ib_income_band_sk, ib_lower_bound, ib_upper_bound], select=[ib_income_band_sk, ib_lower_bound, ib_upper_bound])
            :  :     +- Exchange(distribution=[hash[ib_income_band_sk, ib_lower_bound, ib_upper_bound]])
            :  :        +- LocalHashAggregate(groupBy=[ib_income_band_sk, ib_lower_bound, ib_upper_bound], select=[ib_income_band_sk, ib_lower_bound, ib_upper_bound])
            :  :           +- TableSourceScan(table=[[default_catalog, default_database, income_band]], fields=[ib_income_band_sk, ib_lower_bound, ib_upper_bound])
            :  +- Calc(select=[w_warehouse_sk AS 26v3u, w_warehouse_id AS w_warehouse_id_node_11, w_warehouse_name AS w_warehouse_name_node_11, w_warehouse_sq_ft AS w_warehouse_sq_ft_node_11, w_street_number AS w_street_number_node_11, w_street_name AS w_street_name_node_11, w_street_type AS w_street_type_node_11, w_suite_number AS w_suite_number_node_11, w_city AS w_city_node_11, w_county AS w_county_node_11, w_state AS w_state_node_11, w_zip AS w_zip_node_11, w_country AS w_country_node_11, w_gmt_offset AS w_gmt_offset_node_11], where=[>=(CHAR_LENGTH(w_warehouse_name), 5)])
            :     +- TableSourceScan(table=[[default_catalog, default_database, warehouse, filter=[>=(CHAR_LENGTH(w_warehouse_name), 5)]]], fields=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset])
            +- Exchange(distribution=[broadcast])
               +- Calc(select=[inv_date_sk AS a3EMR, inv_item_sk AS inv_item_sk_node_12, inv_warehouse_sk AS inv_warehouse_sk_node_12, inv_quantity_on_hand AS inv_quantity_on_hand_node_12, 'hello' AS _c4])
                  +- SortLimit(orderBy=[inv_warehouse_sk ASC], offset=[0], fetch=[1], global=[true])
                     +- Exchange(distribution=[single])
                        +- SortLimit(orderBy=[inv_warehouse_sk ASC], offset=[0], fetch=[1], global=[false])
                           +- TableSourceScan(table=[[default_catalog, default_database, inventory]], fields=[inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS C6FAr])
+- SortAggregate(isMerge=[false], groupBy=[w_suite_number_node_11], select=[w_suite_number_node_11, SUM(inv_item_sk_node_12) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[w_suite_number_node_11 ASC])
         +- Exchange(distribution=[hash[w_suite_number_node_11]])
            +- NestedLoopJoin(joinType=[InnerJoin], where=[(inv_warehouse_sk_node_12 = ib_upper_bound)], select=[ib_income_band_sk, ib_lower_bound, ib_upper_bound, 26v3u, w_warehouse_id_node_11, w_warehouse_name_node_11, w_warehouse_sq_ft_node_11, w_street_number_node_11, w_street_name_node_11, w_street_type_node_11, w_suite_number_node_11, w_city_node_11, w_county_node_11, w_state_node_11, w_zip_node_11, w_country_node_11, w_gmt_offset_node_11, a3EMR, inv_item_sk_node_12, inv_warehouse_sk_node_12, inv_quantity_on_hand_node_12, _c4], build=[right])
               :- NestedLoopJoin(joinType=[InnerJoin], where=[(w_warehouse_sq_ft_node_11 = ib_income_band_sk)], select=[ib_income_band_sk, ib_lower_bound, ib_upper_bound, 26v3u, w_warehouse_id_node_11, w_warehouse_name_node_11, w_warehouse_sq_ft_node_11, w_street_number_node_11, w_street_name_node_11, w_street_type_node_11, w_suite_number_node_11, w_city_node_11, w_county_node_11, w_state_node_11, w_zip_node_11, w_country_node_11, w_gmt_offset_node_11], build=[left])
               :  :- Exchange(distribution=[broadcast])
               :  :  +- HashAggregate(isMerge=[true], groupBy=[ib_income_band_sk, ib_lower_bound, ib_upper_bound], select=[ib_income_band_sk, ib_lower_bound, ib_upper_bound])
               :  :     +- Exchange(distribution=[hash[ib_income_band_sk, ib_lower_bound, ib_upper_bound]])
               :  :        +- LocalHashAggregate(groupBy=[ib_income_band_sk, ib_lower_bound, ib_upper_bound], select=[ib_income_band_sk, ib_lower_bound, ib_upper_bound])
               :  :           +- TableSourceScan(table=[[default_catalog, default_database, income_band]], fields=[ib_income_band_sk, ib_lower_bound, ib_upper_bound])
               :  +- Calc(select=[w_warehouse_sk AS 26v3u, w_warehouse_id AS w_warehouse_id_node_11, w_warehouse_name AS w_warehouse_name_node_11, w_warehouse_sq_ft AS w_warehouse_sq_ft_node_11, w_street_number AS w_street_number_node_11, w_street_name AS w_street_name_node_11, w_street_type AS w_street_type_node_11, w_suite_number AS w_suite_number_node_11, w_city AS w_city_node_11, w_county AS w_county_node_11, w_state AS w_state_node_11, w_zip AS w_zip_node_11, w_country AS w_country_node_11, w_gmt_offset AS w_gmt_offset_node_11], where=[(CHAR_LENGTH(w_warehouse_name) >= 5)])
               :     +- TableSourceScan(table=[[default_catalog, default_database, warehouse, filter=[>=(CHAR_LENGTH(w_warehouse_name), 5)]]], fields=[w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset])
               +- Exchange(distribution=[broadcast])
                  +- Calc(select=[inv_date_sk AS a3EMR, inv_item_sk AS inv_item_sk_node_12, inv_warehouse_sk AS inv_warehouse_sk_node_12, inv_quantity_on_hand AS inv_quantity_on_hand_node_12, 'hello' AS _c4])
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