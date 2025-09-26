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
    return values.min()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_6 = table_env.from_path("catalog_page").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("catalog_page").get_schema().get_field_names()])
autonode_8 = table_env.from_path("catalog_page").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("catalog_page").get_schema().get_field_names()])
autonode_7 = table_env.from_path("income_band").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("income_band").get_schema().get_field_names()])
autonode_5 = autonode_8.add_columns(lit("hello"))
autonode_4 = autonode_6.join(autonode_7, col('ib_income_band_sk_node_7') == col('cp_catalog_page_number_node_6'))
autonode_3 = autonode_5.order_by(col('cp_description_node_8'))
autonode_2 = autonode_4.group_by(col('cp_catalog_page_id_node_6')).select(col('ib_upper_bound_node_7').min.alias('ib_upper_bound_node_7'))
autonode_1 = autonode_2.join(autonode_3, col('ib_upper_bound_node_7') == col('cp_end_date_sk_node_8'))
sink = autonode_1.alias('PB6O4')
print(sink.explain())

# ======== Details ========
"""
{
  "is_same": false,
  "result_name": "MismatchException",
  "result_details": {
    "opt": {
      "success": true,
      "error_name": "",
      "error_message": "",
      "stdout": "== Abstract Syntax Tree ==
LogicalProject(PB6O4=[AS($0, _UTF-16LE'PB6O4')], cp_catalog_page_sk_node_8=[$1], cp_catalog_page_id_node_8=[$2], cp_start_date_sk_node_8=[$3], cp_end_date_sk_node_8=[$4], cp_department_node_8=[$5], cp_catalog_number_node_8=[$6], cp_catalog_page_number_node_8=[$7], cp_description_node_8=[$8], cp_type_node_8=[$9], _c9=[$10])
+- LogicalJoin(condition=[=($0, $4)], joinType=[inner])
   :- LogicalProject(ib_upper_bound_node_7=[$1])
   :  +- LogicalAggregate(group=[{1}], EXPR$0=[MIN($11)])
   :     +- LogicalJoin(condition=[=($9, $6)], joinType=[inner])
   :        :- LogicalProject(cp_catalog_page_sk_node_6=[$0], cp_catalog_page_id_node_6=[$1], cp_start_date_sk_node_6=[$2], cp_end_date_sk_node_6=[$3], cp_department_node_6=[$4], cp_catalog_number_node_6=[$5], cp_catalog_page_number_node_6=[$6], cp_description_node_6=[$7], cp_type_node_6=[$8])
   :        :  +- LogicalTableScan(table=[[default_catalog, default_database, catalog_page]])
   :        +- LogicalProject(ib_income_band_sk_node_7=[$0], ib_lower_bound_node_7=[$1], ib_upper_bound_node_7=[$2])
   :           +- LogicalTableScan(table=[[default_catalog, default_database, income_band]])
   +- LogicalSort(sort0=[$7], dir0=[ASC])
      +- LogicalProject(cp_catalog_page_sk_node_8=[$0], cp_catalog_page_id_node_8=[$1], cp_start_date_sk_node_8=[$2], cp_end_date_sk_node_8=[$3], cp_department_node_8=[$4], cp_catalog_number_node_8=[$5], cp_catalog_page_number_node_8=[$6], cp_description_node_8=[$7], cp_type_node_8=[$8], _c9=[_UTF-16LE'hello'])
         +- LogicalTableScan(table=[[default_catalog, default_database, catalog_page]])

== Optimized Physical Plan ==
Calc(select=[ib_upper_bound_node_7 AS PB6O4, cp_catalog_page_sk AS cp_catalog_page_sk_node_8, cp_catalog_page_id AS cp_catalog_page_id_node_8, cp_start_date_sk AS cp_start_date_sk_node_8, cp_end_date_sk AS cp_end_date_sk_node_8, cp_department AS cp_department_node_8, cp_catalog_number AS cp_catalog_number_node_8, cp_catalog_page_number AS cp_catalog_page_number_node_8, cp_description AS cp_description_node_8, cp_type AS cp_type_node_8, 'hello' AS _c9])
+- HashJoin(joinType=[InnerJoin], where=[=(ib_upper_bound_node_7, cp_end_date_sk)], select=[ib_upper_bound_node_7, cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type], isBroadcast=[true], build=[left])
   :- Exchange(distribution=[broadcast])
   :  +- Calc(select=[EXPR$0 AS ib_upper_bound_node_7])
   :     +- HashAggregate(isMerge=[true], groupBy=[cp_catalog_page_id], select=[cp_catalog_page_id, Final_MIN(min$0) AS EXPR$0])
   :        +- Exchange(distribution=[hash[cp_catalog_page_id]])
   :           +- LocalHashAggregate(groupBy=[cp_catalog_page_id], select=[cp_catalog_page_id, Partial_MIN(ib_upper_bound) AS min$0])
   :              +- HashJoin(joinType=[InnerJoin], where=[=(ib_income_band_sk, cp_catalog_page_number)], select=[cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type, ib_income_band_sk, ib_lower_bound, ib_upper_bound], isBroadcast=[true], build=[right])
   :                 :- TableSourceScan(table=[[default_catalog, default_database, catalog_page]], fields=[cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type])
   :                 +- Exchange(distribution=[broadcast])
   :                    +- TableSourceScan(table=[[default_catalog, default_database, income_band]], fields=[ib_income_band_sk, ib_lower_bound, ib_upper_bound])
   +- Sort(orderBy=[cp_description ASC])
      +- Exchange(distribution=[single])
         +- TableSourceScan(table=[[default_catalog, default_database, catalog_page]], fields=[cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type])

== Optimized Execution Plan ==
Calc(select=[ib_upper_bound_node_7 AS PB6O4, cp_catalog_page_sk AS cp_catalog_page_sk_node_8, cp_catalog_page_id AS cp_catalog_page_id_node_8, cp_start_date_sk AS cp_start_date_sk_node_8, cp_end_date_sk AS cp_end_date_sk_node_8, cp_department AS cp_department_node_8, cp_catalog_number AS cp_catalog_number_node_8, cp_catalog_page_number AS cp_catalog_page_number_node_8, cp_description AS cp_description_node_8, cp_type AS cp_type_node_8, 'hello' AS _c9])
+- HashJoin(joinType=[InnerJoin], where=[(ib_upper_bound_node_7 = cp_end_date_sk)], select=[ib_upper_bound_node_7, cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type], isBroadcast=[true], build=[left])
   :- Exchange(distribution=[broadcast])
   :  +- Calc(select=[EXPR$0 AS ib_upper_bound_node_7])
   :     +- HashAggregate(isMerge=[true], groupBy=[cp_catalog_page_id], select=[cp_catalog_page_id, Final_MIN(min$0) AS EXPR$0])
   :        +- Exchange(distribution=[hash[cp_catalog_page_id]])
   :           +- LocalHashAggregate(groupBy=[cp_catalog_page_id], select=[cp_catalog_page_id, Partial_MIN(ib_upper_bound) AS min$0])
   :              +- MultipleInput(readOrder=[1,0], members=[\
HashJoin(joinType=[InnerJoin], where=[(ib_income_band_sk = cp_catalog_page_number)], select=[cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type, ib_income_band_sk, ib_lower_bound, ib_upper_bound], isBroadcast=[true], build=[right])\
:- [#1] TableSourceScan(table=[[default_catalog, default_database, catalog_page]], fields=[cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type])\
+- [#2] Exchange(distribution=[broadcast])\
])
   :                 :- TableSourceScan(table=[[default_catalog, default_database, catalog_page]], fields=[cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type])(reuse_id=[1])
   :                 +- Exchange(distribution=[broadcast])
   :                    +- TableSourceScan(table=[[default_catalog, default_database, income_band]], fields=[ib_income_band_sk, ib_lower_bound, ib_upper_bound])
   +- Sort(orderBy=[cp_description ASC])
      +- Exchange(distribution=[single])
         +- Reused(reference_id=[1])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o47500372.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#94521229:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[7](input=RelSubset#94521227,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[7]), rel#94521226:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[7](input=RelSubset#94521225,groupBy=cp_catalog_page_id, cp_catalog_page_number,select=cp_catalog_page_id, cp_catalog_page_number)]
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
    }
  }
}
"""



//Optimizer Branch Coverage: 0