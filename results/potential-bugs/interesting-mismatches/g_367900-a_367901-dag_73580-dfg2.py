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
    return values.sum()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_6 = table_env.from_path("store_returns").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("store_returns").get_schema().get_field_names()])
autonode_5 = autonode_6.select(col('sr_returned_date_sk_node_6'))
autonode_4 = autonode_5.distinct()
autonode_3 = autonode_4.order_by(col('sr_returned_date_sk_node_6'))
autonode_2 = autonode_3.add_columns(lit("hello"))
autonode_1 = autonode_2.group_by(col('sr_returned_date_sk_node_6')).select(col('sr_returned_date_sk_node_6').max.alias('sr_returned_date_sk_node_6'))
sink = autonode_1.limit(83)
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
LogicalSort(fetch=[83])
+- LogicalProject(sr_returned_date_sk_node_6=[$1])
   +- LogicalAggregate(group=[{0}], EXPR$0=[MAX($0)])
      +- LogicalProject(sr_returned_date_sk_node_6=[$0])
         +- LogicalSort(sort0=[$0], dir0=[ASC])
            +- LogicalAggregate(group=[{0}])
               +- LogicalProject(sr_returned_date_sk_node_6=[$0])
                  +- LogicalTableScan(table=[[default_catalog, default_database, store_returns]])

== Optimized Physical Plan ==
Limit(offset=[0], fetch=[83], global=[true])
+- Exchange(distribution=[single])
   +- Limit(offset=[0], fetch=[83], global=[false])
      +- HashAggregate(isMerge=[true], groupBy=[sr_returned_date_sk], select=[sr_returned_date_sk])
         +- Exchange(distribution=[hash[sr_returned_date_sk]])
            +- LocalHashAggregate(groupBy=[sr_returned_date_sk], select=[sr_returned_date_sk])
               +- TableSourceScan(table=[[default_catalog, default_database, store_returns, project=[sr_returned_date_sk], metadata=[]]], fields=[sr_returned_date_sk])

== Optimized Execution Plan ==
Limit(offset=[0], fetch=[83], global=[true])
+- Exchange(distribution=[single])
   +- Limit(offset=[0], fetch=[83], global=[false])
      +- HashAggregate(isMerge=[true], groupBy=[sr_returned_date_sk], select=[sr_returned_date_sk])
         +- Exchange(distribution=[hash[sr_returned_date_sk]])
            +- LocalHashAggregate(groupBy=[sr_returned_date_sk], select=[sr_returned_date_sk])
               +- TableSourceScan(table=[[default_catalog, default_database, store_returns, project=[sr_returned_date_sk], metadata=[]]], fields=[sr_returned_date_sk])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o200172400.explain.
: java.lang.RuntimeException: Error while applying rule SortProjectTransposeRule, args [rel#404986646:LogicalSort.NONE.any.[](input=RelSubset#404986628,fetch=83), rel#404986642:LogicalProject.NONE.any.[1](input=RelSubset#404986600,inputs=0,exprs=[$0])]
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
Caused by: java.lang.RuntimeException: Error occurred while applying rule SortProjectTransposeRule
\tat org.apache.calcite.plan.volcano.VolcanoRuleCall.transformTo(VolcanoRuleCall.java:157)
\tat org.apache.calcite.plan.RelOptRuleCall.transformTo(RelOptRuleCall.java:273)
\tat org.apache.calcite.rel.rules.SortProjectTransposeRule.onMatch(SortProjectTransposeRule.java:169)
\tat org.apache.calcite.plan.volcano.VolcanoRuleCall.onMatch(VolcanoRuleCall.java:223)
\t... 40 more
Caused by: org.apache.calcite.rel.metadata.CyclicMetadataException
\tat org.apache.calcite.rel.metadata.janino.GeneratedMetadata_RowCountHandler.getRowCount(Unknown Source)
\tat org.apache.calcite.rel.metadata.RelMetadataQuery.getRowCount(RelMetadataQuery.java:258)
\tat org.apache.calcite.rel.metadata.RelMdUtil.estimateFilteredRows(RelMdUtil.java:863)
\tat org.apache.calcite.rel.metadata.RelMdUtil.estimateFilteredRows(RelMdUtil.java:856)
\tat org.apache.flink.table.planner.plan.metadata.FlinkRelMdRowCount.getRowCount(FlinkRelMdRowCount.scala:58)
\tat org.apache.calcite.rel.metadata.janino.GeneratedMetadata_RowCountHandler.getRowCount_$(Unknown Source)
\tat org.apache.calcite.rel.metadata.janino.GeneratedMetadata_RowCountHandler.getRowCount(Unknown Source)
\tat org.apache.calcite.rel.metadata.RelMetadataQuery.getRowCount(RelMetadataQuery.java:258)
\tat org.apache.flink.table.planner.plan.metadata.FlinkRelMdRowCount.getRowCount(FlinkRelMdRowCount.scala:411)
\tat org.apache.calcite.rel.metadata.janino.GeneratedMetadata_RowCountHandler.getRowCount_$(Unknown Source)
\tat org.apache.calcite.rel.metadata.janino.GeneratedMetadata_RowCountHandler.getRowCount(Unknown Source)
\tat org.apache.calcite.rel.metadata.RelMetadataQuery.getRowCount(RelMetadataQuery.java:258)
\tat org.apache.calcite.rel.metadata.RelMdUtil.estimateFilteredRows(RelMdUtil.java:863)
\tat org.apache.calcite.rel.metadata.RelMdUtil.estimateFilteredRows(RelMdUtil.java:856)
\tat org.apache.flink.table.planner.plan.metadata.FlinkRelMdRowCount.getRowCount(FlinkRelMdRowCount.scala:58)
\tat org.apache.calcite.rel.metadata.janino.GeneratedMetadata_RowCountHandler.getRowCount_$(Unknown Source)
\tat org.apache.calcite.rel.metadata.janino.GeneratedMetadata_RowCountHandler.getRowCount(Unknown Source)
\tat org.apache.calcite.rel.metadata.RelMetadataQuery.getRowCount(RelMetadataQuery.java:258)
\tat org.apache.flink.table.planner.plan.nodes.common.CommonCalc.computeSelfCost(CommonCalc.scala:58)
\tat org.apache.flink.table.planner.plan.metadata.FlinkRelMdNonCumulativeCost.getNonCumulativeCost(FlinkRelMdNonCumulativeCost.scala:40)
\tat org.apache.calcite.rel.metadata.janino.GeneratedMetadata_NonCumulativeCostHandler.getNonCumulativeCost_$(Unknown Source)
\tat org.apache.calcite.rel.metadata.janino.GeneratedMetadata_NonCumulativeCostHandler.getNonCumulativeCost(Unknown Source)
\tat org.apache.calcite.rel.metadata.RelMetadataQuery.getNonCumulativeCost(RelMetadataQuery.java:331)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.getCost(VolcanoPlanner.java:727)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.getCostOrInfinite(VolcanoPlanner.java:714)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.propagateCostImprovements(VolcanoPlanner.java:971)
\tat org.apache.calcite.plan.volcano.RelSet.mergeWith(RelSet.java:447)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.merge(VolcanoPlanner.java:1167)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.registerSubset(VolcanoPlanner.java:1427)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.registerImpl(VolcanoPlanner.java:1308)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.register(VolcanoPlanner.java:598)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.ensureRegistered(VolcanoPlanner.java:613)
\tat org.apache.calcite.plan.volcano.VolcanoRuleCall.transformTo(VolcanoRuleCall.java:144)
\t... 43 more
",
      "stdout": "",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0