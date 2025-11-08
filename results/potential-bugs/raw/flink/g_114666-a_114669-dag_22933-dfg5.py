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



def filter_udf_string(arg):

    return arg.isalpha() and len(arg) < 10





@udf(result_type=DataTypes.BOOLEAN())



def filter_udf_integer(arg):

    return arg > 0 and arg % 2 == 0





@udf(result_type=DataTypes.BOOLEAN())



def filter_udf_decimal(arg):

    return arg * 100 % 25 == 0







def preloaded_aggregation(values: pd.Series) -> float:

    return values.kurtosis()





try:

    table_env.drop_temporary_function("preloaded_udf_agg")

except:

    pass



preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")



table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)



autonode_7 = table_env.from_path("web_returns").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("web_returns").get_schema().get_field_names()])

autonode_6 = autonode_7.filter(col('wr_returning_hdemo_sk_node_7') <= 16)

autonode_5 = autonode_6.select(col('wr_reversed_charge_node_7'))

autonode_4 = autonode_5.distinct()

autonode_3 = autonode_4.order_by(col('wr_reversed_charge_node_7'))

autonode_2 = autonode_3.group_by(col('wr_reversed_charge_node_7')).select(col('wr_reversed_charge_node_7').max.alias('wr_reversed_charge_node_7'))

autonode_1 = autonode_2.limit(57)

sink = autonode_1.group_by(col('wr_reversed_charge_node_7')).select(col('wr_reversed_charge_node_7').sum.alias('wr_reversed_charge_node_7'))

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
LogicalProject(wr_reversed_charge_node_7=[$1])
+- LogicalAggregate(group=[{0}], EXPR$0=[SUM($0)])
   +- LogicalSort(fetch=[57])
      +- LogicalProject(wr_reversed_charge_node_7=[$1])
         +- LogicalAggregate(group=[{0}], EXPR$0=[MAX($0)])
            +- LogicalSort(sort0=[$0], dir0=[ASC])
               +- LogicalAggregate(group=[{0}])
                  +- LogicalProject(wr_reversed_charge_node_7=[$21])
                     +- LogicalFilter(condition=[<=($9, 16)])
                        +- LogicalProject(wr_returned_date_sk_node_7=[$0], wr_returned_time_sk_node_7=[$1], wr_item_sk_node_7=[$2], wr_refunded_customer_sk_node_7=[$3], wr_refunded_cdemo_sk_node_7=[$4], wr_refunded_hdemo_sk_node_7=[$5], wr_refunded_addr_sk_node_7=[$6], wr_returning_customer_sk_node_7=[$7], wr_returning_cdemo_sk_node_7=[$8], wr_returning_hdemo_sk_node_7=[$9], wr_returning_addr_sk_node_7=[$10], wr_web_page_sk_node_7=[$11], wr_reason_sk_node_7=[$12], wr_order_number_node_7=[$13], wr_return_quantity_node_7=[$14], wr_return_amt_node_7=[$15], wr_return_tax_node_7=[$16], wr_return_amt_inc_tax_node_7=[$17], wr_fee_node_7=[$18], wr_return_ship_cost_node_7=[$19], wr_refunded_cash_node_7=[$20], wr_reversed_charge_node_7=[$21], wr_account_credit_node_7=[$22], wr_net_loss_node_7=[$23])
                           +- LogicalTableScan(table=[[default_catalog, default_database, web_returns]])
== Optimized Physical Plan ==
Calc(select=[CAST(wr_reversed_charge_node_7 AS DECIMAL(38, 2)) AS wr_reversed_charge_node_7])
+- Limit(offset=[0], fetch=[57], global=[true])
   +- Exchange(distribution=[single])
      +- Limit(offset=[0], fetch=[57], global=[false])
         +- HashAggregate(isMerge=[true], groupBy=[wr_reversed_charge_node_7], select=[wr_reversed_charge_node_7])
            +- Exchange(distribution=[hash[wr_reversed_charge_node_7]])
               +- LocalHashAggregate(groupBy=[wr_reversed_charge_node_7], select=[wr_reversed_charge_node_7])
                  +- Calc(select=[wr_reversed_charge AS wr_reversed_charge_node_7], where=[<=(wr_returning_hdemo_sk, 16)])
                     +- TableSourceScan(table=[[default_catalog, default_database, web_returns, filter=[<=(wr_returning_hdemo_sk, 16)], project=[wr_returning_hdemo_sk, wr_reversed_charge], metadata=[]]], fields=[wr_returning_hdemo_sk, wr_reversed_charge])
== Optimized Execution Plan ==
Calc(select=[CAST(wr_reversed_charge_node_7 AS DECIMAL(38, 2)) AS wr_reversed_charge_node_7])
+- Limit(offset=[0], fetch=[57], global=[true])
   +- Exchange(distribution=[single])
      +- Limit(offset=[0], fetch=[57], global=[false])
         +- HashAggregate(isMerge=[true], groupBy=[wr_reversed_charge_node_7], select=[wr_reversed_charge_node_7])
            +- Exchange(distribution=[hash[wr_reversed_charge_node_7]])
               +- LocalHashAggregate(groupBy=[wr_reversed_charge_node_7], select=[wr_reversed_charge_node_7])
                  +- Calc(select=[wr_reversed_charge AS wr_reversed_charge_node_7], where=[(wr_returning_hdemo_sk <= 16)])
                     +- TableSourceScan(table=[[default_catalog, default_database, web_returns, filter=[<=(wr_returning_hdemo_sk, 16)], project=[wr_returning_hdemo_sk, wr_reversed_charge], metadata=[]]], fields=[wr_returning_hdemo_sk, wr_reversed_charge])
",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o62343710.explain.
: java.lang.RuntimeException: Error while applying rule SortProjectTransposeRule, args [rel#125123934:LogicalSort.NONE.any.[](input=RelSubset#125123896,fetch=57), rel#125123930:LogicalProject.NONE.any.[1](input=RelSubset#125123814,inputs=0,exprs=[$0])]
\tat org.apache.calcite.plan.volcano.VolcanoRuleCall.onMatch(VolcanoRuleCall.java:250)
\tat org.apache.calcite.plan.volcano.IterativeRuleDriver.drive(IterativeRuleDriver.java:59)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.findBestExp(VolcanoPlanner.java:523)
\tat org.apache.calcite.tools.Programs$RuleSetProgram.run(Programs.java:317)
\tat org.apache.flink.table.planner.plan.optimize.program.FlinkVolcanoProgram.optimize(FlinkVolcanoProgram.scala:62)
\tat org.apache.flink.table.planner.plan.optimize.program.FlinkChainedProgram.$anonfun$optimize$1(FlinkChainedProgram.scala:59)
\tat scala.collection.TraversableOnce$folder$1.apply(TraversableOnce.scala:196)
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
\tat jdk.internal.reflect.GeneratedMethodAccessor47.invoke(Unknown Source)
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
\tat org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalAggregate.computeSelfCost(FlinkLogicalAggregate.scala:84)
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
