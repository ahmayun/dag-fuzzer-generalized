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

autonode_8 = table_env.from_path("reason").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("reason").get_schema().get_field_names()])
autonode_7 = table_env.from_path("catalog_sales").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("catalog_sales").get_schema().get_field_names()])
autonode_6 = autonode_7.join(autonode_8, col('cs_call_center_sk_node_7') == col('r_reason_sk_node_8'))
autonode_5 = autonode_6.add_columns(lit("hello"))
autonode_4 = autonode_5.select(col('r_reason_id_node_8'))
autonode_3 = autonode_4.distinct()
autonode_2 = autonode_3.order_by(col('r_reason_id_node_8'))
autonode_1 = autonode_2.group_by(col('r_reason_id_node_8')).select(col('r_reason_id_node_8').min.alias('r_reason_id_node_8'))
sink = autonode_1.limit(37)
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
LogicalSort(fetch=[37])
+- LogicalProject(r_reason_id_node_8=[$1])
   +- LogicalAggregate(group=[{0}], EXPR$0=[MIN($0)])
      +- LogicalSort(sort0=[$0], dir0=[ASC])
         +- LogicalAggregate(group=[{0}])
            +- LogicalProject(r_reason_id_node_8=[$35])
               +- LogicalJoin(condition=[=($11, $34)], joinType=[inner])
                  :- LogicalProject(cs_sold_date_sk_node_7=[$0], cs_sold_time_sk_node_7=[$1], cs_ship_date_sk_node_7=[$2], cs_bill_customer_sk_node_7=[$3], cs_bill_cdemo_sk_node_7=[$4], cs_bill_hdemo_sk_node_7=[$5], cs_bill_addr_sk_node_7=[$6], cs_ship_customer_sk_node_7=[$7], cs_ship_cdemo_sk_node_7=[$8], cs_ship_hdemo_sk_node_7=[$9], cs_ship_addr_sk_node_7=[$10], cs_call_center_sk_node_7=[$11], cs_catalog_page_sk_node_7=[$12], cs_ship_mode_sk_node_7=[$13], cs_warehouse_sk_node_7=[$14], cs_item_sk_node_7=[$15], cs_promo_sk_node_7=[$16], cs_order_number_node_7=[$17], cs_quantity_node_7=[$18], cs_wholesale_cost_node_7=[$19], cs_list_price_node_7=[$20], cs_sales_price_node_7=[$21], cs_ext_discount_amt_node_7=[$22], cs_ext_sales_price_node_7=[$23], cs_ext_wholesale_cost_node_7=[$24], cs_ext_list_price_node_7=[$25], cs_ext_tax_node_7=[$26], cs_coupon_amt_node_7=[$27], cs_ext_ship_cost_node_7=[$28], cs_net_paid_node_7=[$29], cs_net_paid_inc_tax_node_7=[$30], cs_net_paid_inc_ship_node_7=[$31], cs_net_paid_inc_ship_tax_node_7=[$32], cs_net_profit_node_7=[$33])
                  :  +- LogicalTableScan(table=[[default_catalog, default_database, catalog_sales]])
                  +- LogicalProject(r_reason_sk_node_8=[$0], r_reason_id_node_8=[$1], r_reason_desc_node_8=[$2])
                     +- LogicalTableScan(table=[[default_catalog, default_database, reason]])

== Optimized Physical Plan ==
Limit(offset=[0], fetch=[37], global=[true])
+- Exchange(distribution=[single])
   +- Limit(offset=[0], fetch=[37], global=[false])
      +- HashAggregate(isMerge=[true], groupBy=[r_reason_id_node_8], select=[r_reason_id_node_8])
         +- Exchange(distribution=[hash[r_reason_id_node_8]])
            +- LocalHashAggregate(groupBy=[r_reason_id_node_8], select=[r_reason_id_node_8])
               +- Calc(select=[r_reason_id AS r_reason_id_node_8])
                  +- HashJoin(joinType=[InnerJoin], where=[=(cs_call_center_sk, r_reason_sk)], select=[cs_call_center_sk, r_reason_sk, r_reason_id], isBroadcast=[true], build=[right])
                     :- TableSourceScan(table=[[default_catalog, default_database, catalog_sales, project=[cs_call_center_sk], metadata=[]]], fields=[cs_call_center_sk])
                     +- Exchange(distribution=[broadcast])
                        +- TableSourceScan(table=[[default_catalog, default_database, reason, project=[r_reason_sk, r_reason_id], metadata=[]]], fields=[r_reason_sk, r_reason_id])

== Optimized Execution Plan ==
Limit(offset=[0], fetch=[37], global=[true])
+- Exchange(distribution=[single])
   +- Limit(offset=[0], fetch=[37], global=[false])
      +- HashAggregate(isMerge=[true], groupBy=[r_reason_id_node_8], select=[r_reason_id_node_8])
         +- Exchange(distribution=[hash[r_reason_id_node_8]])
            +- LocalHashAggregate(groupBy=[r_reason_id_node_8], select=[r_reason_id_node_8])
               +- Calc(select=[r_reason_id AS r_reason_id_node_8])
                  +- MultipleInput(readOrder=[1,0], members=[\
HashJoin(joinType=[InnerJoin], where=[(cs_call_center_sk = r_reason_sk)], select=[cs_call_center_sk, r_reason_sk, r_reason_id], isBroadcast=[true], build=[right])\
:- [#1] TableSourceScan(table=[[default_catalog, default_database, catalog_sales, project=[cs_call_center_sk], metadata=[]]], fields=[cs_call_center_sk])\
+- [#2] Exchange(distribution=[broadcast])\
])
                     :- TableSourceScan(table=[[default_catalog, default_database, catalog_sales, project=[cs_call_center_sk], metadata=[]]], fields=[cs_call_center_sk])
                     +- Exchange(distribution=[broadcast])
                        +- TableSourceScan(table=[[default_catalog, default_database, reason, project=[r_reason_sk, r_reason_id], metadata=[]]], fields=[r_reason_sk, r_reason_id])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o124722172.explain.
: java.lang.RuntimeException: Error while applying rule SortProjectTransposeRule, args [rel#251645947:LogicalSort.NONE.any.[](input=RelSubset#251645926,fetch=37), rel#251645943:LogicalProject.NONE.any.[1](input=RelSubset#251645872,inputs=0,exprs=[$0])]
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