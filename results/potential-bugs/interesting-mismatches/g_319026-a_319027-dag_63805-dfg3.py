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


def preloaded_aggregation(values: pd.Series) -> int:
    return values.count()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_6 = table_env.from_path("catalog_returns").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("catalog_returns").get_schema().get_field_names()])
autonode_7 = table_env.from_path("store_returns").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("store_returns").get_schema().get_field_names()])
autonode_4 = autonode_6.select(col('cr_ship_mode_sk_node_6'))
autonode_5 = autonode_7.distinct()
autonode_2 = autonode_4.group_by(col('cr_ship_mode_sk_node_6')).select(col('cr_ship_mode_sk_node_6').sum.alias('cr_ship_mode_sk_node_6'))
autonode_3 = autonode_5.order_by(col('sr_reason_sk_node_7'))
autonode_1 = autonode_2.join(autonode_3, col('cr_ship_mode_sk_node_6') == col('sr_reason_sk_node_7'))
sink = autonode_1.group_by(col('sr_return_time_sk_node_7')).select(col('sr_return_ship_cost_node_7').min.alias('sr_return_ship_cost_node_7'))
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
LogicalProject(sr_return_ship_cost_node_7=[$1])
+- LogicalAggregate(group=[{2}], EXPR$0=[MIN($16)])
   +- LogicalJoin(condition=[=($0, $9)], joinType=[inner])
      :- LogicalProject(cr_ship_mode_sk_node_6=[$1])
      :  +- LogicalAggregate(group=[{0}], EXPR$0=[SUM($0)])
      :     +- LogicalProject(cr_ship_mode_sk_node_6=[$13])
      :        +- LogicalTableScan(table=[[default_catalog, default_database, catalog_returns]])
      +- LogicalSort(sort0=[$8], dir0=[ASC])
         +- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}])
            +- LogicalProject(sr_returned_date_sk_node_7=[$0], sr_return_time_sk_node_7=[$1], sr_item_sk_node_7=[$2], sr_customer_sk_node_7=[$3], sr_cdemo_sk_node_7=[$4], sr_hdemo_sk_node_7=[$5], sr_addr_sk_node_7=[$6], sr_store_sk_node_7=[$7], sr_reason_sk_node_7=[$8], sr_ticket_number_node_7=[$9], sr_return_quantity_node_7=[$10], sr_return_amt_node_7=[$11], sr_return_tax_node_7=[$12], sr_return_amt_inc_tax_node_7=[$13], sr_fee_node_7=[$14], sr_return_ship_cost_node_7=[$15], sr_refunded_cash_node_7=[$16], sr_reversed_charge_node_7=[$17], sr_store_credit_node_7=[$18], sr_net_loss_node_7=[$19])
               +- LogicalTableScan(table=[[default_catalog, default_database, store_returns]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS sr_return_ship_cost_node_7])
+- HashAggregate(isMerge=[true], groupBy=[sr_return_time_sk], select=[sr_return_time_sk, Final_MIN(min$0) AS EXPR$0])
   +- Exchange(distribution=[hash[sr_return_time_sk]])
      +- LocalHashAggregate(groupBy=[sr_return_time_sk], select=[sr_return_time_sk, Partial_MIN(sr_return_ship_cost) AS min$0])
         +- HashJoin(joinType=[InnerJoin], where=[=(cr_ship_mode_sk_node_6, sr_reason_sk)], select=[cr_ship_mode_sk_node_6, sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss], isBroadcast=[true], build=[left])
            :- Exchange(distribution=[broadcast])
            :  +- Calc(select=[EXPR$0 AS cr_ship_mode_sk_node_6])
            :     +- HashAggregate(isMerge=[true], groupBy=[cr_ship_mode_sk], select=[cr_ship_mode_sk, Final_SUM(sum$0) AS EXPR$0])
            :        +- Exchange(distribution=[hash[cr_ship_mode_sk]])
            :           +- LocalHashAggregate(groupBy=[cr_ship_mode_sk], select=[cr_ship_mode_sk, Partial_SUM(cr_ship_mode_sk) AS sum$0])
            :              +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns, project=[cr_ship_mode_sk], metadata=[]]], fields=[cr_ship_mode_sk])
            +- Sort(orderBy=[sr_reason_sk ASC])
               +- Exchange(distribution=[single])
                  +- HashAggregate(isMerge=[false], groupBy=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss], select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])
                     +- Exchange(distribution=[hash[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss]])
                        +- TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS sr_return_ship_cost_node_7])
+- HashAggregate(isMerge=[true], groupBy=[sr_return_time_sk], select=[sr_return_time_sk, Final_MIN(min$0) AS EXPR$0])
   +- Exchange(distribution=[hash[sr_return_time_sk]])
      +- LocalHashAggregate(groupBy=[sr_return_time_sk], select=[sr_return_time_sk, Partial_MIN(sr_return_ship_cost) AS min$0])
         +- HashJoin(joinType=[InnerJoin], where=[(cr_ship_mode_sk_node_6 = sr_reason_sk)], select=[cr_ship_mode_sk_node_6, sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss], isBroadcast=[true], build=[left])
            :- Exchange(distribution=[broadcast])
            :  +- Calc(select=[EXPR$0 AS cr_ship_mode_sk_node_6])
            :     +- HashAggregate(isMerge=[true], groupBy=[cr_ship_mode_sk], select=[cr_ship_mode_sk, Final_SUM(sum$0) AS EXPR$0])
            :        +- Exchange(distribution=[hash[cr_ship_mode_sk]])
            :           +- LocalHashAggregate(groupBy=[cr_ship_mode_sk], select=[cr_ship_mode_sk, Partial_SUM(cr_ship_mode_sk) AS sum$0])
            :              +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns, project=[cr_ship_mode_sk], metadata=[]]], fields=[cr_ship_mode_sk])
            +- Sort(orderBy=[sr_reason_sk ASC])
               +- Exchange(distribution=[single])
                  +- HashAggregate(isMerge=[false], groupBy=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss], select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])
                     +- Exchange(distribution=[hash[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss]])
                        +- TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o173522842.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#351057995:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[8](input=RelSubset#351057993,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[8]), rel#351057992:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[8](input=RelSubset#351057991,groupBy=sr_return_time_sk, sr_reason_sk,select=sr_return_time_sk, sr_reason_sk, Partial_MIN(sr_return_ship_cost) AS min$0)]
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