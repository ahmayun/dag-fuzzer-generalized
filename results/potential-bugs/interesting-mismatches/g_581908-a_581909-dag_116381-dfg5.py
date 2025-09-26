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

autonode_4 = table_env.from_path("store_returns").select(*[col(column_name).alias(f"{column_name}_node_4") for column_name in table_env.from_path("store_returns").get_schema().get_field_names()])
autonode_6 = table_env.from_path("store_returns").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("store_returns").get_schema().get_field_names()])
autonode_5 = table_env.from_path("income_band").select(*[col(column_name).alias(f"{column_name}_node_5") for column_name in table_env.from_path("income_band").get_schema().get_field_names()])
autonode_3 = autonode_6.order_by(col('sr_ticket_number_node_6'))
autonode_2 = autonode_4.join(autonode_5, col('ib_income_band_sk_node_5') == col('sr_customer_sk_node_4'))
autonode_1 = autonode_2.join(autonode_3, col('sr_refunded_cash_node_6') == col('sr_refunded_cash_node_4'))
sink = autonode_1.group_by(col('sr_net_loss_node_6')).select(col('sr_returned_date_sk_node_6').min.alias('sr_returned_date_sk_node_6'))
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
LogicalProject(sr_returned_date_sk_node_6=[$1])
+- LogicalAggregate(group=[{42}], EXPR$0=[MIN($23)])
   +- LogicalJoin(condition=[=($39, $16)], joinType=[inner])
      :- LogicalJoin(condition=[=($20, $3)], joinType=[inner])
      :  :- LogicalProject(sr_returned_date_sk_node_4=[$0], sr_return_time_sk_node_4=[$1], sr_item_sk_node_4=[$2], sr_customer_sk_node_4=[$3], sr_cdemo_sk_node_4=[$4], sr_hdemo_sk_node_4=[$5], sr_addr_sk_node_4=[$6], sr_store_sk_node_4=[$7], sr_reason_sk_node_4=[$8], sr_ticket_number_node_4=[$9], sr_return_quantity_node_4=[$10], sr_return_amt_node_4=[$11], sr_return_tax_node_4=[$12], sr_return_amt_inc_tax_node_4=[$13], sr_fee_node_4=[$14], sr_return_ship_cost_node_4=[$15], sr_refunded_cash_node_4=[$16], sr_reversed_charge_node_4=[$17], sr_store_credit_node_4=[$18], sr_net_loss_node_4=[$19])
      :  :  +- LogicalTableScan(table=[[default_catalog, default_database, store_returns]])
      :  +- LogicalProject(ib_income_band_sk_node_5=[$0], ib_lower_bound_node_5=[$1], ib_upper_bound_node_5=[$2])
      :     +- LogicalTableScan(table=[[default_catalog, default_database, income_band]])
      +- LogicalSort(sort0=[$9], dir0=[ASC])
         +- LogicalProject(sr_returned_date_sk_node_6=[$0], sr_return_time_sk_node_6=[$1], sr_item_sk_node_6=[$2], sr_customer_sk_node_6=[$3], sr_cdemo_sk_node_6=[$4], sr_hdemo_sk_node_6=[$5], sr_addr_sk_node_6=[$6], sr_store_sk_node_6=[$7], sr_reason_sk_node_6=[$8], sr_ticket_number_node_6=[$9], sr_return_quantity_node_6=[$10], sr_return_amt_node_6=[$11], sr_return_tax_node_6=[$12], sr_return_amt_inc_tax_node_6=[$13], sr_fee_node_6=[$14], sr_return_ship_cost_node_6=[$15], sr_refunded_cash_node_6=[$16], sr_reversed_charge_node_6=[$17], sr_store_credit_node_6=[$18], sr_net_loss_node_6=[$19])
            +- LogicalTableScan(table=[[default_catalog, default_database, store_returns]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS sr_returned_date_sk_node_6])
+- HashAggregate(isMerge=[false], groupBy=[sr_net_loss0], select=[sr_net_loss0, MIN(sr_returned_date_sk0) AS EXPR$0])
   +- Exchange(distribution=[hash[sr_net_loss0]])
      +- NestedLoopJoin(joinType=[InnerJoin], where=[=(sr_refunded_cash0, sr_refunded_cash)], select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss, ib_income_band_sk, ib_lower_bound, ib_upper_bound, sr_returned_date_sk0, sr_return_time_sk0, sr_item_sk0, sr_customer_sk0, sr_cdemo_sk0, sr_hdemo_sk0, sr_addr_sk0, sr_store_sk0, sr_reason_sk0, sr_ticket_number0, sr_return_quantity0, sr_return_amt0, sr_return_tax0, sr_return_amt_inc_tax0, sr_fee0, sr_return_ship_cost0, sr_refunded_cash0, sr_reversed_charge0, sr_store_credit0, sr_net_loss0], build=[left])
         :- Exchange(distribution=[broadcast])
         :  +- HashJoin(joinType=[InnerJoin], where=[=(ib_income_band_sk, sr_customer_sk)], select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss, ib_income_band_sk, ib_lower_bound, ib_upper_bound], isBroadcast=[true], build=[right])
         :     :- TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])
         :     +- Exchange(distribution=[broadcast])
         :        +- TableSourceScan(table=[[default_catalog, default_database, income_band]], fields=[ib_income_band_sk, ib_lower_bound, ib_upper_bound])
         +- Sort(orderBy=[sr_ticket_number ASC])
            +- Exchange(distribution=[single])
               +- TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS sr_returned_date_sk_node_6])
+- HashAggregate(isMerge=[false], groupBy=[sr_net_loss0], select=[sr_net_loss0, MIN(sr_returned_date_sk0) AS EXPR$0])
   +- Exchange(distribution=[hash[sr_net_loss0]])
      +- NestedLoopJoin(joinType=[InnerJoin], where=[(sr_refunded_cash0 = sr_refunded_cash)], select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss, ib_income_band_sk, ib_lower_bound, ib_upper_bound, sr_returned_date_sk0, sr_return_time_sk0, sr_item_sk0, sr_customer_sk0, sr_cdemo_sk0, sr_hdemo_sk0, sr_addr_sk0, sr_store_sk0, sr_reason_sk0, sr_ticket_number0, sr_return_quantity0, sr_return_amt0, sr_return_tax0, sr_return_amt_inc_tax0, sr_fee0, sr_return_ship_cost0, sr_refunded_cash0, sr_reversed_charge0, sr_store_credit0, sr_net_loss0], build=[left])
         :- Exchange(distribution=[broadcast])
         :  +- MultipleInput(readOrder=[1,0], members=[\
HashJoin(joinType=[InnerJoin], where=[(ib_income_band_sk = sr_customer_sk)], select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss, ib_income_band_sk, ib_lower_bound, ib_upper_bound], isBroadcast=[true], build=[right])\
:- [#1] TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])\
+- [#2] Exchange(distribution=[broadcast])\
])
         :     :- TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])(reuse_id=[1])
         :     +- Exchange(distribution=[broadcast])
         :        +- TableSourceScan(table=[[default_catalog, default_database, income_band]], fields=[ib_income_band_sk, ib_lower_bound, ib_upper_bound])
         +- Sort(orderBy=[sr_ticket_number ASC])
            +- Exchange(distribution=[single])
               +- Reused(reference_id=[1])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o317429111.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#639739415:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[9](input=RelSubset#639739413,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[9]), rel#639739412:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[9](input=RelSubset#639739411,groupBy=sr_refunded_cash, sr_net_loss,select=sr_refunded_cash, sr_net_loss, Partial_MIN(sr_returned_date_sk) AS min$0)]
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