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
    return values.mean()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_9 = table_env.from_path("store_returns").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("store_returns").get_schema().get_field_names()])
autonode_8 = table_env.from_path("household_demographics").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("household_demographics").get_schema().get_field_names()])
autonode_7 = autonode_9.filter(col('sr_ticket_number_node_9') <= 27)
autonode_6 = autonode_8.group_by(col('hd_demo_sk_node_8')).select(col('hd_income_band_sk_node_8').min.alias('hd_income_band_sk_node_8'))
autonode_5 = autonode_7.order_by(col('sr_hdemo_sk_node_9'))
autonode_4 = autonode_6.select(col('hd_income_band_sk_node_8'))
autonode_3 = autonode_4.join(autonode_5, col('hd_income_band_sk_node_8') == col('sr_ticket_number_node_9'))
autonode_2 = autonode_3.group_by(col('sr_reason_sk_node_9')).select(col('sr_item_sk_node_9').max.alias('sr_item_sk_node_9'))
autonode_1 = autonode_2.filter(col('sr_item_sk_node_9') <= 34)
sink = autonode_1.add_columns(lit("hello"))
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
LogicalProject(sr_item_sk_node_9=[$0], _c1=[_UTF-16LE'hello'])
+- LogicalFilter(condition=[<=($0, 34)])
   +- LogicalProject(sr_item_sk_node_9=[$1])
      +- LogicalAggregate(group=[{9}], EXPR$0=[MAX($3)])
         +- LogicalJoin(condition=[=($0, $10)], joinType=[inner])
            :- LogicalProject(hd_income_band_sk_node_8=[$1])
            :  +- LogicalAggregate(group=[{0}], EXPR$0=[MIN($1)])
            :     +- LogicalProject(hd_demo_sk_node_8=[$0], hd_income_band_sk_node_8=[$1])
            :        +- LogicalTableScan(table=[[default_catalog, default_database, household_demographics]])
            +- LogicalSort(sort0=[$5], dir0=[ASC])
               +- LogicalFilter(condition=[<=($9, 27)])
                  +- LogicalProject(sr_returned_date_sk_node_9=[$0], sr_return_time_sk_node_9=[$1], sr_item_sk_node_9=[$2], sr_customer_sk_node_9=[$3], sr_cdemo_sk_node_9=[$4], sr_hdemo_sk_node_9=[$5], sr_addr_sk_node_9=[$6], sr_store_sk_node_9=[$7], sr_reason_sk_node_9=[$8], sr_ticket_number_node_9=[$9], sr_return_quantity_node_9=[$10], sr_return_amt_node_9=[$11], sr_return_tax_node_9=[$12], sr_return_amt_inc_tax_node_9=[$13], sr_fee_node_9=[$14], sr_return_ship_cost_node_9=[$15], sr_refunded_cash_node_9=[$16], sr_reversed_charge_node_9=[$17], sr_store_credit_node_9=[$18], sr_net_loss_node_9=[$19])
                     +- LogicalTableScan(table=[[default_catalog, default_database, store_returns]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS sr_item_sk_node_9, 'hello' AS _c1], where=[<=(EXPR$0, 34)])
+- HashAggregate(isMerge=[true], groupBy=[sr_reason_sk], select=[sr_reason_sk, Final_MAX(max$0) AS EXPR$0])
   +- Exchange(distribution=[hash[sr_reason_sk]])
      +- LocalHashAggregate(groupBy=[sr_reason_sk], select=[sr_reason_sk, Partial_MAX(sr_item_sk) AS max$0])
         +- HashJoin(joinType=[InnerJoin], where=[=(hd_income_band_sk_node_8, sr_ticket_number)], select=[hd_income_band_sk_node_8, sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss], isBroadcast=[true], build=[left])
            :- Exchange(distribution=[broadcast])
            :  +- Calc(select=[EXPR$0 AS hd_income_band_sk_node_8])
            :     +- HashAggregate(isMerge=[true], groupBy=[hd_demo_sk], select=[hd_demo_sk, Final_MIN(min$0) AS EXPR$0])
            :        +- Exchange(distribution=[hash[hd_demo_sk]])
            :           +- LocalHashAggregate(groupBy=[hd_demo_sk], select=[hd_demo_sk, Partial_MIN(hd_income_band_sk) AS min$0])
            :              +- TableSourceScan(table=[[default_catalog, default_database, household_demographics, project=[hd_demo_sk, hd_income_band_sk], metadata=[]]], fields=[hd_demo_sk, hd_income_band_sk])
            +- Sort(orderBy=[sr_hdemo_sk ASC])
               +- Exchange(distribution=[single])
                  +- Calc(select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss], where=[<=(sr_ticket_number, 27)])
                     +- TableSourceScan(table=[[default_catalog, default_database, store_returns, filter=[<=(sr_ticket_number, 27)]]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS sr_item_sk_node_9, 'hello' AS _c1], where=[(EXPR$0 <= 34)])
+- HashAggregate(isMerge=[true], groupBy=[sr_reason_sk], select=[sr_reason_sk, Final_MAX(max$0) AS EXPR$0])
   +- Exchange(distribution=[hash[sr_reason_sk]])
      +- LocalHashAggregate(groupBy=[sr_reason_sk], select=[sr_reason_sk, Partial_MAX(sr_item_sk) AS max$0])
         +- HashJoin(joinType=[InnerJoin], where=[(hd_income_band_sk_node_8 = sr_ticket_number)], select=[hd_income_band_sk_node_8, sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss], isBroadcast=[true], build=[left])
            :- Exchange(distribution=[broadcast])
            :  +- Calc(select=[EXPR$0 AS hd_income_band_sk_node_8])
            :     +- HashAggregate(isMerge=[true], groupBy=[hd_demo_sk], select=[hd_demo_sk, Final_MIN(min$0) AS EXPR$0])
            :        +- Exchange(distribution=[hash[hd_demo_sk]])
            :           +- LocalHashAggregate(groupBy=[hd_demo_sk], select=[hd_demo_sk, Partial_MIN(hd_income_band_sk) AS min$0])
            :              +- TableSourceScan(table=[[default_catalog, default_database, household_demographics, project=[hd_demo_sk, hd_income_band_sk], metadata=[]]], fields=[hd_demo_sk, hd_income_band_sk])
            +- Sort(orderBy=[sr_hdemo_sk ASC])
               +- Exchange(distribution=[single])
                  +- Calc(select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss], where=[(sr_ticket_number <= 27)])
                     +- TableSourceScan(table=[[default_catalog, default_database, store_returns, filter=[<=(sr_ticket_number, 27)]]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o78589530.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#158410974:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[5](input=RelSubset#158410972,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[5]), rel#158410971:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[5](input=RelSubset#158410970,groupBy=sr_reason_sk, sr_ticket_number,select=sr_reason_sk, sr_ticket_number, Partial_MAX(sr_item_sk) AS max$0)]
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