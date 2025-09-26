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
    return values.nunique()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_8 = table_env.from_path("web_returns").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("web_returns").get_schema().get_field_names()])
autonode_7 = table_env.from_path("household_demographics").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("household_demographics").get_schema().get_field_names()])
autonode_6 = autonode_8.order_by(col('wr_refunded_customer_sk_node_8'))
autonode_5 = autonode_7.add_columns(lit("hello"))
autonode_4 = autonode_5.join(autonode_6, col('wr_reason_sk_node_8') == col('hd_demo_sk_node_7'))
autonode_3 = autonode_4.group_by(col('wr_web_page_sk_node_8')).select(col('wr_refunded_hdemo_sk_node_8').max.alias('wr_refunded_hdemo_sk_node_8'))
autonode_2 = autonode_3.add_columns(lit("hello"))
autonode_1 = autonode_2.distinct()
sink = autonode_1.filter(col('wr_refunded_hdemo_sk_node_8') < -44)
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
LogicalFilter(condition=[<($0, -44)])
+- LogicalAggregate(group=[{0, 1}])
   +- LogicalProject(wr_refunded_hdemo_sk_node_8=[$1], _c1=[_UTF-16LE'hello'])
      +- LogicalAggregate(group=[{17}], EXPR$0=[MAX($11)])
         +- LogicalJoin(condition=[=($18, $0)], joinType=[inner])
            :- LogicalProject(hd_demo_sk_node_7=[$0], hd_income_band_sk_node_7=[$1], hd_buy_potential_node_7=[$2], hd_dep_count_node_7=[$3], hd_vehicle_count_node_7=[$4], _c5=[_UTF-16LE'hello'])
            :  +- LogicalTableScan(table=[[default_catalog, default_database, household_demographics]])
            +- LogicalSort(sort0=[$3], dir0=[ASC])
               +- LogicalProject(wr_returned_date_sk_node_8=[$0], wr_returned_time_sk_node_8=[$1], wr_item_sk_node_8=[$2], wr_refunded_customer_sk_node_8=[$3], wr_refunded_cdemo_sk_node_8=[$4], wr_refunded_hdemo_sk_node_8=[$5], wr_refunded_addr_sk_node_8=[$6], wr_returning_customer_sk_node_8=[$7], wr_returning_cdemo_sk_node_8=[$8], wr_returning_hdemo_sk_node_8=[$9], wr_returning_addr_sk_node_8=[$10], wr_web_page_sk_node_8=[$11], wr_reason_sk_node_8=[$12], wr_order_number_node_8=[$13], wr_return_quantity_node_8=[$14], wr_return_amt_node_8=[$15], wr_return_tax_node_8=[$16], wr_return_amt_inc_tax_node_8=[$17], wr_fee_node_8=[$18], wr_return_ship_cost_node_8=[$19], wr_refunded_cash_node_8=[$20], wr_reversed_charge_node_8=[$21], wr_account_credit_node_8=[$22], wr_net_loss_node_8=[$23])
                  +- LogicalTableScan(table=[[default_catalog, default_database, web_returns]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS wr_refunded_hdemo_sk_node_8, 'hello' AS _c1])
+- HashAggregate(isMerge=[true], groupBy=[EXPR$0], select=[EXPR$0])
   +- Exchange(distribution=[hash[EXPR$0]])
      +- LocalHashAggregate(groupBy=[EXPR$0], select=[EXPR$0])
         +- Calc(select=[EXPR$0], where=[<(EXPR$0, -44)])
            +- HashAggregate(isMerge=[true], groupBy=[wr_web_page_sk], select=[wr_web_page_sk, Final_MAX(max$0) AS EXPR$0])
               +- Exchange(distribution=[hash[wr_web_page_sk]])
                  +- LocalHashAggregate(groupBy=[wr_web_page_sk], select=[wr_web_page_sk, Partial_MAX(wr_refunded_hdemo_sk) AS max$0])
                     +- HashJoin(joinType=[InnerJoin], where=[=(wr_reason_sk, hd_demo_sk_node_7)], select=[hd_demo_sk_node_7, hd_income_band_sk_node_7, hd_buy_potential_node_7, hd_dep_count_node_7, hd_vehicle_count_node_7, _c5, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], isBroadcast=[true], build=[left])
                        :- Exchange(distribution=[broadcast])
                        :  +- Calc(select=[hd_demo_sk AS hd_demo_sk_node_7, hd_income_band_sk AS hd_income_band_sk_node_7, hd_buy_potential AS hd_buy_potential_node_7, hd_dep_count AS hd_dep_count_node_7, hd_vehicle_count AS hd_vehicle_count_node_7, 'hello' AS _c5])
                        :     +- TableSourceScan(table=[[default_catalog, default_database, household_demographics]], fields=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
                        +- Sort(orderBy=[wr_refunded_customer_sk ASC])
                           +- Exchange(distribution=[single])
                              +- TableSourceScan(table=[[default_catalog, default_database, web_returns]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS wr_refunded_hdemo_sk_node_8, 'hello' AS _c1])
+- HashAggregate(isMerge=[true], groupBy=[EXPR$0], select=[EXPR$0])
   +- Exchange(distribution=[hash[EXPR$0]])
      +- LocalHashAggregate(groupBy=[EXPR$0], select=[EXPR$0])
         +- Calc(select=[EXPR$0], where=[(EXPR$0 < -44)])
            +- HashAggregate(isMerge=[true], groupBy=[wr_web_page_sk], select=[wr_web_page_sk, Final_MAX(max$0) AS EXPR$0])
               +- Exchange(distribution=[hash[wr_web_page_sk]])
                  +- LocalHashAggregate(groupBy=[wr_web_page_sk], select=[wr_web_page_sk, Partial_MAX(wr_refunded_hdemo_sk) AS max$0])
                     +- HashJoin(joinType=[InnerJoin], where=[(wr_reason_sk = hd_demo_sk_node_7)], select=[hd_demo_sk_node_7, hd_income_band_sk_node_7, hd_buy_potential_node_7, hd_dep_count_node_7, hd_vehicle_count_node_7, _c5, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], isBroadcast=[true], build=[left])
                        :- Exchange(distribution=[broadcast])
                        :  +- Calc(select=[hd_demo_sk AS hd_demo_sk_node_7, hd_income_band_sk AS hd_income_band_sk_node_7, hd_buy_potential AS hd_buy_potential_node_7, hd_dep_count AS hd_dep_count_node_7, hd_vehicle_count AS hd_vehicle_count_node_7, 'hello' AS _c5])
                        :     +- TableSourceScan(table=[[default_catalog, default_database, household_demographics]], fields=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
                        +- Sort(orderBy=[wr_refunded_customer_sk ASC])
                           +- Exchange(distribution=[single])
                              +- TableSourceScan(table=[[default_catalog, default_database, web_returns]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o78014755.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#157083232:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[3](input=RelSubset#157083230,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[3]), rel#157083229:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[3](input=RelSubset#157083228,groupBy=wr_web_page_sk, wr_reason_sk,select=wr_web_page_sk, wr_reason_sk, Partial_MAX(wr_refunded_hdemo_sk) AS max$0)]
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