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

autonode_10 = table_env.from_path("catalog_returns").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("catalog_returns").get_schema().get_field_names()])
autonode_11 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_8 = autonode_10.limit(71)
autonode_9 = autonode_11.alias('tA8Wg')
autonode_6 = autonode_8.order_by(col('cr_warehouse_sk_node_10'))
autonode_7 = autonode_9.select(col('ss_sold_time_sk_node_11'))
autonode_4 = autonode_6.order_by(col('cr_return_amount_node_10'))
autonode_5 = autonode_7.add_columns(lit("hello"))
autonode_2 = autonode_4.filter(col('cr_returned_time_sk_node_10') < 19)
autonode_3 = autonode_5.order_by(col('ss_sold_time_sk_node_11'))
autonode_1 = autonode_2.join(autonode_3, col('ss_sold_time_sk_node_11') == col('cr_item_sk_node_10'))
sink = autonode_1.group_by(col('cr_returned_time_sk_node_10')).select(col('cr_returning_hdemo_sk_node_10').min.alias('cr_returning_hdemo_sk_node_10'))
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
      "error_message": "An error occurred while calling o213401610.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#431957244:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[18](input=RelSubset#431957242,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[18]), rel#431957241:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[18](input=RelSubset#431957240,groupBy=cr_returned_time_sk, cr_item_sk,select=cr_returned_time_sk, cr_item_sk, Partial_MIN(cr_returning_hdemo_sk) AS min$0)]
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
LogicalProject(cr_returning_hdemo_sk_node_10=[$1])
+- LogicalAggregate(group=[{1}], EXPR$0=[MIN($9)])
   +- LogicalJoin(condition=[=($27, $2)], joinType=[inner])
      :- LogicalFilter(condition=[<($1, 19)])
      :  +- LogicalSort(sort0=[$18], dir0=[ASC])
      :     +- LogicalSort(sort0=[$14], dir0=[ASC])
      :        +- LogicalSort(fetch=[71])
      :           +- LogicalProject(cr_returned_date_sk_node_10=[$0], cr_returned_time_sk_node_10=[$1], cr_item_sk_node_10=[$2], cr_refunded_customer_sk_node_10=[$3], cr_refunded_cdemo_sk_node_10=[$4], cr_refunded_hdemo_sk_node_10=[$5], cr_refunded_addr_sk_node_10=[$6], cr_returning_customer_sk_node_10=[$7], cr_returning_cdemo_sk_node_10=[$8], cr_returning_hdemo_sk_node_10=[$9], cr_returning_addr_sk_node_10=[$10], cr_call_center_sk_node_10=[$11], cr_catalog_page_sk_node_10=[$12], cr_ship_mode_sk_node_10=[$13], cr_warehouse_sk_node_10=[$14], cr_reason_sk_node_10=[$15], cr_order_number_node_10=[$16], cr_return_quantity_node_10=[$17], cr_return_amount_node_10=[$18], cr_return_tax_node_10=[$19], cr_return_amt_inc_tax_node_10=[$20], cr_fee_node_10=[$21], cr_return_ship_cost_node_10=[$22], cr_refunded_cash_node_10=[$23], cr_reversed_charge_node_10=[$24], cr_store_credit_node_10=[$25], cr_net_loss_node_10=[$26])
      :              +- LogicalTableScan(table=[[default_catalog, default_database, catalog_returns]])
      +- LogicalSort(sort0=[$0], dir0=[ASC])
         +- LogicalProject(ss_sold_time_sk_node_11=[$1], _c1=[_UTF-16LE'hello'])
            +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS cr_returning_hdemo_sk_node_10])
+- SortAggregate(isMerge=[false], groupBy=[cr_returned_time_sk], select=[cr_returned_time_sk, MIN(cr_returning_hdemo_sk) AS EXPR$0])
   +- Sort(orderBy=[cr_returned_time_sk ASC])
      +- NestedLoopJoin(joinType=[InnerJoin], where=[=(ss_sold_time_sk_node_11, cr_item_sk)], select=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss, ss_sold_time_sk_node_11, _c1], build=[right])
         :- Exchange(distribution=[hash[cr_returned_time_sk]])
         :  +- Calc(select=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss], where=[<(cr_returned_time_sk, 19)])
         :     +- SortLimit(orderBy=[cr_return_amount ASC], offset=[0], fetch=[1], global=[true])
         :        +- Exchange(distribution=[single])
         :           +- SortLimit(orderBy=[cr_return_amount ASC], offset=[0], fetch=[1], global=[false])
         :              +- SortLimit(orderBy=[cr_warehouse_sk ASC], offset=[0], fetch=[1], global=[true])
         :                 +- Exchange(distribution=[single])
         :                    +- SortLimit(orderBy=[cr_warehouse_sk ASC], offset=[0], fetch=[1], global=[false])
         :                       +- Limit(offset=[0], fetch=[71], global=[true])
         :                          +- Exchange(distribution=[single])
         :                             +- Limit(offset=[0], fetch=[71], global=[false])
         :                                +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns, limit=[71]]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])
         +- Exchange(distribution=[broadcast])
            +- Calc(select=[ss_sold_time_sk AS ss_sold_time_sk_node_11, 'hello' AS _c1])
               +- SortLimit(orderBy=[ss_sold_time_sk ASC], offset=[0], fetch=[1], global=[true])
                  +- Exchange(distribution=[single])
                     +- SortLimit(orderBy=[ss_sold_time_sk ASC], offset=[0], fetch=[1], global=[false])
                        +- TableSourceScan(table=[[default_catalog, default_database, store_sales, project=[ss_sold_time_sk], metadata=[]]], fields=[ss_sold_time_sk])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS cr_returning_hdemo_sk_node_10])
+- SortAggregate(isMerge=[false], groupBy=[cr_returned_time_sk], select=[cr_returned_time_sk, MIN(cr_returning_hdemo_sk) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[cr_returned_time_sk ASC])
         +- Exchange(distribution=[keep_input_as_is[hash[cr_returned_time_sk]]])
            +- NestedLoopJoin(joinType=[InnerJoin], where=[(ss_sold_time_sk_node_11 = cr_item_sk)], select=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss, ss_sold_time_sk_node_11, _c1], build=[right])
               :- Exchange(distribution=[hash[cr_returned_time_sk]])
               :  +- Calc(select=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss], where=[(cr_returned_time_sk < 19)])
               :     +- SortLimit(orderBy=[cr_return_amount ASC], offset=[0], fetch=[1], global=[true])
               :        +- Exchange(distribution=[single])
               :           +- SortLimit(orderBy=[cr_return_amount ASC], offset=[0], fetch=[1], global=[false])
               :              +- SortLimit(orderBy=[cr_warehouse_sk ASC], offset=[0], fetch=[1], global=[true])
               :                 +- Exchange(distribution=[single])
               :                    +- SortLimit(orderBy=[cr_warehouse_sk ASC], offset=[0], fetch=[1], global=[false])
               :                       +- Limit(offset=[0], fetch=[71], global=[true])
               :                          +- Exchange(distribution=[single])
               :                             +- Limit(offset=[0], fetch=[71], global=[false])
               :                                +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns, limit=[71]]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])
               +- Exchange(distribution=[broadcast])
                  +- Calc(select=[ss_sold_time_sk AS ss_sold_time_sk_node_11, 'hello' AS _c1])
                     +- SortLimit(orderBy=[ss_sold_time_sk ASC], offset=[0], fetch=[1], global=[true])
                        +- Exchange(distribution=[single])
                           +- SortLimit(orderBy=[ss_sold_time_sk ASC], offset=[0], fetch=[1], global=[false])
                              +- TableSourceScan(table=[[default_catalog, default_database, store_sales, project=[ss_sold_time_sk], metadata=[]]], fields=[ss_sold_time_sk])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0