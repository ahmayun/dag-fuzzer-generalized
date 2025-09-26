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
    return len(values) / (1.0 / values).sum() if (values != 0).all() else None


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_10 = table_env.from_path("catalog_returns").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("catalog_returns").get_schema().get_field_names()])
autonode_9 = table_env.from_path("catalog_page").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("catalog_page").get_schema().get_field_names()])
autonode_8 = autonode_10.order_by(col('cr_warehouse_sk_node_10'))
autonode_7 = autonode_9.add_columns(lit("hello"))
autonode_6 = autonode_8.filter(col('cr_returning_customer_sk_node_10') < -5)
autonode_5 = autonode_7.add_columns(lit("hello"))
autonode_4 = autonode_6.filter(col('cr_store_credit_node_10') >= 32.63423442840576)
autonode_3 = autonode_5.group_by(col('cp_catalog_page_id_node_9')).select(col('cp_catalog_page_sk_node_9').count.alias('cp_catalog_page_sk_node_9'))
autonode_2 = autonode_3.join(autonode_4, col('cr_return_quantity_node_10') == col('cp_catalog_page_sk_node_9'))
autonode_1 = autonode_2.group_by(col('cr_return_amt_inc_tax_node_10')).select(col('cr_refunded_cash_node_10').max.alias('cr_refunded_cash_node_10'))
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
LogicalProject(cr_refunded_cash_node_10=[$1], _c1=[_UTF-16LE'hello'])
+- LogicalAggregate(group=[{21}], EXPR$0=[MAX($24)])
   +- LogicalJoin(condition=[=($18, $0)], joinType=[inner])
      :- LogicalProject(cp_catalog_page_sk_node_9=[$1])
      :  +- LogicalAggregate(group=[{1}], EXPR$0=[COUNT($0)])
      :     +- LogicalProject(cp_catalog_page_sk_node_9=[$0], cp_catalog_page_id_node_9=[$1])
      :        +- LogicalTableScan(table=[[default_catalog, default_database, catalog_page]])
      +- LogicalFilter(condition=[>=($25, 3.263423442840576E1:DOUBLE)])
         +- LogicalFilter(condition=[<($7, -5)])
            +- LogicalSort(sort0=[$14], dir0=[ASC])
               +- LogicalProject(cr_returned_date_sk_node_10=[$0], cr_returned_time_sk_node_10=[$1], cr_item_sk_node_10=[$2], cr_refunded_customer_sk_node_10=[$3], cr_refunded_cdemo_sk_node_10=[$4], cr_refunded_hdemo_sk_node_10=[$5], cr_refunded_addr_sk_node_10=[$6], cr_returning_customer_sk_node_10=[$7], cr_returning_cdemo_sk_node_10=[$8], cr_returning_hdemo_sk_node_10=[$9], cr_returning_addr_sk_node_10=[$10], cr_call_center_sk_node_10=[$11], cr_catalog_page_sk_node_10=[$12], cr_ship_mode_sk_node_10=[$13], cr_warehouse_sk_node_10=[$14], cr_reason_sk_node_10=[$15], cr_order_number_node_10=[$16], cr_return_quantity_node_10=[$17], cr_return_amount_node_10=[$18], cr_return_tax_node_10=[$19], cr_return_amt_inc_tax_node_10=[$20], cr_fee_node_10=[$21], cr_return_ship_cost_node_10=[$22], cr_refunded_cash_node_10=[$23], cr_reversed_charge_node_10=[$24], cr_store_credit_node_10=[$25], cr_net_loss_node_10=[$26])
                  +- LogicalTableScan(table=[[default_catalog, default_database, catalog_returns]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS cr_refunded_cash_node_10, 'hello' AS _c1])
+- HashAggregate(isMerge=[true], groupBy=[cr_return_amt_inc_tax_node_10], select=[cr_return_amt_inc_tax_node_10, Final_MAX(max$0) AS EXPR$0])
   +- Exchange(distribution=[hash[cr_return_amt_inc_tax_node_10]])
      +- LocalHashAggregate(groupBy=[cr_return_amt_inc_tax_node_10], select=[cr_return_amt_inc_tax_node_10, Partial_MAX(cr_refunded_cash_node_10) AS max$0])
         +- Calc(select=[cp_catalog_page_sk_node_9, cr_returned_date_sk_node_10, cr_returned_time_sk_node_10, cr_item_sk_node_10, cr_refunded_customer_sk_node_10, cr_refunded_cdemo_sk_node_10, cr_refunded_hdemo_sk_node_10, cr_refunded_addr_sk_node_10, cr_returning_customer_sk_node_10, cr_returning_cdemo_sk_node_10, cr_returning_hdemo_sk_node_10, cr_returning_addr_sk_node_10, cr_call_center_sk_node_10, cr_catalog_page_sk_node_10, cr_ship_mode_sk_node_10, cr_warehouse_sk_node_10, cr_reason_sk_node_10, cr_order_number_node_10, cr_return_quantity_node_10, cr_return_amount_node_10, cr_return_tax_node_10, cr_return_amt_inc_tax_node_10, cr_fee_node_10, cr_return_ship_cost_node_10, cr_refunded_cash_node_10, cr_reversed_charge_node_10, cr_store_credit_node_10, cr_net_loss_node_10])
            +- HashJoin(joinType=[InnerJoin], where=[=(cr_return_quantity_node_100, cp_catalog_page_sk_node_9)], select=[cp_catalog_page_sk_node_9, cr_returned_date_sk_node_10, cr_returned_time_sk_node_10, cr_item_sk_node_10, cr_refunded_customer_sk_node_10, cr_refunded_cdemo_sk_node_10, cr_refunded_hdemo_sk_node_10, cr_refunded_addr_sk_node_10, cr_returning_customer_sk_node_10, cr_returning_cdemo_sk_node_10, cr_returning_hdemo_sk_node_10, cr_returning_addr_sk_node_10, cr_call_center_sk_node_10, cr_catalog_page_sk_node_10, cr_ship_mode_sk_node_10, cr_warehouse_sk_node_10, cr_reason_sk_node_10, cr_order_number_node_10, cr_return_quantity_node_10, cr_return_amount_node_10, cr_return_tax_node_10, cr_return_amt_inc_tax_node_10, cr_fee_node_10, cr_return_ship_cost_node_10, cr_refunded_cash_node_10, cr_reversed_charge_node_10, cr_store_credit_node_10, cr_net_loss_node_10, cr_return_quantity_node_100], isBroadcast=[true], build=[left])
               :- Exchange(distribution=[broadcast])
               :  +- Calc(select=[EXPR$0 AS cp_catalog_page_sk_node_9])
               :     +- HashAggregate(isMerge=[true], groupBy=[cp_catalog_page_id], select=[cp_catalog_page_id, Final_COUNT(count$0) AS EXPR$0])
               :        +- Exchange(distribution=[hash[cp_catalog_page_id]])
               :           +- LocalHashAggregate(groupBy=[cp_catalog_page_id], select=[cp_catalog_page_id, Partial_COUNT(cp_catalog_page_sk) AS count$0])
               :              +- TableSourceScan(table=[[default_catalog, default_database, catalog_page, project=[cp_catalog_page_sk, cp_catalog_page_id], metadata=[]]], fields=[cp_catalog_page_sk, cp_catalog_page_id])
               +- Calc(select=[cr_returned_date_sk AS cr_returned_date_sk_node_10, cr_returned_time_sk AS cr_returned_time_sk_node_10, cr_item_sk AS cr_item_sk_node_10, cr_refunded_customer_sk AS cr_refunded_customer_sk_node_10, cr_refunded_cdemo_sk AS cr_refunded_cdemo_sk_node_10, cr_refunded_hdemo_sk AS cr_refunded_hdemo_sk_node_10, cr_refunded_addr_sk AS cr_refunded_addr_sk_node_10, cr_returning_customer_sk AS cr_returning_customer_sk_node_10, cr_returning_cdemo_sk AS cr_returning_cdemo_sk_node_10, cr_returning_hdemo_sk AS cr_returning_hdemo_sk_node_10, cr_returning_addr_sk AS cr_returning_addr_sk_node_10, cr_call_center_sk AS cr_call_center_sk_node_10, cr_catalog_page_sk AS cr_catalog_page_sk_node_10, cr_ship_mode_sk AS cr_ship_mode_sk_node_10, cr_warehouse_sk AS cr_warehouse_sk_node_10, cr_reason_sk AS cr_reason_sk_node_10, cr_order_number AS cr_order_number_node_10, cr_return_quantity AS cr_return_quantity_node_10, cr_return_amount AS cr_return_amount_node_10, cr_return_tax AS cr_return_tax_node_10, cr_return_amt_inc_tax AS cr_return_amt_inc_tax_node_10, cr_fee AS cr_fee_node_10, cr_return_ship_cost AS cr_return_ship_cost_node_10, cr_refunded_cash AS cr_refunded_cash_node_10, cr_reversed_charge AS cr_reversed_charge_node_10, cr_store_credit AS cr_store_credit_node_10, cr_net_loss AS cr_net_loss_node_10, CAST(cr_return_quantity AS BIGINT) AS cr_return_quantity_node_100], where=[AND(<(cr_returning_customer_sk, -5), >=(cr_store_credit, 3.263423442840576E1))])
                  +- Sort(orderBy=[cr_warehouse_sk ASC])
                     +- Exchange(distribution=[single])
                        +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS cr_refunded_cash_node_10, 'hello' AS _c1])
+- HashAggregate(isMerge=[true], groupBy=[cr_return_amt_inc_tax_node_10], select=[cr_return_amt_inc_tax_node_10, Final_MAX(max$0) AS EXPR$0])
   +- Exchange(distribution=[hash[cr_return_amt_inc_tax_node_10]])
      +- LocalHashAggregate(groupBy=[cr_return_amt_inc_tax_node_10], select=[cr_return_amt_inc_tax_node_10, Partial_MAX(cr_refunded_cash_node_10) AS max$0])
         +- Calc(select=[cp_catalog_page_sk_node_9, cr_returned_date_sk_node_10, cr_returned_time_sk_node_10, cr_item_sk_node_10, cr_refunded_customer_sk_node_10, cr_refunded_cdemo_sk_node_10, cr_refunded_hdemo_sk_node_10, cr_refunded_addr_sk_node_10, cr_returning_customer_sk_node_10, cr_returning_cdemo_sk_node_10, cr_returning_hdemo_sk_node_10, cr_returning_addr_sk_node_10, cr_call_center_sk_node_10, cr_catalog_page_sk_node_10, cr_ship_mode_sk_node_10, cr_warehouse_sk_node_10, cr_reason_sk_node_10, cr_order_number_node_10, cr_return_quantity_node_10, cr_return_amount_node_10, cr_return_tax_node_10, cr_return_amt_inc_tax_node_10, cr_fee_node_10, cr_return_ship_cost_node_10, cr_refunded_cash_node_10, cr_reversed_charge_node_10, cr_store_credit_node_10, cr_net_loss_node_10])
            +- HashJoin(joinType=[InnerJoin], where=[(cr_return_quantity_node_100 = cp_catalog_page_sk_node_9)], select=[cp_catalog_page_sk_node_9, cr_returned_date_sk_node_10, cr_returned_time_sk_node_10, cr_item_sk_node_10, cr_refunded_customer_sk_node_10, cr_refunded_cdemo_sk_node_10, cr_refunded_hdemo_sk_node_10, cr_refunded_addr_sk_node_10, cr_returning_customer_sk_node_10, cr_returning_cdemo_sk_node_10, cr_returning_hdemo_sk_node_10, cr_returning_addr_sk_node_10, cr_call_center_sk_node_10, cr_catalog_page_sk_node_10, cr_ship_mode_sk_node_10, cr_warehouse_sk_node_10, cr_reason_sk_node_10, cr_order_number_node_10, cr_return_quantity_node_10, cr_return_amount_node_10, cr_return_tax_node_10, cr_return_amt_inc_tax_node_10, cr_fee_node_10, cr_return_ship_cost_node_10, cr_refunded_cash_node_10, cr_reversed_charge_node_10, cr_store_credit_node_10, cr_net_loss_node_10, cr_return_quantity_node_100], isBroadcast=[true], build=[left])
               :- Exchange(distribution=[broadcast])
               :  +- Calc(select=[EXPR$0 AS cp_catalog_page_sk_node_9])
               :     +- HashAggregate(isMerge=[true], groupBy=[cp_catalog_page_id], select=[cp_catalog_page_id, Final_COUNT(count$0) AS EXPR$0])
               :        +- Exchange(distribution=[hash[cp_catalog_page_id]])
               :           +- LocalHashAggregate(groupBy=[cp_catalog_page_id], select=[cp_catalog_page_id, Partial_COUNT(cp_catalog_page_sk) AS count$0])
               :              +- TableSourceScan(table=[[default_catalog, default_database, catalog_page, project=[cp_catalog_page_sk, cp_catalog_page_id], metadata=[]]], fields=[cp_catalog_page_sk, cp_catalog_page_id])
               +- Calc(select=[cr_returned_date_sk AS cr_returned_date_sk_node_10, cr_returned_time_sk AS cr_returned_time_sk_node_10, cr_item_sk AS cr_item_sk_node_10, cr_refunded_customer_sk AS cr_refunded_customer_sk_node_10, cr_refunded_cdemo_sk AS cr_refunded_cdemo_sk_node_10, cr_refunded_hdemo_sk AS cr_refunded_hdemo_sk_node_10, cr_refunded_addr_sk AS cr_refunded_addr_sk_node_10, cr_returning_customer_sk AS cr_returning_customer_sk_node_10, cr_returning_cdemo_sk AS cr_returning_cdemo_sk_node_10, cr_returning_hdemo_sk AS cr_returning_hdemo_sk_node_10, cr_returning_addr_sk AS cr_returning_addr_sk_node_10, cr_call_center_sk AS cr_call_center_sk_node_10, cr_catalog_page_sk AS cr_catalog_page_sk_node_10, cr_ship_mode_sk AS cr_ship_mode_sk_node_10, cr_warehouse_sk AS cr_warehouse_sk_node_10, cr_reason_sk AS cr_reason_sk_node_10, cr_order_number AS cr_order_number_node_10, cr_return_quantity AS cr_return_quantity_node_10, cr_return_amount AS cr_return_amount_node_10, cr_return_tax AS cr_return_tax_node_10, cr_return_amt_inc_tax AS cr_return_amt_inc_tax_node_10, cr_fee AS cr_fee_node_10, cr_return_ship_cost AS cr_return_ship_cost_node_10, cr_refunded_cash AS cr_refunded_cash_node_10, cr_reversed_charge AS cr_reversed_charge_node_10, cr_store_credit AS cr_store_credit_node_10, cr_net_loss AS cr_net_loss_node_10, CAST(cr_return_quantity AS BIGINT) AS cr_return_quantity_node_100], where=[((cr_returning_customer_sk < -5) AND (cr_store_credit >= 3.263423442840576E1))])
                  +- Sort(orderBy=[cr_warehouse_sk ASC])
                     +- Exchange(distribution=[single])
                        +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o149683646.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#301964736:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[14](input=RelSubset#301964734,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[14]), rel#301964733:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[14](input=RelSubset#301964732,groupBy=cr_return_amt_inc_tax_node_10, cr_return_quantity_node_100,select=cr_return_amt_inc_tax_node_10, cr_return_quantity_node_100, Partial_MAX(cr_refunded_cash_node_10) AS max$0)]
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