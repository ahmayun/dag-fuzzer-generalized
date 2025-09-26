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

autonode_6 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_5 = table_env.from_path("web_returns").select(*[col(column_name).alias(f"{column_name}_node_5") for column_name in table_env.from_path("web_returns").get_schema().get_field_names()])
autonode_4 = autonode_6.order_by(col('ss_hdemo_sk_node_6'))
autonode_3 = autonode_5.alias('CY9QG')
autonode_2 = autonode_3.join(autonode_4, col('ss_quantity_node_6') == col('wr_item_sk_node_5'))
autonode_1 = autonode_2.group_by(col('ss_quantity_node_6')).select(col('ss_coupon_amt_node_6').sum.alias('ss_coupon_amt_node_6'))
sink = autonode_1.group_by(col('ss_coupon_amt_node_6')).select(col('ss_coupon_amt_node_6').sum.alias('ss_coupon_amt_node_6'))
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
LogicalProject(ss_coupon_amt_node_6=[$1])
+- LogicalAggregate(group=[{0}], EXPR$0=[SUM($0)])
   +- LogicalProject(ss_coupon_amt_node_6=[$1])
      +- LogicalAggregate(group=[{34}], EXPR$0=[SUM($43)])
         +- LogicalJoin(condition=[=($34, $2)], joinType=[inner])
            :- LogicalProject(CY9QG=[AS($0, _UTF-16LE'CY9QG')], wr_returned_time_sk_node_5=[$1], wr_item_sk_node_5=[$2], wr_refunded_customer_sk_node_5=[$3], wr_refunded_cdemo_sk_node_5=[$4], wr_refunded_hdemo_sk_node_5=[$5], wr_refunded_addr_sk_node_5=[$6], wr_returning_customer_sk_node_5=[$7], wr_returning_cdemo_sk_node_5=[$8], wr_returning_hdemo_sk_node_5=[$9], wr_returning_addr_sk_node_5=[$10], wr_web_page_sk_node_5=[$11], wr_reason_sk_node_5=[$12], wr_order_number_node_5=[$13], wr_return_quantity_node_5=[$14], wr_return_amt_node_5=[$15], wr_return_tax_node_5=[$16], wr_return_amt_inc_tax_node_5=[$17], wr_fee_node_5=[$18], wr_return_ship_cost_node_5=[$19], wr_refunded_cash_node_5=[$20], wr_reversed_charge_node_5=[$21], wr_account_credit_node_5=[$22], wr_net_loss_node_5=[$23])
            :  +- LogicalTableScan(table=[[default_catalog, default_database, web_returns]])
            +- LogicalSort(sort0=[$5], dir0=[ASC])
               +- LogicalProject(ss_sold_date_sk_node_6=[$0], ss_sold_time_sk_node_6=[$1], ss_item_sk_node_6=[$2], ss_customer_sk_node_6=[$3], ss_cdemo_sk_node_6=[$4], ss_hdemo_sk_node_6=[$5], ss_addr_sk_node_6=[$6], ss_store_sk_node_6=[$7], ss_promo_sk_node_6=[$8], ss_ticket_number_node_6=[$9], ss_quantity_node_6=[$10], ss_wholesale_cost_node_6=[$11], ss_list_price_node_6=[$12], ss_sales_price_node_6=[$13], ss_ext_discount_amt_node_6=[$14], ss_ext_sales_price_node_6=[$15], ss_ext_wholesale_cost_node_6=[$16], ss_ext_list_price_node_6=[$17], ss_ext_tax_node_6=[$18], ss_coupon_amt_node_6=[$19], ss_net_paid_node_6=[$20], ss_net_paid_inc_tax_node_6=[$21], ss_net_profit_node_6=[$22])
                  +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS ss_coupon_amt_node_6])
+- HashAggregate(isMerge=[true], groupBy=[ss_coupon_amt_node_6], select=[ss_coupon_amt_node_6, Final_SUM(sum$0) AS EXPR$0])
   +- Exchange(distribution=[hash[ss_coupon_amt_node_6]])
      +- LocalHashAggregate(groupBy=[ss_coupon_amt_node_6], select=[ss_coupon_amt_node_6, Partial_SUM(ss_coupon_amt_node_6) AS sum$0])
         +- Calc(select=[EXPR$0 AS ss_coupon_amt_node_6])
            +- HashAggregate(isMerge=[false], groupBy=[ss_quantity], select=[ss_quantity, SUM(ss_coupon_amt) AS EXPR$0])
               +- Exchange(distribution=[hash[ss_quantity]])
                  +- NestedLoopJoin(joinType=[InnerJoin], where=[=(ss_quantity, wr_item_sk_node_5)], select=[CY9QG, wr_returned_time_sk_node_5, wr_item_sk_node_5, wr_refunded_customer_sk_node_5, wr_refunded_cdemo_sk_node_5, wr_refunded_hdemo_sk_node_5, wr_refunded_addr_sk_node_5, wr_returning_customer_sk_node_5, wr_returning_cdemo_sk_node_5, wr_returning_hdemo_sk_node_5, wr_returning_addr_sk_node_5, wr_web_page_sk_node_5, wr_reason_sk_node_5, wr_order_number_node_5, wr_return_quantity_node_5, wr_return_amt_node_5, wr_return_tax_node_5, wr_return_amt_inc_tax_node_5, wr_fee_node_5, wr_return_ship_cost_node_5, wr_refunded_cash_node_5, wr_reversed_charge_node_5, wr_account_credit_node_5, wr_net_loss_node_5, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], build=[left])
                     :- Exchange(distribution=[broadcast])
                     :  +- Calc(select=[wr_returned_date_sk AS CY9QG, wr_returned_time_sk AS wr_returned_time_sk_node_5, wr_item_sk AS wr_item_sk_node_5, wr_refunded_customer_sk AS wr_refunded_customer_sk_node_5, wr_refunded_cdemo_sk AS wr_refunded_cdemo_sk_node_5, wr_refunded_hdemo_sk AS wr_refunded_hdemo_sk_node_5, wr_refunded_addr_sk AS wr_refunded_addr_sk_node_5, wr_returning_customer_sk AS wr_returning_customer_sk_node_5, wr_returning_cdemo_sk AS wr_returning_cdemo_sk_node_5, wr_returning_hdemo_sk AS wr_returning_hdemo_sk_node_5, wr_returning_addr_sk AS wr_returning_addr_sk_node_5, wr_web_page_sk AS wr_web_page_sk_node_5, wr_reason_sk AS wr_reason_sk_node_5, wr_order_number AS wr_order_number_node_5, wr_return_quantity AS wr_return_quantity_node_5, wr_return_amt AS wr_return_amt_node_5, wr_return_tax AS wr_return_tax_node_5, wr_return_amt_inc_tax AS wr_return_amt_inc_tax_node_5, wr_fee AS wr_fee_node_5, wr_return_ship_cost AS wr_return_ship_cost_node_5, wr_refunded_cash AS wr_refunded_cash_node_5, wr_reversed_charge AS wr_reversed_charge_node_5, wr_account_credit AS wr_account_credit_node_5, wr_net_loss AS wr_net_loss_node_5])
                     :     +- TableSourceScan(table=[[default_catalog, default_database, web_returns]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])
                     +- Sort(orderBy=[ss_hdemo_sk ASC])
                        +- Exchange(distribution=[single])
                           +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS ss_coupon_amt_node_6])
+- HashAggregate(isMerge=[true], groupBy=[ss_coupon_amt_node_6], select=[ss_coupon_amt_node_6, Final_SUM(sum$0) AS EXPR$0])
   +- Exchange(distribution=[hash[ss_coupon_amt_node_6]])
      +- LocalHashAggregate(groupBy=[ss_coupon_amt_node_6], select=[ss_coupon_amt_node_6, Partial_SUM(ss_coupon_amt_node_6) AS sum$0])
         +- Calc(select=[EXPR$0 AS ss_coupon_amt_node_6])
            +- HashAggregate(isMerge=[false], groupBy=[ss_quantity], select=[ss_quantity, SUM(ss_coupon_amt) AS EXPR$0])
               +- Exchange(distribution=[hash[ss_quantity]])
                  +- NestedLoopJoin(joinType=[InnerJoin], where=[(ss_quantity = wr_item_sk_node_5)], select=[CY9QG, wr_returned_time_sk_node_5, wr_item_sk_node_5, wr_refunded_customer_sk_node_5, wr_refunded_cdemo_sk_node_5, wr_refunded_hdemo_sk_node_5, wr_refunded_addr_sk_node_5, wr_returning_customer_sk_node_5, wr_returning_cdemo_sk_node_5, wr_returning_hdemo_sk_node_5, wr_returning_addr_sk_node_5, wr_web_page_sk_node_5, wr_reason_sk_node_5, wr_order_number_node_5, wr_return_quantity_node_5, wr_return_amt_node_5, wr_return_tax_node_5, wr_return_amt_inc_tax_node_5, wr_fee_node_5, wr_return_ship_cost_node_5, wr_refunded_cash_node_5, wr_reversed_charge_node_5, wr_account_credit_node_5, wr_net_loss_node_5, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], build=[left])
                     :- Exchange(distribution=[broadcast])
                     :  +- Calc(select=[wr_returned_date_sk AS CY9QG, wr_returned_time_sk AS wr_returned_time_sk_node_5, wr_item_sk AS wr_item_sk_node_5, wr_refunded_customer_sk AS wr_refunded_customer_sk_node_5, wr_refunded_cdemo_sk AS wr_refunded_cdemo_sk_node_5, wr_refunded_hdemo_sk AS wr_refunded_hdemo_sk_node_5, wr_refunded_addr_sk AS wr_refunded_addr_sk_node_5, wr_returning_customer_sk AS wr_returning_customer_sk_node_5, wr_returning_cdemo_sk AS wr_returning_cdemo_sk_node_5, wr_returning_hdemo_sk AS wr_returning_hdemo_sk_node_5, wr_returning_addr_sk AS wr_returning_addr_sk_node_5, wr_web_page_sk AS wr_web_page_sk_node_5, wr_reason_sk AS wr_reason_sk_node_5, wr_order_number AS wr_order_number_node_5, wr_return_quantity AS wr_return_quantity_node_5, wr_return_amt AS wr_return_amt_node_5, wr_return_tax AS wr_return_tax_node_5, wr_return_amt_inc_tax AS wr_return_amt_inc_tax_node_5, wr_fee AS wr_fee_node_5, wr_return_ship_cost AS wr_return_ship_cost_node_5, wr_refunded_cash AS wr_refunded_cash_node_5, wr_reversed_charge AS wr_reversed_charge_node_5, wr_account_credit AS wr_account_credit_node_5, wr_net_loss AS wr_net_loss_node_5])
                     :     +- TableSourceScan(table=[[default_catalog, default_database, web_returns]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])
                     +- Sort(orderBy=[ss_hdemo_sk ASC])
                        +- Exchange(distribution=[single])
                           +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o127163589.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#256448564:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[5](input=RelSubset#256448562,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[5]), rel#256448561:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[5](input=RelSubset#256448560,groupBy=ss_quantity,select=ss_quantity, Partial_SUM(ss_coupon_amt) AS sum$0)]
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