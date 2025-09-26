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
    return values.quantile(0.25)


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_4 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_4") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_5 = table_env.from_path("store_returns").select(*[col(column_name).alias(f"{column_name}_node_5") for column_name in table_env.from_path("store_returns").get_schema().get_field_names()])
autonode_2 = autonode_4.order_by(col('ss_promo_sk_node_4'))
autonode_3 = autonode_5.group_by(col('sr_customer_sk_node_5')).select(col('sr_refunded_cash_node_5').max.alias('sr_refunded_cash_node_5'))
autonode_1 = autonode_2.join(autonode_3, col('sr_refunded_cash_node_5') == col('ss_ext_list_price_node_4'))
sink = autonode_1.group_by(col('ss_ext_tax_node_4')).select(col('ss_ext_sales_price_node_4').max.alias('ss_ext_sales_price_node_4'))
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
LogicalProject(ss_ext_sales_price_node_4=[$1])
+- LogicalAggregate(group=[{18}], EXPR$0=[MAX($15)])
   +- LogicalJoin(condition=[=($23, $17)], joinType=[inner])
      :- LogicalSort(sort0=[$8], dir0=[ASC])
      :  +- LogicalProject(ss_sold_date_sk_node_4=[$0], ss_sold_time_sk_node_4=[$1], ss_item_sk_node_4=[$2], ss_customer_sk_node_4=[$3], ss_cdemo_sk_node_4=[$4], ss_hdemo_sk_node_4=[$5], ss_addr_sk_node_4=[$6], ss_store_sk_node_4=[$7], ss_promo_sk_node_4=[$8], ss_ticket_number_node_4=[$9], ss_quantity_node_4=[$10], ss_wholesale_cost_node_4=[$11], ss_list_price_node_4=[$12], ss_sales_price_node_4=[$13], ss_ext_discount_amt_node_4=[$14], ss_ext_sales_price_node_4=[$15], ss_ext_wholesale_cost_node_4=[$16], ss_ext_list_price_node_4=[$17], ss_ext_tax_node_4=[$18], ss_coupon_amt_node_4=[$19], ss_net_paid_node_4=[$20], ss_net_paid_inc_tax_node_4=[$21], ss_net_profit_node_4=[$22])
      :     +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])
      +- LogicalProject(sr_refunded_cash_node_5=[$1])
         +- LogicalAggregate(group=[{0}], EXPR$0=[MAX($1)])
            +- LogicalProject(sr_customer_sk_node_5=[$3], sr_refunded_cash_node_5=[$16])
               +- LogicalTableScan(table=[[default_catalog, default_database, store_returns]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS ss_ext_sales_price_node_4])
+- HashAggregate(isMerge=[true], groupBy=[ss_ext_tax], select=[ss_ext_tax, Final_MAX(max$0) AS EXPR$0])
   +- Exchange(distribution=[hash[ss_ext_tax]])
      +- LocalHashAggregate(groupBy=[ss_ext_tax], select=[ss_ext_tax, Partial_MAX(ss_ext_sales_price) AS max$0])
         +- HashJoin(joinType=[InnerJoin], where=[=(sr_refunded_cash_node_5, ss_ext_list_price)], select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit, sr_refunded_cash_node_5], isBroadcast=[true], build=[right])
            :- Sort(orderBy=[ss_promo_sk ASC])
            :  +- Exchange(distribution=[single])
            :     +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
            +- Exchange(distribution=[broadcast])
               +- Calc(select=[EXPR$0 AS sr_refunded_cash_node_5])
                  +- HashAggregate(isMerge=[true], groupBy=[sr_customer_sk], select=[sr_customer_sk, Final_MAX(max$0) AS EXPR$0])
                     +- Exchange(distribution=[hash[sr_customer_sk]])
                        +- LocalHashAggregate(groupBy=[sr_customer_sk], select=[sr_customer_sk, Partial_MAX(sr_refunded_cash) AS max$0])
                           +- TableSourceScan(table=[[default_catalog, default_database, store_returns, project=[sr_customer_sk, sr_refunded_cash], metadata=[]]], fields=[sr_customer_sk, sr_refunded_cash])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS ss_ext_sales_price_node_4])
+- HashAggregate(isMerge=[true], groupBy=[ss_ext_tax], select=[ss_ext_tax, Final_MAX(max$0) AS EXPR$0])
   +- Exchange(distribution=[hash[ss_ext_tax]])
      +- LocalHashAggregate(groupBy=[ss_ext_tax], select=[ss_ext_tax, Partial_MAX(ss_ext_sales_price) AS max$0])
         +- HashJoin(joinType=[InnerJoin], where=[(sr_refunded_cash_node_5 = ss_ext_list_price)], select=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit, sr_refunded_cash_node_5], isBroadcast=[true], build=[right])
            :- Sort(orderBy=[ss_promo_sk ASC])
            :  +- Exchange(distribution=[single])
            :     +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
            +- Exchange(distribution=[broadcast])
               +- Calc(select=[EXPR$0 AS sr_refunded_cash_node_5])
                  +- HashAggregate(isMerge=[true], groupBy=[sr_customer_sk], select=[sr_customer_sk, Final_MAX(max$0) AS EXPR$0])
                     +- Exchange(distribution=[hash[sr_customer_sk]])
                        +- LocalHashAggregate(groupBy=[sr_customer_sk], select=[sr_customer_sk, Partial_MAX(sr_refunded_cash) AS max$0])
                           +- TableSourceScan(table=[[default_catalog, default_database, store_returns, project=[sr_customer_sk, sr_refunded_cash], metadata=[]]], fields=[sr_customer_sk, sr_refunded_cash])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o254620063.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#514880218:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[8](input=RelSubset#514880216,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[8]), rel#514880215:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[8](input=RelSubset#514880214,groupBy=ss_ext_list_price, ss_ext_tax,select=ss_ext_list_price, ss_ext_tax, Partial_MAX(ss_ext_sales_price) AS max$0)]
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