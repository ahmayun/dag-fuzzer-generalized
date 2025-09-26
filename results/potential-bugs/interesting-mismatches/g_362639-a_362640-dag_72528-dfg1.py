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
    return values.std()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_6 = table_env.from_path("catalog_sales").select(*[col(column_name).alias(f"{column_name}_node_6") for column_name in table_env.from_path("catalog_sales").get_schema().get_field_names()])
autonode_7 = table_env.from_path("time_dim").select(*[col(column_name).alias(f"{column_name}_node_7") for column_name in table_env.from_path("time_dim").get_schema().get_field_names()])
autonode_4 = autonode_6.alias('sb07D')
autonode_5 = autonode_7.filter(col('t_meal_time_node_7').char_length > 5)
autonode_2 = autonode_4.order_by(col('cs_quantity_node_6'))
autonode_3 = autonode_5.limit(9)
autonode_1 = autonode_2.join(autonode_3, col('t_time_node_7') == col('cs_ship_addr_sk_node_6'))
sink = autonode_1.group_by(col('t_shift_node_7')).select(col('cs_ext_ship_cost_node_6').max.alias('cs_ext_ship_cost_node_6'))
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
      "error_message": "An error occurred while calling o197356552.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#399021241:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[18](input=RelSubset#399021239,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[18]), rel#399021238:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[18](input=RelSubset#399021237,groupBy=cs_ship_addr_sk,select=cs_ship_addr_sk, Partial_MAX(cs_ext_ship_cost) AS max$0)]
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
LogicalProject(cs_ext_ship_cost_node_6=[$1])
+- LogicalAggregate(group=[{41}], EXPR$0=[MAX($28)])
   +- LogicalJoin(condition=[=($36, $10)], joinType=[inner])
      :- LogicalSort(sort0=[$18], dir0=[ASC])
      :  +- LogicalProject(sb07D=[AS($0, _UTF-16LE'sb07D')], cs_sold_time_sk_node_6=[$1], cs_ship_date_sk_node_6=[$2], cs_bill_customer_sk_node_6=[$3], cs_bill_cdemo_sk_node_6=[$4], cs_bill_hdemo_sk_node_6=[$5], cs_bill_addr_sk_node_6=[$6], cs_ship_customer_sk_node_6=[$7], cs_ship_cdemo_sk_node_6=[$8], cs_ship_hdemo_sk_node_6=[$9], cs_ship_addr_sk_node_6=[$10], cs_call_center_sk_node_6=[$11], cs_catalog_page_sk_node_6=[$12], cs_ship_mode_sk_node_6=[$13], cs_warehouse_sk_node_6=[$14], cs_item_sk_node_6=[$15], cs_promo_sk_node_6=[$16], cs_order_number_node_6=[$17], cs_quantity_node_6=[$18], cs_wholesale_cost_node_6=[$19], cs_list_price_node_6=[$20], cs_sales_price_node_6=[$21], cs_ext_discount_amt_node_6=[$22], cs_ext_sales_price_node_6=[$23], cs_ext_wholesale_cost_node_6=[$24], cs_ext_list_price_node_6=[$25], cs_ext_tax_node_6=[$26], cs_coupon_amt_node_6=[$27], cs_ext_ship_cost_node_6=[$28], cs_net_paid_node_6=[$29], cs_net_paid_inc_tax_node_6=[$30], cs_net_paid_inc_ship_node_6=[$31], cs_net_paid_inc_ship_tax_node_6=[$32], cs_net_profit_node_6=[$33])
      :     +- LogicalTableScan(table=[[default_catalog, default_database, catalog_sales]])
      +- LogicalSort(fetch=[9])
         +- LogicalFilter(condition=[>(CHAR_LENGTH($9), 5)])
            +- LogicalProject(t_time_sk_node_7=[$0], t_time_id_node_7=[$1], t_time_node_7=[$2], t_hour_node_7=[$3], t_minute_node_7=[$4], t_second_node_7=[$5], t_am_pm_node_7=[$6], t_shift_node_7=[$7], t_sub_shift_node_7=[$8], t_meal_time_node_7=[$9])
               +- LogicalTableScan(table=[[default_catalog, default_database, time_dim]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS cs_ext_ship_cost_node_6])
+- SortAggregate(isMerge=[false], groupBy=[t_shift], select=[t_shift, MAX(cs_ext_ship_cost_node_6) AS EXPR$0])
   +- Sort(orderBy=[t_shift ASC])
      +- Exchange(distribution=[hash[t_shift]])
         +- NestedLoopJoin(joinType=[InnerJoin], where=[=(t_time, cs_ship_addr_sk_node_6)], select=[sb07D, cs_sold_time_sk_node_6, cs_ship_date_sk_node_6, cs_bill_customer_sk_node_6, cs_bill_cdemo_sk_node_6, cs_bill_hdemo_sk_node_6, cs_bill_addr_sk_node_6, cs_ship_customer_sk_node_6, cs_ship_cdemo_sk_node_6, cs_ship_hdemo_sk_node_6, cs_ship_addr_sk_node_6, cs_call_center_sk_node_6, cs_catalog_page_sk_node_6, cs_ship_mode_sk_node_6, cs_warehouse_sk_node_6, cs_item_sk_node_6, cs_promo_sk_node_6, cs_order_number_node_6, cs_quantity_node_6, cs_wholesale_cost_node_6, cs_list_price_node_6, cs_sales_price_node_6, cs_ext_discount_amt_node_6, cs_ext_sales_price_node_6, cs_ext_wholesale_cost_node_6, cs_ext_list_price_node_6, cs_ext_tax_node_6, cs_coupon_amt_node_6, cs_ext_ship_cost_node_6, cs_net_paid_node_6, cs_net_paid_inc_tax_node_6, cs_net_paid_inc_ship_node_6, cs_net_paid_inc_ship_tax_node_6, cs_net_profit_node_6, t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time], build=[left])
            :- Exchange(distribution=[broadcast])
            :  +- Calc(select=[cs_sold_date_sk AS sb07D, cs_sold_time_sk AS cs_sold_time_sk_node_6, cs_ship_date_sk AS cs_ship_date_sk_node_6, cs_bill_customer_sk AS cs_bill_customer_sk_node_6, cs_bill_cdemo_sk AS cs_bill_cdemo_sk_node_6, cs_bill_hdemo_sk AS cs_bill_hdemo_sk_node_6, cs_bill_addr_sk AS cs_bill_addr_sk_node_6, cs_ship_customer_sk AS cs_ship_customer_sk_node_6, cs_ship_cdemo_sk AS cs_ship_cdemo_sk_node_6, cs_ship_hdemo_sk AS cs_ship_hdemo_sk_node_6, cs_ship_addr_sk AS cs_ship_addr_sk_node_6, cs_call_center_sk AS cs_call_center_sk_node_6, cs_catalog_page_sk AS cs_catalog_page_sk_node_6, cs_ship_mode_sk AS cs_ship_mode_sk_node_6, cs_warehouse_sk AS cs_warehouse_sk_node_6, cs_item_sk AS cs_item_sk_node_6, cs_promo_sk AS cs_promo_sk_node_6, cs_order_number AS cs_order_number_node_6, cs_quantity AS cs_quantity_node_6, cs_wholesale_cost AS cs_wholesale_cost_node_6, cs_list_price AS cs_list_price_node_6, cs_sales_price AS cs_sales_price_node_6, cs_ext_discount_amt AS cs_ext_discount_amt_node_6, cs_ext_sales_price AS cs_ext_sales_price_node_6, cs_ext_wholesale_cost AS cs_ext_wholesale_cost_node_6, cs_ext_list_price AS cs_ext_list_price_node_6, cs_ext_tax AS cs_ext_tax_node_6, cs_coupon_amt AS cs_coupon_amt_node_6, cs_ext_ship_cost AS cs_ext_ship_cost_node_6, cs_net_paid AS cs_net_paid_node_6, cs_net_paid_inc_tax AS cs_net_paid_inc_tax_node_6, cs_net_paid_inc_ship AS cs_net_paid_inc_ship_node_6, cs_net_paid_inc_ship_tax AS cs_net_paid_inc_ship_tax_node_6, cs_net_profit AS cs_net_profit_node_6])
            :     +- SortLimit(orderBy=[cs_quantity ASC], offset=[0], fetch=[1], global=[true])
            :        +- Exchange(distribution=[single])
            :           +- SortLimit(orderBy=[cs_quantity ASC], offset=[0], fetch=[1], global=[false])
            :              +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])
            +- Limit(offset=[0], fetch=[9], global=[true])
               +- Exchange(distribution=[single])
                  +- Limit(offset=[0], fetch=[9], global=[false])
                     +- Calc(select=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time], where=[>(CHAR_LENGTH(t_meal_time), 5)])
                        +- TableSourceScan(table=[[default_catalog, default_database, time_dim, filter=[>(CHAR_LENGTH(t_meal_time), 5)]]], fields=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS cs_ext_ship_cost_node_6])
+- SortAggregate(isMerge=[false], groupBy=[t_shift], select=[t_shift, MAX(cs_ext_ship_cost_node_6) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[t_shift ASC])
         +- Exchange(distribution=[hash[t_shift]])
            +- NestedLoopJoin(joinType=[InnerJoin], where=[(t_time = cs_ship_addr_sk_node_6)], select=[sb07D, cs_sold_time_sk_node_6, cs_ship_date_sk_node_6, cs_bill_customer_sk_node_6, cs_bill_cdemo_sk_node_6, cs_bill_hdemo_sk_node_6, cs_bill_addr_sk_node_6, cs_ship_customer_sk_node_6, cs_ship_cdemo_sk_node_6, cs_ship_hdemo_sk_node_6, cs_ship_addr_sk_node_6, cs_call_center_sk_node_6, cs_catalog_page_sk_node_6, cs_ship_mode_sk_node_6, cs_warehouse_sk_node_6, cs_item_sk_node_6, cs_promo_sk_node_6, cs_order_number_node_6, cs_quantity_node_6, cs_wholesale_cost_node_6, cs_list_price_node_6, cs_sales_price_node_6, cs_ext_discount_amt_node_6, cs_ext_sales_price_node_6, cs_ext_wholesale_cost_node_6, cs_ext_list_price_node_6, cs_ext_tax_node_6, cs_coupon_amt_node_6, cs_ext_ship_cost_node_6, cs_net_paid_node_6, cs_net_paid_inc_tax_node_6, cs_net_paid_inc_ship_node_6, cs_net_paid_inc_ship_tax_node_6, cs_net_profit_node_6, t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time], build=[left])
               :- Exchange(distribution=[broadcast])
               :  +- Calc(select=[cs_sold_date_sk AS sb07D, cs_sold_time_sk AS cs_sold_time_sk_node_6, cs_ship_date_sk AS cs_ship_date_sk_node_6, cs_bill_customer_sk AS cs_bill_customer_sk_node_6, cs_bill_cdemo_sk AS cs_bill_cdemo_sk_node_6, cs_bill_hdemo_sk AS cs_bill_hdemo_sk_node_6, cs_bill_addr_sk AS cs_bill_addr_sk_node_6, cs_ship_customer_sk AS cs_ship_customer_sk_node_6, cs_ship_cdemo_sk AS cs_ship_cdemo_sk_node_6, cs_ship_hdemo_sk AS cs_ship_hdemo_sk_node_6, cs_ship_addr_sk AS cs_ship_addr_sk_node_6, cs_call_center_sk AS cs_call_center_sk_node_6, cs_catalog_page_sk AS cs_catalog_page_sk_node_6, cs_ship_mode_sk AS cs_ship_mode_sk_node_6, cs_warehouse_sk AS cs_warehouse_sk_node_6, cs_item_sk AS cs_item_sk_node_6, cs_promo_sk AS cs_promo_sk_node_6, cs_order_number AS cs_order_number_node_6, cs_quantity AS cs_quantity_node_6, cs_wholesale_cost AS cs_wholesale_cost_node_6, cs_list_price AS cs_list_price_node_6, cs_sales_price AS cs_sales_price_node_6, cs_ext_discount_amt AS cs_ext_discount_amt_node_6, cs_ext_sales_price AS cs_ext_sales_price_node_6, cs_ext_wholesale_cost AS cs_ext_wholesale_cost_node_6, cs_ext_list_price AS cs_ext_list_price_node_6, cs_ext_tax AS cs_ext_tax_node_6, cs_coupon_amt AS cs_coupon_amt_node_6, cs_ext_ship_cost AS cs_ext_ship_cost_node_6, cs_net_paid AS cs_net_paid_node_6, cs_net_paid_inc_tax AS cs_net_paid_inc_tax_node_6, cs_net_paid_inc_ship AS cs_net_paid_inc_ship_node_6, cs_net_paid_inc_ship_tax AS cs_net_paid_inc_ship_tax_node_6, cs_net_profit AS cs_net_profit_node_6])
               :     +- SortLimit(orderBy=[cs_quantity ASC], offset=[0], fetch=[1], global=[true])
               :        +- Exchange(distribution=[single])
               :           +- SortLimit(orderBy=[cs_quantity ASC], offset=[0], fetch=[1], global=[false])
               :              +- TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])
               +- Limit(offset=[0], fetch=[9], global=[true])
                  +- Exchange(distribution=[single])
                     +- Limit(offset=[0], fetch=[9], global=[false])
                        +- Calc(select=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time], where=[(CHAR_LENGTH(t_meal_time) > 5)])
                           +- TableSourceScan(table=[[default_catalog, default_database, time_dim, filter=[>(CHAR_LENGTH(t_meal_time), 5)]]], fields=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time])

",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0