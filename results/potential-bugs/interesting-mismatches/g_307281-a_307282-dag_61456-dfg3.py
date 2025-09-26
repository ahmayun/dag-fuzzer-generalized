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

autonode_10 = table_env.from_path("web_sales").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("web_sales").get_schema().get_field_names()])
autonode_9 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_8 = table_env.from_path("household_demographics").select(*[col(column_name).alias(f"{column_name}_node_8") for column_name in table_env.from_path("household_demographics").get_schema().get_field_names()])
autonode_7 = autonode_10.filter(col('ws_net_paid_inc_ship_tax_node_10') > 25.538653135299683)
autonode_6 = autonode_8.join(autonode_9, col('ss_sold_time_sk_node_9') == col('hd_dep_count_node_8'))
autonode_5 = autonode_7.select(col('ws_order_number_node_10'))
autonode_4 = autonode_6.distinct()
autonode_3 = autonode_5.filter(col('ws_order_number_node_10') <= 40)
autonode_2 = autonode_4.order_by(col('hd_dep_count_node_8'))
autonode_1 = autonode_2.join(autonode_3, col('hd_dep_count_node_8') == col('ws_order_number_node_10'))
sink = autonode_1.group_by(col('ss_ext_list_price_node_9')).select(col('ss_ext_discount_amt_node_9').max.alias('ss_ext_discount_amt_node_9'))
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
LogicalProject(ss_ext_discount_amt_node_9=[$1])
+- LogicalAggregate(group=[{22}], EXPR$0=[MAX($19)])
   +- LogicalJoin(condition=[=($3, $28)], joinType=[inner])
      :- LogicalSort(sort0=[$3], dir0=[ASC])
      :  +- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27}])
      :     +- LogicalJoin(condition=[=($6, $3)], joinType=[inner])
      :        :- LogicalProject(hd_demo_sk_node_8=[$0], hd_income_band_sk_node_8=[$1], hd_buy_potential_node_8=[$2], hd_dep_count_node_8=[$3], hd_vehicle_count_node_8=[$4])
      :        :  +- LogicalTableScan(table=[[default_catalog, default_database, household_demographics]])
      :        +- LogicalProject(ss_sold_date_sk_node_9=[$0], ss_sold_time_sk_node_9=[$1], ss_item_sk_node_9=[$2], ss_customer_sk_node_9=[$3], ss_cdemo_sk_node_9=[$4], ss_hdemo_sk_node_9=[$5], ss_addr_sk_node_9=[$6], ss_store_sk_node_9=[$7], ss_promo_sk_node_9=[$8], ss_ticket_number_node_9=[$9], ss_quantity_node_9=[$10], ss_wholesale_cost_node_9=[$11], ss_list_price_node_9=[$12], ss_sales_price_node_9=[$13], ss_ext_discount_amt_node_9=[$14], ss_ext_sales_price_node_9=[$15], ss_ext_wholesale_cost_node_9=[$16], ss_ext_list_price_node_9=[$17], ss_ext_tax_node_9=[$18], ss_coupon_amt_node_9=[$19], ss_net_paid_node_9=[$20], ss_net_paid_inc_tax_node_9=[$21], ss_net_profit_node_9=[$22])
      :           +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])
      +- LogicalFilter(condition=[<=($0, 40)])
         +- LogicalProject(ws_order_number_node_10=[$17])
            +- LogicalFilter(condition=[>($32, 2.5538653135299683E1:DOUBLE)])
               +- LogicalProject(ws_sold_date_sk_node_10=[$0], ws_sold_time_sk_node_10=[$1], ws_ship_date_sk_node_10=[$2], ws_item_sk_node_10=[$3], ws_bill_customer_sk_node_10=[$4], ws_bill_cdemo_sk_node_10=[$5], ws_bill_hdemo_sk_node_10=[$6], ws_bill_addr_sk_node_10=[$7], ws_ship_customer_sk_node_10=[$8], ws_ship_cdemo_sk_node_10=[$9], ws_ship_hdemo_sk_node_10=[$10], ws_ship_addr_sk_node_10=[$11], ws_web_page_sk_node_10=[$12], ws_web_site_sk_node_10=[$13], ws_ship_mode_sk_node_10=[$14], ws_warehouse_sk_node_10=[$15], ws_promo_sk_node_10=[$16], ws_order_number_node_10=[$17], ws_quantity_node_10=[$18], ws_wholesale_cost_node_10=[$19], ws_list_price_node_10=[$20], ws_sales_price_node_10=[$21], ws_ext_discount_amt_node_10=[$22], ws_ext_sales_price_node_10=[$23], ws_ext_wholesale_cost_node_10=[$24], ws_ext_list_price_node_10=[$25], ws_ext_tax_node_10=[$26], ws_coupon_amt_node_10=[$27], ws_ext_ship_cost_node_10=[$28], ws_net_paid_node_10=[$29], ws_net_paid_inc_tax_node_10=[$30], ws_net_paid_inc_ship_node_10=[$31], ws_net_paid_inc_ship_tax_node_10=[$32], ws_net_profit_node_10=[$33])
                  +- LogicalTableScan(table=[[default_catalog, default_database, web_sales]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS ss_ext_discount_amt_node_9])
+- HashAggregate(isMerge=[false], groupBy=[ss_ext_list_price], select=[ss_ext_list_price, MAX(ss_ext_discount_amt) AS EXPR$0])
   +- Exchange(distribution=[hash[ss_ext_list_price]])
      +- NestedLoopJoin(joinType=[InnerJoin], where=[=(hd_dep_count, ws_order_number_node_10)], select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit, ws_order_number_node_10], build=[right])
         :- Sort(orderBy=[hd_dep_count ASC])
         :  +- Exchange(distribution=[single])
         :     +- HashAggregate(isMerge=[false], groupBy=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
         :        +- Exchange(distribution=[hash[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit]])
         :           +- HashJoin(joinType=[InnerJoin], where=[=(ss_sold_time_sk, hd_dep_count)], select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], isBroadcast=[true], build=[left])
         :              :- Exchange(distribution=[broadcast])
         :              :  +- TableSourceScan(table=[[default_catalog, default_database, household_demographics]], fields=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
         :              +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
         +- Exchange(distribution=[broadcast])
            +- Calc(select=[ws_order_number AS ws_order_number_node_10], where=[AND(>(ws_net_paid_inc_ship_tax, 2.5538653135299683E1), <=(ws_order_number, 40))])
               +- TableSourceScan(table=[[default_catalog, default_database, web_sales, filter=[and(>(ws_net_paid_inc_ship_tax, 2.5538653135299683E1:DOUBLE), <=(ws_order_number, 40))], project=[ws_order_number, ws_net_paid_inc_ship_tax], metadata=[]]], fields=[ws_order_number, ws_net_paid_inc_ship_tax])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS ss_ext_discount_amt_node_9])
+- HashAggregate(isMerge=[false], groupBy=[ss_ext_list_price], select=[ss_ext_list_price, MAX(ss_ext_discount_amt) AS EXPR$0])
   +- Exchange(distribution=[hash[ss_ext_list_price]])
      +- NestedLoopJoin(joinType=[InnerJoin], where=[(hd_dep_count = ws_order_number_node_10)], select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit, ws_order_number_node_10], build=[right])
         :- Sort(orderBy=[hd_dep_count ASC])
         :  +- Exchange(distribution=[single])
         :     +- HashAggregate(isMerge=[false], groupBy=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
         :        +- Exchange(distribution=[hash[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit]])
         :           +- MultipleInput(readOrder=[0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(ss_sold_time_sk = hd_dep_count)], select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit], isBroadcast=[true], build=[left])\
:- [#1] Exchange(distribution=[broadcast])\
+- [#2] TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])\
])
         :              :- Exchange(distribution=[broadcast])
         :              :  +- TableSourceScan(table=[[default_catalog, default_database, household_demographics]], fields=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
         :              +- TableSourceScan(table=[[default_catalog, default_database, store_sales]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
         +- Exchange(distribution=[broadcast])
            +- Calc(select=[ws_order_number AS ws_order_number_node_10], where=[((ws_net_paid_inc_ship_tax > 2.5538653135299683E1) AND (ws_order_number <= 40))])
               +- TableSourceScan(table=[[default_catalog, default_database, web_sales, filter=[and(>(ws_net_paid_inc_ship_tax, 2.5538653135299683E1:DOUBLE), <=(ws_order_number, 40))], project=[ws_order_number, ws_net_paid_inc_ship_tax], metadata=[]]], fields=[ws_order_number, ws_net_paid_inc_ship_tax])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o167117800.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#337851396:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[3](input=RelSubset#337851394,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[3]), rel#337851393:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[3](input=RelSubset#337851392,groupBy=hd_dep_count, ss_ext_list_price,select=hd_dep_count, ss_ext_list_price, Partial_MAX(ss_ext_discount_amt) AS max$0)]
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